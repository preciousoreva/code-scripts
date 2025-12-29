#!/usr/bin/env python3
"""
Pre-commit hook wrapper for gitleaks that automatically downloads the binary.

This hook is self-contained and does not require gitleaks to be installed globally.
It downloads the appropriate binary for the platform (macOS/Windows/Linux) on first run.
"""

import os
import sys
import platform
import subprocess
import urllib.request
import ssl
import zipfile
import tarfile
from pathlib import Path

# Try to import certifi for SSL certificate verification
try:
    import certifi
except ImportError:
    certifi = None

# Gitleaks version to use
GITLEAKS_VERSION = "v8.18.0"

# Directory to store the gitleaks binary (gitignored)
HOOKS_DIR = Path(__file__).resolve().parent
BIN_DIR = HOOKS_DIR / ".bin"
BIN_DIR.mkdir(exist_ok=True)


def get_platform_info():
    """Determine platform and architecture for gitleaks binary."""
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    # Map platform to gitleaks release naming
    # GitHub releases use format: gitleaks_8.18.0_darwin_arm64.tar.gz (version without 'v' prefix)
    version_num = GITLEAKS_VERSION.lstrip('v')  # Remove 'v' prefix for filename
    
    if system == "darwin":
        if machine in ("arm64", "aarch64"):
            return "darwin", "arm64", "gitleaks_{}_darwin_arm64.tar.gz".format(version_num)
        else:
            return "darwin", "amd64", "gitleaks_{}_darwin_amd64.tar.gz".format(version_num)
    elif system == "windows":
        if machine in ("arm64", "aarch64"):
            return "windows", "arm64", "gitleaks_{}_windows_arm64.zip".format(version_num)
        else:
            return "windows", "amd64", "gitleaks_{}_windows_amd64.zip".format(version_num)
    elif system == "linux":
        if machine in ("arm64", "aarch64"):
            return "linux", "arm64", "gitleaks_{}_linux_arm64.tar.gz".format(version_num)
        else:
            return "linux", "amd64", "gitleaks_{}_linux_amd64.tar.gz".format(version_num)
    else:
        raise RuntimeError(f"Unsupported platform: {system}")


def download_gitleaks():
    """Download and extract gitleaks binary for the current platform."""
    platform_name, arch, filename = get_platform_info()
    binary_name = "gitleaks.exe" if platform_name == "windows" else "gitleaks"
    binary_path = BIN_DIR / binary_name
    
    # If binary already exists, use it
    if binary_path.exists():
        return binary_path
    
    # Download URL
    url = f"https://github.com/gitleaks/gitleaks/releases/download/{GITLEAKS_VERSION}/{filename}"
    
    print(f"Downloading gitleaks {GITLEAKS_VERSION} for {platform_name}/{arch}...")
    print(f"URL: {url}")
    
    # Create SSL context with certifi certificates if available
    ssl_context = ssl.create_default_context()
    if certifi:
        try:
            ssl_context.load_verify_locations(certifi.where())
        except Exception:
            # If certifi fails, use default context (may fail on some systems)
            pass
    
    # Download the release archive
    archive_path = BIN_DIR / filename
    try:
        # Use urlopen with SSL context for certificate verification
        with urllib.request.urlopen(url, context=ssl_context) as response:
            with open(archive_path, 'wb') as out_file:
                out_file.write(response.read())
    except Exception as e:
        print(f"Error downloading gitleaks: {e}", file=sys.stderr)
        if "CERTIFICATE_VERIFY_FAILED" in str(e):
            print("\nTip: Install certificates by running:", file=sys.stderr)
            print("  python3 -m pip install --upgrade certifi", file=sys.stderr)
            print("  Or on macOS: /Applications/Python\\ 3.*/Install\\ Certificates.command", file=sys.stderr)
        sys.exit(1)
    
    # Extract the archive
    try:
        if filename.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zip_ref:
                zip_ref.extractall(BIN_DIR)
        elif filename.endswith(".tar.gz"):
            with tarfile.open(archive_path, "r:gz") as tar_ref:
                tar_ref.extractall(BIN_DIR)
        else:
            raise RuntimeError(f"Unknown archive format: {filename}")
        
        # Clean up archive
        archive_path.unlink()
        
        # Make binary executable on Unix-like systems
        if platform_name != "windows":
            binary_path.chmod(0o755)
        
        if not binary_path.exists():
            raise RuntimeError(f"Binary not found after extraction: {binary_path}")
        
        print(f"✓ gitleaks downloaded to {binary_path}")
        return binary_path
        
    except Exception as e:
        print(f"Error extracting gitleaks: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Run gitleaks on staged files."""
    try:
        # Download gitleaks if needed
        gitleaks_bin = download_gitleaks()
        
        # Run gitleaks protect on staged files
        cmd = [
            str(gitleaks_bin),
            "protect",
            "--no-banner",
            "--staged",
            "--verbose"
        ]
        
        result = subprocess.run(cmd, capture_output=False)
        sys.exit(result.returncode)
        
    except Exception as e:
        print(f"Error running gitleaks: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

