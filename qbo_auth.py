import os
import json
import time
import base64
import stat
import socket
from pathlib import Path

import requests

# Load .env file if it exists (makes credential management easier)
from load_env import load_env_file
load_env_file()

# QBO OAuth token endpoint (same for sandbox and prod)
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# These must be set via environment variables for security
CLIENT_ID = os.environ.get("QBO_CLIENT_ID")
CLIENT_SECRET = os.environ.get("QBO_CLIENT_SECRET")

# Token storage file next to this script
SCRIPT_DIR = Path(__file__).resolve().parent
TOKEN_FILE = SCRIPT_DIR / "qbo_tokens.json"
CACHE_FILE = SCRIPT_DIR / "qbo_tokens_cache.json"


def _validate_credentials() -> None:
    """Validate that required credentials are set via environment variables."""
    if not CLIENT_ID:
        raise RuntimeError(
            "QBO_CLIENT_ID environment variable is not set. "
            "Please set it before running the pipeline:\n"
            "  export QBO_CLIENT_ID='your_client_id'"
        )
    if not CLIENT_SECRET:
        raise RuntimeError(
            "QBO_CLIENT_SECRET environment variable is not set. "
            "Please set it before running the pipeline:\n"
            "  export QBO_CLIENT_SECRET='your_client_secret'"
        )


def load_tokens() -> dict:
    """Load tokens from qbo_tokens.json, or return an empty dict if missing/invalid."""
    if not TOKEN_FILE.exists():
        return {}
    try:
        with TOKEN_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_tokens(tokens: dict) -> None:
    """Persist tokens to qbo_tokens.json with restricted file permissions."""
    with TOKEN_FILE.open("w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)
    
    # Restrict file permissions to owner only (read/write for owner, no access for others)
    TOKEN_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600


def load_cache() -> dict:
    """Load cached tokens from qbo_tokens_cache.json, or return an empty dict if missing/invalid."""
    if not CACHE_FILE.exists():
        return {}
    try:
        with CACHE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(tokens: dict) -> None:
    """Persist cached tokens to qbo_tokens_cache.json with restricted file permissions."""
    with CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)
    
    # Restrict file permissions to owner only (read/write for owner, no access for others)
    CACHE_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600


def is_cache_token_valid(cache: dict) -> bool:
    """Return True if the cached access token exists and is not expired (with 60s safety margin)."""
    access_token = cache.get("access_token")
    expires_at = cache.get("expires_at")

    if not access_token or not expires_at:
        return False

    # Safety margin: consider expired 60 seconds before actual expiry
    return time.time() < (expires_at - 60)


def _check_tunnel_connectivity(broker_url: str) -> tuple[bool, str]:
    """
    Check if the SSH tunnel port is reachable.
    Returns (is_reachable, error_message).
    """
    try:
        # Parse URL to get host and port
        if broker_url.startswith("http://"):
            url_part = broker_url[7:]
        elif broker_url.startswith("https://"):
            url_part = broker_url[8:]
        else:
            return True, ""  # Can't parse, skip check
        
        # Extract host and port
        if "/" in url_part:
            host_port = url_part.split("/")[0]
        else:
            host_port = url_part
        
        if ":" in host_port:
            host, port_str = host_port.split(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                return True, ""  # Invalid port, skip check
        else:
            host = host_port
            port = 8765  # Default port
        
        # Try to connect to the port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result != 0:
            return False, (
                f"SSH tunnel appears to be down. Port {port} on {host} is not reachable.\n"
                f"To fix this, establish the SSH tunnel in a separate terminal:\n"
                f"  ssh -L {port}:127.0.0.1:{port} user@windows-machine\n"
                f"Then run your command again."
            )
        return True, ""
    except Exception:
        # If check fails for any reason, don't block the request
        return True, ""


def get_access_token_from_broker() -> str:
    """
    Fetch access token from Windows broker, cache it locally, and return it.
    Raises RuntimeError if broker is unreachable or returns non-200.
    """
    broker_url = os.environ.get("QBO_TOKEN_BROKER_URL")
    broker_key = os.environ.get("QBO_TOKEN_BROKER_KEY")
    
    if not broker_url or not broker_key:
        raise RuntimeError("Broker URL and key must be set to use broker mode")
    
    # Ensure URL ends with /token (helpful error if missing)
    if not broker_url.rstrip('/').endswith('/token'):
        raise RuntimeError(
            f"Broker URL should end with '/token', got: {broker_url}\n"
            f"Example: http://127.0.0.1:8765/token"
        )
    
    headers = {"x-broker-key": broker_key}
    
    try:
        resp = requests.get(broker_url, headers=headers, timeout=5)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError(
            "Broker request timed out after 5 seconds.\n"
            "This may indicate the SSH tunnel is down or the broker is not responding.\n"
            "Check that:\n"
            "  1. SSH tunnel is established: ssh -L 8765:127.0.0.1:8765 user@windows-machine\n"
            "  2. Windows broker service is running on port 8765"
        )
    except requests.exceptions.ConnectionError as e:
        # Check if tunnel is down and provide helpful guidance
        is_reachable, tunnel_msg = _check_tunnel_connectivity(broker_url)
        if not is_reachable:
            raise RuntimeError(tunnel_msg)
        else:
            raise RuntimeError(
                f"Broker connection failed: {e}\n"
                f"This usually means:\n"
                f"  1. SSH tunnel is not established\n"
                f"  2. Windows broker service is not running\n"
                f"  3. Broker URL is incorrect\n\n"
                f"To establish SSH tunnel:\n"
                f"  ssh -L 8765:127.0.0.1:8765 user@windows-machine"
            )
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Broker returned error: {e}")
    
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError("Broker returned invalid JSON response")
    
    access_token = data.get("access_token")
    expires_at = data.get("expires_at")
    
    if not access_token:
        raise RuntimeError("Broker response missing access_token")
    if expires_at is None:
        raise RuntimeError("Broker response missing expires_at")
    
    # Cache the token with sync timestamp
    cache_data = {
        "access_token": access_token,
        "expires_at": float(expires_at),
        "last_synced": time.time()
    }
    save_cache(cache_data)
    
    return access_token


def is_token_expired(tokens: dict) -> bool:
    """Return True if the stored access token is expired or missing."""
    access_token = tokens.get("access_token")
    expires_at = tokens.get("expires_at")

    if not access_token or not expires_at:
        return True

    # Safety margin: refresh 60 seconds before actual expiry
    return time.time() > (expires_at - 60)


def refresh_access_token(tokens: dict) -> dict:
    """Use the stored refresh_token to obtain a new access token."""
    _validate_credentials()
    
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError(
            "No refresh_token found in qbo_tokens.json. "
            "Run the OAuth flow once and store refresh_token there."
        )

    # Basic auth header: base64(client_id:client_secret)
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
    auth_header = base64.b64encode(auth_str).decode("utf-8")

    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)
    if resp.status_code != 200:
        error_detail = resp.text
        # Provide more helpful error messages for common issues
        if resp.status_code == 401:
            if "invalid_client" in error_detail:
                raise RuntimeError(
                    f"Invalid CLIENT_ID or CLIENT_SECRET (401 invalid_client).\n"
                    f"This usually means:\n"
                    f"  1. The credentials in your .env file are incorrect\n"
                    f"  2. The credentials don't match your QuickBooks app (sandbox vs production)\n"
                    f"  3. The refresh token was issued for different credentials\n\n"
                    f"Please verify:\n"
                    f"  - QBO_CLIENT_ID and QBO_CLIENT_SECRET in your .env file\n"
                    f"  - That they match your Intuit Developer app credentials\n"
                    f"  - That you're using the correct environment (sandbox vs production)\n\n"
                    f"Response: {resp.status_code} {error_detail}"
                )
            else:
                raise RuntimeError(
                    f"Authentication failed (401). Check your CLIENT_ID and CLIENT_SECRET.\n"
                    f"Response: {error_detail}"
                )
        elif resp.status_code == 400 and "invalid_grant" in error_detail:
            raise RuntimeError(
                f"Refresh token is invalid or expired (400 invalid_grant).\n"
                f"This usually means:\n"
                f"  1. The refresh token has expired (typically after ~100 days)\n"
                f"  2. The refresh token was revoked or invalidated\n"
                f"  3. The token was issued for different credentials\n\n"
                f"To fix this, you need to re-authenticate:\n"
                f"  1. Go to Intuit Developer Portal (https://developer.intuit.com/)\n"
                f"  2. Use the OAuth playground or perform OAuth flow\n"
                f"  3. Get new access_token and refresh_token\n"
                f"  4. Update qbo_tokens.json with the new tokens\n\n"
                f"Response: {resp.status_code} {error_detail}"
            )
        else:
            raise RuntimeError(
                f"Failed to refresh access token: {resp.status_code} {error_detail}"
            )

    body = resp.json()
    new_access_token = body.get("access_token")
    new_refresh_token = body.get("refresh_token", refresh_token)
    expires_in = body.get("expires_in", 3600)

    if not new_access_token:
        raise RuntimeError("Token refresh response missing access_token")

    tokens["access_token"] = new_access_token
    tokens["refresh_token"] = new_refresh_token
    tokens["expires_at"] = time.time() + int(expires_in)

    save_tokens(tokens)
    return tokens


def get_access_token() -> str:
    """
    Return a valid access token.
    
    If QBO_TOKEN_BROKER_URL and QBO_TOKEN_BROKER_KEY are set:
    - Fetches token from broker and caches it locally.
    - Falls back to cached token if broker is unreachable.
    - Raises error if broker is unreachable and cache is expired/missing.
    
    Otherwise (backward compatibility):
    - Uses local qbo_tokens.json and refreshes if needed.
    """
    broker_url = os.environ.get("QBO_TOKEN_BROKER_URL")
    broker_key = os.environ.get("QBO_TOKEN_BROKER_KEY")
    
    # Broker mode: Windows is the authority
    if broker_url and broker_key:
        try:
            return get_access_token_from_broker()
        except RuntimeError as broker_error:
            # Broker failed, try cache fallback
            cache = load_cache()
            if is_cache_token_valid(cache):
                return cache["access_token"]
            else:
                # Both broker and cache failed
                raise RuntimeError(
                    f"Broker unreachable and cached token expired/missing.\n"
                    f"Broker error: {broker_error}\n"
                    f"Please ensure the broker is running and SSH tunnel is established."
                )
    
    # Legacy mode: local token management (backward compatibility)
    _validate_credentials()
    
    tokens = load_tokens()
    if not tokens:
        raise RuntimeError(
            "qbo_tokens.json not found or empty. "
            "Create it with at least refresh_token and access_token from the OAuth flow."
        )

    if is_token_expired(tokens):
        tokens = refresh_access_token(tokens)

    return tokens["access_token"]