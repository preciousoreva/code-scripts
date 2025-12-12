import os
import json
import time
import base64
import stat
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
    Return a valid access token, refreshing with the refresh_token if needed.
    """
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