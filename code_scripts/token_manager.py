"""
QBO Token Manager with SQLite Storage

Manages QBO access tokens and refresh tokens in SQLite database, isolated by company_key and realm_id.
Prevents token mixing between companies.
"""

import os
import json
import time
import sqlite3
import stat
import base64
from pathlib import Path
from typing import Optional, Dict, Any
import threading

import requests

from code_scripts.load_env import load_env_file

# Load .env for shared credentials
load_env_file()

# QBO OAuth token endpoint
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# These must be set via environment variables
CLIENT_ID = os.environ.get("QBO_CLIENT_ID")
CLIENT_SECRET = os.environ.get("QBO_CLIENT_SECRET")

# SQLite database file
SCRIPT_DIR = Path(__file__).resolve().parent
DB_FILE = SCRIPT_DIR / "qbo_tokens.sqlite"

# Thread lock for database operations
_db_lock = threading.Lock()


def _validate_credentials() -> None:
    """Validate that required credentials are set."""
    if not CLIENT_ID:
        raise RuntimeError(
            "QBO_CLIENT_ID environment variable is not set. "
            "Please set it in your .env file."
        )
    if not CLIENT_SECRET:
        raise RuntimeError(
            "QBO_CLIENT_SECRET environment variable is not set. "
            "Please set it in your .env file."
        )


def _init_database() -> None:
    """Initialize SQLite database with qbo_tokens table if it doesn't exist."""
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS qbo_tokens (
                    company_key TEXT NOT NULL,
                    realm_id TEXT NOT NULL UNIQUE,
                    access_token TEXT,
                    refresh_token TEXT NOT NULL,
                    access_expires_at INTEGER,
                    updated_at INTEGER NOT NULL,
                    environment TEXT DEFAULT 'production',
                    PRIMARY KEY (company_key, realm_id)
                )
            """)
            conn.commit()
        finally:
            conn.close()
        
        # Restrict file permissions
        try:
            DB_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600
        except OSError as e:
            # On network shares (SMB) or certain filesystems, chmod may be unsupported or treated as read-only.
            # Token reads/writes can still work, so we treat chmod as best-effort.
            if getattr(e, "errno", None) in (1, 30, 95):  # EPERM, EROFS, EOPNOTSUPP
                pass
            else:
                raise


def load_tokens(company_key: str, realm_id: str) -> Optional[Dict[str, Any]]:
    """
    Load tokens from database for a specific company/realm.
    
    Returns:
        Dict with access_token, refresh_token, expires_at, or None if not found
    """
    _init_database()
    
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            cursor = conn.execute(
                "SELECT access_token, refresh_token, access_expires_at, updated_at, environment "
                "FROM qbo_tokens WHERE company_key = ? AND realm_id = ?",
                (company_key, realm_id)
            )
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return {
                "access_token": row[0],
                "refresh_token": row[1],
                "expires_at": row[2],
                "updated_at": row[3],
                "environment": row[4] or "production",
            }
        finally:
            conn.close()


def save_tokens(
    company_key: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expires_at: float,
    environment: str = "production"
) -> None:
    """
    Save tokens to database for a specific company/realm.
    
    Args:
        company_key: Company identifier (e.g., 'company_a', 'company_b')
        realm_id: QBO Realm ID
        access_token: Access token
        refresh_token: Refresh token
        expires_at: Unix timestamp when access token expires
        environment: 'production' or 'sandbox'
    """
    _init_database()
    _validate_credentials()
    
    updated_at = int(time.time())
    
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO qbo_tokens 
                (company_key, realm_id, access_token, refresh_token, access_expires_at, updated_at, environment)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (company_key, realm_id, access_token, refresh_token, int(expires_at), updated_at, environment))
            conn.commit()
        finally:
            conn.close()


def is_token_expired(tokens: Optional[Dict[str, Any]]) -> bool:
    """Return True if token is expired or missing (with 60s safety margin)."""
    if not tokens:
        return True
    
    access_token = tokens.get("access_token")
    expires_at = tokens.get("expires_at")
    
    if not access_token or not expires_at:
        return True
    
    # Safety margin: refresh 60 seconds before actual expiry
    return time.time() > (expires_at - 60)


def refresh_access_token(company_key: str, realm_id: str) -> Dict[str, Any]:
    """
    Refresh access token using refresh token from database.
    
    Returns:
        Updated tokens dict
    """
    _validate_credentials()
    
    tokens = load_tokens(company_key, realm_id)
    if not tokens:
        raise RuntimeError(
            f"No tokens found for {company_key} (realm_id: {realm_id}). "
            "You need to run the OAuth flow first and store tokens."
        )
    
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError(
            f"No refresh_token found for {company_key} (realm_id: {realm_id}). "
            "You need to re-authenticate via OAuth flow."
        )
    
    # Basic auth header
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
        if resp.status_code == 401:
            if "invalid_client" in error_detail:
                raise RuntimeError(
                    f"Invalid CLIENT_ID or CLIENT_SECRET (401 invalid_client).\n"
                    f"Please check your .env file credentials."
                )
            else:
                raise RuntimeError(
                    f"Authentication failed (401). Check your CLIENT_ID and CLIENT_SECRET.\n"
                    f"Response: {error_detail}"
                )
        elif resp.status_code == 400 and "invalid_grant" in error_detail:
            raise RuntimeError(
                f"Refresh token is invalid or expired (400 invalid_grant).\n"
                f"You need to re-authenticate via OAuth flow for {company_key}."
            )
        else:
            raise RuntimeError(
                f"Failed to refresh access token: {resp.status_code} {error_detail}"
            )
    
    body = resp.json()
    new_access_token = body.get("access_token")
    new_refresh_token = body.get("refresh_token", refresh_token)  # Use new if provided, else keep old
    expires_in = body.get("expires_in", 3600)
    
    if not new_access_token:
        raise RuntimeError("Token refresh response missing access_token")
    
    expires_at = time.time() + int(expires_in)
    
    # Save updated tokens
    save_tokens(
        company_key=company_key,
        realm_id=realm_id,
        access_token=new_access_token,
        refresh_token=new_refresh_token,
        expires_at=expires_at,
        environment=tokens.get("environment", "production")
    )
    
    return {
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "expires_at": expires_at,
    }


def get_access_token(company_key: str, realm_id: str) -> str:
    """
    Get a valid access token for the specified company/realm.
    Automatically refreshes if expired.
    
    Args:
        company_key: Company identifier
        realm_id: QBO Realm ID
    
    Returns:
        Valid access token
    """
    tokens = load_tokens(company_key, realm_id)
    
    if not tokens:
        raise RuntimeError(
            f"No tokens found for {company_key} (realm_id: {realm_id}). "
            "You need to run the OAuth flow first and store tokens using store_tokens_from_oauth()."
        )
    
    if is_token_expired(tokens):
        tokens = refresh_access_token(company_key, realm_id)
    
    return tokens["access_token"]


def store_tokens_from_oauth(
    company_key: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    environment: str = "production"
) -> None:
    """
    Store tokens from OAuth flow into database.
    
    Args:
        company_key: Company identifier
        realm_id: QBO Realm ID
        access_token: Access token from OAuth
        refresh_token: Refresh token from OAuth
        expires_in: Expires in seconds
        environment: 'production' or 'sandbox'
    """
    expires_at = time.time() + expires_in
    save_tokens(
        company_key=company_key,
        realm_id=realm_id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
        environment=environment
    )


def verify_realm_match(company_key: str, expected_realm_id: str) -> None:
    """
    Verify that tokens in database match expected realm_id.
    Safety check to prevent cross-posting.
    
    Raises:
        RuntimeError: If realm_id mismatch detected
    """
    tokens = load_tokens(company_key, expected_realm_id)
    if tokens:
        # If we can load tokens for this realm_id, they match
        return
    
    # Check if there are tokens for this company_key but different realm_id
    _init_database()
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            cursor = conn.execute(
                "SELECT realm_id FROM qbo_tokens WHERE company_key = ?",
                (company_key,)
            )
            row = cursor.fetchone()
            if row and row[0] != expected_realm_id:
                raise RuntimeError(
                    f"REALM ID MISMATCH DETECTED!\n"
                    f"Company: {company_key}\n"
                    f"Expected realm_id: {expected_realm_id}\n"
                    f"Token database has realm_id: {row[0]}\n"
                    f"This is a safety check to prevent uploading to the wrong QBO company."
                )
        finally:
            conn.close()

