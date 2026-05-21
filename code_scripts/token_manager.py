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
import hashlib
from typing import Any, Dict, List, Optional, Tuple
import threading

import requests

from code_scripts.load_env import load_env_file
from code_scripts.paths import OPS_ROOT
from code_scripts.company_config import (
    get_runtime_qbo_environment,
    load_company_config,
    normalize_qbo_environment,
)

# Load .env for shared credentials
load_env_file()

# QBO OAuth token endpoint
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# SQLite database file
DB_FILE = OPS_ROOT / "qbo_tokens.sqlite"

# Thread lock for database operations
_db_lock = threading.Lock()

# One-time init per process (avoids DDL + chmod on every load_tokens call)
_db_initialized = False
_db_init_lock = threading.Lock()


def ensure_db_initialized() -> None:
    """Ensure SQLite token DB exists and is ready. Safe to call repeatedly; runs init at most once per process."""
    global _db_initialized
    if _db_initialized:
        return
    # Avoid racing init across threads in Django / CLI contexts.
    with _db_init_lock:
        if _db_initialized:
            return
        _init_database()
        _db_initialized = True


def _validate_credentials() -> None:
    """Validate that required credentials are set."""
    if not _current_client_id():
        raise RuntimeError(
            "QBO_CLIENT_ID environment variable is not set. "
            "Please set it in your .env file."
        )
    if not _current_client_secret():
        raise RuntimeError(
            "QBO_CLIENT_SECRET environment variable is not set. "
            "Please set it in your .env file."
        )


def _current_client_id() -> str | None:
    return os.environ.get("QBO_CLIENT_ID")


def _current_client_secret() -> str | None:
    return os.environ.get("QBO_CLIENT_SECRET")


def _current_client_fingerprint() -> str | None:
    client_id = _current_client_id()
    if not client_id:
        return None
    return hashlib.sha256(client_id.encode("utf-8")).hexdigest()[:16]


def _expected_company_environment(company_key: str) -> str:
    try:
        return load_company_config(company_key).qbo_environment
    except Exception:
        return get_runtime_qbo_environment()


def _assert_token_compatibility(company_key: str, realm_id: str, tokens: Optional[Dict[str, Any]]) -> None:
    if not tokens:
        return

    runtime_environment = get_runtime_qbo_environment()
    company_environment = _expected_company_environment(company_key)
    token_environment = normalize_qbo_environment(tokens.get("environment"), default=runtime_environment)
    if token_environment != runtime_environment or company_environment != runtime_environment:
        raise RuntimeError(
            "QBO environment mismatch.\n"
            f"Runtime environment: {runtime_environment}\n"
            f"Company config environment: {company_environment}\n"
            f"Stored token environment: {token_environment}\n"
            f"Company: {company_key}\n"
            f"Realm ID: {realm_id}\n"
            "Refusing to use tokens from a different QBO environment."
        )

    current_fingerprint = _current_client_fingerprint()
    stored_fingerprint = tokens.get("client_fingerprint")
    if current_fingerprint and stored_fingerprint and current_fingerprint != stored_fingerprint:
        raise RuntimeError(
            "Stored QBO tokens were created with a different Intuit client ID.\n"
            f"Company: {company_key}\n"
            f"Realm ID: {realm_id}\n"
            "Re-run the OAuth flow or store fresh tokens for this environment."
        )


def _init_database() -> None:
    """Initialize SQLite database with qbo_tokens table if it doesn't exist."""
    with _db_lock:
        DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_FILE)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS qbo_tokens (
                    company_key TEXT NOT NULL,
                    realm_id TEXT NOT NULL,
                    access_token TEXT,
                    refresh_token TEXT NOT NULL,
                    access_expires_at INTEGER,
                    refresh_expires_at INTEGER,
                    updated_at INTEGER NOT NULL,
                    environment TEXT DEFAULT 'production',
                    client_fingerprint TEXT,
                    PRIMARY KEY (company_key, realm_id)
                )
                """
            )
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(qbo_tokens)").fetchall()
            }
            if "refresh_expires_at" not in columns:
                conn.execute("ALTER TABLE qbo_tokens ADD COLUMN refresh_expires_at INTEGER")
            if "client_fingerprint" not in columns:
                conn.execute("ALTER TABLE qbo_tokens ADD COLUMN client_fingerprint TEXT")

            # If an older DB was created with `realm_id UNIQUE`, SQLite doesn't support dropping
            # constraints in-place. Rebuild the table once to remove that uniqueness and rely on
            # the composite PK (company_key, realm_id) instead.
            create_sql_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='qbo_tokens'"
            ).fetchone()
            create_sql = (create_sql_row[0] or "") if create_sql_row else ""
            if "realm_id" in create_sql and "UNIQUE" in create_sql and "realm_id TEXT NOT NULL UNIQUE" in create_sql:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS qbo_tokens_v2 (
                        company_key TEXT NOT NULL,
                        realm_id TEXT NOT NULL,
                        access_token TEXT,
                        refresh_token TEXT NOT NULL,
                        access_expires_at INTEGER,
                        refresh_expires_at INTEGER,
                        updated_at INTEGER NOT NULL,
                        environment TEXT DEFAULT 'production',
                        client_fingerprint TEXT,
                        PRIMARY KEY (company_key, realm_id)
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO qbo_tokens_v2
                    (company_key, realm_id, access_token, refresh_token, access_expires_at, refresh_expires_at, updated_at, environment, client_fingerprint)
                    SELECT company_key, realm_id, access_token, refresh_token, access_expires_at, refresh_expires_at, updated_at, environment, client_fingerprint
                    FROM qbo_tokens
                    """
                )
                conn.execute("DROP TABLE qbo_tokens")
                conn.execute("ALTER TABLE qbo_tokens_v2 RENAME TO qbo_tokens")
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
    ensure_db_initialized()

    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            cursor = conn.execute(
                "SELECT access_token, refresh_token, access_expires_at, refresh_expires_at, "
                "updated_at, environment, client_fingerprint "
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
                "refresh_expires_at": row[3],
                "updated_at": row[4],
                "environment": row[5] or "production",
                "client_fingerprint": row[6],
            }
        finally:
            conn.close()


def load_tokens_batch(
    pairs: List[Tuple[str, str]],
) -> Dict[Tuple[str, str], Optional[Dict[str, Any]]]:
    """
    Load tokens for multiple (company_key, realm_id) pairs in one connection.
    Call ensure_db_initialized() once before if this is the first token access in the process.
    Returns dict keyed by (company_key, realm_id) -> token dict or None if not found.
    """
    if not pairs:
        return {}
    ensure_db_initialized()
    result: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            for company_key, realm_id in pairs:
                cursor = conn.execute(
                    "SELECT access_token, refresh_token, access_expires_at, refresh_expires_at, "
                    "updated_at, environment, client_fingerprint "
                    "FROM qbo_tokens WHERE company_key = ? AND realm_id = ?",
                    (company_key, realm_id),
                )
                row = cursor.fetchone()
                if not row:
                    result[(company_key, realm_id)] = None
                else:
                    result[(company_key, realm_id)] = {
                        "access_token": row[0],
                        "refresh_token": row[1],
                        "expires_at": row[2],
                        "refresh_expires_at": row[3],
                        "updated_at": row[4],
                        "environment": row[5] or "production",
                        "client_fingerprint": row[6],
                    }
        finally:
            conn.close()
    return result


def save_tokens(
    company_key: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expires_at: float,
    refresh_expires_at: float | None = None,
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
        refresh_expires_at: Unix timestamp when refresh token expires
        environment: 'production' or 'sandbox'
    """
    ensure_db_initialized()
    _validate_credentials()

    updated_at = int(time.time())
    client_fingerprint = _current_client_fingerprint()

    with _db_lock:
        conn = sqlite3.connect(DB_FILE)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO qbo_tokens 
                (
                    company_key,
                    realm_id,
                    access_token,
                    refresh_token,
                    access_expires_at,
                    refresh_expires_at,
                    updated_at,
                    environment,
                    client_fingerprint
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                company_key,
                realm_id,
                access_token,
                refresh_token,
                int(expires_at),
                int(refresh_expires_at) if refresh_expires_at else None,
                updated_at,
                environment,
                client_fingerprint,
            ))
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
    _assert_token_compatibility(company_key, realm_id, tokens)
    if not refresh_token:
        raise RuntimeError(
            f"No refresh_token found for {company_key} (realm_id: {realm_id}). "
            "You need to re-authenticate via OAuth flow."
        )
    
    # Basic auth header
    auth_str = f"{_current_client_id()}:{_current_client_secret()}".encode("utf-8")
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
    refresh_expires_in = body.get("x_refresh_token_expires_in")
    
    if not new_access_token:
        raise RuntimeError("Token refresh response missing access_token")
    
    expires_at = time.time() + int(expires_in)
    previous_refresh_expires_at = tokens.get("refresh_expires_at")
    refresh_expires_at = (
        time.time() + int(refresh_expires_in)
        if refresh_expires_in is not None
        else previous_refresh_expires_at
    )
    
    # Save updated tokens
    save_tokens(
        company_key=company_key,
        realm_id=realm_id,
        access_token=new_access_token,
        refresh_token=new_refresh_token,
        expires_at=expires_at,
        refresh_expires_at=refresh_expires_at,
        environment=tokens.get("environment", "production")
    )
    
    return {
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "expires_at": expires_at,
        "refresh_expires_at": refresh_expires_at,
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
    _assert_token_compatibility(company_key, realm_id, tokens)
    
    if is_token_expired(tokens):
        tokens = refresh_access_token(company_key, realm_id)
    
    return tokens["access_token"]


def store_tokens_from_oauth(
    company_key: str,
    realm_id: str,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    refresh_expires_in: int | None = None,
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
        refresh_expires_in: Refresh token expiration in seconds
        environment: 'production' or 'sandbox'
    """
    normalized_environment = normalize_qbo_environment(environment, default="production")
    runtime_environment = get_runtime_qbo_environment()
    company_environment = _expected_company_environment(company_key)
    if normalized_environment != runtime_environment or company_environment != runtime_environment:
        raise RuntimeError(
            "Refusing to store QBO tokens for the wrong environment.\n"
            f"Requested token environment: {normalized_environment}\n"
            f"Runtime environment: {runtime_environment}\n"
            f"Company config environment: {company_environment}\n"
            f"Company: {company_key}"
        )

    expires_at = time.time() + expires_in
    refresh_expires_at = (
        time.time() + refresh_expires_in
        if refresh_expires_in is not None
        else None
    )
    save_tokens(
        company_key=company_key,
        realm_id=realm_id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
        refresh_expires_at=refresh_expires_at,
        environment=normalized_environment
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
        _assert_token_compatibility(company_key, expected_realm_id, tokens)
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
