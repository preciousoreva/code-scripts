import os
import time
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from qbo_auth import load_tokens, is_token_expired, refresh_access_token

APP = FastAPI()

BROKER_KEY = os.environ.get("QBO_TOKEN_BROKER_KEY")
if not BROKER_KEY:
    raise RuntimeError("QBO_TOKEN_BROKER_KEY environment variable is required.")

class TokenResp(BaseModel):
    access_token: str
    expires_at: float

@APP.get("/health")
def health():
    return {"ok": True, "ts": time.time()}

@APP.get("/token", response_model=TokenResp)
def token(x_broker_key: str = Header(default="")):
    if x_broker_key != BROKER_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

    tokens = load_tokens()
    if not tokens:
        raise HTTPException(status_code=500, detail="qbo_tokens.json missing/empty")

    if is_token_expired(tokens):
        tokens = refresh_access_token(tokens)  # refresh_access_token also saves

    return TokenResp(
        access_token=tokens["access_token"],
        expires_at=float(tokens["expires_at"]),
    )

    