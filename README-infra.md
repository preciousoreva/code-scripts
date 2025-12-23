# Windows Local Environment Setup (Venv + Token Broker)

This document describes the local Windows setup used for the AKPONORA automation pipeline.
It exists mainly as a reference for future maintenance and re-setup.

## Why this exists

- Windows is the **single authority** for QuickBooks OAuth tokens
- Tokens are refreshed automatically and must stay consistent
- The Mac fetches tokens securely from Windows via SSH (no SMB, no cloud)
- A local Python virtual environment keeps dependencies isolated

---

## Python Virtual Environment (.venv)

### Why use a venv
- Isolates Python dependencies from system Python
- Prevents version conflicts
- Reproducible setup

### Create the venv
```powershell
cd code-scripts
python -m venv .venv

## Activate the venv (POwershell)
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\.venv\Scripts\Activate.ps1

    # Expected Prompt
    (.venv) PS C:\...

## Install dependencies
pip install fastapi uvicorn requests

    # Verify
    python -c "import fastapi, uvicorn, requests; print('deps OK')"

Token Broker (Windows-only)

Purpose
	•	Acts as a local token authority
	•	Reads and refreshes qbo_tokens.json
	•	Exposes a localhost-only HTTP endpoint
	•	Prevents token drift between machines

Location
	•	code-scripts/token_broker.py
	•	Runs on 127.0.0.1:8765 only

Required environment variables
	•	QBO_TOKEN_BROKER_KEY (shared secret)
	•	QBO_CLIENT_ID / QBO_CLIENT_SECRET (from .env)
	•	qbo_tokens.json must exist locally

# Start the broker (manual)
uvicorn token_broker:APP --host 127.0.0.1 --port 8765

# Health check
Invoke-RestMethod http://127.0.0.1:8765/health

Security Notes
	•	Broker binds to localhost only (not LAN, not public)
	•	Access requires x-broker-key header
	•	Mac accesses broker via SSH tunnel
	•	Tokens are never committed to Git

⸻

Git Ignore Rules

The following files/folders are intentionally ignored:
	•	.venv/
	•	myenv/
	•	.env
	•	qbo_tokens.json