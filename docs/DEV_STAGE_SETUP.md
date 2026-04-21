# Developer Sandbox / Staging Setup

This guide explains how to run the OIAT Portal and pipeline locally against a
QuickBooks Online **sandbox** company, isolated from the production deployment
on OIAT-SRV-01.

Production keeps running unchanged: none of the variables below are set on the
production host, so defaults preserve prior behaviour.

## What "sandbox" means here

A developer sandbox profile is a locally isolated run of the same code that runs
in production, pointed at:

- a QuickBooks Online **sandbox** realm (not the production realm)
- an **Intuit Development** app (separate `QBO_CLIENT_ID` / `QBO_CLIENT_SECRET`)
- a **local-disk** state tree (Django DB, QBO tokens, `Uploaded/` archive,
  logs, reports) under `~/.oiat/state/<profile>/`

No production artifacts are read or written. The same code paths are exercised
end-to-end, so bugs caught locally are representative of production.

## Isolation levers

Four environment variables drive isolation. Production leaves all four unset.

| Variable | Purpose | Sandbox value | Production |
|---|---|---|---|
| `OIAT_ENV_FILE` | Path to the per-profile env file | `.oiat/env/<profile>.env` | unset (uses `.env`) |
| `STATE_ROOT` | Root of runtime state (DB, tokens, Uploaded, logs) | `~/.oiat/state/<profile>` | unset (defaults to `runtime/`) |
| `OIAT_COMPANIES_DIR` | Active company JSON directory | `~/.oiat/state/<profile>/code_scripts/companies` | unset (defaults under `STATE_ROOT`) |
| `OIAT_RUNTIME_ENV` | `production` (default) or `sandbox` | `sandbox` | unset |

`~` is expanded by [code_scripts/paths.py](../code_scripts/paths.py) via
`Path.expanduser()`, so tilde-prefixed paths in env files work as expected.

**Why `STATE_ROOT` must be on local disk:** SQLite file locking does not work
reliably over mounted network drives (SMB/NFS), and `/tmp` is cleared on
reboot — you will lose sandbox QBO tokens, Django DB, and run history. Keep
`STATE_ROOT` under `~/.oiat/state/...` on a local-disk path, even when the repo
itself lives on a mount.

## Guards

Three guards in the pipeline refuse to run if the runtime and the company
config disagree:

- `ensure_company_runtime_compatible()` ([code_scripts/company_config.py](../code_scripts/company_config.py))
  refuses to run a company JSON whose `qbo.environment` does not match
  `OIAT_RUNTIME_ENV`. Prevents `OIAT_RUNTIME_ENV=sandbox` accidentally running
  the production `company_a.json`, and vice versa.
- `verify_tokens()` ([code_scripts/token_manager.py](../code_scripts/token_manager.py))
  checks both the runtime environment and the Intuit-client fingerprint
  (`QBO_CLIENT_ID` + `QBO_CLIENT_SECRET` hash) before reusing stored OAuth
  tokens — prod tokens will not be reused in sandbox even if the token file
  were shared.
- `get_qbo_api_base_url()` routes sandbox traffic to
  `https://sandbox-quickbooks.api.intuit.com` so a misconfigured run cannot
  hit the production Intuit endpoint.

## One-time bootstrap

```bash
# 1) Create the profile scaffolding (state dirs + .oiat/env/<profile>.env)
./build/init-dev-profile.sh marvin-dev

# 2) Build the Python 3.11 venv (must match production Python for Django/pandas
#    compatibility — Python 3.14 breaks Django template rendering).
uv python install 3.11
uv venv --python 3.11 ~/.oiat/venv/marvin-dev
~/.oiat/venv/marvin-dev/bin/python -m ensurepip --upgrade
~/.oiat/venv/marvin-dev/bin/python -m pip install -r requirements.txt
~/.oiat/venv/marvin-dev/bin/python -m playwright install chromium
```

After step 1, edit:

- `.oiat/env/marvin-dev.env` — fill in `QBO_CLIENT_ID`, `QBO_CLIENT_SECRET`
  (from [developer.intuit.com](https://developer.intuit.com) **Development**
  keys — **not** production), `DJANGO_SECRET_KEY`, and EPOS credentials.
- `~/.oiat/state/marvin-dev/code_scripts/companies/company_sandbox.json` —
  fill in `realm_id`, `tax_code_id`, `tax_rate_id`, `default_item_id`,
  `default_income_account_id` from your QBO sandbox company.

## Daily use

```bash
# Quick start (defaults OIAT_ENV_FILE to .oiat/env/marvin-dev.env)
./build/run-sandbox.sh

# Or with an explicit profile
OIAT_ENV_FILE=.oiat/env/acme-dev.env ./build/run-local.sh
```

`run-local.sh` loads the env file, activates `OIAT_VENV_PATH`, creates
`STATE_ROOT` and `OIAT_COMPANIES_DIR` if missing, applies Django migrations,
and starts `manage.py runserver` on `$WEB_BIND_HOST:$WEB_BIND_PORT`.

### Running the pipeline against the sandbox

```bash
OIAT_ENV_FILE=.oiat/env/marvin-dev.env \
  ~/.oiat/venv/marvin-dev/bin/python run_pipeline.py \
  --company company_sandbox --target-date 2026-04-17
```

## EPOS credentials

EPOS Now does not provide a sandbox tenant. Two practical options:

- **Reuse a real EPOS login** — downloads are read-only, so production EPOS
  data is safe. The transformed output is uploaded to the QBO **sandbox**
  realm only (never production), because `OIAT_RUNTIME_ENV=sandbox` forces the
  sandbox API base URL.
- **Point at a throwaway EPOS account** — fully fake data, but the sandbox
  will not mirror real sales.

## Docker dev stack (optional)

`docker-compose.dev.yml` overrides the production compose file to expose the
web container on localhost and skip the Caddy/Tailscale TLS terminator:

```bash
OIAT_ENV_FILE=.oiat/env/marvin-dev.env \
  docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```

The scheduler service runs normally in this mode.

## Mirroring company_b locally

If a second sandbox realm is available and you want a sandbox analog of
`company_b`, copy `company_sandbox.example.json` to
`~/.oiat/state/<profile>/code_scripts/companies/company_b_sandbox.json`,
change `company_key`, `display_name`, `realm_id`, and set
`aggregate_products: false` (to match company_b's current config). Both
companies can coexist under one profile.

## Merging dev-stage-setup into master

The `dev-stage-setup` branch introduces the isolation levers and guards. For
production to keep working after merge:

1. **Do not** set `OIAT_ENV_FILE`, `STATE_ROOT`, `OIAT_COMPANIES_DIR`, or
   `OIAT_RUNTIME_ENV` on OIAT-SRV-01. Defaults preserve prior behaviour.
2. Production `company_a.json` and `company_b.json` do not require
   `qbo.environment` — when unset, it defaults to `production`, which matches
   the default `OIAT_RUNTIME_ENV=production`.
3. The `docker-compose.yml` changes are compatible with production as long as
   `TAILSCALE_BIND_IP`, `PORTAL_DOMAIN`, and `CF_API_TOKEN` stay set on the
   host (they already are).
4. The `company_sandbox.example.json` and `.env.development.example` files are
   templates only — they are not loaded by default.

## Troubleshooting

- **`RuntimeError: QBO environment mismatch`** — the runtime env
  (`OIAT_RUNTIME_ENV`) does not match the company JSON's `qbo.environment`.
  Either switch the env file or edit the company JSON.
- **Django template `super().dicts` AttributeError in tests** — you are on
  Python ≥3.14. Rebuild the venv on Python 3.11 (`uv venv --python 3.11 ...`).
- **SQLite `database is locked` errors** — `STATE_ROOT` is on a mounted
  network drive. Move it to a local-disk path (`~/.oiat/state/...`).
- **Lost QBO tokens after reboot** — `STATE_ROOT` was under `/tmp`. Use
  `~/.oiat/state/<profile>` instead.
