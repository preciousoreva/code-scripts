# OIAT EPOS → QuickBooks Automation Platform

A unified automation platform for syncing **Sales** and **Inventory** between EPOS Now and QuickBooks Online, with the **OIAT Portal** (Django) for operators, a **DB-backed scheduler**, and Docker-based deployment.

## What this repo does

- **Sales sync**
  - EPOS BookKeeping CSV download
  - Split by business/trading day (with RAW spill handling)
  - Transform to QBO-ready Sales Receipt data
  - Upload to QuickBooks Online (Sales Receipts)
  - Archive artifacts + reconcile EPOS totals vs QBO totals
  - Dashboard + Slack reporting
- **Inventory sync**
  - EPOS Stock Report download
  - QBO Inventory Item snapshot (includes diagnostic fields where QBO supports them)
  - Audit EPOS expected stock vs QBO `QtyOnHand`
  - Catalog and quantity review reports for pack variants, duplicate base items, and missing base items
  - Manual QBO starting-value correction previews; automated QBO quantity apply is disabled
  - Final audit + pipeline JSON/CSV summary (always includes `child_reports.final_audit`)
  - Dashboard artifact ingestion + compact Slack summary
  - Negative EPOS stock policy: **clamp negative row quantities to 0 before grouping**
- **OIAT Portal (Django)**
  - Runs UI (Sales + Inventory), scheduling, company management, tools, and status dashboards
- **Scheduler**
  - Runs in a separate service/process and enqueues `RunJob` rows from DB schedules
- **Docker deployment**
  - `caddy` (TLS reverse proxy) + `web` (Django/Gunicorn) + `scheduler` (DB schedule worker)
  - Persistent runtime state stored in a Docker volume (`app-data`) under `/data`

## Quick start

### Production / Docker (high level)

1. Create a `.env` (start from `.env.example`) and set required values:
   - QBO OAuth app creds
   - EPOS creds (per company)
   - Portal/Django settings
   - Caddy/Cloudflare DNS challenge settings
2. Build and start the stack:

```bash
docker compose build
docker compose up -d caddy web scheduler
```

3. Optional first-time state import (seed `/data` volume from repo checkout once):

```bash
docker compose run --rm --profile bootstrap bootstrap
docker compose up -d caddy web scheduler
```

For host migration notes, see [`docs/DOCKER_MIGRATION_READY.md`](docs/DOCKER_MIGRATION_READY.md).

### Local / dev (high level)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

Portal (local):

```bash
python manage.py migrate
python manage.py createsuperuser
python manage.py sync_companies_from_json
python manage.py runserver
```

Sandbox/dev runtime profiles (isolated state + sandbox QBO):

```bash
./build/init-dev-profile.sh marvin-dev
./build/run-sandbox.sh
```

See [`docs/DEV_STAGE_SETUP.md`](docs/DEV_STAGE_SETUP.md) for the full guardrails and profile layout.

## Operator workflows

### Sales

1. Download EPOS BookKeeping CSV
2. Split by trading day (RAW spill is stored and merged on the correct day)
3. Transform to Sales Receipt rows
4. Upload to QBO Sales Receipts (dedupe: local ledger + QBO checks)
5. Archive artifacts and reconcile EPOS vs QBO totals
6. Report via Portal + Slack

### Inventory

The operator-facing command is:

- `python -m code_scripts.inventory_pipeline`

The unified inventory pipeline does:

1. EPOS stock snapshot
2. Fresh QBO inventory item snapshot
3. Audit
4. Supported catalog cleanup (where safe/possible)
5. Re-audit
6. Exact-match inventory adjustments
7. Final reports + Slack summary

Detailed operator documentation: [`docs/INVENTORY_SYNC.md`](docs/INVENTORY_SYNC.md).

## Portal overview

The OIAT Portal is the main operator UI. Key pages:

- **Overview**: separates **Sales status**, **Inventory status**, and **Token health**
- **Runs**:
  - **Runs → Sales**: trigger sales runs (single or all companies)
  - **Runs → Inventory**: trigger the unified inventory pipeline with optional category/product filters
- **Schedules**: DB-backed cron schedules that enqueue `RunJob` records
  - Sales should run before Inventory. Weekly/bi-weekly Inventory schedules should fire after the selected day's Sales sync because Inventory is a live EPOS stock correction snapshot.
  - The default weekly Inventory schedule is disabled and targets all products for `company_a`; category/product filters can be used for narrowed schedules.
- **Companies**: manage company configs (DB is source of truth; sync to/from JSON supported)
- **Tools**: lower-level/debug tools (QBO query, verify mapping, etc.) — not the primary operator path

`RunJob` scopes reflect the current system:

- `single_company`
- `all_companies`
- `inventory_pipeline`
- `inventory_sync` (legacy; still supported by the model)

### Portal permissions

Assign via Django admin (`/admin/`):

- `can_trigger_runs`
- `can_edit_companies`
- `can_manage_schedules`
- `can_manage_portal_settings`

Lower-level inventory utilities (audit-only, catalog cleanup planning, snapshot inspection) still exist, but the main operator workflow is **Runs → Inventory** and the unified CLI `python -m code_scripts.inventory_pipeline`.

## CLI examples

### Sales

Single company (yesterday):

```bash
python run_pipeline.py --company company_a
```

Single company (specific date):

```bash
python run_pipeline.py --company company_a --target-date 2025-12-24
```

Range:

```bash
python run_pipeline.py --company company_b --from-date 2025-12-08 --to-date 2025-12-14
```

All companies:

```bash
python run_all_companies.py
```

### Inventory

Product example:

```bash
python -m code_scripts.inventory_pipeline \
  --company company_a \
  --auto-download \
  --auto-fetch-qbo \
  --qbo-force-refresh \
  --product "ACTION BITTERS50ml"
```

Category example:

```bash
python -m code_scripts.inventory_pipeline \
  --company company_a \
  --auto-download \
  --auto-fetch-qbo \
  --qbo-force-refresh \
  --category "ALCOHOLS & SPIRITS"
```

## Runtime state and artifacts

Runtime state is rooted under the configured **state root** (default `runtime/` locally). In Docker, `STATE_ROOT=/data` and the `app-data` volume is the persistent source of truth.

Common paths (relative to state root):

- **Sales archive**: `code_scripts/Uploaded/YYYY-MM-DD/`
- **Inventory audit CSVs**: `code_scripts/reports/inventory_sync/YYYY-MM-DD/`
- **Inventory catalog cleanup reports**: `code_scripts/reports/inventory_catalog_cleanup/YYYY-MM-DD/`
- **Inventory pipeline summaries**: `code_scripts/reports/inventory_pipeline/YYYY-MM-DD/`
- **EPOS exports**: `code_scripts/exports/stock_reports/`
- **QBO snapshots**: `code_scripts/exports/qbo_snapshots/`

Retention/lifecycle plan: [`docs/ARTIFACT_RETENTION_PLAN.md`](docs/ARTIFACT_RETENTION_PLAN.md).

## Configuration

### Environment (`.env`)

- QBO: `QBO_CLIENT_ID`, `QBO_CLIENT_SECRET`
- EPOS: credentials are referenced per-company via `epos.username_env_key` / `epos.password_env_key`
- Portal: `DJANGO_SECRET_KEY`, `DJANGO_ALLOWED_HOSTS`, `DJANGO_CSRF_TRUSTED_ORIGINS` (when behind TLS proxy)
- QuickBooks webhooks: `QBO_WEBHOOK_VERIFIER_TOKEN`; public ingress via Cloudflare Tunnel (`CLOUDFLARE_TUNNEL_TOKEN`, see [Webhook ingress](#webhook-ingress-cloudflare-tunnel)); register `https://qbo-webhooks.<your-domain>/epos-qbo/webhooks/quickbooks/` in Intuit; Slack destinations use `QBO_WEBHOOK_SLACK_URL_COMPANY_A/B` (`QBO_WEBHOOK_SLACK_URL_TEST` can override Intuit sample events)
- Slack: webhook URL env keys are referenced per-company, e.g. `SLACK_WEBHOOK_URL_A`
- Portal base URL/domain is used for run links in operator output (Docker uses `PORTAL_DOMAIN`)

### Company configs (DB + JSON)

Portal uses DB as runtime source-of-truth and supports sync:

```bash
python manage.py sync_companies_from_json
python manage.py sync_companies_to_json
python manage.py check_company_config_drift
```

### QBO tokens

Tokens are stored in a SQLite token DB under the state root (persisted in Docker volume). Expect refresh tokens to expire periodically and require re-auth.

## Deployment summary

Docker Compose services (see `docker-compose.yml`):

- `caddy`: HTTPS reverse proxy (Cloudflare DNS challenge) bound to the host Tailscale IP
- `web`: Django + Gunicorn (runs migrations on start)
- `scheduler`: runs `python manage.py run_schedule_worker` (DB schedules → `RunJob`)
- `cloudflared`: Cloudflare Tunnel exposing only the public webhook endpoint (see [Webhook ingress](#webhook-ingress-cloudflare-tunnel))

Networking model:

- Tailscale network gates access to the host
- The portal's Cloudflare DNS record is **DNS-only** (no Cloudflare proxy/WAF)
- The webhook hostname is the exception: it is Cloudflare-proxied and served via the `cloudflared` tunnel, so inbound webhooks from the public internet can reach an otherwise tailnet-only host

Update flow:

```bash
git pull
docker compose build
docker compose up -d caddy web scheduler cloudflared
```

### Webhook ingress (Cloudflare Tunnel)

The portal is reachable **only over Tailscale** (Caddy binds to the host's Tailscale IP in the `100.64.0.0/10` CGNAT range). That keeps the dashboard private — but QuickBooks Online webhooks are sent from Intuit's public servers, which cannot reach a Tailscale address. The `cloudflared` service gives the **webhook endpoint only** a public entry point.

- **Service:** `cloudflared` — a Cloudflare Tunnel connector running in the compose network.
- **What for:** exposes a single public hostname (e.g. `qbo-webhooks.<your-domain>`) that forwards just the webhook path to Django.
- **Why a tunnel:** the connector makes an outbound-only connection to Cloudflare — no inbound firewall ports and no public IP on the host, and TLS terminates at Cloudflare's edge. The rest of the portal stays Tailscale-only.

Request path: `Intuit → Cloudflare edge → cloudflared → web:8000`.

Setup (one-time):

1. In the Cloudflare Zero Trust dashboard (Networks → Tunnels), create a tunnel and copy its **token**.
2. Add a **Public Hostname** to the tunnel:
   - Hostname: `qbo-webhooks.<your-domain>`
   - Path: `epos-qbo/webhooks/quickbooks` (restricts public access to the webhook only; everything else returns 404)
   - Service: `HTTP` → `web:8000`

   Cloudflare creates the proxied DNS record automatically.
3. In `.env`, set `CLOUDFLARE_TUNNEL_TOKEN` and add the webhook hostname to `DJANGO_ALLOWED_HOSTS`.
4. `docker compose up -d cloudflared` and recreate `web` so it picks up the new allowed host.
5. Register the public URL in the Intuit app: `https://qbo-webhooks.<your-domain>/epos-qbo/webhooks/quickbooks/`.

> On a Windows / Docker Desktop host, pull images from the interactive console before running `docker compose up` over SSH — the credential helper cannot run in a non-interactive session.

## Documentation

- [`docs/INVENTORY_SYNC.md`](docs/INVENTORY_SYNC.md)
- [`docs/ARTIFACT_RETENTION_PLAN.md`](docs/ARTIFACT_RETENTION_PLAN.md)
- [`docs/PORTAL_IMPROVEMENTS_AND_TRACKING.md`](docs/PORTAL_IMPROVEMENTS_AND_TRACKING.md)
- [`docs/DOCKER_MIGRATION_READY.md`](docs/DOCKER_MIGRATION_READY.md)
- [`docs/DEV_STAGE_SETUP.md`](docs/DEV_STAGE_SETUP.md)

## Safety / operations

- A **global run lock** prevents concurrent runs (avoid overlapping Sales/Inventory runs).
- Avoid editing QBO inventory quantities while an inventory run is active.
- Token refresh is expected to fail occasionally when refresh tokens expire; re-auth and store new tokens.
- For local dev, prefer sandbox profiles (see [`docs/DEV_STAGE_SETUP.md`](docs/DEV_STAGE_SETUP.md)) to avoid mixing state.
