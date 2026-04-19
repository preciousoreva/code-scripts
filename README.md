# EPOS → QuickBooks Automation

This repo contains an automation pipeline that:

1. Logs into **EPOS Now HQ** and downloads the daily **BookKeeping** CSV.
2. Splits the raw CSV by date (WAT timezone) and handles **RAW spill** for future dates.
3. Transforms each day's raw data into QuickBooks-ready CSV format.
4. Uploads the data into **QuickBooks Online** as Sales Receipts using the QBO API.
5. Archives all processed files to `Uploaded/<date>/` after successful upload.
6. Reconciles EPOS totals vs QBO totals to verify data integrity.

The pipeline is designed to be run as a single command and take care of all phases in sequence.

---

## TL;DR – Quick Start

1. **Set up credentials:**

   ```bash
   cp .env.example .env
   # Edit .env and fill in your QBO_CLIENT_ID, QBO_CLIENT_SECRET, EPOS_USERNAME, EPOS_PASSWORD
   ```

2. **Create initial OAuth tokens:**

   - Perform OAuth flow to get access/refresh tokens
   - Store tokens in `qbo_tokens.sqlite` using `store_tokens_from_oauth()` (see [Initial Setup](#2-get-initial-oauth-tokens) for details)

3. **Install dependencies:**

   ```bash
   # Create virtual environment (recommended)
   python -m venv .venv

   # Activate virtual environment
   # On Windows (PowerShell):
   .\.venv\Scripts\Activate.ps1
   # On macOS/Linux:
   source .venv/bin/activate

   # Install dependencies
   pip install -r requirements.txt

   # Install Playwright browser (required after installing playwright package)
   playwright install chromium
   ```

4. **Run the pipeline:**

   **Standard (yesterday's data):**

   ```bash
   python run_pipeline.py --company company_a
   ```

   **Specific date:**

   ```bash
   python run_pipeline.py --company company_a --target-date 2025-12-24
   ```

   **Custom date range:**

   ```bash
   python run_pipeline.py --company company_b --from-date 2025-12-08 --to-date 2025-12-14
   ```

   **Skip download (use existing split files):**

   ```bash
   python run_pipeline.py --company company_b --from-date 2025-01-29 --to-date 2025-01-31 --skip-download
   ```

   **Single-company canary (faster inventory sync path):**

   ```bash
   python run_pipeline.py --company company_a --inventory-sync-mode upload_fast
   ```

   > **Note:** `--skip-download` only works in range mode and uses existing split files from `uploads/range_raw/`. Useful when you already have CSV files and want to reprocess without re-downloading from EPOS.

That's it! The pipeline will download, split, transform, upload, archive, and reconcile automatically. If `SLACK_WEBHOOK_URL` is configured, you'll receive notifications for pipeline start, success, failure events, and reconciliation results.

> 💡 **Tip:** See [Initial Setup](#initial-setup) below for detailed instructions on each step.
>
> **Note:** All examples use `python` for cross-platform compatibility. On macOS/Linux, use `python3` if `python` points to Python 2 or is missing.
>
> **Docker deployment:** For the `docker-build` branch, see [Docker Deployment](#docker-deployment-docker-build-branch) before deploying on a server.

### Developer Sandbox Profiles

To develop against a QuickBooks sandbox without touching production state, bootstrap a local profile:

```bash
./build/init-dev-profile.sh marvin-dev   # creates .oiat/env/marvin-dev.env and ~/.oiat/state/marvin-dev
./build/run-sandbox.sh                   # starts Django with the sandbox env loaded
```

This keeps the sandbox Django DB, QBO token DB, company JSON, and artifacts under `~/.oiat/state/<profile>/` on your local disk — separate from production. See [docs/DEV_STAGE_SETUP.md](docs/DEV_STAGE_SETUP.md) for the full flow, guardrails, and what ships to `master` vs what stays local.

---

## First-time setup (all in one)

Use this sequence once per machine (or per new clone/venv) so both the pipeline and the OIAT Portal work, including **triggering runs from the dashboard**:

1. **Create and activate a virtual environment** (recommended)
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # macOS/Linux
   # .\.venv\Scripts\Activate.ps1   # Windows
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Playwright browser** (required for EPOS download; also needed if you trigger runs from the dashboard)
   ```bash
   playwright install chromium
   ```

4. **On Linux/WSL: install Playwright system dependencies** (avoids “libnspr4.so not found” and similar errors when Chromium starts)
   ```bash
   sudo playwright install-deps
   ```

5. **If you use the OIAT Portal:** run [Portal Setup](#portal-setup) (migrate, createsuperuser, sync companies, runserver).

---

## Docker Deployment (`docker-build` branch)

The `docker-build` branch packages the portal into Docker services with a Caddy reverse proxy in front:

- `caddy` — terminates HTTPS on the Tailscale IP and forwards requests to Django
- `web` — runs Django migrations, then serves the portal with Gunicorn
- `scheduler` — runs `python manage.py run_schedule_worker`

For moving this setup to a new host, use [docs/DOCKER_MIGRATION_READY.md](docs/DOCKER_MIGRATION_READY.md).

`web` is not exposed directly on a host port. Caddy is the only entrypoint, and it should be bound only to the Tailscale IP.

The stack uses these volumes:

- `app-data` — Django DB, QBO token DB, logs, uploads, reports, outputs
- `caddy_data` / `caddy_config` — Caddy ACME state, issued certificates, and runtime config

### What you need on the server

1. Docker and Docker Compose installed
2. The repo present on disk
3. A `.env` file in the repo root
4. If you want to preserve existing state, copy your current repo contents first so the one-time bootstrap step can seed the Docker volume
5. `oiatsolutions.com` managed in Cloudflare DNS
6. A Cloudflare API token for DNS challenge issuance
7. A public DNS record for `portal.oiatsolutions.com` pointing at the server's Tailscale IP

### Required `.env` values

At minimum, set these in `.env`:

```env
QBO_CLIENT_ID=your_client_id_here
QBO_CLIENT_SECRET=your_client_secret_here
EPOS_USERNAME_A=...
EPOS_PASSWORD_A=...
EPOS_USERNAME_B=...
EPOS_PASSWORD_B=...

DJANGO_SECRET_KEY='replace-with-a-long-random-secret'
DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1,your-server-ip,portal.oiatsolutions.com
DJANGO_CSRF_TRUSTED_ORIGINS=https://portal.oiatsolutions.com
DJANGO_USE_X_FORWARDED_PROTO=1
DJANGO_USE_X_FORWARDED_HOST=1
DJANGO_CSRF_COOKIE_SECURE=1
DJANGO_SESSION_COOKIE_SECURE=1

PORTAL_DOMAIN=portal.oiatsolutions.com
TAILSCALE_BIND_IP=100.125.133.118
CF_API_TOKEN=your_cloudflare_dns_api_token
```

`DJANGO_SECRET_KEY`
- Django's signing/encryption secret
- Must be long, random, and stable for that environment
- Do not commit it to Git
- If the secret contains `$`, wrap it in single quotes in `.env` so Docker Compose treats it literally

`DJANGO_ALLOWED_HOSTS`
- Comma-separated list of hostnames or IPs the app is allowed to serve
- Include the exact server IP and/or domain you will browse to

`PORTAL_DOMAIN`
- The hostname Caddy serves, for example `portal.oiatsolutions.com`
- This should match the HTTPS origin users will visit

`TAILSCALE_BIND_IP`
- The Tailscale IP on the host where Caddy should listen on `443`
- This keeps the portal bound to the tailnet-facing interface instead of all interfaces

`CF_API_TOKEN`
- Cloudflare API token used by Caddy for ACME DNS challenge issuance
- Scope it to the portal zone with:
  - `Zone.Zone:Read`
  - `Zone.DNS:Edit`
- Restrict it to the specific zone you are serving, for example `oiatsolutions.com`

### Cloudflare DNS setup

Before the first Caddy start, create these in Cloudflare:

1. A DNS record for `portal.oiatsolutions.com`
   - Type: `A`
   - Name: `portal`
   - Content: the server's Tailscale IP, for example `100.125.133.118`
   - Proxy status: `DNS only`
2. An API token scoped to that zone with:
   - `Zone.Zone:Read`
   - `Zone.DNS:Edit`

Important:

- The portal record must be `DNS only`, not proxied through Cloudflare
- Cloudflare's reverse proxy cannot reach a tailnet-only origin on a Tailscale IP
- The public DNS record is acceptable here because the Tailscale IP is only reachable by approved tailnet clients

### Tailscale access model

Use Tailscale as the network gate:

1. Publish `portal.oiatsolutions.com` in public DNS so browsers can resolve it normally
2. Restrict actual connectivity with a Tailscale grant/ACL so only approved users can reach the server on `tcp:443`
3. Keep Django login enabled so approved network users still need valid portal credentials

That gives you:

- browser-trusted HTTPS without per-device certificate import
- tailnet-only reachability
- Django auth as a second access-control layer

For the Caddy/Tailscale setup, also set:

```env
DJANGO_CSRF_TRUSTED_ORIGINS=https://portal.oiatsolutions.com
DJANGO_USE_X_FORWARDED_PROTO=1
DJANGO_USE_X_FORWARDED_HOST=1
DJANGO_CSRF_COOKIE_SECURE=1
DJANGO_SESSION_COOKIE_SECURE=1
```

### First deployment

From the repo root on the server:

```bash
git fetch origin
git switch docker-build
git pull origin docker-build
docker compose build
docker compose up -d caddy web scheduler
docker compose logs -f caddy web scheduler
```

If this is a migration from an existing non-Docker install and you want to import the current DB, token store, logs, uploads, and reports first, run the bootstrap service once before starting the long-lived services:

```bash
docker compose run --rm --profile bootstrap bootstrap
docker compose up -d caddy web scheduler
```

### What happens on first start

Docker creates a named volume called `app-data` and stores persistent runtime state there.

When you run the bootstrap service, the entrypoint seeds the Docker volume from the copied repo directory if those source files or folders exist and the volume is still empty:

- `db.sqlite3`
- `code_scripts/qbo_tokens.sqlite`
- `code_scripts/Uploaded/`
- `code_scripts/uploads/`
- `code_scripts/logs/`
- `code_scripts/reports/`
- `code_scripts/outputs/`

After that, the Docker volume becomes the source of truth. Future container rebuilds and restarts keep using the volume state.

### Important deployment note

The running `web` and `scheduler` services no longer mount the checked-out repo into `/seed`. That is intentional. It reduces exposure of `.env`, `.git`, and other checked-out files if an app container is ever compromised.

If you copy the repo to the server **after** the Docker volume has already been created and populated, those copied SQLite files will not automatically overwrite the existing volume. Bootstrap seeding only works when the volume is empty.

### Updating the deployment

For later updates:

```bash
git pull origin docker-build
docker compose build
docker compose up -d caddy web scheduler
```

Do not rerun the bootstrap service on ordinary code updates. It is for first-time state import only.

### Smoke tests after deployment

Use these checks after the containers come up to confirm the deployment is actually usable, not just running:

```bash
# 1. Confirm all containers are up
docker compose ps

# 2. Tail recent logs
docker compose logs --tail=100 caddy web scheduler

# 3. Confirm the login page responds through Caddy
curl -I https://portal.oiatsolutions.com/login/

# 4. Confirm Django users exist in the seeded database
docker compose exec web python manage.py shell -c "from django.contrib.auth import get_user_model; print(list(get_user_model().objects.values_list('username', flat=True)))"

# 5. Confirm companies exist in the DB
docker compose exec web python manage.py shell -c "from apps.epos_qbo.models import CompanyConfigRecord; print(list(CompanyConfigRecord.objects.values_list('company_key', flat=True)))"

# 6. Confirm QBO tokens are present
docker compose exec web python store_tokens.py --list

# 7. Confirm QBO connectivity with a safe read-only query
docker compose exec web python code_scripts/scripts/qbo_queries/qbo_query.py --company company_a query "select Id, Name from Item maxresults 1"
```

Expected results:

- `docker compose ps` shows `caddy`, `web`, and `scheduler` as `Up`
- `/login/` responds successfully through Caddy
- Django users and company keys are present
- `store_tokens.py --list` shows stored tokens
- the QBO query returns JSON instead of an auth or transport error

### Troubleshooting

**Docker Compose warns that a variable is not set**

If you see warnings like:

```text
The "abc123" variable is not set. Defaulting to a blank string.
```

then a value in `.env` contains `$...` and Docker Compose is trying to interpolate it.

Fix:

- wrap the value in single quotes, for example:

  ```env
  DJANGO_SECRET_KEY='my$literal$secret'
  ```

- or escape each dollar sign as `$$`

Then verify:

```bash
docker compose config > /dev/null
```

If that command prints no interpolation warnings, recreate the containers:

```bash
docker compose up -d --force-recreate caddy web scheduler
```

**`portal.oiatsolutions.com` returns `NXDOMAIN`**

The domain is not published correctly in public DNS yet.

Check:

- `portal.oiatsolutions.com` exists in Cloudflare DNS
- the record type is `A`
- the record points to the server's Tailscale IP
- the record is `DNS only`, not proxied

From a client device, verify:

```bash
nslookup portal.oiatsolutions.com
```

You should get the server's Tailscale IP back.

**Caddy fails to obtain or renew the certificate**

Common causes:

- `CF_API_TOKEN` is missing or invalid
- the token does not have both `Zone.Zone:Read` and `Zone.DNS:Edit`
- the token is scoped to the wrong zone
- `portal.oiatsolutions.com` is not publicly resolvable in Cloudflare DNS yet

Check recent Caddy logs:

```bash
docker compose logs --tail=100 caddy
```

Typical fixes:

- replace `CF_API_TOKEN` with a valid token for the correct zone
- make sure the portal DNS record exists before first certificate issuance
- keep the record `DNS only`

**Login page loads but authentication fails**

If login or CSRF fails behind Caddy, confirm these are set:

```env
DJANGO_CSRF_TRUSTED_ORIGINS=https://portal.oiatsolutions.com
DJANGO_USE_X_FORWARDED_PROTO=1
DJANGO_USE_X_FORWARDED_HOST=1
DJANGO_CSRF_COOKIE_SECURE=1
DJANGO_SESSION_COOKIE_SECURE=1
```

**Users, companies, or tokens are missing**

If the smoke tests show empty users, empty companies, or no tokens, the bootstrap seed probably did not populate the Docker volume. Once the Docker volume exists, copied repo files do not automatically overwrite it.

If you need to restart from the copied repo state, stop the app, remove the Docker volume, and start again:

```bash
docker compose down
docker volume rm code-scripts_app-data
docker compose run --rm --profile bootstrap bootstrap
docker compose up -d caddy web scheduler
```

This is destructive to current container state, so only do it if the volume contents are wrong and you want to reseed from the copied repo.

### Docker services and env behavior

- `bootstrap`, `caddy`, `web`, and `scheduler` all load the relevant env/config
- Caddy listens on `443` only, bound to the Tailscale IP you set in `.env`
- `web` is internal-only and is reached through Docker networking
- Compose requires `DJANGO_SECRET_KEY`, `DJANGO_ALLOWED_HOSTS`, and `CF_API_TOKEN`
- Persistent state lives in `/data` inside the containers, backed by the `app-data` volume
- The copied repo is mounted read-only into `/seed` only for the one-time `bootstrap` service

### Security posture

This setup is materially safer than exposing Django or Gunicorn directly, but it is not "unhackable" and it is not magic DDoS protection.

What is protecting the portal now:

- `web` is not published directly to the host
- Caddy only binds to the Tailscale IP, not all host interfaces
- the browser-facing certificate is a real Let's Encrypt certificate
- Django only serves configured hosts via `DJANGO_ALLOWED_HOSTS`
- secure cookies and forwarded-proto handling are enabled for HTTPS
- the portal still requires Django authentication after network access

What reduces DDoS exposure:

- the service is reachable over the Tailscale interface, not the open public internet
- the public DNS record points to a Tailscale IP, which is not generally routable from the internet

What this does **not** mean:

- Cloudflare is **not** proxying or shielding traffic here because the DNS record is `DNS only`
- this is **not** protected by Cloudflare WAF or Cloudflare DDoS mitigation
- if your Tailscale grants are too broad, any allowed tailnet user can still attempt logins or hammer the app
- the Django login does not currently have built-in brute-force throttling

The most important operational control is still Tailscale policy. Restrict `tcp:443` on this host to only the specific users or groups that should reach the portal.

---

## Running the Pipeline for All Companies (Daily Run)

The `run_all_companies.py` script orchestrates running the pipeline for all configured companies in sequence. It's designed for daily automation via cron or Task Scheduler.

**What it does:**

- Runs `run_pipeline.py` once per configured company
- Uses the pipeline's default behavior (processes "yesterday" if no date is supplied)
- Automatically discovers companies via `get_available_companies()`
- Explicitly ignores template/example configs (e.g., `company_example`)

**Usage:**

```bash
# Process all companies (yesterday's data)
python run_all_companies.py

# Process all companies for a specific date
python run_all_companies.py --target-date 2025-12-24

# Process all companies for a date range
python run_all_companies.py --from-date 2025-12-08 --to-date 2025-12-14

# Process specific companies only
python run_all_companies.py --companies company_a company_b

# Skip download (use existing split files in range mode)
python run_all_companies.py --from-date 2025-01-29 --to-date 2025-01-31 --skip-download
```

**Failure behavior:**

- If one company fails, execution stops immediately
- This is intentional to avoid silent partial failures
- Each company still emits its own Slack notifications (if configured)

**Design note:**

This script is intentionally thin — all business logic remains in `run_pipeline.py`. This makes it suitable for cron / Task Scheduler / daily automation where you want a single entry point that processes all companies sequentially.

Inventory sync mode is intentionally **not** configurable on `run_all_companies.py`; each company uses its own config/env value.

### Scheduled runs via Docker services

Use the in-repo scheduler worker through Docker Compose. The worker reads DB schedules from the `Schedules` page and enqueues `RunJob` records. In the `docker-build` branch, the scheduler is expected to run alongside the `web` service because both share the same runtime image and persistent state.

Execution path:

- `RunSchedule` (DB row) becomes due
- worker enqueues `RunJob` (queued)
- existing dispatcher starts the run and applies normal lock protections

```bash
# Build the app image
docker compose build

# Start web + scheduler in background
docker compose up -d web scheduler

# Tail logs
docker compose logs -f web scheduler
```

Scheduler env vars:

- `OIAT_SCHEDULER_POLL_SECONDS` (default `15`)
- `OIAT_SCHEDULER_ENABLE_ENV_FALLBACK` (default `1`)
- `SCHEDULE_CRON` (default `0 18 * * *`, i.e. 6pm daily) fallback cron when no enabled DB schedules exist
- `SCHEDULE_TZ` (default `Africa/Lagos`) fallback timezone when no enabled DB schedules exist

---

## OIAT Portal (Django Dashboard)

The OIAT Portal is a Django web application that provides a monitoring dashboard, run triggering, and company onboarding UI for the pipeline.

> **Tracking:** Security, performance, and deployment notes (env vars, runserver defaults, migrations) are in [docs/PORTAL_IMPROVEMENTS_AND_TRACKING.md](docs/PORTAL_IMPROVEMENTS_AND_TRACKING.md).

### Portal Setup

Use the **same virtual environment** as the pipeline. If you have not already done so, run the [First-time setup (all in one)](#first-time-setup-all-in-one) steps (venv, `pip install -r requirements.txt`, `playwright install chromium`, and on Linux/WSL `sudo playwright install-deps`). That ensures the dashboard can trigger runs without missing dependencies (pandas, Playwright, Chromium system libs).

Then run:

```bash
# 1. Apply database migrations
python manage.py migrate

# 2. Create an admin/operator user (required — there is no registration page)
python manage.py createsuperuser

# 3. Import existing company configs from JSON into the database
python manage.py sync_companies_from_json

# 4. (Optional) Backfill historical run artifacts from Uploaded/ metadata files
python manage.py ingest_run_history --days 60

# 5. Start the development server
python manage.py runserver
```

Then open `http://127.0.0.1:8000/` in your browser and log in with the superuser credentials.

- Template formatting rule: keep each Django variable tag on one line, for example `{{ value|default:"-" }}`. Do not wrap text inside `{{ ... }}` across lines.

### Portal Permissions

Four custom permissions control dashboard actions:

- `can_trigger_runs` — allows triggering pipeline runs from the Runs page
- `can_edit_companies` — allows creating/editing company configurations
- `can_manage_schedules` — allows creating/editing/toggling/deleting schedules and using Run Now
- `can_manage_portal_settings` — allows editing portal-wide defaults on the Settings page

Assign these to users via Django Admin (`/admin/`). Superusers have all permissions by default.

### Portal Management Commands

| Command | Purpose |
|---------|---------|
| `python manage.py sync_companies_from_json` | Import company configs from JSON files into DB |
| `python manage.py sync_companies_to_json` | Export DB company configs back to JSON files |
| `python manage.py check_company_config_drift` | Detect mismatches between DB and JSON configs |
| `python manage.py ingest_run_history --days 60` | Import historical run metadata from Uploaded/ |
| `python manage.py reconcile_run_jobs` | Mark stuck running jobs as failed (reaper) |
| `python manage.py run_schedule_worker` | Run the DB-backed scheduler worker loop (supports `--once` and `--poll-seconds`) |
| `python manage.py run_scheduled_all_companies --parallel 2` | Run all companies under global lock; creates RunJob for dashboard (used by scheduler service and can be run manually) |

### Portal Dashboard Tuning (Environment Variables)

These optional env vars tune portal defaults/thresholds without code edits:

| Variable | Default | Purpose |
|---------|---------|---------|
| `OIAT_DASHBOARD_TIMEZONE` | `TIME_ZONE` (UTC) | Timezone for non-overview dashboard calendar-day displays (e.g. runs/receipts "today"). |
| `OIAT_BUSINESS_TIMEZONE` | `Africa/Lagos` | Canonical business timezone used for overview target trading date and Quick Sync default date. |
| `OIAT_BUSINESS_DAY_CUTOFF_HOUR` | `5` | Trading-day cutoff hour in business timezone. Before cutoff, overview target date rolls back an extra day. |
| `OIAT_BUSINESS_DAY_CUTOFF_MINUTE` | `0` | Trading-day cutoff minute in business timezone. |
| `OIAT_DASHBOARD_DEFAULT_PARALLEL` | `2` | Default worker count for all-company run trigger form |
| `OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS` | `2` | Default stagger interval for all-company run trigger form |
| `OIAT_DASHBOARD_STALE_HOURS_WARNING` | `48` | Hours since last run before company sync-stale warning appears |
| `OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS` | `7` | Refresh-token warning threshold (in days) |
| `OIAT_DASHBOARD_REAUTH_GUIDANCE` | Built-in guidance text | Operator-facing re-auth instructions shown in dashboard token health states |

---

## Architecture Overview

### Trading day mode vs calendar day

- **Calendar day:** Each receipt is assigned the calendar date of the transaction. Deduplication checks DocNumber only.
- **Trading day mode** (config: `trading_day.enabled: true`): The "day" is determined by a cutoff time (e.g. 05:00). Transactions after midnight but before the cutoff are grouped with the previous calendar day. Used when your business day spans midnight. Deduplication checks both DocNumber and TxnDate so receipts are matched to the correct trading day.

### RAW-First Processing

The pipeline enforces **date correctness BEFORE transformation**:

1. EPOS CSV is treated as a multi-day ledger (may contain rows from multiple dates due to timezone differences)
2. The downloaded CSV is split by WAT date immediately after download
3. Future-date rows become **RAW spill files** (stored for later processing)
4. Transform receives only rows for the target date — it never creates or merges spills

> **Why RAW-first is safer:** Date filtering happens at the raw data level, before any transformation. This prevents double-processing, ensures no rows are lost, and keeps transform.py simple and stateless.

### RAW Spill System (Pipeline-Managed)

When processing date D, if the EPOS download contains rows for future dates (D+1, D+2, etc.):

1. **Creation:** Future rows are written as RAW spill files:

   ```
   uploads/spill_raw/<CompanyDir>/BookKeeping_raw_spill_YYYY-MM-DD.csv
   ```

2. **Merge:** When processing date D+1, the pipeline checks for a RAW spill file and merges it with the split file before transform

3. **Archive:** Used RAW spill files are moved to:

   ```
   Uploaded/YYYY-MM-DD/RAW_SPILL_BookKeeping_raw_spill_YYYY-MM-DD.csv
   ```

4. **Lifecycle:** RAW spill files remain in `uploads/spill_raw/` until their date is processed, then they're archived

> **Note:** There is no `uploads/spill/` directory. The old "transformed spill" system has been removed. All spill handling now happens at the RAW level in `run_pipeline.py`.

### Split Staging (Temporary)

The `uploads/range_raw/` directory is used ONLY as a staging area during processing:

- Single-day: `uploads/range_raw/<CompanyDir>/<date>_to_<date>/`
- Range mode: `uploads/range_raw/<CompanyDir>/<from>_to_<to>/`

These directories are **always cleaned up** after successful runs. No files in `uploads/` are authoritative after success.

### Archive Structure (Authoritative)

After a successful run, all relevant files are archived to:

```
Uploaded/YYYY-MM-DD/
├── ORIGINAL_<EPOS CSV>                          # Original downloaded EPOS CSV
├── RAW_SPLIT_BookKeeping_YYYY-MM-DD.csv         # Split raw file for this date
├── RAW_COMBINED_CombinedRaw_YYYY-MM-DD.csv      # (Only if RAW spill was merged)
├── RAW_SPILL_BookKeeping_raw_spill_*.csv        # (Only if RAW spill was used)
├── gp_sales_receipts_*.csv                      # Transformed/processed CSV
└── transform_metadata.json                       # Processing metadata
```

### Guarantees

- **No duplicate QBO uploads** — Deduplication via local ledger + QBO API checks
- **No silent row loss** — Future rows become RAW spill, past rows are logged
- **Spill rows processed exactly once** — RAW spills are archived after use
- **Repo root clean after success** — Original EPOS CSV is archived, staging dirs removed

---

## Files / Scripts

### Core Pipeline Scripts

- `run_pipeline.py`  
  **Main entry point** — Orchestrates all phases for single-day or range mode:

  1. Download EPOS CSV (`epos_playwright.py`)
  2. Split by WAT date and create RAW spill files for future dates
  3. Merge RAW spill (if exists for target date)
  4. Transform to QuickBooks CSV (`transform.py`)
  5. Upload to QuickBooks (`qbo_upload.py`)
  6. Archive all files to `Uploaded/<date>/`
  7. Reconcile EPOS vs QBO totals

  **Usage:**

  ```bash
  # Single-day (yesterday)
  python run_pipeline.py --company company_a

  # Single-day (specific date)
  python run_pipeline.py --company company_a --target-date 2025-12-24

  # Date range
  python run_pipeline.py --company company_b --from-date 2025-12-08 --to-date 2025-12-14

  # Skip download (use existing split files in uploads/range_raw/)
  python run_pipeline.py --company company_b --from-date 2025-01-29 --to-date 2025-01-31 --skip-download
  ```

  **Skip Download Mode:**

  The `--skip-download` flag allows you to process existing split CSV files without downloading from EPOS. This is useful when:
  - You already have split files in `uploads/range_raw/` from a previous run
  - You want to reprocess data without re-downloading
  - You're working with manually prepared CSV files

  **Requirements:**
  - Only works in range mode (`--from-date` and `--to-date` required)
  - Split files must exist in `uploads/range_raw/<CompanyDir>/<range_folder>/`
  - Files should be named `BookKeeping_YYYY-MM-DD.csv` or `CombinedRaw_YYYY-MM-DD.csv`

- `epos_playwright.py`  
  Uses **Playwright** to log into EPOS Now, navigate to the BookKeeping report, and download the CSV.
  Supports both single-date (`--target-date`) and range (`--from-date` / `--to-date`) downloads.

- `transform.py`  
  Transforms raw EPOS CSV into QuickBooks-ready format using company-specific configuration.

  **Important:** Transform.py receives a pre-filtered raw file via `--raw-file` and transforms only that data. All date filtering and spill handling happens at the RAW level in `run_pipeline.py`.

- `qbo_upload.py`  
  Uploads transformed CSV to QuickBooks Online via REST API.

  **Features:**

  - **Deduplication (Layer A)**: Local ledger tracks uploaded DocNumbers
  - **Deduplication (Layer B)**: Bulk QBO API checks before uploading
    - In **trading-day mode** with `--target-date`: Checks both DocNumber AND TxnDate to ensure receipts exist with the correct trading date
    - In calendar-day mode: Checks DocNumber only
  - Automatic token refresh on 401 errors
  - Location/Department mapping
  - VAT-inclusive amount handling

- `transform.py`  
  Transforms a single raw EPOS CSV file into QuickBooks-ready CSV. Typically invoked by `run_pipeline.py` with `--raw-file`; can be run standalone for testing.

  **Usage:**

  ```bash
  python transform.py --company company_a --target-date 2026-01-28 --raw-file path/to/BookKeeping_2026-01-28.csv
  ```

### Configuration

- `company_config.py` — Loads company-specific settings from JSON files
- `companies/company_a.json` — Company A configuration
- `companies/company_b.json` — Company B configuration

### Supporting Files

- `token_manager.py` — QuickBooks OAuth2 token management (SQLite storage, per-company tokens)
- `slack_notify.py` — Slack notification helpers
- `load_env.py` — Environment variable loader
- `scripts/qbo_queries/` — QBO query and debug scripts (see [QBO query scripts](#qbo-query-scripts) below)

### Re-import Bills from CSV

Bills can be exported to two CSVs (header + lines) with `scripts/qbo_export_bills.py`, then re-created in QBO after you delete them and adjust inventory (e.g. InvStartDate) using `scripts/qbo_import_bills.py`.

**Two-CSV format:**

- **Header CSV** (`bills_header.csv` by default): one row per bill. Required columns: `BillId`, `VendorId`, `TxnDate`. Optional: `DueDate`, `APAccountId`, `DocNumber`, `PrivateNote`, `Currency`, `ExchangeRate`, `TotalAmt`.
- **Lines CSV** (`bills_lines.csv` by default): one row per line. Required: `BillId`, `DetailType`, `Amount`. For **ItemBasedExpenseLineDetail**: `ItemId` (optional: `Qty`, `UnitPrice`, `TaxCodeId`, `CustomerId`, `ClassId`, `BillableStatus`). For **AccountBasedExpenseLineDetail**: `AccountId` (optional: `TaxCodeId`, `CustomerId`, `ClassId`). Optional on any line: `Description`.

The script validates that the header `TotalAmt` (if present) matches the sum of line `Amount` within 0.01 before creating.

**Tax (header “Amounts are” and line tax):**

- By default the script sets **GlobalTaxCalculation** to `TaxInclusive` and resolves the TaxCode named **Exempt** (e.g. “Exempt (0%)”) from QBO and applies it to every expense line. This makes the created bill show “Amounts are: Inclusive of Tax” and line tax “Exempt (0%)” instead of “No VAT” or “Out of Scope”.
- Use `--taxcode-name "Exempt"` (default) or another active TaxCode name; `--global-tax-calc TaxInclusive` (default). Dry-run prints the resolved TaxCode Id and each line’s TaxCodeRef.

**Bill number (DocNumber):**

- If the header CSV has a non-empty `DocNumber`, it is used as-is.
- Otherwise the script generates a deterministic DocNumber: `BILL-<VENDOR_CODE>-<YYYYMMDD>-<SHORT_HASH>` (e.g. `BILL-JNF-20260111-A3F9`), where VENDOR_CODE is the first letters of up to 3 words from the vendor name, and SHORT_HASH is the first 4 characters of a SHA1 of VendorId, TxnDate, TotalAmt, and BillId. Max length 21 characters. Re-importing the same bill produces the same DocNumber.

**Location (DepartmentRef) and Terms (SalesTermRef):**

- By default the script sets **Location** to “Plot C, Golf Road” and **Terms** to “Due on receipt” by resolving Department and Term by name from QBO and adding `DepartmentRef` and `SalesTermRef` to the Bill payload. Use **`--location-name`** (default: `Plot C, Golf Road`) and **`--terms-name`** (default: `Due on receipt`) to override. Dry-run prints the chosen names and resolved IDs.

**Multi-bill and taxcode:**

- Pass exactly one of **`--bill-id`** (single), **`--bill-ids`** (list), or **`--all`** (every BillId in header that has lines). TaxCode is resolved **once** at start (by name or by **`--taxcode-id`**) and reused for every bill. Use **`--taxcode-id 4`** (or your Exempt Id) to skip the TaxCode lookup when you already know it.

**Commands (run from repo root):**

```bash
# Single bill: dry-run or create
python scripts/qbo_import_bills.py --company company_a --bill-id 123 --dry-run
python scripts/qbo_import_bills.py --company company_a --bill-id 123 --create

# Multiple bills by ID list
python scripts/qbo_import_bills.py --company company_a --bill-ids 58984 58985 58986 --headers exports/company_a_bills/bills_header.csv --lines exports/company_a_bills/bills_lines.csv --create

# All bills in the CSVs
python scripts/qbo_import_bills.py --company company_a --all --headers exports/company_a_bills/bills_header.csv --lines exports/company_a_bills/bills_lines.csv --create

# Use known Exempt taxcode Id (no TaxCode query)
python scripts/qbo_import_bills.py --company company_a --bill-ids 58984 58985 --taxcode-id 4 --create

# Custom tax or CSV paths (single bill)
python scripts/qbo_import_bills.py --company company_a --bill-id 123 --taxcode-name "Exempt" --global-tax-calc TaxInclusive --headers path/to/bills_header.csv --lines path/to/bills_lines.csv --dry-run
```

You must pass exactly one of `--dry-run` or `--create`. Use `--dry-run` first to confirm the payload, then `--create` to POST to QBO.

### Data Folders

- `Uploaded/<date>/` — **Authoritative archive** after successful runs
- `uploads/spill_raw/` — RAW spill files awaiting processing (temporary)
- `uploads/range_raw/` — Split staging during processing (temporary, cleaned up)
- `logs/` — Pipeline execution logs

---

## Folder Structure

```text
code-scripts/
├── run_pipeline.py              # Main orchestrator
├── epos_playwright.py           # EPOS download
├── transform.py                 # CSV transformation
├── qbo_upload.py                # QuickBooks upload
├── company_config.py            # Company config loader
├── companies/
│   ├── company_a.json
│   └── company_b.json
│
├── uploads/                     # TEMPORARY staging (ignored by git)
│   ├── spill_raw/              # RAW spill files for future dates
│   │   └── <CompanyDir>/
│   │       └── BookKeeping_raw_spill_YYYY-MM-DD.csv
│   └── range_raw/              # Split staging (cleaned after success)
│       └── <CompanyDir>/
│           └── <from>_to_<to>/
│               ├── BookKeeping_YYYY-MM-DD.csv
│               └── CombinedRaw_YYYY-MM-DD.csv
│
├── Uploaded/                    # AUTHORITATIVE archive (ignored by git)
│   └── YYYY-MM-DD/
│       ├── ORIGINAL_*.csv
│       ├── RAW_SPLIT_*.csv
│       ├── RAW_COMBINED_*.csv   # (if spill merged)
│       ├── RAW_SPILL_*.csv      # (if spill used)
│       ├── gp_sales_receipts_*.csv
│       └── transform_metadata.json
│
└── logs/                        # Execution logs (ignored by git)
    └── pipeline_YYYYMMDD-HHMMSS.log
```

---

## Workflow Details

### Single-Day Mode

```bash
python run_pipeline.py --company company_a --target-date 2025-12-28
```

**Flow:**

1. **Download:** EPOS CSV for 2025-12-28 → repo root
2. **Split:** By WAT date
   - Rows for 2025-12-28 → `uploads/range_raw/.../BookKeeping_2025-12-28.csv`
   - Rows for 2025-12-29 → `uploads/spill_raw/.../BookKeeping_raw_spill_2025-12-29.csv`
3. **Merge:** Check if RAW spill exists for 2025-12-28, merge if so
4. **Transform:** Process merged/split file via `transform.py --raw-file ...`
5. **Upload:** Send to QuickBooks
6. **Archive:** Move all artifacts to `Uploaded/2025-12-28/`
7. **Cleanup:** Remove staging dirs, archive original CSV from repo root

### Range Mode

```bash
python run_pipeline.py --company company_b --from-date 2025-12-08 --to-date 2025-12-14
```

**Flow:**

1. **Download:** EPOS CSV for full range → repo root (skipped if `--skip-download` is used)
2. **Split:** By WAT date (all days) — or use existing split files if `--skip-download`
   - Rows for 2025-12-26 → `uploads/range_raw/.../BookKeeping_2025-12-26.csv`
   - Rows for 2025-12-27 → `uploads/range_raw/.../BookKeeping_2025-12-27.csv`
   - Rows for 2025-12-28 → `uploads/range_raw/.../BookKeeping_2025-12-28.csv`
   - Rows for 2025-12-29 → `uploads/spill_raw/.../BookKeeping_raw_spill_2025-12-29.csv`
3. **Loop per day:** For each day in range:
   - Check/merge RAW spill
   - Transform
   - Upload
   - Archive
4. **Final archive:** Archive range staging folder and original CSV (if downloaded)

**Skip Download Mode:**

When using `--skip-download`, the pipeline:
- Skips the EPOS download step
- Searches for existing split files in `uploads/range_raw/`
- Processes each day's split file (or `CombinedRaw_` file if spill was merged)
- Archives split files after successful completion
- Note: Trading-day cutoff info is included, but per-date reassignment counts are unavailable (requires original raw CSV)

### Timeline Example: RAW Spill Flow

**Day 1: Process 2025-12-27**

```
Download EPOS → Contains rows: 12-27 (500 rows), 12-28 (23 rows)
Split:
  → BookKeeping_2025-12-27.csv (500 rows) → transform → upload → archive
  → BookKeeping_raw_spill_2025-12-28.csv (23 rows) → stays in spill_raw/
```

**Day 2: Process 2025-12-28**

```
Download EPOS → Contains rows: 12-28 (480 rows), 12-29 (15 rows)
Split:
  → BookKeeping_2025-12-28.csv (480 rows)
  → BookKeeping_raw_spill_2025-12-29.csv (15 rows) → stays in spill_raw/
Merge: Found spill for 12-28! Merge 480 + 23 = 503 rows
  → CombinedRaw_2025-12-28.csv (503 rows) → transform → upload → archive
Archive: RAW_SPILL_BookKeeping_raw_spill_2025-12-28.csv moved to Uploaded/2025-12-28/
```

---

## Slack Notifications

If `SLACK_WEBHOOK_URL` is configured, the pipeline sends:

- **Start:** Pipeline beginning (includes date/range and company)
- **Watchdog Update:** When RAW spills are created or merged (high-signal only)
- **Success:** All phases completed with a structured summary: *Updates* (row stats, sales receipt upload stats, stale ledger, inventory items created/patched, warnings/blockers) and *Reconciliation* (MATCH/MISMATCH, EPOS total, QBO total, difference).
- **Failure:** Critical error with concise reason

**Watchdog messages include:**

- Future RAW spill creation: `"Future raw spill: 2025-12-29 (23 rows)"`
- RAW spill merge: `"2025-12-28: merged target split (480 rows) + raw spill (23 rows) -> final (503 rows)"`

**Range Mode Final Summary:**

When running in range mode (`--from-date` / `--to-date`), the final success message includes **Range Totals** that sum reconciliation results across all days:

```
• Range Totals (sum of per-day reconciliation):
  – EPOS: ₦X (N receipts)
  – QBO: ₦Y (M receipts)
  – Difference: ₦(X-Y)
```

If some days had reconciliation NOT RUN, the header shows: `Range Totals (partial — K/T days included):`

---

## Requirements

- **Python 3.9+**
- **EPOS Now HQ** account credentials
- **QuickBooks Online** account with Developer app access

### Install

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .\.venv\Scripts\Activate.ps1  # Windows

# Install dependencies
pip install -r requirements.txt

# Install Playwright browser
playwright install chromium
```

---

## Initial Setup

### 1. Configure Credentials

Copy and edit the environment file:

```bash
cp .env.example .env
```

Required variables:

```
# QuickBooks OAuth credentials (shared across all companies)
QBO_CLIENT_ID=your_client_id
QBO_CLIENT_SECRET=your_client_secret

# EPOS credentials (company-specific)
EPOS_USERNAME_A=your_epos_username_for_company_a
EPOS_PASSWORD_A=your_epos_password_for_company_a
EPOS_USERNAME_B=your_epos_username_for_company_b
EPOS_PASSWORD_B=your_epos_password_for_company_b

# Slack webhooks (optional, company-specific)
SLACK_WEBHOOK_URL_A=your_slack_webhook_for_company_a  # Optional
SLACK_WEBHOOK_URL_B=your_slack_webhook_for_company_b  # Optional
```

**Note:** `QBO_REALM_ID` is **not** required as an environment variable. Realm IDs are configured per-company in `companies/company_a.json` and `companies/company_b.json`.

### 2. Get Initial OAuth Tokens

The pipeline uses `qbo_tokens.sqlite` to store OAuth tokens, isolated by company and realm_id.

**For each company:**

1. Perform OAuth flow via Intuit's OAuth playground or your OAuth implementation
2. Store tokens using the helper script `store_tokens.py`:

**Example store command (company_a):**

```bash
python store_tokens.py --company company_a --access-token "..." --refresh-token "..." --expires-in 3600 --env production
```

**Example store command (company_b):**

```bash
python store_tokens.py --company company_b --access-token "..." --refresh-token "..." --expires-in 3600 --env production
```

**Example list command (view stored tokens):**

```bash
python store_tokens.py --list
```

**Notes:**

- `qbo_tokens.sqlite` is local state, gitignored, and must be created per machine (or copied manually)
- In Docker deployments, `qbo_tokens.sqlite` lives in the `app-data` volume after first start
- Do not commit tokens or the database file
- The script automatically loads the `realm_id` from your company configuration file
- Optional: You can use a GUI tool like [DB Browser for SQLite](https://sqlitebrowser.org/) to view the database contents (useful for debugging or verifying stored tokens)

**Adding a second company:** Simply run the OAuth flow again for the new company and store tokens using the same script with the new company's `--company` argument. The SQLite database stores tokens separately per company.

### 3. QBO query scripts

Ad-hoc query and debug scripts live in `scripts/qbo_queries/`. Run from repo root with `--company` (required). See `scripts/qbo_queries/README.md` for the full list.

| Script | Purpose |
|--------|---------|
| `qbo_query.py` | Run arbitrary QBO SQL-like query |
| `qbo_inv_manager.py` | **Inventory manager:** get item by ID/name (incl. ParentRef/FullyQualifiedName), list InvStartDate issues, set InvStartDate (single, bulk, or from CSV). See [Inventory manager](#inventory-manager-qbo_inv_managerpy) below. |
| `qbo_account_query.py` | Run Account queries (name-based) |
| `qbo_verify_mapping_accounts.py` | Verify Product.Mapping.csv accounts exist in QBO |

#### Inventory manager (qbo_inv_manager.py)

The inventory manager lives in `scripts/qbo_inv_manager.py` and consolidates item lookup, InvStartDate listing, and InvStartDate patching. Run from repo root with `--company` (required).

### Invoice import (company_a only)

Use `scripts/qbo_import_invoices.py` to create QBO invoices from a CSV. This is **separate** from the sales receipt pipeline.

**Template:** `templates/invoice_template.csv`

**Required columns**

- `Customer`
- `InvoiceDate` (YYYY-MM-DD)
- `ItemName`
- `Qty`
- `Rate`
- `Amount`

**Optional columns**

- `ServiceDate` (defaults to `InvoiceDate`)
- `Description` (used only if matched item has no description)
- `Location` (maps to QBO Department/Location)
- `DueDate` (defaults to `InvoiceDate + 30 days`)

**Notes**

- Company scope: **company_a only**
- Item matching: fuzzy match to existing QBO items only (no new items created)
- Optional alias mapping: `templates/item_aliases.csv` (CsvItemName → QboItemName)
- Unmatched items are skipped and reported in `reports/`
- Tax: all lines set to **No VAT**

**Run**

```bash
python3 scripts/qbo_import_invoices.py --company company_a --csv /path/to/invoices.csv
```

**Dry run**

```bash
python3 scripts/qbo_import_invoices.py --company company_a --csv /path/to/invoices.csv --dry-run
```

**Validate only + aliases**

```bash
python3 scripts/qbo_import_invoices.py --company company_a --csv /path/to/invoices.csv --validate-only --aliases templates/item_aliases.csv
```

### Invoice CSV preparation

Use `scripts/prepare_invoice_csv.py` to normalize a source invoice spreadsheet into the invoice template and perform alias-first + fuzzy matching.

**Run (with live QBO item list)**

```bash
python3 scripts/prepare_invoice_csv.py --csv /path/to/source.csv --company company_a
```

**Run (offline QBO item list)**

```bash
python3 scripts/prepare_invoice_csv.py --csv /path/to/source.csv --qbo-items-csv /path/to/qbo_items.csv
```

**Outputs**

- `{source_stem}_prepared.csv`
- `{source_stem}_unmatched.csv` (if any)

| Subcommand | Purpose | Example |
|------------|---------|---------|
| `get` | Get item by ID or search by name | `python scripts/qbo_inv_manager.py --company company_a get --item-id 7220` or `get --name "NAN-OPTIPRO"` |
| `list-invstart` | List inventory items with InvStartDate after cutoff (optional export CSV) | `python scripts/qbo_inv_manager.py --company company_a list-invstart --cutoff-date 2026-01-01 --export-csv reports/issues.csv` |
| `set-invstart` | Set InvStartDate for one item | `python scripts/qbo_inv_manager.py --company company_a set-invstart --item-id 7220 --date 2026-01-01` |
| `set-invstart-bulk` | Find all items with InvStartDate > cutoff and patch to new date | `python scripts/qbo_inv_manager.py --company company_a set-invstart-bulk --cutoff-date 2026-01-01 --new-date 2026-01-01` |
| `set-invstart-from-csv` | Patch InvStartDate for item IDs listed in a CSV (e.g. blockers file) | `python scripts/qbo_inv_manager.py --company company_a set-invstart-from-csv --csv reports/inventory_start_date_blockers_company_a_2026-01-01.csv --new-date 2026-01-01` |

For bypass mode and for `set-invstart-from-csv`, the CSV must have an `ItemId` column. The `list-invstart --export-csv` output uses column `Id`; rename it to `ItemId` for use as blockers CSV or with `set-invstart-from-csv`.

### 4. Verify .gitignore

Ensure these are ignored:

- `qbo_tokens.sqlite` — OAuth tokens database (SQLite)
- `*.sqlite-wal`, `*.sqlite-shm` — SQLite sidecar files
- `.env` — Credentials
- `uploads/` — Temporary staging
- `Uploaded/` — Archive
- `logs/` — Execution logs
- `*.csv` — Processing files

### 5. (Optional) Enable Pre-commit Secret Scanning

To catch hardcoded secrets before committing, you can enable pre-commit hooks:

```bash
# Pre-commit is included in requirements.txt. Install the git hooks:
pre-commit install

# Run on all files (optional, to check existing code)
pre-commit run --all-files
```

**Note:** The pre-commit hook will automatically download gitleaks (v8.18.0) on first run. You do not need to install gitleaks manually — it's fully self-contained and works on macOS, Windows, and Linux.

This will automatically scan for secrets before each commit. The same scanning also runs in CI on pull requests and will block PRs if secrets are detected.

**Note:** Secret scanning is enforced in CI regardless of whether you use pre-commit locally.

---

## Adding a New Company

The pipeline supports multiple companies, each with its own configuration file. Company configs use a **flexible schema** — different companies may have different fields depending on their requirements (tax modes, location mapping, etc.).

### Step-by-Step: Adding `company_c`

1. **Copy the template:**

   ```bash
   cp companies/company.example.json companies/company_c.json
   ```

2. **Edit `companies/company_c.json` and update required fields:**

   **Required (minimum viable schema):**

   - `company_key`: `"company_c"` (must match filename)
   - `qbo.realm_id`: Your QBO Realm ID (replace `"REPLACE_WITH_YOUR_REALM_ID"`)
   - `qbo.deposit_account`: Your deposit account name (e.g., `"100900 - Undeposited Funds"`)
   - `epos.username_env_key`: Environment variable name (e.g., `"EPOS_USERNAME_C"`)
   - `epos.password_env_key`: Environment variable name (e.g., `"EPOS_PASSWORD_C"`)
   - `transform.group_by`: Choose grouping strategy:
     - `["date", "tender"]` — Simple grouping (like Company A)
     - `["date", "location", "tender"]` — Location-aware grouping (like Company B)
   - `transform.date_format`: Date format string (e.g., `"%Y-%m-%d"` or `"%d/%m/%Y"`)
   - `transform.receipt_prefix`: Receipt prefix (e.g., `"SR"`)
   - `transform.receipt_number_format`: Choose format:
     - `"date_tender_sequence"` — For simple grouping (SR-YYYYMMDD-SEQ)
     - `"date_location_sequence"` — For location-aware grouping (SR-YYYYMMDD-LOC-SEQ)
   - `output.csv_prefix`: Unique prefix for CSV files (e.g., `"sales_receipts"`)
   - `output.metadata_file`: Unique metadata filename (e.g., `"last_transform.json"`)
   - `output.uploaded_docnumbers_file`: Unique ledger filename (e.g., `"uploaded_docnumbers.json"`)

   > **Note:** `metadata_file` and `uploaded_docnumbers_file` are per-company state files. They may differ between companies depending on transform logic and should remain unique. For example, Company A uses `last_epos_transform.json` while Company B uses `last_gp_transform.json` — this prevents state file conflicts when running the pipeline for different companies.

   **Optional fields (configure as needed):**

   - `display_name`: Human-readable company name (defaults to `company_key` if omitted)
   - `qbo.tax_mode`:
     - `"vat_inclusive_7_5"` (default) — Single-rate VAT
     - `"tax_inclusive_composite"` — Multi-component tax (requires `tax_components`)
   - `qbo.tax_rate`: Tax rate as decimal (defaults to `0.075` if omitted)
   - `qbo.tax_code_id`: QBO Tax Code ID (optional, used if provided)
   - `qbo.tax_code_name`: Tax code name to query from QBO (optional)
   - `qbo.tax_rate_id`: QBO Tax Rate ID (required for `vat_inclusive_7_5` mode if `tax_code_id` not set)
   - `qbo.default_item_id`: Default item ID (defaults to `"1"`)
   - `qbo.default_income_account_id`: Default income account ID (defaults to `"1"`)
   - `qbo.department_mapping`: Maps location names to QBO Department IDs (empty object `{}` if not needed)
   - `transform.location_mapping`: Maps EPOS location names to location codes (empty object `{}` if not needed)
   - `slack.webhook_url_env_key`: Environment variable name or direct URL for Slack notifications (entire `slack` section optional)

   **Conditional fields (required only for specific tax modes):**

   - `qbo.tax_components`: **Required only if `tax_mode == "tax_inclusive_composite"`**. Array of tax components:
     ```json
     "tax_components": [
       {"name": "VAT", "rate": 0.075, "tax_rate_id": "17"},
       {"name": "Lagos State", "rate": 0.05, "tax_rate_id": "30"}
     ]
     ```

3. **Add environment variables to `.env`:**

   ```bash
   EPOS_USERNAME_C=your_epos_username
   EPOS_PASSWORD_C=your_epos_password
   SLACK_WEBHOOK_URL_C=your_slack_webhook_url  # Optional
   ```

4. **Authorize QBO tokens:**

   Follow the OAuth flow (see [Initial Setup](#2-get-initial-oauth-tokens)) and store tokens for `company_c`:

   ```python
   from token_manager import store_tokens_from_oauth
   from company_config import load_company_config

   config = load_company_config("company_c")
   store_tokens_from_oauth(
       company_key="company_c",
       realm_id=config.realm_id,
       access_token="your_access_token",
       refresh_token="your_refresh_token",
       expires_in=3600
   )
   ```

5. **Test the configuration:**

   ```bash
   python run_pipeline.py --company company_c --target-date 2025-01-01
   ```

### Configuration Schema Notes

- **Flexible schema:** Company configs may vary — some companies need `department_mapping`, others don't. The code handles missing optional fields gracefully.
- **Tax mode differences:**
  - `vat_inclusive_7_5`: Single tax rate, requires `tax_code_id` or `tax_rate_id`
  - `tax_inclusive_composite`: Multiple tax components, requires `tax_components` array
- **Location handling:**
  - If `group_by` includes `"location"`, you'll likely need `location_mapping` to map EPOS locations to codes
  - If `receipt_number_format == "date_location_sequence"`, location codes are used in receipt numbers
- **All company config files are committed to git** (they contain no secrets, only configuration and environment variable key names)

---

## Inventory Items Configuration

The pipeline supports creating QBO Inventory items (instead of Service items) when products don't exist in QuickBooks. This feature is configurable per company and uses category-based account mapping.

### Configuration

Add an optional `inventory` section to your company JSON config:

```json
{
  "inventory": {
    "enable_inventory_items": false,
    "allow_negative_inventory": false,
    "inventory_sync_mode": "inline",
    "inventory_start_date": "today",
    "default_qty_on_hand": 0,
    "product_mapping_file": "mappings/Product.Mapping.csv"
  }
}
```

**Fields:**
- `enable_inventory_items`: Enable inventory item creation (default: `false`)
- `allow_negative_inventory`: Allow negative inventory when posting SalesReceipts (default: `false`)
- `inventory_sync_mode`: Inventory item sync path (default: `"inline"`). Allowed: `"inline"` or `"upload_fast"`
- `inventory_start_date`: Start date for inventory tracking - use `"today"` or ISO date like `"2026-01-26"` (default: `"today"`)
- `default_qty_on_hand`: Starting quantity for new inventory items (default: `0`)
- `product_mapping_file`: Path to category mapping CSV (default: `"mappings/Product.Mapping.csv"`)

### Environment Variable Overrides

Precedence: **ENV → company JSON → defaults**

You can override inventory settings via environment variables:

```bash
COMPANY_A_ENABLE_INVENTORY_ITEMS=true
COMPANY_A_ALLOW_NEGATIVE_INVENTORY=true
COMPANY_A_INVENTORY_SYNC_MODE=inline
COMPANY_A_INVENTORY_START_DATE=2026-01-26  # or "today"
COMPANY_A_DEFAULT_QTY_ON_HAND=0
```

### Product Category Mapping

The pipeline uses `mappings/Product.Mapping.csv` to map EPOS product categories to QBO accounts. The CSV must have these exact headers:

- `Category` — EPOS product category (matches EPOS CSV "Category" column)
- `Inventory Account` — Asset account (e.g., `"120000 - Inventory:120100 - Grocery"`)
- `Revenue Account` — Income account (e.g., `"400000 - Revenue:400100 - Revenue - Grocery"`)
- `Cost of Sale account` — COGS account (e.g., `"200000 - Cost of sales:200100 - Purchases - Groceries"`)

**Account Resolution:**
- Accounts are resolved by `FullyQualifiedName` first
- Falls back to `AccountNumber` if FullyQualifiedName not found
- Account strings format: `"<AccountNumber> - <FullyQualifiedName>"`

**Important:** If any EPOS category is missing in the mapping CSV, the pipeline will fail with a clear error message.

### QuickBooks Settings

When `allow_negative_inventory` is enabled, you must also enable negative inventory in QuickBooks:

1. Go to **Settings** → **Company Settings** → **Sales**
2. Enable **"Allow negative inventory"**
3. Save changes

If negative inventory is not enabled in QBO, SalesReceipts will be rejected with an error message.

### Example: Company A Configuration

```json
{
  "company_key": "company_a",
  "inventory": {
    "enable_inventory_items": true,
    "allow_negative_inventory": true,
    "inventory_start_date": "today",
    "default_qty_on_hand": 0
  }
}
```

### Behavior

**When `enable_inventory_items` is `true`:**
- Item resolution runs **once per run**: all unique item names are prefetched from QBO, then resolved (patch or create) in a single phase. Per line, only a lookup in `item_result_by_name` is used — no per-line QBO API calls.
- Missing products are created as **Inventory items** (not Service items)
- Items start with `QtyOnHand = default_qty_on_hand` (typically 0)
- Accounts are mapped from category using `mappings/Product.Mapping.csv` (categories → Inventory/Revenue/COGS accounts)
- When items are created or patched, UnitPrice and PurchaseCost are set/updated from CSV (UnitPrice: when missing/0 or differs by >0.01; PurchaseCost: when missing/0)
- Unit prices are set from EPOS CSV `NET Sales` column (per-unit); purchase costs from `Cost Price` column (per-unit)

`inventory_sync_mode` controls how existing items are handled:
- `inline` (default): patch existing inventory items inline (pricing/tax/category) and allow wrong-type auto-fix when enabled.
- `upload_fast`: skip expensive existing-item patch path during upload; still create missing inventory items as `Type=Inventory`.

Use `upload_fast` to reduce upload critical-path time, then run maintenance sync to apply catalog drift updates.

### Inventory Catalog Maintenance Sync

When using `upload_fast`, run catalog maintenance separately to sync existing inventory item pricing/tax/category:

```bash
python -m code_scripts.sync_inventory_catalog --company company_a
```

Optional explicit CSV/date:

```bash
python -m code_scripts.sync_inventory_catalog --company company_a --csv outputs/Akponora_Ventures_Ltd/file.csv --target-date 2026-02-17
```

**When `enable_inventory_items` is `false` (default):**
- Missing products are created as **Service items** (existing behavior)
- No account mapping required
- No inventory tracking

**Negative Inventory Handling:**
- If `allow_negative_inventory` is `true` and QBO accepts the SalesReceipt (with warnings), the pipeline continues and logs a warning
- If QBO rejects due to inventory, the pipeline fails with instructions to enable negative inventory in QBO settings
- If `allow_negative_inventory` is `false`, inventory errors are treated as fatal (existing behavior)

### InvStartDate and QBO 6270

Inventory items whose **InvStartDate** is after the receipt date can cause QBO error **6270**. The upload script no longer runs an InvStartDate audit or patch.

- Use the **inventory manager** (see [Inventory manager](#inventory-manager-qbo_inv_managerpy) below) to list issues and set InvStartDate (single item, bulk, or from a blockers CSV).

### Bypass inventory start-date (optional)

When QBO rejects SalesReceipts with error **6270** (transaction date prior to inventory start date), you can optionally **replace** blocked line items with a **Service item** so totals and tax stay correct, without changing inventory items.

**This mode is off by default** and must be explicitly enabled with `--bypass-inventory-startdate`.

**Requirements:**
- Configure a bypass income account: in company JSON set `qbo.bypass_income_account_id` to your QBO income account ID, or set env `COMPANY_A_BYPASS_INCOME_ACCOUNT_ID` (replace `A` with your company key).
- Optionally generate a blockers CSV with the inventory manager:  
  `python scripts/qbo_inv_manager.py --company company_a list-invstart --cutoff-date 2026-01-01 --export-csv reports/inventory_start_date_blockers_company_a_2026-01-01.csv`  
  (Use the same cutoff date as your run. The blockers CSV must have an `ItemId` column; if you use `list-invstart --export-csv`, that file has column `Id` — rename to `ItemId` for bypass or `set-invstart-from-csv`.)

**Flags (all optional except enabling bypass):**

| Flag | Description |
|------|-------------|
| `--bypass-inventory-startdate` | Enable bypass mode (never default). |
| `--bypass-item-name "EPOS Sales (Bypass)"` | Name of the Service item used for swapped lines (default: `EPOS Sales (Bypass)`). |
| `--bypass-income-account <id>` | Override income account ID (otherwise from config/env). |
| `--bypass-report-csv <path>` | Path for swap report CSV (default: `reports/bypass_swaps_<company>_<date>.csv`). |
| `--bypass-blockers-csv <path>` | Path to blockers CSV (default: `reports/inventory_start_date_blockers_<company>_<date>.csv`). |
| `--dry-run` | Build payloads, apply swaps, write report CSV; do not upload to QBO. |

**Example:**

```bash
# With pre-loaded blockers CSV and bypass income account in config
python qbo_upload.py --company company_a --target-date 2026-01-01 --bypass-inventory-startdate

# Dry run: see what would be swapped and where report would be written
python qbo_upload.py --company company_a --target-date 2026-01-01 --bypass-inventory-startdate --dry-run
```

**Behavior:**
- Blocked lines (item in blockers set) are replaced with the bypass Service item; **line Amount, tax code, and tax treatment are unchanged**.
- Each swapped line gets audit text in Description: `[BYPASS_INVSTARTDATE] originalItemId=... originalName=...`
- A CSV report of every swap is written (columns: company, receiptDocNo, receiptTxnDate, originalItemId, originalItemName, bypassItemId, lineAmount, taxCode, reason).
- If the first upload fails with 6270 and blockers were not pre-loaded, the script diagnoses, applies swaps, and **retries the upload once**.

**When to use:** One-time remediation when many receipts are blocked by InvStartDate; prefer fixing InvStartDate on items using `scripts/qbo_inv_manager.py` (e.g. `set-invstart-bulk` or `set-invstart-from-csv`) when possible.

### Verification Checklist

After enabling inventory items, verify:
- [ ] No "Uncategorised items or services" in Profit & Loss
- [ ] Products appear as Inventory items (not Service) in QBO
- [ ] Inventory items show correct accounts (Asset, Income, COGS)
- [ ] Companies without inventory enabled still create Service items (unchanged behavior)
- [ ] Slack summary includes inventory stats (items created, warnings, rejections)

**Inventory pricing (TOTAL Sales / Cost Price):**

1. Run transform so output CSV includes pricing columns:
   `python transform.py --company company_a --target-date 2026-01-28 --raw-file BookKeeping_2026_01_29_1911.csv`
   - Confirm output CSV has columns: **TOTAL Sales**, **NET Sales**, **Cost Price** with row values (e.g. 500.00, 465.12, 329.46).
2. In QBO: Product/Service → open Inventory item → **Purchasing** tab: Purchase cost should match Cost Price / qty (e.g. 329.46). **Sales** tab: Price/rate should match TOTAL Sales / qty (e.g. 500.00) with "Price is inclusive of sales tax" reflected.

---

## Troubleshooting

### QBO query and item gotchas

- **SubItem cannot be selected in some QBO UI dropdowns:** When using item hierarchy (SubItem + ParentRef), sub-items may not appear as selectable in every QBO screen (e.g. when picking an item for a transaction). Use the **parent category** or search by name where supported. The API accepts sub-items for Sales Receipt lines; the limitation is UI-only.
- **"Category:Product" display on Sales Receipts:** When `use_item_hierarchy` is true, QuickBooks displays the **FullyQualifiedName** (e.g. `Category Name:Product Name`) in the Product/Service column. This is expected and not a bug; we are not changing this behavior.

### Logs to look for

- **Mapping loaded:** `[INFO] Loaded N category mappings from mappings/Product.Mapping.csv`
- **Item resolution summary:** `[INFO] Item resolution summary: total_lines=... unique_items=... items_created=... items_patched=... item_lookups_from_prefetch=...`
- **Items created/patched:** `[INFO] Patched Inventory item fields: Id=... UnitPrice:...->... PurchaseCost:...->...` and `[INFO] Attached ParentRef/SubItem to Inventory item '...'`
- **Inventory tip:** `[INFO] For InvStartDate issues (QBO 6270), use: python scripts/qbo_inv_manager.py --company <key> list-invstart / set-invstart-bulk`

### RAW Spill Not Being Merged

- Verify spill file exists: `uploads/spill_raw/<CompanyDir>/BookKeeping_raw_spill_YYYY-MM-DD.csv`
- File name must match target date exactly
- Check logs for "Found raw spill file for..." message

### Duplicate Sales Receipts

The pipeline includes automatic deduplication:

- **Layer A:** Local ledger (`uploaded_docnumbers.json`) — tracks DocNumbers that have been uploaded
- **Layer B:** Bulk QBO API check before upload
  - **Trading-day mode** (when `trading_day.enabled: true` and `--target-date` is provided): Checks both DocNumber AND TxnDate to ensure receipts exist with the correct trading date. This prevents skipping receipts that exist with the wrong date.
  - **Calendar-day mode:** Checks DocNumber only

**QBO is the source of truth:** If a DocNumber exists in QBO (with matching TxnDate in trading-day mode), the upload is skipped. Stale ledger entries (in ledger but not in QBO) are detected, logged, and healed by attempting upload.

If you need to re-upload, delete existing receipts first using the QBO query scripts in `scripts/qbo_queries/` (e.g. `qbo_query.py` to run a query, or QBO UI).

### Token Refresh Fails

- Refresh tokens expire after ~100 days
- Re-authorize via OAuth flow to get new tokens and store using `store_tokens_from_oauth()`
- Verify `QBO_CLIENT_ID` and `QBO_CLIENT_SECRET` are correct in `.env`
- Check that tokens exist in `qbo_tokens.sqlite` for the company/realm_id combination

### Missing Environment Variables

```bash
# Check if set
echo $QBO_CLIENT_ID

# Use .env file (recommended) or export directly
export QBO_CLIENT_ID="your_id"
```

---

## Security Best Practices

- **Credentials:** Use `.env` file or environment variables, never hardcode
- **Django deployment:** In Docker, set `DJANGO_SECRET_KEY` and `DJANGO_ALLOWED_HOSTS` explicitly in `.env`
- **Tokens:** `qbo_tokens.sqlite` is auto-created if missing; in Docker it is persisted in the `app-data` volume
- **Git:** `.gitignore` excludes all sensitive files (including `qbo_tokens.sqlite` and SQLite sidecar files)
- **Production:** Use a secrets manager (AWS Secrets Manager, HashiCorp Vault)

---

## Design Notes

### RAW-First Design

The pipeline uses a RAW-first approach: all date filtering and spill handling happens at the raw data level in `run_pipeline.py`, before transformation. This ensures:

1. **Single source of truth:** Date filtering happens once, at download time
2. **No double-processing:** Rows are assigned to exactly one date
3. **Stateless transform:** `transform.py` receives pre-filtered data and has no knowledge of spills
4. **Clear lifecycle:** RAW spill files are created, awaited, merged, and archived — never modified

### Why RAW-First Is Safer

- **Single source of truth:** Date filtering happens once, at download time
- **Immutable spill files:** RAW spill files are never modified, only archived
- **Clear lifecycle:** Create → Await → Merge → Archive
- **Stateless transform:** `transform.py` has no knowledge of spills

---

## Notes

- Start with a **QuickBooks sandbox** before using production credentials
- Files are automatically archived after success — check `Uploaded/<date>/` if looking for processed data
- The pipeline cleans up staging directories after success — `uploads/range_raw/` should be empty
- RAW spill files stay in `uploads/spill_raw/` until their date is processed
