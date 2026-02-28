# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repo Is

An EPOS → QuickBooks Online automation pipeline with a Django monitoring dashboard (the "OIAT Portal"). The pipeline downloads daily sales CSVs from EPOS Now, transforms them, uploads them to QuickBooks Online via REST API, and archives all artifacts. The Django portal provides run triggering, scheduling, company management, and dashboard views over the pipeline.

## Development Setup

```bash
# One-time setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# Portal DB setup (first time)
python manage.py migrate
python manage.py createsuperuser
python manage.py sync_companies_from_json

# Start the portal
python manage.py runserver
```

## Common Commands

### Pipeline (run from repo root)

```bash
# Single company, yesterday
python run_pipeline.py --company company_a

# Single company, specific date
python run_pipeline.py --company company_a --target-date 2025-12-24

# Date range
python run_pipeline.py --company company_b --from-date 2025-12-08 --to-date 2025-12-14

# Reprocess existing split files without re-downloading
python run_pipeline.py --company company_b --from-date 2025-01-29 --to-date 2025-01-31 --skip-download

# All companies
python run_all_companies.py

# Transform only (standalone, for testing)
python transform.py --company company_a --target-date 2026-01-28 --raw-file path/to/BookKeeping_2026-01-28.csv
```

### CSS (Tailwind)

```bash
# Build minified CSS
npm run build:css

# Watch for changes during development
npm run watch:css
```

Input: `apps/epos_qbo/static_src/css/tailwind.input.css`
Output: `apps/epos_qbo/static/css/tailwind.css`

### Tests

```bash
# Run all tests
python manage.py test

# Run a single test module
python manage.py test apps.epos_qbo.tests.test_auth_and_permissions

# Run a specific test
python manage.py test apps.epos_qbo.tests.test_auth_and_permissions.SomeTestClass.test_method
```

### Portal Management Commands

```bash
python manage.py sync_companies_from_json    # JSON → DB
python manage.py sync_companies_to_json      # DB → JSON
python manage.py check_company_config_drift  # Detect mismatches
python manage.py ingest_run_history --days 60
python manage.py reconcile_run_jobs          # Mark stuck jobs as failed
python manage.py run_schedule_worker         # Start DB-backed scheduler loop
```

### Pre-commit Hooks

```bash
pip install pre-commit && pre-commit install
pre-commit run --all-files
```

Two hooks: a Django template variable tag guard (no multiline `{{ ... }}`) and Gitleaks secret scanning.

## Architecture

### Two Separate Concerns

1. **Pipeline scripts** (`code_scripts/`, `run_pipeline.py`, `transform.py`, etc.) — pure Python CLI scripts, no Django dependency.
2. **OIAT Portal** (`apps/`, `oiat_portal/`, `manage.py`) — Django app that wraps/triggers/monitors the pipeline.

The pipeline scripts live in `code_scripts/` and are importable as a package. Root-level scripts (`run_pipeline.py`, `transform.py`, etc.) are thin wrappers/entry points that import from `code_scripts/`.

### Django App Layout (`apps/`)

- **`apps.core`** — Login/auth middleware (`LoginRequiredMiddleware`), base views, shared utilities.
- **`apps.epos_qbo`** — The main portal app. Contains all models, views, forms, templates, services, management commands, and tests. URL namespace: `epos_qbo`.
- **`apps.dashboard`** — Thin workspace-selector landing page only.

Database: SQLite (`db.sqlite3`). Settings in `oiat_portal/settings.py`.

### `apps.epos_qbo` Services Layer

Business logic is split into service modules under `apps/epos_qbo/services/`:

- `artifact_ingestion.py` — Import run artifacts from `Uploaded/` into DB
- `config_sync.py` — Sync company configs between JSON files and DB
- `job_runner.py` — Dispatch and manage `RunJob` records (wraps `run_pipeline.py` subprocess)
- `locking.py` — Global run lock (prevents concurrent runs)
- `metrics.py` — Dashboard KPI queries
- `schedule_worker.py` — DB-backed scheduler: polls `RunSchedule` rows and enqueues `RunJob`s

### Company Configuration

Companies are defined as JSON files in `code_scripts/companies/`. The DB (`CompanyConfig` model) is the source of truth at runtime; JSON files are synced to/from DB via management commands. Use `company.example.json` as a template when adding a new company.

### Pipeline Data Flow

1. **Download** — Playwright logs into EPOS Now, downloads BookKeeping CSV.
2. **Split** — Raw CSV split by WAT date. Future-date rows become RAW spill files (`uploads/spill_raw/<CompanyDir>/`).
3. **Merge** — Any existing RAW spill for the target date is merged with split file.
4. **Transform** — `transform.py` produces a QuickBooks-ready CSV from the filtered raw file.
5. **Upload** — `qbo_upload.py` posts to QBO API with two-layer deduplication (local ledger + QBO bulk query).
6. **Archive** — All artifacts moved to `Uploaded/YYYY-MM-DD/` (authoritative, gitignored).
7. **Reconcile** — EPOS totals vs QBO totals verified.

Staging directories (`uploads/range_raw/`, `uploads/spill_raw/`) are **temporary**. `Uploaded/` is **authoritative**.

### Portal Permissions

Four custom permissions (assign via `/admin/`):
- `can_trigger_runs` — trigger pipeline runs
- `can_edit_companies` — create/edit company configs
- `can_manage_schedules` — manage cron schedules and Run Now
- `can_manage_portal_settings` — edit portal-wide settings

Superusers have all permissions.

### Template Convention

Keep Django variable tags on one line — never wrap `{{ ... }}` across multiple lines. This is enforced by the pre-commit hook.

### Docker / Scheduler

The `docker-compose.yml` defines a `scheduler` service that runs the schedule worker. Env vars: `OIAT_SCHEDULER_POLL_SECONDS` (default 15), `SCHEDULE_CRON` (default `0 18 * * *`), `SCHEDULE_TZ` (default `Africa/Lagos`).

## Key Environment Variables

See `.env.example`. Pipeline vars: `QBO_CLIENT_ID`, `QBO_CLIENT_SECRET`, `EPOS_USERNAME`, `EPOS_PASSWORD`, `SLACK_WEBHOOK_URL`. Portal vars: `DJANGO_SECRET_KEY`, `DJANGO_DEBUG`, `DJANGO_ALLOWED_HOSTS`. Business-day tuning: `OIAT_BUSINESS_TIMEZONE` (default `Africa/Lagos`), `OIAT_BUSINESS_DAY_CUTOFF_HOUR` (default `5`).
