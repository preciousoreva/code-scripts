# EPOS -> QBO Solution App (`apps/epos_qbo`)

This Django app provides the operator UI and orchestration layer for EPOS -> QBO sync.
It does not replace the pipeline scripts in `code_scripts/`; it wraps and monitors them.

## Route map

- `GET /epos-qbo/` and `GET /epos-qbo/dashboard/`: system overview (KPIs, company status, live log, run reliability)
- `GET /epos-qbo/runs/`: run list + manual trigger form
- `POST /epos-qbo/runs/trigger`: trigger a run job
- `GET /epos-qbo/runs/<uuid:job_id>/`: run detail
- `GET /epos-qbo/runs/<uuid:job_id>/logs?offset=<int>`: log tail polling endpoint
- `GET /epos-qbo/api/runs/status?job_ids=<uuid,...>`: run status polling for toasts/live state
- `GET /epos-qbo/logs/`: structured log/events page with filters
- `GET /epos-qbo/companies/`: company management page (search/filter/sort)
- `GET /epos-qbo/companies/new`: company onboarding (basic form)
- `GET|POST /epos-qbo/companies/<company_key>/advanced`: advanced company settings
- `POST /epos-qbo/companies/<company_key>/sync-json`: force DB -> JSON config sync
- `GET /epos-qbo/companies/<company_key>/`: company detail page
- `POST /epos-qbo/companies/<company_key>/toggle-active/`: activate/deactivate company

All routes require authentication.

## Permissions

- `epos_qbo.can_trigger_runs`: required for triggering runs.
- `epos_qbo.can_edit_companies`: required for creating/editing companies, syncing JSON, and toggling active state.

## Core models

- `CompanyConfigRecord`: canonical company configuration in DB.
- `RunJob`: requested/queued/running/completed run lifecycle record.
- `RunArtifact`: ingested metadata artifacts linked to runs and companies.
- `RunLock`: DB lock row used to serialize run dispatch.

## Current behavior highlights

- Overview company search is client-side and matches `display_name + company_key` via `static/js/overview.js`.
- Overview live log messages use company + run label format (not raw UUID in message text).
- Overview reconciled revenue chart (under Company Status):
  - Shows EPOS money trend per company for `Yesterday`, `Last 7D`, `Last 30D`, `Last 90D` (default `Last 7D`).
  - Supports chart-level filter for `All Companies` or a single company.
  - Uses strict `MATCH` reconciliation data only (`RunArtifact.reconcile_status == "MATCH"` and `reconcile_epos_total` present).
  - Dedupe rule per point: latest artifact by `(company_key, target_date)` using `processed_at/imported_at`.
  - Uses completed-day windows ending at yesterday (future-only strict; older runs without stored reconcile totals may not appear).
- Exit codes are now explained in Run Detail with this reference:
  - `0`: success
  - `1`: pipeline execution failure (generic; inspect Live Log for root cause)
  - `2`: blocked by active lock or invalid CLI usage
  - `3`: dashboard failed to start subprocess
  - `-1`: reconciler marked stale PID as failed
  - `126`: command exists but is not executable
  - `127`: command/dependency not found
- Token health is read-only in page render and based on canonical states from `views._company_token_health`:
  - `connected`, `missing_tokens`, `missing_refresh_token`, `refresh_expired`, `refresh_expiring`
  - Access token state is informational; critical auth state is driven by refresh-token availability/expiry.
  - Re-auth guidance points operators to `code_scripts/store_tokens.py`.

## Run execution and locking

- `services/job_runner.py` builds subprocess commands for:
  - `code_scripts/run_pipeline.py` (single company)
  - `code_scripts/run_all_companies.py` (all companies)
- `code_scripts/run_pipeline.py` now persists reconciliation payload (`status`, totals, counts, difference) into metadata before archive move.
- Dispatch is serialized through `RunLock` and post-exit reconciliation releases lock and attaches artifacts.
- Polling helpers:
  - `run_logs` streams log chunks by byte offset.
  - `run_status_check` returns compact JSON status for active runs.

## Management commands

- `python manage.py reconcile_run_jobs`
  - marks stuck running jobs as failed when PID is no longer alive.
- `python manage.py ingest_run_history --days 60`
  - ingests artifact history from uploaded metadata files.
- `python manage.py sync_companies_from_json`
  - imports `code_scripts/companies/*.json` into DB.
- `python manage.py sync_companies_to_json --company <key>`
  - writes DB company config back to JSON files.
- `python manage.py check_company_config_drift`
  - compares DB payloads vs JSON files and exits non-zero when drift exists.

## Tests

- App tests live under `apps/epos_qbo/tests/`.
- These are automated checks for view logic, auth/permissions, token-health mapping, run orchestration behavior, and rendered UI contracts.
- They are not runtime page scripts and are not loaded by the browser.

## UI notes

- Sidebar entries `Mappings`, `Settings`, and `API Tokens` route to shared placeholder pages (`/coming-soon/<feature>/`) and are marked `Coming Soon`.

## Dashboard tuning (settings/env)

- `OIAT_DASHBOARD_DEFAULT_PARALLEL` (default `2`)
- `OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS` (default `2`)
- `OIAT_DASHBOARD_STALE_HOURS_WARNING` (default `48`)
- `OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS` (default `7`)
- `OIAT_DASHBOARD_REAUTH_GUIDANCE` (default points to `code_scripts/store_tokens.py` guidance text)

## Template formatting rule

Keep each Django variable tag on one line, for example `{{ value|default:"-" }}`.
Do not wrap text inside `{{ ... }}` across lines.
