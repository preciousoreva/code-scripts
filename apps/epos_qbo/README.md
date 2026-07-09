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
- `GET /epos-qbo/schedules/`: schedule manager page
- `POST /epos-qbo/schedules/create`: create DB schedule
- `POST /epos-qbo/schedules/<uuid:schedule_id>/update`: update DB schedule
- `POST /epos-qbo/schedules/<uuid:schedule_id>/toggle`: enable/disable schedule
- `POST /epos-qbo/schedules/<uuid:schedule_id>/run-now`: enqueue run immediately from schedule
- `POST /epos-qbo/schedules/<uuid:schedule_id>/delete`: delete schedule (non-system schedules only)
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
- `epos_qbo.can_manage_schedules`: required for schedule management and run-now actions.
- `epos_qbo.can_manage_portal_settings`: required for editing portal-wide defaults on Settings.

## Core models

- `CompanyConfigRecord`: canonical company configuration in DB.
- `RunJob`: requested/queued/running/completed run lifecycle record.
- `RunArtifact`: ingested metadata artifacts linked to runs and companies.
- `RunLock`: DB lock row used to serialize run dispatch.
- `RunSchedule`: DB-backed cron schedule definition.
- `RunScheduleEvent`: scheduler event/audit stream.

## Current behavior highlights

- Overview company search is client-side and matches `display_name + company_key` via `static/js/overview.js`.
- Overview KPI date basis uses canonical **target trading date** from business settings (`OIAT_BUSINESS_TIMEZONE` + `OIAT_BUSINESS_DAY_CUTOFF_HOUR/MINUTE`). Run Success, Avg Runtime, and Sales Synced are all computed against that same target date for consistent cross-timezone operator views.
- Company list shows **"N receipts from latest sync (date)"** or **"No sync yet"**, using the latest **successful** run artifact per company (same narrative as overview target date). Receipt count and target date come from `RunArtifact` with `run_job__status=SUCCEEDED`; this aligns the Companies page with the overview’s "last successful sync" data.
- Overview **Live Log** shows the last **10** run events (by `created_at`), one entry per run.
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

## Run lifecycle UI refresh system

- Shared run-event orchestration is centralized in `static/js/run_reactivity.js` via `window.OiatRunReactivity.bindRunLifecycleRefresh(...)`.
- Run events are emitted from `static/js/toasts.js`:
  - `oiat:run-started` when newly active runs are discovered.
  - `oiat:run-completed` on terminal statuses (`succeeded`, `failed`, `cancelled`), including first-observation terminal races.
- Base template loads scripts in this order:
  1. `static/js/charts.js`
  2. `static/js/run_reactivity.js`
  3. `static/js/toasts.js`
- Page usage:
  - `overview.js`: panel partial refresh with completion retries, preserving revenue/company filters.
  - `runs.js`, `logs.js`, `company_detail.js`: full-page reload strategy.
  - `companies.js`: targeted refresh of summary cards + company list using current filter/search/sort state.
  - `run_detail.js`: refreshes only for the currently viewed active run ID.
- Run detail polling guard:
  - `toasts.js` only auto-tracks `[data-run-id]` when `data-run-status` is `queued|running` to avoid terminal run reload loops.

## Run execution and locking

- `services/job_runner.py` builds subprocess commands for:
  - `code_scripts/run_pipeline.py` (single company)
  - `code_scripts/run_all_companies.py` (all companies)
- `code_scripts/run_pipeline.py` now persists reconciliation payload (`status`, totals, counts, difference) into metadata before archive move.
- Dispatch is serialized through `RunLock` and post-exit reconciliation releases lock and attaches artifacts.
- DB schedules enqueue `RunJob` through `services/schedule_worker.py` and reuse the same dispatcher/lock path as manual dashboard runs.
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
- `python manage.py run_schedule_worker [--poll-seconds N] [--once]`
  - runs DB schedule evaluation loop, enqueues due jobs, supports env fallback schedule when enabled.

## Tests

- App tests live under `apps/epos_qbo/tests/`.
- These are automated checks for view logic, auth/permissions, token-health mapping, run orchestration behavior, and rendered UI contracts.
- They are not runtime page scripts and are not loaded by the browser.

## UI notes

- Sidebar entries `Tools` and `API Tokens` route to shared placeholder pages (`/coming-soon/<feature>/`) and are marked `Coming Soon`.
- Static JS (overview, companies, toasts) uses paths under `/epos-qbo/`. If the app is mounted at a different URL prefix, update `OVERVIEW_PANELS_URL` in `overview.js`, `currentCompaniesUrl()` in `companies.js`, and `ACTIVE_RUNS_URL` / `RUN_STATUS_URL_BASE` / toast links in `toasts.js`.
- Tailwind is now served as compiled static CSS (`apps/epos_qbo/static/css/tailwind.css`) instead of runtime CDN injection.

## Frontend CSS build

- Tailwind config: `tailwind.config.cjs`
- Source input: `apps/epos_qbo/static_src/css/tailwind.input.css`
- Generated output: `apps/epos_qbo/static/css/tailwind.css`
- Build command: `npm run build:css`
- Watch mode: `npm run watch:css`
- Content scanning includes `apps/**/*.html`, `apps/**/*.js`, and `templates/**/*.html` so utility classes in Django templates and JS are retained.
- The built file `static/css/tailwind.css` is committed so the app works after clone without running Node. Run `npm run build:css` after changing Tailwind source or adding new templates/classes; run `npm run watch:css` during development.
- Core app templates (login, home, coming_soon) use `{% static 'css/tailwind.css' %}`; Django serves it from `epos_qbo/static/`.

## Remote SSH to Windows dev PC

When you work on the repo via remote SSH (e.g. Cursor Remote-SSH) to your Windows machine:

- **Python / Django:** Use the Windows venv. In **PowerShell/cmd** (Windows): `.venv\Scripts\python.exe`. In **WSL/bash** (Linux): use forward slashes — `.venv/Scripts/python.exe` — because backslashes are escape characters in bash and break the path.
- **Tailwind (optional):** The compiled `static/css/tailwind.css` is committed, so the dashboard works without running Node. To rebuild CSS after changing templates or Tailwind source:
  - From the repo root run `npm install` (once), then `npm run build:css`. The scripts use `npx tailwindcss` so the local CLI is used and paths with spaces (e.g. `Developer Projects`) work on Windows.
  - If you use WSL for the repo, run `npm run build:css` from the repo root in WSL; Node/npm should be installed in WSL.
- **Quick sanity check:** From repo root, `python manage.py check` (using your venv Python) and a quick click-through of the dashboard in the browser.

## Dashboard tuning (settings/env)

- `OIAT_DASHBOARD_DEFAULT_PARALLEL` (default `2`)
- `OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS` (default `2`)
- `OIAT_DASHBOARD_STALE_HOURS_WARNING` (default `48`)
- `OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS` (default `7`)
- `OIAT_DASHBOARD_RECON_DIFF_WARNING` (default `1.0`)
- `OIAT_DASHBOARD_REAUTH_GUIDANCE` (default points to `code_scripts/store_tokens.py` guidance text)

## Template formatting rule

Keep each Django variable tag on one line, for example `{{ value|default:"-" }}`.
Do not wrap text inside `{{ ... }}` across lines.
