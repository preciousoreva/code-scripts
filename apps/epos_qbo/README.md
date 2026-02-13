# EPOS → QBO Solution App (`apps.epos_qbo`)

This app is the EPOS→QBO solution module inside the OIAT portal.

## Routes

- `/epos-qbo/` and `/epos-qbo/dashboard/`: overview and KPIs
- `/epos-qbo/runs/`: run history + manual trigger form
- `/epos-qbo/runs/<uuid>/`: run detail page
- `/epos-qbo/runs/<uuid>/logs?offset=<int>`: polling log tail endpoint
- `/epos-qbo/companies/new`: onboarding step 1
- `/epos-qbo/companies/<company_key>/advanced`: onboarding step 2
- `/epos-qbo/companies/<company_key>/sync-json`: force DB→JSON sync

## Models

- `CompanyConfigRecord`: DB-canonical company configuration
- `RunJob`: run request + execution lifecycle
- `RunArtifact`: ingested pipeline artifacts/metadata
- `RunLock`: dashboard lock row for atomic trigger gating

## Management commands

- `python manage.py reconcile_run_jobs`
- `python manage.py ingest_run_history --days 60`
- `python manage.py sync_companies_from_json`
- `python manage.py sync_companies_to_json --company <key>`
- `python manage.py check_company_config_drift`

## Notes

- Dashboard uses DB lock (`RunLock`) and scripts use shared global file lock (`code_scripts/run_lock.py`) for cross-trigger exclusion.
- Scheduled automation remains external; this app observes and safely triggers existing scripts.
