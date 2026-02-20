# OIAT Portal: Improvements & Tracking

This document records security hardenings, performance optimizations, and behavior changes made to the Django portal and related code, so the team can keep track of what was done and how to operate/deploy.

---

## 1. Django settings (security)

**Location:** `oiat_portal/settings.py`

**What changed:**
- `SECRET_KEY`, `DEBUG`, and `ALLOWED_HOSTS` are no longer hardcoded. They are driven by environment variables with safe defaults for local dev.
- When `DEBUG=False`, additional `SECURE_*` and cookie flags are applied (e.g. `SECURE_BROWSER_XSS_FILTER`, `SESSION_COOKIE_SECURE`).

**Environment variables:**

| Variable | When to set | Effect |
|----------|-------------|--------|
| `DJANGO_SECRET_KEY` | Production (required in prod) | Uses this as `SECRET_KEY`. If unset, falls back to `OIAT_DEV_SECRET_KEY` or a dev-only default. |
| `OIAT_DEV_SECRET_KEY` | Optional local override | Dev fallback when `DJANGO_SECRET_KEY` is not set. |
| `DJANGO_DEBUG` | Production: set to `0` or `false` | If **unset**, `DEBUG` defaults to **True** (runserver works with no env). If set to `1`/`true`/`yes`, DEBUG is True; otherwise False. |
| `DJANGO_ALLOWED_HOSTS` | Production (recommended) | Comma-separated list, e.g. `yourdomain.com,www.yourdomain.com`. When unset: if DEBUG=True then localhost + `*`; if DEBUG=False then `localhost`, `127.0.0.1`, `[::1]` so runserver still works. |
| `DJANGO_SECURE_SSL_REDIRECT` | Production behind HTTPS | Set to `1` or `true` to enable. |
| `DJANGO_SECURE_HSTS_SECONDS` | Optional HSTS | Integer; if > 0, HSTS headers are enabled. |

**Runserver:** With no env vars, `DEBUG=True` and `ALLOWED_HOSTS` includes `*`, so `manage.py runserver 0.0.0.0:8000` works without configuration.

---

## 2. Job dispatcher (no unbounded recursion)

**Location:** `apps/epos_qbo/services/job_runner.py`

**What changed:** `dispatch_next_queued_job()` no longer recurses on exception. It uses a bounded loop and stops after `DISPATCH_START_FAILURE_LIMIT` (5) consecutive start failures.

**Behavior:**
- Return values: `(RunJob, "started")`, `(None, "queued")`, `(None, "empty")`, or `(None, "start_failed")`.
- Callers that care about "start_failed" (e.g. background worker or tests) should handle this status.

**Constant:** `DISPATCH_START_FAILURE_LIMIT = 5` (can be tuned if needed).

---

## 3. Token manager (init once, optional batch load)

**Location:** `code_scripts/token_manager.py`

**What changed:**
- **Init once per process:** `ensure_db_initialized()` runs the SQLite DDL and chmod at most once per process. `load_tokens()` and `save_tokens()` call it instead of `_init_database()` directly, so repeated token reads no longer hit DDL on every call.
- **Batch load:** `load_tokens_batch(pairs: List[Tuple[str, str]])` returns a dict keyed by `(company_key, realm_id)` → token dict or None. Used by the overview and companies list to avoid N token DB round-trips per request.

**Usage:** For views that need token health for many companies, call `ensure_db_initialized()` once (or rely on the first `load_tokens`/`load_tokens_batch`), then use `load_tokens_batch([(c.company_key, realm_id), ...])` and pass results into `_company_token_health(company, tokens=...)`.

---

## 4. Login redirect (preserve query string)

**Location:** `apps/core/middleware.py`

**What changed:** The login redirect now uses `request.get_full_path()` (and URL-encodes it) for the `next` parameter, so query strings are preserved after login.

---

## 5. Logs page (fewer queries, cached counts)

**Location:** `apps/epos_qbo/views.py` (logs view)

**What changed:**
- Counts for 7d/30d (total, succeeded, failed) and active runs are computed once and stored in local variables; no repeated `.count()` on the same querysets.
- A single queryset is used for “active runs”; its result is reused for both the count and the list of active run IDs for polling.

---

## 6. Companies list & detail (batch prefetch, N+1 reduction)

**Location:** `apps/epos_qbo/views.py`

**What changed:**
- **`_batch_preload_companies_data(companies)`** (new): Batch-fetches per company_key — latest run, latest artifact, artifacts_today, latest_successful_artifact, and token info (via `load_tokens_batch` + `_company_token_health`). Returns maps keyed by company_key.
- **`_enrich_company_data(company, latest_run, preloaded=None)`:** New optional `preloaded` dict. When provided (e.g. from the batch preload), artifact and token data are taken from it instead of querying per company.
- **`companies_list`:** Uses the batch preload and passes `preloaded` into `_enrich_company_data` for each company, so the companies list view does a fixed number of queries instead of 1 + 4N.

**Note:** Company detail view still calls `_enrich_company_data(company, latest_run)` without `preloaded` (single-company, no batch).

---

## 7. Overview (token batch load + “last run” includes All Companies)

**Location:** `apps/epos_qbo/views.py` (overview view)

**What changed:**
- **Token batch:** Before the per-company loop, the view calls `ensure_db_initialized()`, builds a list of `(company_key, realm_id)` for companies that have a realm_id, and calls `load_tokens_batch(token_pairs)`. Each company then gets `_company_token_health(company, tokens=preloaded_tokens)` so token reads are batched in one connection instead of N.
- **“Last run” per company:** The overview now builds “latest run” per company the same way as the companies list and `_company_runs_queryset`: it includes not only RunJobs with `company_key=that company`, but also RunJobs that have at least one RunArtifact for that company (i.e. **All Companies** runs that processed that company). So when the scheduler runs “All Companies”, each company’s card shows the timestamp of that run (e.g. “12 hours ago”) instead of the older single-company run (e.g. “17 hours ago”).

---

## 8. Models: decimal money fields and indexes

**Location:** `apps/epos_qbo/models.py`

**What changed:**
- **RunArtifact:** `reconcile_difference`, `reconcile_epos_total`, and `reconcile_qbo_total` are now `DecimalField(max_digits=19, decimal_places=4)` instead of `FloatField`, to avoid float rounding drift in reconciliation and analytics.
- **RunJob:** Added indexes on `(status, -created_at)` and `(company_key, -created_at)`.
- **RunArtifact:** Added indexes on `(company_key, -processed_at)`, `(company_key, target_date)`, and `(company_key, reconcile_status)`.

**Migration:** `apps/epos_qbo/migrations/0004_decimal_and_indexes.py` — alter the three reconcile fields and create the indexes. Apply with `manage.py migrate epos_qbo`.

**Compatibility:** Existing code that assigns floats to these fields or compares/uses them in arithmetic continues to work; Django and the existing `_to_decimal` in metrics handle conversion.

---

## 9. Auth test fix (correct patch target)

**Location:** `apps/epos_qbo/tests/test_auth_and_permissions.py`

**What changed:** The test that mocks “start run job” now patches `apps.epos_qbo.services.job_runner.start_run_job` instead of `apps.epos_qbo.views.start_run_job` (which does not exist). This fixes the AttributeError and allows the test to pass when the dispatcher runs.

---

## 10. Running tests

**Recommendation:** Run the Django test suite with the **project virtualenv** so dependencies and Django version match:

- Windows: `.venv\Scripts\python.exe manage.py test apps.epos_qbo`
- macOS/Linux: `.venv/bin/python manage.py test apps.epos_qbo`

Some tests may still fail with Django’s template context copying under Python 3.14 (`'super' object has no attribute 'dicts'`); that is an environment/Django compatibility issue, not from the portal changes above.

---

## 11. Overview “Last successful sync” and metrics line

**Location:** `apps/epos_qbo/views.py` — `resolve_overview_target_date()`

**What changed:**
- **Target date / has_data:** Still from the latest RunArtifact (succeeded) when present.
- **Last successful sync timestamp:** Now the **max** of (1) latest artifact’s `processed_at`/`imported_at` and (2) latest **succeeded** RunJob’s `finished_at` (any scope). So the subtitle “Last successful sync X ago” reflects the real last run even if artifact ingest is delayed.
- When there are no artifacts yet, the view uses the latest succeeded RunJob’s `finished_at` for the subtitle so “X ago” is correct right after an All Companies run.

---

## 12. Companies list: latest run includes All Companies runs

**Location:** `apps/epos_qbo/views.py` — `_batch_preload_companies_data()`

**What changed:** “Latest run” per company is built the same way as the overview: include RunJobs that have a RunArtifact for that company (so **All Companies** runs count). Order by `-finished_at`, `-started_at`, `-created_at`. So the companies list “last run” and the “No sync in X hours” stale warning use the same run as the overview (e.g. Feb 15 17:00), and the incorrect “49 hours” warning goes away when the last run was recent.

---

## 13. Issue styling (amber/red when issues exist)

**Locations:** `apps/epos_qbo/views.py`, `apps/epos_qbo/templates/components/company_cards.html`, `apps/epos_qbo/templates/epos_qbo/company_detail.html`

**What changed:**
- **View:** `_enrich_company_data()` now sets **`issues_highest_severity`**: `"critical"` if any issue has severity `red`, else `"warning"` if any has `amber`, else `None`.
- **Company cards (list):** The “X issue(s)” line and icon use `issues_highest_severity` for color (red or amber), not `company_data.status.color`, so issues are never shown in green.
- **Company detail:** The issues box (border, icon, heading, list text) uses `issues_highest_severity` (red or amber). So “1 issue” and “No sync in 49 hours” are clearly visible as warnings, not green.

---

## 14. Company detail: unified “latest run” and Sales Synced (last run)

**Locations:** `apps/epos_qbo/views.py`, `apps/epos_qbo/templates/epos_qbo/company_detail.html`, `apps/epos_qbo/tests/test_company_run_activity.py`

**What changed:**

- **Unified “latest run” ordering:** Company detail now uses the same ordering as overview and companies list: **`-finished_at`, `-started_at`, `-created_at`**. Added **`_company_runs_queryset_ordered_by_latest(company_key)`** so the canonical ordering lives in one place. Comments at overview and batch-preload call sites note they must match. So “latest run” and “recent runs” on the detail page match the run shown on Overview and Companies list.
- **Sales Synced card:** The company detail card no longer shows “SALES SYNCED (7D)” with the 7-day trend. It now shows **“Sales Synced (last run)”** with the **monetary total from the latest successful run’s artifact** (via `extract_amount_hybrid(..., prefer_reconcile=True)` and `_format_currency`). Subtext is “Target: M j, Y” when there is a target date, or “No successful run yet” when there is no successful artifact.

---

## Quick reference: production checklist

- Set `DJANGO_SECRET_KEY` to a long random value.
- Set `DJANGO_DEBUG=0` (or `false`).
- Set `DJANGO_ALLOWED_HOSTS` to your host(s), comma-separated.
- Optionally set `DJANGO_SECURE_SSL_REDIRECT=1` and `DJANGO_SECURE_HSTS_SECONDS` if served over HTTPS.

---

*Last updated to reflect overview last-successful-sync, companies list latest-run logic, issue styling by severity, company detail unified latest-run ordering and Sales Synced (last run) card.*
