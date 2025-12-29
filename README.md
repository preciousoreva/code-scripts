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
   python3 run_pipeline.py --company company_a
   ```

   **Specific date:**

   ```bash
   python3 run_pipeline.py --company company_a --target-date 2025-12-24
   ```

   **Custom date range:**

   ```bash
   python3 run_pipeline.py --company company_b --from-date 2025-12-01 --to-date 2025-12-05
   ```

That's it! The pipeline will download, split, transform, upload, archive, and reconcile automatically. If `SLACK_WEBHOOK_URL` is configured, you'll receive notifications for pipeline start, success, failure events, and reconciliation results.

> 💡 **Tip:** See [Initial Setup](#initial-setup) below for detailed instructions on each step.

---

## Architecture Overview

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
  python3 run_pipeline.py --company company_a

  # Single-day (specific date)
  python3 run_pipeline.py --company company_a --target-date 2025-12-24

  # Date range
  python3 run_pipeline.py --company company_b --from-date 2025-12-01 --to-date 2025-12-05
  ```

- `epos_playwright.py`  
  Uses **Playwright** to log into EPOS Now, navigate to the BookKeeping report, and download the CSV.
  Supports both single-date (`--target-date`) and range (`--from-date` / `--to-date`) downloads.

- `transform.py`  
  Transforms raw EPOS CSV into QuickBooks-ready format using company-specific configuration.

  **Important:** Transform.py does NOT create or merge spill files. It receives a pre-filtered raw file via `--raw-file` and transforms only that data.

- `qbo_upload.py`  
  Uploads transformed CSV to QuickBooks Online via REST API.

  **Features:**

  - **Deduplication (Layer A)**: Local ledger tracks uploaded DocNumbers
  - **Deduplication (Layer B)**: Bulk QBO API checks before uploading
  - Automatic token refresh on 401 errors
  - Location/Department mapping
  - VAT-inclusive amount handling

### Configuration

- `company_config.py` — Loads company-specific settings from JSON files
- `companies/company_a.json` — Company A configuration
- `companies/company_b.json` — Company B configuration

### Supporting Files

- `token_manager.py` — QuickBooks OAuth2 token management (SQLite storage, per-company tokens)
- `query_qbo_for_company.py` — QuickBooks query/reconciliation tool
- `slack_notify.py` — Slack notification helpers
- `load_env.py` — Environment variable loader

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
python3 run_pipeline.py --company company_a --target-date 2025-12-28
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
python3 run_pipeline.py --company company_b --from-date 2025-12-26 --to-date 2025-12-28
```

**Flow:**

1. **Download:** EPOS CSV for full range → repo root
2. **Split:** By WAT date (all days)
   - Rows for 2025-12-26 → `uploads/range_raw/.../BookKeeping_2025-12-26.csv`
   - Rows for 2025-12-27 → `uploads/range_raw/.../BookKeeping_2025-12-27.csv`
   - Rows for 2025-12-28 → `uploads/range_raw/.../BookKeeping_2025-12-28.csv`
   - Rows for 2025-12-29 → `uploads/spill_raw/.../BookKeeping_raw_spill_2025-12-29.csv`
3. **Loop per day:** For each day in range:
   - Check/merge RAW spill
   - Transform
   - Upload
   - Archive
4. **Final archive:** Archive range staging folder and original CSV

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
- **Success:** All phases completed with summary
- **Failure:** Critical error with concise reason

**Watchdog messages include:**

- Future RAW spill creation: `"Future raw spill: 2025-12-29 (23 rows)"`
- RAW spill merge: `"2025-12-28: merged target split (480 rows) + raw spill (23 rows) -> final (503 rows)"`

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
2. Store tokens using `token_manager.store_tokens_from_oauth()`:

```python
from token_manager import store_tokens_from_oauth
from company_config import load_company_config

# Load company config to get realm_id
config = load_company_config("company_a")  # or "company_b"

# Store tokens from OAuth response
store_tokens_from_oauth(
    company_key=config.company_key,
    realm_id=config.realm_id,
    access_token="your_access_token",
    refresh_token="your_refresh_token",
    expires_in=3600,  # seconds
    environment="production"  # or "sandbox"
)
```

**Alternative:** Create a simple script to store tokens:

```python
# store_tokens.py
import sys
from token_manager import store_tokens_from_oauth
from company_config import load_company_config

if len(sys.argv) < 5:
    print("Usage: python store_tokens.py <company_key> <access_token> <refresh_token> <expires_in>")
    sys.exit(1)

company_key = sys.argv[1]
config = load_company_config(company_key)

store_tokens_from_oauth(
    company_key=company_key,
    realm_id=config.realm_id,
    access_token=sys.argv[2],
    refresh_token=sys.argv[3],
    expires_in=int(sys.argv[4])
)

print(f"Tokens stored for {company_key} (realm_id: {config.realm_id})")
```

**Adding a second company:** Simply run the OAuth flow again for the new company and store tokens using the same function with the new company's `company_key` and `realm_id`. The SQLite database stores tokens separately per company.

### 3. Verify .gitignore

Ensure these are ignored:

- `qbo_tokens.sqlite` — OAuth tokens database (SQLite)
- `*.sqlite-wal`, `*.sqlite-shm` — SQLite sidecar files
- `.env` — Credentials
- `uploads/` — Temporary staging
- `Uploaded/` — Archive
- `logs/` — Execution logs
- `*.csv` — Processing files

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
   python3 run_pipeline.py --company company_c --target-date 2025-01-01
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

## Troubleshooting

### RAW Spill Not Being Merged

- Verify spill file exists: `uploads/spill_raw/<CompanyDir>/BookKeeping_raw_spill_YYYY-MM-DD.csv`
- File name must match target date exactly
- Check logs for "Found raw spill file for..." message

### Duplicate Sales Receipts

The pipeline includes automatic deduplication:

- **Layer A:** Local ledger (`uploaded_docnumbers.json`)
- **Layer B:** Bulk QBO API check before upload

If you need to re-upload, delete existing receipts first using `query_qbo_for_company.py`.

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
- **Tokens:** `qbo_tokens.sqlite` is auto-created with restricted permissions (0o600)
- **Git:** `.gitignore` excludes all sensitive files (including `qbo_tokens.sqlite` and SQLite sidecar files)
- **Production:** Use a secrets manager (AWS Secrets Manager, HashiCorp Vault)

---

## Design Notes

### Why Transformed Spill Logic Was Removed

The original design had `transform.py` creating "transformed spill" files for non-target-date rows. This was removed because:

1. **Double-handling risk:** Rows could be processed twice (once in spill, once in target date)
2. **Complexity:** Spill merging logic in transform made it stateful and harder to debug
3. **Date accounting:** Hard to track which rows were processed when

The RAW-first approach is simpler: split at the raw level, merge at the raw level, transform only sees target-date rows.

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
