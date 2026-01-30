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

   > **Note:** `--skip-download` only works in range mode and uses existing split files from `uploads/range_raw/`. Useful when you already have CSV files and want to reprocess without re-downloading from EPOS.

That's it! The pipeline will download, split, transform, upload, archive, and reconcile automatically. If `SLACK_WEBHOOK_URL` is configured, you'll receive notifications for pipeline start, success, failure events, and reconciliation results.

> 💡 **Tip:** See [Initial Setup](#initial-setup) below for detailed instructions on each step.
>
> **Note:** All examples use `python` for cross-platform compatibility. On macOS/Linux, use `python3` if `python` points to Python 2 or is missing.

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

- `test/run_test_upload.py`  
  Test upload flow for a single raw CSV (no EPOS download): runs transform, QBO upload, reconciliation, and optionally archive. Use for validating transform output and item resolution without running the full pipeline. Run from repo root.

  **Usage:**

  ```bash
  python test/run_test_upload.py --company company_a --target-date 2026-01-28
  # With a specific raw CSV file:
  python test/run_test_upload.py --company company_a --csv path/to/BookKeeping_2026_01_29_1911.csv
  ```

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
- **Success:** All phases completed with summary
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
- Do not commit tokens or the database file
- The script automatically loads the `realm_id` from your company configuration file
- Optional: You can use a GUI tool like [DB Browser for SQLite](https://sqlitebrowser.org/) to view the database contents (useful for debugging or verifying stored tokens)

**Adding a second company:** Simply run the OAuth flow again for the new company and store tokens using the same script with the new company's `--company` argument. The SQLite database stores tokens separately per company.

### 3. QBO query scripts

Ad-hoc query and debug scripts live in `scripts/qbo_queries/`. Run from repo root with `--company` (required). See `scripts/qbo_queries/README.md` for the full list.

| Script | Purpose |
|--------|---------|
| `qbo_query.py` | Run arbitrary QBO SQL-like query |
| `qbo_item_lookup_by_name.py` | Look up Item by exact Name |
| `qbo_item_get_by_id.py` | Get Item by Id or search by Name |
| `qbo_account_query.py` | Run Account queries (name-based) |
| `qbo_check_inventory_start_dates.py` | List Inventory items with InvStartDate after cutoff |
| `qbo_verify_mapping_accounts.py` | Verify Product.Mapping.csv accounts exist in QBO |

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
# Install pre-commit (or use requirements-dev.txt)
pip install -r requirements-dev.txt
# OR: pip install pre-commit

# Install the git hooks
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
    "inventory_start_date": "today",
    "default_qty_on_hand": 0,
    "product_mapping_file": "mappings/Product.Mapping.csv"
  }
}
```

**Fields:**
- `enable_inventory_items`: Enable inventory item creation (default: `false`)
- `allow_negative_inventory`: Allow negative inventory when posting SalesReceipts (default: `false`)
- `inventory_start_date`: Start date for inventory tracking - use `"today"` or ISO date like `"2026-01-26"` (default: `"today"`)
- `default_qty_on_hand`: Starting quantity for new inventory items (default: `0`)
- `product_mapping_file`: Path to category mapping CSV (default: `"mappings/Product.Mapping.csv"`)

### Environment Variable Overrides

Precedence: **ENV → company JSON → defaults**

You can override inventory settings via environment variables:

```bash
COMPANY_A_ENABLE_INVENTORY_ITEMS=true
COMPANY_A_ALLOW_NEGATIVE_INVENTORY=true
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

**When `enable_inventory_items` is `false` (default):**
- Missing products are created as **Service items** (existing behavior)
- No account mapping required
- No inventory tracking

**Negative Inventory Handling:**
- If `allow_negative_inventory` is `true` and QBO accepts the SalesReceipt (with warnings), the pipeline continues and logs a warning
- If QBO rejects due to inventory, the pipeline fails with instructions to enable negative inventory in QBO settings
- If `allow_negative_inventory` is `false`, inventory errors are treated as fatal (existing behavior)

### Pre-flight: Inventory Start Date Check (QBO 6270)

When inventory is enabled and a run has a target date, the upload step runs a **non-blocking** pre-flight check before sending SalesReceipts:

- It queries QBO for Inventory items whose **InvStartDate** is **after** the run’s target date.
- Such items can cause QBO error **6270** (“Transaction date is prior to start date for inventory item”) when posting backdated receipts.
- If any are found:
  - A **warning** is logged with the count.
  - A CSV report is written to `reports/inventory_start_date_issues_{company_key}_{target_date}.csv` (columns: Id, Name, InvStartDate, Active).
- The run **does not fail**; this is reporting-only. Fix items in QBO (edit InvStartDate or delete/re-upload) and re-run as needed.

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
2. Run test upload: `python test/run_test_upload.py --company company_a --target-date 2026-01-28`
   - Logs should show `unit_sales_price_gross`, `unit_purchase_cost_gross` (e.g. 329.46) and, if patching, `PurchaseCost:0->329.46` (or similar).
3. In QBO: Product/Service → open Inventory item → **Purchasing** tab: Purchase cost should match Cost Price / qty (e.g. 329.46). **Sales** tab: Price/rate should match TOTAL Sales / qty (e.g. 500.00) with "Price is inclusive of sales tax" reflected.

---

## Troubleshooting

### QBO query and item gotchas

- **SubItem cannot be selected in some QBO UI dropdowns:** When using item hierarchy (SubItem + ParentRef), sub-items may not appear as selectable in every QBO screen (e.g. when picking an item for a transaction). Use the **parent category** or search by name where supported. The API accepts sub-items for Sales Receipt lines; the limitation is UI-only.
- **"Category:Product" display on Sales Receipts:** When `use_item_hierarchy` is true, QuickBooks displays the **FullyQualifiedName** (e.g. `Category Name:Product Name`) in the Product/Service column. This is expected and not a bug; we are not changing this behavior.

### Logs to look for

- **Mapping loaded:** `[INFO] Loaded N category mappings from mappings/Product.Mapping.csv`
- **Item resolution summary:** `[INFO] Item resolution summary: total_lines=... unique_items=... items_created=... items_patched=... item_lookups_from_prefetch=...`
- **Items created/patched:** `[INFO] Patched Inventory item fields: Id=... UnitPrice:...->... PurchaseCost:...->...` and `[INFO] Attached ParentRef/SubItem to Inventory item '...'`
- **Pre-flight (inventory start date):** `[WARN] Found N inventory items with InvStartDate AFTER target_date=...` and report path if any

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
- **Tokens:** `qbo_tokens.sqlite` is auto-created with restricted permissions (0o600)
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
