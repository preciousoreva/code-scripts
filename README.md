# EPOS → QuickBooks Automation

This repo contains a small automation pipeline that:

1. Logs into **EPOS Now HQ** and downloads the daily **BookKeeping** CSV to the repo root.
2. Transforms the raw EPOS export into a single QuickBooks‑ready CSV in the repo root.
3. Uploads the data into **QuickBooks Online** as Sales Receipts using the QBO API.
4. Archives processed files to `Uploaded/<date>/` after successful upload.
5. Reconciles EPOS totals vs QBO totals to verify data integrity.

The pipeline is designed to be run as a single command and take care of all five phases in sequence.

---

## TL;DR – Quick Start

1. **Set up credentials:**

   ```bash
   cp .env.example .env
   # Edit .env and fill in your QBO_CLIENT_ID, QBO_CLIENT_SECRET, EPOS_USERNAME, EPOS_PASSWORD
   ```

2. **Create initial OAuth tokens:**

   - Perform OAuth flow to get access/refresh tokens
   - Create `qbo_tokens.json` with your tokens (see [Initial Setup](#3-get-initial-oauth-tokens) for details)

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
   python3 run_pipeline.py
   ```

   **Specific date:**

   ```bash
   python3 run_pipeline.py --target-date 2025-12-24
   ```

   **Custom date range:**

   ```bash
   python3 run_pipeline_custom.py --from-date 2025-12-01 --to-date 2025-12-05
   ```

   **Custom date range with target-date filtering:**

   ```bash
   python3 run_pipeline_custom.py --from-date 2025-12-24 --to-date 2025-12-26 --target-date 2025-12-25
   ```

That's it! The pipeline will download, transform, upload, archive, and reconcile automatically. If `SLACK_WEBHOOK_URL` is configured, you'll receive notifications for pipeline start, success, failure events, and reconciliation results.

> 💡 **Tip:** See [Initial Setup](#initial-setup) below for detailed instructions on each step.

---

## Files / Scripts

### Core Pipeline Scripts

- `run_pipeline.py`  
  **Main entry point** — Orchestration script that runs all five phases in order for a target business date:

  1. Download EPOS CSV (`epos_playwright.py`) - downloads data for target date (defaults to yesterday)
  2. Transform to single CSV (`epos_to_qb_single.py`) - filters rows by target date and handles spillover
  3. Upload to QuickBooks (`qbo_upload.py`) - with deduplication to prevent duplicate entries
  4. Archive files (`run_pipeline.py` - Phase 4) - archives processed files and used spill files
  5. Reconcile EPOS vs QBO totals (`qbo_query.py` - Phase 5)

  **Features:**
  - `--target-date YYYY-MM-DD`: Process a specific business date (defaults to yesterday if not provided)
  - **Multi-date spillover handling**: Automatically separates rows from other dates into spill files
  - **Deduplication**: Prevents duplicate uploads using local ledger and QBO API checks
  - Sends Slack notifications with detailed summary (target date, dates present, row counts, upload stats)
  
  Returns exit code 0 on success, 1 on failure.

- `run_pipeline_custom.py`  
  **Custom date range pipeline** — Same as `run_pipeline.py` but allows you to specify a custom date range:

  ```bash
  python3 run_pipeline_custom.py --from-date 2025-12-01 --to-date 2025-12-05
  ```

  Downloads EPOS data for the specified date range. Also supports optional `--target-date` filtering:
  
  ```bash
  python3 run_pipeline_custom.py --from-date 2025-12-24 --to-date 2025-12-26 --target-date 2025-12-25
  ```
  
  **Features:**
  - Downloads data for the specified date range
  - Optional `--target-date`: Filter to process only a specific date within the range (useful for handling spillover)
  - Sends Slack notifications with the date range included
  - **Archive folders:** Files are archived to `Uploaded/<date_range>/` (e.g., `Uploaded/2025-12-01 to 2025-12-05/`) instead of a single date folder

- `epos_playwright.py`  
  Uses **Playwright** to log into EPOS Now, navigate to the BookKeeping report, and download the CSV directly to the repo root directory.  
  Supports `--target-date YYYY-MM-DD` parameter to download data for a specific date (defaults to yesterday if not provided).  
  **Note:** EPOS credentials are loaded from `.env` file or environment variables (see [Initial Setup](#2-configure-credentials-required)).

- `epos_playwright_custom.py`  
  Custom date range version of `epos_playwright.py`. Accepts `--from-date` and `--to-date` arguments to download data for a specific date range.  
  Automatically navigates the calendar to the correct month and selects the specified dates.

- `epos_to_qb_single.py`  
  Reads the latest raw CSV from the repo root and transforms it into a single consolidated CSV for QuickBooks import.  
  **Features:**
  - `--target-date YYYY-MM-DD`: Filters rows to only process the target business date (in WAT timezone)
  - **Multi-date spillover handling**: Automatically detects and separates rows from other dates into spill files in `uploads/spill/`
  - **Spill file merging**: When processing a date, automatically checks for and merges existing spill files for that date
  - Output is written to `single_sales_receipts_*.csv` in the repo root
  - Creates `last_epos_transform.json` metadata file with file paths, target date, dates present, row counts, and spill file information
  
  **Dependency:** Uses `sales_recepit_script.py` for transformation logic.

- `qbo_upload.py`  
  Reads the latest `single_sales_receipts_*.csv` from the repo root and uses the **QuickBooks Online REST API** to create Sales Receipts.  
  Each `*SalesReceiptNo` group becomes one Sales Receipt, with the tender type stored in the memo and the **Payment method** automatically mapped from the memo.  
  **Features:**
  - **Deduplication (Layer A)**: Local ledger (`uploaded_docnumbers.json`) tracks successfully uploaded DocNumbers to prevent re-uploads
  - **Deduplication (Layer B)**: Bulk QBO API queries check for existing SalesReceipts by DocNumber before uploading
  - Automatically refreshes expired access tokens on 401 errors
  - Maps **Location** data from CSV to QuickBooks **Departments** (shown as "Location" in QBO UI)
  - Auto-creates missing Items if `AUTO_CREATE_ITEMS = True`
  - Treats EPOS line amounts as **VAT-inclusive** and sends both **net** and **gross** values to QBO so that:
    - Subtotal and Total in QBO stay equal to the EPOS gross total
    - QBO *backs out* VAT for display using `GlobalTaxCalculation = "TaxInclusive"` and `TaxInclusiveAmt`
  - Validates API responses and raises errors on upload failures
  - Uses efficient `TokenManager` to avoid redundant token checks
  - Tracks upload statistics (attempted/skipped/uploaded/failed) in metadata

### Supporting Files

- `qbo_auth.py`  
  Handles QuickBooks OAuth2 authentication and token management. Contains client ID, client secret, and refresh-token logic. Automatically refreshes expired access tokens.

- `qbo_query.py`  
  **Unified QuickBooks query and management tool** — Supports multiple operations:
  - `count`: Count SalesReceipts for a date or date range
  - `list`: List SalesReceipts with details (supports pagination)
  - `delete`: Delete SalesReceipts for a date or date range (with confirmation)
  - `query`: Execute custom QBO queries
  - `reconcile`: Reconcile EPOS totals vs QBO totals for a date or date range (integrated into pipeline as Phase 5)
  
  All operations support single dates or date ranges. See [QuickBooks Query Tool](#quickbooks-query-tool) section for usage examples.

- `slack_notify.py`  
  Sends Slack notifications for pipeline events (start, success, failure). Requires `SLACK_WEBHOOK_URL` environment variable. Failure notifications automatically extract concise, user-friendly error reasons from error messages. Uses SSL certificate verification with certifi support.

- `load_env.py`  
  Utility to automatically load environment variables from `.env` file. Makes credential management easier without modifying shell profiles.

- `sales_recepit_script.py`  
  Core transformation library used by `epos_to_qb_single.py`. Converts raw EPOS CSV format into QuickBooks-compatible format.

### Data Folders

- `Uploaded/<date>/` or `Uploaded/<date_range>/` – Archived files after successful upload (created automatically by Phase 4)
  - Contains raw CSV, processed CSV(s), used spill files, and `last_epos_transform.json` metadata
  - Standard pipeline: Organized by target date (format: `YYYY-MM-DD`)
  - Custom pipeline: Organized by date range (format: `YYYY-MM-DD to YYYY-MM-DD`)
- `uploads/spill/` – Spillover files for future dates (created automatically, cleaned up after use)
  - Contains `BookKeeping_spill_YYYY-MM-DD.csv` files for dates that appear in CSV downloads for other dates
  - These files are automatically merged when processing their target date
  - Moved to `Uploaded/<date>/` after successful upload
- `logs/` – Log files for each full pipeline run (created automatically by `run_pipeline.py`)

**Note:** Files are temporarily stored in the repo root during processing, then automatically archived after successful upload.

---

## Requirements

You'll need:

- **Python 3.9+** installed on your machine
- **EPOS Now HQ** account credentials
- **QuickBooks Online** account with Developer app access
- The following Python packages (see `requirements.txt` for versions):
  - `playwright` - Web automation for EPOS downloads
  - `pandas` - Data processing and CSV manipulation
  - `requests` - HTTP client for QuickBooks API
  - `certifi` - SSL certificate bundle (recommended for Slack notifications)

### Install Python dependencies

**Recommended: Use a virtual environment** to isolate dependencies:

From the `code-scripts` folder:

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# On macOS/Linux:
source .venv/bin/activate

# Install all dependencies from requirements.txt
pip install -r requirements.txt

# Install Playwright browser (required after installing playwright package)
playwright install chromium
```

**Alternative: Install without virtual environment** (not recommended):

```bash
pip install -r requirements.txt
playwright install chromium
```

**Alternative: Install packages individually:**

```bash
pip install playwright pandas requests certifi
playwright install chromium
```

> **Note:** 
> - On macOS, you may need to use `pip3` and `python3` instead of `pip` and `python`.
> - If you encounter PowerShell execution policy errors on Windows, run: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
> - The `playwright install chromium` command is **required** after installing the playwright package - it downloads the browser binary.

---

## Initial Setup

### 1. Set Up QuickBooks Developer App

1. Create a QuickBooks Developer account at [developer.intuit.com](https://developer.intuit.com)
2. Create a new app (start with **Sandbox** for testing)
3. Note your **Client ID** and **Client Secret**
4. Configure OAuth redirect URLs as required by Intuit

### 2. Configure Credentials (Required)

**All credentials must be set for security.** You have two options:

#### Option A: Using .env File (Recommended - Easiest)

1. **Copy the example file:**

   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` and add your credentials:**

   ```bash
   nano .env  # or use any text editor
   ```

   Replace the placeholder values:

   ```
   QBO_CLIENT_ID=your_actual_client_id
   QBO_CLIENT_SECRET=your_actual_client_secret
   QBO_REALM_ID=your_realm_id  # QuickBooks Company ID
   EPOS_USERNAME=your_actual_username
   EPOS_PASSWORD=your_actual_password
   SLACK_WEBHOOK_URL=your_slack_webhook_url  # Optional: for pipeline notifications
   ```

3. **That's it!** The pipeline will automatically load credentials from `.env` when you run it.

> ✅ **Benefits:** All credentials in one place, easy to edit, automatically loaded, already in `.gitignore`

#### Option B: Environment Variables (Alternative)

If you prefer using system environment variables:

```bash
# Set for current session
export QBO_CLIENT_ID="your_client_id"
export QBO_CLIENT_SECRET="your_client_secret"
export QBO_REALM_ID="your_realm_id"  # QuickBooks Company ID
export EPOS_USERNAME="your_username"
export EPOS_PASSWORD="your_password"
export SLACK_WEBHOOK_URL="your_slack_webhook_url"  # Optional: for pipeline notifications
```

**For persistent setup**, add to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
# Add to ~/.zshrc or ~/.bash_profile
export QBO_CLIENT_ID="your_client_id"
export QBO_CLIENT_SECRET="your_client_secret"
export QBO_REALM_ID="your_realm_id"  # QuickBooks Company ID
export EPOS_USERNAME="your_username"
export EPOS_PASSWORD="your_password"
export SLACK_WEBHOOK_URL="your_slack_webhook_url"  # Optional: for pipeline notifications
```

Then reload: `source ~/.zshrc`

> ⚠️ **Security:** The `.env` file is already in `.gitignore`, so it won't be committed. Never commit credentials to version control.

### 3. Get Initial OAuth Tokens

Before running the pipeline for the first time, you need to obtain initial OAuth tokens:

1. **Perform OAuth Authorization Flow:**

   - Use Intuit's OAuth playground or a custom script
   - Authorize your app to access your QuickBooks company
   - You'll receive an authorization code

2. **Exchange Authorization Code for Tokens:**
   - Exchange the authorization code for an access token and refresh token
   - Save these tokens to `qbo_tokens.json`:

```json
{
  "access_token": "your_access_token",
  "refresh_token": "your_refresh_token",
  "expires_at": 1700000000
}
```

3. **Get Your Realm ID:**
   - The Realm ID is your QuickBooks company ID
   - Add it to your `.env` file as `QBO_REALM_ID` (or set as environment variable)
   - Found in your QBO app settings or API responses

### 4. Configure Payment Methods in QuickBooks

Ensure your QuickBooks company has Payment Methods configured that match the tender values in your CSV. Common values include:

- `Cash`
- `Card`
- `Transfer`
- `Cash/Transfer`
- `Card/Transfer`
- `Card/Cash`
- `Card/Cash/Transfer`

The script maps these from the memo field to QuickBooks Payment Methods by name. You may need to update the `PAYMENT_METHOD_BY_NAME` dictionary in `qbo_upload.py` to match your specific Payment Method IDs.

### 5. Verify `.gitignore` is Set Up

A `.gitignore` file is included in the repository to exclude sensitive files. It includes:

- `qbo_tokens.json` - QuickBooks OAuth tokens
- `*.csv` - Temporary processing files
- `last_epos_transform.json` - Processing metadata
- `logs/` - Log files
- `Uploaded/` - Archived files (optional)
- Other common exclusions (Python cache, IDE files, etc.)

> **Security Note:** The `.gitignore` file ensures sensitive credentials and tokens are never committed to version control. Always verify your credentials are set via environment variables, not hardcoded in source files.

---

## QuickBooks Online Configuration

The upload script uses the QuickBooks Online API and expects:

- A **QuickBooks Developer app** (sandbox or production)
- Valid **OAuth tokens** (access token and refresh token)
- The **realm ID** for the QBO company you're connecting to
- Pre‑configured **Payment Methods** in QBO that match the tender values used in the CSV
- Pre‑configured **Items** (products/services) in QBO, or the script will use a default item

The script maps the memo field (tender type) to QuickBooks Payment Methods by name and sends the corresponding `PaymentMethodRef` ID when creating each Sales Receipt.

**Location/Department Support:**

- The script automatically maps **Location** data from the CSV to QuickBooks **Departments** (shown as "Location" in the QBO UI)
- If a location name from the CSV doesn't exist in QuickBooks, a warning is logged and the Sales Receipt is created without a Department reference
- Ensure your QuickBooks company has Departments configured with names matching your CSV location values

**Item Auto-Creation:**

- By default, `AUTO_CREATE_ITEMS = True` in `qbo_upload.py`
- If an item/product doesn't exist in QuickBooks, the script will automatically create it as a Service item
- Items are cached during the run to avoid duplicate queries

> ⚠️ **Important:** When using production credentials, be careful not to run the pipeline multiple times for the same day unless you intend to create duplicate Sales Receipts.

---

## Token Refresh (OAuth2)

The pipeline uses OAuth2 tokens to communicate with QuickBooks Online. Access tokens expire every 60 minutes, so the script supports automatic token refresh.

### How Token Refresh Works

- `qbo_auth.py` manages your **client ID**, **client secret**, and refresh-token logic
- `qbo_upload.py` uses a `TokenManager` class that:
  1. Fetches a valid access token once at the start of the run
  2. Automatically detects 401 authentication errors during API calls
  3. Refreshes the token using the refresh token
  4. Updates the token state and retries the failed request
  5. Uses the refreshed token for all subsequent requests

This ensures efficient token management without redundant checks, and handles mid-run token expiry automatically.

### Token Storage File (`qbo_tokens.json`)

The script stores your current **access token** and **refresh token** inside a local JSON file named `qbo_tokens.json`.

This file is automatically updated whenever:

- A new access token is issued
- A refresh token is exchanged for a new one

Example structure:

```json
{
  "access_token": "...",
  "refresh_token": "...",
  "expires_at": 1700000000
}
```

#### Important Security Notes

- This file **must be kept private** — add it to `.gitignore`
- If `expires_at` is in the past, the upload script will automatically refresh the tokens
- If the refresh token has expired (typically after ~100 days), you must manually perform a new OAuth authorization to regenerate tokens
- **Never commit your tokens or credentials** to git
- In production, always store credentials using environment variables or a secrets manager

---

## Folder Structure

The expected structure under `code-scripts/` is roughly:

```text
code-scripts/
  epos_playwright.py
  epos_playwright_custom.py
  epos_to_qb_single.py
  qbo_upload.py
  qbo_auth.py
  qbo_query.py
  sales_recepit_script.py
  run_pipeline.py
  run_pipeline_custom.py
  slack_notify.py
  load_env.py
  README.md

  # Temporary files (during processing)
  BookKeeping_YYYYMMDD_HHMMSS.csv        # downloaded by epos_playwright.py
  single_sales_receipts_BookKeeping_....csv  # created by epos_to_qb_single.py
  last_epos_transform.json                 # metadata for archiving

  Uploaded/
    2025-01-15/  # Standard pipeline (single date)
      BookKeeping_20250115_120000.csv
      single_sales_receipts_BookKeeping_....csv
      last_epos_transform.json
    2025-10-15 to 2025-10-17/  # Custom pipeline (date range)
      BookKeeping_20251015_120000.csv
      single_sales_receipts_BookKeeping_....csv
      last_epos_transform.json

  logs/
    pipeline_YYYYMMDD-HHMMSS.log
```

**Workflow:**

1. Phase 1 downloads CSV to repo root
2. Phase 2 processes CSV and saves to repo root + creates metadata
3. Phase 3 uploads from repo root
4. Phase 4 archives all files to `Uploaded/<date>/` after successful upload
5. Phase 5 reconciles EPOS totals vs QBO totals to verify data integrity

- `Uploaded/` folder structure is created automatically by Phase 4
- `logs/` is created automatically by `run_pipeline.py` when logging is enabled

---

## Workflow Details

### File Flow

1. **Download Phase:** EPOS CSV is downloaded directly to the repo root directory
2. **Transform Phase:** Raw CSV is processed and output CSV is saved to repo root. Metadata file `last_epos_transform.json` is created with:
   - Raw file path and name
   - Processed file name(s)
   - Normalized date extracted from CSV data
   - Processing timestamp
3. **Upload Phase:** Processed CSV is read from repo root and uploaded to QuickBooks
4. **Archive Phase:** After successful upload, all files are moved to `Uploaded/<date>/`:
   - Raw CSV file
   - Processed CSV file(s)
   - Metadata file (`last_epos_transform.json`)
5. **Reconciliation Phase:** EPOS totals are compared against QBO totals for the processed date(s) to verify data integrity. Results are logged and sent via Slack notification.

The target date used for archiving is either:
- The `--target-date` parameter provided (preferred)
- The normalized date extracted from the CSV's `*SalesReceiptDate` column (fallback)

This ensures files are organized by the intended business date rather than processing date.

**Multi-Date Spillover Handling:**
When processing a target date, the pipeline:
1. Filters rows by target date (using WAT timezone conversion)
2. Separates spillover rows (from other dates) into `uploads/spill/BookKeeping_spill_YYYY-MM-DD.csv` files
3. Checks for existing spill files matching the target date and merges them
4. Processes all merged data together
5. Archives used spill files to `Uploaded/<date>/` after successful upload
6. Keeps future-date spill files in `uploads/spill/` for later processing

This prevents duplicate uploads when spillover rows from one day's CSV are processed again on their actual date.

---

## Running the Full Pipeline

### Standard Pipeline (Target Date)

From the `code-scripts` directory, run:

```bash
# Process yesterday's data (default)
python3 run_pipeline.py

# Process a specific date
python3 run_pipeline.py --target-date 2025-12-24
```

**What happens:**
1. Downloads CSV for the target date (or yesterday if not specified)
2. Filters rows to only process the target date (handles timezone differences)
3. Separates spillover rows (from other dates) into `uploads/spill/` files
4. Checks for and merges existing spill files for the target date
5. Uploads to QuickBooks with deduplication
6. Archives all files including used spill files

**Multi-Date Spillover Handling:**
Due to timezone differences, EPOS CSV downloads for one date may include rows from adjacent dates. The pipeline automatically:
- Filters rows by target date using WAT timezone conversion
- Saves spillover rows (other dates) to `uploads/spill/BookKeeping_spill_YYYY-MM-DD.csv`
- When processing a date, automatically checks for and merges existing spill files for that date
- Archives spill files after successful upload

### Windows Task Scheduler

For automated scheduling on Windows, use the provided `run_pipeline.cmd` batch file:

```batch
@echo off
setlocal

set ROOT=C:\Users\MARVIN-DEV\Documents\Developer Projects\Scripts\Precious Oreva\AKPONORA
cd /d "%ROOT%\code-scripts"

call "%ROOT%\code-scripts\.venv\Scripts\activate.bat"

python run_pipeline.py
echo Pipeline exit code: %ERRORLEVEL%

endlocal
```

**Setup:**
1. Update the `ROOT` path in `run_pipeline.cmd` to match your project location
2. Create a scheduled task in Windows Task Scheduler
3. Set the action to run `run_pipeline.cmd`
4. Configure your desired schedule (e.g., daily at a specific time)

**Exit Codes:**
- Exit code **0**: Pipeline succeeded (all phases completed)
- Exit code **1**: Pipeline failed (critical error occurred)

The pipeline includes reconciliation as Phase 5, so no separate reconciliation step is needed.

### Custom Date Range Pipeline

To process a specific date range:

```bash
python3 run_pipeline_custom.py --from-date 2025-12-01 --to-date 2025-12-05
```

**Arguments:**

- `--from-date`: Start date in `YYYY-MM-DD` format (required)
- `--to-date`: End date in `YYYY-MM-DD` format (required)
- `--target-date`: Optional target business date in `YYYY-MM-DD` format. If provided, filters rows to only this date (useful for handling spillover within a range)

**Examples:**

```bash
# Single day
python3 run_pipeline_custom.py --from-date 2025-12-13 --to-date 2025-12-13

# Date range (processes all dates in range)
python3 run_pipeline_custom.py --from-date 2025-12-01 --to-date 2025-12-05

# Date range with target-date filtering (downloads range but processes only one date)
python3 run_pipeline_custom.py --from-date 2025-12-24 --to-date 2025-12-26 --target-date 2025-12-25
```

**When to use `--target-date` with date ranges:**
- When you want to download a date range but process only a specific date (e.g., to handle spillover from previous runs)
- When testing spillover handling for a specific date

### Pipeline Phases

Both pipelines run the same five phases:

1. **Phase 1:** Launch Playwright and download EPOS BookKeeping CSV to the repo root.
2. **Phase 2:** Transform the raw CSV into a single QuickBooks‑ready CSV in the repo root, and create metadata file.
3. **Phase 3:** Upload Sales Receipts into QuickBooks via the API.
4. **Phase 4:** Archive all processed files (raw CSV, processed CSV, and metadata) to `Uploaded/<date>/` folder.
5. **Phase 5:** Reconcile EPOS totals vs QBO totals to verify data integrity. Compares receipt counts and total amounts.

### Error Handling

- If any critical step fails (for example, EPOS login changes or the API token is expired), the pipeline will stop and send a failure notification to Slack (if configured).
- **Note:** If archiving (Phase 4) fails, the pipeline will log a warning but continue since the upload was successful.
- **Note:** If reconciliation (Phase 5) fails, the pipeline will log a warning but continue since the upload was successful.
- Sales Receipt upload failures are now properly detected and will stop the pipeline before archiving.
- Pipeline returns exit code **0** on success, **1** on failure, making it suitable for automated scheduling (e.g., Windows Task Scheduler).

### Slack Notifications

If `SLACK_WEBHOOK_URL` is set in your environment or `.env` file, the pipeline will send notifications:

- **Start:** When the pipeline begins (includes date range for custom pipeline)
- **Success:** When all phases complete successfully
- **Failure:** When any critical phase fails (includes concise error reason)
- **Reconciliation:** Phase 5 automatically sends reconciliation results via Slack (includes match/mismatch status, totals, and difference)

Failure notifications automatically extract and display user-friendly error reasons instead of full tracebacks. Common error types detected include:
- Token authentication errors (invalid/expired refresh tokens)
- Missing credentials (CLIENT_ID, CLIENT_SECRET, REALM_ID)
- File not found errors
- Network/API errors (connection issues, rate limits, auth failures)
- Phase-specific failures

Notifications include the log file name and date range (for custom pipeline) for easy debugging. Full error details are always available in the log files.

---

## QuickBooks Query Tool

The `qbo_query.py` script provides a unified interface for querying and managing QuickBooks data. It supports multiple operations through subcommands.

### Commands

#### Count SalesReceipts

Count the number of SalesReceipts for a date or date range:

```bash
# Single date
python3 qbo_query.py count 2025-10-19

# Date range
python3 qbo_query.py count 2025-10-15 2025-10-17
```

#### List SalesReceipts

List SalesReceipts with details (supports pagination):

```bash
# Single date (shows first 100)
python3 qbo_query.py list 2025-10-19

# Date range with custom limit
python3 qbo_query.py list 2025-10-15 2025-10-17 --max-results 50
```

#### Delete SalesReceipts

Delete SalesReceipts for a date or date range (with confirmation):

```bash
# Single date (will prompt for confirmation)
python3 qbo_query.py delete 2025-10-19

# Date range (will prompt for confirmation)
python3 qbo_query.py delete 2025-10-15 2025-10-17

# Skip confirmation prompt
python3 qbo_query.py delete 2025-10-15 2025-10-17 --yes
```

**⚠️ Warning:** Deletion is permanent. The script will:
- Show a preview of receipts to be deleted
- Ask for confirmation (unless `--yes` is used)
- Send a Slack notification when deletion completes (if `SLACK_WEBHOOK_URL` is configured)

#### Execute Custom Query

Run any custom QuickBooks query:

```bash
python3 qbo_query.py query "SELECT * FROM Customer MAXRESULTS 10"
```

#### Reconcile EPOS vs QBO Totals

Reconcile EPOS totals against QBO totals for a date or date range:

```bash
# Single date
python3 qbo_query.py reconcile --from-date 2025-10-19

# Yesterday (convenience flag)
python3 qbo_query.py reconcile --yesterday

# Date range
python3 qbo_query.py reconcile --from-date 2025-10-15 --to-date 2025-10-17

# With tolerance (allow small differences)
python3 qbo_query.py reconcile --from-date 2025-10-19 --tolerance 0.01
```

**Features:**
- Compares receipt counts and total amounts between EPOS and QBO
- Automatically finds EPOS files from `Uploaded/` folders or repo root metadata
- Sends Slack notification with reconciliation results
- Shows match/mismatch status and difference amount
- Supports tolerance for small rounding differences

**Note:** Reconciliation is automatically run as Phase 5 of the pipeline, so manual reconciliation is typically only needed for troubleshooting or historical verification.

### Features

- **Pagination Support:** Automatically handles queries that return more than 1000 results
- **Date Range Support:** All date-based commands support single dates or date ranges
- **Slack Integration:** Delete and reconcile operations send notifications on completion
- **Environment Variables:** Uses `QBO_REALM_ID` from `.env` or environment variables
- **Automatic Reconciliation:** Reconciliation runs automatically as Phase 5 of the pipeline

### Common Use Cases

**Before re-uploading data:**
1. Count receipts: `python3 qbo_query.py count 2025-10-19`
2. Delete receipts: `python3 qbo_query.py delete 2025-10-19`
3. Run pipeline: `python3 run_pipeline_custom.py --from-date 2025-10-19 --to-date 2025-10-19`

**Probe data:**
```bash
# See what receipts exist for a date range
python3 qbo_query.py list 2025-10-15 2025-10-17 --max-results 20
```

---

## Troubleshooting

### Missing Environment Variables

- **Symptom:** `EPOS_USERNAME environment variable is not set` or `QBO_CLIENT_ID environment variable is not set`
- **Solutions:**
  - **Easiest:** Use a `.env` file (recommended):
    1. Copy the example: `cp .env.example .env`
    2. Edit `.env` and add your credentials (including `QBO_REALM_ID`)
    3. Run the pipeline - it will automatically load from `.env`
  - **Alternative:** Set environment variables:
    ```bash
    export QBO_CLIENT_ID="your_client_id"
    export QBO_CLIENT_SECRET="your_client_secret"
    export QBO_REALM_ID="your_realm_id"
    export EPOS_USERNAME="your_username"
    export EPOS_PASSWORD="your_password"
    export SLACK_WEBHOOK_URL="your_slack_webhook_url"  # Optional
    ```
  - For persistent setup, add to your shell profile (`~/.zshrc` or `~/.bashrc`)
  - Verify variables are set: `echo $QBO_CLIENT_ID` or check your `.env` file

### EPOS Login Fails

- **Symptom:** Playwright script fails to log in or download CSV
- **Solutions:**
  - Verify EPOS credentials are set correctly via environment variables
  - Check if EPOS website structure has changed (may require updating Playwright selectors)
  - Ensure browser can access the EPOS website (check network/firewall)
  - Try running with `headless=False` to see what's happening

### QuickBooks Token Errors

- **Symptom:** `qbo_tokens.json not found or empty` or `No refresh_token found`
- **Solutions:**
  - Ensure `qbo_tokens.json` exists with valid `access_token` and `refresh_token`
  - Re-run OAuth authorization flow to get new tokens
  - Check that `QBO_CLIENT_ID` and `QBO_CLIENT_SECRET` are set correctly

### Token Refresh Fails

- **Symptom:** `Failed to refresh access token` error
- **Solutions:**
  - Refresh token may have expired (~100 days) — re-authorize to get new tokens
  - Verify `CLIENT_ID` and `CLIENT_SECRET` are correct
  - Check network connectivity to Intuit OAuth endpoint

### QuickBooks API Errors

- **Symptom:** 401 Unauthorized, 403 Forbidden, or other API errors
- **Solutions:**
  - Verify `QBO_REALM_ID` in your `.env` file or environment variables matches your QuickBooks company
  - Check that your app has the correct permissions/scopes
  - Ensure tokens are valid (check `expires_at` in `qbo_tokens.json`)
  - For sandbox, verify you're using sandbox credentials and sandbox company
  - **Note:** The script automatically refreshes tokens on 401 errors, so if you see a 401, it will retry once with a fresh token. If it still fails, check your credentials.

### Sales Receipt Upload Failures

- **Symptom:** Pipeline continues even when Sales Receipt uploads fail
- **Solution:** This has been fixed! The pipeline now properly validates API responses and will stop with an error if any Sales Receipt fails to upload. Check the error message for details about what went wrong (missing items, invalid data, etc.)

### Payment Method Mapping Errors

- **Symptom:** Sales Receipts created but payment method is wrong or missing
- **Solutions:**
  - Verify Payment Methods exist in QuickBooks with exact names matching CSV values
  - Update `PAYMENT_METHOD_BY_NAME` dictionary in `qbo_upload.py` with correct Payment Method IDs
  - Check CSV memo/tender column values match expected format

### Location/Department Not Appearing

- **Symptom:** Sales Receipts created but Location field is empty in QuickBooks
- **Solutions:**
  - Verify Departments exist in QuickBooks with names matching your CSV "Location" column values
  - Check that the Location column is present in your processed CSV
  - If a location name doesn't match exactly, the script will log a warning and continue without the Department reference

### CSV Processing Errors

- **Symptom:** `No raw CSV files found` or transformation errors
- **Solutions:**
  - Ensure repo root contains raw CSV files (downloaded by Phase 1)
  - Verify CSV file format matches expected EPOS BookKeeping format
  - Check that `sales_recepit_script.py` is present (required dependency)
  - If files were already archived, they'll be in `Uploaded/<date>/` folders

### Duplicate Sales Receipts

- **Symptom:** Running pipeline multiple times creates duplicate entries
- **Solutions:**
  - The pipeline now includes **automatic deduplication**:
    - **Layer A**: Local ledger (`uploaded_docnumbers.json`) tracks successfully uploaded DocNumbers
    - **Layer B**: Bulk QBO API queries check for existing SalesReceipts before uploading
  - Duplicate DocNumbers are automatically skipped with a log message
  - If you need to re-upload, delete existing receipts first using `qbo_query.py delete`
  - For testing, use QuickBooks Sandbox to avoid production data issues

### Spillover Files Not Being Created

- **Symptom:** No spill files appear in `uploads/spill/` folder
- **Solutions:**
  - Verify that the CSV actually contains rows from multiple dates (check date distribution in logs)
  - Ensure `--target-date` is provided when running the pipeline
  - Check that spillover rows exist (rows with dates different from target date)
  - Spill files are only created when there are rows from dates other than the target date

### Spill Files Not Being Merged

- **Symptom:** Spill files exist but aren't being merged when processing target date
- **Solutions:**
  - Verify spill file naming: `BookKeeping_spill_YYYY-MM-DD.csv` (must match target date exactly)
  - Check that `--target-date` matches the date in the spill filename
  - Ensure spill files are in `uploads/spill/` directory (relative to repo root)
  - Check logs for "Found existing spill file" messages

---

## Security Best Practices

- **Environment Variables:** All credentials (QuickBooks, EPOS, and Slack) must be set via environment variables or `.env` file. Never hardcode credentials in source files.
- **Token File Protection:** The `qbo_tokens.json` file is automatically created with restricted file permissions (owner read/write only, 0o600) for security.
- **Version Control:** The `.gitignore` file ensures sensitive files are never committed. Always verify credentials are not in source code before committing.
- **Production:** For production deployments, use a secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.) instead of environment variables.
- **Token Rotation:** Refresh tokens expire after ~100 days. Plan to re-authorize periodically.
- **Archive Safety:** The archive function includes safety checks to prevent accidentally moving the repository root directory. Invalid file paths in metadata are skipped with warnings.

## Notes

- For development and testing, it's recommended to start with a **QuickBooks sandbox company** before pointing the pipeline at a live production company.
- The pipeline processes the **latest** raw CSV file in the repo root (excluding processed files), so ensure you're working with the correct files if multiple exist.
- Files are automatically archived after successful upload to keep the repo root clean. Archived files are organized by date in `Uploaded/<date>/` folders.
- The `last_epos_transform.json` metadata file tracks which files were processed and their normalized date for proper archiving.
