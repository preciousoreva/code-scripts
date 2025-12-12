# EPOS → QuickBooks Automation

This repo contains a small automation pipeline that:

1. Logs into **EPOS Now HQ** and downloads the daily **BookKeeping** CSV to the repo root.
2. Transforms the raw EPOS export into a single QuickBooks‑ready CSV in the repo root.
3. Uploads the data into **QuickBooks Online** as Sales Receipts using the QBO API.
4. Archives processed files to `Uploaded/<date>/` after successful upload.

The pipeline is designed to be run as a single command and take care of all four phases in sequence.

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
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Run the pipeline:**
   ```bash
   python3 run_pipeline.py
   ```

That's it! The pipeline will download, transform, upload, and archive automatically.

> 💡 **Tip:** See [Initial Setup](#initial-setup) below for detailed instructions on each step.

---

## Files / Scripts

### Core Pipeline Scripts

- `run_pipeline.py`  
  **Main entry point** — Orchestration script that runs all four phases in order:

  1. Download EPOS CSV (`epos_playwright.py`)
  2. Transform to single CSV (`epos_to_qb_single.py`)
  3. Upload to QuickBooks (`qbo_upload.py`)
  4. Archive files (`run_pipeline.py` - Phase 4)

- `epos_playwright.py`  
  Uses **Playwright** to log into EPOS Now, navigate to the BookKeeping report, and download the CSV directly to the repo root directory.  
  **Note:** EPOS credentials are loaded from `.env` file or environment variables (see [Initial Setup](#2-configure-credentials-required)).

- `epos_to_qb_single.py`  
  Reads the latest raw CSV from the repo root and transforms it into a single consolidated CSV for QuickBooks import.  
  Output is written to `single_sales_receipts_*.csv` in the repo root.  
  Creates `last_epos_transform.json` metadata file with file paths and normalized date for archiving.  
  **Dependency:** Uses `sales_recepit_script.py` for transformation logic.

- `qbo_upload.py`  
  Reads the latest `single_sales_receipts_*.csv` from the repo root and uses the **QuickBooks Online REST API** to create Sales Receipts.  
  Each `*SalesReceiptNo` group becomes one Sales Receipt, with the tender type stored in the memo and the **Payment method** automatically mapped from the memo.

### Supporting Files

- `qbo_auth.py`  
  Handles QuickBooks OAuth2 authentication and token management. Contains client ID, client secret, and refresh-token logic. Automatically refreshes expired access tokens.

- `sales_recepit_script.py`  
  Core transformation library used by `epos_to_qb_single.py`. Converts raw EPOS CSV format into QuickBooks-compatible format.

### Data Folders

- `Uploaded/<date>/` – Archived files after successful upload (created automatically by Phase 4)
  - Contains raw CSV, processed CSV(s), and `last_epos_transform.json` metadata
  - Organized by normalized date extracted from CSV data (format: `YYYY-MM-DD`)
- `logs/` – Log files for each full pipeline run (created automatically by `run_pipeline.py`)

**Note:** Files are temporarily stored in the repo root during processing, then automatically archived after successful upload.

---

## Requirements

You'll need:

- **Python 3.9+** installed on your machine
- **EPOS Now HQ** account credentials
- **QuickBooks Online** account with Developer app access
- The following Python packages:
  - `playwright`
  - `pandas`
  - `requests`

### Install Python dependencies

From the `code-scripts` folder:

```bash
# Install all dependencies
pip install -r requirements.txt

# Install Playwright browser (required after installing playwright package)
playwright install chromium
```

**Alternative:** If you prefer to install packages individually:

```bash
pip install playwright pandas requests
playwright install chromium
```

> **Note:** On macOS, you may need to use `pip3` and `python3` instead of `pip` and `python`.

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
   EPOS_USERNAME=your_actual_username
   EPOS_PASSWORD=your_actual_password
   ```

3. **That's it!** The pipeline will automatically load credentials from `.env` when you run it.

> ✅ **Benefits:** All credentials in one place, easy to edit, automatically loaded, already in `.gitignore`

#### Option B: Environment Variables (Alternative)

If you prefer using system environment variables:

```bash
# Set for current session
export QBO_CLIENT_ID="your_client_id"
export QBO_CLIENT_SECRET="your_client_secret"
export EPOS_USERNAME="your_username"
export EPOS_PASSWORD="your_password"
```

**For persistent setup**, add to your shell profile (`~/.zshrc` or `~/.bashrc`):

```bash
# Add to ~/.zshrc or ~/.bash_profile
export QBO_CLIENT_ID="your_client_id"
export QBO_CLIENT_SECRET="your_client_secret"
export EPOS_USERNAME="your_username"
export EPOS_PASSWORD="your_password"
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
   - Update `REALM_ID` in `qbo_upload.py`:

```python
REALM_ID = "your_realm_id"  # Found in your QBO app settings or API responses
```

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

> ⚠️ **Important:** When using production credentials, be careful not to run the pipeline multiple times for the same day unless you intend to create duplicate Sales Receipts.

---

## Token Refresh (OAuth2)

The pipeline uses OAuth2 tokens to communicate with QuickBooks Online. Access tokens expire every 60 minutes, so the script supports automatic token refresh.

### How Token Refresh Works

- `qbo_auth.py` manages your **client ID**, **client secret**, and refresh-token logic
- Whenever the upload script (`qbo_upload.py`) detects an expired token, it:
  1. Calls the QuickBooks OAuth2 token endpoint
  2. Exchanges the refresh token for a **new access token**
  3. Saves the new tokens back to `qbo_tokens.json`
  4. Retries the failed request automatically

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
  epos_to_qb_single.py
  qbo_upload.py
  qbo_auth.py
  sales_recepit_script.py
  run_pipeline.py
  README.md

  # Temporary files (during processing)
  BookKeeping_YYYYMMDD_HHMMSS.csv        # downloaded by epos_playwright.py
  single_sales_receipts_BookKeeping_....csv  # created by epos_to_qb_single.py
  last_epos_transform.json                 # metadata for archiving

  Uploaded/
    2025-01-15/
      BookKeeping_20250115_120000.csv
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

The normalized date used for archiving is extracted from the CSV's `*SalesReceiptDate` column, ensuring files are organized by the actual transaction date rather than processing date.

---

## Running the Full Pipeline

From the `code-scripts` directory, run:

```bash
python run_pipeline.py
```

or explicitly with Python 3:

```bash
python3 run_pipeline.py
```

This will:

1. **Phase 1:** Launch Playwright and download the latest EPOS BookKeeping CSV to the repo root.
2. **Phase 2:** Transform the raw CSV into a single QuickBooks‑ready CSV in the repo root, and create metadata file.
3. **Phase 3:** Upload Sales Receipts into QuickBooks via the API.
4. **Phase 4:** Archive all processed files (raw CSV, processed CSV, and metadata) to `Uploaded/<date>/` folder.

If any step fails (for example, EPOS login changes or the API token is expired), the pipeline will stop and print an error message indicating which phase failed. **Note:** If archiving (Phase 4) fails, the pipeline will log a warning but continue since the upload was successful.

---

## Troubleshooting

### Missing Environment Variables

- **Symptom:** `EPOS_USERNAME environment variable is not set` or `QBO_CLIENT_ID environment variable is not set`
- **Solutions:**
  - **Easiest:** Use a `.env` file (recommended):
    1. Copy the example: `cp .env.example .env`
    2. Edit `.env` and add your credentials
    3. Run the pipeline - it will automatically load from `.env`
  - **Alternative:** Set environment variables:
    ```bash
    export QBO_CLIENT_ID="your_client_id"
    export QBO_CLIENT_SECRET="your_client_secret"
    export EPOS_USERNAME="your_username"
    export EPOS_PASSWORD="your_password"
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
  - Verify `REALM_ID` in `qbo_upload.py` matches your QuickBooks company
  - Check that your app has the correct permissions/scopes
  - Ensure tokens are valid (check `expires_at` in `qbo_tokens.json`)
  - For sandbox, verify you're using sandbox credentials and sandbox company

### Payment Method Mapping Errors

- **Symptom:** Sales Receipts created but payment method is wrong or missing
- **Solutions:**
  - Verify Payment Methods exist in QuickBooks with exact names matching CSV values
  - Update `PAYMENT_METHOD_BY_NAME` dictionary in `qbo_upload.py` with correct Payment Method IDs
  - Check CSV memo/tender column values match expected format

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
  - QuickBooks doesn't prevent duplicates by default
  - Check `DocNumber` in created receipts — they should be unique
  - Consider adding duplicate detection logic or only running once per day
  - For testing, use QuickBooks Sandbox to avoid production data issues

---

## Security Best Practices

- **Environment Variables:** All credentials (QuickBooks and EPOS) must be set via environment variables. Never hardcode credentials in source files.
- **Token File Protection:** The `qbo_tokens.json` file is automatically created with restricted file permissions (owner read/write only) for security.
- **Version Control:** The `.gitignore` file ensures sensitive files are never committed. Always verify credentials are not in source code before committing.
- **Production:** For production deployments, use a secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.) instead of environment variables.
- **Token Rotation:** Refresh tokens expire after ~100 days. Plan to re-authorize periodically.

## Notes

- For development and testing, it's recommended to start with a **QuickBooks sandbox company** before pointing the pipeline at a live production company.
- The pipeline processes the **latest** raw CSV file in the repo root (excluding processed files), so ensure you're working with the correct files if multiple exist.
- Files are automatically archived after successful upload to keep the repo root clean. Archived files are organized by date in `Uploaded/<date>/` folders.
- The `last_epos_transform.json` metadata file tracks which files were processed and their normalized date for proper archiving.
