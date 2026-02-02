# QBO query and debug scripts

Run from repo root. All scripts use `--company` (required) and company config for realm_id and tokens.

**Inventory manager** (recommended for item lookup and InvStartDate):

| Script | Purpose | Example |
|--------|---------|---------|
| `qbo_inv_manager.py` | Get item by ID/name; list InvStartDate issues; set InvStartDate (single, bulk, or from CSV); **import products** from QBO-style CSV (e.g. from `export-products`) | `python scripts/qbo_inv_manager.py --company company_a get --item-id 7220`; `list-invstart --cutoff-date 2026-01-01`; `set-invstart-bulk --cutoff-date 2026-01-01 --new-date 2026-01-01`; `import-products --csv exports/company_a_products.csv --as-of-date 2026-01-01 --report-csv reports/imported_products_company_a.csv` |

**Query scripts** (under `scripts/qbo_queries/`):

| Script | Purpose | Example |
|--------|---------|---------|
| `qbo_query.py` | Run arbitrary QBO SQL-like query | `python scripts/qbo_queries/qbo_query.py --company company_a query "select Id, Name from Item maxresults 5"` |
| `qbo_account_query.py` | Run Account queries (Name-based matching) | `python scripts/qbo_queries/qbo_account_query.py --company company_a --account-name "120300 - Non - Food Items"` |
| `qbo_verify_mapping_accounts.py` | Verify Product.Mapping.csv accounts exist in QBO | `python scripts/qbo_queries/qbo_verify_mapping_accounts.py --company company_a` |

**Export scripts** (run from repo root; script lives under `scripts/`):

| Script | Purpose | Example |
|--------|---------|---------|
| `qbo_export_bills.py` | Export Bills to CSV by date range (bills_header.csv + bills_lines.csv) | `python scripts/qbo_export_bills.py --company company_a --from 2020-01-01 --to 2026-01-31 --out ./exports/company_a_bills` |

Optional: `--verbose` or `--raw-json` where supported to print full JSON. For `qbo_export_bills.py`: `--page-size` (default 1000), `--dry-run` to print count and first 3 bill IDs without writing files.
