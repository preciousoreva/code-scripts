# Invoice scripts

Scripts for preparing and importing customer invoices into QuickBooks Online.

**Run from repo root** (code-scripts) so relative paths (e.g. `templates/item_aliases.csv`, `invoices/...`) resolve correctly.

### Config files (`templates/`)

Config CSVs live under **`templates/`** at the repo root so paths stay consistent and out of source data folders.

| File | Purpose |
|------|---------|
| `templates/item_aliases.csv` | Map CSV item names to QBO item names (columns: `CsvItemName`, `QboItemName`). |
| `templates/spelling_corrections.csv` | Fix typos before matching (columns: `Wrong`, `Correct`). Used by prepare and import. |
| `templates/invoice_template.csv` | Column template for formatted invoice CSV (includes `Terms`). |

## 1. Transform raw invoice CSV

Converts the block/section raw format (DATE, QTY, ITEMS, RATE, AMOUNT) into template-shaped CSV.

```bash
python scripts/invoice/transform_invoice_raw.py --csv invoices/company_a_raw_invoice.csv
```

Output: `invoices/company_a_raw_invoice_formatted.csv` (same dir as source). Use `-o path` to set a different output path.

## 2. Prepare invoice CSV (alias + fuzzy match)

Maps item names to QBO items (alias file first, then fuzzy match), fills DueDate/Terms by group, and writes prepared CSV + unmatched report.

```bash
python scripts/invoice/prepare_invoice_csv.py --csv invoices/company_a_raw_invoice_formatted.csv --company company_a
```

- **Prepared CSV:** `invoices/exports/<stem>_prepared.csv`
- **Unmatched report:** `invoices/exports/<stem>_unmatched.csv` (overwritten each run; header-only when there are no unmatched rows).
- Optional: `--aliases templates/item_aliases.csv` (default), `--spelling-corrections templates/spelling_corrections.csv` (default), `--qbo-items-csv path` (offline), `--min-similarity 0.90`

## 3. Import invoices into QBO

Creates QBO Invoices from the prepared CSV (company_a only). Requires valid QBO tokens.

```bash
python scripts/invoice/qbo_import_invoices.py --company company_a --csv invoices/exports/company_a_raw_invoice_formatted_prepared.csv
```

- Use `--dry-run` or `--validate-only` to avoid creating invoices.
- `Terms` in the prepared CSV is mapped to QBO `SalesTermRef` (e.g. `Net 30`). If missing, the importer infers a standard term from the DueDate delta.
- Reports: `reports/invoice_item_matches_*.csv`, `reports/invoice_unmatched_items_*.csv`, `reports/invoice_missing_customers_*.csv`.

## Full pipeline (after you have formatted CSV)

```bash
# Step 2: prepare (alias/fuzzy match, output to invoices/exports/)
python scripts/invoice/prepare_invoice_csv.py --csv invoices/company_a_raw_invoice_formatted.csv --company company_a

# Step 3: import into QBO (optional)
python scripts/invoice/qbo_import_invoices.py --company company_a --csv invoices/exports/company_a_raw_invoice_formatted_prepared.csv
```
