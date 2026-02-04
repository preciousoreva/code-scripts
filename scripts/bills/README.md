# Bills scripts

Scripts for exporting and re-importing QBO Bills (e.g. for InvStartDate fixes or bulk operations).

**Run from repo root** (code-scripts).

## Export Bills to CSV

Export bills in a date range to header + line CSVs for backup or re-import.

```bash
python scripts/bills/qbo_export_bills.py --company company_a --from 2020-01-01 --to 2026-01-31 --out ./exports/company_a_bills/
```

## Re-import Bills

Re-create one or more bills from exported CSVs (e.g. after deleting in QBO and updating InvStartDate).

```bash
# Dry run
python scripts/bills/qbo_import_bills.py --company company_a --bill-id 123 --dry-run

# Create
python scripts/bills/qbo_import_bills.py --company company_a --bill-id 123 --create
python scripts/bills/qbo_import_bills.py --company company_a --bill-ids 58984 58985 58986 --create
python scripts/bills/qbo_import_bills.py --company company_a --all --create
```

Pass exactly one of: `--bill-id`, `--bill-ids`, or `--all`. See script docstring for tax and DocNumber options.
