# Daily Bank Reconciliation (Totals-First)

This script reconciles **daily transformed EPOS totals** against **daily bank statement header totals**.

It does **not** do receipt-level matching. It compares:

1. EPOS transformed sales total for the date (`*_sales_receipts_*` CSV)
2. Sum of `Total Credit:` values from daily statement headers
3. Tender totals (Card / Transfer / Cash / mixed) to explain variance

## Command

Run from repo root:

```bash
python -m code_scripts.scripts.reconciliation.reconcile_sales_to_bank \
  --company company_a \
  --date 2026-02-18
```

## Inputs

- Transformed EPOS CSV (auto-discovered):
  - `single_sales_receipts_*_<YYYY-MM-DD>.csv` (company_a)
  - `gp_sales_receipts_*_<YYYY-MM-DD>.csv` (company_b)
- Daily statement `.xlsx` files under:
  - `docs/<company>/bank-reconciliation/statements/<YYYY-MM-DD>/`
- Optional account mapping:
  - `docs/<company>/bank-reconciliation/account_mapping.csv`

## Outputs

Written to:

`code_scripts/reports/reconciliation/<company>/<YYYY-MM-DD>/`

Files:

- `reconciliation_summary.json`
- `statement_credit_totals.csv`
- `epos_tender_totals.csv`
- `daily_reconciliation_overview.csv`

During each run, legacy receipt-level files are removed if present:

- `epos_to_bank_matches.csv`
- `bank_credits_to_epos.csv`
- `debug_unmatched_epos.csv`
- `debug_unmatched_bank.csv`

`statement_credit_totals.csv` includes:

- `channel_classification` (`POS`, `TRANSFER_OR_OTHER`, `EXPENSE_OR_OTHER`)
- `include_in_sales_total` (`True`/`False`) used to compute sales-reconciliation bank totals.

## Main Fields in `reconciliation_summary.json`

- `totals.actual_sales`
- `totals.bank_total_credits`
- `totals.bank_total_credits_all`
- `variance_analysis.variance_epos_minus_bank`
- `tender_totals.cash_total`
- `tender_totals.mixed_with_cash_total`
- `variance_analysis.status`
- `variance_analysis.note`

## Key Flags

- `--epos-file`: explicit transformed sales CSV path
- `--epos-dir`: transformed file discovery directory
- `--statements-dir`: statement directory override
- `--account-mapping`: mapping CSV override
- `--vat-rate`: default `0.075`
- `--variance-tolerance`: default `5.0`
