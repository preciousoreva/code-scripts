# QBO Inventory Remediation

This utility is for remediation only. It helps plan and optionally delete historical QuickBooks `InventoryAdjustment` transactions created by prior inventory automation runs. It does not run the normal inventory sync, does not create new inventory adjustments, and does not touch sales sync.

The default workflow is plan-only:

```bash
python -m code_scripts.qbo_inventory_remediation plan \
  --company company_a \
  --from-date 2026-04-29 \
  --to-date 2026-04-30 \
  --number-prefix INVCON \
  --candidate-csv /path/to/remediation_candidates.csv \
  --exclude-number INVCON-20260430-14620 \
  --output-dir runtime/code_scripts/reports/qbo_inventory_remediation
```

Apply requires both explicit flags:

```bash
python -m code_scripts.qbo_inventory_remediation delete \
  --company company_a \
  --from-date 2026-04-29 \
  --to-date 2026-04-30 \
  --number-prefix INVCON \
  --candidate-csv /path/to/remediation_candidates.csv \
  --max-transactions 5 \
  --apply \
  --confirm-delete-inventory-adjustments \
  --output-dir runtime/code_scripts/reports/qbo_inventory_remediation
```

For the first production remediation run, also pass the transactions that were already manually deleted so the script does not waste time planning them as already missing:

```bash
  --exclude-number INVCON-20260429-9275 \
  --exclude-number INVCON-20260429-9285 \
  --exclude-number INVCON-20260429-9185 \
  --exclude-number INVCON-20260429-9307 \
  --exclude-number INVCON-20260429-9341 \
  --exclude-number INVCON-20260429-9153 \
  --exclude-number INVCON-20260429-9326 \
  --exclude-number INVCON-20260429-9151 \
  --exclude-number INVCON-20260429-9283 \
  --exclude-number INVCON-20260429-9189
```

Safety rules:

- Plan-only is the default operational mode.
- Delete mode refuses to run unless `--apply` and `--confirm-delete-inventory-adjustments` are both present.
- `INVCON` is the default target family. `INVADJ` is refused by default unless `--allow-invadj` is explicitly passed for an approved remediation run.
- The known excluded transactions are built into the script and can be extended with repeatable `--exclude-number`.
- `--max-transactions` caps actual delete candidates and prevents oversized batches.
- Candidate CSV rows are sorted by `expected_impact` descending when that column is present.
- Deletion failures are logged per transaction and do not stop the batch unless `--fail-fast` is passed.

After each deletion batch, export the QBO Inventory Shrinkage / Inventory Asset reports again and compare the accounting movement before deciding the next batch. Do not run this remediation utility during sales sync or inventory sync.

Do not commit QBO exports, remediation ledgers, plan outputs, or deletion logs.
