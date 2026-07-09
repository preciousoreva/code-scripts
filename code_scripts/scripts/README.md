# Scripts

Run all commands from the **repo root** (`code-scripts`).

| Folder / script | Purpose |
|-----------------|--------|
| [invoice/](invoice/) | Transform raw invoice CSV, prepare (alias/fuzzy match), import invoices into QBO |
| [bills/](bills/) | Export QBO Bills to CSV, re-import bills from CSV |
| [qbo_queries/](qbo_queries/) | Ad-hoc QBO queries (items, accounts, etc.) |
| `qbo_delete_sales_receipts.py` | Delete sales receipts in QBO (e.g. by date or DocNumber) |
| `qbo_inv_manager.py` | Inventory / InvStartDate management |
