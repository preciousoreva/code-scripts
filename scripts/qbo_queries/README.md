# QBO query and debug scripts

Run from repo root. All scripts use `--company` (required) and company config for realm_id and tokens.

| Script | Purpose | Example |
|--------|---------|---------|
| `qbo_query.py` | Run arbitrary QBO SQL-like query | `python scripts/qbo_queries/qbo_query.py --company company_a query "select Id, Name from Item maxresults 5"` |
| `qbo_item_lookup_by_name.py` | Look up Item by exact Name (SubItem/ParentRef, FullyQualifiedName) | `python scripts/qbo_queries/qbo_item_lookup_by_name.py --company company_a --name "MARY & MAY 12g"` |
| `qbo_item_get_by_id.py` | Get Item by Id or search by Name | `python scripts/qbo_queries/qbo_item_get_by_id.py --company company_a --item-id 9110` |
| `qbo_account_query.py` | Run Account queries (Name-based matching) | `python scripts/qbo_queries/qbo_account_query.py --company company_a --account-name "120300 - Non - Food Items"` |
| `qbo_check_inventory_start_dates.py` | List Inventory items with InvStartDate after cutoff | `python scripts/qbo_queries/qbo_check_inventory_start_dates.py --company company_a --cutoff-date 2026-01-28` |
| `qbo_verify_mapping_accounts.py` | Verify Product.Mapping.csv accounts exist in QBO | `python scripts/qbo_queries/qbo_verify_mapping_accounts.py --company company_a` |

Optional: `--verbose` or `--raw-json` where supported to print full JSON.
