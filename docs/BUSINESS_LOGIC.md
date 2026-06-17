# Business Logic Reference

This file summarizes the current business rules implemented in the code scripts
repo, with emphasis on EPOS to QuickBooks Online product and inventory behavior.

## Product and Inventory Item Handling

### Product CSV import creates Inventory items

The `qbo_inv_manager import-products` path treats every non-Category CSV row as
a QBO `Inventory` item by default.

- Category rows are skipped.
- Service and NonInventory rows are converted to `Inventory` unless the operator
  runs with `--inventory-only`.
- With `--inventory-only`, non-Inventory rows are skipped instead.
- A category is required through `ParentRef_Name`.
- The category must exist in `Product.Mapping.csv`.
- New item payloads include `Type=Inventory`, `TrackQtyOnHand=True`,
  `QtyOnHand`, `InvStartDate`, `UnitPrice`, `PurchaseCost`, account refs, and
  category `ParentRef`.

Evidence:

- `code_scripts/scripts/qbo_inv_manager.py:704`
- `code_scripts/scripts/qbo_inv_manager.py:719`
- `code_scripts/scripts/qbo_inv_manager.py:788`
- `code_scripts/scripts/qbo_inv_manager.py:794`

### Existing wrong-type QBO items can be replaced by Inventory items

During sales upload item resolution, if an item already exists in QBO but is not
an `Inventory` item, the code expects Inventory when inventory sync is enabled.
If `auto_fix_wrong_type_items` is enabled for the company, the flow:

1. Renames the existing wrong-type item to a legacy name.
2. Sets the old item inactive.
3. Creates a new `Inventory` item with the original name.
4. Records the action in the auto-fixed items report.

If auto-fix is disabled or fails, the existing wrong-type item is used and the
item is recorded in the wrong-type report.

Evidence:

- `code_scripts/qbo_upload.py:1341`
- `code_scripts/qbo_upload.py:1485`
- `code_scripts/qbo_upload.py:1633`
- `code_scripts/qbo_upload.py:1888`

## Missing Products in QBO

### Sales upload creates missing products when Inventory mode is active

When a sales upload line contains a product that does not exist in QBO, the item
resolver can create a new QBO item.

If inventory mapping is available, the new item is created as `Inventory` using:

- EPOS product name.
- EPOS category.
- Per-unit sales price from `TOTAL Sales / ItemQuantity`.
- Per-unit purchase cost from `Cost Price / ItemQuantity`.
- Category account refs from `Product.Mapping.csv`.
- QBO item category as `ParentRef`.

If inventory mapping is not available, the fallback path can create a Service
item instead.

Evidence:

- `code_scripts/qbo_upload.py:1020`
- `code_scripts/qbo_upload.py:1218`
- `code_scripts/qbo_upload.py:1674`
- `code_scripts/qbo_upload.py:1710`

### Inventory Review creates missing QBO Inventory items only after review

The inventory review flow has a separate missing-item creation path. It is not
an automatic blind create during every inventory audit.

The portal preview classifies missing-from-QBO rows as safe or blocked. Safe
rows require:

- Product name is not blank or a summary row.
- Pack variants are not created when the base item already exists.
- Duplicate candidate base names are blocked.
- `Product.Mapping.csv` is loaded.
- EPOS category is present.
- Category exists in `Product.Mapping.csv`.
- Inventory, revenue, and COGS account mappings are present.

Queued missing-item creation creates only the safe, allowed base names from the
review payload. It checks a fresh QBO snapshot before creating, skips rows that
already exist, creates a QBO item category when needed, and creates an Inventory
item with initial `QtyOnHand` from EPOS.

Important current behavior: this review-created Inventory item uses
`UnitPrice=0.0` and `PurchaseCost=0.0`. It is meant to establish the missing
Inventory item and quantity, not to set product pricing.

Evidence:

- `apps/epos_qbo/services/inventory_review_actions.py:361`
- `apps/epos_qbo/services/inventory_review_actions.py:383`
- `apps/epos_qbo/services/inventory_review_actions.py:592`
- `code_scripts/inventory_review_missing_candidates.py:120`
- `code_scripts/inventory_pipeline.py:1882`
- `code_scripts/inventory_pipeline.py:2003`

## Cost Price Rules

### Confirmed: EPOS zero cost does not overwrite QBO PurchaseCost

The sales upload builds an incoming per-unit purchase cost from EPOS:

```text
unit_purchase_cost_gross = Cost Price / ItemQuantity
```

When patching an existing QBO Inventory item, the code only sets
`PurchaseCost` when:

- Current QBO `PurchaseCost` is missing or zero, and
- Incoming EPOS-derived unit purchase cost is greater than zero.

Therefore, an EPOS cost value of `0` does not overwrite an existing QBO
PurchaseCost.

Evidence:

- `code_scripts/qbo_upload.py:1031`
- `code_scripts/qbo_upload.py:1037`
- `code_scripts/qbo_upload.py:1563`
- `code_scripts/qbo_upload.py:1578`

### Confirmed: existing non-zero QBO PurchaseCost is not overwritten

The current patch guard does not overwrite any non-zero QBO `PurchaseCost`,
whether the EPOS value is lower, equal, or higher. It only fills a missing or
zero QBO purchase cost with a positive EPOS value.

Evidence:

- `code_scripts/qbo_upload.py:1578`
- `code_scripts/qbo_upload.py:1829`

### Gap: lower EPOS cost is not currently alerted for review

The requested rule says:

```text
If EPOS cost is less than the QBO cost, do not overwrite QBO. Alert for review.
List those products on the portal.
```

The "do not overwrite" portion is already satisfied because non-zero QBO
PurchaseCost is never overwritten. However, the repository does not currently
appear to create a dedicated alert, review row, or portal list for products
where EPOS cost is lower than QBO cost.

The inventory quantity preview has cost-related risk flags, but those use QBO
cost to estimate inventory value impact. They do not compare EPOS cost against
QBO PurchaseCost.

Evidence:

- `code_scripts/inventory_pipeline.py:493`
- `code_scripts/inventory_pipeline.py:527`
- `code_scripts/inventory_pipeline.py:541`

## Proposed Follow-up for Lower Cost Review

To fully implement the requested lower-cost review behavior:

1. During sales upload item-state build, keep the EPOS unit purchase cost per
   product.
2. During existing Inventory item patch checks, compare EPOS unit purchase cost
   with current QBO `PurchaseCost` when both are positive.
3. If EPOS cost is lower than QBO cost, do not patch `PurchaseCost` and append a
   review row with product name, QBO item id, category, EPOS cost, QBO cost,
   difference, transaction date, and document number where available.
4. Persist the review rows as a run artifact or database-backed report.
5. Add a portal view/table for "Cost price review" so operators can filter by
   company, run, product, and date.

Recommended status labels:

- `epos_cost_zero`: EPOS cost is zero; no update applied.
- `epos_cost_lower_than_qbo`: EPOS cost is below QBO cost; review required.
- `qbo_cost_missing_filled`: QBO cost was missing or zero and was filled from
  positive EPOS cost.
- `qbo_cost_nonzero_preserved`: QBO cost already had a non-zero value and was
  left unchanged.

## Other Inventory Sync Safety Rules

### Inventory quantity apply is disabled

Forward inventory sync is audit and preview first. The code does not post QBO
InventoryAdjustment transactions for the normal inventory sync path. Operators
use generated reports for manual QBO starting-value corrections.

Evidence:

- `docs/INVENTORY_SYNC.md`
- `code_scripts/inventory_pipeline.py:252`

### Zero or missing QBO cost blocks quantity apply eligibility

For quantity preview risk scoring, if QBO cost is missing or zero, the preview
adds `missing_or_zero_cost` and blocks apply eligibility by default. Quantity
apply itself is currently removed, but the risk signal remains in reports.

Evidence:

- `code_scripts/inventory_pipeline.py:541`
- `code_scripts/inventory_pipeline.py:553`
- `code_scripts/tests/test_inventory_pipeline.py:936`
