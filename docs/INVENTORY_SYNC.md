# Inventory Sync

Inventory Sync is the operator-facing EPOS to QuickBooks inventory flow. From the Django Runs page, use the Inventory tab, choose a company, optionally choose a category or product filter, then click **Sync Inventory**.

The unified pipeline:

1. Downloads an EPOS stock report.
2. Fetches a fresh QBO inventory item snapshot.
3. Normalizes EPOS stock into base-unit quantities.
4. Audits EPOS against QBO.
5. Applies supported catalog cleanup.
6. Re-audits.
7. Applies exact-match quantity adjustments.
8. Writes final CSV/JSON reports and sends a Slack summary when configured.

## Django Run

For local portal work, run the Django portal (optionally using a sandbox/dev profile), then open **Runs → Inventory**.

Local (no profile):

```bash
python manage.py migrate
python manage.py createsuperuser
python manage.py sync_companies_from_json
python manage.py runserver
```

Sandbox/dev profile (isolated state):

```bash
./build/init-dev-profile.sh <profile>
./build/run-sandbox.sh
```

The main Inventory card intentionally exposes only the operator workflow:

- Company
- Category
- Product filter
- Sync Inventory

Dry-run, audit-only, and low-level catalog cleanup controls stay out of the main Runs UI.

## CLI Examples

One product:

```bash
python -m code_scripts.inventory_pipeline \
  --company company_a \
  --auto-download \
  --auto-fetch-qbo \
  --qbo-force-refresh \
  --product "SMIRNOFF ICE DOUBLE BLACK CAN 330ml*24"
```

Category:

```bash
python -m code_scripts.inventory_pipeline \
  --company company_a \
  --auto-download \
  --auto-fetch-qbo \
  --qbo-force-refresh \
  --category "ALCOHOLS & SPIRITS"
```

## Reports

Pipeline summaries are written under:

```text
runtime/code_scripts/reports/inventory_pipeline/YYYY-MM-DD/
```

Audit CSVs are written under:

```text
runtime/code_scripts/reports/inventory_sync/YYYY-MM-DD/
```

Every pipeline summary JSON includes `child_reports.final_audit`. The pipeline also writes a `final_*` audit CSV each run, even when the final audit is identical to the initial or post-catalog audit.

Inspect latest pipeline JSON:

```bash
ls -t runtime/code_scripts/reports/inventory_pipeline/*/inventory_pipeline_*.json | head -1
```

Inspect latest final audit:

```bash
ls -t runtime/code_scripts/reports/inventory_sync/*/inventory_audit_*_final_*.csv | head -1
```

## Slack Summary

Slack summarizes:

- completion status
- scope
- products checked
- in sync
- catalog fixes
- base items created
- duplicate base items resolved
- quantity updates
- blocked items
- EPOS negative rows clamped to zero, only when nonzero
- final report path

When `OIAT_PORTAL_BASE_URL` and `OIAT_RUN_JOB_ID` are set, Slack includes a Django run link:

```text
https://portal.example.com/epos-qbo/runs/<run_job_id>/
```

## Blocked Items

Blocked items mean the final audit is not fully `in_sync`. Common causes:

- `missing_from_qbo`: no QBO inventory item exists for the EPOS base product.
- `only_pack_variant_exists`: QBO only has pack variants and no base item.
- `base_with_pack_variants`: QBO has both a base item and pack variants.
- `multiple_active_base_items`: QBO has duplicate active base items.
- `needs_adjustment`: quantity still differs after safe catalog cleanup.

Supported cases are fixed automatically. Unsupported or ambiguous cases remain visible in the final audit and summary.

## Quantity Semantics

EPOS pack rows are normalized to single-unit quantities using the pack multiplier and Current Volume when available.

Inventory sync applies the explicit EPOS negative stock policy `clamp_to_zero`: any EPOS stock row that computes to a negative single-unit quantity is treated as `0` before product-level grouping. This applies only to inventory sync normalization. It does not change sales receipt upload behavior.

The policy exists because EPOS can report a negative balance on one pack row while a sibling pack row still has valid positive stock. Without clamping, the negative row can incorrectly subtract from the product total and push QBO negative.

Example:

```text
ACTION BITTERS50ml*20   Current Volume  15 of 20 Each    ->  15 units
ACTION BITTERS50ml*120  Current Volume -30 of 120 Each   ->   0 units after clamp
Grouped ACTION BITTERS50ml expected quantity             ->  15 units
```

The audit and pipeline summary expose the policy through:

- `epos_negative_rows_clamped`
- `epos_negative_units_clamped`
- `epos_negative_stock_policy`
- `epos_negative_clamped_row_names` in the audit CSV

QBO pack variant rows are treated as separate QBO inventory items with their own raw `QtyOnHand` during catalog cleanup. QBO pack `QtyOnHand` is not multiplier-expanded. After cleanup, the final exact-match quantity adjustment forces the base item to the EPOS expected quantity.

This assumption should be revisited if future categories show QBO pack quantity semantics that differ by product family or company setup.

## Created Base Item Price/Cost

When the pipeline creates a missing base item from an existing pack variant, it currently copies `UnitPrice` and `PurchaseCost` from the source pack item. That may be carton-level pricing, not single-unit pricing. The summary records the source pack item and copied values in `created_base_details` so operators can review them.

Future improvement: derive base item price/cost from EPOS sale/cost fields or an explicit product mapping.

## During Runs

Avoid manual QBO inventory edits while a run is active. The pipeline relies on a fresh QBO snapshot, posts catalog and quantity changes, marks the snapshot stale after writes, then refreshes before re-auditing.

Retention and destructive cleanup are intentionally separate from this workflow. See `docs/ARTIFACT_RETENTION_PLAN.md`.
