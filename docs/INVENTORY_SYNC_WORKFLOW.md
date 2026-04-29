# Inventory Sync Workflow

> **Historical/background document:** For the current operator-facing workflow and CLI entrypoint, start with [`docs/INVENTORY_SYNC.md`](docs/INVENTORY_SYNC.md).

This document covers how inventory data flows from EPOS Now into QuickBooks
Online (QBO), why the design is shaped the way it is, and the exact steps an
operator runs to bring a category of products into a clean, syncable state.

It complements `docs/DEV_STAGE_SETUP.md` (developer environment) and
`README.md` (high-level project overview). For sales receipts, see the
existing pipeline docs — that flow is **separate** from inventory sync.

---

## EPOS is the source of truth for physical stock

Earlier this year, when products in `company_a` were converted from QBO
Service items to Inventory items, many old products were inactivated and new
inventory items were created using the same product names. Some of those
new inventory items were seeded with a default starting quantity (often
around 10) because accurate counts were not available at the time.

Today, the team is doing real category-by-category stock counts in EPOS
Now, and **EPOS is the canonical record of physical stock**. The
inventory-sync tooling treats QBO `QtyOnHand` as a number to reconcile
against EPOS, **not** as historical truth.

If a category has been counted in EPOS, that EPOS data is the target. The
job of the tooling is to bring QBO into agreement with it — not to merge
or migrate stale QBO numbers into something new.

---

## One active QBO inventory item per base product

Business policy: QBO should track **one active inventory item per base
product, in single units**. Pack-size variants like `*6`, `*12`, `*24`
should not exist as separate active inventory items.

If a crate has 12 bottles and EPOS has 2 crates, QBO holds **24 bottles**
against the base item, **not 2 against a `*12` item**. If someone opens a
crate and sells one bottle, QBO reflects 23 bottles — not "1 pack plus 11
loose."

Why we landed here:

* The sales-receipt pipeline already implements this policy (see
  `code_scripts/transform.py::strip_pack_multiplier` and the
  `aggregate_products` config option).
* The QBO API treats each `*N` variant as its own inventory item with its
  own `QtyOnHand`. Having multiple variants per base product means every
  base name resolves to several QBO items — which is exactly what made
  the inventory-sync audit return `ambiguous_in_qbo` for 114 of 147 alcohol
  products in the first prod run.
* Single units everywhere keeps EPOS, QBO, and operator mental models in
  sync.

The tooling implements that policy in two operational steps below.

---

## The two-step operational flow

For each product or category we want to bring into sync:

### Step 1 — Pack-variant consolidation (sets QBO to the EPOS truth)

`code_scripts.qbo_pack_variant_consolidation` posts **one**
`InventoryAdjustment` per base product that:

1. Sets the active exact-base item's `QtyOnHand` to the EPOS single-unit
   target (`base_qty_diff_to_target`).
2. Sets every active pack-variant item's `QtyOnHand` to **0**
   (`pack_variant_qty_diffs_to_zero`).

After this step the truth is in the base item; pack variants are at zero
but still active. Inventory adjustments use:

* `AdjustAccountRef` = `qbo.inventory_adjustment_account_id` from the
  company config (see "QBO config requirement" below).
* `DocNumber` = `INVCON-{YYYYMMDD}-{base_item_id}` (deterministic — a
  same-day rerun against the same base item collides on `DocNumber` and
  QBO surfaces it as a duplicate, surfaced as a `[DUPLICATE]` line in the
  CLI output rather than a silent retry).
* `PrivateNote` = a structured block recording base name, base id, EPOS
  target, pack item ids, and run scope.
* `TxnDate` = today (override with `--txn-date YYYY-MM-DD`).

### Step 2 — Pack-variant cleanup (renames and inactivates zero-qty packs)

After consolidation has driven pack-variant `QtyOnHand` to 0 for the
products in scope, `code_scripts.qbo_pack_variant_cleanup` runs a separate
sparse update per pack variant:

```
POST /v3/company/{realm}/item
{ "Id":..., "SyncToken":..., "sparse":true,
  "Name":"{original_name} (old-{item_id})", "Active":false }
```

Pack variants are renamed (so the base name remains free for any future
reuse) and `Active=false`. Cleanup will refuse to inactivate a variant
whose `QtyOnHand != 0` — that's why consolidation runs first.

After both steps, an inventory_sync audit on the same products should
report them as `in_sync`.

---

## QBO config requirement

`InventoryAdjustment` POSTs require an offsetting expense / cost-of-sales
account. We use **Inventory Shrinkage** (`Cost of Goods Sold`) for
`company_a`.

Set on the company JSON:

```json
{
  "qbo": {
    "inventory_adjustment_account_id": "82"
  }
}
```

Sources:

* QBO web UI → Accounting → Chart of Accounts → "Inventory Shrinkage" id
  `82` for `company_a`'s realm.
* Or via the CLI account-candidate query (see prior runbook).

If this is missing, `qbo_pack_variant_consolidation --apply` and
`inventory_sync --apply` both refuse to run with a clear error before any
QBO call is made.

`{COMPANY_KEY}_INVENTORY_ADJUSTMENT_ACCOUNT_ID` env var is also accepted
as an override.

---

## Safety caps & rollout sequence

Both apply tools enforce caps that the operator must explicitly raise to
post anything large or wide-reaching.

| Tool | Flag | Default | Effect |
|---|---|---|---|
| consolidation | `--max-products` | (required) | hard cap on rows posted |
| consolidation | `--max-abs-base-diff` | `1000` | block rows whose `\|base_qty_diff_to_target\|` exceeds the cap |
| consolidation | `--max-lines` | `10` | block rows whose `planned_line_count` exceeds the cap |
| consolidation | `--product` / `--category` | (one required) | apply must be scoped — no whole-catalog runs |
| cleanup | `--max-items` | (required) | hard cap on items inactivated |
| inventory_sync | `--max-adjustments` | `25` | hard cap on lines posted |
| inventory_sync | `--max-qty-delta` | from `qbo.inventory_max_qty_delta` | per-item absolute qty-delta cap |

Both `--apply` paths also acquire `code_scripts.run_lock.GlobalRunLock`
and verify the QBO realm matches the runtime environment (sandbox vs
production) before posting.

### Recommended rollout sequence per category

1. **Audit first.** Run `inventory_sync` (audit mode, no `--apply`) for
   the category. Read the report — confirm `in_sync` / `needs_adjustment`
   / `ambiguous_in_qbo` totals look plausible.
2. **Plan consolidation.** Run `qbo_pack_variant_consolidation` (audit
   mode) for the same category. Read the consolidation report — the
   `Top by |base_qty_diff_to_target|` list is the easiest place to
   spot-check.
3. **Dry-run consolidation.** Re-run with `--dry-run --product "<one
   product>" --max-products 1` to see the exact `InventoryAdjustment`
   payload that `--apply` would POST. Verify the line list, account
   ref, and DocNumber.
4. **Pilot apply.** Run `--apply --product "<one product>" --max-products
   1`. Verify in QBO web UI that `QtyOnHand` for the base lands at the
   EPOS target and pack variants land at 0.
5. **Re-audit.** Run `inventory_sync` again, scoped to the product — it
   should now report `in_sync`.
6. **Scale up.** Increase `--max-products` slowly: 5, then 25, etc. Run
   `qbo_pack_variant_cleanup --apply --max-items 5` as a parallel
   pilot once consolidation has driven pack qtys to 0.
7. **Sweep the category.** Once the sample looks right, raise caps to
   process the remaining products in the category.

---

## Production validation summary

The two-step flow has been validated end-to-end on `company_a` for
products in `ALCOHOLS & SPIRITS`:

| Product | Notes |
|---|---|
| TROPHY LAGER CAN 500ML | Consolidated, pack variants inactivated, audit reports `in_sync` |
| TROPHY LAGER BOTTLE-600ML | Consolidated, pack variant inactivated, `in_sync` |
| SEAMANS PREMIUM SCHNAPPS750ml | `in_sync` |
| LEGEND EXTRA STOUT BOTTLE 600ml | `in_sync` |
| HUGELBRUDER-LONG RIDER WHISKY200ml | `in_sync` |
| BUDWEISER BOTTLE 600ml | `in_sync` |
| ACTION BITTERS750ml | `in_sync` |

These validate:

* `qbo_pack_variant_consolidation --apply` posts one
  `InventoryAdjustment` per base, with the right line count, the right
  `AdjustAccountRef`, and the deterministic `DocNumber`.
* `qbo_pack_variant_cleanup --apply` renames + inactivates zero-qty pack
  variants.
* `inventory_sync` re-audit reports the result as `in_sync`.

Other categories will follow the same recipe once their EPOS counts are
finalised.

---

## Slack notifications

When a company config has `slack_webhook_url` set, apply operations post a
short summary to Slack on completion. Audit-only `inventory_sync` runs are
quiet by default for exploratory CLI use; pass `--notify-slack` to opt in, or
run through the portal/job runner path where `OIAT_RUN_JOB_ID` marks the run as
an operational job. Dry-runs remain console/report-only.

The formatters live in `code_scripts/inventory_notifications.py` and produce a
consistent layout: title, company name + key, mode, scope, counts, report path,
optional warnings/error.

Slack failures are non-blocking — a failed notify is logged but never
fails the underlying operation.
