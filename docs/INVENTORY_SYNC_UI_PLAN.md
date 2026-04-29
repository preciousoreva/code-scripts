# Inventory Sync — Django UI plan

> **Historical planning document:** The portal now ships a simplified Runs → Inventory workflow. This doc captures an earlier plan for exposing low-level inventory tools in the UI; it is intentionally not implemented in the current portal.

## Why this is a plan, not code

The portal already has a working **Inventory Audit** trigger UI that was
shipped with the inventory-sync slice (see
`apps/epos_qbo/forms.py::InventoryTriggerForm`,
`apps/epos_qbo/views.py::trigger_inventory_run`,
`apps/epos_qbo/templates/epos_qbo/runs.html`). Adding portal triggers for
the **pack-variant consolidation** and **pack-variant cleanup** tools
follows the same pattern but is **not** a small change — it's a
multi-file, schema-touching addition. We're documenting the plan now so
the next slice can land cleanly without surprises.

The polish branch (`inventory-pipeline-polish`) intentionally does **not**
ship UI changes for the new tools. The CLI is already battle-tested in
production (`docs/INVENTORY_SYNC_WORKFLOW.md`), and shipping a half-built
UI would be more risky than waiting.

---

## What the UI needs to do

Five operator actions, each surfaced as a portal trigger like the
existing Sales / Inventory tabs on `/runs/`:

| # | Action | Backing CLI | Mode |
|---|---|---|---|
| 1 | Inventory Audit | `code_scripts.inventory_sync` (no flags) | audit-only |
| 2 | Pack-Variant Consolidation Plan | `code_scripts.qbo_pack_variant_consolidation` (no flags) | audit-only |
| 3 | Pack-Variant Consolidation Apply | `code_scripts.qbo_pack_variant_consolidation --apply` | apply |
| 4 | Pack-Variant Cleanup Audit | `code_scripts.qbo_pack_variant_cleanup` (no flags) | audit-only |
| 5 | Pack-Variant Cleanup Apply | `code_scripts.qbo_pack_variant_cleanup --apply` | apply |

(Action 1 already exists; the remaining four are new.)

Each action exposes the safe knobs the operator needs:

| Form field | Used by | CLI flag |
|---|---|---|
| company | all 5 | `--company` |
| category | 1, 2, 3 | `--category` |
| product | 1, 2, 3, 4, 5 | `--product` |
| stock CSV path or auto-download | 1, 2, 3 | `--stock-csv` / auto-download |
| max products | 3 | `--max-products` |
| max abs base diff | 3 | `--max-abs-base-diff` (default 1000) |
| max lines | 3 | `--max-lines` (default 10) |
| max items | 5 | `--max-items` |
| dry-run | 3, 5 | `--dry-run` (mutually exclusive with apply) |

Apply actions (3 + 5) MUST require explicit caps and require either
`--product` or `--category` to scope the run. Whole-catalog applies stay
disallowed at the form level (matches CLI behaviour).

---

## Implementation outline

The portal already handles the `inventory_sync` trigger via
`RunJob(scope=SCOPE_INVENTORY_SYNC)` and a `_build_inventory_command`
helper in `apps/epos_qbo/services/job_runner.py`. The pattern extends
cleanly.

### 1. Schema — new `RunJob.SCOPE_*` choices and a migration

`apps/epos_qbo/models.py::RunJob`:

```python
SCOPE_PACK_VARIANT_CONSOLIDATION = "pack_variant_consolidation"
SCOPE_PACK_VARIANT_CLEANUP       = "pack_variant_cleanup"
SCOPE_CHOICES = [
    ...,
    (SCOPE_PACK_VARIANT_CONSOLIDATION, "Pack-Variant Consolidation"),
    (SCOPE_PACK_VARIANT_CLEANUP,       "Pack-Variant Cleanup"),
]
```

The existing `inventory_options_json: JSONField` already handles
arbitrary CLI options for the inventory-sync trigger. The two new tools
have different option sets, so use **separate JSON fields** rather than
overloading:

```python
consolidation_options_json = models.JSONField(default=dict, blank=True)
cleanup_options_json       = models.JSONField(default=dict, blank=True)
```

Plus widen `RunSchedule.scope` to mirror the new choices (matching the
0010 migration's pattern for `SCOPE_INVENTORY_SYNC`).

Migration: `0011_pack_variant_scopes.py`. Purely additive.

### 2. Forms — two new triggers

`apps/epos_qbo/forms.py`:

```python
class PackVariantConsolidationTriggerForm(forms.Form):
    company_key      = forms.SlugField(max_length=64)
    category         = forms.CharField(max_length=255, required=False)
    product          = forms.CharField(max_length=255, required=False)
    stock_csv        = forms.CharField(max_length=1024, required=False)
    auto_fetch_qbo   = forms.BooleanField(required=False, initial=True)
    apply            = forms.BooleanField(required=False)
    dry_run          = forms.BooleanField(required=False)
    max_products     = forms.IntegerField(required=False, min_value=1)
    max_abs_base_diff = forms.FloatField(required=False, min_value=0, initial=1000)
    max_lines        = forms.IntegerField(required=False, min_value=1, initial=10)

    def clean(self):
        cleaned = super().clean()
        # apply XOR dry_run; apply requires max_products + (product OR category)
        ...
        return cleaned


class PackVariantCleanupTriggerForm(forms.Form):
    company_key   = forms.SlugField(max_length=64)
    category      = forms.CharField(max_length=255, required=False)
    product       = forms.CharField(max_length=255, required=False)
    auto_fetch_qbo = forms.BooleanField(required=False, initial=True)
    apply         = forms.BooleanField(required=False)
    dry_run       = forms.BooleanField(required=False)
    max_items     = forms.IntegerField(required=False, min_value=1)

    def clean(self):
        cleaned = super().clean()
        # apply XOR dry_run; apply requires max_items
        ...
        return cleaned
```

### 3. Views — two new POST endpoints

`apps/epos_qbo/views.py`:

```python
@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def trigger_pack_variant_consolidation(request):
    form = PackVariantConsolidationTriggerForm(request.POST)
    ...
    job = RunJob.objects.create(
        scope=RunJob.SCOPE_PACK_VARIANT_CONSOLIDATION,
        company_key=...,
        consolidation_options_json={...},
        requested_by=request.user,
        status=RunJob.STATUS_QUEUED,
    )
    dispatch_next_queued_job()
    ...


@login_required
@permission_required("epos_qbo.can_trigger_runs", raise_exception=True)
@require_POST
def trigger_pack_variant_cleanup(request):
    ...  # mirror trigger_pack_variant_consolidation
```

### 4. Job runner — two new command builders

`apps/epos_qbo/services/job_runner.py::build_command()`:

```python
if scope == RunJob.SCOPE_PACK_VARIANT_CONSOLIDATION:
    return _build_pack_variant_consolidation_command(python_exe, cleaned)
if scope == RunJob.SCOPE_PACK_VARIANT_CLEANUP:
    return _build_pack_variant_cleanup_command(python_exe, cleaned)
```

Each `_build_*_command` translates the JSON options dict into the
right `python -m code_scripts.qbo_pack_variant_*` argv list, mirroring
the existing `_build_inventory_command` pattern.

### 5. Artifact ingestion — two new artifact kinds

`apps/epos_qbo/models.py::RunArtifact.KIND_*` already has
`KIND_INVENTORY_AUDIT`. Add:

```python
KIND_PACK_VARIANT_CONSOLIDATION = "pack_variant_consolidation"
KIND_PACK_VARIANT_CLEANUP       = "pack_variant_cleanup"
```

Both consolidation and cleanup tools already write a CSV report
(`qbo_pack_variant_*` reports under `OPS_REPORTS_DIR`). The ingestion
service can scan those directories the same way it scans
`inventory_audit_*.json` today, but **it needs an upstream change to
the CLIs**: each tool should also write a small JSON sidecar per run
(like `inventory_audit_*.json`) so artifact ingestion has a single
canonical metadata file to parse. That's the only **CLI change**
the UI work needs.

### 6. URLs

```python
path("runs/trigger-consolidation", views.trigger_pack_variant_consolidation, name="run-trigger-consolidation"),
path("runs/trigger-cleanup",       views.trigger_pack_variant_cleanup,       name="run-trigger-cleanup"),
```

### 7. Templates

Extend the existing tabbed trigger panel on
`apps/epos_qbo/templates/epos_qbo/runs.html` to add **two more tabs**:
"Pack Consolidation" and "Pack Cleanup", with the form fields above.

The current `dashboard_company_cards.html` shows "Latest Inventory
Audit" per company; consider adding "Latest Consolidation" and
"Latest Cleanup" chips once the artifact kinds exist.

### 8. Tests

For each new view + form + job runner builder + artifact kind, add tests
mirroring the existing `apps/epos_qbo/tests/test_inventory_*.py` files:

* form validation (apply XOR dry-run, scope required for apply)
* view permission gates
* job_runner argv list matches CLI
* artifact ingestion picks up the new sidecar files

---

## Estimated scope

| Item | Files touched |
|---|---|
| Schema + migration | `models.py`, `migrations/0011_pack_variant_scopes.py` |
| Forms | `forms.py` (+2 classes) |
| Views | `views.py` (+2 functions) |
| URLs | `urls.py` (+2 paths) |
| Job runner | `services/job_runner.py` (+2 helpers, +2 dispatch branches) |
| Artifact ingestion | `services/artifact_ingestion.py` (+2 ingestors), upstream CLI sidecar emission |
| Templates | `templates/epos_qbo/runs.html` (+2 tab panels) |
| Tests | `tests/test_pack_variant_*.py` (new) |

This is a fair-sized slice — probably its own PR — but it's mechanical:
every file change has an existing analogue from the inventory-sync slice.

---

## Out of scope for the first UI slice

* Scheduled (cron) consolidation / cleanup runs. `RunSchedule` doesn't
  yet carry pack-variant options. Audit runs are cheap enough to schedule
  unconditionally; apply runs should stay manual until we have more
  production runtime.
* "Approve from chip" UX — clicking a Slack message to trigger a run.
  Out of scope; the CLI / portal trigger is enough for now.
* Inline editing of `qbo.inventory_adjustment_account_id` from the
  portal. The company advanced settings page can pick this up in a
  separate slice once we want non-technical operators editing it.

---

## When this should ship

After at least one more production category (beyond ALCOHOLS & SPIRITS)
has been brought into sync via the CLI flow. That gives us:

1. Confidence that the safety caps in the apply tools are calibrated
   correctly for real categories.
2. A second prod data point for testing the artifact ingestion against
   non-alcohol shapes.

Until then, the workflow doc plus the existing CLI is the documented
path.
