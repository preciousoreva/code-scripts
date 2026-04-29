## Artifact retention plan (local)

### Goal
Make pipeline and inventory operations **auditable and recoverable** without letting local disks fill up or losing the “ground truth” evidence used to justify a change (especially inventory adjustments posted to QBO).

EPOS stock remains the source of truth for inventory counts; our retention policy should preserve the evidence trail for how QBO was brought into alignment.

### Proposed local directory structure

```
/data/code_scripts/
  artifacts/
    <company_key>/
      sales_sync/
      inventory_sync/       # inventory pipeline audit CSVs + summaries
      pack_variant/         # lower-level debug tools (if/when used)
  archive/
    <company_key>/
  backups/
```

### What to keep and for how long

- **Keep indefinitely**
  - Final reports (inventory audit CSVs, reconciliation summaries)
  - Inventory adjustment reports (including all lines posted)
  - QBO transaction metadata for posted changes (DocNumber, Id, TxnDate, payload hash or equivalent)
  - Any operator-approved “final” artifacts used as the source for a production decision

- **Keep 180 days**
  - EPOS stock reports (raw exports used for inventory syncing)
  - QBO inventory snapshots (item exports used for comparing QtyOnHand)
  - Raw sales exports (unless a future compliance requirement or reconciliation policy requires longer)

- **Keep 90 days**
  - Dry-run payload previews and intermediate reports generated for operator review
  - Temporary “work-in-progress” CSVs that are not the final artifact of record

- **Backups**
  - Manual retention for now (until a rotation policy exists)
  - Once a rotation policy is added, define: frequency, retention period, and restore test cadence

### Operational notes

- This document proposes a structure and policy only.
- Do **not** move files or change existing artifact paths yet (backward compatibility).
- Current artifact locations remain under the configured state root (locally `runtime/`, in Docker `/data`), for example:
  - `code_scripts/Uploaded/`
  - `code_scripts/reports/inventory_sync/` (audit CSVs)
  - `code_scripts/reports/inventory_pipeline/` (pipeline summaries; includes `child_reports.final_audit`)
- In a future iteration, add a janitor/rotation job that enforces the policy safely (dry-run first, then delete).

### Future option (not implemented now)

- Migrate long-term artifact storage to **Cloudflare R2** or other **S3-compatible object storage**.
- Keep the local directory layout as the staging/working area, then sync “keep indefinitely” artifacts to object storage.
- Do **not** implement remote storage in this branch.

