# Bank Reconciliation — Codebase Review

This document summarizes what the EPOS-to-bank reconciliation code is designed to do, what it actually does, and what you can expect from it.

---

## 1. Two Different “Reconciliation” Concepts in the Repo

There are **two separate reconciliation flows**; only one is bank reconciliation:

| Concept | Where | Purpose |
|--------|--------|---------|
| **EPOS vs QBO reconciliation** | `run_pipeline.py` Phase 4, `qbo_query.py`, dashboard views | Compares **EPOS daily totals** to **QuickBooks Online** totals after upload. Dashboard “Reconciled EPOS Revenue”, MATCH/MISMATCH, and reconciliation warnings come from this. **Not** bank reconciliation. |
| **EPOS to Bank reconciliation** | `code_scripts/reconciliation/`, `reconcile_sales_to_bank.py` | Matches **EPOS receipt-level sales** to **Moniepoint bank statement credits**. This is the actual **bank reconciliation** and is what this document focuses on. |

---

## 2. What the Bank Reconciliation Is **Designed** to Do (Docs / README)

From `code_scripts/scripts/reconciliation/README.md` and package docstrings:

1. **Reconstruct receipts** from the BookKeeping CSV by grouping on Date/Time, Device Name, Staff, Location Name, and Tender; compute gross, service fee, expected credit, and EMTL.
2. **Parse Moniepoint statement xlsx**: account number from header, transaction table (Date, Narration, Debit, Credit, Balance), and link EMTL debits to nearby credits.
3. **Match** electronic EPOS receipts to bank credits with amount tolerance (default ₦1), time window (default 180 minutes), and 1:1 greedy best-fit.
4. **Write** to `code_scripts/reports/reconciliation/<company>/<date>/`:
   - `epos_to_bank_matches.csv` (receipt-centric)
   - `bank_credits_to_epos.csv` (bank-centric)
   - `reconciliation_summary.json` (totals, match rate, EMTL)
   - `debug_unmatched_epos.csv` / `debug_unmatched_bank.csv`

Tender handling is specified as: Card/Transfer → electronic (eligible for matching); Cash → not expected as bank credit (`UNMATCHED_CASH`); Mixed → `REVIEW_MIXED_TENDER`.

---

## 3. What the Code **Actually** Does (Current Implementation)

### 3.1 Entry point and flow

- **CLI:** `python -m code_scripts.scripts.reconciliation.reconcile_sales_to_bank --company company_a --date YYYY-MM-DD`
- Resolves paths from `--company` or `--base-dir` / explicit paths (e.g. `docs/<company>/bank-reconciliation/`, `epos/<date>/`, `statements/<date>/`, `account_mapping.csv`).
- Discovers BookKeeping CSV for the date (prefer `ORIGINAL_BookKeeping_*` when `--prefer-original` is true).
- Loads receipts → loads all statement xlsx → filters candidate credits → runs matching → writes all five outputs.

### 3.2 Receipt reconstruction (`epos_receipts.py`)

- **Grouping key:** (Date/Time, Device Name, Staff, Location Name, Tender).
- **Aggregation:** `gross_amount = sum(TOTAL Sales)` per group; `line_count = len(rows)`.
- **Tender classification:** `Card` / `Transfer` → electronic; `Cash` → cash; if `/` or both “cash” and “card”/“transfer” → mixed; other single tenders treated as electronic.
- **Fee model** (`fees.py`): `service_fee = min(0.005 * gross, 100)`, `expected_credit = gross - service_fee`, `expected_emtl = 50` if `gross >= 10_000` else `0`.
- **Receipt id:** deterministic hash of group key + gross.
- Collision flag exists on the model but is never set to `True` in `build_receipts` (comment says “same key, different gross” would be collision; current grouping produces one receipt per key).

### 3.3 Bank statements (`bank_statements.py`)

- **Parsing:** openpyxl; find “Account Number” in first rows; find header row with Date, Narration, Debit, Credit (and optional Reference, Balance); parse data rows.
- **EMTL:** Rows with “Electronic Money Transfer Levy” in narration are marked `is_emtl`; `link_emtl_to_credits()` attaches each EMTL debit to the closest **preceding** credit in the same account within 10 minutes; `linked_emtl_amount` / `linked_emtl_posted_at` set on the **credit**.
- **Candidate credits for matching:** `filter_candidate_credits()` keeps only rows with `credit > 0` and `"PURCHASE FOR" in narration`, and (if `date_filter` is set) `posted_at` date equals the given YYYY-MM-DD. So only “PURCHASE FOR” credits on that calendar day are used.

### 3.4 Matching (`matcher.py` — current 129-line version)

- **Eligible receipts:** Only those with `tender_kind == ELECTRONIC` are matched; Cash → `UNMATCHED_CASH`, Mixed → `REVIEW_MIXED_TENDER`, collision → `REVIEW_COLLISION`.
- **Algorithm:** Sort electronic receipts by `receipt_datetime`. For each receipt, collect **candidates**: bank credits with `|bank.credit - receipt.expected_credit| <= amount_tolerance` and `0 <= (bank.posted_at - receipt.receipt_datetime) <= time_window_mins`.
- **Scoring:** `score = amount_diff + (time_diff_minutes / 60)`; if narration contains “PURCHASE FOR”, score is reduced by 0.1 (prefer these).
- **Assignment:** Best score among **unused** credits; if two candidates have scores within 0.01, receipt gets `REVIEW_MULTIPLE_CANDIDATES` and no bank is assigned; otherwise assign and mark bank as used.
- **Output:** List of `Match` (receipt + optional bank_txn + status) and list of unmatched bank credits.
- **Not used in this version:** No fee-learning, no relaxed/bundled passes, no “transfer” vs “purchase” filter modes. The CLI only calls this simple `run_matching(receipts, bank_credits, amount_tolerance, time_window_mins, account_name_by_number)`.

### 3.5 Account mapping (`account_mapping.py`)

- Loads CSV with columns like “QBO / Bank Statement Account Number” and “Monipoint Account Name” (or similar); returns `dict[account_number, account_name]` used only for labelling in reports (e.g. `matched_account_name`), not for filtering or matching logic.

### 3.6 Reports (`report_writer.py`)

- **epos_to_bank_matches.csv:** One row per receipt with receipt fields, match status, and (if matched) bank account, posted_at, credit, reference, narration, time_diff, amount_diff.
- **bank_credits_to_epos.csv:** One row per bank credit (matched + unmatched); matched rows include receipt_id and expected gross.
- **debug_unmatched_epos.csv:** All non-MATCHED receipts (subset of columns).
- **debug_unmatched_bank.csv:** Unmatched credits (credit > 0).
- **reconciliation_summary.json:** `build_summary()` computes totals by tender (electronic/cash/mixed gross), total EPOS gross, total bank credits, inferred service fees sum, EMTL count/sum, counts per `MatchStatus`, `matched_count`, `electronic_receipt_count`, `match_rate_percent` = matched_count / electronic_count × 100.

The **current** `build_summary()` does **not** include fields like `match_rate_percent_strict`, `REVIEW_RELAXED_MATCH`, `REVIEW_BUNDLED_MATCH`, `credit_filter_mode`, `profile`, or `reason_counts`. If you have a `reconciliation_summary.json` on disk with those fields, it was produced by a different or older version of the matcher/report writer (e.g. with multi-pass matching and fee learning).

---

## 4. What You Can **Expect** It to Do (and Gaps)

**Aligned with design:**

- Run per company/date with standard folder layout.
- Rebuild receipts from BookKeeping CSV with the stated fee/EMTL model.
- Parse Moniepoint xlsx and link EMTL to credits.
- Restrict matching to electronic tenders; treat cash/mixed as non-matched/review.
- Produce receipt-centric and bank-centric CSVs plus debug files and a summary with match rate and totals.

**Gaps / caveats:**

1. **Only “PURCHASE FOR” credits** are used. Any other same-day credits (e.g. NIP transfer, other narrations) are ignored for matching, so they will appear as unmatched bank credits even if they correspond to EPOS sales.
2. **Single-day filter:** Bank credits are filtered by exact `posted_at` date (YYYY-MM-DD). There is no ±1 day or timezone handling, so late-posted or timezone-boundary cases may be excluded.
3. **Fee learning not used:** `fee_learning.py` and `FeeAdjustmentModel` exist and tests reference them, but the CLI never passes a fee model or enables fee learning, so the current run uses only the fixed fee formula.
4. **Extended matcher not wired:** The repo references (e.g. in tests and in your summary JSON) suggest a richer matcher with strict/relaxed/bundled passes, transfer vs purchase filter, and more statuses. The **in-repo** `matcher.py` is the simple 129-line version; the CLI and `run_matching()` signature match that. So either that extended logic lives in another branch or the CLI was simplified and the summary you have came from an older run.
5. **Collision never set:** Receipts are never marked with `collision=True`, so `REVIEW_COLLISION` is effectively unused with current grouping.
6. **EMTL in matching:** EMTL is parsed and linked to credits for reporting; the matcher compares `bank.credit` to `receipt.expected_credit` (post fee, no EMTL). So the 50 Naira EMTL is not added to the expected credit when matching; that’s consistent with “credit = gross - fee” and EMTL as a separate debit.

---

## 5. Summary Table

| Aspect | Intended (docs) | Current behaviour |
|--------|------------------|-------------------|
| Inputs | BookKeeping CSV + Moniepoint xlsx + account mapping | Same; paths from company or base-dir. |
| Receipts | Group by date/time, device, staff, location, tender; fee model | Same; collision never set. |
| Bank candidates | Credits for matching | Only credits with “PURCHASE FOR” and posted on the given date. |
| Matching | 1:1, amount tolerance, time window | Same; greedy by score (amount + time); no fee learning. |
| Outputs | 5 files (2 CSVs, 2 debug, 1 summary) | Same; summary has basic match rate and totals only. |
| Extra (in your JSON / tests) | Strict/relaxed/bundled, transfer filter, fee learning, reason counts | Not present in current CLI/matcher; likely from another version. |

---

## 6. Related Documentation

- **Usage and folder convention:** `code_scripts/scripts/reconciliation/README.md`
- **Standard layout:** `docs/<company>/bank-reconciliation/` with `account_mapping.csv`, `epos/<YYYY-MM-DD>/`, `statements/<YYYY-MM-DD>/`
