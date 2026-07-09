#!/usr/bin/env python3
"""Plan and optionally delete historical QBO InventoryAdjustment transactions.

This is a remediation-only utility. It does not run inventory sync, create new
InventoryAdjustments, or touch sales transactions.
"""

from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from code_scripts.artifact_paths import artifact_day_stamp
from code_scripts.company_config import get_available_companies, get_qbo_api_base_url, load_company_config
from code_scripts.load_env import load_env_file
from code_scripts.qbo_upload import TokenManager, _make_qbo_request
from code_scripts.token_manager import verify_realm_match


MINORVERSION = "70"
DEFAULT_NUMBER_PREFIX = "INVCON"
CONFIRM_DELETE_FLAG = "--confirm-delete-inventory-adjustments"
DEFAULT_EXCLUDED_NUMBERS = {
    "INVCON-20260430-14620",
    "INVCON-20260430-14607",
    "INVCON-20260430-12742",
}

PLAN_FIELDNAMES = [
    "company_key",
    "realm_id",
    "txn_date",
    "doc_number",
    "qbo_transaction_id",
    "source_family",
    "expected_impact",
    "memo_private_note",
    "line_count",
    "item_names",
    "qty_changes",
    "action",
    "reason",
    "status",
]

DELETE_FIELDNAMES = [
    *PLAN_FIELDNAMES,
    "result",
    "error_message",
    "deleted_at",
    "operator_host",
    "operator_user",
]


def _parse_date(value: str) -> str:
    text = str(value or "").strip()
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"date must be YYYY-MM-DD, got {value!r}") from exc
    return text


def _parse_optional_float(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    cleaned = (
        text.replace(",", "")
        .replace("₦", "")
        .replace("NGN", "")
        .replace("$", "")
        .strip()
    )
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def _impact_sort_value(row: dict[str, Any]) -> float:
    parsed = _parse_optional_float(row.get("expected_impact"))
    return parsed if parsed is not None else float("-inf")


def _source_family(doc_number: str) -> str:
    return str(doc_number or "").strip().split("-", 1)[0].upper()


def _normalize_doc_number(value: Any) -> str:
    return str(value or "").strip()


def _candidate_doc_column(fieldnames: list[str]) -> str | None:
    aliases = {
        "doc_number",
        "docnumber",
        "reference_number",
        "reference",
        "ref_number",
        "txn_number",
        "transaction_number",
        "number",
    }
    for name in fieldnames:
        if name.strip().lower().replace(" ", "_") in aliases:
            return name
    return None


def _candidate_impact_column(fieldnames: list[str]) -> str | None:
    aliases = {
        "expected_impact",
        "impact",
        "inventory_shrinkage_impact",
        "shrinkage_impact",
        "amount",
        "net_impact",
    }
    for name in fieldnames:
        if name.strip().lower().replace(" ", "_") in aliases:
            return name
    return None


def load_candidate_csv(path: Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        doc_col = _candidate_doc_column(fieldnames)
        if not doc_col:
            raise ValueError(
                "candidate CSV must include a transaction number column "
                "(doc_number, reference_number, txn_number, or similar)."
            )
        impact_col = _candidate_impact_column(fieldnames)
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in reader:
            doc = _normalize_doc_number(raw.get(doc_col))
            if not doc or doc in seen:
                continue
            seen.add(doc)
            candidates.append(
                {
                    "doc_number": doc,
                    "expected_impact": str(raw.get(impact_col) or "").strip() if impact_col else "",
                    "source_row": dict(raw),
                }
            )
    return candidates


def _qbo_query(token_mgr: TokenManager, realm_id: str, query: str) -> dict[str, Any]:
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/query?query={quote(query)}&minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        raise RuntimeError(f"QBO query failed: HTTP {resp.status_code}: {resp.text[:1000]}")
    return resp.json()


def query_inventory_adjustments(
    token_mgr: TokenManager,
    realm_id: str,
    *,
    from_date: str,
    to_date: str,
    maxresults: int = 1000,
) -> list[dict[str, Any]]:
    adjustments: list[dict[str, Any]] = []
    startposition = 1
    while True:
        query = (
            "select Id, DocNumber, TxnDate, SyncToken, PrivateNote from InventoryAdjustment "
            f"where TxnDate >= '{from_date}' and TxnDate <= '{to_date}' "
            f"startposition {startposition} maxresults {maxresults}"
        )
        data = _qbo_query(token_mgr, realm_id, query)
        items = data.get("QueryResponse", {}).get("InventoryAdjustment", [])
        if not isinstance(items, list):
            items = [items] if items else []
        adjustments.extend(items)
        if len(items) < maxresults:
            break
        startposition += maxresults
    return adjustments


def query_inventory_adjustments_by_doc_numbers(
    token_mgr: TokenManager,
    realm_id: str,
    doc_numbers: list[str],
) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for i in range(0, len(doc_numbers), 50):
        batch = doc_numbers[i : i + 50]
        safe = "', '".join(doc.replace("'", "''") for doc in batch)
        query = (
            "select Id, DocNumber, TxnDate, SyncToken, PrivateNote from InventoryAdjustment "
            f"where DocNumber in ('{safe}')"
        )
        data = _qbo_query(token_mgr, realm_id, query)
        items = data.get("QueryResponse", {}).get("InventoryAdjustment", [])
        if not isinstance(items, list):
            items = [items] if items else []
        for item in items:
            doc = _normalize_doc_number(item.get("DocNumber"))
            if doc:
                full = fetch_inventory_adjustment(token_mgr, realm_id, str(item.get("Id") or "")) if item.get("Id") else None
                found[doc] = full or item
    return found


def fetch_inventory_adjustment(
    token_mgr: TokenManager,
    realm_id: str,
    adjustment_id: str,
) -> dict[str, Any] | None:
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/inventoryadjustment/{adjustment_id}?minorversion={MINORVERSION}"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise RuntimeError(f"QBO fetch failed for InventoryAdjustment {adjustment_id}: HTTP {resp.status_code}: {resp.text[:1000]}")
    return (resp.json() or {}).get("InventoryAdjustment") or None


def _line_summary(adjustment: dict[str, Any]) -> tuple[int, str, str]:
    names: list[str] = []
    qtys: list[str] = []
    lines = adjustment.get("Line") or []
    if not isinstance(lines, list):
        lines = [lines] if lines else []
    for line in lines:
        detail = line.get("ItemAdjustmentLineDetail") or {}
        item_ref = detail.get("ItemRef") or {}
        name = str(item_ref.get("name") or item_ref.get("value") or "").strip()
        if name:
            names.append(name)
        qty = detail.get("QtyDiff", detail.get("NewQty", ""))
        if str(qty).strip():
            qtys.append(str(qty).strip())
    return len(lines), " | ".join(names), " | ".join(qtys)


def fetch_full_inventory_adjustments(
    token_mgr: TokenManager,
    realm_id: str,
    adjustments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    full_rows: list[dict[str, Any]] = []
    for adjustment in adjustments:
        adjustment_id = str(adjustment.get("Id") or "").strip()
        if not adjustment_id:
            full_rows.append(adjustment)
            continue
        full = fetch_inventory_adjustment(token_mgr, realm_id, adjustment_id)
        full_rows.append(full or adjustment)
    return full_rows


def _plan_row_from_adjustment(
    *,
    company_key: str,
    realm_id: str,
    adjustment: dict[str, Any] | None,
    doc_number: str,
    expected_impact: Any = "",
    action: str,
    reason: str,
    status: str,
) -> dict[str, Any]:
    adjustment = adjustment or {}
    line_count, item_names, qty_changes = _line_summary(adjustment)
    return {
        "company_key": company_key,
        "realm_id": realm_id,
        "txn_date": str(adjustment.get("TxnDate") or ""),
        "doc_number": doc_number,
        "qbo_transaction_id": str(adjustment.get("Id") or ""),
        "source_family": _source_family(doc_number),
        "expected_impact": "" if expected_impact is None else str(expected_impact),
        "memo_private_note": str(adjustment.get("PrivateNote") or ""),
        "line_count": str(line_count),
        "item_names": item_names,
        "qty_changes": qty_changes,
        "action": action,
        "reason": reason,
        "status": status,
    }


def build_plan(
    *,
    company_key: str,
    realm_id: str,
    qbo_adjustments: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]] | None,
    number_prefix: str,
    exclude_numbers: set[str],
    max_transactions: int | None,
    min_impact: float | None,
    allow_invadj: bool,
    token_mgr: TokenManager | None = None,
) -> list[dict[str, Any]]:
    prefix = str(number_prefix or DEFAULT_NUMBER_PREFIX).strip().upper()
    if prefix.startswith("INVADJ") and not allow_invadj:
        raise ValueError("Refusing to target INVADJ by default. Pass --allow-invadj only for an approved remediation run.")

    rows: list[dict[str, Any]] = []
    if candidate_rows is not None:
        candidates = sorted(candidate_rows, key=_impact_sort_value, reverse=True)
        found_by_doc: dict[str, dict[str, Any]] = {}
        if token_mgr is not None:
            found_by_doc = query_inventory_adjustments_by_doc_numbers(
                token_mgr,
                realm_id,
                [_normalize_doc_number(c["doc_number"]) for c in candidates],
            )
        for candidate in candidates:
            doc = _normalize_doc_number(candidate.get("doc_number"))
            expected_impact = candidate.get("expected_impact", "")
            adjustment = found_by_doc.get(doc)
            rows.append(
                _classify_plan_candidate(
                    company_key=company_key,
                    realm_id=realm_id,
                    doc_number=doc,
                    expected_impact=expected_impact,
                    adjustment=adjustment,
                    prefix=prefix,
                    exclude_numbers=exclude_numbers,
                    min_impact=min_impact,
                    allow_invadj=allow_invadj,
                )
            )
    else:
        for adjustment in qbo_adjustments:
            doc = _normalize_doc_number(adjustment.get("DocNumber"))
            rows.append(
                _classify_plan_candidate(
                    company_key=company_key,
                    realm_id=realm_id,
                    doc_number=doc,
                    expected_impact="",
                    adjustment=adjustment,
                    prefix=prefix,
                    exclude_numbers=exclude_numbers,
                    min_impact=min_impact,
                    allow_invadj=allow_invadj,
                )
            )

    selected = [row for row in rows if row["action"] == "delete_candidate"]
    selected.sort(key=_impact_sort_value, reverse=True)
    if max_transactions is not None:
        allowed_docs = {row["doc_number"] for row in selected[: max(0, int(max_transactions))]}
        for row in rows:
            if row["action"] == "delete_candidate" and row["doc_number"] not in allowed_docs:
                row["action"] = "skipped"
                row["reason"] = "max_transactions_limit"
                row["status"] = "skipped"

    return rows


def _classify_plan_candidate(
    *,
    company_key: str,
    realm_id: str,
    doc_number: str,
    expected_impact: Any,
    adjustment: dict[str, Any] | None,
    prefix: str,
    exclude_numbers: set[str],
    min_impact: float | None,
    allow_invadj: bool,
) -> dict[str, Any]:
    if not doc_number:
        return _plan_row_from_adjustment(
            company_key=company_key,
            realm_id=realm_id,
            adjustment=None,
            doc_number="",
            expected_impact=expected_impact,
            action="skipped",
            reason="missing_doc_number",
            status="skipped",
        )
    if doc_number in exclude_numbers:
        return _plan_row_from_adjustment(
            company_key=company_key,
            realm_id=realm_id,
            adjustment=adjustment,
            doc_number=doc_number,
            expected_impact=expected_impact,
            action="excluded",
            reason="excluded_transaction_number",
            status="skipped",
        )
    family = _source_family(doc_number)
    if family == "INVADJ" and not allow_invadj:
        return _plan_row_from_adjustment(
            company_key=company_key,
            realm_id=realm_id,
            adjustment=adjustment,
            doc_number=doc_number,
            expected_impact=expected_impact,
            action="skipped",
            reason="invadj_refused_by_default",
            status="skipped",
        )
    if prefix and not doc_number.upper().startswith(prefix):
        return _plan_row_from_adjustment(
            company_key=company_key,
            realm_id=realm_id,
            adjustment=adjustment,
            doc_number=doc_number,
            expected_impact=expected_impact,
            action="skipped",
            reason=f"doc_number_prefix_mismatch:{prefix}",
            status="skipped",
        )
    if min_impact is not None:
        parsed = _parse_optional_float(expected_impact)
        if parsed is None or parsed < min_impact:
            return _plan_row_from_adjustment(
                company_key=company_key,
                realm_id=realm_id,
                adjustment=adjustment,
                doc_number=doc_number,
                expected_impact=expected_impact,
                action="skipped",
                reason="below_min_impact",
                status="skipped",
            )
    if not adjustment:
        return _plan_row_from_adjustment(
            company_key=company_key,
            realm_id=realm_id,
            adjustment=None,
            doc_number=doc_number,
            expected_impact=expected_impact,
            action="already_missing",
            reason="not_found_in_qbo",
            status="skipped",
        )
    return _plan_row_from_adjustment(
        company_key=company_key,
        realm_id=realm_id,
        adjustment=adjustment,
        doc_number=doc_number,
        expected_impact=expected_impact,
        action="delete_candidate",
        reason="matches_remediation_criteria",
        status="planned",
    )


def delete_inventory_adjustment(
    token_mgr: TokenManager,
    realm_id: str,
    *,
    adjustment_id: str,
    sync_token: str,
) -> tuple[bool, str]:
    base_url = get_qbo_api_base_url()
    url = f"{base_url}/v3/company/{realm_id}/inventoryadjustment?operation=delete&minorversion={MINORVERSION}"
    resp = _make_qbo_request(
        "POST",
        url,
        token_mgr,
        json={"Id": str(adjustment_id), "SyncToken": str(sync_token)},
    )
    if resp.status_code in (200, 201):
        return True, ""
    try:
        body = resp.json()
        detail = body.get("Fault", {}).get("Error", [])
        msg = "; ".join((e.get("Message") or e.get("Detail") or str(e)) for e in detail) if detail else resp.text[:500]
    except Exception:
        msg = resp.text[:500] if resp.text else ""
    return False, f"HTTP {resp.status_code}: {msg}"


def apply_deletions(
    *,
    token_mgr: TokenManager,
    realm_id: str,
    plan_rows: list[dict[str, Any]],
    fail_fast: bool = False,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    operator_host = socket.gethostname()
    operator_user = getpass.getuser()
    for row in plan_rows:
        if row.get("action") != "delete_candidate":
            continue
        deleted_at = datetime.now(timezone.utc).isoformat()
        base = {
            **row,
            "deleted_at": deleted_at,
            "operator_host": operator_host,
            "operator_user": operator_user,
        }
        adjustment_id = str(row.get("qbo_transaction_id") or "").strip()
        sync_token = str(row.get("sync_token") or "").strip()
        if not sync_token:
            full = fetch_inventory_adjustment(token_mgr, realm_id, adjustment_id) if adjustment_id else None
            sync_token = str((full or {}).get("SyncToken") or "").strip()
        if not adjustment_id or not sync_token:
            result = {**base, "result": "skipped", "error_message": "missing qbo_transaction_id or SyncToken"}
            results.append(result)
            if fail_fast:
                break
            continue
        ok, error = delete_inventory_adjustment(
            token_mgr,
            realm_id,
            adjustment_id=adjustment_id,
            sync_token=sync_token,
        )
        result = {
            **base,
            "result": "deleted" if ok else "failed",
            "error_message": error,
        }
        results.append(result)
        if error and fail_fast:
            break
    return results


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _default_output_dir() -> Path:
    return Path("runtime") / "code_scripts" / "reports" / "qbo_inventory_remediation" / artifact_day_stamp()


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--company", required=True, choices=get_available_companies(), help="Company key")
    parser.add_argument("--from-date", required=True, type=_parse_date, help="Start TxnDate, YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, type=_parse_date, help="End TxnDate, YYYY-MM-DD")
    parser.add_argument("--number-prefix", default=DEFAULT_NUMBER_PREFIX, help="DocNumber prefix to target (default: INVCON)")
    parser.add_argument("--candidate-csv", type=Path, help="CSV containing transaction numbers and optional expected impact")
    parser.add_argument("--exclude-number", action="append", default=[], help="DocNumber to exclude from deletion; repeatable")
    parser.add_argument("--max-transactions", type=int, help="Maximum delete candidates to select/delete")
    parser.add_argument("--min-impact", type=float, help="Minimum expected impact when candidate CSV provides impact")
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir(), help="Directory for plan and deletion logs")
    parser.add_argument("--allow-invadj", action="store_true", help="Allow targeting INVADJ numbers for an approved remediation run")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plan or delete historical QBO InventoryAdjustment remediation candidates.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Write a dry-run remediation plan; no deletions")
    _add_common_args(plan)

    delete = subparsers.add_parser("delete", help="Delete selected candidates; requires explicit confirmation flags")
    _add_common_args(delete)
    delete.add_argument("--apply", action="store_true", help="Required for deletion")
    delete.add_argument(
        "--confirm-delete-inventory-adjustments",
        action="store_true",
        help="Second required confirmation for deleting QBO InventoryAdjustment transactions",
    )
    delete.add_argument("--fail-fast", action="store_true", help="Stop after the first deletion failure")
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if args.from_date > args.to_date:
        raise ValueError("--from-date must be <= --to-date")
    if args.max_transactions is not None and args.max_transactions <= 0:
        raise ValueError("--max-transactions must be > 0")
    if str(args.number_prefix or "").strip().upper().startswith("INVADJ") and not args.allow_invadj:
        raise ValueError("Refusing to target INVADJ by default. Pass --allow-invadj only for an approved remediation run.")
    if args.command == "delete":
        if not args.apply or not args.confirm_delete_inventory_adjustments:
            raise ValueError(f"delete requires --apply and {CONFIRM_DELETE_FLAG}")


def run(args: argparse.Namespace) -> int:
    _validate_args(args)
    load_env_file()
    config = load_company_config(args.company)
    verify_realm_match(config.company_key, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)
    exclude_numbers = set(DEFAULT_EXCLUDED_NUMBERS)
    exclude_numbers.update(_normalize_doc_number(v) for v in args.exclude_number if _normalize_doc_number(v))

    candidate_rows: list[dict[str, Any]] | None = None
    qbo_adjustments: list[dict[str, Any]] = []
    if args.candidate_csv:
        candidate_rows = load_candidate_csv(args.candidate_csv)
    else:
        qbo_adjustments = query_inventory_adjustments(
            token_mgr,
            config.realm_id,
            from_date=args.from_date,
            to_date=args.to_date,
        )
        qbo_adjustments = fetch_full_inventory_adjustments(token_mgr, config.realm_id, qbo_adjustments)

    plan_rows = build_plan(
        company_key=config.company_key,
        realm_id=config.realm_id,
        qbo_adjustments=qbo_adjustments,
        candidate_rows=candidate_rows,
        number_prefix=args.number_prefix,
        exclude_numbers=exclude_numbers,
        max_transactions=args.max_transactions,
        min_impact=args.min_impact,
        allow_invadj=bool(args.allow_invadj),
        token_mgr=token_mgr if candidate_rows is not None else None,
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).expanduser()
    plan_csv = output_dir / f"qbo_inventory_remediation_plan_{config.company_key}_{stamp}.csv"
    plan_json = output_dir / f"qbo_inventory_remediation_plan_{config.company_key}_{stamp}.json"
    _write_csv(plan_csv, plan_rows, PLAN_FIELDNAMES)
    _write_json(
        plan_json,
        {
            "command": args.command,
            "company_key": config.company_key,
            "realm_id": config.realm_id,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "number_prefix": args.number_prefix,
            "excluded_numbers": sorted(exclude_numbers),
            "candidate_count": len(plan_rows),
            "delete_candidate_count": sum(1 for r in plan_rows if r["action"] == "delete_candidate"),
            "rows": plan_rows,
        },
    )

    print(f"[INFO] Wrote remediation plan CSV: {plan_csv}")
    print(f"[INFO] Wrote remediation plan JSON: {plan_json}")
    selected = [row for row in plan_rows if row["action"] == "delete_candidate"]
    print(f"[INFO] Delete candidates: {len(selected)}")
    for row in selected:
        print(f"  candidate DocNumber={row['doc_number']} Id={row['qbo_transaction_id']} TxnDate={row['txn_date']} impact={row['expected_impact']}")

    if args.command == "plan":
        return 0

    if not selected:
        print("[ERROR] Refusing apply: candidate list is empty.", file=sys.stderr)
        return 2

    deletion_rows = apply_deletions(
        token_mgr=token_mgr,
        realm_id=config.realm_id,
        plan_rows=selected,
        fail_fast=bool(args.fail_fast),
    )
    delete_csv = output_dir / f"qbo_inventory_remediation_delete_{config.company_key}_{stamp}.csv"
    delete_json = output_dir / f"qbo_inventory_remediation_delete_{config.company_key}_{stamp}.json"
    _write_csv(delete_csv, deletion_rows, DELETE_FIELDNAMES)
    _write_json(
        delete_json,
        {
            "company_key": config.company_key,
            "realm_id": config.realm_id,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "number_prefix": args.number_prefix,
            "deleted_count": sum(1 for r in deletion_rows if r["result"] == "deleted"),
            "failed_count": sum(1 for r in deletion_rows if r["result"] == "failed"),
            "skipped_count": sum(1 for r in deletion_rows if r["result"] == "skipped"),
            "rows": deletion_rows,
        },
    )
    print(f"[INFO] Wrote deletion log CSV: {delete_csv}")
    print(f"[INFO] Wrote deletion log JSON: {delete_json}")
    return 1 if any(r["result"] == "failed" for r in deletion_rows) else 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except ValueError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
