from __future__ import annotations

import argparse
import sys
import re
from datetime import datetime, timedelta
from pathlib import Path
from difflib import SequenceMatcher
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from load_env import load_env_file
from company_config import load_company_config
from token_manager import verify_realm_match
from qbo_upload import (
    BASE_URL,
    TokenManager,
    _make_qbo_request,
    prefetch_all_items,
    get_tax_code_id_by_name,
    get_department_id,
    get_term_id,
)
from slack_notify import notify_invoice_import_start, notify_invoice_import_success

load_env_file()

REQUIRED_COLS = [
    "Customer",
    "InvoiceDate",
    "ItemName",
    "Qty",
    "Rate",
    "Amount",
]


def _normalize_name(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _load_spelling_corrections(path: Optional[str]) -> Dict[str, str]:
    """Load Wrong,Correct pairs from CSV; keys are normalized (lowercase) for matching."""
    if not path or not str(path).strip():
        return {}
    p = Path(path)
    if not p.is_absolute():
        p = _REPO_ROOT / p
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    if "Wrong" not in df.columns or "Correct" not in df.columns:
        return {}
    out = {}
    for _, row in df.iterrows():
        wrong = str(row.get("Wrong", "")).strip().lower()
        correct = str(row.get("Correct", "")).strip()
        if wrong and correct:
            out[wrong] = correct
    return out


def _correct_spelling(normalized: str, spelling_corrections: Dict[str, str]) -> str:
    """Replace known misspellings (whole words) with correct spelling for matching."""
    if not normalized or not spelling_corrections:
        return normalized
    words = normalized.split()
    corrected = [spelling_corrections.get(w, w) for w in words]
    return " ".join(corrected)


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


# Never choose these as fuzzy match for product/invoice lines (generic QBO items).
BLOCKLIST_NORMALIZED = frozenset({"services", "hours", "service", "hour"})
CONTAINMENT_MIN_SCORE = 0.95  # min score when candidate name contains the query in full


def _parse_date(value: str) -> str:
    if value is None or str(value).strip() == "":
        raise ValueError("InvoiceDate is required")
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Last resort: pandas parse
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Invalid date: {value}")
    return dt.strftime("%Y-%m-%d")


def _infer_terms(invoice_date: str, due_date: str) -> str:
    if not invoice_date or not due_date:
        return ""
    try:
        inv_dt = datetime.strptime(invoice_date, "%Y-%m-%d")
        due_dt = datetime.strptime(due_date, "%Y-%m-%d")
    except ValueError:
        return ""
    delta_days = (due_dt - inv_dt).days
    mapping = {
        0: "Due on receipt",
        15: "Net 15",
        30: "Net 30",
        45: "Net 45",
        60: "Net 60",
    }
    return mapping.get(delta_days, "")


def _get_customer_id_by_name(name: str, token_mgr: TokenManager, realm_id: str) -> Optional[str]:
    safe_name = name.replace("'", "''")
    query = f"select Id, DisplayName from Customer where DisplayName = '{safe_name}' maxresults 5"
    url = f"{BASE_URL}/v3/company/{realm_id}/query?query={query}&minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        return None
    data = resp.json()
    customers = data.get("QueryResponse", {}).get("Customer", [])
    if not customers:
        return None
    return customers[0].get("Id")


def _get_item_detail(item_id: str, token_mgr: TokenManager, realm_id: str, cache: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if item_id in cache:
        return cache[item_id]
    url = f"{BASE_URL}/v3/company/{realm_id}/item/{item_id}?minorversion=70"
    resp = _make_qbo_request("GET", url, token_mgr)
    if resp.status_code != 200:
        cache[item_id] = {}
        return cache[item_id]
    data = resp.json().get("Item", {}) or {}
    cache[item_id] = data
    return data


def _build_item_index(items_by_name: Dict[str, Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    indexed: List[Tuple[str, Dict[str, Any]]] = []
    for name, item in items_by_name.items():
        indexed.append((name, item))
        fqn = (item.get("FullyQualifiedName") or "").strip()
        if fqn and fqn != name:
            indexed.append((fqn, item))
    return indexed


def _match_item(
    csv_name: str,
    indexed_items: List[Tuple[str, Dict[str, Any]]],
    min_similarity: float,
    aliases: Dict[str, str],
    spelling_corrections: Dict[str, str],
) -> Tuple[Optional[Dict[str, Any]], float, Optional[str], bool]:
    target = _correct_spelling(_normalize_name(csv_name), spelling_corrections)
    if not target:
        return None, 0.0, None, False

    alias_used = False
    alias_target = aliases.get(target)
    if alias_target:
        alias_used = True
        # Try exact alias name match first
        for candidate_name, item in indexed_items:
            if candidate_name == alias_target:
                return item, 1.0, candidate_name, True
        # Fallback: fuzzy against alias target
        alias_norm = _correct_spelling(_normalize_name(alias_target), spelling_corrections)
        best_item = None
        best_score = 0.0
        best_name = None
        for candidate_name, item in indexed_items:
            cand = _normalize_name(candidate_name)
            if not cand:
                continue
            if cand in BLOCKLIST_NORMALIZED:
                continue
            score = _similarity(alias_norm, cand)
            if alias_norm and alias_norm in cand:
                score = max(score, CONTAINMENT_MIN_SCORE)
            if alias_norm and " " in alias_norm:
                cand_padded = f" {cand} "
                for word in alias_norm.split():
                    if len(word) >= 2 and f" {word} " in cand_padded:
                        score = max(score, CONTAINMENT_MIN_SCORE)
                        break
            if score > best_score:
                best_score = score
                best_item = item
                best_name = candidate_name
        if best_item:
            return best_item, best_score, best_name, True
    best_item = None
    best_score = 0.0
    best_name = None
    for candidate_name, item in indexed_items:
        cand = _normalize_name(candidate_name)
        if not cand:
            continue
        if cand in BLOCKLIST_NORMALIZED:
            continue
        score = _similarity(target, cand)
        if target and target in cand:
            score = max(score, CONTAINMENT_MIN_SCORE)
        if target and " " in target:
            cand_padded = f" {cand} "
            for word in target.split():
                if len(word) >= 2 and f" {word} " in cand_padded:
                    score = max(score, CONTAINMENT_MIN_SCORE)
                    break
        if score > best_score:
            best_score = score
            best_item = item
            best_name = candidate_name
    if best_score >= min_similarity:
        return best_item, best_score, best_name, False
    return None, best_score, best_name, False


def _write_report(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _load_aliases(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}
    alias_path = Path(path)
    if not alias_path.exists():
        return {}
    df = pd.read_csv(alias_path)
    if "CsvItemName" not in df.columns or "QboItemName" not in df.columns:
        return {}
    aliases = {}
    for _, row in df.iterrows():
        key = _normalize_name(str(row.get("CsvItemName", "")))
        val = str(row.get("QboItemName", "")).strip()
        if key and val:
            aliases[key] = val
    return aliases


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Invoices from CSV into QBO (company_a only).")
    parser.add_argument("--company", required=True, help="Company key (company_a)")
    parser.add_argument("--csv", required=True, help="Path to invoice CSV")
    parser.add_argument("--dry-run", action="store_true", help="Do not upload invoices, just log actions")
    parser.add_argument("--validate-only", action="store_true", help="Only validate/match and report; no uploads")
    parser.add_argument("--min-similarity", type=float, default=0.90, help="Min fuzzy match score (0-1)")
    parser.add_argument("--aliases", help="Optional CSV of item aliases (CsvItemName,QboItemName)")
    parser.add_argument("--spelling-corrections", default="templates/spelling_corrections.csv", help="Spelling corrections CSV (Wrong,Correct)")
    args = parser.parse_args()

    config = load_company_config(args.company)
    if config.company_key != "company_a":
        print("[ERROR] Invoice import is currently supported for company_a only.")
        return 1

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        return 1

    df = pd.read_csv(csv_path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing required columns: {', '.join(missing)}")
        return 1

    verify_realm_match(config.company_key, config.realm_id)
    token_mgr = TokenManager(config.company_key, config.realm_id)

    # Prefetch items for matching
    items_by_name = prefetch_all_items(token_mgr, config.realm_id)
    items_by_name = {name: item for name, item in items_by_name.items() if item.get("Active", True)}
    indexed_items = _build_item_index(items_by_name)
    aliases = _load_aliases(args.aliases)
    spelling_corrections = _load_spelling_corrections(args.spelling_corrections)

    # Resolve No VAT tax code once
    tax_code_cache: Dict[str, Optional[str]] = {}
    no_vat_id = get_tax_code_id_by_name("No VAT", token_mgr, config.realm_id, tax_code_cache)
    if not no_vat_id:
        print("[ERROR] Tax code 'No VAT' not found in QBO.")
        return 1

    # Normalize dates
    df["InvoiceDate"] = df["InvoiceDate"].apply(_parse_date)
    if "ServiceDate" in df.columns:
        df["ServiceDate"] = df["ServiceDate"].apply(_parse_date)
    else:
        df["ServiceDate"] = df["InvoiceDate"]

    # Numeric fields
    for col in ("Qty", "Rate", "Amount"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Optional columns
    if "Description" not in df.columns:
        df["Description"] = ""

    reports_dir = Path("reports")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    match_rows: List[Dict[str, Any]] = []
    skip_rows: List[Dict[str, Any]] = []

    # Group by Customer + InvoiceDate
    group_cols = ["Customer", "InvoiceDate"]
    grouped = df.groupby(group_cols, dropna=False)

    invoice_seq: Dict[str, int] = {}
    department_cache: Dict[str, Optional[str]] = {}
    term_cache: Dict[str, Optional[str]] = {}
    item_detail_cache: Dict[str, Dict[str, Any]] = {}

    invoices_created = 0
    lines_uploaded = 0
    lines_skipped = 0
    missing_customers: List[Dict[str, Any]] = []

    # Pre-validate customers
    unique_customers = sorted({str(c).strip() for c in df["Customer"].dropna().tolist() if str(c).strip()})
    customer_id_cache: Dict[str, Optional[str]] = {}
    for cust in unique_customers:
        cust_id = _get_customer_id_by_name(cust, token_mgr, config.realm_id)
        customer_id_cache[cust] = cust_id
        if not cust_id:
            missing_customers.append({"Customer": cust, "Reason": "not_found"})

    created_invoice_totals: List[Tuple[str, str, float]] = []
    if not args.dry_run and not args.validate_only and config.slack_webhook_url:
        notify_invoice_import_start(
            config.display_name,
            csv_path.name,
            len(grouped),
            len(df),
            config.slack_webhook_url,
        )

    for (customer_name, invoice_date), group in grouped:
        customer_name = str(customer_name).strip()
        if not customer_name:
            print("[WARN] Skipping group with empty Customer name.")
            continue

        customer_id = customer_id_cache.get(customer_name)
        if not customer_id:
            print(f"[ERROR] Customer not found in QBO: {customer_name}")
            continue

        seq = invoice_seq.get(invoice_date, 0) + 1
        invoice_seq[invoice_date] = seq
        doc_number = f"INV-{invoice_date.replace('-', '')}-{seq:02d}"

        due_date = None
        if "DueDate" in group.columns:
            due_vals = group["DueDate"].dropna().astype(str).tolist()
            if due_vals:
                due_date = _parse_date(due_vals[0])
        if not due_date:
            due_date = (datetime.strptime(invoice_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")

        terms_name = ""
        if "Terms" in group.columns:
            for tval in group["Terms"].dropna().astype(str).tolist():
                tval = tval.strip()
                if tval and tval.lower() != "nan":
                    terms_name = tval
                    break
        if not terms_name:
            terms_name = _infer_terms(invoice_date, due_date)

        lines = []
        for _, row in group.iterrows():
            csv_item_name = str(row.get("ItemName", "")).strip()
            item, score, matched_name, alias_used = _match_item(
                csv_item_name,
                indexed_items,
                args.min_similarity,
                aliases,
                spelling_corrections,
            )
            if not item:
                lines_skipped += 1
                skip_rows.append({
                    "Customer": customer_name,
                    "InvoiceDate": invoice_date,
                    "ItemName": csv_item_name,
                    "BestMatch": matched_name,
                    "Similarity": round(score, 4),
                    "Reason": "no_match",
                })
                continue

            item_id = item.get("Id")
            item_name = item.get("Name") or matched_name or csv_item_name
            item_detail = _get_item_detail(item_id, token_mgr, config.realm_id, item_detail_cache)
            item_desc = (
                item_detail.get("Description")
                or item_detail.get("SalesOrPurchase", {}).get("Desc")
                or row.get("Description", "")
            )

            qty = float(row["Qty"])
            rate = float(row["Rate"])
            amount = float(row["Amount"])
            service_date = row.get("ServiceDate")

            line = {
                "Amount": amount,
                "DetailType": "SalesItemLineDetail",
                "Description": item_desc,
                "SalesItemLineDetail": {
                    "ItemRef": {"value": item_id, "name": item_name},
                    "Qty": qty,
                    "UnitPrice": rate,
                    "ServiceDate": service_date,
                    "TaxCodeRef": {"value": no_vat_id},
                },
            }
            lines.append(line)
            lines_uploaded += 1
            match_rows.append({
                "Customer": customer_name,
                "InvoiceDate": invoice_date,
                "CsvItemName": csv_item_name,
                "MatchedItemName": item_name,
                "MatchedItemId": item_id,
                "Similarity": round(score, 4),
                "AliasUsed": alias_used,
                "Qty": qty,
                "Rate": rate,
                "Amount": amount,
            })

        if not lines:
            print(f"[WARN] No matched lines for invoice {doc_number}; skipping invoice creation.")
            continue

        payload: Dict[str, Any] = {
            "CustomerRef": {"value": customer_id},
            "TxnDate": invoice_date,
            "DueDate": due_date,
            "DocNumber": doc_number,
            "Line": lines,
        }

        # Optional Location (Department) if provided in CSV
        if "Location" in group.columns:
            location_val = ""
            if group["Location"].notna().any():
                for lval in group["Location"].dropna().astype(str).tolist():
                    lval = lval.strip()
                    if lval and lval.lower() != "nan":
                        location_val = lval
                        break
            if location_val:
                department_id = get_department_id(location_val, token_mgr, config.realm_id, department_cache, config=config)
                if department_id:
                    payload["DepartmentRef"] = {"value": department_id}
                else:
                    print(f"[WARN] Location not found in QBO: {location_val} (invoice {doc_number})")

        # Optional Terms (SalesTermRef) if provided or inferred
        if terms_name:
            term_id = get_term_id(terms_name, token_mgr, config.realm_id, term_cache)
            if term_id:
                payload["SalesTermRef"] = {"value": term_id}
            else:
                print(f"[WARN] Terms not found in QBO: {terms_name} (invoice {doc_number})")

        if args.validate_only or args.dry_run:
            print(f"[DRY-RUN] Would create Invoice {doc_number} for {customer_name} ({len(lines)} lines)")
        else:
            url = f"{BASE_URL}/v3/company/{config.realm_id}/invoice?minorversion=70"
            resp = _make_qbo_request("POST", url, token_mgr, json=payload)
            if resp.status_code not in (200, 201):
                print(f"[ERROR] Failed to create invoice {doc_number}: {resp.status_code} {resp.text}")
                continue
            invoices_created += 1
            inv_total = sum(line["Amount"] for line in lines)
            created_invoice_totals.append((doc_number, customer_name, inv_total))
            print(f"[OK] Created invoice {doc_number} ({len(lines)} lines)")

    if not args.dry_run and not args.validate_only and config.slack_webhook_url:
        grand_total = sum(t[2] for t in created_invoice_totals)
        notify_invoice_import_success(
            config.display_name,
            invoices_created,
            lines_uploaded,
            lines_skipped,
            created_invoice_totals,
            grand_total,
            config.slack_webhook_url,
        )

    # Reports
    _write_report(reports_dir / f"invoice_item_matches_{timestamp}.csv", match_rows)
    _write_report(reports_dir / f"invoice_unmatched_items_{timestamp}.csv", skip_rows)
    _write_report(reports_dir / f"invoice_missing_customers_{timestamp}.csv", missing_customers)

    print("\n=== Invoice Import Summary ===")
    print(f"Invoices created: {invoices_created}")
    print(f"Lines uploaded: {lines_uploaded}")
    print(f"Lines skipped: {lines_skipped}")
    if skip_rows:
        print(f"Unmatched items report: reports/invoice_unmatched_items_{timestamp}.csv")
    if match_rows:
        print(f"Match report: reports/invoice_item_matches_{timestamp}.csv")
    if missing_customers:
        print(f"Missing customers report: reports/invoice_missing_customers_{timestamp}.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
