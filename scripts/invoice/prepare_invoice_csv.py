from __future__ import annotations

import argparse
import re
import sys
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
from qbo_upload import TokenManager, prefetch_all_items

load_env_file()


REQUIRED_OUT_COLS = [
    "Customer",
    "InvoiceDate",
    "ServiceDate",
    "ItemName",
    "Description",
    "Qty",
    "Rate",
    "Amount",
    "Location",
    "DueDate",
]

INFER_MAP = {
    "InvoiceDate": ["invoice date", "date", "invoice_date", "invoice date", "inv date"],
    "ItemName": ["item", "item name", "product", "product name", "itemname", "item_name"],
    "Qty": ["qty", "quantity", "qty sold", "qtty"],
    "Rate": ["rate", "unit price", "price", "unitprice", "unit_price"],
    "Amount": ["amount", "total", "line total", "line amount", "total amount"],
    "Customer": ["customer", "customer name", "customername"],
    "ServiceDate": ["service date", "service_date", "servicedate"],
    "Description": ["description", "desc", "item description", "item_desc"],
    "Location": ["location", "location name", "location_name"],
    "DueDate": ["due date", "due_date", "duedate"],
}


# Names that must never be chosen as a fuzzy match for product/invoice lines (generic QBO items).
BLOCKLIST_NORMALIZED = frozenset({"services", "hours", "service", "hour"})

CONTAINMENT_MIN_SCORE = 0.95  # min score when candidate name contains the query in full

# Unmatched report column names (written even when empty so file is not stale).
UNMATCHED_COLS = ["CsvItemName", "BestMatch", "Similarity", "InvoiceDate"]


def _normalize_name(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _correct_spelling(normalized: str, spelling_corrections: Dict[str, str]) -> str:
    """Replace known misspellings (whole words) with correct spelling for matching."""
    if not normalized or not spelling_corrections:
        return normalized
    words = normalized.split()
    corrected = [spelling_corrections.get(w, w) for w in words]
    return " ".join(corrected)


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _parse_date(value: str) -> str:
    if value is None or str(value).strip() == "":
        raise ValueError("Date is required")
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        raise ValueError(f"Invalid date: {value}")
    return dt.strftime("%Y-%m-%d")


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


def _load_aliases(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}
    alias_path = Path(path)
    if not alias_path.is_absolute():
        alias_path = _REPO_ROOT / alias_path
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


def _load_product_names(products_dir: Optional[str]) -> List[str]:
    """Load unique product names from ProductList*.csv in products_dir (semicolon-delimited, column Name)."""
    if not products_dir or not str(products_dir).strip():
        return []
    path = Path(products_dir)
    if not path.is_absolute():
        path = _REPO_ROOT / path
    if not path.is_dir() or not path.exists():
        return []
    names: List[str] = []
    for csv_path in sorted(path.glob("ProductList*.csv")):
        try:
            df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
        except Exception:
            continue
        name_col = None
        for col in df.columns:
            if col.strip().lower() == "name":
                name_col = col
                break
        if name_col is None and len(df.columns):
            name_col = df.columns[0]
        if name_col is not None:
            for v in df[name_col].dropna().astype(str).str.strip():
                if v:
                    names.append(v)
    return list(dict.fromkeys(names))


def _load_qbo_item_names(company: Optional[str], qbo_items_csv: Optional[str]) -> List[str]:
    if qbo_items_csv:
        path = Path(qbo_items_csv)
        if path.exists():
            df = pd.read_csv(path)
            name_col = None
            for col in df.columns:
                if col.strip().lower() in ("name", "itemname", "item name"):
                    name_col = col
                    break
            if name_col:
                return [str(x).strip() for x in df[name_col].dropna().tolist()]
    if company:
        config = load_company_config(company)
        verify_realm_match(config.company_key, config.realm_id)
        token_mgr = TokenManager(config.company_key, config.realm_id)
        items_by_name = prefetch_all_items(token_mgr, config.realm_id)
        return [name for name, item in items_by_name.items() if item.get("Active", True)]
    return []


def _match_item_name(
    csv_name: str,
    qbo_names: List[str],
    min_similarity: float,
    aliases: Dict[str, str],
    spelling_corrections: Dict[str, str],
) -> Tuple[str, Optional[str], float]:
    target = _correct_spelling(_normalize_name(csv_name), spelling_corrections)
    if not target:
        return csv_name, None, 0.0
    alias_target = aliases.get(target)
    if alias_target:
        return alias_target, alias_target, 1.0

    if not qbo_names:
        return csv_name, None, 0.0

    best_name = None
    best_score = 0.0
    for cand in qbo_names:
        cand_norm = _normalize_name(cand)
        if not cand_norm:
            continue
        if cand_norm in BLOCKLIST_NORMALIZED:
            continue
        score = _similarity(target, cand_norm)
        if target and target in cand_norm:
            score = max(score, CONTAINMENT_MIN_SCORE)
        if target and " " in target:
            cand_padded = f" {cand_norm} "
            for word in target.split():
                if len(word) >= 2 and f" {word} " in cand_padded:
                    score = max(score, CONTAINMENT_MIN_SCORE)
                    break
        if score > best_score:
            best_score = score
            best_name = cand
    if best_score >= min_similarity and best_name:
        return best_name, best_name, best_score
    return csv_name, best_name, best_score


def _infer_column(df: pd.DataFrame, target: str, override: Optional[str]) -> Optional[str]:
    if override:
        return override
    def norm(s: str) -> str:
        return re.sub(r"\s+", "", (s or "").strip().lower())
    candidates = INFER_MAP.get(target, [])
    lookup = {norm(c): c for c in df.columns}
    for cand in candidates:
        if norm(cand) in lookup:
            return lookup[norm(cand)]
    # Exact template column name (e.g. from transform_invoice_raw output)
    if norm(target) in lookup:
        return lookup[norm(target)]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare invoice CSV for qbo_import_invoices.py")
    parser.add_argument("--csv", required=True, help="Source invoice CSV")
    parser.add_argument("--company", help="Company key for QBO item list (company_a)")
    parser.add_argument("--qbo-items-csv", help="Optional CSV with QBO item names (offline)")
    parser.add_argument("--products-dir", help="Dir with ProductList*.csv (EPOS product names) for fuzzy match; default for company_a: products/Company A Products (EPOS)")
    parser.add_argument("--aliases", default="templates/item_aliases.csv", help="Alias CSV (CsvItemName,QboItemName)")
    parser.add_argument("--spelling-corrections", default="templates/spelling_corrections.csv", help="Spelling corrections CSV (Wrong,Correct)")
    parser.add_argument("--min-similarity", type=float, default=0.90, help="Min fuzzy match score (0-1)")

    for col in REQUIRED_OUT_COLS:
        parser.add_argument(f"--col-{col}", help=f"Override column for {col}")

    args = parser.parse_args()

    if args.products_dir is None and args.company == "company_a":
        args.products_dir = "products/Company A Products (EPOS)"

    src_path = Path(args.csv)
    if not src_path.exists():
        print(f"[ERROR] Source CSV not found: {src_path}")
        return 1

    df = pd.read_csv(src_path)

    col_map: Dict[str, Optional[str]] = {}
    for col in REQUIRED_OUT_COLS:
        override = getattr(args, f"col_{col}", None)
        col_map[col] = _infer_column(df, col, override)

    # Required for processing
    required = ["InvoiceDate", "ItemName", "Qty", "Rate", "Amount"]
    missing = [c for c in required if not col_map.get(c)]
    if missing:
        print(f"[ERROR] Missing required columns: {', '.join(missing)}")
        print("Use --col-<ColumnName>=<YourColumn> to map.")
        return 1

    aliases = _load_aliases(args.aliases)
    spelling_corrections = _load_spelling_corrections(args.spelling_corrections)
    product_names = _load_product_names(args.products_dir)
    qbo_names = _load_qbo_item_names(args.company, args.qbo_items_csv)
    reference_names = list(dict.fromkeys(product_names + qbo_names))
    if product_names:
        print(f"[INFO] Loaded {len(product_names)} product names from products dir for fuzzy match")
    if qbo_names:
        print(f"[INFO] Loaded {len(qbo_names)} QBO item names for fuzzy match")
    if reference_names:
        print(f"[INFO] Combined reference list: {len(reference_names)} unique names")

    # Build output dataframe
    out_rows: List[Dict[str, Any]] = []
    unmatched_rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        invoice_date = _parse_date(row[col_map["InvoiceDate"]])
        service_date = invoice_date
        if col_map.get("ServiceDate") and pd.notna(row[col_map["ServiceDate"]]):
            service_date = _parse_date(row[col_map["ServiceDate"]])

        due_date = None
        if col_map.get("DueDate") and pd.notna(row[col_map["DueDate"]]):
            due_date = _parse_date(row[col_map["DueDate"]])

        item_name_raw = str(row[col_map["ItemName"]]).strip()
        matched_name, best_match, score = _match_item_name(
            item_name_raw, reference_names, args.min_similarity, aliases, spelling_corrections
        )
        if best_match is None or score < args.min_similarity:
            unmatched_rows.append({
                "CsvItemName": item_name_raw,
                "BestMatch": best_match or "",
                "Similarity": round(score, 4),
                "InvoiceDate": invoice_date,
            })

        description = ""
        if col_map.get("Description") and pd.notna(row[col_map["Description"]]):
            description = str(row[col_map["Description"]]).strip()

        location = ""
        if col_map.get("Location") and pd.notna(row[col_map["Location"]]):
            location = str(row[col_map["Location"]]).strip()

        out_rows.append({
            "Customer": "GPFH",
            "InvoiceDate": invoice_date,
            "ServiceDate": service_date,
            "ItemName": matched_name,
            "Description": description,
            "Qty": float(row[col_map["Qty"]]),
            "Rate": float(row[col_map["Rate"]]),
            "Amount": float(row[col_map["Amount"]]),
            "Location": location,
            "DueDate": due_date or "",
        })

    out_df = pd.DataFrame(out_rows)

    # Fill group due dates
    out_df["DueDate"] = out_df["DueDate"].replace("", pd.NA)
    grouped = out_df.groupby("InvoiceDate", dropna=False)
    for invoice_date, idx in grouped.groups.items():
        if out_df.loc[idx, "DueDate"].notna().any():
            due_val = out_df.loc[idx, "DueDate"].dropna().iloc[0]
        else:
            due_val = (datetime.strptime(invoice_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
        out_df.loc[idx, "DueDate"] = due_val

    # Ensure column order
    out_df = out_df[REQUIRED_OUT_COLS]

    exports_dir = src_path.parent / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    prepared_path = exports_dir / f"{src_path.stem}_prepared.csv"
    unmatched_path = exports_dir / f"{src_path.stem}_unmatched.csv"
    out_df.to_csv(prepared_path, index=False)

    # Always write unmatched report (header-only when empty) so file is not stale.
    unmatched_df = pd.DataFrame(unmatched_rows, columns=UNMATCHED_COLS) if unmatched_rows else pd.DataFrame(columns=UNMATCHED_COLS)
    unmatched_df.to_csv(unmatched_path, index=False)

    print(f"Prepared CSV: {prepared_path} ({len(out_df)} rows)")
    if unmatched_rows:
        print(f"Unmatched report: {unmatched_path} ({len(unmatched_rows)} rows)")
    else:
        print("Unmatched report: none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
