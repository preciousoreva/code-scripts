from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from code_scripts.artifact_paths import stock_exports_dir
from code_scripts.paths import REPO_CODE_SCRIPTS_DIR


_CATEGORY_COLUMNS = ("Categories", "Category", "CategoryName")


def _resolve_code_scripts_path(value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return REPO_CODE_SCRIPTS_DIR / path


def _mapping_candidates(company_key: str, config_json: dict) -> list[Path]:
    configured = ((config_json.get("inventory") or {}).get("product_mapping_file") or "").strip()
    default_path = REPO_CODE_SCRIPTS_DIR / "mappings" / f"{company_key}_product_mapping.csv"
    out: list[Path] = []
    if configured:
        out.append(_resolve_code_scripts_path(configured))
    if default_path not in out:
        out.append(default_path)
    return out


def _category_column(fieldnames: Iterable[str] | None) -> str | None:
    by_lower = {
        str(name).strip().lower(): str(name)
        for name in (fieldnames or [])
        if str(name).strip()
    }
    for candidate in _CATEGORY_COLUMNS:
        found = by_lower.get(candidate.lower())
        if found:
            return found
    return None


def _categories_from_csv(path: Path) -> list[str]:
    if not path.exists() or not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        key = _category_column(reader.fieldnames)
        if not key:
            return []
        seen: dict[str, str] = {}
        for row in reader:
            value = (row.get(key) or "").strip()
            if value:
                seen.setdefault(value.casefold(), value)
    return sorted(seen.values(), key=lambda value: value.casefold())


def _latest_stock_report_path(company_key: str) -> Path | None:
    root = stock_exports_dir().parent
    if not root.exists():
        return None
    candidates = []
    for path in root.rglob("*.csv"):
        if path.is_file() and path.name.startswith(f"{company_key}_"):
            try:
                candidates.append((path.stat().st_mtime, path))
            except OSError:
                continue
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate[0])[1]


def load_inventory_categories_for_company(company_key: str, config_json: dict | None = None) -> list[str]:
    config = config_json or {}
    for path in _mapping_candidates(company_key, config):
        try:
            categories = _categories_from_csv(path)
        except Exception:
            categories = []
        if categories:
            return categories

    latest_stock = _latest_stock_report_path(company_key)
    if latest_stock is None:
        return []
    try:
        return _categories_from_csv(latest_stock)
    except Exception:
        return []


def load_inventory_categories_by_company(companies: Iterable) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for company in companies:
        company_key = str(company.company_key)
        out[company_key] = load_inventory_categories_for_company(
            company_key,
            company.config_json or {},
        )
    return out
