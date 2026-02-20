from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from django.contrib.auth import get_user_model
from django.utils import timezone

from oiat_portal.paths import OPS_COMPANIES_DIR

from ..forms import CompanyAdvancedForm, CompanyBasicForm
from ..models import CompanyConfigRecord

User = get_user_model()


REQUIRED_TOP_LEVEL_KEYS = {
    "company_key",
    "display_name",
    "qbo",
    "epos",
    "transform",
    "output",
}

REQUIRED_NESTED_KEYS = {
    "qbo": {"realm_id", "deposit_account", "tax_mode"},
    "epos": {"username_env_key", "password_env_key"},
    "transform": {"group_by", "date_format", "receipt_prefix", "receipt_number_format"},
    "output": {"csv_prefix", "metadata_file", "uploaded_docnumbers_file"},
}


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str]


def canonical_json(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def checksum(data: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(data).encode("utf-8")).hexdigest()


def validate_company_config(payload: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    if not isinstance(payload, dict):
        return ValidationResult(valid=False, errors=["Payload must be a JSON object."])

    missing = REQUIRED_TOP_LEVEL_KEYS - set(payload.keys())
    if missing:
        errors.append(f"Missing top-level keys: {', '.join(sorted(missing))}")

    for section, required_keys in REQUIRED_NESTED_KEYS.items():
        section_obj = payload.get(section)
        if not isinstance(section_obj, dict):
            errors.append(f"{section} must be an object")
            continue
        missing_nested = required_keys - set(section_obj.keys())
        if missing_nested:
            errors.append(f"Missing {section} keys: {', '.join(sorted(missing_nested))}")

    qbo = payload.get("qbo", {}) if isinstance(payload.get("qbo"), dict) else {}
    if not qbo.get("realm_id"):
        errors.append("qbo.realm_id is required")

    epos = payload.get("epos", {}) if isinstance(payload.get("epos"), dict) else {}
    if not epos.get("username_env_key"):
        errors.append("epos.username_env_key is required")
    if not epos.get("password_env_key"):
        errors.append("epos.password_env_key is required")

    transform = payload.get("transform", {}) if isinstance(payload.get("transform"), dict) else {}
    if not isinstance(transform.get("group_by"), list):
        errors.append("transform.group_by must be a list")

    output = payload.get("output", {}) if isinstance(payload.get("output"), dict) else {}
    if not output.get("metadata_file"):
        errors.append("output.metadata_file is required")
    if not output.get("uploaded_docnumbers_file"):
        errors.append("output.uploaded_docnumbers_file is required")

    return ValidationResult(valid=not errors, errors=errors)


def build_basic_payload(form: CompanyBasicForm) -> dict[str, Any]:
    d = form.cleaned_data
    payload = {
        "company_key": d["company_key"],
        "display_name": d["display_name"],
        "qbo": {
            "realm_id": d["realm_id"],
            "deposit_account": d["deposit_account"],
            "tax_mode": d["tax_mode"],
            "tax_rate": 0.075,
            "tax_code_id": "",
            "tax_code_name": None,
            "default_item_id": "1",
            "default_income_account_id": "1",
            "department_mapping": {},
        },
        "epos": {
            "username_env_key": d["epos_username_env_key"],
            "password_env_key": d["epos_password_env_key"],
        },
        "transform": {
            "group_by": ["date", "tender"],
            "date_format": "%Y-%m-%d",
            "receipt_prefix": "SR",
            "receipt_number_format": "date_tender_sequence",
            "location_mapping": {},
        },
        "output": {
            "csv_prefix": d["csv_prefix"],
            "metadata_file": d["metadata_file"],
            "uploaded_docnumbers_file": d["uploaded_docnumbers_file"],
        },
        "slack": {
            "webhook_url_env_key": d.get("slack_webhook_env_key") or "",
        },
        "trading_day": {
            "enabled": False,
            "start_hour": 5,
            "start_minute": 0,
        },
        "inventory": {
            "enable_inventory_items": False,
            "allow_negative_inventory": False,
            "inventory_start_date": "today",
            "default_qty_on_hand": 0,
            "auto_fix_wrong_type_items": False,
            "auto_fix_inv_start_date_blockers": False,
            "use_item_hierarchy": False,
        },
    }
    return payload


def apply_advanced_payload(payload: dict[str, Any], form: CompanyAdvancedForm) -> dict[str, Any]:
    d = form.cleaned_data
    payload = dict(payload)
    payload.setdefault("qbo", {})
    payload.setdefault("transform", {})
    payload.setdefault("trading_day", {})
    payload.setdefault("inventory", {})

    payload["trading_day"].update(
        {
            "enabled": bool(d.get("trading_day_enabled")),
            "start_hour": int(d.get("trading_day_start_hour") or 5),
            "start_minute": int(d.get("trading_day_start_minute") or 0),
        }
    )
    payload["inventory"].update(
        {
            "enable_inventory_items": bool(d.get("inventory_enabled")),
            "allow_negative_inventory": bool(d.get("allow_negative_inventory")),
            "inventory_start_date": d.get("inventory_start_date") or "today",
            "default_qty_on_hand": int(d.get("default_qty_on_hand") or 0),
        }
    )
    if d.get("tax_rate") is not None:
        payload["qbo"]["tax_rate"] = float(d["tax_rate"])
    if d.get("tax_code_id"):
        payload["qbo"]["tax_code_id"] = d["tax_code_id"]
    if d.get("tax_code_name"):
        payload["qbo"]["tax_code_name"] = d["tax_code_name"]

    payload["transform"].update(
        {
            "date_format": d.get("date_format") or "%Y-%m-%d",
            "receipt_prefix": d.get("receipt_prefix") or "SR",
            "receipt_number_format": d.get("receipt_number_format") or "date_tender_sequence",
            "group_by": form.cleaned_group_by() or ["date", "tender"],
        }
    )
    return payload


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def sync_record_to_json(record: CompanyConfigRecord) -> Path:
    destination = OPS_COMPANIES_DIR / f"{record.company_key}.json"
    _atomic_write_json(destination, record.config_json)
    record.checksum = checksum(record.config_json)
    record.last_synced_to_json_at = timezone.now()
    record.save(update_fields=["checksum", "last_synced_to_json_at", "updated_at"])
    return destination


def import_json_file(path: Path, user: User | None = None) -> CompanyConfigRecord:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    result = validate_company_config(payload)
    if not result.valid:
        raise ValueError(f"Invalid config {path.name}: {'; '.join(result.errors)}")

    company_key = payload["company_key"]
    display_name = payload.get("display_name", company_key)
    record, created = CompanyConfigRecord.objects.get_or_create(
        company_key=company_key,
        defaults={
            "display_name": display_name,
            "config_json": payload,
            "checksum": checksum(payload),
            "created_by": user,
            "updated_by": user,
        },
    )
    if not created:
        record.display_name = display_name
        record.config_json = payload
        record.checksum = checksum(payload)
        now = timezone.now()
        record.last_synced_from_json_at = now
        record.updated_at = now
        if user:
            record.updated_by = user
        update_fields = ["display_name", "config_json", "checksum", "last_synced_from_json_at", "updated_at"]
        if user:
            update_fields.append("updated_by")
        record.save(update_fields=update_fields)
    else:
        record.last_synced_from_json_at = timezone.now()
        record.save(update_fields=["last_synced_from_json_at"]) 
    return record


def import_all_company_json(user: User | None = None, strict: bool = False) -> list[CompanyConfigRecord]:
    OPS_COMPANIES_DIR.mkdir(parents=True, exist_ok=True)
    imported: list[CompanyConfigRecord] = []
    for path in sorted(OPS_COMPANIES_DIR.glob("*.json")):
        if path.name.startswith("company.example"):
            continue
        try:
            imported.append(import_json_file(path, user=user))
        except Exception:
            if strict:
                raise
    return imported


def check_drift() -> list[str]:
    drifts: list[str] = []
    for record in CompanyConfigRecord.objects.all():
        path = OPS_COMPANIES_DIR / f"{record.company_key}.json"
        if not path.exists():
            drifts.append(f"{record.company_key}: missing JSON file")
            continue
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if checksum(payload) != checksum(record.config_json):
            drifts.append(f"{record.company_key}: checksum mismatch")
    return drifts
