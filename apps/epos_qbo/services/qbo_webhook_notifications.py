from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import requests

from code_scripts.company_config import get_qbo_api_base_url
from code_scripts.slack_notify import send_slack_success
from code_scripts.token_manager import get_access_token

from ..models import CompanyConfigRecord

logger = logging.getLogger(__name__)

QBO_MINOR_VERSION = "70"
QBO_LOOKUP_TIMEOUT_SECS = 12


@dataclass(frozen=True)
class WebhookEntity:
    name: str
    entity_id: str
    operation: str
    last_updated: str


def process_qbo_webhook_payload(payload: dict[str, Any]) -> int:
    """Process Intuit data-change webhooks and forward Slack notifications."""
    notifications = payload.get("eventNotifications")
    if not isinstance(notifications, list):
        logger.warning("QBO webhook ignored: missing eventNotifications list")
        return 0

    sent_count = 0
    for notification in notifications:
        if not isinstance(notification, dict):
            continue
        realm_id = str(notification.get("realmId") or "").strip()
        if not realm_id:
            logger.warning("QBO webhook notification skipped: missing realmId")
            continue
        company = _find_company_by_realm_id(realm_id)
        if company is None:
            logger.warning("QBO webhook notification skipped: unknown realmId=%s", realm_id)
            continue
        webhook_url = _company_slack_webhook(company.company_key)
        if not webhook_url:
            logger.info("QBO webhook Slack skipped for %s: no webhook configured", company.company_key)
            continue

        for entity in _iter_entities(notification):
            message = _format_entity_notification(company=company, realm_id=realm_id, entity=entity)
            send_slack_success(message, webhook_url)
            sent_count += 1
    return sent_count


def _find_company_by_realm_id(realm_id: str) -> CompanyConfigRecord | None:
    for company in CompanyConfigRecord.objects.filter(is_active=True):
        qbo = company.config_json.get("qbo") if isinstance(company.config_json, dict) else {}
        if isinstance(qbo, dict) and str(qbo.get("realm_id") or "").strip() == realm_id:
            return company
    return None


def _company_slack_webhook(company_key: str) -> str:
    key_suffix = company_key.upper().replace("-", "_")
    company_specific = os.getenv(f"QBO_WEBHOOK_SLACK_URL_{key_suffix}", "").strip()
    if company_specific:
        return company_specific
    return os.getenv("QBO_WEBHOOK_SLACK_URL", "").strip()


def _iter_entities(notification: dict[str, Any]) -> list[WebhookEntity]:
    data_change = notification.get("dataChangeEvent")
    if not isinstance(data_change, dict):
        return []
    entities = data_change.get("entities")
    if not isinstance(entities, list):
        return []
    out: list[WebhookEntity] = []
    for raw in entities:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        entity_id = str(raw.get("id") or "").strip()
        if not name or not entity_id:
            continue
        out.append(
            WebhookEntity(
                name=name,
                entity_id=entity_id,
                operation=str(raw.get("operation") or "").strip() or "Unknown",
                last_updated=str(raw.get("lastUpdated") or "").strip(),
            )
        )
    return out


def _format_entity_notification(
    *,
    company: CompanyConfigRecord,
    realm_id: str,
    entity: WebhookEntity,
) -> str:
    if entity.name == "Item":
        return _format_item_notification(company=company, realm_id=realm_id, entity=entity)
    return _format_generic_notification(company=company, realm_id=realm_id, entity=entity)


def _format_generic_notification(
    *,
    company: CompanyConfigRecord,
    realm_id: str,
    entity: WebhookEntity,
    detail_note: str = "",
) -> str:
    lines = [
        f"*QuickBooks {entity.name} {entity.operation}*",
        f"• Company: {company.display_name}",
        f"• Realm ID: `{realm_id}`",
        f"• Entity ID: `{entity.entity_id}`",
    ]
    if entity.last_updated:
        lines.append(f"• Updated: {entity.last_updated}")
    if detail_note:
        lines.append(f"• Details: {detail_note}")
    return "\n".join(lines)


def _format_item_notification(
    *,
    company: CompanyConfigRecord,
    realm_id: str,
    entity: WebhookEntity,
) -> str:
    item, error = _fetch_qbo_item(company=company, realm_id=realm_id, item_id=entity.entity_id)
    if error:
        return _format_generic_notification(
            company=company,
            realm_id=realm_id,
            entity=entity,
            detail_note=f"Item lookup unavailable ({error}).",
        )
    if not item:
        return _format_generic_notification(
            company=company,
            realm_id=realm_id,
            entity=entity,
            detail_note="QuickBooks returned no Item details.",
        )

    item_name = str(item.get("Name") or item.get("FullyQualifiedName") or entity.entity_id).strip()
    item_type = str(item.get("Type") or "Unknown").strip()
    lines = [
        f"*QuickBooks Item {entity.operation}*",
        f"• Company: {company.display_name}",
        f"• Item: {item_name}",
        f"• Type: {item_type}",
        f"• Entity ID: `{entity.entity_id}`",
    ]

    for label, value in [
        ("Fully qualified name", item.get("FullyQualifiedName")),
        ("Active", _yes_no(item.get("Active"))),
        ("Track quantity", _yes_no(item.get("TrackQtyOnHand"))),
        ("Quantity on hand", item.get("QtyOnHand")),
        ("Inventory start date", item.get("InvStartDate")),
        ("Unit price", item.get("UnitPrice")),
        ("Purchase cost", item.get("PurchaseCost")),
        ("Parent", _ref_name(item.get("ParentRef"))),
        ("Income account", _ref_name(item.get("IncomeAccountRef"))),
        ("Asset account", _ref_name(item.get("AssetAccountRef"))),
        ("Expense account", _ref_name(item.get("ExpenseAccountRef"))),
    ]:
        if value not in (None, ""):
            lines.append(f"• {label}: {value}")
    if entity.last_updated:
        lines.append(f"• Updated: {entity.last_updated}")
    return "\n".join(lines)


def _fetch_qbo_item(
    *,
    company: CompanyConfigRecord,
    realm_id: str,
    item_id: str,
) -> tuple[dict[str, Any] | None, str]:
    config = company.config_json if isinstance(company.config_json, dict) else {}
    qbo = config.get("qbo") if isinstance(config.get("qbo"), dict) else {}
    environment = str(qbo.get("environment") or "production")
    try:
        access_token = get_access_token(company.company_key, realm_id)
    except Exception as exc:
        return None, str(exc)

    base_url = get_qbo_api_base_url(environment)
    url = f"{base_url}/v3/company/{realm_id}/item/{item_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    params = {"minorversion": QBO_MINOR_VERSION}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=QBO_LOOKUP_TIMEOUT_SECS)
    except requests.Timeout:
        return None, "QuickBooks item lookup timed out"
    except requests.RequestException as exc:
        return None, f"QuickBooks network error: {exc}"
    if resp.status_code != 200:
        return None, f"QuickBooks returned HTTP {resp.status_code}"
    try:
        payload = resp.json()
    except ValueError:
        return None, "QuickBooks returned invalid JSON"
    item = payload.get("Item")
    return (item if isinstance(item, dict) else None), ""


def _ref_name(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    name = str(value.get("name") or "").strip()
    ref_id = str(value.get("value") or "").strip()
    if name and ref_id:
        return f"{name} (`{ref_id}`)"
    return name or ref_id


def _yes_no(value: Any) -> str:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    return ""
