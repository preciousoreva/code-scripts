"""Shared formatters for inventory-side Slack notifications.

The sales receipt pipeline uses :mod:`code_scripts.slack_notify` with
ad-hoc message strings.  The inventory tools were doing the same — every
caller hand-rolled its own bullet list.  That made the messages drift in
both *content* (cleanup mentioning "Inactivated" but consolidation having
no Slack at all) and *style* (different emoji, different field order).

This module exposes two small formatters that the inventory tools call
in place of building strings inline:

* :func:`format_inventory_audit_summary` for ``code_scripts.inventory_sync``
  audit / dry-run / apply.
* :func:`format_pack_variant_apply_summary` for both
  ``qbo_pack_variant_consolidation`` and ``qbo_pack_variant_cleanup``
  apply runs.

Both return the *message string* — they don't talk to Slack.  Sending is
still :func:`code_scripts.slack_notify.send_slack_success`, kept
non-blocking by the callers.

Design notes
------------
* No new dependencies.  Stdlib only.
* Output is identical for the same input — easy to assert in tests.
* ``mode`` is taken verbatim (``"audit"``, ``"dry-run"``, ``"apply"``)
  so callers don't have to remember a fixed enum.
* Counts are passed as a dict so adding/removing a counter never
  changes the formatter signature.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence


_TITLE_BY_KIND = {
    "inventory_audit": "Inventory sync",
    "pack_variant_consolidation": "Pack-variant consolidation",
    "pack_variant_cleanup": "Pack-variant cleanup",
}


def _emoji_for(mode: str, *, failed: int = 0) -> str:
    mode_l = (mode or "").strip().lower()
    if mode_l in ("dry-run", "dry_run"):
        return "🧪"
    if mode_l == "audit":
        return "📋"
    if failed > 0:
        return "⚠️"
    return "✅"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _friendly_mode_label(mode: str) -> str:
    m = (mode or "").strip().lower()
    if m in ("apply",):
        return "Applied updates to QuickBooks"
    if m in ("audit",):
        return "Audit only"
    if m in ("dry-run", "dry_run"):
        return "Preview only — no QuickBooks changes made"
    return str(mode or "").strip() or "Unknown"


def _friendly_scope_label(scope: str) -> str:
    s = str(scope or "").strip()
    if not s:
        return ""
    # Common inventory scope formats: "category=A, B; product=X" or subset thereof.
    parts: list[str] = []
    for chunk in [c.strip() for c in s.split(";") if c.strip()]:
        if chunk.startswith("category="):
            parts.append(chunk.removeprefix("category=").strip())
        elif chunk.startswith("product="):
            parts.append(chunk.removeprefix("product=").strip())
        else:
            parts.append(chunk)
    return " — ".join([p for p in parts if p])


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _friendly_count_lines(counts: Mapping[str, Any]) -> list[str]:
    checked = _as_int(counts.get("total_groups"), 0)
    already_correct = _as_int(counts.get("in_sync"), 0)
    needs_update = _as_int(counts.get("needs_adjustment"), 0)
    need_review = _as_int(counts.get("ambiguous_in_qbo"), 0)
    missing = _as_int(counts.get("missing_in_qbo"), 0)
    posted = _as_int(counts.get("posted"), 0)
    skipped = _as_int(counts.get("skipped"), 0)

    lines: list[str] = []
    if checked:
        lines.append(f"• Products checked: {checked}")
    lines.append(f"• Already correct: {already_correct}")
    lines.append(f"• Need quantity update: {needs_update}")
    if posted or "posted" in counts:
        lines.append(f"• Updated in QuickBooks: {posted}")
    if skipped or "skipped" in counts:
        lines.append(f"• Skipped safely: {skipped}")
    # Manual review includes ambiguity + missing in QBO (and any other warnings passed separately).
    lines.append(f"• Need review before update: {need_review + missing}")

    txn_date = str(counts.get("txn_date") or "").strip()
    if txn_date:
        lines.append(f"• Transaction date: {txn_date}")
    return lines


def _friendly_review_reason(raw: str) -> str:
    text = (raw or "").strip().lower()
    if "non_exact_pick_not_allowed" in text or "fallback_largest_qty" in text:
        return "no exact QuickBooks item match found"
    if "multiple_active_exact_base_in_qbo" in text or "ambiguous_in_qbo" in text:
        return "multiple possible QuickBooks items found"
    if "no_active_exact_base_in_qbo" in text:
        return "no active matching QuickBooks item found"
    if "missing_in_qbo" in text:
        return "product not found in QuickBooks"
    return (raw or "").strip()


def _normalize_manual_review_examples(examples: Sequence[str] | None) -> tuple[list[str], bool]:
    """Return (normalized_examples, truncated_flag)."""
    if not examples:
        return [], False
    cleaned = [str(x).strip() for x in examples if str(x).strip()]
    if not cleaned:
        return [], False
    truncated = len(cleaned) > 10
    cleaned = cleaned[:10]
    out: list[str] = []
    for line in cleaned:
        if "—" in line:
            left, right = [p.strip() for p in line.split("—", 1)]
            out.append(f"{left} — {_friendly_review_reason(right)}")
        else:
            out.append(_friendly_review_reason(line))
    return out, truncated


def format_scope(category: Optional[Any] = None, product: Optional[str] = None) -> str:
    """Render ``--category`` / ``--product`` as a single human line.

    ``category`` may be a list (multi-flag) or a string.  Returns the
    empty string when neither filter is set; callers can branch on that
    to decide whether to emit a ``Scope:`` bullet.
    """
    parts: list[str] = []
    if category:
        if isinstance(category, str):
            cats = [category.strip()] if category.strip() else []
        else:
            cats = [str(c).strip() for c in category if str(c).strip()]
        if cats:
            parts.append("category=" + ", ".join(cats))
    if product:
        prod = str(product).strip()
        if prod:
            parts.append(f"product={prod}")
    return "; ".join(parts)


def _format_counts(counts: Mapping[str, Any]) -> str:
    """Render a counts dict as ``key=value | key=value`` skipping empties.

    Falsy values that are explicit zeros (``0``) are kept; ``None`` and
    empty strings are omitted so callers don't have to pre-filter.
    """
    chunks: list[str] = []
    for key, value in counts.items():
        if value is None or value == "":
            continue
        chunks.append(f"{key}={value}")
    return "  |  ".join(chunks)


def format_inventory_audit_summary(
    *,
    company_display_name: str,
    company_key: str,
    mode: str,
    scope: str = "",
    counts: Mapping[str, Any] | None = None,
    report_path: Optional[str] = None,
    error: Optional[str] = None,
    warnings_count: int = 0,
    manual_review_examples: Sequence[str] | None = None,
) -> str:
    """Compose the Slack message for an :mod:`inventory_sync` run.

    ``mode`` is the human label (``"audit"``, ``"dry-run"``, ``"apply"``).
    ``counts`` typically carries::

        {"total_groups": 134, "in_sync": 41, "needs_adjustment": 12,
         "ambiguous_in_qbo": 60, "missing_in_qbo": 8,
         "posted": 0, "skipped": 0}

    The formatter does not validate keys — it just renders them.
    """
    failed = int((counts or {}).get("failed", 0) or 0)
    emoji = "❌" if error else _emoji_for(mode, failed=failed)
    title = _TITLE_BY_KIND["inventory_audit"]
    head = (
        "failed" if error else
        ("preview" if (mode or "").lower() in ("dry-run", "dry_run") else "completed")
    )
    lines = [
        f"{emoji} *{title} {head}* — {company_display_name} ({company_key})",
        f"• Time: {_now_iso()}",
        f"• Mode: {_friendly_mode_label(mode)}",
    ]
    if scope:
        lines.append(f"• Scope: {_friendly_scope_label(scope)}")
    if counts:
        lines.extend(_friendly_count_lines(counts))
    if warnings_count:
        # Keep the explicit operator-facing count, but in plain language.
        lines.append(f"• Need review before update: {int(warnings_count)}")

    normalized, truncated = _normalize_manual_review_examples(manual_review_examples)
    if normalized:
        lines.append("")
        lines.append("Needs review:")
        for ex in normalized:
            lines.append(f"• {ex}")
        if truncated:
            lines.append("• ... see report for full list.")
    if report_path:
        lines.append("")
        lines.append("Report:")
        lines.append(f"`{report_path}`")
    if error:
        lines.append(f"• Error: {error}")
    return "\n".join(lines)


def format_pack_variant_apply_summary(
    *,
    kind: str,
    company_display_name: str,
    company_key: str,
    mode: str,
    scope: str = "",
    counts: Mapping[str, Any] | None = None,
    report_path: Optional[str] = None,
    error: Optional[str] = None,
) -> str:
    """Compose the Slack message for a pack-variant tool's run.

    ``kind`` is one of:

    * ``"pack_variant_consolidation"`` (used by
      :mod:`qbo_pack_variant_consolidation`)
    * ``"pack_variant_cleanup"`` (used by
      :mod:`qbo_pack_variant_cleanup`)

    Same standardised bullet list as the inventory-audit formatter so
    operators can scan the channel without re-learning a layout per
    tool.
    """
    if kind not in _TITLE_BY_KIND or kind == "inventory_audit":
        raise ValueError(f"Unsupported kind: {kind!r}")
    failed = int((counts or {}).get("failed", 0) or 0)
    emoji = "❌" if error else _emoji_for(mode, failed=failed)
    title = _TITLE_BY_KIND[kind]
    head = (
        "failed" if error else
        ("preview" if (mode or "").lower() in ("dry-run", "dry_run") else "completed")
    )
    lines = [
        f"{emoji} *{title} {head}* — {company_display_name} ({company_key})",
        f"• Time: {_now_iso()}",
        f"• Mode: {mode}",
    ]
    if scope:
        lines.append(f"• Scope: {scope}")
    if counts:
        rendered = _format_counts(counts)
        if rendered:
            lines.append(f"• Counts: {rendered}")
    if report_path:
        lines.append(f"• Report: `{report_path}`")
    if error:
        lines.append(f"• Error: {error}")
    return "\n".join(lines)
