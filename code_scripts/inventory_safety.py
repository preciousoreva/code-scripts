from __future__ import annotations

import os
from typing import Any

from code_scripts.company_config import get_runtime_qbo_environment, normalize_qbo_environment


INVENTORY_APPLY_DISABLED_MESSAGE = (
    "Inventory apply mode is disabled during QBO remediation. Audit/preview is allowed; "
    "catalog cleanup and QBO quantity adjustments are blocked."
)

ALLOW_INVENTORY_APPLY_ENV = "OIAT_ALLOW_INVENTORY_APPLY"


class InventoryApplyDisabledError(RuntimeError):
    """Raised when production inventory apply mode is frozen."""


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def is_production_inventory_environment(config: Any) -> bool:
    company_environment = normalize_qbo_environment(
        getattr(config, "qbo_environment", None),
        default="production",
    )
    runtime_environment = get_runtime_qbo_environment()
    return company_environment == "production" or runtime_environment == "production"


def assert_inventory_apply_allowed(config: Any, *, action: str = "inventory_apply") -> None:
    """
    Fail closed for production inventory write paths during remediation.

    Audit, preview, snapshot fetch, and EPOS download paths should avoid calling this.
    Sales sync intentionally does not import or use this guard.
    """
    if not is_production_inventory_environment(config):
        return
    if _env_truthy(ALLOW_INVENTORY_APPLY_ENV):
        return
    raise InventoryApplyDisabledError(INVENTORY_APPLY_DISABLED_MESSAGE)
