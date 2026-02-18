"""
Company Configuration Loader

Loads and validates company-specific configuration from JSON files.
Provides a single source of truth for company settings.
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

from code_scripts.paths import OPS_COMPANIES_DIR, OPS_ROOT


class CompanyConfig:
    """Company configuration loaded from JSON file."""
    
    def __init__(self, config_path: Path):
        """Load and validate company configuration."""
        if not config_path.exists():
            raise FileNotFoundError(f"Company config not found: {config_path}")
        
        with open(config_path, "r") as f:
            self._data = json.load(f)
        
        self._validate()
    
    def _validate(self) -> None:
        """Validate required fields in config."""
        required = ["company_key", "qbo", "epos", "transform", "output"]
        for field in required:
            if field not in self._data:
                raise ValueError(f"Missing required field: {field}")
        
        if "realm_id" not in self._data["qbo"]:
            raise ValueError("Missing qbo.realm_id in config")
        
        if "username_env_key" not in self._data["epos"]:
            raise ValueError("Missing epos.username_env_key in config")
        
        if "password_env_key" not in self._data["epos"]:
            raise ValueError("Missing epos.password_env_key in config")
    
    @property
    def company_key(self) -> str:
        """Company identifier (e.g., 'company_a', 'company_b')."""
        return self._data["company_key"]
    
    @property
    def display_name(self) -> str:
        """Human-readable company name."""
        return self._data.get("display_name", self.company_key)
    
    @property
    def realm_id(self) -> str:
        """QBO Realm ID for this company."""
        realm_id = self._data["qbo"]["realm_id"]
        if realm_id.startswith("REPLACE_WITH_"):
            raise ValueError(
                f"Realm ID not configured for {self.display_name}. "
                f"Please update {self.company_key}.json with the actual realm_id."
            )
        return realm_id
    
    @property
    def deposit_account(self) -> str:
        """Deposit account name for this company."""
        return self._data["qbo"]["deposit_account"]
    
    @property
    def tax_mode(self) -> str:
        """Tax mode: 'vat_inclusive_7_5' or 'sales_tax_company_b'."""
        return self._data["qbo"].get("tax_mode", "vat_inclusive_7_5")
    
    @property
    def tax_code_id(self) -> Optional[str]:
        """Tax code ID (for Company A VAT mode)."""
        return self._data["qbo"].get("tax_code_id")
    
    @property
    def tax_code_name(self) -> Optional[str]:
        """Tax code name (for Company B sales tax mode)."""
        return self._data["qbo"].get("tax_code_name")
    
    @property
    def tax_rate(self) -> float:
        """Tax rate as decimal (e.g., 0.075 for 7.5%, 0.125 for 12.5%)."""
        return self._data["qbo"].get("tax_rate", 0.075)  # Default to 7.5% if not specified
    
    @property
    def epos_username(self) -> str:
        """EPOS username from environment variable."""
        env_key = self._data["epos"]["username_env_key"]
        username = os.environ.get(env_key)
        if not username:
            raise RuntimeError(
                f"EPOS username not found. Set {env_key} environment variable "
                f"or add it to .env file."
            )
        return username
    
    @property
    def epos_password(self) -> str:
        """EPOS password from environment variable."""
        env_key = self._data["epos"]["password_env_key"]
        password = os.environ.get(env_key)
        if not password:
            raise RuntimeError(
                f"EPOS password not found. Set {env_key} environment variable "
                f"or add it to .env file."
            )
        return password
    
    @property
    def group_by(self) -> list:
        """List of fields to group by: ['date', 'tender'] or ['date', 'location', 'tender']."""
        return self._data["transform"]["group_by"]
    
    @property
    def date_format(self) -> str:
        """Date format string for transform output."""
        return self._data["transform"]["date_format"]
    
    @property
    def receipt_prefix(self) -> str:
        """Prefix for receipt numbers (e.g., 'SR')."""
        return self._data["transform"]["receipt_prefix"]
    
    @property
    def receipt_number_format(self) -> str:
        """Receipt number format: 'date_tender_sequence' or 'date_location_sequence'."""
        return self._data["transform"]["receipt_number_format"]
    
    @property
    def location_mapping(self) -> Dict[str, str]:
        """Mapping from location names to location codes (for Company B)."""
        return self._data["transform"].get("location_mapping", {})
    
    @property
    def csv_prefix(self) -> str:
        """Prefix for output CSV files."""
        return self._data["output"]["csv_prefix"]
    
    @property
    def metadata_file(self) -> str:
        """Name of metadata JSON file."""
        return self._data["output"]["metadata_file"]
    
    @property
    def uploaded_docnumbers_file(self) -> str:
        """Name of uploaded docnumbers ledger file."""
        return self._data["output"]["uploaded_docnumbers_file"]
    
    @property
    def slack_webhook_url(self) -> Optional[str]:
        """
        Slack webhook URL (optional).
        
        Supports two formats:
        1. Direct URL in config: "webhook_url_env_key": "https://hooks.slack.com/..."
        2. Environment variable key: "webhook_url_env_key": "SLACK_WEBHOOK_URL_A"
        """
        slack_config = self._data.get("slack", {})
        webhook_value = slack_config.get("webhook_url_env_key")
        if not webhook_value:
            return None
        
        # If it looks like a URL (starts with http), use it directly
        if webhook_value.startswith("http://") or webhook_value.startswith("https://"):
            return webhook_value
        
        # Otherwise, treat it as an environment variable key
        return os.environ.get(webhook_value)
    
    @property
    def trading_day_enabled(self) -> bool:
        """Whether trading day mode is enabled (default: False)."""
        return self._data.get("trading_day", {}).get("enabled", False)
    
    @property
    def trading_day_start_hour(self) -> int:
        """Trading day start hour (default: 5)."""
        return self._data.get("trading_day", {}).get("start_hour", 5)
    
    @property
    def trading_day_start_minute(self) -> int:
        """Trading day start minute (default: 0)."""
        return self._data.get("trading_day", {}).get("start_minute", 0)
    
    def _get_env_or_config(self, env_key: str, config_key: str, default: Any) -> Any:
        """Get value from ENV (if set) or config, with fallback to default.
        
        Precedence: ENV → company JSON → default
        """
        env_value = os.environ.get(env_key)
        if env_value is not None:
            # Convert string ENV values to appropriate types
            if isinstance(default, bool):
                return env_value.lower() in ("true", "1", "yes", "on")
            elif isinstance(default, int):
                try:
                    return int(env_value)
                except ValueError:
                    return default
            else:
                return env_value
        return self._data.get("inventory", {}).get(config_key, default)
    
    @property
    def inventory_enabled(self) -> bool:
        """Whether inventory items are enabled (default: False).
        
        ENV override: {COMPANY_KEY}_ENABLE_INVENTORY_ITEMS
        """
        env_key = f"{self.company_key.upper()}_ENABLE_INVENTORY_ITEMS"
        return self._get_env_or_config(env_key, "enable_inventory_items", False)
    
    @property
    def allow_negative_inventory(self) -> bool:
        """Whether negative inventory is allowed (default: False).
        
        ENV override: {COMPANY_KEY}_ALLOW_NEGATIVE_INVENTORY
        """
        env_key = f"{self.company_key.upper()}_ALLOW_NEGATIVE_INVENTORY"
        return self._get_env_or_config(env_key, "allow_negative_inventory", False)
    
    @property
    def inventory_start_date(self) -> str:
        """Inventory start date as ISO string (default: "today").
        
        If "today", returns current date in YYYY-MM-DD format.
        ENV override: {COMPANY_KEY}_INVENTORY_START_DATE
        """
        env_key = f"{self.company_key.upper()}_INVENTORY_START_DATE"
        value = self._get_env_or_config(env_key, "inventory_start_date", "today")
        
        if value == "today":
            return datetime.now().strftime("%Y-%m-%d")
        return str(value)
    
    @property
    def default_qty_on_hand(self) -> int:
        """Default quantity on hand for new inventory items (default: 0).
        
        ENV override: {COMPANY_KEY}_DEFAULT_QTY_ON_HAND
        """
        env_key = f"{self.company_key.upper()}_DEFAULT_QTY_ON_HAND"
        return self._get_env_or_config(env_key, "default_qty_on_hand", 0)
    
    @property
    def auto_fix_wrong_type_items(self) -> bool:
        """Whether to automatically rename and inactivate wrong-type items to free names for inventory creation (default: False).
        
        ENV override: {COMPANY_KEY}_AUTO_FIX_WRONG_TYPE_ITEMS
        """
        env_key = f"{self.company_key.upper()}_AUTO_FIX_WRONG_TYPE_ITEMS"
        return self._get_env_or_config(env_key, "auto_fix_wrong_type_items", False)

    @property
    def inventory_sync_mode(self) -> str:
        """Inventory sync mode for upload pipeline.

        - "inline": preserve current behavior (patch existing inventory and optionally auto-fix wrong-type items inline)
        - "upload_fast": skip expensive existing-item patch path during upload; still create missing inventory items

        ENV override: {COMPANY_KEY}_INVENTORY_SYNC_MODE
        """
        env_key = f"{self.company_key.upper()}_INVENTORY_SYNC_MODE"
        mode = str(self._get_env_or_config(env_key, "inventory_sync_mode", "inline")).strip().lower()
        if mode not in {"inline", "upload_fast"}:
            return "inline"
        return mode

    @property
    def use_item_hierarchy(self) -> bool:
        """Always True. SubItem/ParentRef (category hierarchy) is always used for inventory items.
        Config/ENV value is ignored; kept for backward compatibility only.
        """
        return True

    @property
    def auto_fix_inv_start_date_blockers(self) -> bool:
        """Whether to automatically PATCH Item.InvStartDate for inventory start-date blockers before upload (default: False).
        ENV override: {COMPANY_KEY}_AUTO_FIX_INV_START_DATE_BLOCKERS
        """
        env_key = f"{self.company_key.upper()}_AUTO_FIX_INV_START_DATE_BLOCKERS"
        return self._get_env_or_config(env_key, "auto_fix_inv_start_date_blockers", False)

    @property
    def inv_start_date_floor(self) -> str:
        """Floor date (YYYY-MM-DD) for InvStartDate patches; do not set InvStartDate earlier than this.
        If not set in config, uses inventory_start_date (resolved)."""
        explicit = self._data.get("inventory", {}).get("inv_start_date_floor")
        if explicit is not None and str(explicit).strip():
            return str(explicit).strip()[:10]
        return self.inventory_start_date

    @property
    def product_mapping_file(self) -> Path:
        """Path to product category mapping CSV file (default: mappings/Product.Mapping.csv)."""
        mapping_file = self._data.get("inventory", {}).get("product_mapping_file", "mappings/Product.Mapping.csv")
        return OPS_ROOT / mapping_file

    @property
    def bypass_income_account_id(self) -> Optional[str]:
        """
        Income account ID for the bypass Service item (InvStartDate bypass mode).
        ENV override: {COMPANY_KEY}_BYPASS_INCOME_ACCOUNT_ID
        Config: qbo.bypass_income_account_id
        Required when --bypass-inventory-startdate is used.
        """
        env_key = f"{self.company_key.upper().replace('-', '_')}_BYPASS_INCOME_ACCOUNT_ID"
        value = os.environ.get(env_key)
        if value is not None and str(value).strip():
            return str(value).strip()
        return self._data.get("qbo", {}).get("bypass_income_account_id")
    
    def get_qbo_config(self) -> Dict[str, Any]:
        """Get QBO-specific configuration."""
        return self._data["qbo"].copy()
    
    def get_transform_config(self) -> Dict[str, Any]:
        """Get transform-specific configuration."""
        return self._data["transform"].copy()


def load_company_config(company_key: str) -> CompanyConfig:
    """
    Load company configuration by company key.
    
    Args:
        company_key: 'company_a' or 'company_b'
    
    Returns:
        CompanyConfig instance
    
    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    config_path = OPS_COMPANIES_DIR / f"{company_key}.json"
    
    return CompanyConfig(config_path)


def get_available_companies() -> list:
    """Return list of available company keys."""
    companies_dir = OPS_COMPANIES_DIR
    
    if not companies_dir.exists():
        return []
    
    companies = []
    for config_file in companies_dir.glob("*.json"):
        try:
            with open(config_file, "r") as f:
                data = json.load(f)
                if "company_key" in data:
                    companies.append(data["company_key"])
        except Exception:
            continue
    
    return sorted(companies)
