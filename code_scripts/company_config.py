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

from code_scripts.paths import OPS_COMPANIES_DIR, REPO_CODE_SCRIPTS_DIR


def normalize_qbo_environment(raw_value: str | None, default: str = "production") -> str:
    cleaned = str(raw_value or "").strip().lower()
    if cleaned in {"sandbox", "development", "dev", "stage", "staging", "test"}:
        return "sandbox"
    if cleaned == "production":
        return "production"
    return default


def get_runtime_qbo_environment() -> str:
    return normalize_qbo_environment(os.getenv("OIAT_RUNTIME_ENV"), default="production")


def get_qbo_api_base_url(environment: str | None = None) -> str:
    resolved = normalize_qbo_environment(environment, default=get_runtime_qbo_environment())
    if resolved == "sandbox":
        return "https://sandbox-quickbooks.api.intuit.com"
    return "https://quickbooks.api.intuit.com"


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
    def owner(self) -> Optional[str]:
        """Optional owner tag for developer/staging configs."""
        value = self._data.get("owner")
        return str(value).strip() if value is not None and str(value).strip() else None

    @property
    def source_company_key(self) -> Optional[str]:
        """Optional pointer to the production company this sandbox config mirrors."""
        value = self._data.get("source_company_key")
        return str(value).strip() if value is not None and str(value).strip() else None
    
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
    def qbo_environment(self) -> str:
        """QBO environment for this company: production or sandbox."""
        return normalize_qbo_environment(self._data.get("qbo", {}).get("environment"), default="production")
    
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
    def aggregate_products(self) -> bool:
        """Whether to normalize pack-multiplier product names and aggregate duplicate
        product rows within each tender/receipt group during transform.

        When True the transform step will:
        1. Strip trailing ``*N`` suffixes from product names (e.g. "WATER 50CL*12" → "WATER 50CL").
        2. Multiply ``ItemQuantity`` by N to get effective units.
        3. Collapse rows that share the same tender + normalized product name,
           summing quantities and monetary columns.

        Default ``False`` — opt-in per company via ``transform.aggregate_products``.
        """
        return self._data.get("transform", {}).get("aggregate_products", False)

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
        return REPO_CODE_SCRIPTS_DIR / mapping_file

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

    @property
    def inventory_adjustment_account_id(self) -> Optional[str]:
        """
        Legacy Chart of Accounts Id for historical InventoryAdjustment workflows.
        Forward inventory quantity apply is disabled; prefer
        opening_balance_adjust_account_id for manual correction previews.

        Config: qbo.inventory_adjustment_account_id
        ENV:    {COMPANY_KEY}_INVENTORY_ADJUSTMENT_ACCOUNT_ID
        """
        env_key = f"{self.company_key.upper().replace('-', '_')}_INVENTORY_ADJUSTMENT_ACCOUNT_ID"
        value = os.environ.get(env_key)
        if value is not None and str(value).strip():
            return str(value).strip()
        raw = self._data.get("qbo", {}).get("inventory_adjustment_account_id")
        if raw is None:
            return None
        s = str(raw).strip()
        return s or None

    @property
    def opening_balance_adjust_account_id(self) -> Optional[str]:
        """
        Chart of Accounts Id used as AdjustAccountRef for baseline/opening-balance-style
        inventory quantity corrections.

        Config: qbo.opening_balance_adjust_account_id
        ENV:    {COMPANY_KEY}_OPENING_BALANCE_ADJUST_ACCOUNT_ID
        """
        env_key = f"{self.company_key.upper().replace('-', '_')}_OPENING_BALANCE_ADJUST_ACCOUNT_ID"
        value = os.environ.get(env_key)
        if value is not None and str(value).strip():
            return str(value).strip()
        raw = self._data.get("qbo", {}).get("opening_balance_adjust_account_id")
        if raw is None:
            return None
        s = str(raw).strip()
        return s or None

    @property
    def opening_balance_adjust_account_name(self) -> Optional[str]:
        """
        Display name for the account configured by opening_balance_adjust_account_id.

        Config: qbo.opening_balance_adjust_account_name
        ENV:    {COMPANY_KEY}_OPENING_BALANCE_ADJUST_ACCOUNT_NAME
        """
        env_key = f"{self.company_key.upper().replace('-', '_')}_OPENING_BALANCE_ADJUST_ACCOUNT_NAME"
        value = os.environ.get(env_key)
        if value is not None and str(value).strip():
            return str(value).strip()
        raw = self._data.get("qbo", {}).get("opening_balance_adjust_account_name")
        if raw is None:
            return None
        s = str(raw).strip()
        return s or None

    @property
    def inventory_max_qty_delta(self) -> Optional[float]:
        """
        Per-item absolute qty-delta safety cap used by inventory preview modes.

        Any manual correction candidate whose |QtyDiff| exceeds this cap is
        skipped for review. A value of 0 or negative disables the cap.

        Config: qbo.inventory_max_qty_delta
        ENV:    {COMPANY_KEY}_INVENTORY_MAX_QTY_DELTA
        """
        env_key = f"{self.company_key.upper().replace('-', '_')}_INVENTORY_MAX_QTY_DELTA"
        raw = os.environ.get(env_key)
        if raw is None or str(raw).strip() == "":
            raw = self._data.get("qbo", {}).get("inventory_max_qty_delta")
        if raw is None or str(raw).strip() == "":
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

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


def ensure_company_runtime_compatible(config: CompanyConfig) -> None:
    """
    Prevent sandbox runtimes from using production company configs and vice versa.
    """
    runtime_environment = get_runtime_qbo_environment()
    company_environment = config.qbo_environment
    if runtime_environment != company_environment:
        raise RuntimeError(
            "QBO environment mismatch.\n"
            f"Runtime environment: {runtime_environment}\n"
            f"Company config environment: {company_environment}\n"
            f"Company: {config.company_key} ({config.display_name})\n"
            "Use a company JSON with the matching qbo.environment or switch OIAT_RUNTIME_ENV."
        )


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
