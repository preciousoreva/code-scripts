from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.test import SimpleTestCase

from apps.epos_qbo.forms import CompanyBasicForm
from apps.epos_qbo.services.config_sync import build_basic_payload
from code_scripts.company_config import (
    CompanyConfig,
    ensure_company_runtime_compatible,
    get_qbo_api_base_url,
    get_runtime_qbo_environment,
    normalize_qbo_environment,
)


def _basic_form_data(environment: str = "production") -> dict[str, str]:
    return {
        "company_key": "company_sandbox",
        "display_name": "Sandbox Co",
        "qbo_environment": environment,
        "realm_id": "4620816365123456789",
        "deposit_account": "Undeposited Funds",
        "tax_mode": "vat_inclusive_7_5",
        "epos_username_env_key": "EPOS_USERNAME_A",
        "epos_password_env_key": "EPOS_PASSWORD_A",
        "csv_prefix": "sales_receipts",
        "metadata_file": "last_transform.json",
        "uploaded_docnumbers_file": "uploaded_docnumbers.json",
    }


class CompanyBasicFormEnvironmentTests(SimpleTestCase):
    def test_build_basic_payload_includes_qbo_environment(self):
        form = CompanyBasicForm(data=_basic_form_data(environment="sandbox"))
        self.assertTrue(form.is_valid(), form.errors)

        payload = build_basic_payload(form)

        self.assertEqual(payload["qbo"]["environment"], "sandbox")

    def test_build_basic_payload_defaults_to_production(self):
        form = CompanyBasicForm(data=_basic_form_data(environment="production"))
        self.assertTrue(form.is_valid(), form.errors)

        payload = build_basic_payload(form)

        self.assertEqual(payload["qbo"]["environment"], "production")


class EnvironmentHelpersTests(SimpleTestCase):
    def test_normalize_qbo_environment_aliases_map_to_sandbox(self):
        for alias in ("sandbox", "development", "dev", "stage", "staging", "test"):
            self.assertEqual(normalize_qbo_environment(alias), "sandbox", alias)

    def test_normalize_qbo_environment_defaults_when_unset(self):
        self.assertEqual(normalize_qbo_environment(None), "production")
        self.assertEqual(normalize_qbo_environment(""), "production")

    def test_get_qbo_api_base_url_routes_sandbox_separately(self):
        self.assertEqual(
            get_qbo_api_base_url("sandbox"),
            "https://sandbox-quickbooks.api.intuit.com",
        )
        self.assertEqual(
            get_qbo_api_base_url("production"),
            "https://quickbooks.api.intuit.com",
        )

    def test_get_runtime_qbo_environment_reads_env_var(self):
        with mock.patch.dict(os.environ, {"OIAT_RUNTIME_ENV": "sandbox"}, clear=False):
            self.assertEqual(get_runtime_qbo_environment(), "sandbox")
        with mock.patch.dict(os.environ, {"OIAT_RUNTIME_ENV": "production"}, clear=False):
            self.assertEqual(get_runtime_qbo_environment(), "production")


class RuntimeCompatibilityGuardTests(SimpleTestCase):
    def _write_config(self, tmp_dir: str, environment: str) -> Path:
        payload = {
            "company_key": "company_sandbox",
            "display_name": "Sandbox Co",
            "qbo": {
                "environment": environment,
                "realm_id": "4620816365123456789",
                "deposit_account": "Undeposited Funds",
                "tax_mode": "vat_inclusive_7_5",
                "tax_rate": 0.075,
            },
            "epos": {
                "username_env_key": "EPOS_USERNAME_A",
                "password_env_key": "EPOS_PASSWORD_A",
            },
            "transform": {
                "group_by": ["date", "tender"],
                "date_format": "%Y-%m-%d",
                "receipt_prefix": "SR",
                "receipt_number_format": "date_tender_sequence",
                "aggregate_products": False,
            },
            "output": {
                "csv_prefix": "sales_receipts",
                "metadata_file": "last_transform.json",
                "uploaded_docnumbers_file": "uploaded_docnumbers.json",
            },
            "slack": {"webhook_url_env_key": "SLACK_WEBHOOK_URL_A"},
            "trading_day": {"enabled": False, "start_hour": 5, "start_minute": 0},
            "inventory": {"enable_inventory_items": False},
        }
        config_path = Path(tmp_dir) / "company_sandbox.json"
        config_path.write_text(json.dumps(payload), encoding="utf-8")
        return config_path

    def test_ensure_company_runtime_compatible_rejects_mismatch(self):
        with TemporaryDirectory() as tmp_dir:
            config_path = self._write_config(tmp_dir, environment="sandbox")
            config = CompanyConfig(config_path)

            with mock.patch.dict(os.environ, {"OIAT_RUNTIME_ENV": "production"}, clear=False):
                with self.assertRaises(RuntimeError) as ctx:
                    ensure_company_runtime_compatible(config)

            self.assertIn("QBO environment mismatch", str(ctx.exception))

    def test_ensure_company_runtime_compatible_accepts_match(self):
        with TemporaryDirectory() as tmp_dir:
            config_path = self._write_config(tmp_dir, environment="sandbox")
            config = CompanyConfig(config_path)

            with mock.patch.dict(os.environ, {"OIAT_RUNTIME_ENV": "sandbox"}, clear=False):
                ensure_company_runtime_compatible(config)

    def test_ensure_company_runtime_compatible_defaults_production(self):
        with TemporaryDirectory() as tmp_dir:
            config_path = self._write_config(tmp_dir, environment="production")
            config = CompanyConfig(config_path)

            env_without_runtime = {k: v for k, v in os.environ.items() if k != "OIAT_RUNTIME_ENV"}
            with mock.patch.dict(os.environ, env_without_runtime, clear=True):
                ensure_company_runtime_compatible(config)
