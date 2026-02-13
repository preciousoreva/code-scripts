from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.test import TestCase

from apps.epos_qbo.models import CompanyConfigRecord
from apps.epos_qbo.services.config_sync import checksum, sync_record_to_json, validate_company_config


def _valid_payload() -> dict:
    return {
        "company_key": "company_a",
        "display_name": "Company A",
        "qbo": {
            "realm_id": "123456",
            "deposit_account": "Cash on hand",
            "tax_mode": "vat_inclusive_7_5",
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
        },
        "output": {
            "csv_prefix": "sales_receipts",
            "metadata_file": "last_transform.json",
            "uploaded_docnumbers_file": "uploaded_docnumbers.json",
        },
        "slack": {"webhook_url_env_key": "SLACK_WEBHOOK_A"},
        "trading_day": {"enabled": False, "start_hour": 5, "start_minute": 0},
        "inventory": {"enable_inventory_items": False},
    }


class ConfigSyncTests(TestCase):
    def test_validator_rejects_missing_required_keys(self):
        payload = _valid_payload()
        del payload["qbo"]["realm_id"]

        result = validate_company_config(payload)

        self.assertFalse(result.valid)
        self.assertTrue(any("realm_id" in err for err in result.errors))

    def test_sync_record_to_json_writes_file_and_updates_checksum(self):
        record = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json=_valid_payload(),
        )

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            with mock.patch("apps.epos_qbo.services.config_sync.OPS_COMPANIES_DIR", temp_path):
                written_path = sync_record_to_json(record)

                self.assertTrue(written_path.exists())
                with open(written_path, "r", encoding="utf-8") as handle:
                    written_payload = json.load(handle)

        record.refresh_from_db()
        self.assertEqual(written_payload["company_key"], "company_a")
        self.assertEqual(record.checksum, checksum(record.config_json))
        self.assertIsNotNone(record.last_synced_to_json_at)
