from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase

from code_scripts.run_pipeline import build_reconcile_payload, persist_reconcile_to_metadata


class RunPipelineMetadataPersistenceTests(SimpleTestCase):
    def test_build_reconcile_payload_normalizes_fields(self):
        payload = build_reconcile_payload(
            {
                "status": "MATCH",
                "epos_total": "100.50",
                "epos_count": "4",
                "qbo_total": 100.5,
                "qbo_count": 4,
                "difference": "0",
                "extra": "ignore",
            }
        )
        self.assertEqual(
            payload,
            {
                "status": "MATCH",
                "epos_total": 100.5,
                "epos_count": 4,
                "qbo_total": 100.5,
                "qbo_count": 4,
                "difference": 0.0,
            },
        )

    def test_persist_reconcile_to_metadata_updates_json_file(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            metadata_path = root / "last_transform.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump({"company_key": "company_a", "target_date": "2026-02-13"}, f)

            persisted = persist_reconcile_to_metadata(
                root,
                "last_transform.json",
                {
                    "status": "MISMATCH",
                    "epos_total": 200.0,
                    "epos_count": 8,
                    "qbo_total": 198.5,
                    "qbo_count": 8,
                    "difference": 1.5,
                },
            )

            self.assertTrue(persisted)
            with open(metadata_path, "r", encoding="utf-8") as f:
                data = json.load(f)

        self.assertIn("reconcile", data)
        self.assertEqual(data["reconcile"]["status"], "MISMATCH")
        self.assertEqual(data["reconcile"]["epos_total"], 200.0)
        self.assertEqual(data["reconcile"]["difference"], 1.5)

    def test_persist_reconcile_to_metadata_returns_false_when_missing(self):
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            persisted = persist_reconcile_to_metadata(root, "missing.json", {"status": "MATCH"})
        self.assertFalse(persisted)
