from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.test import TestCase

from apps.epos_qbo.models import RunArtifact, RunJob
from apps.epos_qbo.services.artifact_ingestion import ingest_metadata_file, parse_metadata_file


class ArtifactIngestionTests(TestCase):
    def _metadata_payload(self) -> dict:
        return {
            "company_key": "company_a",
            "target_date": "2026-02-10",
            "processed_at": "2026-02-10T11:00:00Z",
            "rows_total": 250,
            "rows_kept": 200,
            "rows_non_target": 50,
            "upload_stats": {"created": 200, "failed": 0},
            "reconcile": {"status": "OK", "difference": 0},
            "raw_file": "raw.csv",
            "processed_files": ["processed.csv"],
        }

    def test_parse_metadata_file(self):
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "last_company_a_transform.json"
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(self._metadata_payload(), handle)

            parsed = parse_metadata_file(path)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.company_key, "company_a")
        self.assertEqual(parsed.rows_kept, 200)
        self.assertEqual(parsed.reliability_status, RunArtifact.RELIABILITY_WARNING)

    def test_ingest_deduplicates_by_source_hash(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_a")
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "last_company_a_transform.json"
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(self._metadata_payload(), handle)

            with mock.patch("apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", Path(temp_dir)):
                artifact_1, created_1 = ingest_metadata_file(path, run_job=job)
                artifact_2, created_2 = ingest_metadata_file(path, run_job=job)

        self.assertIsNotNone(artifact_1)
        self.assertIsNotNone(artifact_2)
        self.assertTrue(created_1)
        self.assertFalse(created_2)
        self.assertEqual(RunArtifact.objects.count(), 1)
