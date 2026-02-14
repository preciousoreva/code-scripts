from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.test import TestCase

from apps.epos_qbo.models import RunArtifact, RunJob
from apps.epos_qbo.services.artifact_ingestion import (
    attach_recent_artifacts_to_job,
    ingest_metadata_file,
    parse_metadata_file,
)


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
            "reconcile": {
                "status": "MATCH",
                "difference": 0.0,
                "epos_total": 145000.5,
                "epos_count": 200,
                "qbo_total": 145000.5,
                "qbo_count": 200,
            },
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
        self.assertEqual(parsed.reconcile_status, "MATCH")
        self.assertEqual(parsed.reconcile_epos_total, 145000.5)
        self.assertEqual(parsed.reconcile_qbo_total, 145000.5)
        self.assertEqual(parsed.reconcile_epos_count, 200)
        self.assertEqual(parsed.reconcile_qbo_count, 200)

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
        self.assertEqual(artifact_2.reconcile_epos_total, 145000.5)
        self.assertEqual(artifact_2.reconcile_qbo_total, 145000.5)
        self.assertEqual(artifact_2.reconcile_epos_count, 200)
        self.assertEqual(artifact_2.reconcile_qbo_count, 200)

    def test_parse_metadata_file_without_reconcile_totals(self):
        payload = self._metadata_payload()
        payload["reconcile"] = {"status": "NOT RUN", "reason": "not available"}
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "last_company_a_transform.json"
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            parsed = parse_metadata_file(path)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.reconcile_status, "NOT RUN")
        self.assertIsNone(parsed.reconcile_epos_total)
        self.assertIsNone(parsed.reconcile_qbo_total)
        self.assertIsNone(parsed.reconcile_epos_count)
        self.assertIsNone(parsed.reconcile_qbo_count)

    def test_attach_recent_artifacts_to_job_does_not_cross_link_single_company(self):
        job = RunJob.objects.create(scope=RunJob.SCOPE_SINGLE, company_key="company_b")

        payload_a = self._metadata_payload()
        payload_b = self._metadata_payload()
        payload_b["company_key"] = "company_b"
        payload_b["processed_at"] = "2026-02-10T12:00:00Z"
        payload_b["target_date"] = "2026-02-11"

        with TemporaryDirectory() as temp_dir:
            uploaded_dir = Path(temp_dir)
            metadata_a = uploaded_dir / "last_company_a_transform.json"
            metadata_b = uploaded_dir / "last_company_b_transform.json"
            with open(metadata_a, "w", encoding="utf-8") as handle:
                json.dump(payload_a, handle)
            with open(metadata_b, "w", encoding="utf-8") as handle:
                json.dump(payload_b, handle)

            with mock.patch("apps.epos_qbo.services.artifact_ingestion.OPS_UPLOADED_DIR", uploaded_dir):
                attached_count = attach_recent_artifacts_to_job(job)

        self.assertEqual(attached_count, 1)
        artifact_a = RunArtifact.objects.get(company_key="company_a")
        artifact_b = RunArtifact.objects.get(company_key="company_b")
        self.assertIsNone(artifact_a.run_job_id)
        self.assertEqual(artifact_b.run_job_id, job.id)
