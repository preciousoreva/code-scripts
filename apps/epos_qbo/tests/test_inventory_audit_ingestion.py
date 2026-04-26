from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from django.test import TestCase
from django.utils import timezone

from apps.epos_qbo.models import RunArtifact, RunJob
from apps.epos_qbo.services.artifact_ingestion import (
    _attach_inventory_artifacts_to_job,
    attach_recent_artifacts_to_job,
    ingest_inventory_audit_file,
    parse_inventory_audit_metadata,
)


def _payload(**overrides) -> dict:
    base = {
        "company_key": "company_a",
        "display_name": "ACME",
        "processed_at": "2026-04-14T12:00:00Z",
        "report_csv": "/tmp/reports/inventory_audit_company_a_2026-04-14.csv",
        "stock_csv": "/tmp/stock.csv",
        "qbo_csv": "/tmp/qbo.csv",
        "total_groups": 42,
        "status_counts": {
            "in_sync": 30,
            "needs_adjustment": 8,
            "ambiguous_in_qbo": 2,
            "missing_in_qbo": 2,
        },
        "apply": {"mode": "audit_only", "posted": 0, "skipped": 0},
    }
    base.update(overrides)
    return base


class InventoryAuditParseTests(TestCase):
    def test_parse_returns_none_for_missing_company(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "inventory_audit_x.json"
            path.write_text(json.dumps({"processed_at": "2026-04-14T00:00:00Z"}))
            self.assertIsNone(parse_inventory_audit_metadata(path))

    def test_parse_returns_dict(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "inventory_audit_x.json"
            path.write_text(json.dumps(_payload()))
            data = parse_inventory_audit_metadata(path)
        self.assertIsNotNone(data)
        assert data is not None
        self.assertEqual(data["company_key"], "company_a")


class InventoryAuditIngestTests(TestCase):
    def test_ingest_creates_artifact_with_inventory_kind(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "inventory_audit_company_a_x.json"
            path.write_text(json.dumps(_payload()))
            with mock.patch("apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", Path(td)):
                artifact, created = ingest_inventory_audit_file(path)

        self.assertTrue(created)
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.kind, RunArtifact.KIND_INVENTORY_AUDIT)
        self.assertEqual(artifact.company_key, "company_a")
        self.assertEqual(artifact.upload_stats_json["status_counts"]["in_sync"], 30)
        self.assertEqual(artifact.upload_stats_json["total_groups"], 42)
        self.assertEqual(
            artifact.upload_stats_json["report_csv"],
            "/tmp/reports/inventory_audit_company_a_2026-04-14.csv",
        )

    def test_ingest_is_idempotent(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "inventory_audit_company_a_y.json"
            path.write_text(json.dumps(_payload()))
            with mock.patch("apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", Path(td)):
                _, created1 = ingest_inventory_audit_file(path)
                _, created2 = ingest_inventory_audit_file(path)
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual(
            RunArtifact.objects.filter(kind=RunArtifact.KIND_INVENTORY_AUDIT).count(), 1
        )

    def test_attach_by_run_job_id_metadata(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_SYNC,
            company_key="company_a",
            status=RunJob.STATUS_QUEUED,
        )
        payload = _payload(run_job_id=str(job.id))
        with TemporaryDirectory() as td:
            reports_dir = Path(td)
            path = reports_dir / "inventory_audit_company_a_z.json"
            path.write_text(json.dumps(payload))
            with mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_REPORTS_DIR", reports_dir
            ), mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", reports_dir
            ):
                attached = _attach_inventory_artifacts_to_job(job)
        self.assertEqual(attached, 1)
        artifact = RunArtifact.objects.get(kind=RunArtifact.KIND_INVENTORY_AUDIT)
        self.assertEqual(artifact.run_job_id, job.id)

    def test_attach_by_company_and_time_window_without_job_id(self):
        dispatched = timezone.now()
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_SYNC,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
            dispatched_at=dispatched,
        )
        processed_at = (dispatched - timedelta(minutes=5)).isoformat()
        payload = _payload(processed_at=processed_at)  # no run_job_id
        with TemporaryDirectory() as td:
            reports_dir = Path(td)
            path = reports_dir / "inventory_audit_company_a_w.json"
            path.write_text(json.dumps(payload))
            with mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_REPORTS_DIR", reports_dir
            ), mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", reports_dir
            ):
                attached = _attach_inventory_artifacts_to_job(job)
        self.assertEqual(attached, 1)

    def test_attach_skips_when_company_mismatch_without_job_id(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_SYNC,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
            dispatched_at=timezone.now(),
        )
        payload = _payload(company_key="company_b")  # mismatch
        with TemporaryDirectory() as td:
            reports_dir = Path(td)
            path = reports_dir / "inventory_audit_company_b_w.json"
            path.write_text(json.dumps(payload))
            with mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_REPORTS_DIR", reports_dir
            ), mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", reports_dir
            ):
                attached = _attach_inventory_artifacts_to_job(job)
        self.assertEqual(attached, 0)

    def test_attach_recent_artifacts_to_job_dispatches_by_scope(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_SYNC,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
            dispatched_at=timezone.now(),
        )
        payload = _payload(run_job_id=str(job.id))
        with TemporaryDirectory() as td:
            reports_dir = Path(td)
            path = reports_dir / "inventory_audit_company_a_v.json"
            path.write_text(json.dumps(payload))
            with mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_REPORTS_DIR", reports_dir
            ), mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", reports_dir
            ):
                attached = attach_recent_artifacts_to_job(job)
        self.assertEqual(attached, 1)
