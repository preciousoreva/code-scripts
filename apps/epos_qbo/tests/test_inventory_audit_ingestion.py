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
    ingest_inventory_pipeline_file,
    parse_inventory_audit_metadata,
    parse_inventory_pipeline_metadata,
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


def _pipeline_payload(**overrides) -> dict:
    base = {
        "run_type": "inventory_pipeline",
        "company_key": "company_a",
        "display_name": "ACME",
        "started_at": "2026-04-14T11:59:00Z",
        "finished_at": "2026-04-14T12:00:00Z",
        "summary_json": "/tmp/reports/inventory_pipeline_company_a_120000.json",
        "summary_csv": "/tmp/reports/inventory_pipeline_company_a_120000.csv",
        "stock_csv": "/tmp/stock.csv",
        "qbo_csv": "/tmp/qbo.csv",
        "products_checked": 1,
        "already_correct": 1,
        "in_sync": 1,
        "catalog_fixes_applied": 1,
        "base_items_created": 0,
        "duplicate_base_items_resolved": 0,
        "quantity_updates_applied": 0,
        "blocked_items": 0,
        "missing_base_item_in_qbo": 0,
        "duplicate_base_items_in_qbo": 0,
        "epos_negative_rows_clamped": 0,
        "epos_negative_units_clamped": 0.0,
        "epos_negative_stock_policy": "clamp_to_zero",
        "skipped_unsupported": 0,
        "skipped_safely": 0,
        "still_needs_review": 0,
        "max_catalog_fixes": 1,
        "max_quantity_adjustments": 1,
        "final_status_counts": {"in_sync": 1},
        "final_catalog_issue_counts": {"exact_name_match": 1},
        "unsupported_catalog_issues": {"missing_from_qbo": 0},
        "child_reports": {
            "catalog_cleanup": "/tmp/reports/catalog_cleanup_company_a_120000.csv",
            "final_audit": "/tmp/reports/inventory_audit_company_a_120000.csv",
        },
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

    def test_parse_pipeline_returns_dict(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "inventory_pipeline_x.json"
            path.write_text(json.dumps(_pipeline_payload()))
            data = parse_inventory_pipeline_metadata(path)
        self.assertIsNotNone(data)
        assert data is not None
        self.assertEqual(data["run_type"], "inventory_pipeline")
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

    def test_ingest_pipeline_summary_creates_inventory_artifact(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "inventory_pipeline_company_a_120000.json"
            path.write_text(json.dumps(_pipeline_payload(summary_json=str(path))))
            with mock.patch("apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", Path(td)):
                artifact, created = ingest_inventory_pipeline_file(path)

        self.assertTrue(created)
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.kind, RunArtifact.KIND_INVENTORY_AUDIT)
        self.assertEqual(artifact.company_key, "company_a")
        self.assertEqual(artifact.rows_total, 1)
        self.assertEqual(artifact.rows_kept, 1)
        self.assertEqual(artifact.upload_stats_json["report_type"], "inventory_pipeline")
        self.assertEqual(artifact.upload_stats_json["catalog_fixes_applied"], 1)
        self.assertEqual(artifact.upload_stats_json["in_sync"], 1)
        self.assertEqual(artifact.upload_stats_json["blocked_items"], 0)
        self.assertEqual(artifact.upload_stats_json["base_items_created"], 0)
        self.assertEqual(artifact.upload_stats_json["duplicate_base_items_resolved"], 0)
        self.assertEqual(artifact.upload_stats_json["quantity_updates_applied"], 0)
        self.assertEqual(artifact.upload_stats_json["epos_negative_rows_clamped"], 0)
        self.assertEqual(artifact.upload_stats_json["epos_negative_units_clamped"], 0.0)
        self.assertEqual(artifact.upload_stats_json["epos_negative_stock_policy"], "clamp_to_zero")
        self.assertIn(
            "/tmp/reports/inventory_pipeline_company_a_120000.csv",
            artifact.processed_files_json,
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

    def test_attach_recent_artifacts_to_pipeline_job_links_pipeline_summary(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_RUNNING,
            dispatched_at=timezone.now(),
        )
        payload = _pipeline_payload(run_job_id=str(job.id))
        with TemporaryDirectory() as td:
            reports_dir = Path(td)
            path = reports_dir / "inventory_pipeline_company_a_120000.json"
            path.write_text(json.dumps(payload))
            with mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_REPORTS_DIR", reports_dir
            ), mock.patch(
                "apps.epos_qbo.services.artifact_ingestion.OPS_LOGS_DIR", reports_dir
            ):
                attached = attach_recent_artifacts_to_job(job)
        self.assertEqual(attached, 1)
        artifact = RunArtifact.objects.get(kind=RunArtifact.KIND_INVENTORY_AUDIT)
        self.assertEqual(artifact.run_job_id, job.id)
        self.assertEqual(artifact.upload_stats_json["report_type"], "inventory_pipeline")
