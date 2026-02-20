from __future__ import annotations

from datetime import datetime, timedelta

from django.test import TestCase
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import RunArtifact, RunJob


class OverviewTargetDateResolverTests(TestCase):
    def test_resolver_picks_latest_successful_artifact(self):
        succeeded_old = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=succeeded_old,
            company_key="company_a",
            target_date=datetime(2026, 2, 13).date(),
            processed_at=timezone.make_aware(datetime(2026, 2, 14, 8, 0, 0)),
            source_path="/tmp/a_old.json",
            source_hash="a-old",
        )

        failed_newer = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_FAILED,
        )
        RunArtifact.objects.create(
            run_job=failed_newer,
            company_key="company_a",
            target_date=datetime(2026, 2, 14).date(),
            processed_at=timezone.make_aware(datetime(2026, 2, 14, 10, 0, 0)),
            source_path="/tmp/a_failed.json",
            source_hash="a-failed",
        )

        succeeded_latest = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
        )
        latest_processed_at = timezone.make_aware(datetime(2026, 2, 14, 12, 0, 0))
        RunArtifact.objects.create(
            run_job=succeeded_latest,
            company_key="company_b",
            target_date=datetime(2026, 2, 14).date(),
            processed_at=latest_processed_at,
            source_path="/tmp/b_latest.json",
            source_hash="b-latest",
        )

        resolved = views.resolve_overview_target_date(["company_a", "company_b"])
        self.assertTrue(resolved["has_data"])
        self.assertEqual(resolved["target_date"].isoformat(), "2026-02-14")
        self.assertEqual(resolved["prev_target_date"].isoformat(), "2026-02-13")
        self.assertEqual(resolved["last_successful_at"], latest_processed_at)

    def test_resolver_uses_imported_at_when_processed_at_missing(self):
        succeeded = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        artifact = RunArtifact.objects.create(
            run_job=succeeded,
            company_key="company_a",
            target_date=datetime(2026, 2, 15).date(),
            processed_at=None,
            source_path="/tmp/a_imported_only.json",
            source_hash="a-imported-only",
        )

        resolved = views.resolve_overview_target_date(["company_a"])
        self.assertTrue(resolved["has_data"])
        self.assertEqual(resolved["target_date"].isoformat(), "2026-02-15")
        self.assertIsNotNone(resolved["last_successful_at"])
        self.assertLess(abs((resolved["last_successful_at"] - artifact.imported_at).total_seconds()), 1)

    def test_resolver_returns_no_data_when_no_successful_artifacts(self):
        failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_FAILED,
        )
        RunArtifact.objects.create(
            run_job=failed,
            company_key="company_a",
            target_date=datetime(2026, 2, 13).date(),
            processed_at=timezone.now() - timedelta(hours=2),
            source_path="/tmp/a_failed_only.json",
            source_hash="a-failed-only",
        )

        resolved = views.resolve_overview_target_date(["company_a"])
        self.assertFalse(resolved["has_data"])
        self.assertIsNone(resolved["target_date"])
        self.assertIsNone(resolved["prev_target_date"])
        self.assertIsNone(resolved["last_successful_at"])
