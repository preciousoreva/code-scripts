from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest import mock

from django.test import TestCase
from django.utils import timezone

from apps.epos_qbo.models import RunArtifact, RunJob
from apps.epos_qbo.services.metrics import (
    compute_sales_day_snapshot_for_companies,
    compute_sales_trend,
    compute_sales_trend_for_companies,
    extract_amount,
    extract_amount_hybrid,
)


class SalesTrendMetricsTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 14, 12, 0, 0))
        self.company_key = "company_a"

    def _create_run(self, *, status: str, company_key: str | None = None) -> RunJob:
        return RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=company_key or self.company_key,
            status=status,
            started_at=self.fixed_now - timedelta(minutes=5),
        )

    def _create_artifact(
        self,
        *,
        source_hash: str,
        processed_at: datetime | None,
        target_date: date | None,
        upload_stats: dict | None = None,
        reconcile_total: float | None = None,
        reconcile_status: str = "",
        run_status: str | None = None,
        unlinked: bool = False,
        company_key: str | None = None,
    ) -> RunArtifact:
        run_job = None
        if run_status and not unlinked:
            run_job = self._create_run(status=run_status, company_key=company_key)
        artifact = RunArtifact.objects.create(
            run_job=run_job,
            company_key=company_key or self.company_key,
            target_date=target_date,
            processed_at=processed_at,
            source_path=f"/tmp/{source_hash}.json",
            source_hash=source_hash,
            upload_stats_json=upload_stats or {},
            reconcile_epos_total=reconcile_total,
            reconcile_status=reconcile_status,
        )
        return artifact

    def test_extract_amount_prefers_upload_stats_key(self):
        artifact = RunArtifact(
            company_key=self.company_key,
            source_path="/tmp/a.json",
            source_hash="hash-a",
            upload_stats_json={"total_sales": "1234.50"},
            reconcile_epos_total=999.0,
        )
        self.assertEqual(extract_amount(artifact), Decimal("1234.50"))

    def test_extract_amount_falls_back_to_reconcile_total(self):
        artifact = RunArtifact(
            company_key=self.company_key,
            source_path="/tmp/b.json",
            source_hash="hash-b",
            upload_stats_json={"uploaded": 10},
            reconcile_epos_total=145000.75,
        )
        self.assertEqual(extract_amount(artifact), Decimal("145000.75"))

    def test_extract_amount_hybrid_can_prefer_reconcile_first(self):
        artifact = RunArtifact(
            company_key=self.company_key,
            source_path="/tmp/bb.json",
            source_hash="hash-bb",
            upload_stats_json={"total_amount": "100"},
            reconcile_epos_total=250.0,
        )
        self.assertEqual(
            extract_amount_hybrid(artifact, prefer_reconcile=True),
            Decimal("250.0"),
        )

    def test_extract_amount_missing_sources_returns_zero_and_logs_warning(self):
        artifact = RunArtifact(
            company_key=self.company_key,
            source_path="/tmp/c.json",
            source_hash="hash-c",
            upload_stats_json={"uploaded": 10},
            reconcile_epos_total=None,
        )
        with mock.patch("apps.epos_qbo.services.metrics.logger.warning") as warning_mock:
            amount = extract_amount(artifact)
        self.assertEqual(amount, Decimal("0"))
        warning_mock.assert_called_once()

    def test_compute_sales_trend_prev_week_zero_marks_new(self):
        self._create_artifact(
            source_hash="new-this-week",
            processed_at=self.fixed_now - timedelta(days=1),
            target_date=date(2026, 2, 13),
            upload_stats={"total_amount": "200000"},
            run_status=RunJob.STATUS_SUCCEEDED,
        )

        trend = compute_sales_trend(self.company_key, now=self.fixed_now)
        self.assertEqual(trend["sales_7d_total"], Decimal("200000"))
        self.assertEqual(trend["sales_7d_prev_total"], Decimal("0"))
        self.assertEqual(trend["sales_7d_pct_change"], 100.0)
        self.assertTrue(trend["sales_7d_is_new"])
        self.assertEqual(trend["sales_7d_trend_dir"], "up")
        self.assertEqual(trend["sales_7d_trend_text"], "↑ New vs last week")

    def test_compute_sales_trend_classifies_up_down_and_flat(self):
        self._create_artifact(
            source_hash="up-prev",
            processed_at=self.fixed_now - timedelta(days=10),
            target_date=date(2026, 2, 4),
            upload_stats={"total_amount": "100"},
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_up",
        )
        self._create_artifact(
            source_hash="up-this",
            processed_at=self.fixed_now - timedelta(days=2),
            target_date=date(2026, 2, 12),
            upload_stats={"total_amount": "120"},
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_up",
        )
        self.assertEqual(compute_sales_trend("company_up", now=self.fixed_now)["sales_7d_trend_dir"], "up")

        self._create_artifact(
            source_hash="down-prev",
            processed_at=self.fixed_now - timedelta(days=10),
            target_date=date(2026, 2, 4),
            upload_stats={"total_amount": "200"},
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_down",
        )
        self._create_artifact(
            source_hash="down-this",
            processed_at=self.fixed_now - timedelta(days=2),
            target_date=date(2026, 2, 12),
            upload_stats={"total_amount": "100"},
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_down",
        )
        self.assertEqual(compute_sales_trend("company_down", now=self.fixed_now)["sales_7d_trend_dir"], "down")

        self._create_artifact(
            source_hash="flat-prev",
            processed_at=self.fixed_now - timedelta(days=10),
            target_date=date(2026, 2, 4),
            upload_stats={"total_amount": "100"},
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_flat",
        )
        self._create_artifact(
            source_hash="flat-this",
            processed_at=self.fixed_now - timedelta(days=2),
            target_date=date(2026, 2, 12),
            upload_stats={"total_amount": "100.5"},
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_flat",
        )
        self.assertEqual(compute_sales_trend("company_flat", now=self.fixed_now)["sales_7d_trend_dir"], "flat")

    def test_compute_sales_trend_uses_latest_succeeded_per_day(self):
        day = date(2026, 2, 13)
        self._create_artifact(
            source_hash="day-old-success",
            processed_at=self.fixed_now - timedelta(days=1, hours=3),
            target_date=day,
            upload_stats={"total_amount": "100"},
            run_status=RunJob.STATUS_SUCCEEDED,
        )
        self._create_artifact(
            source_hash="day-new-success",
            processed_at=self.fixed_now - timedelta(days=1, hours=2),
            target_date=day,
            upload_stats={"total_amount": "150"},
            run_status=RunJob.STATUS_SUCCEEDED,
        )
        self._create_artifact(
            source_hash="day-latest-failed",
            processed_at=self.fixed_now - timedelta(days=1, hours=1),
            target_date=day,
            upload_stats={"total_amount": "500"},
            run_status=RunJob.STATUS_FAILED,
        )

        trend = compute_sales_trend(self.company_key, now=self.fixed_now)
        self.assertEqual(trend["sales_7d_total"], Decimal("150"))

    def test_compute_sales_trend_includes_succeeded_even_when_reconcile_mismatch(self):
        self._create_artifact(
            source_hash="mismatch-success",
            processed_at=self.fixed_now - timedelta(days=2),
            target_date=date(2026, 2, 12),
            upload_stats={"total_amount": "220"},
            reconcile_status="MISMATCH",
            run_status=RunJob.STATUS_SUCCEEDED,
        )

        trend = compute_sales_trend(self.company_key, now=self.fixed_now)
        self.assertEqual(trend["sales_7d_total"], Decimal("220"))

    def test_compute_sales_trend_uses_unlinked_fallback_only_when_no_succeeded(self):
        self._create_artifact(
            source_hash="failed-linked-day-1",
            processed_at=self.fixed_now - timedelta(days=3, hours=1),
            target_date=date(2026, 2, 11),
            upload_stats={"total_amount": "500"},
            run_status=RunJob.STATUS_FAILED,
        )
        self._create_artifact(
            source_hash="unlinked-day-1",
            processed_at=self.fixed_now - timedelta(days=3),
            target_date=date(2026, 2, 11),
            upload_stats={"total_amount": "120"},
            unlinked=True,
        )
        self._create_artifact(
            source_hash="succeeded-linked-day-2",
            processed_at=self.fixed_now - timedelta(days=2, hours=1),
            target_date=date(2026, 2, 12),
            upload_stats={"total_amount": "200"},
            run_status=RunJob.STATUS_SUCCEEDED,
        )
        self._create_artifact(
            source_hash="unlinked-day-2",
            processed_at=self.fixed_now - timedelta(days=2),
            target_date=date(2026, 2, 12),
            upload_stats={"total_amount": "300"},
            unlinked=True,
        )

        trend = compute_sales_trend(self.company_key, now=self.fixed_now)
        self.assertEqual(trend["sales_7d_total"], Decimal("320"))

    def test_compute_sales_trend_uses_imported_at_when_processed_at_missing(self):
        artifact = self._create_artifact(
            source_hash="imported-at-only",
            processed_at=None,
            target_date=None,
            upload_stats={"total_amount": "75"},
            unlinked=True,
        )
        imported_at = self.fixed_now - timedelta(days=2)
        RunArtifact.objects.filter(id=artifact.id).update(imported_at=imported_at)

        trend = compute_sales_trend(self.company_key, now=self.fixed_now)
        self.assertEqual(trend["sales_7d_total"], Decimal("75"))

    def test_compute_sales_trend_for_companies_prev_zero_math_and_label(self):
        self._create_artifact(
            source_hash="ov-new",
            processed_at=self.fixed_now - timedelta(hours=2),
            target_date=date(2026, 2, 14),
            reconcile_total=1200.0,
            run_status=RunJob.STATUS_SUCCEEDED,
        )
        trend = compute_sales_trend_for_companies(
            [self.company_key],
            now=self.fixed_now,
            window_hours=24,
            prefer_reconcile=True,
            comparison_label="vs yesterday",
            flat_symbol="—",
        )
        self.assertEqual(trend["total"], Decimal("1200.0"))
        self.assertEqual(trend["prev_total"], Decimal("0"))
        self.assertEqual(trend["pct_change"], 100.0)
        self.assertTrue(trend["is_new"])
        self.assertEqual(trend["trend_text"], "↑ New vs yesterday")

    def test_compute_sales_trend_for_companies_trend_dir_classification(self):
        self._create_artifact(
            source_hash="ov-flat-prev",
            processed_at=self.fixed_now - timedelta(hours=30),
            target_date=date(2026, 2, 13),
            reconcile_total=100.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_flat_ov",
        )
        self._create_artifact(
            source_hash="ov-flat-this",
            processed_at=self.fixed_now - timedelta(hours=6),
            target_date=date(2026, 2, 14),
            reconcile_total=100.5,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_flat_ov",
        )
        trend_flat = compute_sales_trend_for_companies(
            ["company_flat_ov"],
            now=self.fixed_now,
            window_hours=24,
            prefer_reconcile=True,
            comparison_label="vs yesterday",
            flat_symbol="—",
        )
        self.assertEqual(trend_flat["trend_dir"], "flat")
        self.assertIn("—", trend_flat["trend_text"])

    def test_compute_sales_day_snapshot_uses_latest_succeeded_per_company(self):
        self._create_artifact(
            source_hash="today-company-a-old",
            processed_at=self.fixed_now - timedelta(hours=10),
            target_date=date(2026, 2, 13),
            reconcile_total=3995250.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_a",
        )
        self._create_artifact(
            source_hash="today-company-a-new",
            processed_at=self.fixed_now - timedelta(hours=6),
            target_date=date(2026, 2, 12),
            reconcile_total=2645250.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_a",
        )
        self._create_artifact(
            source_hash="today-company-b-old",
            processed_at=self.fixed_now - timedelta(hours=9),
            target_date=date(2026, 2, 13),
            reconcile_total=9505350.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_b",
        )
        self._create_artifact(
            source_hash="today-company-b-new",
            processed_at=self.fixed_now - timedelta(hours=5),
            target_date=date(2026, 2, 12),
            reconcile_total=9374050.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_b",
        )

        trend = compute_sales_day_snapshot_for_companies(
            ["company_a", "company_b"],
            now=self.fixed_now,
            prefer_reconcile=True,
            comparison_label="vs yesterday",
            flat_symbol="—",
        )

        self.assertEqual(trend["total"], Decimal("12019300.0"))
        self.assertEqual(trend["sample_count"], 2)

    def test_compute_sales_day_snapshot_uses_previous_day_for_comparison(self):
        self._create_artifact(
            source_hash="yesterday",
            processed_at=self.fixed_now - timedelta(days=1, hours=2),
            target_date=date(2026, 2, 13),
            reconcile_total=100.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_a",
        )
        self._create_artifact(
            source_hash="today",
            processed_at=self.fixed_now - timedelta(hours=2),
            target_date=date(2026, 2, 12),
            reconcile_total=200.0,
            run_status=RunJob.STATUS_SUCCEEDED,
            company_key="company_a",
        )

        trend = compute_sales_day_snapshot_for_companies(
            ["company_a"],
            now=self.fixed_now,
            prefer_reconcile=True,
            comparison_label="vs yesterday",
            flat_symbol="—",
        )
        self.assertEqual(trend["total"], Decimal("200.0"))
        self.assertEqual(trend["prev_total"], Decimal("100.0"))
        self.assertEqual(trend["trend_dir"], "up")
