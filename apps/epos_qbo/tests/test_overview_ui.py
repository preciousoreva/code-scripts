from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from unittest import mock

from django.contrib.auth.models import Permission
from django.contrib.auth.models import User
from django.template.loader import render_to_string
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo import views
from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob
from apps.epos_qbo.tests.utils import suppress_expected_request_logs


class OverviewUIContextTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 13, 12, 0, 0))
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123456789"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "inventory": {"enable_inventory_items": True},
            },
        )

    def _token_payload(self) -> dict:
        now_ts = int(self.fixed_now.timestamp())
        return {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "expires_at": now_ts + 3600,
            "refresh_expires_at": now_ts + (30 * 86400),
            "updated_at": now_ts,
            "environment": "production",
        }

    def _overview_context(self, revenue_period: str = "7d", *, company_key: str | None = None) -> dict:
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            with suppress_expected_request_logs():
                return views._overview_context(revenue_period, company_key=company_key)

    def _company_row(self) -> dict:
        context = self._overview_context()
        return next(item for item in context["companies"] if item["company_key"] == self.company.company_key)

    def _set_inventory_enabled(self, enabled: bool = True):
        cfg = self.company.config_json
        cfg["inventory"] = {"enable_inventory_items": enabled}
        self.company.config_json = cfg
        self.company.save(update_fields=["config_json"])

    def _create_sales_run(self, *, status=RunJob.STATUS_SUCCEEDED, minutes_ago=60, with_artifact=False, reconcile_status="MATCH"):
        finished_at = self.fixed_now - timedelta(minutes=minutes_ago)
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=status,
            started_at=finished_at - timedelta(minutes=2),
            finished_at=finished_at,
        )
        if with_artifact:
            RunArtifact.objects.create(
                run_job=run,
                company_key=self.company.company_key,
                kind=RunArtifact.KIND_SALES_UPLOAD,
                target_date=(self.fixed_now - timedelta(days=1)).date(),
                processed_at=finished_at,
                source_path=f"/tmp/sales_{minutes_ago}.json",
                source_hash=f"sales-{minutes_ago}",
                reconcile_status=reconcile_status,
                upload_stats_json={"uploaded": 1, "failed": 0},
            )
        return run

    def _create_inventory_run(
        self,
        *,
        status=RunJob.STATUS_SUCCEEDED,
        minutes_ago=10,
        products_checked=147,
        in_sync=147,
        blocked_items=0,
        still_needs_review=0,
        updates=0,
        final_status_counts=None,
        inventory_stats_extra=None,
        with_artifact=True,
    ):
        finished_at = self.fixed_now - timedelta(minutes=minutes_ago)
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key=self.company.company_key,
            status=status,
            started_at=finished_at - timedelta(minutes=2),
            finished_at=finished_at,
        )
        if with_artifact:
            upload_stats = {
                "report_type": "inventory_pipeline",
                "products_checked": products_checked,
                "in_sync": in_sync,
                "blocked_items": blocked_items,
                "still_needs_review": still_needs_review,
                "catalog_fixes_applied": updates,
                "base_items_created": 0,
                "duplicate_base_items_resolved": 0,
                "quantity_updates_applied": 0,
                "final_status_counts": final_status_counts or {"in_sync": in_sync},
            }
            if inventory_stats_extra:
                upload_stats.update(inventory_stats_extra)
            RunArtifact.objects.create(
                run_job=run,
                company_key=self.company.company_key,
                kind=RunArtifact.KIND_INVENTORY_AUDIT,
                target_date=None,
                processed_at=finished_at,
                source_path=f"/tmp/inventory_pipeline_company_a_{minutes_ago}.json",
                source_hash=f"inventory-{minutes_ago}",
                rows_total=products_checked,
                rows_kept=in_sync,
                rows_non_target=blocked_items,
                upload_stats_json=upload_stats,
            )
        return run

    def test_inventory_pipeline_run_does_not_set_sales_not_reconciled(self):
        self._create_inventory_run(products_checked=147, in_sync=147, blocked_items=0)

        company_row = self._company_row()

        self.assertEqual(company_row["sales_status"]["label"], "No successful sales sync recorded")
        self.assertEqual(company_row["inventory_status"]["label"], "In sync")
        self.assertNotEqual(company_row["sales_status"]["label"], "Not reconciled")

    def test_sales_unreconciled_and_inventory_in_sync_are_separate(self):
        self._create_sales_run(status=RunJob.STATUS_SUCCEEDED, minutes_ago=20, with_artifact=False)
        self._create_inventory_run(products_checked=147, in_sync=147, blocked_items=0, minutes_ago=5)

        company_row = self._company_row()

        self.assertEqual(company_row["sales_status"]["label"], "Not reconciled")
        self.assertEqual(company_row["inventory_status"]["label"], "In sync")
        self.assertEqual(company_row["inventory_status"]["products_checked"], 147)
        self.assertEqual(company_row["inventory_status"]["blocked_items"], 0)

    def test_inventory_blocked_items_show_needs_review(self):
        self._create_inventory_run(
            products_checked=147,
            in_sync=146,
            blocked_items=1,
            still_needs_review=1,
            final_status_counts={"in_sync": 146, "ambiguous_in_qbo": 1},
        )

        company_row = self._company_row()

        self.assertEqual(company_row["inventory_status"]["label"], "Needs review")
        self.assertEqual(company_row["inventory_status"]["blocked_items"], 1)

    def test_no_inventory_run_shows_not_checked(self):
        self._set_inventory_enabled(True)
        self._create_sales_run(with_artifact=True, reconcile_status="MATCH")

        company_row = self._company_row()

        self.assertTrue(company_row["inventory_enabled"])
        self.assertEqual(company_row["sales_status"]["label"], "Reconciled")
        self.assertEqual(company_row["inventory_status"]["label"], "Not checked")
        self.assertEqual(company_row["status"], "unknown")

    def test_inventory_capability_accepts_boolean_like_config_values(self):
        for raw in (True, "true", "1", "yes", "on"):
            with self.subTest(raw=raw):
                cfg = self.company.config_json
                cfg["inventory"] = {"enable_inventory_items": raw}
                self.company.config_json = cfg
                self.assertTrue(views._company_inventory_enabled(self.company))

        cfg = self.company.config_json
        cfg["inventory"] = {"enable_inventory_items": "false"}
        self.company.config_json = cfg
        self.assertFalse(views._company_inventory_enabled(self.company))

    def test_inventory_disabled_omits_operational_inventory_from_overview(self):
        self._set_inventory_enabled(False)
        self._create_sales_run(with_artifact=True, reconcile_status="MATCH")

        context = self._overview_context()
        company_row = next(item for item in context["companies"] if item["company_key"] == self.company.company_key)
        html = render_to_string(
            "components/company_list.html",
            {
                "companies": [company_row],
                "revenue_company_options": [],
                "revenue_period_options": [],
                "revenue_chart_payload": {},
            },
        )

        self.assertFalse(company_row["inventory_enabled"])
        self.assertEqual(company_row["status"], "healthy")
        self.assertNotIn("Inventory: Not checked", html)
        self.assertNotIn("Inventory sync:", html)

    def test_inventory_disabled_omits_operational_inventory_from_company_card(self):
        self._set_inventory_enabled(False)
        run = self._create_sales_run(with_artifact=True, reconcile_status="MATCH")
        artifact = RunArtifact.objects.get(run_job=run)
        company_data = views._enrich_company_data(
            self.company,
            run,
            preloaded={
                "latest_activity_job": run,
                "latest_sales_job": run,
                "latest_sales_artifact": artifact,
                "latest_successful_sales_artifact": artifact,
                "artifacts_today": [artifact],
                "token_info": {"severity": "healthy", "display_label": "Connected", "display_subtext": ""},
                "sales_reconcile_statuses_by_company_job": {
                    (self.company.company_key, str(run.id)): ["MATCH"]
                },
            },
        )
        html = render_to_string("components/company_cards.html", {"companies_data": [company_data]})

        self.assertFalse(company_data["inventory_enabled"])
        self.assertNotIn("Inventory: Not checked", html)
        self.assertNotIn("Inventory Sync", html)

    def test_inventory_enabled_not_checked_renders_inventory_marker(self):
        self._set_inventory_enabled(True)
        self._create_sales_run(with_artifact=True, reconcile_status="MATCH")

        context = self._overview_context()
        company_row = next(item for item in context["companies"] if item["company_key"] == self.company.company_key)
        html = render_to_string(
            "components/company_list.html",
            {
                "companies": [company_row],
                "revenue_company_options": [],
                "revenue_period_options": [],
                "revenue_chart_payload": {},
            },
        )

        self.assertTrue(company_row["inventory_enabled"])
        self.assertIn("Inventory: Not checked", html)
        self.assertIn("Inventory sync:", html)

    def test_latest_inventory_activity_keeps_sales_copy_precise(self):
        self._set_inventory_enabled(True)
        self._create_inventory_run(products_checked=147, in_sync=147, blocked_items=0)

        company_row = self._company_row()

        self.assertEqual(company_row["latest_activity_label"], "Inventory audit")
        self.assertIn("Inventory audit", company_row["latest_activity_display"])
        self.assertEqual(company_row["sales_status"]["label"], "No successful sales sync recorded")
        self.assertEqual(company_row["latest_sales_sync_display"], "No successful sales sync recorded")
        self.assertEqual(company_row["status"], "unknown")

    def test_inventory_card_uses_operator_copy_for_mode_and_stats(self):
        run = self._create_inventory_run(products_checked=5, in_sync=2, blocked_items=0)
        artifact = RunArtifact.objects.get(run_job=run)
        stats = artifact.upload_stats_json
        stats.update(
            {
                "total_groups": 5,
                "status_counts": {
                    "in_sync": 2,
                    "needs_adjustment": 1,
                    "ambiguous_in_qbo": 1,
                    "missing_in_qbo": 1,
                },
                "apply": {"mode": "audit_only", "posted": 0, "skipped": 0},
            }
        )
        artifact.upload_stats_json = stats
        artifact.save(update_fields=["upload_stats_json"])
        company_data = views._enrich_company_data(
            self.company,
            run,
            preloaded={
                "latest_activity_job": run,
                "latest_inventory_job": run,
                "latest_inventory_artifact": artifact,
                "artifacts_today": [artifact],
                "token_info": {"severity": "healthy", "display_label": "Connected", "display_subtext": ""},
                "sales_reconcile_statuses_by_company_job": {},
            },
        )

        html = render_to_string("components/company_cards.html", {"companies_data": [company_data]})

        self.assertIn("Checked only", html)
        self.assertNotIn("audit_only", html)
        self.assertIn("Product groups", html)
        self.assertIn("Already in sync", html)
        self.assertIn("Need updates", html)
        self.assertIn("Multiple QBO matches", html)
        self.assertIn("Missing in QBO", html)
        self.assertNotIn("Needs adj.", html)
        self.assertNotIn("Ambiguous:", html)

    def test_latest_sales_artifact_receipt_copy_uses_sales_artifact(self):
        self._create_sales_run(with_artifact=True, reconcile_status="MATCH")
        artifact = RunArtifact.objects.get(kind=RunArtifact.KIND_SALES_UPLOAD)
        artifact.upload_stats_json = {"uploaded": 22, "skipped": 0, "failed": 0}
        artifact.target_date = (self.fixed_now - timedelta(days=1)).date()
        artifact.save(update_fields=["upload_stats_json", "target_date"])

        company_row = self._company_row()

        self.assertEqual(company_row["latest_sales_sync_display"], "22 receipts — Feb 12, 2026")

    def test_failed_inventory_run_shows_failed(self):
        self._create_inventory_run(
            status=RunJob.STATUS_FAILED,
            with_artifact=False,
            minutes_ago=4,
        )

        company_row = self._company_row()

        self.assertEqual(company_row["inventory_status"]["label"], "Failed")
        self.assertEqual(company_row["inventory_status"]["severity"], "critical")

    def test_clean_inventory_run_with_updates_stays_in_sync_with_update_detail(self):
        self._create_inventory_run(
            products_checked=147,
            in_sync=147,
            blocked_items=0,
            still_needs_review=0,
            updates=5,
            final_status_counts={"in_sync": 147},
        )

        company_row = self._company_row()

        self.assertEqual(company_row["inventory_status"]["label"], "In sync")
        self.assertEqual(company_row["inventory_status"]["updates_applied"], 5)
        self.assertEqual(company_row["inventory_status"]["subtext"], "5 updates applied")

    def test_latest_inventory_status_uses_audit_only_mode_label(self):
        self._create_inventory_run(
            products_checked=147,
            in_sync=147,
            blocked_items=0,
            inventory_stats_extra={"inventory_mode": "audit_only"},
        )

        company_row = self._company_row()

        self.assertEqual(company_row["inventory_status"]["label"], "Audit only")
        self.assertEqual(company_row["inventory_status"]["severity"], "healthy")

    def test_latest_inventory_status_uses_preview_mode_label(self):
        self._create_inventory_run(
            products_checked=147,
            in_sync=147,
            blocked_items=0,
            inventory_stats_extra={"inventory_mode": "quantity_preview"},
        )

        company_row = self._company_row()

        self.assertEqual(company_row["inventory_status"]["label"], "Preview only")

    def test_latest_inventory_status_uses_catalog_plan_label(self):
        self._create_inventory_run(
            products_checked=147,
            in_sync=147,
            blocked_items=0,
            inventory_stats_extra={
                "inventory_mode": "catalog_plan_only",
            },
        )

        company_row = self._company_row()

        self.assertEqual(company_row["inventory_status"]["label"], "Catalog plan only")

    def test_company_last_run_falls_back_to_latest_artifact_time(self):
        RunArtifact.objects.create(
            company_key=self.company.company_key,
            kind=RunArtifact.KIND_SALES_UPLOAD,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(minutes=10),
            source_path="/tmp/company_a_last_transform.json",
            source_hash="artifact-hash-company-a",
            rows_kept=42,
        )

        context = self._overview_context()

        company_row = next(item for item in context["companies"] if item["company_key"] == self.company.company_key)
        self.assertIsNotNone(company_row["last_run"])

    def test_system_health_severity_classification(self):
        self.assertEqual(views._classify_system_health(2, 0, 0)["label"], "All Operational")
        self.assertEqual(views._classify_system_health(1, 1, 0)["label"], "Warning")
        self.assertEqual(views._classify_system_health(1, 0, 1)["label"], "Degraded")

    def test_system_health_breakdown_compacts_zero_buckets(self):
        self.assertEqual(
            views._format_system_health_breakdown(2, 0, 0, 0),
            "2 healthy",
        )
        self.assertEqual(
            views._format_system_health_breakdown(1, 0, 1, 0),
            "1 healthy • 1 critical",
        )
        self.assertEqual(
            views._format_system_health_breakdown(1, 1, 0, 2),
            "1 healthy • 1 warning • 2 unknown",
        )

    def test_company_summary_visibility_rules(self):
        self.assertFalse(views._should_show_company_summary("healthy", "Last run succeeded.", []))
        self.assertFalse(views._should_show_company_summary("unknown", "No successful sales sync recorded.", []))
        self.assertFalse(
            views._should_show_company_summary(
                "warning",
                "Reconciliation mismatch above threshold.",
                ["Reconciliation mismatch above threshold."],
            )
        )
        self.assertTrue(views._should_show_company_summary("critical", "Latest run failed.", ["Latest run failed"]))

    def test_overview_sales_24h_uses_reconcile_first_and_computes_trend(self):
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        # Prev target date (Feb 11): total 100
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=2)).date(),
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_24h.json",
            source_hash="hash-prev-24h",
            upload_stats_json={"total_amount": 999},
            reconcile_epos_total=100.0,
        )
        current_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        # This target date (Feb 12 / yesterday): total 200
        RunArtifact.objects.create(
            run_job=current_run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_curr_24h.json",
            source_hash="hash-curr-24h",
            upload_stats_json={"total_amount": 500},
            reconcile_epos_total=200.0,
        )

        context = self._overview_context()

        kpis = context["kpis"]
        self.assertEqual(kpis["sales_24h_total"], Decimal("200.0000"))
        self.assertEqual(kpis["sales_prev_24h_total"], Decimal("100.0000"))
        self.assertEqual(kpis["sales_24h_trend_dir"], "up")
        self.assertEqual(kpis["sales_24h_trend_text"], "↑ 100.0% increase vs Feb 11")

    def test_overview_context_includes_avg_runtime_24h(self):
        completed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=30),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=completed.id).update(created_at=self.fixed_now - timedelta(hours=1))
        RunArtifact.objects.create(
            run_job=completed,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(minutes=5),
            source_path="/tmp/company_a_runtime.json",
            source_hash="hash-runtime",
            reconcile_epos_total=100.0,
            upload_stats_json={"uploaded": 3, "skipped": 0, "failed": 0},
        )
        context = self._overview_context()
        self.assertGreaterEqual(context["kpis"]["avg_runtime_24h_seconds"], 0)
        display = context["kpis"]["avg_runtime_24h_display"]
        self.assertTrue(any(unit in display for unit in ("s", "m", "h", "d")))
        self.assertIn("vs Feb 11", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_sales_24h_shows_no_monetary_totals_when_artifacts_have_no_amount(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run,
            company_key=self.company.company_key,
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/company_a_no_amount.json",
            source_hash="hash-no-amount",
            upload_stats_json={"uploaded": 5},
        )
        context = self._overview_context()
        self.assertEqual(context["kpis"]["sales_24h_trend_text"], "No monetary totals found")

    def test_overview_context_shows_no_data_basis_line_without_successful_run_artifacts(self):
        context = self._overview_context()
        self.assertFalse(context["overview_has_data"])
        self.assertEqual(context["metric_basis_line"], "No successful run data yet.")

    def test_overview_avg_runtime_today_uses_faster_slower_wording(self):
        yesterday_date = (self.fixed_now - timedelta(days=1)).date()
        prev_date = (self.fixed_now - timedelta(days=2)).date()
        y_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(days=1, minutes=40),
            finished_at=self.fixed_now - timedelta(days=1, minutes=10),
        )
        RunJob.objects.filter(id=y_run.id).update(created_at=self.fixed_now - timedelta(days=1, minutes=41))
        RunArtifact.objects.create(
            run_job=y_run,
            company_key=self.company.company_key,
            target_date=prev_date,
            processed_at=self.fixed_now - timedelta(days=1, minutes=5),
            source_path="/tmp/y_run.json",
            source_hash="hash-y",
            reconcile_epos_total=50.0,
            upload_stats_json={"uploaded": 4, "skipped": 0, "failed": 0},
        )
        t_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=20),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=t_run.id).update(created_at=self.fixed_now - timedelta(minutes=21))
        RunArtifact.objects.create(
            run_job=t_run,
            company_key=self.company.company_key,
            target_date=yesterday_date,
            processed_at=self.fixed_now - timedelta(minutes=5),
            source_path="/tmp/t_run.json",
            source_hash="hash-t",
            reconcile_epos_total=50.0,
            upload_stats_json={"uploaded": 4, "skipped": 0, "failed": 0},
        )

        context = self._overview_context()
        self.assertIn("faster vs Feb 11", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_run_success_uses_target_date_artifact_linkage(self):
        """Run Success counts completed runs linked to artifacts for target trading date."""
        target_date = (self.fixed_now - timedelta(days=1)).date()
        other_date = (self.fixed_now - timedelta(days=2)).date()

        run_target_success = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            finished_at=self.fixed_now - timedelta(hours=4),
        )
        RunArtifact.objects.create(
            run_job=run_target_success,
            company_key=self.company.company_key,
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/target-success.json",
            source_hash="hash-target-success",
        )
        run_target_failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            finished_at=self.fixed_now - timedelta(hours=2),
        )
        RunArtifact.objects.create(
            run_job=run_target_failed,
            company_key=self.company.company_key,
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=1),
            source_path="/tmp/target-failed.json",
            source_hash="hash-target-failed",
        )

        run_other_date = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            finished_at=self.fixed_now - timedelta(hours=8),
        )
        RunArtifact.objects.create(
            run_job=run_other_date,
            company_key=self.company.company_key,
            target_date=other_date,
            processed_at=self.fixed_now - timedelta(hours=7),
            source_path="/tmp/other-date.json",
            source_hash="hash-other-date",
        )

        context = self._overview_context()
        # Only runs linked to artifacts for the target trading date should count.
        self.assertEqual(context["kpis"]["total_completed_runs_24h"], 2)
        self.assertEqual(context["kpis"]["successful_runs_24h"], 1)
        self.assertEqual(context["kpis"]["run_success_pct_24h"], 50.0)
        self.assertEqual(context["kpis"]["run_success_ratio_24h"], "1/2")

    def test_overview_avg_runtime_today_uses_successful_runs_only(self):
        yesterday_date = (self.fixed_now - timedelta(days=1)).date()
        succeeded = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=20),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=succeeded.id).update(created_at=self.fixed_now - timedelta(minutes=21))
        RunArtifact.objects.create(
            run_job=succeeded,
            company_key=self.company.company_key,
            target_date=yesterday_date,
            processed_at=self.fixed_now - timedelta(minutes=5),
            source_path="/tmp/succeeded.json",
            source_hash="hash-succeeded",
            reconcile_epos_total=100.0,
            upload_stats_json={"uploaded": 5, "skipped": 0, "failed": 0},
        )
        failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_FAILED,
            started_at=self.fixed_now - timedelta(minutes=90),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunJob.objects.filter(id=failed.id).update(created_at=self.fixed_now - timedelta(minutes=91))

        context = self._overview_context()
        # 10 minutes from the succeeded run only (target-date logic).
        self.assertEqual(context["kpis"]["avg_runtime_today_seconds"], 600)

    def test_overview_avg_runtime_includes_successful_runs_with_zero_uploads(self):
        yesterday_date = (self.fixed_now - timedelta(days=1)).date()
        prev_date = (self.fixed_now - timedelta(days=2)).date()

        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(days=1, minutes=4),
            finished_at=self.fixed_now - timedelta(days=1, minutes=2),
        )
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=prev_date,
            processed_at=self.fixed_now - timedelta(days=1, minutes=1),
            source_path="/tmp/prev_zero_uploads.json",
            source_hash="hash-prev-zero-uploads",
            upload_stats_json={"uploaded": 0, "skipped": 2, "failed": 0},
        )

        today_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=2),
            finished_at=self.fixed_now - timedelta(minutes=1),
        )
        RunArtifact.objects.create(
            run_job=today_run,
            company_key=self.company.company_key,
            target_date=yesterday_date,
            processed_at=self.fixed_now - timedelta(seconds=30),
            source_path="/tmp/today_zero_uploads.json",
            source_hash="hash-today-zero-uploads",
            upload_stats_json={"uploaded": 0, "skipped": 3, "failed": 0},
        )

        context = self._overview_context()

        self.assertEqual(context["kpis"]["avg_runtime_today_seconds"], 60)
        self.assertIn("50.0% faster vs Feb 11", context["kpis"]["avg_runtime_today_trend_text"])

    def test_overview_sales_24h_uses_decrease_wording_for_negative_delta(self):
        prev_date = (self.fixed_now - timedelta(days=2)).date()
        this_date = (self.fixed_now - timedelta(days=1)).date()
        prev_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=prev_run,
            company_key=self.company.company_key,
            target_date=prev_date,
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_drop.json",
            source_hash="hash-prev-drop",
            reconcile_epos_total=200.0,
        )
        current_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company.company_key,
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=current_run,
            company_key=self.company.company_key,
            target_date=this_date,
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_curr_drop.json",
            source_hash="hash-curr-drop",
            reconcile_epos_total=100.0,
        )
        context = self._overview_context()
        self.assertEqual(context["kpis"]["sales_24h_trend_text"], "↓ 50.0% decrease vs Feb 11")

    def test_overview_sales_today_uses_latest_successful_artifact_per_company(self):
        CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "987654321"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )
        # Target date Feb 12 (yesterday): company_a 3,995,250, company_b 9,505,350.
        # Target date Feb 11 (prev): company_a 2,645,250, company_b 9,374,050.
        run_a_old = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_a_old,
            company_key="company_a",
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=10),
            source_path="/tmp/company_a_old.json",
            source_hash="company-a-old",
            reconcile_epos_total=3995250.0,
        )
        run_a_new = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_a_new,
            company_key="company_a",
            target_date=(self.fixed_now - timedelta(days=2)).date(),
            processed_at=self.fixed_now - timedelta(hours=6),
            source_path="/tmp/company_a_new.json",
            source_hash="company-a-new",
            reconcile_epos_total=2645250.0,
        )
        run_b_old = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_b_old,
            company_key="company_b",
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=9),
            source_path="/tmp/company_b_old.json",
            source_hash="company-b-old",
            reconcile_epos_total=9505350.0,
        )
        run_b_new = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_b_new,
            company_key="company_b",
            target_date=(self.fixed_now - timedelta(days=2)).date(),
            processed_at=self.fixed_now - timedelta(hours=5),
            source_path="/tmp/company_b_new.json",
            source_hash="company-b-new",
            reconcile_epos_total=9374050.0,
        )

        context = self._overview_context()

        # Resolver picks the latest succeeded artifact by processed_at; here that's Feb 11.
        self.assertEqual(context["target_date_iso"], (self.fixed_now - timedelta(days=2)).date().isoformat())
        # By target date: Feb 11 total = 2,645,250 + 9,374,050 = 12,019,300.
        self.assertEqual(context["kpis"]["sales_24h_total"], Decimal("12019300.0000"))
        self.assertEqual(context["kpis"]["sales_prev_24h_total"], Decimal("0.0000"))

    def test_overview_context_company_filter_changes_kpis_but_keeps_revenue_chart_scope(self):
        CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "987654321"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )
        target_date = (self.fixed_now - timedelta(days=1)).date()
        prev_target_date = (self.fixed_now - timedelta(days=2)).date()

        run_a_prev = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_a_prev,
            company_key="company_a",
            target_date=prev_target_date,
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_filter.json",
            source_hash="company-a-prev-filter",
            reconcile_epos_total=100.0,
            upload_stats_json={"uploaded": 2, "skipped": 0, "failed": 0},
        )
        run_a_current = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=15),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunArtifact.objects.create(
            run_job=run_a_current,
            company_key="company_a",
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=2),
            source_path="/tmp/company_a_current_filter.json",
            source_hash="company-a-current-filter",
            reconcile_epos_total=150.0,
            upload_stats_json={"uploaded": 2, "skipped": 0, "failed": 0},
        )

        run_b_prev = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_b_prev,
            company_key="company_b",
            target_date=prev_target_date,
            processed_at=self.fixed_now - timedelta(hours=29),
            source_path="/tmp/company_b_prev_filter.json",
            source_hash="company-b-prev-filter",
            reconcile_epos_total=200.0,
            upload_stats_json={"uploaded": 2, "skipped": 0, "failed": 0},
        )
        run_b_current = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=25),
            finished_at=self.fixed_now - timedelta(minutes=20),
        )
        RunArtifact.objects.create(
            run_job=run_b_current,
            company_key="company_b",
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=3),
            source_path="/tmp/company_b_current_filter.json",
            source_hash="company-b-current-filter",
            reconcile_epos_total=260.0,
            upload_stats_json={"uploaded": 2, "skipped": 0, "failed": 0},
        )
        run_b_failed = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_b",
            status=RunJob.STATUS_FAILED,
        )
        RunArtifact.objects.create(
            run_job=run_b_failed,
            company_key="company_b",
            target_date=target_date,
            processed_at=self.fixed_now - timedelta(hours=1),
            source_path="/tmp/company_b_failed_filter.json",
            source_hash="company-b-failed-filter",
            reconcile_epos_total=999.0,
            upload_stats_json={"uploaded": 1, "skipped": 0, "failed": 0},
        )

        all_context = self._overview_context()
        company_context = self._overview_context(company_key="company_a")

        self.assertEqual(all_context["kpis"]["run_success_ratio_24h"], "2/3")
        self.assertEqual(company_context["kpis"]["run_success_ratio_24h"], "1/1")
        self.assertEqual(company_context["kpis"]["sales_24h_total"], Decimal("150.0000"))
        self.assertEqual(
            {series["company_key"] for series in company_context["revenue_series"]},
            {"company_a", "company_b"},
        )
        self.assertEqual(
            {item["company_key"] for item in company_context["revenue_company_options"]},
            {"company_a", "company_b"},
        )


class OverviewUITemplateTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 13, 12, 0, 0))
        self.user = User.objects.create_user(username="operator", password="pw12345")
        CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123456789"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "inventory": {"enable_inventory_items": True},
            },
        )
        self.client.login(username="operator", password="pw12345")

    def _token_payload(self) -> dict:
        now_ts = int(self.fixed_now.timestamp())
        return {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "expires_at": now_ts + 3600,
            "refresh_expires_at": now_ts + (30 * 86400),
            "updated_at": now_ts,
            "environment": "production",
        }

    def test_overview_renders_search_and_overview_script(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('id="overview-company-filter"', html)
        self.assertIn(f'data-panels-url="{reverse("epos_qbo:overview-panels")}"', html)
        self.assertIn("js/overview.js", html)

    def test_overview_card_renders_separate_sales_inventory_and_token_statuses(self):
        sales_run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=30),
            finished_at=self.fixed_now - timedelta(minutes=20),
        )
        inventory_run = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            started_at=self.fixed_now - timedelta(minutes=12),
            finished_at=self.fixed_now - timedelta(minutes=10),
        )
        RunArtifact.objects.create(
            run_job=inventory_run,
            company_key="company_a",
            kind=RunArtifact.KIND_INVENTORY_AUDIT,
            processed_at=self.fixed_now - timedelta(minutes=10),
            source_path="/tmp/inventory_pipeline_company_a_summary.json",
            source_hash="overview-inventory-summary",
            rows_total=147,
            rows_kept=147,
            rows_non_target=0,
            upload_stats_json={
                "report_type": "inventory_pipeline",
                "products_checked": 147,
                "in_sync": 147,
                "blocked_items": 0,
                "still_needs_review": 0,
                "catalog_fixes_applied": 0,
                "base_items_created": 0,
                "duplicate_base_items_resolved": 0,
                "quantity_updates_applied": 0,
                "final_status_counts": {"in_sync": 147},
            },
        )
        self.assertIsNotNone(sales_run)

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Sales: Not reconciled", html)
        self.assertIn("Inventory: In sync", html)
        self.assertIn("Token: Connected", html)
        self.assertIn("Products checked: 147", html)
        self.assertIn("Blocked: 0", html)
        self.assertIn("Access token expires in", html)
        self.assertNotIn("inventory_pipeline", html)
        self.assertNotIn("/tmp/inventory_pipeline_company_a_summary.json", html)

    def test_overview_does_not_render_run_reliability_panel(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertNotIn("Run Reliability", html)
        self.assertNotIn("Failure Sources (Last 60 Days)", html)

    def test_live_log_uses_company_and_run_label_not_uuid(self):
        run = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunJob.objects.filter(id=run.id).update(created_at=self.fixed_now - timedelta(minutes=10))
        run.refresh_from_db()

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        html = response.content.decode("utf-8")
        self.assertIn(f"Company A: Run {run.friendly_id} succeeded", html)
        self.assertNotIn(str(run.id), html)

    def test_overview_panels_endpoint_renders_fragment(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview-panels"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("System Health", html)
        self.assertIn('id="overview-company-filter"', html)
        self.assertIn("Live Log", html)
        self.assertNotIn("Run Reliability", html)

    def test_overview_panels_respects_revenue_period_param(self):
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview-panels"), {"revenue_period": "90d"})

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('<option value="90d" selected>', html)

    def test_overview_panels_company_filter_keeps_revenue_company_options(self):
        CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="Company B",
            config_json={
                "company_key": "company_b",
                "display_name": "Company B",
                "qbo": {"realm_id": "987654321"},
                "epos": {"username_env_key": "EPOS_USERNAME_B", "password_env_key": "EPOS_PASSWORD_B"},
            },
        )
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview-panels"), {"company": "company_a"})

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('value="company_a">Company A</option>', html)
        self.assertIn('value="company_b">Company B</option>', html)

    def test_overview_topbar_uses_quick_sync_label(self):
        perm = Permission.objects.get(codename="can_trigger_runs")
        self.user.user_permissions.add(perm)
        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("Quick Sync", html)
        self.assertNotIn("Manual Sync", html)
        self.assertIn('name="date_mode" value="target_date"', html)
        self.assertIn('name="target_date"', html)

    def test_overview_renders_consolidated_kpi_row(self):
        run_prev = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_prev,
            company_key="company_a",
            target_date=(self.fixed_now - timedelta(days=1)).date(),
            processed_at=self.fixed_now - timedelta(hours=30),
            source_path="/tmp/company_a_prev_kpi.json",
            source_hash="hash-company-a-prev-kpi",
            reconcile_epos_total=100.0,
        )
        run_now = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
        )
        RunArtifact.objects.create(
            run_job=run_now,
            company_key="company_a",
            target_date=self.fixed_now.date(),
            processed_at=self.fixed_now - timedelta(hours=4),
            source_path="/tmp/company_a_now_kpi.json",
            source_hash="hash-company-a-now-kpi",
            reconcile_epos_total=140.0,
        )

        with (
            mock.patch("apps.epos_qbo.business_date.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.timezone.now", return_value=self.fixed_now),
            mock.patch("apps.epos_qbo.views.load_tokens", return_value=self._token_payload()),
        ):
            response = self.client.get(reverse("epos_qbo:overview"))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn("System Health", html)
        self.assertIn("Sales Synced", html)
        self.assertIn("Run Success", html)
        self.assertIn("Avg Runtime", html)
        self.assertIn("Metrics are based on Target Date:", html)
        self.assertIn("Last successful sync", html)
        self.assertNotIn("KPI basis: trading day cutoff", html)
        self.assertNotIn("Healthy Companies", html)
        self.assertNotIn("Critical Errors", html)
        self.assertNotIn("Records Synced (24h)", html)
        self.assertNotIn("Active Runs", html)
