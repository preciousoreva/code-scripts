"""Slack helpers for manual Inventory Review pipeline jobs."""

from __future__ import annotations

from unittest import mock

from django.contrib.auth.models import User
from django.test import RequestFactory, TestCase, override_settings

from apps.epos_qbo.models import CompanyConfigRecord, RunJob
from apps.epos_qbo.services.inventory_review_slack import (
    send_inventory_review_action_failed_notification,
    send_inventory_review_action_queued,
)


class InventoryReviewFailureSlackTests(TestCase):
    @mock.patch("apps.epos_qbo.services.inventory_review_slack.send_slack_success")
    @mock.patch("apps.epos_qbo.services.inventory_review_slack.load_company_config")
    def test_failed_review_retry_sends_slack(self, load_cfg, send_slack):
        cfg = mock.Mock()
        cfg.display_name = "Company X"
        cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
        load_cfg.return_value = cfg
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            inventory_options_json={
                "review_retry": {
                    "intent": "review_retry_quantity_adjustments",
                    "source_final_audit": "/data/inventory_audit_company_a_final_191504.csv",
                    "row_count": 8,
                    "affected_base_names": ["A"],
                },
                "base_names": ["A"],
            },
            exit_code=2,
            failure_reason="catalog cleanup failed",
        )
        with override_settings(OIAT_PORTAL_BASE_URL="https://portal.example.com"):
            send_inventory_review_action_failed_notification(job)
        send_slack.assert_called_once()
        msg = send_slack.call_args.args[0]
        webhook_arg = send_slack.call_args.args[1]
        self.assertEqual(webhook_arg, cfg.slack_webhook_url)
        self.assertIn("Inventory Review Action Failed", msg)
        self.assertIn("Quantity adjustment retry", msg)
        self.assertIn("inventory_audit_company_a_final_191504.csv", msg)
        self.assertIn("epos-qbo/runs/", msg)

    @mock.patch("apps.epos_qbo.services.inventory_review_slack.send_slack_success")
    def test_failure_skipped_without_review_envelope(self, send_slack):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            inventory_options_json={"categories": ["X"]},
            exit_code=1,
        )
        send_inventory_review_action_failed_notification(job)
        send_slack.assert_not_called()

    @mock.patch("apps.epos_qbo.services.inventory_review_slack.send_slack_success")
    def test_failure_skipped_for_non_inventory_scope(self, send_slack):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            inventory_options_json={
                "review_retry": {
                    "intent": "review_retry_catalog_cleanup",
                    "row_count": 1,
                }
            },
            exit_code=1,
        )
        send_inventory_review_action_failed_notification(job)
        send_slack.assert_not_called()


class InventoryReviewQueuedSlackTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="oiat_admin", password="pw")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="AKPONORA VENTURES LTD.",
            config_json={"company_key": "company_a", "display_name": "AKPONORA VENTURES LTD."},
        )

    @mock.patch("apps.epos_qbo.services.inventory_review_slack.send_slack_success")
    @mock.patch("apps.epos_qbo.services.inventory_review_slack.load_company_config")
    def test_queued_message_includes_action_and_user(self, load_cfg, send_slack):
        cfg = mock.Mock()
        cfg.slack_webhook_url = "https://hooks.slack.com/services/FAKE"
        load_cfg.return_value = cfg
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            inventory_options_json={
                "review_retry": {
                    "intent": "review_retry_catalog_cleanup",
                    "source_final_audit": "/tmp/inventory_audit_company_a_final_191504.csv",
                    "row_count": 2,
                },
            },
            status=RunJob.STATUS_QUEUED,
        )
        request = RequestFactory().get("/")
        request.user = self.user
        request.build_absolute_uri = lambda loc: f"https://portal.example{loc}"

        with mock.patch(
            "apps.epos_qbo.services.inventory_review_slack.reverse",
            return_value=f"/epos-qbo/runs/{job.id}/",
        ):
            send_inventory_review_action_queued(company=self.company, job=job, request=request)

        send_slack.assert_called_once()
        msg = send_slack.call_args.args[0]
        self.assertIn("Inventory Review Action Queued", msg)
        self.assertIn("Catalog cleanup retry", msg)
        self.assertIn("AKPONORA", msg)
        self.assertIn("oiat_admin", msg)
        self.assertIn("inventory_audit_company_a_final_191504.csv", msg)

    @mock.patch("apps.epos_qbo.services.inventory_review_slack.send_slack_success")
    @mock.patch("apps.epos_qbo.services.inventory_review_slack.load_company_config")
    def test_queued_skips_without_webhook(self, load_cfg, send_slack):
        load_cfg.return_value = mock.Mock(slack_webhook_url=None)
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key="company_a",
            inventory_options_json={
                "review_retry": {
                    "intent": "review_retry_catalog_cleanup",
                    "row_count": 1,
                }
            },
        )
        request = mock.Mock()
        request.user = self.user
        send_inventory_review_action_queued(company=self.company, job=job, request=request)
        send_slack.assert_not_called()
