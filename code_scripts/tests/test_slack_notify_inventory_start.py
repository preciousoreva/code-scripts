from __future__ import annotations

import os
import unittest
from unittest import mock

from code_scripts import slack_notify


class InventoryPipelineSlackStartTests(unittest.TestCase):
    def test_notify_inventory_pipeline_start_sends_compact_message(self):
        with mock.patch.object(slack_notify, "send_slack_success") as send_mock, mock.patch.dict(
            os.environ,
            {
                "OIAT_RUN_JOB_ID": "e8333646-3066-4953-9627-b0b4b1526f86",
                "OIAT_PORTAL_BASE_URL": "https://portal.example",
                "OIAT_RUN_STARTED_AT": "2026-04-30T15:00:00+00:00",
            },
            clear=False,
        ):
            slack_notify.notify_inventory_pipeline_start(
                company_name="AKPONORA VENTURES LTD.",
                company_key="company_a",
                categories=["ALCOHOLS & SPIRITS"],
                product_filter="",
                dry_run=True,
                webhook_url="https://hooks.slack.invalid/test",
                metadata={"run_job_id": os.environ["OIAT_RUN_JOB_ID"]},
            )

        self.assertEqual(send_mock.call_count, 1)
        message = send_mock.call_args.args[0]
        self.assertIn("Inventory Sync started", message)
        self.assertIn("AKPONORA VENTURES LTD.", message)
        self.assertIn("(`company_a`)", message)
        self.assertIn("Category: ALCOHOLS & SPIRITS", message)
        self.assertIn("Mode: Preview only", message)
        self.assertIn("<https://portal.example/epos-qbo/runs/", message)
        self.assertIn("|Inventory Run INV-", message)

    def test_notify_inventory_pipeline_start_is_noop_without_webhook(self):
        with mock.patch.object(slack_notify, "send_slack_success") as send_mock:
            slack_notify.notify_inventory_pipeline_start(
                company_name="Company A",
                company_key="company_a",
                categories=[],
                product_filter=None,
                dry_run=False,
                webhook_url=None,
            )
        send_mock.assert_not_called()

