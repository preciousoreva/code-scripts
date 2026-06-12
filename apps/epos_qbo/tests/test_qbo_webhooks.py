from __future__ import annotations

import base64
import hashlib
import hmac
import json
from unittest import mock

from django.test import Client, TestCase
from django.urls import reverse

from apps.epos_qbo.models import CompanyConfigRecord


def _signature(body: bytes, token: str) -> str:
    digest = hmac.new(token.encode("utf-8"), body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


class QuickBooksWebhookViewTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.url = reverse("epos_qbo:quickbooks-webhook")
        self.token = "test-verifier-token"

    def _post(self, payload: dict, *, token: str | None = None):
        body = json.dumps(payload).encode("utf-8")
        verifier = token if token is not None else self.token
        return self.client.post(
            self.url,
            data=body,
            content_type="application/json",
            HTTP_INTUIT_SIGNATURE=_signature(body, verifier),
        )

    @mock.patch.dict("os.environ", {"QBO_WEBHOOK_VERIFIER_TOKEN": "test-verifier-token"})
    @mock.patch("apps.epos_qbo.webhooks.process_qbo_webhook_payload", return_value=2)
    def test_valid_signature_processes_payload_without_login(self, process_payload):
        response = self._post({"eventNotifications": []})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["notifications_sent"], 2)
        process_payload.assert_called_once_with({"eventNotifications": []})

    @mock.patch.dict("os.environ", {"QBO_WEBHOOK_VERIFIER_TOKEN": "test-verifier-token"})
    @mock.patch("apps.epos_qbo.webhooks.process_qbo_webhook_payload")
    def test_invalid_signature_rejected(self, process_payload):
        response = self._post({"eventNotifications": []}, token="wrong-token")
        self.assertEqual(response.status_code, 401)
        process_payload.assert_not_called()

    @mock.patch.dict("os.environ", {}, clear=True)
    def test_missing_verifier_token_is_service_unavailable(self):
        response = self.client.post(self.url, data=b"{}", content_type="application/json")
        self.assertEqual(response.status_code, 503)


class QuickBooksWebhookNotificationTests(TestCase):
    def setUp(self):
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Oreva Innovations & Tech",
            config_json={
                "company_key": "company_a",
                "display_name": "Oreva Innovations & Tech",
                "qbo": {"realm_id": "9341455406194328", "environment": "production"},
            },
        )
        self.payload = {
            "eventNotifications": [
                {
                    "realmId": "9341455406194328",
                    "dataChangeEvent": {
                        "entities": [
                            {
                                "name": "Item",
                                "id": "14895",
                                "operation": "Create",
                                "lastUpdated": "2026-06-11T18:55:00.000Z",
                            }
                        ]
                    },
                }
            ]
        }

    @mock.patch("apps.epos_qbo.services.qbo_webhook_notifications.requests.get")
    @mock.patch("apps.epos_qbo.services.qbo_webhook_notifications.get_access_token", return_value="access")
    @mock.patch("apps.epos_qbo.services.qbo_webhook_notifications.send_slack_success")
    @mock.patch.dict(
        "os.environ",
        {"QBO_WEBHOOK_SLACK_URL_COMPANY_A": "https://hooks.slack.test/qbo-company-a"},
    )
    def test_item_event_sends_enriched_slack(self, send_slack, _get_token, requests_get):
        requests_get.return_value = mock.Mock(
            status_code=200,
            json=lambda: {
                "Item": {
                    "Name": "Coke 50CL",
                    "Type": "Inventory",
                    "Active": True,
                    "TrackQtyOnHand": True,
                    "FullyQualifiedName": "Drinks:Coke 50CL",
                    "IncomeAccountRef": {"name": "Product Sales", "value": "91"},
                    "AssetAccountRef": {"name": "Inventory Asset", "value": "92"},
                }
            },
        )

        from apps.epos_qbo.services.qbo_webhook_notifications import process_qbo_webhook_payload

        sent_count = process_qbo_webhook_payload(self.payload)

        self.assertEqual(sent_count, 1)
        send_slack.assert_called_once()
        message, webhook = send_slack.call_args.args
        self.assertEqual(webhook, "https://hooks.slack.test/qbo-company-a")
        self.assertIn("QuickBooks Item Create", message)
        self.assertIn("Oreva Innovations & Tech", message)
        self.assertIn("Coke 50CL", message)
        self.assertIn("Inventory", message)
        self.assertIn("Product Sales", message)

    @mock.patch("apps.epos_qbo.services.qbo_webhook_notifications.send_slack_success")
    @mock.patch.dict("os.environ", {"QBO_WEBHOOK_SLACK_URL": "https://hooks.slack.test/qbo-default"})
    def test_non_item_event_sends_generic_slack(self, send_slack):
        payload = {
            "eventNotifications": [
                {
                    "realmId": "9341455406194328",
                    "dataChangeEvent": {
                        "entities": [
                            {
                                "name": "Customer",
                                "id": "42",
                                "operation": "Update",
                                "lastUpdated": "2026-06-11T19:00:00.000Z",
                            }
                        ]
                    },
                }
            ]
        }

        from apps.epos_qbo.services.qbo_webhook_notifications import process_qbo_webhook_payload

        sent_count = process_qbo_webhook_payload(payload)

        self.assertEqual(sent_count, 1)
        message = send_slack.call_args.args[0]
        self.assertIn("QuickBooks Customer Update", message)
        self.assertIn("Entity ID: `42`", message)

    @mock.patch("apps.epos_qbo.services.qbo_webhook_notifications.send_slack_success")
    @mock.patch.dict("os.environ", {"QBO_WEBHOOK_SLACK_URL": "https://hooks.slack.test/qbo-default"})
    def test_unknown_realm_sends_no_slack(self, send_slack):
        payload = {
            "eventNotifications": [
                {
                    "realmId": "unknown",
                    "dataChangeEvent": {"entities": [{"name": "Item", "id": "1", "operation": "Create"}]},
                }
            ]
        }

        from apps.epos_qbo.services.qbo_webhook_notifications import process_qbo_webhook_payload

        self.assertEqual(process_qbo_webhook_payload(payload), 0)
        send_slack.assert_not_called()

    @mock.patch("apps.epos_qbo.services.qbo_webhook_notifications.send_slack_success")
    @mock.patch.dict("os.environ", {"SLACK_WEBHOOK_URL_A": "https://hooks.slack.test/pipeline"}, clear=True)
    def test_pipeline_slack_env_is_not_used_for_qbo_webhooks(self, send_slack):
        from apps.epos_qbo.services.qbo_webhook_notifications import process_qbo_webhook_payload

        self.assertEqual(process_qbo_webhook_payload(self.payload), 0)
        send_slack.assert_not_called()
