from __future__ import annotations

import json
from datetime import datetime, timezone as dt_timezone

from django.contrib.auth.models import User
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from apps.websites.models import Website, WebsiteLogEvent


class WebsiteViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.website, _ = Website.objects.update_or_create(
            slug="working-people-united",
            defaults={
                "name": "Working People United",
                "domain": "workingpeopleunited.org",
                "platform": Website.PLATFORM_WIX,
                "public_url": "https://workingpeopleunited.org",
                "log_ingest_secret": "test-secret",
            },
        )

    def test_index_requires_login(self):
        response = self.client.get(reverse("websites:index"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_index_renders_website_card(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("websites:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Working People United")
        self.assertContains(response, "workingpeopleunited.org")

    @override_settings(WEBSITE_LOGS_PUBLIC_BASE_URL="https://website-logs.oiatsolutions.com")
    def test_detail_shows_public_wix_endpoint(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("websites:detail", kwargs={"site_slug": self.website.slug}))
        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            "https://website-logs.oiatsolutions.com/websites/webhooks/wix/working-people-united/test-secret/",
        )

    def test_logs_filter_by_search(self):
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_ERROR,
            message="Backend function failed",
            request_id="req-123",
        )
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="Page loaded",
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(
            reverse("websites:logs", kwargs={"site_slug": self.website.slug}),
            {"q": "req-123"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Backend function failed")
        self.assertNotContains(response, "Page loaded")

    def test_logs_filter_by_extracted_context(self):
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="Registration downstream outcome",
            context={"membershipId": "WOPU-KAN-GPU4X84C", "emailSent": True},
            context_text="membershipId:WOPU-KAN-GPU4X84C emailSent:true",
        )
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="Page loaded",
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(
            reverse("websites:logs", kwargs={"site_slug": self.website.slug}),
            {"q": "GPU4X84C"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Registration downstream outcome")
        self.assertNotContains(response, "Page loaded")

    def test_logs_filter_by_trace_and_date_range(self):
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_ERROR,
            message="Target trace",
            request_id="trace-abc",
            received_at=datetime(2026, 7, 8, 12, 0, tzinfo=dt_timezone.utc),
        )
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_ERROR,
            message="Wrong trace",
            request_id="trace-other",
            received_at=datetime(2026, 7, 8, 12, 0, tzinfo=dt_timezone.utc),
        )
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_ERROR,
            message="Wrong day",
            request_id="trace-abc",
            received_at=datetime(2026, 7, 6, 12, 0, tzinfo=dt_timezone.utc),
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(
            reverse("websites:logs", kwargs={"site_slug": self.website.slug}),
            {"trace": "trace-abc", "date_from": "2026-07-08", "date_to": "2026-07-08"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Target trace")
        self.assertNotContains(response, "Wrong trace")
        self.assertNotContains(response, "Wrong day")

    def test_logs_filter_by_dynamic_source(self):
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="Registration page loaded",
            source="pages/Registration.iebmg.js",
        )
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="Membership card backend",
            source="backend/membership-card.web.js",
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(
            reverse("websites:logs", kwargs={"site_slug": self.website.slug}),
            {"source": "backend/membership-card.web.js"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "backend/membership-card.web.js")
        self.assertContains(response, "pages/Registration.iebmg.js")
        self.assertContains(response, "Membership card backend")
        self.assertNotContains(response, "Registration page loaded")

    def test_logs_api_returns_filtered_json(self):
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_ERROR,
            message="API target",
            trace_id="trace-json",
            pathname="/join",
            received_at=timezone.now(),
        )
        WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="API other",
            trace_id="other-json",
            received_at=timezone.now(),
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(
            reverse("websites:logs-api", kwargs={"site_slug": self.website.slug}),
            {"trace": "trace-json"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["total"], 1)
        self.assertEqual(data["logs"][0]["message"], "API target")
        self.assertEqual(data["logs"][0]["trace_display"], "trace-json")
        self.assertEqual(data["logs"][0]["pathname"], "/join")

    def test_log_detail_api_returns_raw_payload(self):
        event = WebsiteLogEvent.objects.create(
            website=self.website,
            severity=WebsiteLogEvent.SEVERITY_INFO,
            message="Raw payload",
            context={"membershipId": "WOPU-KAN-GPU4X84C"},
            context_text="membershipId:WOPU-KAN-GPU4X84C",
            raw_payload={"jsonPayload": {"message": "Raw payload"}, "severity": "INFO"},
            request_headers={"HTTP_USER_AGENT": "Wix Logs"},
        )
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(
            reverse(
                "websites:log-detail-api",
                kwargs={"site_slug": self.website.slug, "log_id": event.id},
            )
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["log"]["message"], "Raw payload")
        self.assertEqual(data["log"]["context"]["membershipId"], "WOPU-KAN-GPU4X84C")
        self.assertIn("membershipId=WOPU-KAN-GPU4X84C", data["log"]["context_summary"])
        self.assertEqual(data["raw_payload"]["jsonPayload"]["message"], "Raw payload")
        self.assertEqual(data["request_headers"]["HTTP_USER_AGENT"], "Wix Logs")

    def test_logs_api_requires_login(self):
        response = self.client.get(reverse("websites:logs-api", kwargs={"site_slug": self.website.slug}))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)


class WixLogIngestTests(TestCase):
    def setUp(self):
        self.website, _ = Website.objects.update_or_create(
            slug="working-people-united",
            defaults={
                "name": "Working People United",
                "domain": "workingpeopleunited.org",
                "platform": Website.PLATFORM_WIX,
                "log_ingest_secret": "test-secret",
                "is_active": True,
            },
        )
        self.url = reverse(
            "websites:wix-log-ingest",
            kwargs={"site_slug": self.website.slug, "secret": self.website.log_ingest_secret},
        )

    def test_valid_payload_creates_log_without_login(self):
        payload = {
            "timestamp": "2026-07-08T16:10:00Z",
            "level": "error",
            "source": "backend",
            "message": "Form submission failed",
            "requestId": "req-123",
            "path": "/volunteer",
            "functionName": "submitVolunteerForm",
        }
        response = self.client.post(
            self.url,
            data=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            HTTP_USER_AGENT="Wix Logs",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["events_created"], 1)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.website, self.website)
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_ERROR)
        self.assertEqual(event.message, "Form submission failed")
        self.assertEqual(event.request_id, "req-123")
        self.assertEqual(event.pathname, "/volunteer")
        self.assertEqual(event.function_name, "submitVolunteerForm")

    def test_wix_nested_payload_is_normalized(self):
        payload = {
            "timestamp": "2026-07-08T19:58:21.091Z",
            "operation": {
                "id": "ATS8_68",
                "producer": "https://www.workingpeopleunited.org",
            },
            "labels": {
                "pageName": "Registration",
                "namespace": "Velo",
            },
            "insertId": "9B_AHPZ6",
            "jsonPayload": {
                "message": "Running the code for the Registration page.",
            },
            "sourceLocation": {
                "file": "pages/Registration.iebmg.js",
            },
            "severity": "INFO",
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_INFO)
        self.assertEqual(event.message, "Running the code for the Registration page.")
        self.assertEqual(event.source, "pages/Registration.iebmg.js")
        self.assertEqual(event.request_id, "9B_AHPZ6")
        self.assertEqual(event.pathname, "Registration")
        self.assertEqual(event.event_type, "INFO")

    def test_wix_runtime_payload_without_severity_defaults_to_info(self):
        payload = {
            "timestamp": "2026-07-08T21:09:15.522Z",
            "labels": {
                "siteUrl": "https://www.workingpeopleunited.org",
                "revision": "173",
                "namespace": "Velo",
                "tenantId": "f1f58ea8-5051-4190-9579-37b57fe17e67",
                "viewMode": "Site",
                "pageName": "Registration",
            },
            "insertId": "6RMZ3JVZK5pQQ08wVkt942",
            "jsonPayload": {
                "message": "Running the code for the Registration page. To debug this code in your browser's dev tools, open iebmg.js.",
            },
            "sourceLocation": {
                "file": "pages/Registration.iebmg.js",
            },
            "operation": {
                "id": "501SHLb1hBj7AMiClEGRAn",
                "producer": "https://www.workingpeopleunited.org",
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_INFO)
        self.assertEqual(event.message, payload["jsonPayload"]["message"])
        self.assertEqual(event.source, "pages/Registration.iebmg.js")
        self.assertEqual(event.request_id, "6RMZ3JVZK5pQQ08wVkt942")
        self.assertEqual(event.pathname, "Registration")
        self.assertEqual(event.event_type, "Velo")

    def test_wix_nested_severity_is_normalized(self):
        payload = {
            "timestamp": "2026-07-08T21:09:16Z",
            "jsonPayload": {
                "level": "warn",
                "message": "Membership update frontend call failed",
            },
            "sourceLocation": {
                "file": "pages/Membership Update.g64dw.js",
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_WARNING)
        self.assertEqual(event.event_type, "warn")

    def test_wix_structured_context_is_extracted_and_searchable(self):
        payload = {
            "timestamp": "2026-07-08T21:12:04Z",
            "jsonPayload": {
                "message": (
                    "Registration downstream outcome: "
                    '{"primaryStore":"WoPU-AWS-Members/members","primaryOk":true,'
                    '"legacyMirrorOk":true,"legacyMirrorDuplicate":false,'
                    '"legacyMirrorError":"","membershipId":"WOPU-KAN-GPU4X84C",'
                    '"mirrorOk":true,"mirrorError":"","emailSent":false,'
                    '"emailError":"SMTP timeout","finalized":true}'
                ),
            },
            "sourceLocation": {
                "file": "backend/membership-card.web.js",
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_WARNING)
        self.assertEqual(event.context["membershipId"], "WOPU-KAN-GPU4X84C")
        self.assertTrue(event.context["primaryOk"])
        self.assertFalse(event.context["emailSent"])
        self.assertEqual(event.context["emailError"], "SMTP timeout")
        self.assertIn("WOPU-KAN-GPU4X84C", event.context_text)

        user = User.objects.create_user(username="context-user", password="pw12345")
        self.client.force_login(user)
        logs_response = self.client.get(
            reverse("websites:logs-api", kwargs={"site_slug": self.website.slug}),
            {"q": "SMTP timeout"},
        )
        self.assertEqual(logs_response.status_code, 200)
        logs_data = logs_response.json()
        self.assertEqual(logs_data["total"], 1)
        self.assertEqual(logs_data["logs"][0]["context"]["membershipId"], "WOPU-KAN-GPU4X84C")
        self.assertIn("emailError=SMTP timeout", logs_data["logs"][0]["context_summary"])

    def test_successful_registration_downstream_outcome_is_info(self):
        payload = {
            "timestamp": "2026-07-09T13:48:04.079Z",
            "labels": {"namespace": "Velo"},
            "insertId": "9VfqN",
            "sourceLocation": {"file": "backend/membership-card.web.js", "line": 2190},
            "jsonPayload": {
                "message": (
                    "Registration downstream outcome: "
                    '{"primaryStore":"WoPU-AWS-Members/members","primaryOk":true,'
                    '"primaryError":"","legacyMirrorOk":true,'
                    '"legacyMirrorDuplicate":false,"legacyMirrorError":"",'
                    '"membershipId":"WOPU-LAG-YB5QSMQT","mirrorOk":true,'
                    '"mirrorDuplicate":false,"mirrorError":"","emailSent":true,'
                    '"emailError":"","finalized":true,"alreadySent":false}'
                ),
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_INFO)
        self.assertEqual(event.context["membershipId"], "WOPU-LAG-YB5QSMQT")

    def test_failed_registration_downstream_outcome_is_error(self):
        payload = {
            "timestamp": "2026-07-09T13:48:04.079Z",
            "jsonPayload": {
                "message": (
                    "Registration downstream outcome: "
                    '{"primaryOk":false,"primaryError":"primary write failed",'
                    '"legacyMirrorOk":false,"legacyMirrorError":"",'
                    '"membershipId":"WOPU-FAILED","mirrorOk":false,'
                    '"mirrorError":"","emailSent":false,"emailError":"",'
                    '"finalized":false}'
                ),
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_ERROR)

    def test_welcome_email_contact_resolved_is_info(self):
        payload = {
            "timestamp": "2026-07-09T14:38:13.663Z",
            "labels": {"namespace": "Velo"},
            "sourceLocation": {"file": "backend/membership-card.web.js", "line": 715},
            "jsonPayload": {
                "message": (
                    "Welcome email contact resolved: "
                    '{"intendedEmail":"adebajograce8@gmail.com",'
                    '"contactId":"a56fff71-f0f0-43c2-9813-2fb1b77182bb",'
                    '"source":"appendOrCreateContact"}'
                ),
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_INFO)

    def test_unresolved_welcome_email_contact_checkpoint_stays_unknown(self):
        payload = {
            "timestamp": "2026-07-09T14:38:13.663Z",
            "jsonPayload": {
                "message": (
                    "Welcome email contact resolved: "
                    '{"intendedEmail":"adebajograce8@gmail.com","source":"appendOrCreateContact"}'
                ),
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_UNKNOWN)

    def test_registration_primary_write_complete_is_info(self):
        payload = {
            "timestamp": "2026-07-09T14:49:56.505Z",
            "labels": {"namespace": "Velo"},
            "sourceLocation": {"file": "backend/membership-card.web.js", "line": 2112},
            "jsonPayload": {
                "message": (
                    "Registration primary write complete: "
                    '{"primaryStore":"WoPU-AWS-Members/members","primaryOk":true,'
                    '"membershipId":"WOPU-OGU-S84UW4Y3","finalizePending":true}'
                ),
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_INFO)
        self.assertEqual(event.context["membershipId"], "WOPU-OGU-S84UW4Y3")

    def test_membership_card_svg_served_is_info(self):
        payload = {
            "timestamp": "2026-07-09T14:51:50.949Z",
            "labels": {"namespace": "Velo"},
            "sourceLocation": {"file": "backend/http-functions.js", "line": 938},
            "jsonPayload": {
                "message": (
                    "Membership card SVG served: "
                    '{"lookupType":"cardKey","cardKey":"4W4C...5BK2",'
                    '"usedProfilePicture":true,"usedSeal":true}'
                ),
            },
        }
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        event = WebsiteLogEvent.objects.get()
        self.assertEqual(event.severity, WebsiteLogEvent.SEVERITY_INFO)

    def test_array_payload_creates_multiple_logs(self):
        payload = [
            {"level": "info", "message": "A"},
            {"level": "warn", "message": "B"},
        ]
        response = self.client.post(self.url, data=json.dumps(payload), content_type="application/json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["events_created"], 2)
        self.assertEqual(WebsiteLogEvent.objects.count(), 2)

    def test_invalid_secret_returns_404_without_login_redirect(self):
        response = self.client.post(
            "/websites/webhooks/wix/working-people-united/wrong-secret/",
            data=b"{}",
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(WebsiteLogEvent.objects.count(), 0)

    def test_invalid_json_returns_400(self):
        response = self.client.post(self.url, data=b"{not-json", content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(WebsiteLogEvent.objects.count(), 0)

    def test_inactive_site_rejects_payload(self):
        self.website.is_active = False
        self.website.save(update_fields=["is_active"])
        response = self.client.post(self.url, data=b"{}", content_type="application/json")
        self.assertEqual(response.status_code, 404)
