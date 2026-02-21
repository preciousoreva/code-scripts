from __future__ import annotations

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse

from apps.epos_qbo.models import CompanyConfigRecord


class SchedulePermissionsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="Company A",
            config_json={
                "company_key": "company_a",
                "display_name": "Company A",
                "qbo": {"realm_id": "123"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
            },
        )

    def test_schedules_page_requires_login(self):
        response = self.client.get(reverse("epos_qbo:schedules"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_schedules_page_requires_manage_permission(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:schedules"))
        self.assertEqual(response.status_code, 403)

    def test_schedules_page_with_permission_returns_ok(self):
        perm = Permission.objects.get(codename="can_manage_schedules")
        self.user.user_permissions.add(perm)
        self.client.login(username="operator", password="pw12345")
        response = self.client.get(reverse("epos_qbo:schedules"))
        self.assertEqual(response.status_code, 200)

    def test_schedule_create_requires_manage_permission(self):
        self.client.login(username="operator", password="pw12345")
        response = self.client.post(
            reverse("epos_qbo:schedule-create"),
            {
                "name": "Daily",
                "enabled": "on",
                "scope": "all_companies",
                "company_key": "",
                "cron_expr": "*/5 * * * *",
                "timezone_name": "UTC",
                "target_date_mode": "trading_date",
                "parallel": "2",
                "stagger_seconds": "2",
            },
        )
        self.assertEqual(response.status_code, 403)
