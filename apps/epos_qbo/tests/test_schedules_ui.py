from __future__ import annotations

from datetime import datetime
from unittest import mock

from django.contrib.auth.models import Permission, User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import CompanyConfigRecord, RunJob, RunSchedule, RunScheduleEvent


class SchedulesUiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        perm = Permission.objects.get(codename="can_manage_schedules")
        self.user.user_permissions.add(perm)
        self.client.login(username="operator", password="pw12345")
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 20, 10, 0, 0))
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

    def _create_payload(self) -> dict[str, str]:
        return {
            "name": "Daily all companies",
            "enabled": "on",
            "scope": RunJob.SCOPE_ALL,
            "company_key": "",
            "cron_expr": "*/5 * * * *",
            "timezone_name": "UTC",
            "target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            "parallel": "2",
            "stagger_seconds": "2",
            "continue_on_failure": "",
        }

    def test_schedules_page_renders(self):
        response = self.client.get(reverse("epos_qbo:schedules"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Create Schedule")
        self.assertContains(response, "Configured Schedules")

    def test_create_update_toggle_delete_schedule(self):
        response = self.client.post(reverse("epos_qbo:schedule-create"), self._create_payload())
        self.assertEqual(response.status_code, 302)
        schedule = RunSchedule.objects.get(name="Daily all companies")
        self.assertTrue(schedule.enabled)
        self.assertIsNotNone(schedule.next_fire_at)

        update_payload = self._create_payload()
        update_payload["name"] = "Daily all companies updated"
        update_payload["cron_expr"] = "*/10 * * * *"
        response = self.client.post(reverse("epos_qbo:schedule-update", args=[schedule.id]), update_payload)
        self.assertEqual(response.status_code, 302)
        schedule.refresh_from_db()
        self.assertEqual(schedule.name, "Daily all companies updated")
        self.assertEqual(schedule.cron_expr, "*/10 * * * *")

        response = self.client.post(reverse("epos_qbo:schedule-toggle", args=[schedule.id]))
        self.assertEqual(response.status_code, 302)
        schedule.refresh_from_db()
        self.assertFalse(schedule.enabled)

        response = self.client.post(reverse("epos_qbo:schedule-delete", args=[schedule.id]))
        self.assertEqual(response.status_code, 302)
        self.assertFalse(RunSchedule.objects.filter(id=schedule.id).exists())

    @mock.patch("apps.epos_qbo.views.dispatch_next_queued_job")
    @mock.patch("apps.epos_qbo.services.schedule_worker.get_target_trading_date")
    def test_run_now_enqueues_job(self, mock_target_date, _mock_dispatch):
        mock_target_date.return_value = self.fixed_now.date()
        schedule = RunSchedule.objects.create(
            name="Run now schedule",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/5 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            parallel=2,
            stagger_seconds=2,
        )

        response = self.client.post(reverse("epos_qbo:schedule-run-now", args=[schedule.id]))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(RunJob.objects.filter(scheduled_by=schedule).count(), 1)

    def test_system_schedule_cannot_be_deleted(self):
        schedule = RunSchedule.objects.create(
            name="System schedule",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/5 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            is_system_managed=True,
        )
        response = self.client.post(reverse("epos_qbo:schedule-delete", args=[schedule.id]))
        self.assertEqual(response.status_code, 302)
        self.assertTrue(RunSchedule.objects.filter(id=schedule.id).exists())

    def test_recent_events_keep_schedule_name_after_schedule_delete(self):
        schedule = RunSchedule.objects.create(
            name="Transient schedule",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/5 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            parallel=2,
            stagger_seconds=2,
        )
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_ALL,
            status=RunJob.STATUS_QUEUED,
            scheduled_by=schedule,
        )
        RunScheduleEvent.objects.create(
            schedule=schedule,
            run_job=job,
            event_type=RunScheduleEvent.TYPE_QUEUED,
            message="Run queued (worker).",
            payload_json={"schedule_name": schedule.name},
        )
        schedule.delete()

        response = self.client.get(reverse("epos_qbo:schedules"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Transient schedule")

    def test_recent_events_show_legacy_scope_when_schedule_name_missing(self):
        RunScheduleEvent.objects.create(
            schedule=None,
            run_job=None,
            event_type=RunScheduleEvent.TYPE_QUEUED,
            message="Run queued (worker).",
            payload_json={"scope": RunJob.SCOPE_ALL, "company_key": None},
        )

        response = self.client.get(reverse("epos_qbo:schedules"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "All companies (legacy)")
