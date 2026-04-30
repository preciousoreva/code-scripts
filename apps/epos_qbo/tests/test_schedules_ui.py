from __future__ import annotations

from datetime import datetime, timedelta
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
            display_name="AKPONORA VENTURES LTD.",
            config_json={
                "company_key": "company_a",
                "display_name": "AKPONORA VENTURES LTD.",
                "qbo": {"realm_id": "123"},
                "epos": {"username_env_key": "EPOS_USERNAME_A", "password_env_key": "EPOS_PASSWORD_A"},
                "inventory": {"enable_inventory_items": True},
            },
        )

    def _create_payload(self) -> dict[str, str]:
        return {
            "name": "Daily all companies",
            "enabled": "on",
            "schedule_type": RunSchedule.SCHEDULE_TYPE_RECURRING,
            "workflow": "sales",
            "company_target": "all",
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
        self.assertContains(response, "Worker Offline")
        self.assertNotContains(response, "Scheduler: Running")
        self.assertNotContains(response, "Scheduler: Not running")
        self.assertContains(response, "Online means the scheduler worker is polling for due schedules")
        self.assertContains(response, "It does not mean every schedule is enabled or successful")
        self.assertContains(response, "Offline means scheduled runs will not be picked up")
        self.assertContains(response, "Run type")
        self.assertContains(response, "Sales Sync")
        self.assertContains(response, "Inventory Sync")
        self.assertContains(response, "One-time")
        self.assertContains(response, "Recurring")
        self.assertNotContains(response, "Sales - all companies")
        self.assertNotContains(response, "Sales - single company")

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

    def test_seeded_weekly_inventory_schedule_defaults_to_all_products(self):
        schedule = RunSchedule.objects.get(name="Weekly Inventory Sync", is_system_managed=False)

        self.assertFalse(schedule.enabled)
        self.assertEqual(schedule.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        self.assertEqual(schedule.company_key, "company_a")
        self.assertEqual(schedule.cron_expr, "0 20 * * 0")
        self.assertEqual(schedule.timezone_name, "Africa/Lagos")
        self.assertEqual(schedule.inventory_options_json, {})

        response = self.client.get(reverse("epos_qbo:schedules"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Weekly Inventory Sync")
        self.assertContains(response, "AKPONORA VENTURES LTD. · All products")

    def test_create_inventory_schedule_persists_inventory_options(self):
        payload = self._create_payload()
        payload.update(
            {
                "name": "Weekly Inventory Sync",
                "enabled": "",
                "schedule_type": RunSchedule.SCHEDULE_TYPE_RECURRING,
                "workflow": "inventory",
                "company_target": "one",
                "company_key": "company_a",
                "cron_expr": "0 20 * * 0",
                "timezone_name": "Africa/Lagos",
                "parallel": "2",
                "stagger_seconds": "2",
                "continue_on_failure": "on",
                "category": "ALCOHOLS & SPIRITS",
                "product_filter": "TROPHY",
            }
        )

        response = self.client.post(reverse("epos_qbo:schedule-create"), payload)

        self.assertEqual(response.status_code, 302)
        schedule = RunSchedule.objects.filter(name="Weekly Inventory Sync").order_by("-created_at").first()
        assert schedule is not None
        self.assertFalse(schedule.enabled)
        self.assertEqual(schedule.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        self.assertEqual(schedule.company_key, "company_a")
        self.assertEqual(
            schedule.inventory_options_json,
            {"categories": ["ALCOHOLS & SPIRITS"], "product_filter": "TROPHY"},
        )
        self.assertEqual(schedule.parallel, 1)
        self.assertFalse(schedule.continue_on_failure)

    def test_schedules_page_uses_operator_friendly_schedule_wording(self):
        RunSchedule.objects.create(
            name="All Companies Daily Run",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="0 19 * * *",
            timezone_name="Africa/Lagos",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            next_fire_at=self.fixed_now + timedelta(hours=1),
        )
        RunSchedule.objects.create(
            name="Legacy Env Fallback",
            enabled=False,
            scope=RunJob.SCOPE_ALL,
            cron_expr="0 18 * * *",
            timezone_name="Africa/Lagos",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            is_system_managed=True,
        )

        response = self.client.get(reverse("epos_qbo:schedules"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Daily Sales Sync")
        self.assertNotContains(response, "All Companies Daily Run")
        self.assertContains(response, "System Fallback Schedule")
        self.assertContains(response, "Legacy environment configuration")
        self.assertContains(response, "System-managed")
        self.assertNotContains(response, "Legacy Env Fallback")
        self.assertContains(response, "Daily at 19:00")
        self.assertContains(response, "Workflow")
        self.assertContains(response, "Status")
        self.assertContains(response, "Schedule")
        self.assertContains(response, "Next Run")
        self.assertContains(response, "Actions")
        self.assertNotContains(response, "Last Result")
        self.assertNotContains(response, "Next Fire")
        self.assertNotContains(response, "Cron / TZ")
        self.assertContains(response, "Recent Schedule Activity")
        self.assertNotContains(response, "Recent Scheduled Events")

    def test_schedules_page_displays_one_time_completed_schedule(self):
        completed_at = self.fixed_now - timedelta(minutes=10)
        RunSchedule.objects.create(
            name="Sunday Inventory Sync",
            enabled=False,
            schedule_type=RunSchedule.SCHEDULE_TYPE_ONE_TIME,
            scope=RunJob.SCOPE_INVENTORY_PIPELINE,
            company_key=self.company.company_key,
            cron_expr="",
            timezone_name="Africa/Lagos",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            run_once_at=self.fixed_now,
            completed_at=completed_at,
            last_fired_at=completed_at,
            last_result=RunSchedule.LAST_RESULT_QUEUED,
        )

        response = self.client.get(reverse("epos_qbo:schedules"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Sunday Inventory Sync")
        self.assertContains(response, "One-time")
        self.assertContains(response, "Completed")
        self.assertContains(response, "Queued once at")

    def test_last_result_uses_latest_terminal_event_not_queued(self):
        schedule = RunSchedule.objects.create(
            name="Daily Sales Sync",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="0 19 * * *",
            timezone_name="Africa/Lagos",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
        )
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_ALL,
            status=RunJob.STATUS_SUCCEEDED,
            scheduled_by=schedule,
            finished_at=self.fixed_now,
        )
        RunScheduleEvent.objects.create(
            schedule=schedule,
            run_job=job,
            event_type=RunScheduleEvent.TYPE_QUEUED,
            message="Run queued (worker).",
            payload_json={"schedule_name": schedule.name},
        )
        RunScheduleEvent.objects.create(
            schedule=schedule,
            run_job=job,
            event_type=RunScheduleEvent.TYPE_RUN_SUCCEEDED,
            message="Run completed with status=succeeded exit_code=0",
            payload_json={"schedule_name": schedule.name},
        )

        response = self.client.get(reverse("epos_qbo:schedules"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Succeeded")
        self.assertContains(response, "Last run:")
        self.assertContains(response, "Run completed successfully")
        self.assertContains(response, "Run queued")
        self.assertNotContains(response, "status=succeeded exit_code=0")

    def test_recent_events_render_friendly_type_and_message(self):
        schedule = RunSchedule.objects.create(
            name="Daily Sales Sync",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="0 19 * * *",
            timezone_name="Africa/Lagos",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
        )
        RunScheduleEvent.objects.create(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_SKIPPED_OVERLAP,
            message="Skipped worker enqueue because this schedule already has a queued/running run.",
        )
        RunScheduleEvent.objects.create(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_RUN_FAILED,
            message="Run completed with status=failed exit_code=1",
        )

        response = self.client.get(reverse("epos_qbo:schedules"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Run skipped")
        self.assertContains(response, "Skipped because another run is active")
        self.assertContains(response, "Run failed")
        self.assertNotContains(response, "queued/running")
        self.assertNotContains(response, "exit_code=1")

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
