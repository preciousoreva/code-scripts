from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest import mock

from django.test import TestCase
from django.utils import timezone

from apps.epos_qbo.models import RunJob, RunSchedule, RunScheduleEvent, SchedulerWorkerHeartbeat
from apps.epos_qbo.services import schedule_worker


class ScheduleWorkerTests(TestCase):
    def setUp(self):
        self.fixed_now = timezone.make_aware(datetime(2026, 2, 20, 10, 0, 0))

    @mock.patch("apps.epos_qbo.services.schedule_worker.dispatch_next_queued_job")
    @mock.patch("apps.epos_qbo.services.schedule_worker.get_target_trading_date", return_value=date(2026, 2, 19))
    def test_due_schedule_queues_run_job(self, _mock_target_date, _mock_dispatch):
        schedule = RunSchedule.objects.create(
            name="Daily all companies",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/5 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            parallel=2,
            stagger_seconds=2,
            continue_on_failure=False,
            next_fire_at=self.fixed_now - timedelta(minutes=1),
        )

        stats = schedule_worker.process_schedule_cycle(now=self.fixed_now)

        self.assertEqual(stats["due"], 1)
        self.assertEqual(stats["queued"], 1)
        queued_jobs = RunJob.objects.filter(scheduled_by=schedule)
        self.assertEqual(queued_jobs.count(), 1)
        queued_job = queued_jobs.first()
        assert queued_job is not None
        self.assertEqual(queued_job.status, RunJob.STATUS_QUEUED)
        self.assertEqual(queued_job.target_date.isoformat(), "2026-02-19")

        schedule.refresh_from_db()
        self.assertEqual(schedule.last_result, RunSchedule.LAST_RESULT_QUEUED)
        self.assertIsNotNone(schedule.next_fire_at)
        self.assertGreater(schedule.next_fire_at, self.fixed_now)
        event = RunScheduleEvent.objects.filter(
            schedule=schedule,
            event_type=RunScheduleEvent.TYPE_QUEUED,
        ).first()
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.payload_json.get("schedule_name"), schedule.name)
        self.assertEqual(event.payload_json.get("schedule_id"), str(schedule.id))

    @mock.patch("apps.epos_qbo.services.schedule_worker.dispatch_next_queued_job")
    @mock.patch("apps.epos_qbo.services.schedule_worker.get_target_trading_date", return_value=date(2026, 2, 19))
    def test_due_schedule_skips_overlap(self, _mock_target_date, _mock_dispatch):
        schedule = RunSchedule.objects.create(
            name="Daily overlap check",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/5 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            parallel=2,
            stagger_seconds=2,
            continue_on_failure=False,
            next_fire_at=self.fixed_now - timedelta(minutes=1),
        )
        RunJob.objects.create(
            scope=RunJob.SCOPE_ALL,
            status=RunJob.STATUS_RUNNING,
            scheduled_by=schedule,
            target_date=date(2026, 2, 19),
        )

        stats = schedule_worker.process_schedule_cycle(now=self.fixed_now)

        self.assertEqual(stats["due"], 1)
        self.assertEqual(stats["queued"], 0)
        self.assertEqual(stats["skipped_overlap"], 1)
        self.assertEqual(
            RunJob.objects.filter(scheduled_by=schedule, status=RunJob.STATUS_QUEUED).count(),
            0,
        )
        self.assertTrue(
            RunScheduleEvent.objects.filter(
                schedule=schedule,
                event_type=RunScheduleEvent.TYPE_SKIPPED_OVERLAP,
            ).exists()
        )

    @mock.patch("apps.epos_qbo.services.schedule_worker.dispatch_next_queued_job")
    def test_fallback_schedule_is_created_when_enabled_and_no_user_schedule(self, _mock_dispatch):
        with mock.patch.dict(
            "os.environ",
            {
                "OIAT_SCHEDULER_ENABLE_ENV_FALLBACK": "1",
                "SCHEDULE_CRON": "*/7 * * * *",
                "SCHEDULE_TZ": "UTC",
            },
            clear=False,
        ):
            stats = schedule_worker.process_schedule_cycle(now=self.fixed_now)

        self.assertEqual(stats["fallback_enabled"], 1)
        fallback = RunSchedule.objects.get(name=schedule_worker.FALLBACK_SCHEDULE_NAME, is_system_managed=True)
        self.assertTrue(fallback.enabled)
        self.assertEqual(fallback.cron_expr, "*/7 * * * *")
        self.assertEqual(fallback.timezone_name, "UTC")
        self.assertIsNotNone(fallback.next_fire_at)

    @mock.patch("apps.epos_qbo.services.schedule_worker.dispatch_next_queued_job")
    def test_fallback_schedule_is_disabled_when_user_schedule_exists(self, _mock_dispatch):
        RunSchedule.objects.create(
            name="User schedule",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/10 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            next_fire_at=self.fixed_now + timedelta(minutes=5),
        )
        fallback = RunSchedule.objects.create(
            name=schedule_worker.FALLBACK_SCHEDULE_NAME,
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="*/5 * * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            is_system_managed=True,
            next_fire_at=self.fixed_now + timedelta(minutes=1),
        )

        with mock.patch.dict(
            "os.environ",
            {
                "OIAT_SCHEDULER_ENABLE_ENV_FALLBACK": "1",
                "SCHEDULE_CRON": "*/5 * * * *",
                "SCHEDULE_TZ": "UTC",
            },
            clear=False,
        ):
            stats = schedule_worker.process_schedule_cycle(now=self.fixed_now)

        self.assertEqual(stats["fallback_disabled"], 1)
        fallback.refresh_from_db()
        self.assertFalse(fallback.enabled)
        self.assertTrue(
            RunScheduleEvent.objects.filter(
                schedule=fallback,
                event_type=RunScheduleEvent.TYPE_FALLBACK_DISABLED,
            ).exists()
        )

    @mock.patch("apps.epos_qbo.services.schedule_worker.dispatch_next_queued_job")
    @mock.patch("apps.epos_qbo.services.schedule_worker.get_target_trading_date", return_value=date(2026, 2, 19))
    def test_process_schedule_cycle_records_heartbeat(self, _mock_target_date, _mock_dispatch):
        RunSchedule.objects.create(
            name="Daily",
            enabled=True,
            scope=RunJob.SCOPE_ALL,
            cron_expr="0 18 * * *",
            timezone_name="UTC",
            target_date_mode=RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            next_fire_at=self.fixed_now + timedelta(hours=1),
        )
        schedule_worker.process_schedule_cycle(now=self.fixed_now)
        hb = SchedulerWorkerHeartbeat.objects.filter(id=1).first()
        self.assertIsNotNone(hb)
        self.assertEqual(hb.last_seen, self.fixed_now)

    def test_get_scheduler_status_no_heartbeat_returns_not_running(self):
        SchedulerWorkerHeartbeat.objects.filter(id=1).delete()
        status = schedule_worker.get_scheduler_status()
        self.assertFalse(status["running"])
        self.assertIsNone(status["last_seen"])
        self.assertIn("not run", status["message"])

    def test_get_scheduler_status_recent_heartbeat_returns_running(self):
        now = timezone.now()
        SchedulerWorkerHeartbeat.objects.update_or_create(id=1, defaults={"last_seen": now})
        with mock.patch("apps.epos_qbo.services.schedule_worker.configured_poll_seconds", return_value=15):
            status = schedule_worker.get_scheduler_status()
        self.assertTrue(status["running"])
        self.assertEqual(status["last_seen"], now)
        self.assertIn("Worker is polling", status["message"])
