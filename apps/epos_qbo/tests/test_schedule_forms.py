from __future__ import annotations

from datetime import datetime, timezone as dt_timezone

from django.test import TestCase
from django.utils import timezone

from apps.epos_qbo.forms import RunScheduleForm
from apps.epos_qbo.models import CompanyConfigRecord, RunJob, RunSchedule


class RunScheduleFormTests(TestCase):
    def setUp(self):
        self.inventory_company = CompanyConfigRecord.objects.create(
            company_key="company_a",
            display_name="AKPONORA VENTURES LTD.",
            is_active=True,
            config_json={
                "company_key": "company_a",
                "display_name": "AKPONORA VENTURES LTD.",
                "inventory": {"enable_inventory_items": True},
            },
        )
        self.sales_only_company = CompanyConfigRecord.objects.create(
            company_key="company_b",
            display_name="GOLDPLATES FEASTHOUSE LTD.",
            is_active=True,
            config_json={
                "company_key": "company_b",
                "display_name": "GOLDPLATES FEASTHOUSE LTD.",
                "inventory": {"enable_inventory_items": False},
            },
        )

    def _payload(self, **overrides) -> dict[str, str]:
        payload = {
            "name": "Daily Sales Sync",
            "enabled": "on",
            "schedule_type": RunSchedule.SCHEDULE_TYPE_RECURRING,
            "workflow": RunScheduleForm.WORKFLOW_SALES,
            "company_target": RunScheduleForm.COMPANY_TARGET_ALL,
            "company_key": "",
            "cron_expr": "0 18 * * *",
            "timezone_name": "UTC",
            "target_date_mode": RunSchedule.TARGET_DATE_MODE_TRADING_DATE,
            "parallel": "3",
            "stagger_seconds": "4",
            "continue_on_failure": "on",
            "category": "ALCOHOLS & SPIRITS",
            "product_filter": "TROPHY",
        }
        payload.update(overrides)
        return payload

    def test_recurring_sales_all_maps_to_all_scope_and_keeps_parallel_options(self):
        form = RunScheduleForm(data=self._payload())

        self.assertTrue(form.is_valid(), form.errors)
        schedule = form.save(commit=False)

        self.assertEqual(schedule.scope, RunJob.SCOPE_ALL)
        self.assertIsNone(schedule.company_key)
        self.assertEqual(schedule.inventory_options_json, {})
        self.assertEqual(schedule.parallel, 3)
        self.assertEqual(schedule.stagger_seconds, 4)
        self.assertTrue(schedule.continue_on_failure)

    def test_recurring_sales_one_company_forces_single_company_execution(self):
        form = RunScheduleForm(
            data=self._payload(
                workflow=RunScheduleForm.WORKFLOW_SALES,
                company_target=RunScheduleForm.COMPANY_TARGET_ONE,
                company_key=self.sales_only_company.company_key,
                continue_on_failure="on",
            )
        )

        self.assertTrue(form.is_valid(), form.errors)
        schedule = form.save(commit=False)

        self.assertEqual(schedule.scope, RunJob.SCOPE_SINGLE)
        self.assertEqual(schedule.company_key, self.sales_only_company.company_key)
        self.assertEqual(schedule.parallel, 1)
        self.assertEqual(schedule.stagger_seconds, 0)
        self.assertFalse(schedule.continue_on_failure)
        self.assertEqual(schedule.inventory_options_json, {})

    def test_recurring_inventory_one_company_requires_inventory_enabled_company(self):
        disabled_form = RunScheduleForm(
            data=self._payload(
                workflow=RunScheduleForm.WORKFLOW_INVENTORY,
                company_target=RunScheduleForm.COMPANY_TARGET_ONE,
                company_key=self.sales_only_company.company_key,
                category="ALCOHOLS & SPIRITS",
                product_filter="TROPHY",
            )
        )
        self.assertFalse(disabled_form.is_valid())
        self.assertIn("inventory-enabled company", str(disabled_form.errors))

        form = RunScheduleForm(
            data=self._payload(
                workflow=RunScheduleForm.WORKFLOW_INVENTORY,
                company_target=RunScheduleForm.COMPANY_TARGET_ONE,
                company_key=self.inventory_company.company_key,
                category="ALCOHOLS & SPIRITS",
                product_filter="TROPHY",
                parallel="5",
                continue_on_failure="on",
            )
        )

        self.assertTrue(form.is_valid(), form.errors)
        schedule = form.save(commit=False)

        self.assertEqual(schedule.scope, RunJob.SCOPE_INVENTORY_PIPELINE)
        self.assertEqual(schedule.company_key, self.inventory_company.company_key)
        self.assertEqual(
            schedule.inventory_options_json,
            {"categories": ["ALCOHOLS & SPIRITS"], "product_filter": "TROPHY"},
        )
        self.assertEqual(schedule.parallel, 1)
        self.assertEqual(schedule.stagger_seconds, 0)
        self.assertFalse(schedule.continue_on_failure)

    def test_inventory_all_companies_is_invalid(self):
        form = RunScheduleForm(
            data=self._payload(
                workflow=RunScheduleForm.WORKFLOW_INVENTORY,
                company_target=RunScheduleForm.COMPANY_TARGET_ALL,
                company_key="",
            )
        )

        self.assertFalse(form.is_valid())
        self.assertIn("not supported yet", str(form.errors))

    def test_one_time_schedule_requires_date_time_and_computes_run_once_at(self):
        missing_form = RunScheduleForm(
            data=self._payload(
                schedule_type=RunSchedule.SCHEDULE_TYPE_ONE_TIME,
                cron_expr="",
                run_once_date="",
                run_once_time="",
            )
        )
        self.assertFalse(missing_form.is_valid())
        self.assertIn("Run date is required", str(missing_form.errors))
        self.assertIn("Run time is required", str(missing_form.errors))

        form = RunScheduleForm(
            data=self._payload(
                name="Sunday Inventory Sync",
                schedule_type=RunSchedule.SCHEDULE_TYPE_ONE_TIME,
                workflow=RunScheduleForm.WORKFLOW_INVENTORY,
                company_target=RunScheduleForm.COMPANY_TARGET_ONE,
                company_key=self.inventory_company.company_key,
                cron_expr="",
                timezone_name="Africa/Lagos",
                run_once_date="2026-05-03",
                run_once_time="18:00",
            )
        )

        self.assertTrue(form.is_valid(), form.errors)
        schedule = form.save(commit=False)

        expected = timezone.make_aware(datetime(2026, 5, 3, 17, 0, 0), dt_timezone.utc)
        self.assertEqual(schedule.schedule_type, RunSchedule.SCHEDULE_TYPE_ONE_TIME)
        self.assertEqual(schedule.run_once_at, expected)
        self.assertEqual(schedule.compute_next_fire_at(), expected)
        self.assertEqual(schedule.cron_expr, "")
