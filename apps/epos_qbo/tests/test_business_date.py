from __future__ import annotations

from datetime import datetime, timezone as dt_tz

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from apps.epos_qbo.business_date import (
    get_business_day_cutoff,
    get_business_timezone_display,
    get_prev_trading_date,
    get_target_trading_date,
)


class BusinessDateTests(SimpleTestCase):
    @override_settings(
        OIAT_BUSINESS_TIMEZONE="Africa/Lagos",
        OIAT_BUSINESS_DAY_CUTOFF_HOUR=5,
        OIAT_BUSINESS_DAY_CUTOFF_MINUTE=0,
    )
    def test_target_trading_date_before_cutoff_uses_two_days_back(self):
        now_utc = timezone.make_aware(datetime(2026, 2, 13, 3, 30, 0), dt_tz.utc)  # 04:30 WAT
        self.assertEqual(get_target_trading_date(now=now_utc).isoformat(), "2026-02-11")

    @override_settings(
        OIAT_BUSINESS_TIMEZONE="Africa/Lagos",
        OIAT_BUSINESS_DAY_CUTOFF_HOUR=5,
        OIAT_BUSINESS_DAY_CUTOFF_MINUTE=0,
    )
    def test_target_trading_date_at_or_after_cutoff_uses_one_day_back(self):
        at_cutoff = timezone.make_aware(datetime(2026, 2, 13, 4, 0, 0), dt_tz.utc)  # 05:00 WAT
        after_cutoff = timezone.make_aware(datetime(2026, 2, 13, 10, 0, 0), dt_tz.utc)  # 11:00 WAT
        self.assertEqual(get_target_trading_date(now=at_cutoff).isoformat(), "2026-02-12")
        self.assertEqual(get_target_trading_date(now=after_cutoff).isoformat(), "2026-02-12")

    @override_settings(
        OIAT_BUSINESS_TIMEZONE="Africa/Lagos",
        OIAT_BUSINESS_DAY_CUTOFF_HOUR=5,
        OIAT_BUSINESS_DAY_CUTOFF_MINUTE=15,
    )
    def test_cutoff_and_prev_date_helpers(self):
        cutoff_hour, cutoff_minute = get_business_day_cutoff()
        self.assertEqual((cutoff_hour, cutoff_minute), (5, 15))
        now_utc = timezone.make_aware(datetime(2026, 2, 14, 6, 0, 0), dt_tz.utc)  # 07:00 WAT
        target_date = get_target_trading_date(now=now_utc)
        self.assertEqual(target_date.isoformat(), "2026-02-13")
        self.assertEqual(get_prev_trading_date(target_date).isoformat(), "2026-02-12")

    @override_settings(OIAT_BUSINESS_TIMEZONE="Africa/Lagos")
    def test_business_timezone_display_includes_zone_name(self):
        now_utc = timezone.make_aware(datetime(2026, 2, 14, 6, 0, 0), dt_tz.utc)
        display = get_business_timezone_display(now=now_utc)
        self.assertIn("Africa/Lagos", display)
