from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone
from unittest import mock

from django.test import SimpleTestCase

from apps.epos_qbo.models import format_relative_time


class RelativeTimeFilterTests(SimpleTestCase):
    def test_returns_yesterday_for_one_day_old_timestamp(self):
        now = datetime(2026, 2, 21, 12, 0, 0, tzinfo=dt_timezone.utc)
        value = now - timedelta(days=1, hours=1)
        with mock.patch("apps.epos_qbo.models.timezone.now", return_value=now):
            self.assertEqual(format_relative_time(value), "yesterday")

    def test_returns_day_count_for_older_timestamp(self):
        now = datetime(2026, 2, 21, 12, 0, 0, tzinfo=dt_timezone.utc)
        value = now - timedelta(days=2, hours=3)
        with mock.patch("apps.epos_qbo.models.timezone.now", return_value=now):
            self.assertEqual(format_relative_time(value), "2 days ago")

    def test_returns_just_now_for_recent_timestamp(self):
        now = datetime(2026, 2, 21, 12, 0, 0, tzinfo=dt_timezone.utc)
        value = now - timedelta(seconds=20)
        with mock.patch("apps.epos_qbo.models.timezone.now", return_value=now):
            self.assertEqual(format_relative_time(value), "just now")
