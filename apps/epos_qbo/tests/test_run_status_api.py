from __future__ import annotations

from datetime import datetime, timedelta

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from apps.epos_qbo.models import RunJob


class RunActiveIdsApiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.client.login(username="operator", password="pw12345")
        self.base_time = timezone.make_aware(datetime(2026, 2, 13, 12, 0, 0))

    def _create_job(self, status: str, offset_seconds: int) -> RunJob:
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=status,
        )
        RunJob.objects.filter(id=job.id).update(created_at=self.base_time + timedelta(seconds=offset_seconds))
        job.refresh_from_db()
        return job

    def test_returns_only_queued_and_running_jobs(self):
        queued_job = self._create_job(RunJob.STATUS_QUEUED, 1)
        running_job = self._create_job(RunJob.STATUS_RUNNING, 2)
        self._create_job(RunJob.STATUS_SUCCEEDED, 3)
        self._create_job(RunJob.STATUS_FAILED, 4)

        response = self.client.get(reverse("epos_qbo:run-active-ids"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("job_ids", payload)
        self.assertEqual(payload["job_ids"], [str(running_job.id), str(queued_job.id)])

    def test_caps_results_to_25_and_orders_newest_first(self):
        created_ids = []
        for idx in range(30):
            created_ids.append(str(self._create_job(RunJob.STATUS_QUEUED, idx).id))

        response = self.client.get(reverse("epos_qbo:run-active-ids"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["job_ids"]), 25)
        self.assertEqual(payload["job_ids"][0], created_ids[-1])
        self.assertEqual(payload["job_ids"][-1], created_ids[-25])
