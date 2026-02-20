from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse

from apps.epos_qbo.models import RunJob


class LogPollingTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="viewer", password="pw12345")
        self.client.login(username="viewer", password="pw12345")

    def test_log_endpoint_returns_incremental_chunks(self):
        with TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "run.log"
            log_path.write_text("line-one\nline-two\n", encoding="utf-8")

            job = RunJob.objects.create(
                scope=RunJob.SCOPE_ALL,
                status=RunJob.STATUS_RUNNING,
                log_file_path=str(log_path),
            )

            url = reverse("epos_qbo:run-logs", kwargs={"job_id": job.id})
            response_1 = self.client.get(url, {"offset": 0})
            self.assertEqual(response_1.status_code, 200)
            payload_1 = response_1.json()
            self.assertIn("line-one", payload_1["chunk"])

            response_2 = self.client.get(url, {"offset": payload_1["next_offset"]})
            self.assertEqual(response_2.status_code, 200)
            payload_2 = response_2.json()
            self.assertEqual(payload_2["chunk"], "")
