from __future__ import annotations

from django.contrib.auth.models import User
from django.test import SimpleTestCase, TestCase
from django.urls import reverse

from apps.epos_qbo.models import RunJob
from apps.epos_qbo.views import _exit_code_info


class ExitCodeInfoTests(SimpleTestCase):
    def test_known_exit_code_mapping(self):
        info = _exit_code_info(1)
        self.assertIsNotNone(info)
        self.assertEqual(info["label"], "Pipeline failure")

    def test_signal_exit_code_mapping(self):
        info = _exit_code_info(-9)
        self.assertIsNotNone(info)
        self.assertEqual(info["label"], "Terminated by signal")
        self.assertIn("9", info["description"])

    def test_none_exit_code_returns_none(self):
        self.assertIsNone(_exit_code_info(None))


class RunDetailExitCodeGuideTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="operator", password="pw12345")
        self.client.login(username="operator", password="pw12345")

    def test_failed_run_shows_exit_code_guide(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_FAILED,
            exit_code=1,
            failure_reason="Subprocess exited with code 1",
        )

        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Exit Code Guide")
        self.assertContains(response, "Pipeline failure")

    def test_success_run_hides_exit_code_guide(self):
        job = RunJob.objects.create(
            scope=RunJob.SCOPE_SINGLE,
            company_key="company_a",
            status=RunJob.STATUS_SUCCEEDED,
            exit_code=0,
        )

        response = self.client.get(reverse("epos_qbo:run-detail", kwargs={"job_id": job.id}))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Exit Code Guide")
