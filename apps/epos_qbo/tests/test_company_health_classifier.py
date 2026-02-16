from __future__ import annotations

from decimal import Decimal

from django.test import TestCase

from apps.epos_qbo import views
from apps.epos_qbo.models import CompanyConfigRecord, RunArtifact, RunJob


class CompanyHealthClassifierTests(TestCase):
    def setUp(self):
        self.company_ok = CompanyConfigRecord.objects.create(
            company_key="company_ok",
            display_name="Company OK",
            config_json={
                "company_key": "company_ok",
                "display_name": "Company OK",
                "qbo": {"realm_id": "123"},
                "epos": {
                    "username_env_key": "EPOS_USERNAME_OK",
                    "password_env_key": "EPOS_PASSWORD_OK",
                },
            },
        )
        self.company_missing_epos = CompanyConfigRecord.objects.create(
            company_key="company_missing_epos",
            display_name="Company Missing EPOS",
            config_json={
                "company_key": "company_missing_epos",
                "display_name": "Company Missing EPOS",
                "qbo": {"realm_id": "123"},
                "epos": {},
            },
        )
        self.token_healthy = {"severity": "healthy", "status_message": "Connected"}
        self.token_warning = {"severity": "warning", "status_message": "Refresh token expires in 2 days"}
        self.token_critical = {"severity": "critical", "status_message": "QBO re-authentication required"}

    def _artifact(self, *, failed_uploads: int = 0, reconcile_difference: Decimal | None = None) -> RunArtifact:
        return RunArtifact(
            company_key=self.company_ok.company_key,
            source_path="/tmp/health-matrix.json",
            source_hash="health-matrix",
            upload_stats_json={"failed": failed_uploads},
            reconcile_difference=reconcile_difference,
        )

    def _run(self, status: str) -> RunJob:
        return RunJob(
            scope=RunJob.SCOPE_SINGLE,
            company_key=self.company_ok.company_key,
            status=status,
        )

    def test_classifier_matrix_levels_and_reasons(self):
        cases = [
            {
                "name": "missing epos config",
                "company": self.company_missing_epos,
                "token_info": self.token_healthy,
                "artifact": self._artifact(),
                "job": None,
                "expected_level": "warning",
                "expected_reason": "EPOS_CONFIG_MISSING",
            },
            {
                "name": "critical token",
                "company": self.company_ok,
                "token_info": self.token_critical,
                "artifact": self._artifact(),
                "job": None,
                "expected_level": "critical",
                "expected_reason": "TOKEN_CRITICAL",
            },
            {
                "name": "failed latest run",
                "company": self.company_ok,
                "token_info": self.token_healthy,
                "artifact": self._artifact(),
                "job": self._run(RunJob.STATUS_FAILED),
                "expected_level": "critical",
                "expected_reason": "LATEST_RUN_FAILED",
            },
            {
                "name": "token expiring warning",
                "company": self.company_ok,
                "token_info": self.token_warning,
                "artifact": self._artifact(),
                "job": None,
                "expected_level": "warning",
                "expected_reason": "TOKEN_EXPIRING_SOON",
            },
            {
                "name": "no artifact yet unknown",
                "company": self.company_ok,
                "token_info": self.token_healthy,
                "artifact": None,
                "job": None,
                "expected_level": "unknown",
                "expected_reason": "NO_ARTIFACT_METADATA",
            },
            {
                "name": "failed uploads critical",
                "company": self.company_ok,
                "token_info": self.token_healthy,
                "artifact": self._artifact(failed_uploads=2),
                "job": None,
                "expected_level": "critical",
                "expected_reason": "UPLOAD_FAILURE",
            },
            {
                "name": "reconciliation mismatch warning",
                "company": self.company_ok,
                "token_info": self.token_healthy,
                "artifact": self._artifact(reconcile_difference=Decimal("2.25")),
                "job": None,
                "expected_level": "warning",
                "expected_reason": "RECON_MISMATCH",
            },
            {
                "name": "healthy with running activity",
                "company": self.company_ok,
                "token_info": self.token_healthy,
                "artifact": self._artifact(),
                "job": self._run(RunJob.STATUS_RUNNING),
                "expected_level": "healthy",
                "expected_reason": None,
                "expected_activity": "running",
            },
        ]

        for case in cases:
            with self.subTest(case["name"]):
                health = views._company_health_snapshot(
                    case["company"],
                    latest_artifact=case["artifact"],
                    latest_job=case["job"],
                    token_info=case["token_info"],
                )
                self.assertEqual(health["level"], case["expected_level"])
                expected_reason = case.get("expected_reason")
                if expected_reason:
                    self.assertIn(expected_reason, health["reason_codes"])
                else:
                    self.assertEqual(health["reason_codes"], [])
                if case.get("expected_activity"):
                    self.assertEqual(health["run_activity"], case["expected_activity"])
