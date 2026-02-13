from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models
from django.utils import timezone


class CompanyConfigRecord(models.Model):
    company_key = models.SlugField(max_length=64, unique=True)
    display_name = models.CharField(max_length=255)
    config_json = models.JSONField(default=dict)
    is_active = models.BooleanField(default=True)
    config_version = models.PositiveIntegerField(default=1)
    checksum = models.CharField(max_length=64, blank=True)
    schema_version = models.CharField(max_length=32, default="1.0.0")
    last_synced_to_json_at = models.DateTimeField(null=True, blank=True)
    last_synced_from_json_at = models.DateTimeField(null=True, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="created_company_configs",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="updated_company_configs",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        permissions = [
            ("can_edit_companies", "Can edit company configuration"),
        ]
        ordering = ["company_key"]

    def __str__(self) -> str:
        return f"{self.company_key} ({self.display_name})"


class RunJob(models.Model):
    SCOPE_SINGLE = "single_company"
    SCOPE_ALL = "all_companies"
    SCOPE_CHOICES = [
        (SCOPE_SINGLE, "Single Company"),
        (SCOPE_ALL, "All Companies"),
    ]

    STATUS_QUEUED = "queued"
    STATUS_RUNNING = "running"
    STATUS_SUCCEEDED = "succeeded"
    STATUS_FAILED = "failed"
    STATUS_CANCELLED = "cancelled"
    STATUS_CHOICES = [
        (STATUS_QUEUED, "Queued"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCEEDED, "Succeeded"),
        (STATUS_FAILED, "Failed"),
        (STATUS_CANCELLED, "Cancelled"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    scope = models.CharField(max_length=32, choices=SCOPE_CHOICES)
    company_key = models.SlugField(max_length=64, null=True, blank=True)
    target_date = models.DateField(null=True, blank=True)
    from_date = models.DateField(null=True, blank=True)
    to_date = models.DateField(null=True, blank=True)
    skip_download = models.BooleanField(default=False)
    parallel = models.PositiveSmallIntegerField(default=1)
    stagger_seconds = models.PositiveSmallIntegerField(default=2)
    continue_on_failure = models.BooleanField(default=False)
    command_json = models.JSONField(default=list)
    command_display = models.TextField(blank=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_QUEUED)
    pid = models.IntegerField(null=True, blank=True)
    exit_code = models.IntegerField(null=True, blank=True)
    log_file_path = models.TextField(blank=True)
    requested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="requested_run_jobs",
    )
    failure_reason = models.TextField(blank=True)
    queued_at = models.DateTimeField(default=timezone.now)
    dispatched_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        permissions = [
            ("can_trigger_runs", "Can trigger pipeline runs"),
        ]
        ordering = ["-created_at"]

    @property
    def display_label(self) -> str:
        """Human-readable run label: YYYY-MM-DD HH:MM from started_at or created_at."""
        dt = self.started_at or self.created_at
        return dt.strftime("%Y-%m-%d %H:%M") if dt else str(self.id)

    def __str__(self) -> str:
        return f"RunJob {self.id} [{self.status}]"


class RunArtifact(models.Model):
    RELIABILITY_HIGH = "high"
    RELIABILITY_WARNING = "warning"
    RELIABILITY_CHOICES = [
        (RELIABILITY_HIGH, "High"),
        (RELIABILITY_WARNING, "Warning"),
    ]

    run_job = models.ForeignKey(RunJob, null=True, blank=True, on_delete=models.SET_NULL, related_name="artifacts")
    company_key = models.SlugField(max_length=64)
    target_date = models.DateField(null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    source_path = models.TextField()
    source_hash = models.CharField(max_length=64)
    reliability_status = models.CharField(max_length=16, choices=RELIABILITY_CHOICES, default=RELIABILITY_WARNING)
    rows_total = models.IntegerField(null=True, blank=True)
    rows_kept = models.IntegerField(null=True, blank=True)
    rows_non_target = models.IntegerField(null=True, blank=True)
    upload_stats_json = models.JSONField(default=dict)
    reconcile_status = models.CharField(max_length=32, blank=True)
    reconcile_difference = models.FloatField(null=True, blank=True)
    raw_file = models.CharField(max_length=255, blank=True)
    processed_files_json = models.JSONField(default=list)
    nearest_log_file = models.TextField(blank=True)
    imported_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["company_key", "target_date", "processed_at", "source_hash"],
                name="uniq_run_artifact_source",
            )
        ]
        ordering = ["-processed_at", "-imported_at"]


class RunLock(models.Model):
    id = models.PositiveSmallIntegerField(primary_key=True, default=1, editable=False)
    active = models.BooleanField(default=False)
    holder = models.CharField(max_length=255, blank=True)
    owner_run_job = models.ForeignKey(RunJob, null=True, blank=True, on_delete=models.SET_NULL, related_name="owned_locks")
    acquired_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    def save(self, *args, **kwargs):
        self.id = 1
        return super().save(*args, **kwargs)
