from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone as dt_timezone
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone


def _parse_int(value: str) -> int:
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"Invalid integer: {value}") from exc


def _expand_cron_part(
    token: str,
    *,
    minimum: int,
    maximum: int,
    allow_seven_for_sunday: bool = False,
) -> tuple[set[int], bool]:
    raw = (token or "").strip()
    if not raw:
        raise ValueError("Empty cron field.")

    wildcard = raw == "*"
    allowed: set[int] = set()
    maximum_allowed = 7 if allow_seven_for_sunday else maximum

    for segment in raw.split(","):
        part = segment.strip()
        if not part:
            raise ValueError("Empty cron segment.")

        if "/" in part:
            base, step_raw = part.split("/", 1)
            step = _parse_int(step_raw)
            if step <= 0:
                raise ValueError("Step must be > 0.")
        else:
            base = part
            step = 1

        if base == "*":
            start = minimum
            end = maximum
        elif "-" in base:
            start_raw, end_raw = base.split("-", 1)
            start = _parse_int(start_raw)
            end = _parse_int(end_raw)
        else:
            start = _parse_int(base)
            end = start

        if start < minimum or start > maximum_allowed:
            raise ValueError("Value out of range.")
        if end < minimum or end > maximum_allowed:
            raise ValueError("Value out of range.")
        if start > end:
            raise ValueError("Invalid range in cron field.")

        for value in range(start, end + 1, step):
            if allow_seven_for_sunday and value == 7:
                value = 0
            if value < minimum or value > maximum:
                raise ValueError("Value out of range.")
            allowed.add(value)

    if not allowed:
        raise ValueError("Cron field has no allowed values.")
    return allowed, wildcard


def _parse_cron_expr(expr: str) -> dict:
    parts = (expr or "").strip().split()
    if len(parts) != 5:
        raise ValueError("Cron expression must contain 5 fields.")

    minutes, minutes_any = _expand_cron_part(parts[0], minimum=0, maximum=59)
    hours, hours_any = _expand_cron_part(parts[1], minimum=0, maximum=23)
    dom, dom_any = _expand_cron_part(parts[2], minimum=1, maximum=31)
    months, months_any = _expand_cron_part(parts[3], minimum=1, maximum=12)
    dow, dow_any = _expand_cron_part(
        parts[4],
        minimum=0,
        maximum=6,
        allow_seven_for_sunday=True,
    )

    return {
        "minutes": minutes,
        "hours": hours,
        "dom": dom,
        "months": months,
        "dow": dow,
        "minutes_any": minutes_any,
        "hours_any": hours_any,
        "dom_any": dom_any,
        "months_any": months_any,
        "dow_any": dow_any,
    }


def _cron_matches(candidate: datetime, parsed: dict) -> bool:
    cron_dow = (candidate.weekday() + 1) % 7
    if candidate.minute not in parsed["minutes"]:
        return False
    if candidate.hour not in parsed["hours"]:
        return False
    if candidate.month not in parsed["months"]:
        return False

    dom_match = candidate.day in parsed["dom"]
    dow_match = cron_dow in parsed["dow"]
    dom_any = parsed["dom_any"]
    dow_any = parsed["dow_any"]

    if dom_any and dow_any:
        day_ok = True
    elif dom_any:
        day_ok = dow_match
    elif dow_any:
        day_ok = dom_match
    else:
        day_ok = dom_match or dow_match
    return day_ok


def _next_fire_fallback(expr: str, *, from_dt: datetime, tz: ZoneInfo) -> datetime:
    parsed = _parse_cron_expr(expr)
    local = from_dt.astimezone(tz).replace(second=0, microsecond=0)
    candidate = local + timedelta(minutes=1)
    upper_bound_minutes = 366 * 24 * 60
    for _ in range(upper_bound_minutes):
        if _cron_matches(candidate, parsed):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError("Could not compute next fire time from cron expression.")


def validate_cron_expr(value: str) -> None:
    expr = (value or "").strip()
    if not expr:
        raise ValidationError("Cron expression is required.")
    try:
        from croniter import croniter
        if not croniter.is_valid(expr):
            raise ValidationError("Invalid cron expression.")
        return
    except ImportError:
        pass
    except Exception as exc:
        raise ValidationError("Invalid cron expression.") from exc

    try:
        _parse_cron_expr(expr)
    except Exception as exc:
        raise ValidationError("Invalid cron expression.") from exc


def validate_timezone_name(value: str) -> None:
    name = (value or "").strip()
    if not name:
        raise ValidationError("Timezone is required.")
    try:
        ZoneInfo(name)
    except Exception as exc:  # pragma: no cover - defensive
        raise ValidationError("Invalid timezone name.") from exc


def format_relative_time(value: datetime | None, *, now: datetime | None = None) -> str:
    if value is None:
        return "-"

    if timezone.is_naive(value):
        value = timezone.make_aware(value, timezone.get_current_timezone())

    current = now or timezone.now()
    delta = current - value
    total_seconds = int(delta.total_seconds())

    if total_seconds < 0:
        future_seconds = abs(total_seconds)
        if future_seconds < 60:
            return "in under a minute"
        if future_seconds < 3600:
            minutes = max(1, future_seconds // 60)
            return f"in {minutes} minute{'s' if minutes != 1 else ''}"
        hours = max(1, future_seconds // 3600)
        return f"in {hours} hour{'s' if hours != 1 else ''}"

    if total_seconds < 60:
        return "just now"
    if total_seconds < 3600:
        minutes = max(1, total_seconds // 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    if total_seconds < 86400:
        hours = max(1, total_seconds // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"

    days = delta.days
    if days == 1:
        return "yesterday"
    return f"{days} days ago"


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
    scheduled_by = models.ForeignKey(
        "RunSchedule",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="scheduled_jobs",
    )
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
        indexes = [
            models.Index(fields=["status", "-created_at"]),
            models.Index(fields=["company_key", "-created_at"]),
            models.Index(
                fields=["scheduled_by", "status", "-created_at"],
                name="epos_qbo_rj_sched_status_idx",
            ),
        ]

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
    reconcile_difference = models.DecimalField(
        max_digits=19, decimal_places=4, null=True, blank=True
    )
    reconcile_epos_total = models.DecimalField(
        max_digits=19, decimal_places=4, null=True, blank=True
    )
    reconcile_qbo_total = models.DecimalField(
        max_digits=19, decimal_places=4, null=True, blank=True
    )
    reconcile_epos_count = models.IntegerField(null=True, blank=True)
    reconcile_qbo_count = models.IntegerField(null=True, blank=True)
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
        indexes = [
            models.Index(fields=["company_key", "-processed_at"]),
            models.Index(fields=["company_key", "target_date"]),
            models.Index(fields=["company_key", "reconcile_status"]),
        ]


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


class RunSchedule(models.Model):
    TARGET_DATE_MODE_TRADING_DATE = "trading_date"
    TARGET_DATE_MODE_CHOICES = [
        (TARGET_DATE_MODE_TRADING_DATE, "Target Trading Date"),
    ]

    LAST_RESULT_QUEUED = "queued"
    LAST_RESULT_SKIPPED_OVERLAP = "skipped_overlap"
    LAST_RESULT_SKIPPED_INVALID = "skipped_invalid"
    LAST_RESULT_ERROR = "error"
    LAST_RESULT_CHOICES = [
        (LAST_RESULT_QUEUED, "Queued"),
        (LAST_RESULT_SKIPPED_OVERLAP, "Skipped (Overlap)"),
        (LAST_RESULT_SKIPPED_INVALID, "Skipped (Invalid)"),
        (LAST_RESULT_ERROR, "Error"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=120)
    enabled = models.BooleanField(default=True)
    scope = models.CharField(max_length=32, choices=RunJob.SCOPE_CHOICES, default=RunJob.SCOPE_ALL)
    company_key = models.SlugField(max_length=64, null=True, blank=True)
    cron_expr = models.CharField(max_length=120)
    timezone_name = models.CharField(max_length=64, default="UTC")
    target_date_mode = models.CharField(
        max_length=32,
        choices=TARGET_DATE_MODE_CHOICES,
        default=TARGET_DATE_MODE_TRADING_DATE,
    )
    parallel = models.PositiveSmallIntegerField(default=2)
    stagger_seconds = models.PositiveSmallIntegerField(default=2)
    continue_on_failure = models.BooleanField(default=False)
    next_fire_at = models.DateTimeField(null=True, blank=True)
    last_fired_at = models.DateTimeField(null=True, blank=True)
    last_result = models.CharField(max_length=32, choices=LAST_RESULT_CHOICES, blank=True)
    last_error = models.TextField(blank=True)
    is_system_managed = models.BooleanField(default=False)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="created_run_schedules",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="updated_run_schedules",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        permissions = [
            ("can_manage_schedules", "Can manage run schedules"),
        ]
        ordering = ["name", "created_at"]
        indexes = [
            models.Index(fields=["enabled", "next_fire_at"], name="epos_qbo_rs_enabled_next_idx"),
            models.Index(fields=["is_system_managed", "enabled"], name="epos_qbo_rs_system_enabled_idx"),
            models.Index(fields=["scope", "company_key"], name="epos_qbo_rs_scope_company_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.cron_expr})"

    @property
    def last_fired_relative(self) -> str:
        return format_relative_time(self.last_fired_at)

    def clean(self) -> None:
        errors: dict[str, str] = {}
        try:
            validate_cron_expr(self.cron_expr)
        except ValidationError:
            errors["cron_expr"] = "Enter a valid cron expression."
        try:
            validate_timezone_name(self.timezone_name)
        except ValidationError:
            errors["timezone_name"] = "Enter a valid timezone."

        if self.scope == RunJob.SCOPE_SINGLE and not (self.company_key or "").strip():
            errors["company_key"] = "Company key is required for single-company schedules."
        if self.scope == RunJob.SCOPE_ALL:
            self.company_key = None
        if self.scope == RunJob.SCOPE_SINGLE:
            self.parallel = 1
            self.continue_on_failure = False

        if errors:
            raise ValidationError(errors)

    def compute_next_fire_at(self, *, from_dt: datetime | None = None) -> datetime:
        validate_cron_expr(self.cron_expr)
        validate_timezone_name(self.timezone_name)

        tz = ZoneInfo(self.timezone_name)
        base = from_dt or timezone.now()
        if timezone.is_naive(base):
            base = timezone.make_aware(base, dt_timezone.utc)

        try:
            from croniter import croniter

            local_base = base.astimezone(tz)
            next_local = croniter(self.cron_expr, local_base).get_next(datetime)
            if next_local.tzinfo is None:
                next_local = next_local.replace(tzinfo=tz)
        except ImportError:
            next_local = _next_fire_fallback(self.cron_expr, from_dt=base, tz=tz)
        return next_local.astimezone(dt_timezone.utc)


class RunScheduleEvent(models.Model):
    TYPE_QUEUED = "queued"
    TYPE_SKIPPED_OVERLAP = "skipped_overlap"
    TYPE_SKIPPED_INVALID = "skipped_invalid"
    TYPE_ERROR = "error"
    TYPE_FALLBACK_ENABLED = "fallback_enabled"
    TYPE_FALLBACK_DISABLED = "fallback_disabled"
    TYPE_RUN_SUCCEEDED = "run_succeeded"
    TYPE_RUN_FAILED = "run_failed"
    EVENT_TYPE_CHOICES = [
        (TYPE_QUEUED, "Queued"),
        (TYPE_SKIPPED_OVERLAP, "Skipped Overlap"),
        (TYPE_SKIPPED_INVALID, "Skipped Invalid"),
        (TYPE_ERROR, "Error"),
        (TYPE_FALLBACK_ENABLED, "Fallback Enabled"),
        (TYPE_FALLBACK_DISABLED, "Fallback Disabled"),
        (TYPE_RUN_SUCCEEDED, "Run Succeeded"),
        (TYPE_RUN_FAILED, "Run Failed"),
    ]

    schedule = models.ForeignKey(
        RunSchedule,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="events",
    )
    run_job = models.ForeignKey(
        RunJob,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="schedule_events",
    )
    event_type = models.CharField(max_length=32, choices=EVENT_TYPE_CHOICES)
    message = models.TextField(blank=True)
    payload_json = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["event_type", "-created_at"], name="epos_qbo_rse_type_created_idx"),
            models.Index(fields=["schedule", "-created_at"], name="epos_qbo_rse_sched_created_idx"),
        ]

    @property
    def resolved_schedule_name(self) -> str:
        if self.schedule is not None:
            return self.schedule.name
        if isinstance(self.payload_json, dict):
            payload_name = self.payload_json.get("schedule_name")
            if payload_name:
                return str(payload_name)
            payload_scope = self.payload_json.get("scope")
            if payload_scope == RunJob.SCOPE_ALL:
                return "All companies (legacy)"
            if payload_scope == RunJob.SCOPE_SINGLE:
                payload_company = (self.payload_json.get("company_key") or "").strip()
                if payload_company:
                    return f"{payload_company} (legacy)"
                return "Single company (legacy)"
        if self.run_job is not None and self.run_job.scheduled_by is not None:
            return self.run_job.scheduled_by.name
        return "-"

    def __str__(self) -> str:
        return f"{self.event_type} @ {self.created_at.isoformat() if self.created_at else '-'}"


class SchedulerWorkerHeartbeat(models.Model):
    """Single-row heartbeat updated by the schedule worker each poll cycle. Used by the Schedules page to show whether the scheduler service is running."""

    id = models.PositiveIntegerField(primary_key=True)  # Singleton row: worker uses id=1
    last_seen = models.DateTimeField()

    class Meta:
        verbose_name = "Scheduler worker heartbeat"
        verbose_name_plural = "Scheduler worker heartbeats"


class PortalSettings(models.Model):
    """Singleton (id=1) for dashboard defaults. Null means use env/settings; non-null overrides."""

    id = models.PositiveIntegerField(primary_key=True)  # Singleton: id=1
    default_parallel = models.PositiveSmallIntegerField(null=True, blank=True)
    default_stagger_seconds = models.PositiveSmallIntegerField(null=True, blank=True)
    stale_hours_warning = models.PositiveIntegerField(null=True, blank=True)
    refresh_expiring_days = models.PositiveIntegerField(null=True, blank=True)
    reconcile_diff_warning = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True
    )
    reauth_guidance = models.TextField(null=True, blank=True)
    dashboard_timezone = models.CharField(max_length=64, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="updated_portal_settings",
    )

    class Meta:
        permissions = [
            ("can_manage_portal_settings", "Can manage portal settings"),
        ]
        verbose_name = "Portal settings"
        verbose_name_plural = "Portal settings"

    def save(self, *args, **kwargs):
        result = super().save(*args, **kwargs)
        from . import portal_settings  # local import avoids circular import at module load

        portal_settings.invalidate_cache()
        return result

    def delete(self, *args, **kwargs):
        result = super().delete(*args, **kwargs)
        from . import portal_settings  # local import avoids circular import at module load

        portal_settings.invalidate_cache()
        return result


class DashboardUserPreference(models.Model):
    """Per-user dashboard preferences (default Overview company and revenue period)."""

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="dashboard_preference",
    )
    default_overview_company_key = models.CharField(max_length=64, null=True, blank=True)
    default_revenue_period = models.CharField(max_length=16, default="7d")
    notify_on_run_failure = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Dashboard user preference"
        verbose_name_plural = "Dashboard user preferences"
