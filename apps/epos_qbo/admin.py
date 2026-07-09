from django.contrib import admin

from .models import (
    CompanyConfigRecord,
    RunArtifact,
    RunJob,
    RunLock,
    RunSchedule,
    RunScheduleEvent,
)


@admin.register(CompanyConfigRecord)
class CompanyConfigRecordAdmin(admin.ModelAdmin):
    list_display = ("company_key", "display_name", "is_active", "config_version", "last_synced_to_json_at", "updated_at")
    search_fields = ("company_key", "display_name")


@admin.register(RunJob)
class RunJobAdmin(admin.ModelAdmin):
    list_display = ("id", "scope", "company_key", "status", "pid", "exit_code", "created_at", "finished_at")
    list_filter = ("scope", "status", "company_key")
    search_fields = ("company_key", "command_display")


@admin.register(RunArtifact)
class RunArtifactAdmin(admin.ModelAdmin):
    list_display = ("company_key", "target_date", "processed_at", "reliability_status", "rows_kept", "imported_at")
    list_filter = ("company_key", "reliability_status", "reconcile_status")
    search_fields = ("company_key", "source_path", "raw_file")


@admin.register(RunLock)
class RunLockAdmin(admin.ModelAdmin):
    list_display = ("id", "active", "holder", "owner_run_job", "acquired_at", "updated_at")


@admin.register(RunSchedule)
class RunScheduleAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "enabled",
        "scope",
        "company_key",
        "cron_expr",
        "timezone_name",
        "next_fire_at",
        "last_fired_at",
        "is_system_managed",
    )
    list_filter = ("enabled", "scope", "is_system_managed", "timezone_name")
    search_fields = ("name", "company_key", "cron_expr")


@admin.register(RunScheduleEvent)
class RunScheduleEventAdmin(admin.ModelAdmin):
    list_display = ("event_type", "schedule", "run_job", "created_at")
    list_filter = ("event_type",)
    search_fields = ("message",)
