from __future__ import annotations

import uuid

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="RunJob",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("scope", models.CharField(choices=[("single_company", "Single Company"), ("all_companies", "All Companies")], max_length=32)),
                ("company_key", models.SlugField(blank=True, max_length=64, null=True)),
                ("target_date", models.DateField(blank=True, null=True)),
                ("from_date", models.DateField(blank=True, null=True)),
                ("to_date", models.DateField(blank=True, null=True)),
                ("skip_download", models.BooleanField(default=False)),
                ("command_json", models.JSONField(default=list)),
                ("command_display", models.TextField(blank=True)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("queued", "Queued"),
                            ("running", "Running"),
                            ("succeeded", "Succeeded"),
                            ("failed", "Failed"),
                            ("cancelled", "Cancelled"),
                        ],
                        default="queued",
                        max_length=16,
                    ),
                ),
                ("pid", models.IntegerField(blank=True, null=True)),
                ("exit_code", models.IntegerField(blank=True, null=True)),
                ("log_file_path", models.TextField(blank=True)),
                ("failure_reason", models.TextField(blank=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "requested_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="requested_run_jobs",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
                "permissions": [("can_trigger_runs", "Can trigger pipeline runs")],
            },
        ),
        migrations.CreateModel(
            name="CompanyConfigRecord",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("company_key", models.SlugField(max_length=64, unique=True)),
                ("display_name", models.CharField(max_length=255)),
                ("config_json", models.JSONField(default=dict)),
                ("is_active", models.BooleanField(default=True)),
                ("config_version", models.PositiveIntegerField(default=1)),
                ("checksum", models.CharField(blank=True, max_length=64)),
                ("schema_version", models.CharField(default="1.0.0", max_length=32)),
                ("last_synced_to_json_at", models.DateTimeField(blank=True, null=True)),
                ("last_synced_from_json_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="created_company_configs",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "updated_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="updated_company_configs",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["company_key"],
                "permissions": [("can_edit_companies", "Can edit company configuration")],
            },
        ),
        migrations.CreateModel(
            name="RunArtifact",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("company_key", models.SlugField(max_length=64)),
                ("target_date", models.DateField(blank=True, null=True)),
                ("processed_at", models.DateTimeField(blank=True, null=True)),
                ("source_path", models.TextField()),
                ("source_hash", models.CharField(max_length=64)),
                ("reliability_status", models.CharField(choices=[("high", "High"), ("warning", "Warning")], default="warning", max_length=16)),
                ("rows_total", models.IntegerField(blank=True, null=True)),
                ("rows_kept", models.IntegerField(blank=True, null=True)),
                ("rows_non_target", models.IntegerField(blank=True, null=True)),
                ("upload_stats_json", models.JSONField(default=dict)),
                ("reconcile_status", models.CharField(blank=True, max_length=32)),
                ("reconcile_difference", models.FloatField(blank=True, null=True)),
                ("raw_file", models.CharField(blank=True, max_length=255)),
                ("processed_files_json", models.JSONField(default=list)),
                ("nearest_log_file", models.TextField(blank=True)),
                ("imported_at", models.DateTimeField(auto_now_add=True)),
                (
                    "run_job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="artifacts",
                        to="epos_qbo.runjob",
                    ),
                ),
            ],
            options={
                "ordering": ["-processed_at", "-imported_at"],
            },
        ),
        migrations.CreateModel(
            name="RunLock",
            fields=[
                ("id", models.PositiveSmallIntegerField(default=1, editable=False, primary_key=True, serialize=False)),
                ("active", models.BooleanField(default=False)),
                ("holder", models.CharField(blank=True, max_length=255)),
                ("acquired_at", models.DateTimeField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "owner_run_job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="owned_locks",
                        to="epos_qbo.runjob",
                    ),
                ),
            ],
        ),
        migrations.AddConstraint(
            model_name="runartifact",
            constraint=models.UniqueConstraint(
                fields=("company_key", "target_date", "processed_at", "source_hash"),
                name="uniq_run_artifact_source",
            ),
        ),
    ]
