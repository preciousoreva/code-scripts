import apps.websites.models
import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Website",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("slug", models.SlugField(max_length=80, unique=True)),
                ("name", models.CharField(max_length=255)),
                ("domain", models.CharField(max_length=255, unique=True)),
                (
                    "platform",
                    models.CharField(
                        choices=[
                            ("wix", "Wix Studio"),
                            ("wordpress", "WordPress"),
                            ("static", "Static"),
                            ("other", "Other"),
                        ],
                        default="wix",
                        max_length=32,
                    ),
                ),
                ("public_url", models.URLField(blank=True)),
                (
                    "log_ingest_secret",
                    models.CharField(default=apps.websites.models.generate_ingest_secret, max_length=96, unique=True),
                ),
                ("is_active", models.BooleanField(default=True)),
                ("notes", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["name"],
                "permissions": [("can_manage_websites", "Can manage website monitoring")],
            },
        ),
        migrations.CreateModel(
            name="WebsiteLogEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("received_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("occurred_at", models.DateTimeField(blank=True, null=True)),
                (
                    "severity",
                    models.CharField(
                        choices=[
                            ("debug", "Debug"),
                            ("info", "Info"),
                            ("warning", "Warning"),
                            ("error", "Error"),
                            ("critical", "Critical"),
                            ("unknown", "Unknown"),
                        ],
                        default="unknown",
                        max_length=16,
                    ),
                ),
                ("source", models.CharField(blank=True, max_length=255)),
                ("event_type", models.CharField(blank=True, max_length=255)),
                ("message", models.TextField(blank=True)),
                ("request_id", models.CharField(blank=True, max_length=255)),
                ("trace_id", models.CharField(blank=True, max_length=255)),
                ("pathname", models.CharField(blank=True, max_length=512)),
                ("function_name", models.CharField(blank=True, max_length=255)),
                ("remote_addr", models.GenericIPAddressField(blank=True, null=True)),
                ("user_agent", models.TextField(blank=True)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("request_headers", models.JSONField(blank=True, default=dict)),
                (
                    "website",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="log_events",
                        to="websites.website",
                    ),
                ),
            ],
            options={
                "ordering": ["-received_at", "-id"],
                "indexes": [
                    models.Index(fields=["website", "-received_at"], name="websites_we_website_a08df6_idx"),
                    models.Index(fields=["website", "severity", "-received_at"], name="websites_we_website_39ab27_idx"),
                    models.Index(fields=["website", "event_type", "-received_at"], name="websites_we_website_801ed5_idx"),
                    models.Index(fields=["request_id"], name="websites_we_request_94d8e4_idx"),
                ],
            },
        ),
    ]
