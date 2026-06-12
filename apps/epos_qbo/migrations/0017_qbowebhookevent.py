from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("epos_qbo", "0016_inventoryreviewacknowledgement"),
    ]

    operations = [
        migrations.CreateModel(
            name="QboWebhookEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("received_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("signature_valid", models.BooleanField(default=False)),
                ("realm_id", models.CharField(blank=True, max_length=64)),
                ("company_key", models.SlugField(blank=True, max_length=64)),
                ("company_display_name", models.CharField(blank=True, max_length=255)),
                ("entity_name", models.CharField(blank=True, max_length=64)),
                ("entity_id", models.CharField(blank=True, max_length=64)),
                ("operation", models.CharField(blank=True, max_length=32)),
                ("last_updated", models.CharField(blank=True, max_length=64)),
                ("is_test_event", models.BooleanField(default=False)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("received", "Received"),
                            ("sent", "Sent"),
                            ("skipped", "Skipped"),
                            ("failed", "Failed"),
                            ("rejected", "Rejected"),
                        ],
                        default="received",
                        max_length=16,
                    ),
                ),
                ("slack_webhook_configured", models.BooleanField(default=False)),
                ("slack_sent", models.BooleanField(default=False)),
                ("skip_reason", models.TextField(blank=True)),
                ("error_message", models.TextField(blank=True)),
                ("payload_json", models.JSONField(blank=True, default=dict)),
            ],
            options={
                "ordering": ["-received_at", "-id"],
            },
        ),
        migrations.AddIndex(
            model_name="qbowebhookevent",
            index=models.Index(fields=["-received_at"], name="epos_qbo_qb_receive_4c8581_idx"),
        ),
        migrations.AddIndex(
            model_name="qbowebhookevent",
            index=models.Index(fields=["realm_id", "-received_at"], name="epos_qbo_qb_realm_i_79ede4_idx"),
        ),
        migrations.AddIndex(
            model_name="qbowebhookevent",
            index=models.Index(fields=["company_key", "-received_at"], name="epos_qbo_qb_company_4e1890_idx"),
        ),
        migrations.AddIndex(
            model_name="qbowebhookevent",
            index=models.Index(fields=["status", "-received_at"], name="epos_qbo_qb_status_7b550d_idx"),
        ),
    ]
