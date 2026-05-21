from django.db import migrations, models


def create_weekly_inventory_schedule(apps, schema_editor):
    RunSchedule = apps.get_model("epos_qbo", "RunSchedule")
    RunSchedule.objects.get_or_create(
        name="Weekly Inventory Sync",
        is_system_managed=False,
        defaults={
            "enabled": False,
            "scope": "inventory_pipeline",
            "company_key": "company_a",
            "cron_expr": "0 20 * * 0",
            "timezone_name": "Africa/Lagos",
            "inventory_options_json": {},
            "target_date_mode": "trading_date",
            "parallel": 1,
            "stagger_seconds": 2,
            "continue_on_failure": False,
            "next_fire_at": None,
        },
    )


class Migration(migrations.Migration):

    dependencies = [
        ("epos_qbo", "0012_remove_catalog_cleanup_scope"),
    ]

    operations = [
        migrations.AddField(
            model_name="runschedule",
            name="inventory_options_json",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AlterField(
            model_name="runschedule",
            name="last_result",
            field=models.CharField(
                blank=True,
                choices=[
                    ("queued", "Queued"),
                    ("succeeded", "Succeeded"),
                    ("failed", "Failed"),
                    ("cancelled", "Cancelled"),
                    ("skipped_overlap", "Skipped (Overlap)"),
                    ("skipped_invalid", "Skipped (Invalid)"),
                    ("error", "Error"),
                ],
                max_length=32,
            ),
        ),
        migrations.RunPython(create_weekly_inventory_schedule, migrations.RunPython.noop),
    ]
