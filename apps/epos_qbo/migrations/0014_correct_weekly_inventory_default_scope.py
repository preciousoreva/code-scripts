from django.db import migrations


OLD_VALIDATION_OPTIONS = {"categories": ["ALCOHOLS & SPIRITS"]}


def clear_validation_category_from_weekly_inventory(apps, schema_editor):
    RunSchedule = apps.get_model("epos_qbo", "RunSchedule")
    queryset = RunSchedule.objects.filter(
        name="Weekly Inventory Sync",
        is_system_managed=False,
        enabled=False,
        scope="inventory_pipeline",
        company_key="company_a",
        cron_expr="0 20 * * 0",
        timezone_name="Africa/Lagos",
    )
    for schedule in queryset:
        if schedule.inventory_options_json == OLD_VALIDATION_OPTIONS:
            schedule.inventory_options_json = {}
            schedule.save(update_fields=["inventory_options_json"])


class Migration(migrations.Migration):

    dependencies = [
        ("epos_qbo", "0013_inventory_schedule_support"),
    ]

    operations = [
        migrations.RunPython(clear_validation_category_from_weekly_inventory, migrations.RunPython.noop),
    ]
