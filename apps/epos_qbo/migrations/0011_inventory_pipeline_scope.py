from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("epos_qbo", "0010_inventory_sync_scope_and_artifact_kind"),
    ]

    operations = [
        migrations.AlterField(
            model_name="runjob",
            name="scope",
            field=models.CharField(
                choices=[
                    ("single_company", "Single Company"),
                    ("all_companies", "All Companies"),
                    ("inventory_pipeline", "Inventory"),
                    ("inventory_sync", "Inventory Sync"),
                    ("inventory_catalog_cleanup", "Catalog Cleanup"),
                ],
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="runschedule",
            name="scope",
            field=models.CharField(
                choices=[
                    ("single_company", "Single Company"),
                    ("all_companies", "All Companies"),
                    ("inventory_pipeline", "Inventory"),
                    ("inventory_sync", "Inventory Sync"),
                    ("inventory_catalog_cleanup", "Catalog Cleanup"),
                ],
                default="all_companies",
                max_length=32,
            ),
        ),
    ]
