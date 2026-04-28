from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("epos_qbo", "0011_inventory_pipeline_scope"),
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
                ],
                default="all_companies",
                max_length=32,
            ),
        ),
        migrations.AlterField(
            model_name="runartifact",
            name="kind",
            field=models.CharField(
                choices=[
                    ("sales_upload", "Sales Upload"),
                    ("inventory_audit", "Inventory"),
                ],
                default="sales_upload",
                max_length=32,
            ),
        ),
    ]
