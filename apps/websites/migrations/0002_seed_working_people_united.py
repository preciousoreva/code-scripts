from django.db import migrations


def seed_working_people_united(apps, schema_editor):
    Website = apps.get_model("websites", "Website")
    Website.objects.get_or_create(
        slug="working-people-united",
        defaults={
            "name": "Working People United",
            "domain": "workingpeopleunited.org",
            "platform": "wix",
            "public_url": "https://workingpeopleunited.org",
        },
    )


def remove_working_people_united(apps, schema_editor):
    Website = apps.get_model("websites", "Website")
    Website.objects.filter(slug="working-people-united", log_events__isnull=True).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("websites", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(seed_working_people_united, remove_working_people_united),
    ]
