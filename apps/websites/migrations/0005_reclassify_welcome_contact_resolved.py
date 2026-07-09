from django.db import migrations
from django.db.models import Q


def reclassify_welcome_contact_resolved(apps, schema_editor):
    WebsiteLogEvent = apps.get_model("websites", "WebsiteLogEvent")
    WebsiteLogEvent.objects.filter(
        Q(message__contains='"contactId"')
        & Q(message__contains='"source":"appendOrCreateContact"'),
        severity="unknown",
        message__startswith="Welcome email contact resolved:",
    ).update(severity="info")


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("websites", "0004_reclassify_structured_outcome_severity"),
    ]

    operations = [
        migrations.RunPython(reclassify_welcome_contact_resolved, noop_reverse),
    ]
