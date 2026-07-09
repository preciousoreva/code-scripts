from django.db import migrations
from django.db.models import Q


def reclassify_success_checkpoints(apps, schema_editor):
    WebsiteLogEvent = apps.get_model("websites", "WebsiteLogEvent")
    WebsiteLogEvent.objects.filter(
        Q(message__contains='"primaryOk":true') & Q(message__contains='"membershipId"'),
        severity="unknown",
        message__startswith="Registration primary write complete:",
    ).update(severity="info")
    WebsiteLogEvent.objects.filter(
        Q(message__contains='"lookupType"') & Q(message__contains='"cardKey"'),
        severity="unknown",
        message__startswith="Membership card SVG served:",
    ).update(severity="info")


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("websites", "0005_reclassify_welcome_contact_resolved"),
    ]

    operations = [
        migrations.RunPython(reclassify_success_checkpoints, noop_reverse),
    ]
