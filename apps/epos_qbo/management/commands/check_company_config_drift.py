from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.epos_qbo.services.config_sync import check_drift


class Command(BaseCommand):
    help = "Check drift between DB config payloads and JSON files."

    def handle(self, *args, **options):
        drifts = check_drift()
        if not drifts:
            self.stdout.write(self.style.SUCCESS("No company config drift detected."))
            return
        for line in drifts:
            self.stdout.write(self.style.WARNING(line))
        raise SystemExit(1)
