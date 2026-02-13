from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from apps.epos_qbo.services.config_sync import import_all_company_json


class Command(BaseCommand):
    help = "Import company configs from code_scripts/companies JSON into DB"

    def handle(self, *args, **options):
        try:
            imported = import_all_company_json(strict=True)
        except Exception as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(self.style.SUCCESS(f"Imported/updated {len(imported)} company config record(s)."))
