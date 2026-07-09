from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.epos_qbo.models import CompanyConfigRecord
from apps.epos_qbo.services.config_sync import sync_record_to_json


class Command(BaseCommand):
    help = "Write DB company config records to JSON files."

    def add_arguments(self, parser):
        parser.add_argument("--company", type=str, default="")

    def handle(self, *args, **options):
        company = (options.get("company") or "").strip()
        qs = CompanyConfigRecord.objects.all()
        if company:
            qs = qs.filter(company_key=company)
        count = 0
        for record in qs:
            sync_record_to_json(record)
            count += 1
        self.stdout.write(self.style.SUCCESS(f"Synced {count} company config record(s) to JSON."))
