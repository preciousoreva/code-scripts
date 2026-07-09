from __future__ import annotations

from django.core.management.base import BaseCommand

from apps.epos_qbo.services.artifact_ingestion import ingest_history


class Command(BaseCommand):
    help = "Ingest run history from Uploaded metadata files."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=60)

    def handle(self, *args, **options):
        days = options["days"]
        count = ingest_history(days=days)
        self.stdout.write(self.style.SUCCESS(f"Imported {count} artifact record(s) from last {days} day(s)."))
