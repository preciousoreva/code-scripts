from __future__ import annotations

import time

from django.core.management.base import BaseCommand

from apps.epos_qbo.services.schedule_worker import configured_poll_seconds, process_schedule_cycle


class Command(BaseCommand):
    help = "Run DB-backed schedule worker that enqueues RunJob records from RunSchedule."

    def add_arguments(self, parser):
        parser.add_argument(
            "--poll-seconds",
            type=int,
            default=None,
            help="Worker polling interval in seconds (default from OIAT_SCHEDULER_POLL_SECONDS or 15).",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Process one worker cycle and exit.",
        )

    def handle(self, *args, **options):
        poll_seconds = options["poll_seconds"] or configured_poll_seconds()
        if poll_seconds < 1:
            poll_seconds = 1

        once = bool(options["once"])
        self.stdout.write(
            self.style.SUCCESS(
                f"Schedule worker started (poll_seconds={poll_seconds}, once={once})."
            )
        )

        while True:
            stats = process_schedule_cycle()
            if any(
                stats.get(k, 0) > 0
                for k in [
                    "initialized",
                    "due",
                    "queued",
                    "skipped_overlap",
                    "skipped_invalid",
                    "errors",
                    "fallback_enabled",
                    "fallback_disabled",
                ]
            ):
                self.stdout.write(
                    (
                        "cycle "
                        f"initialized={stats['initialized']} "
                        f"due={stats['due']} "
                        f"queued={stats['queued']} "
                        f"skipped_overlap={stats['skipped_overlap']} "
                        f"skipped_invalid={stats['skipped_invalid']} "
                        f"errors={stats['errors']} "
                        f"fallback_enabled={stats['fallback_enabled']} "
                        f"fallback_disabled={stats['fallback_disabled']}"
                    )
                )

            if once:
                break
            time.sleep(poll_seconds)

        self.stdout.write(self.style.SUCCESS("Schedule worker stopped."))
