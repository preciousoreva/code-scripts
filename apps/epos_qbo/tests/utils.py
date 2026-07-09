from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager


@contextmanager
def suppress_expected_request_logs(
    *,
    extra_loggers: Iterable[str] = (),
    level: int = logging.CRITICAL + 1,
) -> Iterator[None]:
    """
    Suppress expected noisy logs for negative-path tests only.

    This is intentionally *scoped* (per `with` block) and *targeted* (specific loggers),
    so production logging behavior and the rest of the test suite remain unchanged.
    """

    logger_names = [
        # Django's request logger emits WARNING/ERROR for 4xx/5xx responses.
        "django.request",
        # App-level logger(s) that may log expected validation / warning messages.
        "apps.epos_qbo",
        "apps.epos_qbo.services.artifact_ingestion",
        "apps.epos_qbo.services.metrics",
        *list(extra_loggers),
    ]

    previous: dict[str, int] = {}
    for name in logger_names:
        logger = logging.getLogger(name)
        previous[name] = logger.level
        logger.setLevel(level)

    try:
        yield
    finally:
        for name, old_level in previous.items():
            logging.getLogger(name).setLevel(old_level)
