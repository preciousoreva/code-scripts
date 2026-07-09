from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from code_scripts.load_env import load_env_file
from code_scripts.scheduler_config import load_scheduler_config, resolve_logging_level

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGGER = logging.getLogger("oiat.scheduler")


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=resolve_logging_level(level_name),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def _parse_run_cmd(run_cmd: str) -> list[str]:
    args = shlex.split(run_cmd, posix=(os.name != "nt"))
    if not args:
        raise ValueError("RUN_CMD produced no executable arguments")
    return args


def execute_run_command(run_cmd: str) -> int:
    args = _parse_run_cmd(run_cmd)
    LOGGER.info("Executing scheduled command: %s", run_cmd)

    proc = subprocess.Popen(
        args,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=dict(os.environ),
    )

    if proc.stdout is not None:
        for line in proc.stdout:
            LOGGER.info("[run_cmd] %s", line.rstrip("\n"))

    return_code = proc.wait()
    LOGGER.info("Scheduled command exited with code %s", return_code)
    return return_code


def run_scheduler() -> None:
    load_env_file()
    config = load_scheduler_config()
    configure_logging(config.log_level)

    tz = ZoneInfo(config.schedule_tz)
    scheduler = BlockingScheduler(timezone=tz)
    trigger = CronTrigger.from_crontab(config.schedule_cron, timezone=tz)

    def _scheduled_job() -> None:
        try:
            exit_code = execute_run_command(config.run_cmd)
            if exit_code != 0:
                LOGGER.error("Scheduled command failed with exit code %s", exit_code)
        except Exception:
            LOGGER.exception("Scheduled command crashed before completion")

    scheduler.add_job(
        _scheduled_job,
        trigger=trigger,
        id="scheduled_all_companies",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=config.misfire_grace_seconds,
        replace_existing=True,
    )

    LOGGER.info(
        "Scheduler started: cron='%s' tz='%s' cmd='%s'",
        config.schedule_cron,
        config.schedule_tz,
        config.run_cmd,
    )
    scheduler.start()


def main() -> int:
    try:
        run_scheduler()
    except Exception:
        LOGGER.exception("Scheduler failed to start")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
