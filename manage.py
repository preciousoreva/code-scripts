#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys

from code_scripts.load_env import load_env_file


def main() -> None:
    load_env_file()
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "oiat_portal.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
