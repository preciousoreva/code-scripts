#!/bin/sh
# DEPRECATED: no longer used; Python wrapper is the active hook.
# This file is kept for reference but is not used by .pre-commit-config.yaml
# The active hook is: .pre-commit-hooks/gitleaks-wrapper.py (Python-only, cross-platform)

if command -v python3 >/dev/null 2>&1; then
    exec python3 .pre-commit-hooks/gitleaks-wrapper.py "$@"
elif command -v python >/dev/null 2>&1; then
    exec python .pre-commit-hooks/gitleaks-wrapper.py "$@"
else
    echo "Error: Neither python3 nor python found in PATH" >&2
    exit 1
fi

