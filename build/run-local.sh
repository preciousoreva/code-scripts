#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment
if [ ! -d ".venv" ]; then
  echo "No .venv found. Run: python -m venv .venv && pip install -r requirements.txt"
  exit 1
fi

source .venv/bin/activate

# Load .env if present
if [ -f ".env" ]; then
  set -a
  source .env
  set +a
fi

# Apply any pending migrations
python manage.py migrate --run-syncdb

# Start the dev server
python manage.py runserver "${1:-8000}"
