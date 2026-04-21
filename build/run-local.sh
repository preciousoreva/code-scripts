#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

resolve_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$SCRIPT_DIR/$1" ;;
  esac
}

load_env_profile() {
  local env_path="$1"
  local raw_line line key value

  while IFS= read -r raw_line || [ -n "$raw_line" ]; do
    line="${raw_line%$'\r'}"

    case "$line" in
      ''|\#*) continue ;;
    esac

    case "$line" in
      *=*) ;;
      *) continue ;;
    esac

    key="${line%%=*}"
    value="${line#*=}"

    key="$(printf '%s' "$key" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
    value="$(printf '%s' "$value" | sed 's/^[[:space:]]*//')"

    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi

    if [ -n "$key" ]; then
      export "$key=$value"
    fi
  done < "$env_path"
}

# Load env profile if present
ENV_FILE="${OIAT_ENV_FILE:-.env}"
ENV_FILE_PATH="$(resolve_path "$ENV_FILE")"
if [ -f "$ENV_FILE_PATH" ]; then
  load_env_profile "$ENV_FILE_PATH"
fi

# Activate virtual environment
VENV_PATH="${OIAT_VENV_PATH:-.venv}"
VENV_PATH="$(resolve_path "$VENV_PATH")"

if [ ! -d "$VENV_PATH" ]; then
  echo "No virtual environment found at: $VENV_PATH"
  echo "Set OIAT_VENV_PATH or run: python -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

source "$VENV_PATH/bin/activate"

if [ -n "${STATE_ROOT:-}" ]; then
  mkdir -p "$(resolve_path "$STATE_ROOT")"
fi

if [ -n "${OIAT_COMPANIES_DIR:-}" ]; then
  mkdir -p "$(resolve_path "$OIAT_COMPANIES_DIR")"
fi

# Apply any pending migrations
python manage.py migrate --run-syncdb

# Start the dev server
RUNSERVER_ARGS="${OIAT_RUNSERVER_ARGS:---noreload}"
python manage.py runserver $RUNSERVER_ARGS "${1:-8000}"
