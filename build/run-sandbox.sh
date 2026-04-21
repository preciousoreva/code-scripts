#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

export OIAT_ENV_FILE="${OIAT_ENV_FILE:-.oiat/env/marvin-dev.env}"

if [ ! -f "$OIAT_ENV_FILE" ]; then
  echo "Sandbox env file not found: $OIAT_ENV_FILE"
  echo "Create it first with: ./build/init-dev-profile.sh marvin-dev"
  exit 1
fi

exec ./build/run-local.sh "$@"
