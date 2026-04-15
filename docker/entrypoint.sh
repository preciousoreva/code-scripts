#!/bin/sh
set -eu

APP_ROOT="/app"
STATE_ROOT="${STATE_ROOT:-/data}"

mkdir -p \
  "${STATE_ROOT}" \
  "${STATE_ROOT}/runtime" \
  "${STATE_ROOT}/code_scripts" \
  "${STATE_ROOT}/code_scripts/Uploaded" \
  "${STATE_ROOT}/code_scripts/uploads" \
  "${STATE_ROOT}/code_scripts/uploads/range_raw" \
  "${STATE_ROOT}/code_scripts/uploads/spill_raw" \
  "${STATE_ROOT}/code_scripts/logs" \
  "${STATE_ROOT}/code_scripts/logs/runs" \
  "${STATE_ROOT}/code_scripts/reports" \
  "${STATE_ROOT}/code_scripts/outputs"

touch "${STATE_ROOT}/db.sqlite3"
touch "${STATE_ROOT}/code_scripts/qbo_tokens.sqlite"

link_path() {
  src="$1"
  dst="$2"

  mkdir -p "$(dirname "${dst}")"
  rm -rf "${dst}"
  ln -s "${src}" "${dst}"
}

link_path "${STATE_ROOT}/db.sqlite3" "${APP_ROOT}/db.sqlite3"
link_path "${STATE_ROOT}/runtime" "${APP_ROOT}/runtime"
link_path "${STATE_ROOT}/code_scripts/qbo_tokens.sqlite" "${APP_ROOT}/code_scripts/qbo_tokens.sqlite"
link_path "${STATE_ROOT}/code_scripts/Uploaded" "${APP_ROOT}/code_scripts/Uploaded"
link_path "${STATE_ROOT}/code_scripts/uploads" "${APP_ROOT}/code_scripts/uploads"
link_path "${STATE_ROOT}/code_scripts/logs" "${APP_ROOT}/code_scripts/logs"
link_path "${STATE_ROOT}/code_scripts/reports" "${APP_ROOT}/code_scripts/reports"
link_path "${STATE_ROOT}/code_scripts/outputs" "${APP_ROOT}/code_scripts/outputs"

exec "$@"
