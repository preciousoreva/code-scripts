#!/bin/sh
set -eu

APP_ROOT="/app"
STATE_ROOT="${STATE_ROOT:-/data}"
SEED_ROOT="${SEED_ROOT:-/seed}"

mkdir -p \
  "${STATE_ROOT}" \
  "${STATE_ROOT}/runtime" \
  "${STATE_ROOT}/code_scripts" \
  "${STATE_ROOT}/code_scripts/Uploaded" \
  "${STATE_ROOT}/code_scripts/uploads" \
  "${STATE_ROOT}/code_scripts/logs" \
  "${STATE_ROOT}/code_scripts/reports" \
  "${STATE_ROOT}/code_scripts/outputs"

touch "${STATE_ROOT}/db.sqlite3"
touch "${STATE_ROOT}/code_scripts/qbo_tokens.sqlite"

seed_file_if_empty() {
  dst="$1"
  src="$2"

  if [ -f "${src}" ] && [ -s "${src}" ] && [ ! -s "${dst}" ]; then
    cp "${src}" "${dst}"
  fi
}

dir_has_entries() {
  find "$1" -mindepth 1 -print -quit | grep -q .
}

seed_dir_if_empty() {
  dst="$1"
  src="$2"

  if [ -d "${src}" ] && ! dir_has_entries "${dst}"; then
    cp -a "${src}/." "${dst}/"
  fi
}

link_path() {
  src="$1"
  dst="$2"

  mkdir -p "$(dirname "${dst}")"
  rm -rf "${dst}"
  ln -s "${src}" "${dst}"
}

seed_file_if_empty "${STATE_ROOT}/db.sqlite3" "${SEED_ROOT}/db.sqlite3"
seed_file_if_empty "${STATE_ROOT}/code_scripts/qbo_tokens.sqlite" "${SEED_ROOT}/code_scripts/qbo_tokens.sqlite"
seed_dir_if_empty "${STATE_ROOT}/code_scripts/Uploaded" "${SEED_ROOT}/code_scripts/Uploaded"
seed_dir_if_empty "${STATE_ROOT}/code_scripts/uploads" "${SEED_ROOT}/code_scripts/uploads"
seed_dir_if_empty "${STATE_ROOT}/code_scripts/logs" "${SEED_ROOT}/code_scripts/logs"
seed_dir_if_empty "${STATE_ROOT}/code_scripts/reports" "${SEED_ROOT}/code_scripts/reports"
seed_dir_if_empty "${STATE_ROOT}/code_scripts/outputs" "${SEED_ROOT}/code_scripts/outputs"

mkdir -p \
  "${STATE_ROOT}/code_scripts/uploads/range_raw" \
  "${STATE_ROOT}/code_scripts/uploads/spill_raw" \
  "${STATE_ROOT}/code_scripts/logs/runs"

link_path "${STATE_ROOT}/db.sqlite3" "${APP_ROOT}/db.sqlite3"
link_path "${STATE_ROOT}/runtime" "${APP_ROOT}/runtime"
link_path "${STATE_ROOT}/code_scripts/qbo_tokens.sqlite" "${APP_ROOT}/code_scripts/qbo_tokens.sqlite"
link_path "${STATE_ROOT}/code_scripts/Uploaded" "${APP_ROOT}/code_scripts/Uploaded"
link_path "${STATE_ROOT}/code_scripts/uploads" "${APP_ROOT}/code_scripts/uploads"
link_path "${STATE_ROOT}/code_scripts/logs" "${APP_ROOT}/code_scripts/logs"
link_path "${STATE_ROOT}/code_scripts/reports" "${APP_ROOT}/code_scripts/reports"
link_path "${STATE_ROOT}/code_scripts/outputs" "${APP_ROOT}/code_scripts/outputs"

exec "$@"
