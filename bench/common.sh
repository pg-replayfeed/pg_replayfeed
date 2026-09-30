#!/bin/bash
# bench/common.sh -- shared utilities for the benchmark suite.
set -euo pipefail

# Portable timeout helper (macOS coreutils fallback via perl)
_run_with_timeout() {
  local sec="$1"; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$sec" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$sec" "$@"
  elif command -v perl >/dev/null 2>&1; then
    perl -e 'alarm shift; exec @ARGV' "$sec" "$@"
  else
    "$@"
  fi
}

# q() runs SQL and prints one clean value.
q() {
  psql -q -X -t -A -c "$1"
}

# ms_now() returns high-resolution timestamp in milliseconds from PostgreSQL clock.
ms_now() {
  q "SELECT extract(epoch FROM clock_timestamp()) * 1000;"
}
