#!/bin/bash
# scripts/pg_throwaway.sh -- shared helper: provision a throwaway PostgreSQL
# instance for tests, benchmarks, and dev shells. Source it; do not execute it.
#
# Usage:
#   source scripts/pg_throwaway.sh
#   pg_throwaway_require_tools          # pg_config, initdb, pg_ctl, psql in PATH
#   pg_throwaway_start /tmp/mydb 55499  # starts instance; exports PGHOST/PGPORT/PGUSER
#   ... psql ...
#   pg_throwaway_stop                   # stops postgres and removes the instance dir
#
# Portable across Linux and macOS. All paths come from pg_config.

# Check tools exist and put the server bindir on PATH.
pg_throwaway_require_tools() {
  command -v pg_config >/dev/null || {
    echo "error: pg_config not found in PATH." >&2
    echo "hint: install PostgreSQL (server + dev headers) and ensure its bin dir is on PATH." >&2
    echo "  macOS (Homebrew): brew install postgresql@17 && export PATH=\"\$(brew --prefix postgresql@17)/bin:\$PATH\"" >&2
    echo "  Ubuntu:           sudo apt install postgresql-server-dev-17 postgresql-17" >&2
    return 1
  }
  local bindir
  bindir="$(pg_config --bindir)"
  for t in initdb pg_ctl psql; do
    [ -x "$bindir/$t" ] || {
      echo "error: $t not found in $bindir (from pg_config --bindir)." >&2
      return 1
    }
  done
  export PATH="$bindir:$PATH"
}

# initdb refuses to run as root. On Linux boxes running as root, re-exec the
# command as the postgres OS user. Otherwise run it directly (normal case,
# including macOS).
_tw_run_as_pg() {
  if [ "$(id -u)" = "0" ]; then
    if ! id postgres >/dev/null 2>&1; then
      echo "error: running as root but no 'postgres' OS user exists." >&2
      echo "hint: create one (useradd postgres) or run as a non-root user." >&2
      return 1
    fi
    su -s /bin/bash postgres -c "$1"
  else
    bash -c "$1"
  fi
}

# Start a throwaway instance in $1 (work dir) on port $2 (unix socket only).
# Exports PG_TW_WORK/PG_TW_SOCK/PG_TW_PORT and sets PGHOST/PGPORT/PGUSER.
pg_throwaway_start() {
  local work="$1" port="$2"
  local sock="$work/sock"
  rm -rf "$work"
  mkdir -p "$sock"
  if [ "$(id -u)" = "0" ]; then
    chown -R postgres:postgres "$work"
  fi
  _tw_run_as_pg "export PATH=\"$PATH\"; initdb -D '$work/data' -U postgres --no-locale -E UTF8 -A trust" || {
    echo "error: initdb failed for throwaway instance in $work" >&2
    return 1
  }
  # pg_replayfeed 2.0 is a logical-decoding output plugin: every instance
  # provisioned here runs with the replication settings the extension needs.
  #
  # output_plugin_libraries (CVE-2026-6471 fix) allowlists which libraries may
  # act as output plugins. It exists only on patched servers (18.6+/17.11+/
  # 16.15+/15.19+/14.24+); older servers reject it as unrecognized, so we
  # probe and fall back.
  _tw_run_as_pg "export PATH=\"$PATH\"; cat >> '$work/data/postgresql.conf' <<'EOF'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
listen_addresses = ''
output_plugin_libraries = 'pg_replayfeed'
EOF" || return 1
  if ! _tw_run_as_pg "export PATH=\"$PATH\"; pg_ctl -D '$work/data' -o \"-k $sock -p $port\" -l '$work/log' start" 2>/dev/null; then
    if grep -q 'unrecognized configuration parameter "output_plugin_libraries"' "$work/log" 2>/dev/null; then
      # Server predates the CVE-2026-6471 allowlist: drop it and retry.
      _tw_run_as_pg "export PATH=\"$PATH\"; sed -i.bak '/^output_plugin_libraries/d' '$work/data/postgresql.conf'" || return 1
      if ! _tw_run_as_pg "export PATH=\"$PATH\"; pg_ctl -D '$work/data' -o \"-k $sock -p $port\" -l '$work/log' start"; then
        echo "error: pg_ctl could not start the throwaway instance in $work" >&2
        if [ -f "$work/log" ]; then
          echo "--- server log ($work/log) ---" >&2
          cat "$work/log" >&2
          echo "--- end server log ---" >&2
        fi
        return 1
      fi
    else
      echo "error: pg_ctl could not start the throwaway instance in $work" >&2
      if [ -f "$work/log" ]; then
        echo "--- server log ($work/log) ---" >&2
        cat "$work/log" >&2
        echo "--- end server log ---" >&2
      fi
      return 1
    fi
  fi
  export PG_TW_WORK="$work" PG_TW_SOCK="$sock" PG_TW_PORT="$port"
  export PGHOST="$sock" PGPORT="$port" PGUSER=postgres
}

# Stop the instance started by pg_throwaway_start and remove its directory.
pg_throwaway_stop() {
  [ -n "${PG_TW_WORK:-}" ] || return 0
  _tw_run_as_pg "export PATH=\"$PATH\"; pg_ctl -D '$PG_TW_WORK/data' stop -m fast >/dev/null 2>&1" || true
  rm -rf "$PG_TW_WORK"
  unset PG_TW_WORK PG_TW_SOCK PG_TW_PORT
}
