#!/bin/bash
# test/run_tests.sh -- full test suite for pg_replayfeed 2.0.
#
# Provisions a throwaway PostgreSQL instance and runs:
#   pg_regress suites and the 1.1 -> 2.0 breaking upgrade.
# Portable across Linux/macOS; every path is derived from pg_config.
# Run from the repo root after `make` + `make install` (or via `make test`).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/pg_throwaway.sh"

pg_throwaway_require_tools

# Locate pg_regress:
#   1. explicit env override: PG_REGRESS_BIN
#   2. next to the PGXS sources (full server-dev install)
#   3. source build under /tmp/postgresql-<server-version>/ (Homebrew's
#      postgresql@17 does not ship it; see README for the one-time build)
if [ -z "${PG_REGRESS_BIN:-}" ]; then
  PGXS_MK="$(pg_config --pgxs)"
  PGXS_SRC="$(cd "$(dirname "$PGXS_MK")/../.." && pwd)"
  if [ -x "$PGXS_SRC/test/regress/pg_regress" ]; then
    PG_REGRESS_BIN="$PGXS_SRC/test/regress/pg_regress"
  else
    _PG_VER="$(pg_config --version | awk '{print $2}')"
    for _cand in "/tmp/postgresql-${_PG_VER}/src/test/regress/pg_regress" \
                 /tmp/postgresql-*/src/test/regress/pg_regress; do
      if [ -x "$_cand" ]; then
        PG_REGRESS_BIN="$_cand"
        break
      fi
    done
    unset _cand _PG_VER
  fi
fi
[ -x "${PG_REGRESS_BIN:-}" ] || {
  echo "error: pg_regress not found or not executable." >&2
  echo "hint: one-time build from the matching PostgreSQL source -- see README." >&2
  echo "      Or export PG_REGRESS_BIN explicitly." >&2
  exit 1
}
export PATH="$(dirname "$PG_REGRESS_BIN"):$PATH"

# A source-built pg_regress links the shared libpq from its own source tree,
# whose recorded path may not exist on this machine (e.g. a --prefix the
# user never installed to). Make the test binaries resolvable.
if [ -n "${PG_REGRESS_BIN:-}" ]; then
  _IN_TREE_LIBPQ="$(cd "$(dirname "$PG_REGRESS_BIN")/../../interfaces/libpq" 2>/dev/null && pwd)" || true
  if [ -f "$_IN_TREE_LIBPQ/libpq.5.dylib" ] || [ -f "$_IN_TREE_LIBPQ/libpq.so.5" ]; then
    if [ "$(uname -s)" = "Darwin" ]; then
      # macOS: permanently point pg_regress at the in-tree libpq
      # (idempotent; skipped when the recorded path already exists).
      _FIX_LIB="$_IN_TREE_LIBPQ/libpq.5.dylib"
      if [ -f "$_FIX_LIB" ] && command -v otool >/dev/null 2>&1 \
          && command -v install_name_tool >/dev/null 2>&1; then
        _tbin="$PG_REGRESS_BIN"
        _ref="$(otool -L "$_tbin" 2>/dev/null | grep 'libpq\.' | awk '{print $1}' | head -n 1)"
        if [ -n "$_ref" ] && [ "$_ref" != "$_FIX_LIB" ] && [ ! -f "$_ref" ]; then
          if install_name_tool -change "$_ref" "$_FIX_LIB" "$_tbin" 2>/dev/null; then
            echo "run_tests: repointed libpq in $(basename "$_tbin") at in-tree build" >&2
          else
            echo "run_tests: WARNING: could not repoint libpq in $_tbin" >&2
            echo "run_tests:   manual fix: install_name_tool -change \"$_ref\" \"$_FIX_LIB\" \"$_tbin\"" >&2
          fi
        fi
        unset _tbin _ref
      fi
      unset _FIX_LIB
    fi
    # Loader-path export: enough for pg_regress itself (direct child) and on
    # Linux for everything; kept as a fallback on macOS too.
    case "$(uname -s)" in
      Darwin) export DYLD_LIBRARY_PATH="$_IN_TREE_LIBPQ${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
      *)      export LD_LIBRARY_PATH="$_IN_TREE_LIBPQ${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
    esac
  fi
  unset _IN_TREE_LIBPQ
fi

pg_throwaway_start /tmp/pg_replayfeed_test 55417
trap pg_throwaway_stop EXIT

rm -rf "$ROOT/test/output"

psql -d postgres -q -c "CREATE DATABASE tdb;"
export PGDATABASE=tdb

echo "=== pg_regress ==="
# --bindir: a source-built pg_regress defaults to /usr/local/pgsql/bin;
# pg_config --bindir is right on every install layout.
pg_regress --bindir="$(pg_config --bindir)" --inputdir="$ROOT/test" --outputdir="$ROOT/test/output" \
  --host="$PGHOST" --port="$PGPORT" --user=postgres \
  basic txn datatypes ddl retention security

echo "=== upgrade 1.1 -> 2.0 (breaking) ==="
"$ROOT/test/upgrade/test_upgrade.sh"

echo "ALL TESTS PASSED"
