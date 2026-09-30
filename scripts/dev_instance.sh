#!/bin/bash
# scripts/dev_instance.sh -- throwaway dev database with pg_replayfeed installed.
#
# Drops you into psql with the extension ready. Exiting psql tears the
# instance down. Run via `make run` (builds + installs first).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/pg_throwaway.sh"

pg_throwaway_require_tools
pg_throwaway_start /tmp/pg_replayfeed_dev 55419
trap pg_throwaway_stop EXIT

export PGDATABASE=devdb
psql -q -X -d postgres -c "CREATE DATABASE devdb;"
psql -q -X -c "CREATE EXTENSION pg_replayfeed;"

cat <<'HELP'

  dev database ready (extension installed). Try:

    CREATE TABLE docs(id bigint PRIMARY KEY, tenant text);
    SELECT replayfeed.create_feed('docs', 'docs', 'id', 'tenant');
    INSERT INTO docs VALUES (1, 'acme'), (2, 'acme');
    -- then stream with a logical replication client:
    --   SELECT * FROM pg_create_logical_replication_slot('docs_slot', 'pg_replayfeed');
    --   START_REPLICATION SLOT docs_slot LOGICAL 0/0 (feed 'docs');

  Exit psql (Ctrl-D) to tear the instance down.
HELP

psql
