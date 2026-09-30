#!/bin/bash
# test/upgrade/test_upgrade.sh -- 1.1 -> 2.0 breaking upgrade.
# The 2.0 migration is deliberately destructive: triggers, the event table,
# and the 1.x functions are dropped; history and feed registrations are NOT
# preserved. The 1.1 C code is gone, so this reconstructs a representative
# 1.1 schema, runs the migration script directly, and verifies the cleanup
# plus a working 2.0.
# Expects PGHOST/PGPORT/PGUSER/PGDATABASE to point at a live server with the
# extension's .so installed (called from test/run_tests.sh).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

psql -v ON_ERROR_STOP=1 <<'EOF'
-- run_tests.sh runs pg_regress in this same database first: reset cleanly.
DROP SCHEMA IF EXISTS replayfeed CASCADE;
DROP TABLE IF EXISTS upg CASCADE;

-- Reconstruct the 1.1 catalog shape the migration expects: the feed table
-- plus a source table carrying the per-feed triggers.
CREATE SCHEMA replayfeed;
CREATE TABLE replayfeed.feed (
    feed_id       serial PRIMARY KEY,
    name          text NOT NULL UNIQUE,
    source_table  regclass NOT NULL UNIQUE,
    key_column    name NOT NULL,
    tenant_column name,
    retention     interval NOT NULL
);
CREATE TABLE upg (id bigint PRIMARY KEY, t text NOT NULL, v int);
INSERT INTO replayfeed.feed (name, source_table, key_column, tenant_column, retention)
VALUES ('upg', 'upg', 'id', 't', '1 hour');
CREATE FUNCTION replayfeed.trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER "rf_INSERT_1" AFTER INSERT ON upg FOR EACH ROW EXECUTE FUNCTION replayfeed.trigger_fn();
CREATE TRIGGER "rf_UPDATE_1" AFTER UPDATE ON upg FOR EACH ROW EXECUTE FUNCTION replayfeed.trigger_fn();
CREATE TRIGGER "rf_DELETE_1" AFTER DELETE ON upg FOR EACH ROW EXECUTE FUNCTION replayfeed.trigger_fn();
CREATE TABLE replayfeed.event (feed_id int, cursor bigint, entity_key text, operation text);
INSERT INTO replayfeed.event VALUES (1, 1, '1', 'INSERT');
CREATE FUNCTION replayfeed.events_after(text, text, bigint, integer) RETURNS void LANGUAGE sql AS $$ SELECT NULL $$;

-- pre-upgrade: triggers and event history exist
SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'rf\_%';
SELECT count(*) FROM replayfeed.event;
EOF

# Run the 1.1 -> 2.0 migration script directly (it guards against psql
# sourcing with \echo...\quit, which we strip; ALTER EXTENSION would run the
# same file). MODULE_PATHNAME is substituted by ALTER EXTENSION normally;
_SO="$(pg_config --pkglibdir)/pg_replayfeed"
if [ -f "$_SO.dylib" ]; then
  _SO="$_SO.dylib"
else
  _SO="$_SO.so"
fi
grep -v '^\\echo\|^\\quit' "$ROOT/sql/pg_replayfeed--1.1--2.0.sql" \
  | sed "s|MODULE_PATHNAME|$_SO|g" \
  | psql -v ON_ERROR_STOP=1 -q

psql -v ON_ERROR_STOP=1 <<'EOF'
-- post-upgrade: triggers gone, event table gone, old functions gone
SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'rf\_%';
SELECT to_regclass('replayfeed.event');
SELECT count(*) FROM pg_proc WHERE pronamespace = 'replayfeed'::regnamespace AND proname = 'events_after';

-- 2.0 API works: re-register the feed and stream from the WAL
SELECT replayfeed.create_feed('upg', 'upg', 'id', 't');
SELECT slot_name FROM pg_create_logical_replication_slot('rf_upg', 'pg_replayfeed');
INSERT INTO upg VALUES (1, 'a', 10);
SELECT count(*) FROM pg_logical_slot_get_changes('rf_upg', NULL, NULL);

-- cleanup
SELECT pg_drop_replication_slot('rf_upg');
SELECT replayfeed.drop_feed('upg');
DROP TABLE upg;
DROP SCHEMA replayfeed CASCADE;
EOF

echo "upgrade 1.1 -> 2.0 OK"
