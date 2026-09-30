/* pg_replayfeed--1.1--2.0.sql -- breaking upgrade to the WAL-native design.
 *
 * 2.0 turns pg_replayfeed into a logical-decoding output plugin: the event
 * table, the polling functions and the write-path triggers are gone.
 * Changes now stream out of the binary WAL through logical replication
 * slots, keyed by commit LSN.
 *
 * What this script does:
 *   1. Drops the per-feed triggers from every source table (best effort;
 *      missing triggers are ignored so a half-cleaned table cannot fail
 *      the upgrade).
 *   2. Drops all 1.x functions, the event table, the old feed catalog and
 *      the operation enum.
 *   3. Recreates the 2.0 catalog (replayfeed.feed) and the create_feed /
 *      drop_feed functions.
 *
 * What it does NOT do -- read carefully:
 *   - Event history is NOT migrated. The 1.x bigint cursors have no meaning
 *     in the LSN-keyed 2.0 stream. After upgrading, recreate replication
 *     slots and resnapshot every consumer from the source tables.
 *   - Feed registrations are NOT preserved: re-run create_feed() for each
 *     feed (it now also sets REPLICA IDENTITY FULL on the source table).
 *   - The server must have wal_level = logical for the new streaming to
 *     work; that is a postgresql.conf change plus a restart, outside the
 *     scope of this script.
 */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_replayfeed UPDATE" to load this file. \quit

-- ---------------------------------------------------------------------------
-- 1. Drop the write-path triggers from every registered source table.
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT feed_id, source_table FROM replayfeed.feed LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s',
                       'rf_INSERT_' || r.feed_id, r.source_table::text);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s',
                       'rf_UPDATE_' || r.feed_id, r.source_table::text);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s',
                       'rf_DELETE_' || r.feed_id, r.source_table::text);
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s',
                       'rf_notruncate_' || r.feed_id, r.source_table::text);
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- 2. Drop the 1.x functions.
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS replayfeed.create_feed(text, regclass, name, name, interval);
DROP FUNCTION IF EXISTS replayfeed.drop_feed(text);
DROP FUNCTION IF EXISTS replayfeed.fence(text);
DROP FUNCTION IF EXISTS replayfeed.events_after(text, text, bigint, integer);
DROP FUNCTION IF EXISTS replayfeed.prune(text, integer);
DROP FUNCTION IF EXISTS replayfeed.prune_all(integer);
DROP FUNCTION IF EXISTS replayfeed.trigger_fn();
DROP FUNCTION IF EXISTS replayfeed.truncate_guard();
DROP FUNCTION IF EXISTS replayfeed.stats();

-- ---------------------------------------------------------------------------
-- 3. Drop the 1.x catalog.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS replayfeed.event;
DROP TABLE IF EXISTS replayfeed.feed;
DROP TYPE IF EXISTS replayfeed.operation;

-- ---------------------------------------------------------------------------
-- 4. Recreate the 2.0 catalog and functions (same definitions as
--    pg_replayfeed--2.0.sql).
-- ---------------------------------------------------------------------------

CREATE TABLE replayfeed.feed (
    feed_id                integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                   text NOT NULL UNIQUE,
    source_table           regclass NOT NULL UNIQUE,
    key_column             name NOT NULL,
    tenant_column          name,
    created_at             timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT feed_name_not_empty CHECK (name <> '')
);

COMMENT ON TABLE replayfeed.feed IS
'One row per registered feed. The change stream itself lives in WAL and is read through logical replication slots (output plugin pg_replayfeed), so this table holds configuration only. Changing key/tenant/source requires drop + recreate so stream semantics can never be invalidated by a config edit.';

CREATE FUNCTION replayfeed.create_feed(
    name text,
    source_table regclass,
    key_column name,
    tenant_column name DEFAULT NULL
)
RETURNS integer
LANGUAGE C
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_create_feed';

COMMENT ON FUNCTION replayfeed.create_feed(text, regclass, name, name) IS
'Register a feed on a table. Caller needs TRIGGER privilege on the source table. Validates: ordinary logged table, key column NOT NULL with a single-column unique index, tenant column exists. Sets REPLICA IDENTITY FULL on the source table (needed to key DELETEs from WAL). One feed per table.';

CREATE FUNCTION replayfeed.drop_feed(name text)
RETURNS void
LANGUAGE C STRICT
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_drop_feed';

COMMENT ON FUNCTION replayfeed.drop_feed(text) IS
'Remove a feed: deletes the catalog row. Existing replication slots stop emitting for the table; consumers keep their positions.';

REVOKE ALL ON FUNCTION replayfeed.create_feed(text, regclass, name, name) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.drop_feed(text) FROM PUBLIC;

REVOKE ALL ON TABLE replayfeed.feed FROM PUBLIC;
