/* pg_replayfeed--2.0.sql -- WAL-native feed catalog.
 *
 * 2.0 is a breaking redesign: pg_replayfeed is now a logical-decoding
 * output plugin. Changes stream straight out of the binary WAL through the
 * replication protocol; there is no event table, no polling function, and
 * no trigger on the write path.
 *
 * Consumers:
 *   SELECT pg_create_logical_replication_slot('my_slot', 'pg_replayfeed');
 *   -- then START_REPLICATION SLOT my_slot LOGICAL <lsn>
 *   -- (or SELECT * FROM pg_logical_slot_get_changes('my_slot', NULL, NULL);)
 *
 * Each committed transaction that touched a feed table produces one JSON
 * message; see the header comment of src/replayfeed.h for the wire format.
 * The cursor is the commit LSN ("0/16B1978"); delivery is at-least-once.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_replayfeed" to load this file. \quit

-- The extension always lives in the `replayfeed` schema (see
-- pg_replayfeed.control: schema = 'replayfeed', relocatable = false), which
-- CREATE EXTENSION creates. All C code references objects as replayfeed.*.

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

-- ---------------------------------------------------------------------------
-- C functions
-- ---------------------------------------------------------------------------

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

-- ---------------------------------------------------------------------------
-- Default-deny privilege model. The DBA grants EXECUTE to the roles that
-- administer feeds. End users are never granted these directly. Replication
-- connections additionally need the LOGIN role to have REPLICATION privilege
-- (or be superuser) to create and read logical slots.
-- ---------------------------------------------------------------------------

REVOKE ALL ON FUNCTION replayfeed.create_feed(text, regclass, name, name) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.drop_feed(text) FROM PUBLIC;

REVOKE ALL ON TABLE replayfeed.feed FROM PUBLIC;
