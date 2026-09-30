-- ddl: schema changes on feed tables.
--
-- Feeds track tables by OID. This suite verifies behavior when the table
-- schema evolves: adding columns, dropping non-key columns, and renaming.
-- Dropped key columns fail closed (ERROR, stream stops rather than silently
-- diverging).
--
-- pg_regress runs every suite in one database: reset to a clean slate even
-- if the previous suite died midway.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'rf_ddl';
DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
DROP TABLE IF EXISTS t_ddl CASCADE;
CREATE EXTENSION pg_replayfeed;
SET client_min_messages = warning;
-- Terse verbosity: ERROR output shows only the message, not the volatile
-- DETAIL/CONTEXT (which embeds the commit LSN).
\set VERBOSITY terse

CREATE FUNCTION stream_changes(slot text)
RETURNS TABLE (changes json, truncate json)
LANGUAGE sql AS $$
  SELECT d->'changes', d->'truncate'
    FROM (SELECT data::json AS d
            FROM pg_logical_slot_get_changes(slot, NULL, NULL)) s;
$$;

CREATE TABLE t_ddl (id bigint PRIMARY KEY, tenant text NOT NULL, v text);
SELECT replayfeed.create_feed('f_ddl', 't_ddl', 'id', 'tenant');
SELECT slot_name FROM pg_create_logical_replication_slot('rf_ddl', 'pg_replayfeed');

-- baseline
INSERT INTO t_ddl VALUES (1, 'acme', 'a');
SELECT * FROM stream_changes('rf_ddl');

-- ADD COLUMN: stream continues, new column ignored (key-only)
ALTER TABLE t_ddl ADD COLUMN extra text DEFAULT 'dflt';
INSERT INTO t_ddl VALUES (2, 'acme', 'b', 'e2');
SELECT * FROM stream_changes('rf_ddl');

-- UPDATE touching only the new column still emits (row changed)
UPDATE t_ddl SET extra = 'e3' WHERE id = 2;
SELECT * FROM stream_changes('rf_ddl');

-- DROP non-key column: stream continues
ALTER TABLE t_ddl DROP COLUMN v;
INSERT INTO t_ddl VALUES (3, 'acme', 'e3');
SELECT * FROM stream_changes('rf_ddl');

-- RENAME key column: feed tracks by column name, breaks until re-registered
ALTER TABLE t_ddl RENAME COLUMN id TO order_id;
INSERT INTO t_ddl VALUES (4, 'acme', 'e4');
SELECT * FROM stream_changes('rf_ddl');

-- re-register under the new key column name
SELECT replayfeed.drop_feed('f_ddl');
SELECT replayfeed.create_feed('f_ddl2', 't_ddl', 'order_id', 'tenant');
INSERT INTO t_ddl VALUES (5, 'acme', 'e5');
SELECT * FROM stream_changes('rf_ddl');

-- DROP the key column entirely: fail closed (ERROR, stream stops rather
-- than silently diverging)
ALTER TABLE t_ddl DROP COLUMN order_id;
INSERT INTO t_ddl VALUES ('acme', 'e6');
SELECT * FROM stream_changes('rf_ddl');

-- The slot is stuck on the failed change: even TRUNCATE cannot be decoded
-- until the feed definition is fixed. Fail-closed means the stream stops
-- here, not that it skips ahead.
TRUNCATE t_ddl;
SELECT * FROM stream_changes('rf_ddl');

-- cleanup
SELECT pg_drop_replication_slot('rf_ddl');
DROP FUNCTION stream_changes(text);
DROP TABLE t_ddl CASCADE;
