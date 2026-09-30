-- datatypes: key column types, tenant types, JSON escaping, NULL handling.
--
-- pg_replayfeed 2.0 streams key-only changes. Keys and tenants are rendered
-- as text; this suite verifies every supported key type, JSON escaping of
-- special characters, and NULL/missing value behavior.
--
-- pg_regress runs every suite in one database: reset to a clean slate even
-- if the previous suite died midway.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'rf_datatypes';
DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
DROP TABLE IF EXISTS t_bigint, t_int, t_text, t_varchar, t_uuid, t_numeric CASCADE;
CREATE EXTENSION pg_replayfeed;
SET client_min_messages = warning;

CREATE FUNCTION stream_changes(slot text)
RETURNS TABLE (changes json, truncate json)
LANGUAGE sql AS $$
  SELECT d->'changes', d->'truncate'
    FROM (SELECT data::json AS d
            FROM pg_logical_slot_get_changes(slot, NULL, NULL)) s;
$$;

-- bigint key (the common case)
CREATE TABLE t_bigint (id bigint PRIMARY KEY, tenant text NOT NULL, v text);
SELECT replayfeed.create_feed('f_bigint', 't_bigint', 'id', 'tenant');
SELECT slot_name FROM pg_create_logical_replication_slot('rf_datatypes', 'pg_replayfeed');
INSERT INTO t_bigint VALUES (9223372036854775807, 'acme', 'max'), (-9223372036854775808, 'acme', 'min'), (0, 'acme', 'zero');
SELECT * FROM stream_changes('rf_datatypes');

-- integer key
CREATE TABLE t_int (id integer PRIMARY KEY, tenant text, v text);
SELECT replayfeed.create_feed('f_int', 't_int', 'id', 'tenant');
INSERT INTO t_int VALUES (1, 't1', 'a'), (2147483647, NULL, 'b'), (-2147483648, 't1', 'c');
SELECT * FROM stream_changes('rf_datatypes');

-- text key with JSON-special characters: quotes, backslash, newline, tab
CREATE TABLE t_text (id text PRIMARY KEY, tenant text, v text);
SELECT replayfeed.create_feed('f_text', 't_text', 'id', NULL);
INSERT INTO t_text VALUES ('say "hi"', 't1', 'q'), ('back\slash', 't1', 'b'), ('line' || chr(10) || 'break', 't1', 'n'), ('tab' || chr(9) || 'sep', 't1', 't');
SELECT * FROM stream_changes('rf_datatypes');

-- varchar key
CREATE TABLE t_varchar (id varchar(50) PRIMARY KEY, tenant text, v text);
SELECT replayfeed.create_feed('f_varchar', 't_varchar', 'id', 'tenant');
INSERT INTO t_varchar VALUES ('abc', 't1', 'x'), ('', 't1', 'empty-key');
SELECT * FROM stream_changes('rf_datatypes');

-- uuid key
CREATE TABLE t_uuid (id uuid PRIMARY KEY, tenant text, v text);
SELECT replayfeed.create_feed('f_uuid', 't_uuid', 'id', 'tenant');
INSERT INTO t_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 't1', 'u1'), ('00000000-0000-0000-0000-000000000000', 't1', 'u2');
SELECT * FROM stream_changes('rf_datatypes');

-- numeric key
CREATE TABLE t_numeric (id numeric PRIMARY KEY, tenant text, v text);
SELECT replayfeed.create_feed('f_numeric', 't_numeric', 'id', 'tenant');
INSERT INTO t_numeric VALUES (123.456, 't1', 'n1'), (-0.001, 't1', 'n2'), (1e10, 't1', 'n3');
SELECT * FROM stream_changes('rf_datatypes');

-- tenant as bigint (non-text tenant column)
CREATE TABLE t_tenant_bigint (id bigint PRIMARY KEY, tenant_id bigint, v text);
SELECT replayfeed.create_feed('f_tenant_bigint', 't_tenant_bigint', 'id', 'tenant_id');
INSERT INTO t_tenant_bigint VALUES (1, 999, 'a'), (2, NULL, 'b');
SELECT * FROM stream_changes('rf_datatypes');

-- unicode in key and tenant passes through unescaped (valid UTF-8)
INSERT INTO t_text VALUES ('caf' || chr(233), 't' || chr(233) || 'nant', 'u');
SELECT * FROM stream_changes('rf_datatypes');

-- control characters below 0x20 use \u00XX escapes
INSERT INTO t_text VALUES ('bell' || chr(7), 't1', 'c'), ('null' || chr(0), 't1', 'z');
SELECT * FROM stream_changes('rf_datatypes');

-- UPDATE changing only non-key columns still emits (key unchanged)
UPDATE t_bigint SET v = 'changed' WHERE id = 0;
SELECT * FROM stream_changes('rf_datatypes');

-- DELETE emits tombstone with key
DELETE FROM t_bigint WHERE id = 0;
SELECT * FROM stream_changes('rf_datatypes');

-- cleanup
SELECT pg_drop_replication_slot('rf_datatypes');
DROP FUNCTION stream_changes(text);
DROP TABLE t_bigint, t_int, t_text, t_varchar, t_uuid, t_numeric, t_tenant_bigint CASCADE;
