-- basic: feed registration, WAL streaming of I/U/D, key change, tenant
-- move, TRUNCATE, drop_feed. pg_replayfeed 2.0 is a logical-decoding output
-- plugin: changes stream out of the WAL through replication slots.
--
-- The envelope carries a volatile commit LSN ("cursor") and wall-clock
-- "committed_at_ms", so tests project the deterministic parts: the change
-- and truncate arrays, plus boolean checks that cursor/timestamp are
-- well-formed.
--
-- pg_regress runs every suite in one database: reset to a clean slate even
-- if the previous suite died midway.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'rf_basic';
DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
DROP TABLE IF EXISTS orders, bad_nokey CASCADE;
CREATE EXTENSION pg_replayfeed;
SET client_min_messages = warning;

CREATE TABLE orders (
    order_id   bigint PRIMARY KEY,
    account_id bigint NOT NULL,
    amount     numeric NOT NULL,
    status     text NOT NULL DEFAULT 'new'
);

-- deterministic projection of the stream
CREATE FUNCTION stream_changes(slot text)
RETURNS TABLE (cursor_ok boolean, ts_ok boolean, changes json, truncate json)
LANGUAGE sql AS $$
  SELECT (d->>'cursor') ~ '^[0-9A-F]+/[0-9A-F]+$',
         (d->>'committed_at_ms')::bigint > 0,
         d->'changes',
         d->'truncate'
    FROM (SELECT data::json AS d
            FROM pg_logical_slot_get_changes(slot, NULL, NULL)) s;
$$;

-- registration rejections
CREATE TABLE bad_nokey (id bigint, v text);
SELECT replayfeed.create_feed('bad1', 'bad_nokey', 'id', NULL);
SELECT replayfeed.create_feed('bad2', 'orders', 'nope', NULL);
CREATE VIEW v_orders AS SELECT * FROM orders;
SELECT replayfeed.create_feed('bad3', 'v_orders', 'order_id', NULL);
SELECT replayfeed.create_feed('bad4', 'orders', 'order_id', 'order_id');
SELECT replayfeed.create_feed('bad5', 'orders', 'amount', NULL);

-- happy path
SELECT replayfeed.create_feed('orders', 'orders', 'order_id', 'account_id');

-- REPLICA IDENTITY FULL was set so old row images are logged
SELECT relreplident FROM pg_class WHERE relname = 'orders';

-- duplicate name / second feed on same table rejected
SELECT replayfeed.create_feed('orders', 'orders', 'order_id', 'account_id');

SELECT slot_name FROM pg_create_logical_replication_slot('rf_basic', 'pg_replayfeed');

-- multi-row INSERT: one envelope, ordered changes
INSERT INTO orders VALUES (1, 42, 100, 'new'), (2, 42, 200, 'new'), (3, 7, 50, 'new');
SELECT * FROM stream_changes('rf_basic');

-- UPDATE and DELETE
UPDATE orders SET status = 'paid' WHERE order_id = 1;
DELETE FROM orders WHERE order_id = 2;
SELECT * FROM stream_changes('rf_basic');

-- no-op UPDATE emits nothing
UPDATE orders SET status = 'paid' WHERE order_id = 1;
SELECT count(*) FROM stream_changes('rf_basic');

-- key change becomes DELETE(old) + INSERT(new)
UPDATE orders SET order_id = 100 WHERE order_id = 1;
SELECT * FROM stream_changes('rf_basic');

-- tenant move carries old_tenant
UPDATE orders SET account_id = 99 WHERE order_id = 3;
SELECT * FROM stream_changes('rf_basic');

-- TRUNCATE is a resnapshot marker, not row events
TRUNCATE orders;
SELECT * FROM stream_changes('rf_basic');

-- drop_feed: the table goes quiet
SELECT replayfeed.drop_feed('orders');
INSERT INTO orders VALUES (9, 1, 10, 'new');
SELECT count(*) FROM stream_changes('rf_basic');

-- cleanup
SELECT pg_drop_replication_slot('rf_basic');
DROP FUNCTION stream_changes(text);
DROP VIEW v_orders;
DROP TABLE orders;
DROP TABLE bad_nokey;
