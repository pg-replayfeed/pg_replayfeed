-- txn: commit/rollback coupling, one envelope per transaction, cursor
-- (commit LSN) semantics. Uncommitted work never reaches the stream; each
-- committed transaction yields exactly one envelope.
--
-- pg_regress runs every suite in one database: reset to a clean slate even
-- if the previous suite died midway.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'rf_txn';
DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
DROP TABLE IF EXISTS items CASCADE;
CREATE EXTENSION pg_replayfeed;
SET client_min_messages = warning;

CREATE TABLE items (
    id     bigint PRIMARY KEY,
    tenant text NOT NULL,
    v      integer NOT NULL
);
SELECT replayfeed.create_feed('items', 'items', 'id', 'tenant');
SELECT slot_name FROM pg_create_logical_replication_slot('rf_txn', 'pg_replayfeed');

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

-- committed insert is visible: one envelope, one change
BEGIN;
INSERT INTO items VALUES (1, 'a', 10);
COMMIT;
SELECT * FROM stream_changes('rf_txn');

-- rolled-back insert vanishes entirely: no envelope, nothing retained
BEGIN;
INSERT INTO items VALUES (2, 'a', 20);
ROLLBACK;
SELECT count(*) FROM stream_changes('rf_txn');

-- next committed write streams normally after the rollback
INSERT INTO items VALUES (3, 'a', 30);
SELECT * FROM stream_changes('rf_txn');

-- multi-statement transaction: ONE envelope, changes in commit order
BEGIN;
INSERT INTO items VALUES (4, 'a', 40);
UPDATE items SET v = 41 WHERE id = 4;
DELETE FROM items WHERE id = 1;
COMMIT;
SELECT * FROM stream_changes('rf_txn');

-- multi-row statement: one envelope, one change per row
INSERT INTO items VALUES (5, 'a', 50), (6, 'a', 60), (7, 'b', 70);
SELECT * FROM stream_changes('rf_txn');

-- statements touching no rows emit nothing
UPDATE items SET v = v WHERE false;
DELETE FROM items WHERE false;
SELECT count(*) FROM stream_changes('rf_txn');

-- rolled-back update leaves no trace
BEGIN;
UPDATE items SET v = 999 WHERE id = 5;
ROLLBACK;
SELECT count(*) FROM stream_changes('rf_txn');

-- transaction touching only other tables emits nothing
CREATE TABLE other (x int);
INSERT INTO other VALUES (1);
SELECT count(*) FROM stream_changes('rf_txn');
DROP TABLE other;

-- cursors are distinct commit LSNs, one per envelope
BEGIN;
INSERT INTO items VALUES (8, 'a', 80);
COMMIT;
BEGIN;
INSERT INTO items VALUES (9, 'a', 90);
COMMIT;
SELECT count(*), count(DISTINCT d->>'cursor')
  FROM (SELECT data::json AS d
          FROM pg_logical_slot_get_changes('rf_txn', NULL, NULL)) s;

-- SAVEPOINT rollback: only the surviving part streams
BEGIN;
INSERT INTO items VALUES (10, 'a', 100);
SAVEPOINT sp1;
INSERT INTO items VALUES (11, 'a', 110);
ROLLBACK TO SAVEPOINT sp1;
COMMIT;
SELECT * FROM stream_changes('rf_txn');

-- full rollback to savepoint then commit of earlier work
BEGIN;
INSERT INTO items VALUES (12, 'a', 120);
SAVEPOINT sp2;
UPDATE items SET v = 121 WHERE id = 12;
ROLLBACK TO SAVEPOINT sp2;
COMMIT;
SELECT * FROM stream_changes('rf_txn');

-- mixed feed + non-feed tables: only feed changes in envelope
CREATE TABLE other2 (x int);
BEGIN;
INSERT INTO items VALUES (13, 'a', 130);
INSERT INTO other2 VALUES (1), (2), (3);
COMMIT;
SELECT * FROM stream_changes('rf_txn');
DROP TABLE other2;

-- INSERT then DELETE same row in one txn: both events, net-zero row
BEGIN;
INSERT INTO items VALUES (14, 'a', 140);
DELETE FROM items WHERE id = 14;
COMMIT;
SELECT * FROM stream_changes('rf_txn');

-- UPDATE key twice in one txn: D(old)+I(mid), D(mid)+I(new)
BEGIN;
INSERT INTO items VALUES (15, 'a', 150);
COMMIT;
SELECT count(*) FROM stream_changes('rf_txn');
BEGIN;
UPDATE items SET id = 150 WHERE id = 15;
UPDATE items SET id = 1500 WHERE id = 150;
COMMIT;
SELECT * FROM stream_changes('rf_txn');

-- cursors strictly increase across envelopes
SELECT d->>'cursor' AS c1, lead(d->>'cursor') OVER () AS c2,
       (d->>'cursor') < (lead(d->>'cursor') OVER ()) AS increasing
  FROM (SELECT data::json AS d
          FROM pg_logical_slot_get_changes('rf_txn', NULL, NULL)) s
 LIMIT 5;

-- cleanup
SELECT replayfeed.drop_feed('items');
SELECT pg_drop_replication_slot('rf_txn');
DROP FUNCTION stream_changes(text);
DROP TABLE items;
