-- slot: replication-slot lifecycle for the change stream. In 2.0 there is no
-- event table and no pruning: retention IS the slot. Unconsumed changes
-- accumulate; consuming advances the slot; peek does not consume.
--
-- pg_regress runs every suite in one database: reset to a clean slate even
-- if the previous suite died midway.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'rf_slot';
DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
DROP TABLE IF EXISTS sl CASCADE;
CREATE EXTENSION pg_replayfeed;
SET client_min_messages = warning;

CREATE TABLE sl (id bigint PRIMARY KEY, t text NOT NULL, v int);
SELECT replayfeed.create_feed('sl', 'sl', 'id', 't');
SELECT slot_name FROM pg_create_logical_replication_slot('rf_slot', 'pg_replayfeed');

-- two transactions pile up unconsumed
INSERT INTO sl VALUES (1, 'a', 10);
INSERT INTO sl VALUES (2, 'a', 20);
SELECT count(*) FROM pg_logical_slot_peek_changes('rf_slot', NULL, NULL);

-- get consumes: the changes are gone afterwards
SELECT count(*) FROM pg_logical_slot_get_changes('rf_slot', NULL, NULL);
SELECT count(*) FROM pg_logical_slot_get_changes('rf_slot', NULL, NULL);

-- peek does not consume
INSERT INTO sl VALUES (3, 'a', 30);
SELECT count(*) FROM pg_logical_slot_peek_changes('rf_slot', NULL, NULL);
SELECT count(*) FROM pg_logical_slot_peek_changes('rf_slot', NULL, NULL);
SELECT count(*) FROM pg_logical_slot_get_changes('rf_slot', NULL, NULL);

-- consuming advances the slot's restart_lsn: WAL before it can be recycled
SELECT slot_name, active, slot_type FROM pg_replication_slots WHERE slot_name = 'rf_slot';

-- a second slot is independent: same changes, own position
SELECT slot_name FROM pg_create_logical_replication_slot('rf_slot2', 'pg_replayfeed');
INSERT INTO sl VALUES (4, 'a', 40);
SELECT count(*) FROM pg_logical_slot_get_changes('rf_slot', NULL, NULL);
SELECT count(*) FROM pg_logical_slot_get_changes('rf_slot2', NULL, NULL);
SELECT count(*) FROM pg_logical_slot_get_changes('rf_slot2', NULL, NULL);

-- cleanup
SELECT pg_drop_replication_slot('rf_slot');
SELECT pg_drop_replication_slot('rf_slot2');
SELECT replayfeed.drop_feed('sl');
DROP TABLE sl;
