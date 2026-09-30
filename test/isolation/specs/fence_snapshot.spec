# fence_snapshot: the fence()/REPEATABLE READ protocol.
#
# Invariant under test: events visible in the reader's snapshot have
# cursor <= fence; events committed after the snapshot have cursor > fence
# and are all replayable, exactly once. The reader replays with cursor >
# the VALUE fence returned, so each permutation uses the matching replay step.
#
# Permutation 1: writer starts after the snapshot read. Snapshot sees only
#   id=1, fence=1, replay(cursor>1) returns the insert of id=2. No dup/miss.
# Permutation 2: writer in-flight across fence. The uncommitted insert is
#   invisible in the snapshot (still only id=1) but commits later with
#   cursor 2 > fence=1, so replay returns it. Nothing lost, nothing doubled.
# Permutation 3: writer commits before fence. Snapshot sees id=2, fence=2,
#   replay(cursor>2) is empty: already-seen state is NOT replayed again.
# Permutations 4/5: same shape with DELETE (tombstone): the delete is
#   invisible until commit, then replayed exactly once as a DELETE event.

setup
{
    DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
    DROP TABLE IF EXISTS fs CASCADE;
    CREATE EXTENSION pg_replayfeed;
    CREATE TABLE fs (id bigint PRIMARY KEY, tenant text NOT NULL, v integer);
    SELECT replayfeed.create_feed('fs', 'fs', 'id', 'tenant', '1 hour');
    INSERT INTO fs VALUES (1, 'a', 10);
}

teardown
{
    DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
    DROP TABLE IF EXISTS fs CASCADE;
}

session s1
setup		{ BEGIN ISOLATION LEVEL REPEATABLE READ; }
step fence	{ SELECT replayfeed.fence('fs') AS f; }
step snapread	{ SELECT id, v FROM fs WHERE tenant = 'a' ORDER BY id; }
step commitr	{ COMMIT; }

session s2
setup		{ BEGIN; }
step winst	{ INSERT INTO fs VALUES (2, 'a', 20); }
step cwin	{ COMMIT; }
step wdel	{ DELETE FROM fs WHERE id = 1; }
step cdel	{ COMMIT; }

session s3
step replay_after_1	{ SELECT cursor, entity_key, operation FROM replayfeed.events_after('fs', 'a', 1, 10) ORDER BY cursor; }
step replay_after_2	{ SELECT cursor, entity_key, operation FROM replayfeed.events_after('fs', 'a', 2, 10) ORDER BY cursor; }

permutation fence snapread winst cwin commitr replay_after_1
permutation winst fence snapread cwin commitr replay_after_1
permutation winst cwin fence snapread commitr replay_after_2
permutation wdel fence snapread cdel commitr replay_after_1
permutation fence snapread wdel cdel commitr replay_after_1
