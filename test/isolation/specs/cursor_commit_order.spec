# cursor_commit_order: cursor allocation order must equal commit order.
#
# s1 allocates cursor 1 and holds the feed row lock while uncommitted.
# s2's statement MUST block on that lock: it cannot commit (or even finish
# allocating) before s1 ends. If nextval()-style allocation were used, s2
# could commit first and a reader advancing past cursor 2 would lose event 1
# when s1 later commits.
#
# Permutation 1 (commit/commit): both events visible, cursor order == commit order.
# Permutation 2 (abort/commit): s1's uncommitted cursor-1 allocation is rolled
# back and RECYCLED by s2 (allocation lives in the writer's transaction, so
# there are no gaps, unlike nextval). The reader must see ONLY cursor 1 =
# s2's event: no phantom from the aborted txn, nothing lost. If
# nextval()-style allocation were used, s2 would get cursor 2 and a reader
# advancing past 2 would wait forever / or s1's late commit would reorder.

setup
{
    DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
    DROP TABLE IF EXISTS co CASCADE;
    CREATE EXTENSION pg_replayfeed;
    CREATE TABLE co (id bigint PRIMARY KEY, t text NOT NULL, v integer);
    SELECT replayfeed.create_feed('co', 'co', 'id', 't', '1 hour');
}

teardown
{
    DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
    DROP TABLE IF EXISTS co CASCADE;
}

session s1
setup		{ BEGIN; }
step w1		{ INSERT INTO co VALUES (1, 'a', 1); }
step c1		{ COMMIT; }
step a1		{ ROLLBACK; }

session s2
setup		{ BEGIN; }
step w2		{ INSERT INTO co VALUES (2, 'a', 2); }
step c2		{ COMMIT; }

session s3
step read1	{ SELECT cursor, entity_key, operation FROM replayfeed.events_after('co', 'a', 0, 10) ORDER BY cursor; }
step read2	{ SELECT cursor, entity_key FROM replayfeed.events_after('co', 'a', 2, 10); }

permutation w1 w2 c1 c2 read1 read2
permutation w1 w2 a1 c2 read1 read2
