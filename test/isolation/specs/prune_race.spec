# prune_race: concurrent prune and replay must never produce a silent gap.
#
# events_after() is a single statement (single snapshot): a replay either
# sees the pre-prune state (all rows) or the post-prune watermark (sentinel),
# never a partial silent window.

setup
{
    DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
    DROP TABLE IF EXISTS pr CASCADE;
    CREATE EXTENSION pg_replayfeed;
    CREATE TABLE pr (id bigint PRIMARY KEY, t text NOT NULL);
    SELECT replayfeed.create_feed('pr', 'pr', 'id', 't', '100 milliseconds');
    INSERT INTO pr SELECT g, 'a' FROM generate_series(1, 10) g;
    SELECT pg_sleep(0.3);
}

teardown
{
    DROP EXTENSION IF EXISTS pg_replayfeed CASCADE;
    DROP TABLE IF EXISTS pr CASCADE;
}

session s1
step prune	{ SELECT replayfeed.prune('pr', 100); }

session s2
step replay	{ SELECT cursor, entity_key, operation FROM replayfeed.events_after('pr', 'a', 0, 10) ORDER BY cursor; }

permutation prune replay
permutation replay prune
