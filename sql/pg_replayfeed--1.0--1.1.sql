/* pg_replayfeed--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_replayfeed UPDATE" to load this file. \quit

-- 1.1 adds a scheduler-friendly prune_all() wrapper. No existing data or
-- catalog shape is touched; feeds, events and cursor watermarks are preserved.

CREATE FUNCTION replayfeed.prune_all(batch_size integer DEFAULT 10000)
RETURNS TABLE (
    feed_name text,
    deleted bigint
)
LANGUAGE C
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_prune_all';

COMMENT ON FUNCTION replayfeed.prune_all(integer) IS
'Prune every feed once (one bounded batch each). Designed for pg_cron, e.g.: SELECT cron.schedule(''replayfeed-prune'', ''*/5 * * * *'', $$SELECT replayfeed.prune_all(10000)$$);';

REVOKE ALL ON FUNCTION replayfeed.prune_all(integer) FROM PUBLIC;
