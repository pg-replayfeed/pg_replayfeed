/* pg_replayfeed--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_replayfeed" to load this file. \quit

-- The extension always lives in the `replayfeed` schema (see
-- pg_replayfeed.control: schema = 'replayfeed', relocatable = false), which
-- CREATE EXTENSION creates. All C code references objects as replayfeed.*.

CREATE TYPE replayfeed.operation AS ENUM ('INSERT', 'UPDATE', 'DELETE');

CREATE TABLE replayfeed.feed (
    feed_id                integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name                   text NOT NULL UNIQUE,
    source_table           regclass NOT NULL UNIQUE,
    key_column             name NOT NULL,
    tenant_column          name,
    retention              interval NOT NULL DEFAULT interval '24 hours',
    latest_cursor          bigint NOT NULL DEFAULT 0,
    oldest_retained_cursor bigint NOT NULL DEFAULT 1,
    event_count            bigint NOT NULL DEFAULT 0,
    trigger_calls          bigint NOT NULL DEFAULT 0,
    last_pruned_at         timestamptz,
    created_at             timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT feed_config_immutable_note CHECK (true)
);

COMMENT ON TABLE replayfeed.feed IS
'One row per registered feed. Configuration columns (name, source_table, key_column, tenant_column, retention) have no UPDATE API: changing key/tenant/source requires drop + recreate so cursor semantics can never be invalidated by a config edit.';

CREATE TABLE replayfeed.event (
    feed_id      integer NOT NULL REFERENCES replayfeed.feed ON DELETE CASCADE,
    cursor       bigint NOT NULL,
    tenant_key   text,
    entity_key   text NOT NULL,
    operation    replayfeed.operation NOT NULL,
    committed_at timestamptz NOT NULL,
    PRIMARY KEY (feed_id, cursor)
);

COMMENT ON TABLE replayfeed.event IS
'Key-only replay log. Every event gets a unique, monotonically increasing cursor. A modifying statement allocates a contiguous cursor range (latest_cursor+1 .. latest_cursor+row_count) while holding the feed row lock, so cursor order equals commit order of transactions affecting the feed (see docs/architecture.md). Allocation and inserts happen in the writer transaction: rollback recycles the range, so committed cursors form a dense sequence outside retention.';

-- Hot path: tenant-scoped replay scans.
CREATE INDEX replayfeed_event_feed_tenant_cursor
    ON replayfeed.event (feed_id, tenant_key, cursor);
-- The PRIMARY KEY on (feed_id, cursor) serves feed-wide cursor range scans.

-- ---------------------------------------------------------------------------
-- C functions
-- ---------------------------------------------------------------------------

CREATE FUNCTION replayfeed.create_feed(
    name text,
    source_table regclass,
    key_column name,
    tenant_column name DEFAULT NULL,
    retention interval DEFAULT interval '24 hours'
)
RETURNS integer
LANGUAGE C
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_create_feed';

COMMENT ON FUNCTION replayfeed.create_feed(text, regclass, name, name, interval) IS
'Register a feed on a table. Caller needs TRIGGER privilege on the source table. Validates: ordinary logged table, key column NOT NULL with a single-column unique index, tenant column exists, positive retention. Installs statement-level triggers.';

CREATE FUNCTION replayfeed.drop_feed(name text)
RETURNS void
LANGUAGE C STRICT
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_drop_feed';

COMMENT ON FUNCTION replayfeed.drop_feed(text) IS
'Remove a feed: drops triggers, deletes the feed row (events cascade). Quiesce writers first; in-flight writers fail their current statement.';

CREATE FUNCTION replayfeed.fence(feed_name text)
RETURNS bigint
LANGUAGE C STRICT STABLE
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_fence';

COMMENT ON FUNCTION replayfeed.fence(text) IS
'Snapshot boundary. Call as the FIRST statement of a REPEATABLE READ transaction, then read the source table in the same transaction, then replay events_after() with cursors greater than the fenced value. See docs/architecture.md section 5.';

CREATE FUNCTION replayfeed.events_after(
    feed_name text,
    tenant text,
    after_cursor bigint,
    batch_limit integer DEFAULT 500
)
RETURNS TABLE (
    cursor bigint,
    entity_key text,
    operation text,
    committed_at timestamptz
)
LANGUAGE C STABLE
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_events_after';

COMMENT ON FUNCTION replayfeed.events_after(text, text, bigint, integer) IS
'Replay retained events for a tenant with cursor > after_cursor, ascending cursor order, at most batch_limit rows (clamped to 10000). Returns a single (NULL, NULL, ''RESNAPSHOT_REQUIRED'', NULL) row when after_cursor fell outside the retained window. Cursors are feed-scoped, unique per event, and monotonically follow commit order.';

CREATE FUNCTION replayfeed.prune(feed_name text, batch_size integer DEFAULT 10000)
RETURNS bigint
LANGUAGE C
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_prune';

COMMENT ON FUNCTION replayfeed.prune(text, integer) IS
'Delete one bounded batch of events older than the feed retention interval and advance oldest_retained_cursor. Idempotent; schedule repeatedly (e.g. pg_cron) until it returns 0. Returns rows deleted by this call.';

CREATE FUNCTION replayfeed.trigger_fn()
RETURNS trigger
LANGUAGE C
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_trigger_fn';

CREATE FUNCTION replayfeed.truncate_guard()
RETURNS trigger
LANGUAGE C
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS 'MODULE_PATHNAME', 'replayfeed_truncate_guard';

-- ---------------------------------------------------------------------------
-- Observability
-- ---------------------------------------------------------------------------

CREATE FUNCTION replayfeed.stats()
RETURNS TABLE (
    feed_name text,
    latest_cursor bigint,
    oldest_retained_cursor bigint,
    event_count bigint,
    retention interval,
    oldest_event_age interval,
    pruning_lag interval,
    trigger_calls bigint,
    last_pruned_at timestamptz
)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = replayfeed, pg_catalog
AS $$
    SELECT f.name,
           f.latest_cursor,
           f.oldest_retained_cursor,
           f.event_count,
           f.retention,
           (SELECT now() - e.committed_at
              FROM replayfeed.event e
             WHERE e.feed_id = f.feed_id
             ORDER BY e.cursor
             LIMIT 1),
           (SELECT now() - e.committed_at - f.retention
              FROM replayfeed.event e
             WHERE e.feed_id = f.feed_id
             ORDER BY e.cursor
             LIMIT 1),
           f.trigger_calls,
           f.last_pruned_at
      FROM replayfeed.feed f
     ORDER BY f.feed_id;
$$;

COMMENT ON FUNCTION replayfeed.stats() IS
'Per-feed diagnostics: cursor watermarks, event counts, retention target, oldest-event age, pruning lag (positive = overdue), trigger invocations.';

-- ---------------------------------------------------------------------------
-- Default-deny privilege model. The DBA grants EXECUTE to a dedicated relay
-- role; end users are never granted these directly. Example:
--   GRANT EXECUTE ON FUNCTION replayfeed.events_after(text,text,bigint,integer) TO relay;
--   GRANT EXECUTE ON FUNCTION replayfeed.fence(text) TO relay;
-- ---------------------------------------------------------------------------

REVOKE ALL ON FUNCTION replayfeed.create_feed(text, regclass, name, name, interval) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.drop_feed(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.fence(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.events_after(text, text, bigint, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.prune(text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.trigger_fn() FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.truncate_guard() FROM PUBLIC;
REVOKE ALL ON FUNCTION replayfeed.stats() FROM PUBLIC;

REVOKE ALL ON TABLE replayfeed.feed FROM PUBLIC;
REVOKE ALL ON TABLE replayfeed.event FROM PUBLIC;
REVOKE ALL ON TYPE replayfeed.operation FROM PUBLIC;
