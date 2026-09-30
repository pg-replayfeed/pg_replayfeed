/*
 * pg_replayfeed -- retention pruning.
 *
 * prune(feed_name [, batch_size]) deletes events older than the feed's
 * retention interval in bounded batches and advances oldest_retained_cursor.
 * It is idempotent and safe to retry (designed for pg_cron). prune_all()
 * applies prune() to every feed.
 *
 * The watermark update takes the feed row lock only for a single UPDATE, so
 * writers are blocked for microseconds, not for the whole delete batch.
 */
#include "replayfeed.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(replayfeed_prune);
PG_FUNCTION_INFO_V1(replayfeed_prune_all);

/*
 * Retention cutoff: the smallest cursor still inside the retention window.
 * committed_at is non-decreasing in cursor order (cursors are allocated
 * under the writer-serialization lock), so every event with cursor < cutoff
 * is older than the retention interval. When no event is inside the window,
 * everything has expired: cutoff = latest_cursor + 1.
 */
static int64
rf_prune_cutoff(int32 feed_id)
{
	static const char *query =
		"SELECT COALESCE("
		"  (SELECT min(cursor) FROM replayfeed.event"
		"    WHERE feed_id = $1"
		"      AND committed_at >= now() - (SELECT retention FROM replayfeed.feed WHERE feed_id = $1)),"
		"  (SELECT latest_cursor FROM replayfeed.feed WHERE feed_id = $1) + 1"
		")";
	Oid			argtypes[1] = {INT4OID};
	Datum		values[1];
	SPIPlanPtr	plan;
	bool		isnull;
	int64		cutoff;

	plan = SPI_prepare(query, 1, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = Int32GetDatum(feed_id);
	if (SPI_execute_plan(plan, values, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: retention cutoff query failed")));
	cutoff = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										 SPI_tuptable->tupdesc, 1, &isnull));
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);

	return cutoff;
}

/*
 * Delete one bounded batch of expired events (cursor < cutoff), oldest
 * first. Bounded so a single prune never holds locks or generates WAL
 * without limit; retry-safe (re-running deletes nothing new).
 * Returns the number of rows deleted.
 */
static uint64
rf_prune_delete_batch(int32 feed_id, int64 cutoff, int32 batch_size)
{
	static const char *query =
		"DELETE FROM replayfeed.event WHERE ctid IN ("
		"  SELECT ctid FROM replayfeed.event"
		"   WHERE feed_id = $1 AND cursor < $2"
		"   ORDER BY cursor LIMIT $3)";
	Oid			argtypes[3] = {INT4OID, INT8OID, INT4OID};
	Datum		values[3];
	SPIPlanPtr	plan;
	uint64		deleted;

	plan = SPI_prepare(query, 3, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = Int32GetDatum(feed_id);
	values[1] = Int64GetDatum(cutoff);
	values[2] = Int32GetDatum(batch_size);
	if (SPI_execute_plan(plan, values, NULL, false, 0) != SPI_OK_DELETE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: prune delete failed")));
	deleted = SPI_processed;
	SPI_freeplan(plan);

	return deleted;
}

/*
 * Advance oldest_retained_cursor, but ONLY to the oldest row actually still
 * present (or latest_cursor + 1 when the log is empty). Advancing to the
 * full retention cutoff after a bounded (partial) delete would lie:
 * events_after() would report RESNAPSHOT_REQUIRED for cursors whose rows
 * are still retained. The min(cursor) subquery runs after our DELETE in the
 * same transaction, so it is the true new low-water mark; greatest() keeps
 * the watermark monotonic under concurrent prunes.
 */
static void
rf_prune_advance_watermark(int32 feed_id)
{
	static const char *query =
		"UPDATE replayfeed.feed"
		"   SET oldest_retained_cursor = greatest(oldest_retained_cursor,"
		"           COALESCE((SELECT min(cursor) FROM replayfeed.event"
		"                      WHERE feed_id = $1),"
		"                  latest_cursor + 1)),"
		"       last_pruned_at = clock_timestamp()"
		" WHERE feed_id = $1";
	Oid			argtypes[1] = {INT4OID};
	Datum		values[1];
	SPIPlanPtr	plan;

	plan = SPI_prepare(query, 1, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = Int32GetDatum(feed_id);
	if (SPI_execute_plan(plan, values, NULL, false, 0) != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: watermark update failed")));
	SPI_freeplan(plan);
}

/*
 * Prune one feed: compute the retention cutoff, delete one bounded batch
 * of expired events, then advance the watermark honestly. SPI must already
 * be connected. Returns rows deleted by this call.
 */
static int64
rf_prune_one_feed(int32 feed_id, int32 batch_size)
{
	int64		cutoff = rf_prune_cutoff(feed_id);
	uint64		deleted = rf_prune_delete_batch(feed_id, cutoff, batch_size);

	rf_prune_advance_watermark(feed_id);

	return (int64) deleted;
}

Datum
replayfeed_prune(PG_FUNCTION_ARGS)
{
	char	   *name;
	int32		batch_size;
	ReplayFeed	feed;
	int64		deleted;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: feed name is required")));
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	batch_size = PG_ARGISNULL(1) ? 10000 : PG_GETARG_INT32(1);
	if (batch_size < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: batch_size must be >= 1")));
	if (batch_size > 1000000)
		batch_size = 1000000;

	rf_spi_connect();

	if (!rf_get_feed_by_name(name, false, &feed))
	{
		SPI_finish();
		rf_ereport_no_feed(name);
	}

	deleted = rf_prune_one_feed(feed.feed_id, batch_size);

	SPI_finish();
	PG_RETURN_INT64(deleted);
}

/*
 * prune_all([batch_size]) -> SETOF (feed_name text, deleted bigint)
 * Convenience wrapper for schedulers: prune every feed once.
 *
 * Materialized SRF: fill the executor-provided tuplestore and return; see
 * replay.c for why we avoid the value-per-call HeapTuple return path.
 */
Datum
replayfeed_prune_all(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int32		batch_size;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;

	batch_size = PG_ARGISNULL(0) ? 10000 : PG_GETARG_INT32(0);
	if (batch_size < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: batch_size must be >= 1")));
	if (batch_size > 1000000)
		batch_size = 1000000;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	rf_spi_connect();

	{
		static const char *q_feeds =
			"SELECT feed_id, name FROM replayfeed.feed ORDER BY feed_id";
		SPIPlanPtr	plan;
		uint64		n_feeds;
		uint64		i;
		int32	   *feed_ids;
		char	  **feed_names;

		plan = SPI_prepare(q_feeds, 0, NULL);
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: SPI_prepare failed")));
		if (SPI_execute_plan(plan, NULL, NULL, true, 0) != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: feed scan failed")));

		/*
		 * Snapshot the feed list first: rf_prune_one_feed() issues its
		 * own SPI calls, which overwrite the global SPI_tuptable we are
		 * reading here.
		 */
		n_feeds = SPI_processed;
		feed_ids = (int32 *) palloc(n_feeds * sizeof(int32));
		feed_names = (char **) palloc(n_feeds * sizeof(char *));
		for (i = 0; i < n_feeds; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			TupleDesc	td = SPI_tuptable->tupdesc;
			bool		isnull;

			feed_ids[i] = DatumGetInt32(SPI_getbinval(tuple, td, 1, &isnull));
			feed_names[i] = text_to_cstring(DatumGetTextPP(SPI_getbinval(tuple, td, 2, &isnull)));
		}
		SPI_freeplan(plan);
		SPI_freetuptable(SPI_tuptable);

		for (i = 0; i < n_feeds; i++)
		{
			int64		deleted;
			Datum		v[2];
			bool		n[2] = {false, false};

			deleted = rf_prune_one_feed(feed_ids[i], batch_size);

			v[0] = CStringGetTextDatum(feed_names[i]);
			v[1] = Int64GetDatum(deleted);
			tuplestore_putvalues(tupstore, tupdesc, v, n);
		}
	}

	SPI_finish();
	PG_RETURN_VOID();
}
