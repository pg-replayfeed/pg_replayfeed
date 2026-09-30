/*
 * pg_replayfeed -- replay API.
 *
 * events_after(feed_name, tenant, after_cursor, batch_limit) returns the
 * retained events for the tenant with cursor > after_cursor, in ascending
 * cursor order, bounded by batch_limit. Cursors are unique per event, so
 * cursor order alone is the deterministic replay order.
 *
 * Retention boundary: if after_cursor + 1 < oldest_retained_cursor, part of
 * the requested window has been pruned, so instead of silently returning a
 * gap we return a single sentinel row with operation = 'RESNAPSHOT_REQUIRED'.
 *
 * Implementation note: this is a materialized SRF (InitMaterializedSRF).
 * We fill a tuplestore on the single call and hand it to the executor via
 * ReturnSetInfo; the executor then streams rows itself. This avoids
 * returning HeapTuple datums across the SRF boundary, which is the
 * fragile path (tupdesc ownership / executor-side interpretation).
 */
#include "replayfeed.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(replayfeed_events_after);

Datum
replayfeed_events_after(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char	   *feed_name;
	char	   *tenant;
	int64		after_cursor;
	int32		batch_limit;
	ReplayFeed	feed;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;

	/* Cheap argument validation before touching SPI. */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: feed name is required")));
	feed_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	tenant = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(1));
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: cursor is required")));
	after_cursor = PG_GETARG_INT64(2);
	batch_limit = PG_ARGISNULL(3) ? 500 : PG_GETARG_INT32(3);

	if (after_cursor < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: cursor must be >= 0")));
	if (batch_limit < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: batch_limit must be >= 1")));
	if (batch_limit > REPLAYFEED_MAX_BATCH)
		batch_limit = REPLAYFEED_MAX_BATCH;

	/*
	 * Materialize mode: the executor gives us its expected tuple
	 * descriptor (guaranteed to match what the caller wants) and a
	 * tuplestore to fill. InitMaterializedSRF errors cleanly if we are
	 * not in a FROM-clause set context.
	 */
	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	rf_spi_connect();

	if (!rf_get_feed_by_name(feed_name, false, &feed))
	{
		SPI_finish();
		rf_ereport_no_feed(feed_name);
	}

	if (after_cursor < feed.oldest_retained_cursor - 1)
	{
		/* Expired window: explicit sentinel, never a silent gap. */
		Datum		values[4];
		bool		nulls[4] = {true, true, false, true};

		values[0] = (Datum) 0;
		values[1] = (Datum) 0;
		values[2] = CStringGetTextDatum(REPLAYFEED_RESNAPSHOT_REQUIRED);
		values[3] = (Datum) 0;
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	else
	{
		/*
		 * Two static query variants so the planner can use the
		 * (feed_id, tenant_key, cursor) index in both cases. A single
		 * "tenant_key IS NOT DISTINCT FROM $2" predicate does not match the
		 * index for non-null tenants (verified via EXPLAIN): the planner
		 * falls back to a pkey bitmap scan plus a heap filter and sort.
		 */
		static const char *query_tenant =
			"SELECT cursor, entity_key, operation::text, committed_at"
			"  FROM replayfeed.event"
			" WHERE feed_id = $1"
			"   AND tenant_key = $2"
			"   AND cursor > $3"
			" ORDER BY cursor"
			" LIMIT $4";
		static const char *query_no_tenant =
			"SELECT cursor, entity_key, operation::text, committed_at"
			"  FROM replayfeed.event"
			" WHERE feed_id = $1"
			"   AND tenant_key IS NULL"
			"   AND cursor > $2"
			" ORDER BY cursor"
			" LIMIT $3";
		const char *query = (tenant != NULL) ? query_tenant : query_no_tenant;
		Oid			argtypes_tenant[4] = {INT4OID, TEXTOID, INT8OID, INT4OID};
		Oid			argtypes_no_tenant[3] = {INT4OID, INT8OID, INT4OID};
		Oid		   *argtypes;
		Datum		values[4];
		char		nulls[4];
		SPIPlanPtr	plan;
		uint64		i;
		int			nargs;

		if (tenant != NULL)
		{
			nargs = 4;
			argtypes = argtypes_tenant;
			values[0] = Int32GetDatum(feed.feed_id);
			values[1] = CStringGetTextDatum(tenant);
			values[2] = Int64GetDatum(after_cursor);
			values[3] = Int32GetDatum(batch_limit);
			nulls[0] = nulls[1] = nulls[2] = nulls[3] = ' ';
		}
		else
		{
			/* query_no_tenant: $1 = feed_id, $2 = after_cursor, $3 = limit */
			nargs = 3;
			argtypes = argtypes_no_tenant;
			values[0] = Int32GetDatum(feed.feed_id);
			values[1] = Int64GetDatum(after_cursor);
			values[2] = Int32GetDatum(batch_limit);
			nulls[0] = nulls[1] = nulls[2] = ' ';
		}

		plan = SPI_prepare(query, nargs, argtypes);
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: SPI_prepare failed")));
		if (SPI_execute_plan(plan, values, nulls, true, 0) != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: replay scan failed")));

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			TupleDesc	td = SPI_tuptable->tupdesc;
			Datum		v[4];
			bool		n[4];

			v[0] = SPI_getbinval(tuple, td, 1, &n[0]);
			v[1] = SPI_getbinval(tuple, td, 2, &n[1]);
			v[2] = SPI_getbinval(tuple, td, 3, &n[2]);
			v[3] = SPI_getbinval(tuple, td, 4, &n[3]);
			tuplestore_putvalues(tupstore, tupdesc, v, n);
		}
		SPI_freeplan(plan);
		SPI_freetuptable(SPI_tuptable);
	}

	SPI_finish();

	/* In materialize mode the return value is ignored; the executor reads
	 * rsinfo->setResult. */
	PG_RETURN_VOID();
}
