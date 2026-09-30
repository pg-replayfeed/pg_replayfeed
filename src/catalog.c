/*
 * pg_replayfeed -- SPI helpers and the feed registry.
 *
 * The output plugin rebuilds the feed registry from replayfeed.feed at the
 * start of every decoded transaction. The table is tiny and changes rarely,
 * so a fresh read per transaction is cheaper than cache invalidation logic
 * and can never serve a stale feed definition.
 *
 * Contract: rf_load_feed_registry() and rf_get_feed_by_name() open and
 * close their own SPI connection; callers must not hold one across the call.
 */
#include "replayfeed.h"
#include "utils/builtins.h"

void
rf_spi_connect(void)
{
	int			rc = SPI_connect();

	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("pg_replayfeed: SPI_connect failed")));
}

/*
 * Fail fast when the extension is not installed in the database the slot
 * lives in. A silent empty stream would be far worse than a loud error at
 * slot creation / replication start. SPI must already be connected.
 */
static void
rf_require_catalog(void)
{
	static const char *q =
		"SELECT to_regclass('replayfeed.feed') IS NOT NULL";
	SPIPlanPtr	plan;
	bool		isnull;
	Datum		d;

	plan = SPI_prepare(q, 0, NULL);
	if (plan == NULL ||
		SPI_execute_plan(plan, NULL, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: catalog check failed")));
	d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull || !DatumGetBool(d))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_replayfeed: extension is not installed in this database"),
				 errhint("Run CREATE EXTENSION pg_replayfeed in the database before creating a replication slot with this output plugin.")));
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);
}

/* Copy one feed row out of the SPI result. Both queries used here return
 * (source_table, key_column, tenant_column) at attributes 2, 3, 4; only the
 * name differs (attribute 1 in the registry scan, passed in for lookups). */
static void
rf_copy_feed_row(HeapTuple tup, TupleDesc tupdesc, RfFeedEntry *e,
				 const char *name, bool name_first)
{
	bool		isnull;
	Name		colname;
	Datum		d;

	if (name_first)
	{
		/* The catalog defines name as text (not the name type). */
		char	   *s = text_to_cstring(DatumGetTextPP(
											SPI_getbinval(tup, tupdesc, 1, &isnull)));

		namestrcpy((Name) e->name, s);
		pfree(s);
	}
	else
		namestrcpy((Name) e->name, name);
	e->relid = DatumGetObjectId(SPI_getbinval(tup, tupdesc, 2, &isnull));
	colname = DatumGetName(SPI_getbinval(tup, tupdesc, 3, &isnull));
	namestrcpy((Name) e->key_col, NameStr(*colname));
	d = SPI_getbinval(tup, tupdesc, 4, &isnull);
	if (isnull)
	{
		e->has_tenant = false;
		e->tenant_col[0] = '\0';
	}
	else
	{
		colname = DatumGetName(d);
		namestrcpy((Name) e->tenant_col, NameStr(*colname));
		e->has_tenant = true;
	}

	e->cached_natts = -1;
	e->key_attno = InvalidAttrNumber;
	e->tenant_attno = InvalidAttrNumber;
}

RfFeedEntry *
rf_load_feed_registry(int *nfeeds)
{
	static const char *q =
		"SELECT name, source_table, key_column, tenant_column"
		"  FROM replayfeed.feed ORDER BY feed_id";
	MemoryContext caller_cxt = CurrentMemoryContext;
	SPIPlanPtr	plan;
	RfFeedEntry *feeds = NULL;
	uint64		i;

	rf_spi_connect();
	/*
	 * SPI_connect() switches CurrentMemoryContext to its private procedure
	 * context, which SPI_finish() tears down. Switch back now: the registry
	 * array is allocated below and must survive SPI_finish(), because the
	 * decoder keeps using it until the next begin_cb.
	 */
	MemoryContextSwitchTo(caller_cxt);
	rf_require_catalog();

	plan = SPI_prepare(q, 0, NULL);
	if (plan == NULL ||
		SPI_execute_plan(plan, NULL, NULL, true, 0) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: could not read replayfeed.feed")));

	*nfeeds = (int) SPI_processed;
	if (*nfeeds > 0)
	{
		/*
		 * Allocate the registry in the caller's context explicitly, right
		 * here: SPI calls above may leave CurrentMemoryContext pointing at
		 * SPI-internal contexts, and anything allocated there dies with
		 * SPI_finish() below, leaving a dangling registry.
		 */
		MemoryContextSwitchTo(caller_cxt);
		feeds = (RfFeedEntry *) palloc(sizeof(RfFeedEntry) * (Size) (*nfeeds));
		for (i = 0; i < SPI_processed; i++)
			rf_copy_feed_row(SPI_tuptable->vals[i], SPI_tuptable->tupdesc,
							 &feeds[i], NULL, true);
	}
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	return feeds;
}

bool
rf_get_feed_by_name(const char *name, RfFeedEntry *feed, int32 *feed_id_out)
{
	static const char *q =
		"SELECT feed_id, source_table, key_column, tenant_column"
		"  FROM replayfeed.feed WHERE name = $1";
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];
	SPIPlanPtr	plan;
	bool		isnull;
	bool		found;

	rf_spi_connect();
	rf_require_catalog();

	plan = SPI_prepare(q, 1, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = CStringGetTextDatum(name);
	if (SPI_execute_plan(plan, values, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: feed lookup failed")));

	found = (SPI_processed > 0);
	if (found)
	{
		HeapTuple	tup = SPI_tuptable->vals[0];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;

		if (feed_id_out != NULL)
			*feed_id_out =
				DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
		rf_copy_feed_row(tup, tupdesc, feed, name, false);
	}
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	return found;
}

void
rf_ereport_no_feed(const char *name)
{
	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("pg_replayfeed: feed \"%s\" does not exist", name)));
}
