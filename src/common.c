/*
 * pg_replayfeed -- shared SPI helpers.
 */
#include "replayfeed.h"


void
rf_spi_connect(void)
{
	int			rc = SPI_connect();

	if (rc != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("pg_replayfeed: SPI_connect failed")));
}

static bool
rf_fetch_feed(bool by_id, Datum key, bool lock, ReplayFeed *feed)
{
	static const char *qname =
		"SELECT feed_id, source_table, key_column, tenant_column,"
		"       latest_cursor, oldest_retained_cursor"
		"  FROM replayfeed.feed WHERE name = $1";
	static const char *qname_lock =
		"SELECT feed_id, source_table, key_column, tenant_column,"
		"       latest_cursor, oldest_retained_cursor"
		"  FROM replayfeed.feed WHERE name = $1 FOR UPDATE";
	static const char *qid =
		"SELECT feed_id, source_table, key_column, tenant_column,"
		"       latest_cursor, oldest_retained_cursor"
		"  FROM replayfeed.feed WHERE feed_id = $1";
	static const char *qid_lock =
		"SELECT feed_id, source_table, key_column, tenant_column,"
		"       latest_cursor, oldest_retained_cursor"
		"  FROM replayfeed.feed WHERE feed_id = $1 FOR UPDATE";

	const char *query;
	Oid			argtype;
	SPIPlanPtr	plan;
	Datum		values[1];
	bool		isnull;
	HeapTuple	tup;
	TupleDesc	tupdesc;
	Name		colname;
	Datum		d;

	if (by_id)
	{
		query = lock ? qid_lock : qid;
		argtype = INT4OID;
	}
	else
	{
		query = lock ? qname_lock : qname;
		argtype = TEXTOID;
	}

	plan = SPI_prepare(query, 1, &argtype);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));

	values[0] = key;

	if (SPI_execute_plan(plan, values, NULL, false, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: feed lookup failed")));

	if (SPI_processed == 0)
	{
		SPI_freeplan(plan);
		return false;
	}

	tup = SPI_tuptable->vals[0];
	tupdesc = SPI_tuptable->tupdesc;

	feed->feed_id = DatumGetInt32(SPI_getbinval(tup, tupdesc, 1, &isnull));
	feed->source_table = DatumGetObjectId(SPI_getbinval(tup, tupdesc, 2, &isnull));

	colname = DatumGetName(SPI_getbinval(tup, tupdesc, 3, &isnull));
	feed->key_column = pstrdup(NameStr(*colname));

	d = SPI_getbinval(tup, tupdesc, 4, &isnull);
	if (isnull)
		feed->tenant_column = NULL;
	else
	{
		colname = DatumGetName(d);
		feed->tenant_column = pstrdup(NameStr(*colname));
	}

	feed->latest_cursor = DatumGetInt64(SPI_getbinval(tup, tupdesc, 5, &isnull));
	feed->oldest_retained_cursor = DatumGetInt64(SPI_getbinval(tup, tupdesc, 6, &isnull));
	feed->name = NULL;

	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);
	return true;
}

bool
rf_get_feed_by_name(const char *name, bool lock, ReplayFeed *feed)
{
	bool		ok = rf_fetch_feed(false, CStringGetTextDatum(name), lock, feed);

	if (ok)
		feed->name = pstrdup(name);
	return ok;
}

bool
rf_get_feed_by_id(int32 feed_id, bool lock, ReplayFeed *feed)
{
	return rf_fetch_feed(true, Int32GetDatum(feed_id), lock, feed);
}

void
rf_ereport_no_feed(const char *name)
{
	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("pg_replayfeed: feed \"%s\" does not exist", name)));
}
