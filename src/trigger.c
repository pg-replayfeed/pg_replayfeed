/*
 * pg_replayfeed -- statement-level change trigger.
 *
 * Installed per feed as:
 *   AFTER INSERT OR UPDATE OR DELETE ... REFERENCING OLD TABLE AS rf_old
 *   NEW TABLE AS rf_new FOR EACH STATEMENT
 *   EXECUTE FUNCTION replayfeed.trigger_fn(<feed_id>)
 *
 * The trigger reads transition tuples from tg_oldtable/tg_newtable, validates
 * them, then serializes cursor allocation through the feed's catalog row
 * (SELECT ... FOR UPDATE, held to transaction end). While holding the lock it
 * allocates one unique, monotonically increasing cursor per event as the
 * contiguous range latest_cursor+1 .. latest_cursor+row_count. Cursor order
 * therefore equals commit order (see docs/architecture.md section 3).
 *
 * Allocation, event inserts and the latest_cursor bump happen in the same
 * transaction: rollback recycles the range, so committed cursors stay dense
 * (no gaps outside retention). Rollback removes the events.
 */
#include "replayfeed.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/tuptable.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(replayfeed_trigger_fn);

typedef struct
{
	char	   *key;			/* never NULL (NULL keys are rejected) */
	char	   *tenant;			/* NULL when tenant_null */
	bool		tenant_null;
} RowKey;

/* Datum -> cstring via the type's output function. */
static char *
rf_datum_to_cstring(Datum d, Oid typid)
{
	Oid			typoutput;
	bool		typisvarlena;

	getTypeOutputInfo(typid, &typoutput, &typisvarlena);
	return OidOutputFunctionCall(typoutput, d);
}

/*
 * Drain a transition tuplestore into an array of (key, tenant) pairs.
 * NULL keys raise ERROR. Returns the array and sets *n_out.
 */
static RowKey *
rf_collect_rows(Tuplestorestate *ts, TupleDesc tupdesc,
				AttrNumber key_attno, Oid key_typid,
				AttrNumber tenant_attno, Oid tenant_typid,
				const char *key_colname, int *n_out)
{
	RowKey	   *rows;
	int			n = 0;
	int			cap = 64;
	TupleTableSlot *slot;

	rows = (RowKey *) palloc(cap * sizeof(RowKey));
	/* transition tuplestores hold minimal tuples */
	slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsMinimalTuple);

	tuplestore_rescan(ts);
	while (tuplestore_gettupleslot(ts, true, false, slot))
	{
		bool		kisnull;
		bool		tisnull = true;
		Datum		kd;
		Datum		td = (Datum) 0;

		kd = slot_getattr(slot, key_attno, &kisnull);
		if (kisnull)
			ereport(ERROR,
					(errcode(ERRCODE_NOT_NULL_VIOLATION),
					 errmsg("pg_replayfeed: key column \"%s\" produced a NULL key",
							key_colname),
					 errdetail("NULL keys cannot be represented in the replay log.")));
		if (tenant_attno != InvalidAttrNumber)
			td = slot_getattr(slot, tenant_attno, &tisnull);

		if (n >= cap)
		{
			cap *= 2;
			rows = (RowKey *) repalloc(rows, cap * sizeof(RowKey));
		}
		rows[n].key = rf_datum_to_cstring(kd, key_typid);
		if (tenant_attno == InvalidAttrNumber || tisnull)
		{
			rows[n].tenant = NULL;
			rows[n].tenant_null = true;
		}
		else
		{
			rows[n].tenant = rf_datum_to_cstring(td, tenant_typid);
			rows[n].tenant_null = false;
		}
		n++;
		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);
	*n_out = n;
	return rows;
}

static int
rf_rowkey_cmp(const void *a, const void *b)
{
	const RowKey *ra = (const RowKey *) a;
	const RowKey *rb = (const RowKey *) b;

	return strcmp(ra->key, rb->key);
}

/*
 * UPDATE validation: the multiset of keys must be unchanged and every row's
 * tenant must be unchanged. Rows are aligned by sorting on the key, which is
 * sound because the key column has a unique index.
 */
static void
rf_validate_update(RowKey *old_rows, int n_old, RowKey *new_rows, int n_new,
				   const char *key_colname, const char *tenant_colname)
{
	int			i;

	if (n_old != n_new)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: transition table row count mismatch (%d vs %d)",
						n_old, n_new)));

	qsort(old_rows, n_old, sizeof(RowKey), rf_rowkey_cmp);
	qsort(new_rows, n_new, sizeof(RowKey), rf_rowkey_cmp);

	for (i = 0; i < n_old; i++)
	{
		if (strcmp(old_rows[i].key, new_rows[i].key) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_replayfeed: key column \"%s\" may not be modified (\"%s\" -> \"%s\")",
							key_colname, old_rows[i].key, new_rows[i].key),
					 errdetail("Changing entity keys is rejected in v1: drop and recreate the feed instead.")));
		if (old_rows[i].tenant_null != new_rows[i].tenant_null ||
			(!old_rows[i].tenant_null &&
			 strcmp(old_rows[i].tenant, new_rows[i].tenant) != 0))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_replayfeed: tenant column \"%s\" may not be modified (entity \"%s\")",
							tenant_colname ? tenant_colname : "(none)",
							new_rows[i].key),
					 errdetail("An entity must never move between tenants inside the replay log.")));
	}
}

Datum
replayfeed_trigger_fn(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	int32		feed_id;
	ReplayFeed	feed;
	bool		is_insert;
	bool		is_update;
	bool		is_delete;
	TupleDesc	tupdesc;
	AttrNumber	key_attno;
	AttrNumber	tenant_attno = InvalidAttrNumber;
	Oid			key_typid;
	Oid			tenant_typid = InvalidOid;
	RowKey	   *old_rows = NULL;
	RowKey	   *new_rows = NULL;
	RowKey	   *rows;
	int			n_old = 0;
	int			n_new = 0;
	int			n;
	const char *opname;
	int64		range_start;
	int64		range_end;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				 errmsg("pg_replayfeed: trigger_fn must be called as a trigger")));
	trigdata = (TriggerData *) fcinfo->context;
	if (!TRIGGER_FIRED_FOR_STATEMENT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				 errmsg("pg_replayfeed: trigger_fn must be a FOR EACH STATEMENT trigger")));
	if (trigdata->tg_trigger->tgnargs < 1)
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				 errmsg("pg_replayfeed: trigger_fn requires the feed id as TG_ARGV[0]")));
	feed_id = (int32) atoi(trigdata->tg_trigger->tgargs[0]);
	if (feed_id <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: invalid feed id argument")));

	is_insert = TRIGGER_FIRED_BY_INSERT(trigdata->tg_event);
	is_update = TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event);
	is_delete = TRIGGER_FIRED_BY_DELETE(trigdata->tg_event);
	opname = is_insert ? "INSERT" : (is_update ? "UPDATE" : "DELETE");

	rf_spi_connect();

	if (!rf_get_feed_by_id(feed_id, false, &feed))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("pg_replayfeed: feed %d does not exist", feed_id)));

	tupdesc = RelationGetDescr(trigdata->tg_relation);
	key_attno = get_attnum(feed.source_table, feed.key_column);
	if (key_attno == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("pg_replayfeed: key column \"%s\" no longer exists on the source table",
						feed.key_column),
				 errdetail("Drop and recreate the feed after altering the source table.")));
	key_typid = TupleDescAttr(tupdesc, key_attno - 1)->atttypid;

	if (feed.tenant_column != NULL)
	{
		tenant_attno = get_attnum(feed.source_table, feed.tenant_column);
		if (tenant_attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("pg_replayfeed: tenant column \"%s\" no longer exists on the source table",
							feed.tenant_column),
					 errdetail("Drop and recreate the feed after altering the source table.")));
		tenant_typid = TupleDescAttr(tupdesc, tenant_attno - 1)->atttypid;
	}

	if (!is_insert && trigdata->tg_oldtable != NULL)
		old_rows = rf_collect_rows(trigdata->tg_oldtable, tupdesc,
								   key_attno, key_typid,
								   tenant_attno, tenant_typid,
								   feed.key_column, &n_old);
	if (!is_delete && trigdata->tg_newtable != NULL)
		new_rows = rf_collect_rows(trigdata->tg_newtable, tupdesc,
								   key_attno, key_typid,
								   tenant_attno, tenant_typid,
								   feed.key_column, &n_new);

	if (is_update)
	{
		rf_validate_update(old_rows, n_old, new_rows, n_new,
						   feed.key_column, feed.tenant_column);
		rows = new_rows;
		n = n_new;
	}
	else if (is_insert)
	{
		rows = new_rows;
		n = n_new;
	}
	else
	{
		rows = old_rows;
		n = n_old;
	}

	/* Statements that touched no rows allocate no cursor and take no lock,
	 * keeping latest_cursor exactly equal to max(event.cursor). */
	if (n == 0)
	{
		SPI_finish();
		PG_RETURN_VOID();
	}

	/*
	 * Serialize cursor allocation: the FOR UPDATE row lock is held until
	 * transaction end, so allocation order == commit order. Allocate one
	 * unique cursor per event as the contiguous range
	 * latest_cursor+1 .. latest_cursor+n. Rollback undoes the whole range,
	 * letting the next writer reuse it.
	 */
	if (!rf_get_feed_by_id(feed_id, true, &feed))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("pg_replayfeed: feed %d does not exist", feed_id)));
	range_start = feed.latest_cursor + 1;
	range_end = feed.latest_cursor + n;

	/* Bulk insert the events with a single statement. unnest supplies the
	 * 1-based ordinality, so row i lands on cursor range_start + i - 1.
	 * $3 is the entity-key array, $4 the tenant-key array. */
	{
		static const char *q_insert =
			" INSERT INTO replayfeed.event"
			"       (feed_id, cursor, tenant_key, entity_key, operation, committed_at)"
			" SELECT $1, $2 + u.seq - 1, u.tenant_key, u.entity_key,"
			"        $5::replayfeed.operation, clock_timestamp()"
			" FROM unnest($3::text[], $4::text[]) WITH ORDINALITY"
			"        AS u(entity_key, tenant_key, seq)";
		Oid			argtypes[5] = {INT4OID, INT8OID, TEXTARRAYOID, TEXTARRAYOID, TEXTOID};
		Datum		values[5];
		Datum	   *kdatums;
		Datum	   *tdatums;
		bool	   *tnulls;
		ArrayType  *karr;
		ArrayType  *tarr;
		int			dims[1];
		int			i;
		SPIPlanPtr	plan;

		kdatums = (Datum *) palloc(n * sizeof(Datum));
		tdatums = (Datum *) palloc(n * sizeof(Datum));
		tnulls = (bool *) palloc(n * sizeof(bool));
		for (i = 0; i < n; i++)
		{
			kdatums[i] = CStringGetTextDatum(rows[i].key);
			if (rows[i].tenant_null)
			{
				tdatums[i] = (Datum) 0;
				tnulls[i] = true;
			}
			else
			{
				tdatums[i] = CStringGetTextDatum(rows[i].tenant);
				tnulls[i] = false;
			}
		}
		karr = construct_array(kdatums, n, TEXTOID, -1, false, 'i');
		dims[0] = n;
		{
			int			lbs[1] = {1};

			tarr = construct_md_array(tdatums, tnulls, 1, dims, lbs,
									  TEXTOID, -1, false, 'i');
		}

		plan = SPI_prepare(q_insert, 5, argtypes);
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: SPI_prepare failed")));
		values[0] = Int32GetDatum(feed_id);
		values[1] = Int64GetDatum(range_start);
		values[2] = PointerGetDatum(karr);
		values[3] = PointerGetDatum(tarr);
		values[4] = CStringGetTextDatum(opname);
		if (SPI_execute_plan(plan, values, NULL, false, 0) != SPI_OK_INSERT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: event insert failed")));
		SPI_freeplan(plan);
	}

	/* Publish the range end and bump observability counters. */
	{
		static const char *q_update =
			"UPDATE replayfeed.feed"
			"   SET latest_cursor = $2,"
			"       event_count = event_count + $3,"
			"       trigger_calls = trigger_calls + 1"
			" WHERE feed_id = $1";
		Oid			argtypes[3] = {INT4OID, INT8OID, INT8OID};
		Datum		values[3];
		SPIPlanPtr	plan;

		plan = SPI_prepare(q_update, 3, argtypes);
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: SPI_prepare failed")));
		values[0] = Int32GetDatum(feed_id);
		values[1] = Int64GetDatum(range_end);
		values[2] = Int64GetDatum((int64) n);
		if (SPI_execute_plan(plan, values, NULL, false, 0) != SPI_OK_UPDATE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: feed cursor update failed")));
		SPI_freeplan(plan);
	}

	/* Wake-up hint only; delivered at commit. The relay never trusts it. */
	{
		static const char *q_notify = "SELECT pg_notify('replayfeed', $1)";
		Oid			argtypes[1] = {TEXTOID};
		Datum		values[1];
		SPIPlanPtr	plan;
		char		payload[32];

		snprintf(payload, sizeof(payload), "%d", feed_id);
		plan = SPI_prepare(q_notify, 1, argtypes);
		if (plan == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: SPI_prepare failed")));
		values[0] = CStringGetTextDatum(payload);
		if (SPI_execute_plan(plan, values, NULL, false, 0) != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: pg_notify failed")));
		SPI_freeplan(plan);
		SPI_freetuptable(SPI_tuptable);
	}

	SPI_finish();
	PG_RETURN_VOID();
}
