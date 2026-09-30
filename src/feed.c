/*
 * pg_replayfeed -- feed registration and lifecycle.
 *
 * create_feed() validates the source table thoroughly and registers it in
 * replayfeed.feed. There are no triggers: changes are decoded straight from
 * WAL by the output plugin (see decode.c). drop_feed() removes the catalog
 * row; in-flight replication slots simply stop emitting for the table.
 *
 * Logical decoding needs the old row image for UPDATE/DELETE keying, so
 * create_feed() sets REPLICA IDENTITY FULL on the source table when it is
 * not already set. Without it, DELETEs (and key-change UPDATEs) would have
 * no key to emit -- a silent data-loss event for consumers. Setting it here,
 * loudly, is the "won't fail at first place" choice.
 */
#include "replayfeed.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"

PG_FUNCTION_INFO_V1(replayfeed_create_feed);
PG_FUNCTION_INFO_V1(replayfeed_drop_feed);

/*
 * Does the invoker hold the given privilege on the table?
 *
 * Feed administration runs as SECURITY DEFINER, so the two-argument
 * has_table_privilege(table, priv) would test the *definer* and always
 * pass. We pass GetOuterUserId() explicitly: the user who invoked this
 * function (respecting SET ROLE), never the definer identity.
 */
static bool
rf_invoker_has_table_privilege(Oid tbl_oid, const char *priv)
{
	static const char *query = "SELECT has_table_privilege($1, $2, $3)";
	Oid			argtypes[3] = {OIDOID, OIDOID, TEXTOID};
	Datum		values[3];
	SPIPlanPtr	plan;
	bool		isnull;
	Datum		d;
	bool		result;

	plan = SPI_prepare(query, 3, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = ObjectIdGetDatum(GetOuterUserId());
	values[1] = ObjectIdGetDatum(tbl_oid);
	values[2] = CStringGetTextDatum(priv);

	if (SPI_execute_plan(plan, values, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: privilege check failed")));
	SPI_freeplan(plan);

	d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	result = !isnull && DatumGetBool(d);
	SPI_freetuptable(SPI_tuptable);
	return result;
}

/* Single-row existence check helper: SELECT 1 ... -> true/false. */
static bool
rf_exists(const char *query, int nargs, Oid *argtypes, Datum *values)
{
	SPIPlanPtr	plan;
	bool		found;

	plan = SPI_prepare(query, nargs, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	if (SPI_execute_plan(plan, values, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: catalog check failed")));
	found = (SPI_processed > 0);
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);
	return found;
}

/*
 * Validate the source table for feed registration. Raises ERROR with a
 * specific message for every unsupported case.
 */
static void
rf_validate_source_table(Oid tbl_oid, const char *key_col,
						 const char *tenant_col, AttrNumber *key_attno)
{
	static const char *q_relkind =
		"SELECT relkind, relpersistence, relreplident"
		"  FROM pg_catalog.pg_class WHERE oid = $1";
	static const char *q_att =
		"SELECT attnum, attnotnull FROM pg_catalog.pg_attribute"
		" WHERE attrelid = $1 AND attname = $2 AND attnum > 0 AND NOT attisdropped";
	static const char *q_unique =
		"SELECT 1 FROM pg_catalog.pg_index"
		" WHERE indrelid = $1 AND indisunique AND indisvalid AND indisready"
		"   AND indnatts = 1 AND indkey[0] = $2";
	Oid			argtypes1[1] = {OIDOID};
	Oid			argtypes2[2] = {OIDOID, TEXTOID};
	Oid			argtypes3[2] = {OIDOID, INT2OID};
	Datum		values1[1];
	Datum		values2[2];
	Datum		values3[2];
	SPIPlanPtr	plan;
	bool		isnull;
	HeapTuple	tup;
	TupleDesc	tupdesc;
	char		relkind;
	char		relpersist;
	char		relreplident;
	AttrNumber	attno;
	bool		attnotnull;

	values1[0] = ObjectIdGetDatum(tbl_oid);
	plan = SPI_prepare(q_relkind, 1, argtypes1);
	if (plan == NULL ||
		SPI_execute_plan(plan, values1, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: catalog check failed")));
	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("pg_replayfeed: source table does not exist")));
	tup = SPI_tuptable->vals[0];
	tupdesc = SPI_tuptable->tupdesc;
	relkind = DatumGetChar(SPI_getbinval(tup, tupdesc, 1, &isnull));
	relpersist = DatumGetChar(SPI_getbinval(tup, tupdesc, 2, &isnull));
	relreplident = DatumGetChar(SPI_getbinval(tup, tupdesc, 3, &isnull));
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);

	if (relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("pg_replayfeed: source must be an ordinary table"),
				 errdetail("relkind '%c' is not supported: views, foreign tables and partitioned tables are rejected.",
						   relkind)));
	if (relpersist != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("pg_replayfeed: source table must be a permanent (logged) table"),
				 errdetail("Temporary and unlogged tables are rejected: their WAL is not logically decodable.")));

	/* key column must exist and be NOT NULL */
	values2[0] = ObjectIdGetDatum(tbl_oid);
	values2[1] = CStringGetTextDatum(key_col);
	plan = SPI_prepare(q_att, 2, argtypes2);
	if (plan == NULL ||
		SPI_execute_plan(plan, values2, NULL, true, 1) != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: catalog check failed")));
	if (SPI_processed == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("pg_replayfeed: key column \"%s\" does not exist", key_col)));
	tup = SPI_tuptable->vals[0];
	tupdesc = SPI_tuptable->tupdesc;
	attno = DatumGetInt16(SPI_getbinval(tup, tupdesc, 1, &isnull));
	attnotnull = DatumGetBool(SPI_getbinval(tup, tupdesc, 2, &isnull));
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);

	if (!attnotnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: key column \"%s\" must be NOT NULL", key_col)));

	/* key column must have a single-column unique index (or PK) */
	values3[0] = ObjectIdGetDatum(tbl_oid);
	values3[1] = Int16GetDatum(attno);
	if (!rf_exists(q_unique, 2, argtypes3, values3))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: key column \"%s\" must have a single-column unique index or primary key",
						key_col),
				 errdetail("The key identifies entities across the change stream.")));

	if (key_attno)
		*key_attno = attno;

	/* tenant column must exist when configured */
	if (tenant_col != NULL)
	{
		values2[1] = CStringGetTextDatum(tenant_col);
		if (!rf_exists(q_att, 2, argtypes2, values2))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("pg_replayfeed: tenant column \"%s\" does not exist", tenant_col)));
	}

	/*
	 * REPLICA IDENTITY FULL is what lets the decoder key DELETEs and
	 * key-change UPDATEs from the old row image. Set it when it is not
	 * already FULL; without it those changes would be silently unkeyable.
	 */
	if (relreplident != REPLICA_IDENTITY_FULL)
	{
		StringInfoData buf;
		char	   *qtbl;

		qtbl = quote_qualified_identifier(
			get_namespace_name(get_rel_namespace(tbl_oid)),
			get_rel_name(tbl_oid));
		initStringInfo(&buf);
		appendStringInfo(&buf, "ALTER TABLE %s REPLICA IDENTITY FULL", qtbl);
		if (SPI_execute(buf.data, false, 0) != SPI_OK_UTILITY)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: could not set REPLICA IDENTITY FULL on the source table"),
					 errdetail("Logical decoding needs the old row image to key DELETEs.")));
		ereport(NOTICE,
				(errmsg("pg_replayfeed: set REPLICA IDENTITY FULL on %s", qtbl),
				 errdetail("The old row image is logged for UPDATE/DELETE so the change stream can key every event.")));
	}
}

/*
 * create_feed(name, source_table, key_column, tenant_column)
 *
 * SECURITY DEFINER. The caller must hold TRIGGER privilege on the source
 * table (checked explicitly against the invoker via GetOuterUserId(), so
 * SET ROLE is honored and the definer's rights never leak into the check);
 * the definer rights are used only for writing the replayfeed catalog and
 * for the REPLICA IDENTITY FULL ALTER.
 */
Datum
replayfeed_create_feed(PG_FUNCTION_ARGS)
{
	char	   *name;
	Oid			tbl_oid;
	char	   *key_col;
	char	   *tenant_col;
	AttrNumber	key_attno;
	int32		feed_id;
	/* Function-scope: NameGetDatum stores a pointer, so these must outlive
	 * SPI_execute_plan. */
	NameData	key_nd;
	NameData	tenant_nd;
	Datum		values[4];
	char		nulls[4];
	Oid			argtypes[4] = {TEXTOID, REGCLASSOID, NAMEOID, NAMEOID};
	SPIPlanPtr	plan;
	bool		isnull;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: name, source_table and key_column are required")));

	name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	tbl_oid = PG_GETARG_OID(1);
	key_col = pstrdup(NameStr(*PG_GETARG_NAME(2)));
	tenant_col = PG_ARGISNULL(3) ? NULL : pstrdup(NameStr(*PG_GETARG_NAME(3)));

	if (name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: feed name must not be empty")));
	if (tenant_col != NULL && pg_strcasecmp(tenant_col, key_col) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: tenant column must differ from key column")));

	rf_spi_connect();

	/* Authorization: explicit TRIGGER-privilege check on the invoker
	 * (GetOuterUserId), not the SECURITY DEFINER identity. */
	if (!rf_invoker_has_table_privilege(tbl_oid, "TRIGGER"))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("pg_replayfeed: permission denied"),
				 errdetail("User \"%s\" needs TRIGGER privilege on the source table to register a feed.",
						   GetUserNameFromId(GetOuterUserId(), false))));

	rf_validate_source_table(tbl_oid, key_col, tenant_col, &key_attno);

	/* Name / table uniqueness with friendly errors. */
	{
		Oid			at[1] = {TEXTOID};
		Datum		v[1] = {CStringGetTextDatum(name)};

		if (rf_exists("SELECT 1 FROM replayfeed.feed WHERE name = $1", 1, at, v))
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("pg_replayfeed: feed \"%s\" already exists", name)));
	}
	{
		Oid			at[1] = {REGCLASSOID};
		Datum		v[1] = {ObjectIdGetDatum(tbl_oid)};
		static const char *q =
			"SELECT name FROM replayfeed.feed WHERE source_table = $1";
		SPIPlanPtr	p;
		char	   *other;

		p = SPI_prepare(q, 1, at);
		if (p == NULL ||
			SPI_execute_plan(p, v, NULL, true, 1) != SPI_OK_SELECT)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_replayfeed: catalog check failed")));
		if (SPI_processed > 0)
		{
			other = text_to_cstring(DatumGetTextPP(SPI_getbinval(SPI_tuptable->vals[0],
																SPI_tuptable->tupdesc, 1, &isnull)));
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_replayfeed: table already feeds \"%s\" (one feed per table)",
							other)));
		}
		SPI_freeplan(p);
		SPI_freetuptable(SPI_tuptable);
	}

	/* Insert the feed row. */
	plan = SPI_prepare(
		"INSERT INTO replayfeed.feed (name, source_table, key_column, tenant_column)"
		" VALUES ($1, $2, $3, $4) RETURNING feed_id",
		4, argtypes);
	if (plan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = CStringGetTextDatum(name);
	values[1] = ObjectIdGetDatum(tbl_oid);
	namestrcpy(&key_nd, key_col);
	values[2] = NameGetDatum(&key_nd);
	nulls[0] = nulls[1] = nulls[2] = ' ';
	if (tenant_col != NULL)
	{
		namestrcpy(&tenant_nd, tenant_col);
		values[3] = NameGetDatum(&tenant_nd);
		nulls[3] = ' ';
	}
	else
	{
		values[3] = (Datum) 0;
		nulls[3] = 'n';
	}
	if (SPI_execute_plan(plan, values, nulls, false, 1) != SPI_OK_INSERT_RETURNING)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_replayfeed: feed insert failed")));
	feed_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 1, &isnull));
	SPI_freeplan(plan);
	SPI_freetuptable(SPI_tuptable);

	SPI_finish();
	PG_RETURN_INT32(feed_id);
}

/*
 * drop_feed(name)
 *
 * Deletes the feed row. Requires TRIGGER privilege on the source table.
 * Replication slots keep existing; they simply stop emitting for the table.
 * (WAL already decoded is unaffected; consumers keep their positions.)
 */
Datum
replayfeed_drop_feed(PG_FUNCTION_ARGS)
{
	char	   *name;
	RfFeedEntry feed;
	Oid			argtypes[1] = {TEXTOID};
	Datum		values[1];
	SPIPlanPtr	plan;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: feed name is required")));
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* rf_get_feed_by_name() opens and closes its own SPI connection. */
	if (!rf_get_feed_by_name(name, &feed, NULL))
		rf_ereport_no_feed(name);

	rf_spi_connect();

	if (!rf_invoker_has_table_privilege(feed.relid, "TRIGGER"))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("pg_replayfeed: permission denied"),
				 errdetail("User \"%s\" needs TRIGGER privilege on the source table to drop feed \"%s\".",
						   GetUserNameFromId(GetOuterUserId(), false), name)));

	plan = SPI_prepare("DELETE FROM replayfeed.feed WHERE name = $1",
					   1, argtypes);
	if (plan == NULL)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_replayfeed: SPI_prepare failed")));
	values[0] = CStringGetTextDatum(name);
	if (SPI_execute_plan(plan, values, NULL, false, 0) != SPI_OK_DELETE)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("pg_replayfeed: feed delete failed")));
	SPI_freeplan(plan);

	SPI_finish();
	PG_RETURN_VOID();
}
