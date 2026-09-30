/*
 * pg_replayfeed -- tuple extraction for the WAL decoder.
 *
 * Turns a decoded heap tuple into the (key, tenant) strings that go on the
 * wire. Column numbers are resolved by NAME against the relation's own
 * tuple descriptor on every call, so DDL that only reorders or adds columns
 * keeps working without feed downtime.
 *
 * Nothing here raises ERROR: a change the plugin cannot key (dropped key
 * column, NULL key, missing old tuple) is reported to the caller, which
 * warns once per transaction and skips it. An ERROR in the decoder would
 * kill the replication stream for every consumer.
 */
#include "replayfeed.h"

#include "access/htup_details.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* Attribute number for a column name, skipping dropped columns. */
static AttrNumber
rf_attno_by_name(TupleDesc desc, const char *colname)
{
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (!attr->attisdropped && strcmp(NameStr(attr->attname), colname) == 0)
			return attr->attnum;
	}
	return InvalidAttrNumber;
}

/* Fast datum -> cstring using cached output function (detoasting if varlena). */
static inline char *
rf_datum_cstring_fast(Datum d, Oid typoutput, bool typisvarlena)
{
	if (typisvarlena)
		d = PointerGetDatum(pg_detoast_datum((struct varlena *) DatumGetPointer(d)));
	return OidOutputFunctionCall(typoutput, d);
}

bool
rf_extract_key(Relation rel, HeapTuple tuple, RfFeedEntry *feed,
			   char **key_p, char **tenant_p)
{
	TupleDesc	desc = RelationGetDescr(rel);
	bool		isnull;
	Datum		d;

	*key_p = NULL;
	*tenant_p = NULL;

	/* Warm/refresh metadata cache on schema change or initial access */
	if (unlikely(feed->cached_natts != desc->natts))
	{
		AttrNumber	attno = rf_attno_by_name(desc, feed->key_col);

		if (attno == InvalidAttrNumber)
			return false;		/* key column dropped after feed creation */

		feed->key_attno = attno;
		getTypeOutputInfo(TupleDescAttr(desc, attno - 1)->atttypid,
						  &feed->key_typoutput, &feed->key_typisvarlena);

		if (feed->has_tenant)
		{
			attno = rf_attno_by_name(desc, feed->tenant_col);
			feed->tenant_attno = attno;
			if (attno != InvalidAttrNumber)
				getTypeOutputInfo(TupleDescAttr(desc, attno - 1)->atttypid,
								  &feed->tenant_typoutput, &feed->tenant_typisvarlena);
		}
		feed->cached_natts = desc->natts;
	}

	if (feed->key_attno == InvalidAttrNumber)
		return false;

	d = heap_getattr(tuple, feed->key_attno, desc, &isnull);
	if (isnull)
		return false;			/* NOT NULL was dropped after feed creation */

	*key_p = rf_datum_cstring_fast(d, feed->key_typoutput, feed->key_typisvarlena);

	if (feed->has_tenant && feed->tenant_attno != InvalidAttrNumber)
	{
		d = heap_getattr(tuple, feed->tenant_attno, desc, &isnull);
		if (!isnull)
			*tenant_p = rf_datum_cstring_fast(d, feed->tenant_typoutput, feed->tenant_typisvarlena);
	}
	return true;
}

/*
 * Are two heap tuples identical in user-visible content?
 *
 * Compares attribute by attribute (skipping dropped columns) instead of
 * comparing raw tuple bytes: the tuple headers always differ between row
 * versions (xmin/xmax), so a byte comparison could never detect a no-op
 * UPDATE. A NULL/missing comparison is exact; for non-NULL values the
 * stored bytes are compared, which is exact for identical content and can
 * only err toward "different" (emitting a redundant event -- always safe).
 */
bool
rf_tuples_identical(Relation rel, HeapTuple oldtup, HeapTuple newtup)
{
	TupleDesc	desc = RelationGetDescr(rel);

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);
		bool		oldnull;
		bool		newnull;
		Datum		oldd;
		Datum		newd;

		if (attr->attisdropped)
			continue;

		oldd = heap_getattr(oldtup, attr->attnum, desc, &oldnull);
		newd = heap_getattr(newtup, attr->attnum, desc, &newnull);
		if (oldnull != newnull)
			return false;
		if (oldnull)
			continue;
		if (attr->attbyval)
		{
			if (oldd != newd)
				return false;
		}
		else
		{
			Size		oldlen = datumGetSize(oldd, false, attr->attlen);
			Size		newlen = datumGetSize(newd, false, attr->attlen);

			if (oldlen != newlen ||
				memcmp(DatumGetPointer(oldd), DatumGetPointer(newd), oldlen) != 0)
				return false;
		}
	}
	return true;
}
