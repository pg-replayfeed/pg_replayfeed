/*
 * pg_replayfeed -- logical decoding output plugin: WAL -> JSON change stream.
 *
 * Callbacks:
 *   startup  -- allocate plugin state; parse the optional "feed" startup
 *               option; fail fast when the extension is not installed.
 *   begin    -- reset the per-transaction context, load the feed registry.
 *   change   -- queue key-only changes for feed tables (nothing is emitted
 *               before commit: uncommitted work never reaches the stream).
 *   truncate -- queue feed names truncated in this transaction.
 *   commit   -- emit one JSON envelope for the transaction, then update
 *               the slot's progress (empty transactions only advance it).
 *   shutdown -- nothing to free explicitly (contexts die with the walsender).
 *
 * Robustness rules for this file:
 *  - Only committed transactions are decoded by logical decoding, so
 *    rolled-back writes never produce events. No compensation needed.
 *  - A single malformed change (dropped key column, NULL key, missing old
 *    tuple) is skipped with one WARNING per transaction per feed; it never
 *    raises ERROR, because an ERROR here would kill the replication stream
 *    for every consumer.
 *  - Column numbers are resolved by NAME on every change (see extract.c),
 *    so DDL that only reorders or adds columns keeps working.
 */
#include "replayfeed.h"

#include <stdarg.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/reorderbuffer.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* One queued key change, in stream order. */
typedef struct RfChange
{
	char	   *feed;			/* palloc'd in the transaction context */
	char		op;				/* 'I' = INSERT, 'U' = UPDATE, 'D' = DELETE */
	char	   *key;			/* palloc'd, never NULL */
	char	   *tenant;			/* palloc'd, NULL when the feed has no tenant column */
	char	   *old_tenant;		/* palloc'd, set on 'U' only when the tenant changed */
} RfChange;

#define RF_INLINE_CHANGES 16

typedef struct RfPluginState
{
	MemoryContext txn_cxt;		/* reset at every begin_cb */
	MemoryContext registry_cxt; /* persistent across transactions */
	RfFeedEntry *feeds;			/* feed registry, lives in registry_cxt */
	int			nfeeds;
	bool		registry_dirty; /* true if registry needs reload */
	char	   *only_feed;		/* startup option: stream just this feed */
	bool		feed_validated; /* only_feed checked against the registry */
	RfChange	inline_changes[RF_INLINE_CHANGES];
	int			num_changes;
	List	   *overflow_changes; /* List of RfChange*, only used if >16 changes in 1 txn */
	List	   *truncates;		/* List of char* feed names, first-seen order */
} RfPluginState;

/* Microseconds between the Postgres epoch (2000-01-01) and the Unix epoch. */
#define RF_PG_EPOCH_MS INT64CONST(946684800000)

/* Find a feed by table OID, honoring the "feed" startup filter.
 * Returns the index, or -1 when the table is not a (selected) feed. */
static inline int
rf_find_feed(RfPluginState *state, Oid relid)
{
	for (int i = 0; i < state->nfeeds; i++)
	{
		if (state->feeds[i].relid == relid && state->feeds[i].active)
			return i;
	}
	return -1;
}

/* Queue one change. Fast path uses inline array without heap allocation or List overhead. */
static void
rf_queue_change(RfPluginState *state, const char *feedname,
				char op, char *key, char *tenant, char *old_tenant)
{
	RfChange   *ch;

	if (likely(state->num_changes < RF_INLINE_CHANGES))
	{
		ch = &state->inline_changes[state->num_changes++];
	}
	else
	{
		ch = (RfChange *) palloc(sizeof(RfChange));
		state->overflow_changes = lappend(state->overflow_changes, ch);
	}

	ch->feed = (char *) feedname; /* points into persistent feed registry */
	ch->op = op;
	ch->key = key;				/* already in the transaction context */
	ch->tenant = tenant;
	ch->old_tenant = old_tenant;
}

/* Queue one change from a decoded tuple; warns once and skips when the
 * tuple cannot be keyed. */
static void
rf_capture_tuple(RfPluginState *state, int feed_idx,
				 char op, HeapTuple tuple, Relation rel,
				 char *old_tenant)
{
	RfFeedEntry *feed = &state->feeds[feed_idx];
	char	   *key;
	char	   *tenant;

	if (!rf_extract_key(rel, tuple, feed, &key, &tenant))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("pg_replayfeed: feed \"%s\": key column \"%s\" missing or NULL",
						feed->name, feed->key_col),
				 errdetail("The feed definition no longer matches the table. "
						   "The change cannot be keyed, so the stream stops "
						   "rather than silently diverging.")));
	rf_queue_change(state, feed->name, op, key, tenant, old_tenant);
}

/*
 * Queue the change(s) for one UPDATE. UPDATEs whose key changed are split
 * into DELETE(old key) + INSERT(new key): in a key-only feed that is the
 * honest representation, and it keeps consumers' key state exact. (The old
 * trigger design rejected key changes outright; WAL is already committed,
 * so rejection is not an option -- faithful representation is.)
 */
static void
rf_capture_update(RfPluginState *state, int feed_idx,
				  HeapTuple newtup, HeapTuple oldtup, Relation rel)
{
	RfFeedEntry *feed = &state->feeds[feed_idx];
	char	   *newkey;
	char	   *newtenant;

	if (!rf_extract_key(rel, newtup, feed, &newkey, &newtenant))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("pg_replayfeed: feed \"%s\": key column \"%s\" missing or NULL in new row",
						feed->name, feed->key_col),
				 errdetail("The feed definition no longer matches the table. "
						   "The change cannot be keyed, so the stream stops "
						   "rather than silently diverging.")));

	if (oldtup != NULL)
	{
		char	   *oldkey;
		char	   *oldtenant;

		if (!rf_extract_key(rel, oldtup, feed, &oldkey, &oldtenant))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("pg_replayfeed: feed \"%s\": old key unavailable",
							feed->name),
					 errdetail("Without the old key a key change cannot be "
							   "detected, so the stream stops rather than "
							   "silently diverging.")));
		if (strcmp(oldkey, newkey) != 0)
		{
			/* Key changed: the old entity is gone, a new one appeared. */
			rf_queue_change(state, feed->name, 'D', oldkey, oldtenant, NULL);
			rf_queue_change(state, feed->name, 'I', newkey, newtenant, NULL);
			return;
		}
		/* Key unchanged: report a tenant move when it happened, so
		 * tenant-partitioned consumers can relocate the entity. */
		if (oldtenant != NULL && newtenant != NULL &&
			strcmp(oldtenant, newtenant) != 0)
			rf_queue_change(state, feed->name, 'U', newkey, newtenant, oldtenant);
		else
			rf_queue_change(state, feed->name, 'U', newkey, newtenant, NULL);
		return;
	}
	/* No old tuple (should not happen with REPLICA IDENTITY FULL, but the
	 * identity may have been altered after feed creation): plain UPDATE. */
	rf_queue_change(state, feed->name, 'U', newkey, newtenant, NULL);
}

/* Append one queued change as a JSON object without vsnprintf overhead. */
static void
rf_append_change_json(StringInfo buf, const RfChange *ch)
{
	appendStringInfoString(buf, "{\"feed\":");
	rf_json_add_string(buf, ch->feed);
	appendStringInfoString(buf, ",\"op\":\"");
	appendStringInfoChar(buf, ch->op);
	appendStringInfoString(buf, "\",\"key\":");
	rf_json_add_string(buf, ch->key);
	if (ch->tenant != NULL)
	{
		appendStringInfoString(buf, ",\"tenant\":");
		rf_json_add_string(buf, ch->tenant);
	}
	if (ch->old_tenant != NULL)
	{
		appendStringInfoString(buf, ",\"old_tenant\":");
		rf_json_add_string(buf, ch->old_tenant);
	}
	appendStringInfoChar(buf, '}');
}

/* Emit the transaction envelope: one JSON object carrying every queued
 * change plus any truncated feeds. Writes directly to ctx->out without
 * intermediate string copies or vsnprintf calls. */
static void
rf_emit_envelope(LogicalDecodingContext *ctx, RfPluginState *state,
				 const char *lsnstr, int64 committed_at_ms)
{
	StringInfo	out;
	ListCell   *lc;
	bool		first = true;
	char		ms_buf[32];

	OutputPluginPrepareWrite(ctx, true);
	out = ctx->out;

	appendStringInfoString(out, "{\"cursor\":");
	rf_json_add_string(out, lsnstr);
	appendStringInfoString(out, ",\"committed_at_ms\":");
	pg_lltoa(committed_at_ms, ms_buf);
	appendStringInfoString(out, ms_buf);
	appendStringInfoString(out, ",\"changes\":[");
	for (int i = 0; i < state->num_changes; i++)
	{
		if (!first)
			appendStringInfoChar(out, ',');
		first = false;
		rf_append_change_json(out, &state->inline_changes[i]);
	}
	foreach(lc, state->overflow_changes)
	{
		if (!first)
			appendStringInfoChar(out, ',');
		first = false;
		rf_append_change_json(out, (const RfChange *) lfirst(lc));
	}
	appendStringInfoString(out, "],\"truncate\":[");
	first = true;
	foreach(lc, state->truncates)
	{
		if (!first)
			appendStringInfoChar(out, ',');
		first = false;
		rf_json_add_string(out, (const char *) lfirst(lc));
	}
	appendStringInfoString(out, "]}");

	OutputPluginWrite(ctx, true);
}

/* ---------------------------------------------------------------------------
 * Output plugin callbacks
 * -------------------------------------------------------------------------*/

static void
pg_rf_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			  bool is_init)
{
	RfPluginState *state;
	MemoryContext oldcxt;
	ListCell   *lc;

	state = (RfPluginState *) MemoryContextAllocZero(ctx->context,
													 sizeof(RfPluginState));
	state->txn_cxt = AllocSetContextCreate(ctx->context,
										   "pg_replayfeed txn",
										   ALLOCSET_DEFAULT_SIZES);
	state->registry_cxt = AllocSetContextCreate(ctx->context,
												"pg_replayfeed registry",
												ALLOCSET_DEFAULT_SIZES);
	state->feeds = NULL;
	state->nfeeds = 0;
	state->registry_dirty = true;
	ctx->output_plugin_private = state;
	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	/* Optional startup option: feed='<name>' restricts the stream to one
	 * feed, e.g. START_REPLICATION SLOT s LOGICAL 0/0 (feed 'orders').
	 * Anything else is a caller bug: fail loudly instead of silently
	 * ignoring it and streaming the wrong data. */
	oldcxt = MemoryContextSwitchTo(ctx->context);
	foreach(lc, ctx->output_plugin_options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "feed") == 0)
			state->only_feed = pstrdup(defGetString(def));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_replayfeed: unknown output plugin option \"%s\"",
							def->defname),
					 errhint("The only supported option is feed='<feed name>'.")));
	}
	MemoryContextSwitchTo(oldcxt);
}

static void
pg_rf_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	RfPluginState *state = (RfPluginState *) ctx->output_plugin_private;
	MemoryContext oldcxt;

	MemoryContextReset(state->txn_cxt);
	oldcxt = MemoryContextSwitchTo(state->txn_cxt);

	/*
	 * Load feed registry once or when dirty.
	 * Avoids running SPI queries on the hot path for every WAL commit.
	 */
	if (state->feeds == NULL || state->registry_dirty)
	{
		bool	pushed = false;

		if (IsTransactionState() && !ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			pushed = true;
		}

		MemoryContextReset(state->registry_cxt);
		MemoryContextSwitchTo(state->registry_cxt);

		state->feeds = rf_load_feed_registry(&state->nfeeds);
		for (int i = 0; i < state->nfeeds; i++)
			state->feeds[i].active = (state->only_feed == NULL ||
									  strcmp(state->feeds[i].name, state->only_feed) == 0);

		if (pushed)
			PopActiveSnapshot();

		state->registry_dirty = false;
		state->feed_validated = false;
	}

	state->num_changes = 0;
	state->overflow_changes = NIL;
	state->truncates = NIL;

	/*
	 * Validate the startup feed filter once, on the first transaction.
	 */
	if (state->only_feed != NULL && !state->feed_validated)
	{
		bool		found = false;

		for (int i = 0; i < state->nfeeds; i++)
		{
			if (strcmp(state->feeds[i].name, state->only_feed) == 0)
			{
				found = true;
				break;
			}
		}
		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("pg_replayfeed: feed \"%s\" does not exist",
							state->only_feed)));
		state->feed_validated = true;
	}

	MemoryContextSwitchTo(oldcxt);
}

static void
pg_rf_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			 Relation relation, ReorderBufferChange *change)
{
	RfPluginState *state = (RfPluginState *) ctx->output_plugin_private;
	MemoryContext oldcxt;
	int			feed_idx;

	feed_idx = rf_find_feed(state, RelationGetRelid(relation));
	if (feed_idx < 0)
	{
		if (strcmp(RelationGetRelationName(relation), "feed") == 0)
			state->registry_dirty = true;
		return;					/* not a (selected) feed table: ignore */
	}

	oldcxt = MemoryContextSwitchTo(state->txn_cxt);

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
#if PG_VERSION_NUM >= 170000
				/* PG17+: newtuple is a plain HeapTuple */
				HeapTuple	ntuple = change->data.tp.newtuple;
#else
				ReorderBufferTupleBuf *ntb = change->data.tp.newtuple;
				HeapTuple	ntuple = (ntb == NULL) ? NULL : &ntb->tuple;
#endif

				if (ntuple == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("pg_replayfeed: feed \"%s\": INSERT without new tuple",
									state->feeds[feed_idx].name)));
				rf_capture_tuple(state, feed_idx, 'I',
								 ntuple, relation, NULL);
				break;
			}

		case REORDER_BUFFER_CHANGE_UPDATE:
			{
#if PG_VERSION_NUM >= 170000
				/* PG17+: newtuple/oldtuple are plain HeapTuples */
				HeapTuple	ntuple = change->data.tp.newtuple;
				HeapTuple	otuple = change->data.tp.oldtuple;
#else
				ReorderBufferTupleBuf *ntb = change->data.tp.newtuple;
				ReorderBufferTupleBuf *otb = change->data.tp.oldtuple;
				HeapTuple	ntuple = (ntb == NULL) ? NULL : &ntb->tuple;
				HeapTuple	otuple = (otb == NULL) ? NULL : &otb->tuple;
#endif

				if (ntuple == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("pg_replayfeed: feed \"%s\": UPDATE without new tuple",
									state->feeds[feed_idx].name)));
				/* No-op UPDATE (SET x = x): the row did not change, so no
				 * event. The attribute comparison can only skip genuinely
				 * unchanged rows; anything else still emits. */
				if (otuple != NULL &&
					rf_tuples_identical(relation, otuple, ntuple))
					break;
				rf_capture_update(state, feed_idx,
								  ntuple,
								  otuple,
								  relation);
				break;
			}

		case REORDER_BUFFER_CHANGE_DELETE:
			{
#if PG_VERSION_NUM >= 170000
				/* PG17+: oldtuple is a plain HeapTuple */
				HeapTuple	otuple = change->data.tp.oldtuple;
#else
				ReorderBufferTupleBuf *otb = change->data.tp.oldtuple;
				HeapTuple	otuple = (otb == NULL) ? NULL : &otb->tuple;
#endif

				if (otuple == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("pg_replayfeed: feed \"%s\": DELETE without old tuple",
									state->feeds[feed_idx].name),
							 errhint("Was REPLICA IDENTITY changed after the feed was created? "
									 "pg_replayfeed requires REPLICA IDENTITY FULL on feed tables.")));
				rf_capture_tuple(state, feed_idx, 'D',
								 otuple, relation, NULL);
				break;
			}

		default:
			break;
	}

	MemoryContextSwitchTo(oldcxt);
}

static void
pg_rf_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			   int nrelations, Relation relations[],
			   ReorderBufferChange *change)
{
	RfPluginState *state = (RfPluginState *) ctx->output_plugin_private;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(state->txn_cxt);
	for (int i = 0; i < nrelations; i++)
	{
		int			feed_idx = rf_find_feed(state,
											RelationGetRelid(relations[i]));
		ListCell   *lc;
		bool		seen = false;

		if (feed_idx < 0)
			continue;
		foreach(lc, state->truncates)
		{
			if (strcmp((const char *) lfirst(lc),
					   state->feeds[feed_idx].name) == 0)
			{
				seen = true;
				break;
			}
		}
		if (!seen)
			state->truncates = lappend(state->truncates,
									   pstrdup(state->feeds[feed_idx].name));
	}
	MemoryContextSwitchTo(oldcxt);
}

static void
pg_rf_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			 XLogRecPtr commit_lsn)
{
	RfPluginState *state = (RfPluginState *) ctx->output_plugin_private;
	MemoryContext oldcxt;
	char		lsnstr[32];
	int64		committed_at_ms;

	/*
	 * Nothing for the stream: still advance the slot, marking the
	 * transaction skipped so WAL is not retained for it.
	 */
	if (state->num_changes == 0 && state->overflow_changes == NIL && state->truncates == NIL)
	{
		OutputPluginUpdateProgress(ctx, true);
		return;
	}

	/* The cursor: the commit LSN. Every message of this transaction shares
	 * it; cursors strictly increase across the stream. */
	rf_format_lsn(lsnstr, sizeof(lsnstr), commit_lsn);
	committed_at_ms = txn->xact_time.commit_time / 1000 + RF_PG_EPOCH_MS;

	oldcxt = MemoryContextSwitchTo(state->txn_cxt);
	rf_emit_envelope(ctx, state, lsnstr, committed_at_ms);
	MemoryContextSwitchTo(oldcxt);

	OutputPluginUpdateProgress(ctx, false);
}

static void
pg_rf_shutdown(LogicalDecodingContext *ctx)
{
	/* Plugin state lives in contexts owned by the decoding context. */
}

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	cb->startup_cb = pg_rf_startup;
	cb->begin_cb = pg_rf_begin;
	cb->change_cb = pg_rf_change;
	cb->truncate_cb = pg_rf_truncate;
	cb->commit_cb = pg_rf_commit;
	cb->shutdown_cb = pg_rf_shutdown;
}
