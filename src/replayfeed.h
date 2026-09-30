/*
 * pg_replayfeed -- shared declarations.
 *
 * pg_replayfeed is a logical-decoding output plugin: it streams key-only
 * change events straight out of the binary WAL. For every committed
 * transaction that touched a registered feed table, the plugin emits one
 * JSON message at commit time. Consumers read the stream with the
 * replication protocol (START_REPLICATION ... LOGICAL); there is no
 * polling, no event table, and no trigger on the write path.
 *
 * Wire contract (see docs/architecture.md for the full spec):
 *
 *   Transaction envelope (at most one per committed transaction that
 *   touched a feed table; nothing is emitted for other transactions):
 *     {"cursor":"0/16B1978","committed_at_ms":1727690096789,
 *      "changes":[{"feed":"orders","op":"I","key":"42","tenant":"acme"},
 *                 {"feed":"orders","op":"U","key":"43"},
 *                 {"feed":"orders","op":"D","key":"44","tenant":"acme"}],
 *      "truncate":["other_feed"]}
 *
 *   "changes" carries the ordered key-only changes; "truncate" lists feeds
 *   whose tables were truncated (consumers must resnapshot those feeds).
 *   Both arrays are always present, possibly empty.
 *
 * Cursor: the commit LSN, text form "X/X". One envelope per transaction
 * means cursors strictly increase across the stream. Delivery is
 * at-least-once with transaction-atomic redelivery: a consumer records the
 * cursor of the last fully applied envelope and resumes with
 * START_REPLICATION ... FROM <cursor>, so it must be idempotent across a
 * redelivered envelope.
 *
 * Module layout:
 *   pg_replayfeed.c  module entry: _PG_output_plugin_init
 *   decode.c         output-plugin callbacks, transaction state, change queue
 *   extract.c        tuple -> (key, tenant) extraction, no-op UPDATE filter
 *   encode.c         JSON framing and emission
 *   catalog.c        SPI helpers and the feed registry
 *   feed.c           create_feed / drop_feed administration
 */
#ifndef PG_REPLAYFEED_H
#define PG_REPLAYFEED_H

#include "postgres.h"
#include "fmgr.h"
#include "access/attnum.h"
#include "executor/spi.h"
#include "utils/builtins.h"

/* ---------------------------------------------------------------------------
 * Feed registry (catalog.c)
 * -------------------------------------------------------------------------*/

/* In-memory view of one replayfeed.feed row. Strings live in the caller's
 * memory context. */
typedef struct RfFeedEntry
{
	Oid			relid;			/* source_table */
	char		name[NAMEDATALEN];
	char		key_col[NAMEDATALEN];
	char		tenant_col[NAMEDATALEN];
	bool		has_tenant;
	bool		active;

	/* Cached attribute and type info for fast-path tuple extraction */
	int			cached_natts;
	AttrNumber	key_attno;
	Oid			key_typoutput;
	bool		key_typisvarlena;
	AttrNumber	tenant_attno;
	Oid			tenant_typoutput;
	bool		tenant_typisvarlena;
} RfFeedEntry;

/* Connect to SPI, raising ERROR on failure. */
extern void rf_spi_connect(void);

/*
 * Load the whole feed registry (all rows of replayfeed.feed) into the
 * current memory context. Sets *nfeeds; returns NULL when empty. Raises
 * ERROR with a clear message when the extension is not installed in this
 * database.
 */
extern RfFeedEntry *rf_load_feed_registry(int *nfeeds);

/* Look up a single feed by name for the admin functions. */
extern bool rf_get_feed_by_name(const char *name, RfFeedEntry *feed,
								int32 *feed_id_out);

/* Raise ERRCODE_UNDEFINED_OBJECT for a missing feed name. */
extern void rf_ereport_no_feed(const char *name);

/* ---------------------------------------------------------------------------
 * Tuple extraction (extract.c)
 * -------------------------------------------------------------------------*/

/*
 * Extract the key and tenant strings from a decoded heap tuple.
 *
 * Returns true on success with *key_p set (never NULL) and *tenant_p set
 * (NULL when the feed has no tenant column or the value is NULL). Returns
 * false when the change cannot be keyed (key column dropped, NULL key);
 * the caller must warn (once per transaction) and skip the change. Never
 * raises ERROR: a malformed change must not kill the replication stream.
 */
extern bool rf_extract_key(Relation rel, HeapTuple tuple,
						   RfFeedEntry *feed,
						   char **key_p, char **tenant_p);

/*
 * True when the two decoded row versions hold identical user-visible
 * content, i.e. the UPDATE changed nothing and must not produce an event.
 * Compares attribute by attribute (never raw tuple bytes: row headers
 * always differ between versions). Can only skip genuinely unchanged rows;
 * it can never hide a real change.
 */
extern bool rf_tuples_identical(Relation rel, HeapTuple oldtup,
								HeapTuple newtup);

/* ---------------------------------------------------------------------------
 * JSON encoding (encode.c)
 * -------------------------------------------------------------------------*/

/* Append a JSON string literal (with quotes) to buf. */
extern void rf_json_add_string(StringInfo buf, const char *s);

/* Format an LSN as "X/X" text. */
extern void rf_format_lsn(char *dst, size_t dstlen, XLogRecPtr lsn);

#endif							/* PG_REPLAYFEED_H */
