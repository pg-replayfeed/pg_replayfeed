/*
 * pg_replayfeed -- snapshot fence.
 *
 * fence(feed_name) returns the feed's latest_cursor as visible in the
 * caller's snapshot. Called as the first statement of a REPEATABLE READ
 * transaction, it establishes the boundary between "state read from the
 * snapshot" and "changes replayed afterwards" (see docs/architecture.md
 * section 5 for the invariant and its proof).
 *
 * The function deliberately takes no locks: long-running snapshots must not
 * block feed writers.
 */
#include "replayfeed.h"

#include "catalog/pg_type.h"

PG_FUNCTION_INFO_V1(replayfeed_fence);

Datum
replayfeed_fence(PG_FUNCTION_ARGS)
{
	char	   *name;
	ReplayFeed	feed;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_replayfeed: feed name is required")));
	name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	rf_spi_connect();

	if (!rf_get_feed_by_name(name, false, &feed))
	{
		SPI_finish();
		rf_ereport_no_feed(name);
	}

	SPI_finish();
	PG_RETURN_INT64(feed.latest_cursor);
}
