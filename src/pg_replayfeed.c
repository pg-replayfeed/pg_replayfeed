/*
 * pg_replayfeed -- module entry point.
 *
 * This shared library serves two roles:
 *  - a logical-decoding output plugin (see decode.c), loaded by the
 *    walsender when a replication slot names it;
 *  - a regular extension providing the create_feed / drop_feed SQL
 *    functions (see feed.c).
 *
 * The output-plugin callbacks are registered by _PG_output_plugin_init
 * in decode.c; PG_MODULE_MAGIC below covers both roles.
 */
#include "replayfeed.h"

PG_MODULE_MAGIC;
