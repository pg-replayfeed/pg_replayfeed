/*
 * pg_replayfeed -- JSON framing for the change stream.
 *
 * The message builders live in decode.c; this file holds the small,
 * easily-tested primitives: JSON string escaping and LSN text formatting.
 */
#include "replayfeed.h"

#include "lib/stringinfo.h"

/* Append s as a JSON string literal, with surrounding quotes. Control
 * characters, quotes and backslashes are escaped; everything else (including
 * multi-byte UTF-8) passes through verbatim. */
void
rf_json_add_string(StringInfo buf, const char *s)
{
	const unsigned char *p = (const unsigned char *) s;
	bool needs_escape = false;

	while (*p)
	{
		if (unlikely(*p < 0x20 || *p == '"' || *p == '\\'))
		{
			needs_escape = true;
			break;
		}
		p++;
	}

	appendStringInfoChar(buf, '"');
	if (likely(!needs_escape))
	{
		appendBinaryStringInfo(buf, s, (int)(p - (const unsigned char *)s));
		appendStringInfoChar(buf, '"');
		return;
	}

	/* Escape path for strings with quotes or control characters */
	p = (const unsigned char *) s;
	for (; *p; p++)
	{
		switch (*p)
		{
			case '"':
				appendStringInfoString(buf, "\\\"");
				break;
			case '\\':
				appendStringInfoString(buf, "\\\\");
				break;
			case '\b':
				appendStringInfoString(buf, "\\b");
				break;
			case '\f':
				appendStringInfoString(buf, "\\f");
				break;
			case '\n':
				appendStringInfoString(buf, "\\n");
				break;
			case '\r':
				appendStringInfoString(buf, "\\r");
				break;
			case '\t':
				appendStringInfoString(buf, "\\t");
				break;
			default:
				if (*p < 0x20)
					appendStringInfo(buf, "\\u%04x", *p);
				else
					appendStringInfoChar(buf, *p);
				break;
		}
	}
	appendStringInfoChar(buf, '"');
}

static const char rf_hex_digits[] = "0123456789ABCDEF";

static inline int
rf_fmt_hex_u32(char *dst, uint32 val)
{
	char tmp[8];
	int len = 0;
	if (val == 0)
	{
		*dst = '0';
		return 1;
	}
	while (val > 0)
	{
		tmp[len++] = rf_hex_digits[val & 0xF];
		val >>= 4;
	}
	for (int i = 0; i < len; i++)
		dst[i] = tmp[len - 1 - i];
	return len;
}

/* Format an LSN as the canonical "X/X" text (e.g. "0/16B1978"). This is the
 * stream cursor: consumers store it and resume with
 * START_REPLICATION ... FROM <cursor>. */
void
rf_format_lsn(char *dst, size_t dstlen, XLogRecPtr lsn)
{
	uint32 hi = (uint32) (lsn >> 32);
	uint32 lo = (uint32) lsn;
	int pos = rf_fmt_hex_u32(dst, hi);
	dst[pos++] = '/';
	pos += rf_fmt_hex_u32(dst + pos, lo);
	dst[pos] = '\0';
}
