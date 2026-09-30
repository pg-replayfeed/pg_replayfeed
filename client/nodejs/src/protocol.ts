/**
 * PostgreSQL replication protocol packet constants and utilities.
 */

// Postgres epoch: midnight 2000-01-01 UTC in milliseconds
const PG_EPOCH_MS = 946684800000;

export const MSG_XLOG_DATA = 0x77; // 'w'
export const MSG_KEEPALIVE = 0x6b; // 'k'
export const MSG_STANDBY_STATUS = 0x72; // 'r'

export interface ParsedLSN {
  upper: number;
  lower: number;
  raw: bigint;
}

/**
 * Parses an LSN string (e.g. "0/16B1978" or "1A/5F3") into its components.
 */
export function parseLSN(lsnStr: string): ParsedLSN {
  const parts = lsnStr.trim().split('/');
  if (parts.length !== 2) {
    throw new Error(`Invalid LSN string: "${lsnStr}"`);
  }
  const upper = parseInt(parts[0], 16);
  const lower = parseInt(parts[1], 16);
  if (isNaN(upper) || isNaN(lower)) {
    throw new Error(`Invalid hexadecimal in LSN: "${lsnStr}"`);
  }
  const raw = (BigInt(upper) << 32n) | BigInt(lower >>> 0);
  return { upper, lower, raw };
}

/**
 * Formats a BigInt or upper/lower integers as a PostgreSQL LSN string.
 */
export function formatLSN(raw: bigint): string;
export function formatLSN(upper: number, lower: number): string;
export function formatLSN(arg1: bigint | number, arg2?: number): string {
  if (typeof arg1 === 'bigint') {
    const upper = Number(arg1 >> 32n);
    const lower = Number(arg1 & 0xffffffffn);
    return `${upper.toString(16).toUpperCase()}/${lower.toString(16).toUpperCase()}`;
  }
  const upper = arg1;
  const lower = arg2 ?? 0;
  return `${(upper >>> 0).toString(16).toUpperCase()}/${(lower >>> 0).toString(16).toUpperCase()}`;
}

/**
 * Calculates current time in microseconds since PostgreSQL epoch (2000-01-01).
 */
export function currentPgTimeMicroseconds(): bigint {
  const diffMs = Date.now() - PG_EPOCH_MS;
  return BigInt(diffMs) * 1000n;
}

/**
 * Builds a Standby Status Update CopyData message (34 bytes).
 *
 * Byte 0: 'r' (0x72)
 * Bytes 1-8: WAL write position
 * Bytes 9-16: WAL flush position
 * Bytes 17-24: WAL apply position
 * Bytes 25-32: client time in microseconds since 2000-01-01
 * Byte 33: replyRequested (0 or 1)
 */
export function buildStandbyStatusUpdate(
  lsn: string | bigint,
  replyRequested = false
): Buffer {
  const rawLsn = typeof lsn === 'string' ? parseLSN(lsn).raw : lsn;
  const buf = Buffer.alloc(34);

  buf.writeUInt8(MSG_STANDBY_STATUS, 0); // 'r'
  buf.writeBigUInt64BE(rawLsn, 1);       // WALWritePosition
  buf.writeBigUInt64BE(rawLsn, 9);       // WALFlushPosition
  buf.writeBigUInt64BE(rawLsn, 17);      // WALApplyPosition
  buf.writeBigUInt64BE(currentPgTimeMicroseconds(), 25);
  buf.writeUInt8(replyRequested ? 1 : 0, 33);

  return buf;
}

export interface PrimaryKeepalive {
  serverWalEnd: string;
  serverWalEndBigInt: bigint;
  serverTime: bigint;
  replyRequested: boolean;
}

/**
 * Parses a Primary Keepalive message from the server.
 */
export function parsePrimaryKeepalive(buf: Buffer): PrimaryKeepalive {
  if (buf.length < 18 || buf[0] !== MSG_KEEPALIVE) {
    throw new Error('Invalid primary keepalive buffer');
  }
  const serverWalEndBigInt = buf.readBigUInt64BE(1);
  const serverTime = buf.readBigInt64BE(9);
  const replyRequested = buf.readUInt8(17) !== 0;

  return {
    serverWalEnd: formatLSN(serverWalEndBigInt),
    serverWalEndBigInt,
    serverTime,
    replyRequested,
  };
}

export interface XLogData {
  walStart: string;
  walStartBigInt: bigint;
  walEnd: string;
  walEndBigInt: bigint;
  serverTime: bigint;
  walData: Buffer;
}

/**
 * Parses an XLogData message containing WAL data.
 */
export function parseXLogData(buf: Buffer): XLogData {
  if (buf.length < 25 || buf[0] !== MSG_XLOG_DATA) {
    throw new Error('Invalid xlog data buffer');
  }
  const walStartBigInt = buf.readBigUInt64BE(1);
  const walEndBigInt = buf.readBigUInt64BE(9);
  const serverTime = buf.readBigInt64BE(17);
  const walData = buf.subarray(25);

  return {
    walStart: formatLSN(walStartBigInt),
    walStartBigInt,
    walEnd: formatLSN(walEndBigInt),
    walEndBigInt,
    serverTime,
    walData,
  };
}
