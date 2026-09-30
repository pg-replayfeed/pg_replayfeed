import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  parseLSN,
  formatLSN,
  buildStandbyStatusUpdate,
  parsePrimaryKeepalive,
  parseXLogData,
  MSG_STANDBY_STATUS,
  MSG_KEEPALIVE,
  MSG_XLOG_DATA,
} from '../src/protocol';
import { Envelope } from '../src/types';

describe('Protocol & LSN Utilities', () => {
  it('should parse valid LSN strings', () => {
    const lsn = parseLSN('0/16B1978');
    assert.strictEqual(lsn.upper, 0);
    assert.strictEqual(lsn.lower, 0x16b1978);
    assert.strictEqual(lsn.raw, 0x16b1978n);

    const lsn2 = parseLSN('1F/ABCD');
    assert.strictEqual(lsn2.upper, 0x1f);
    assert.strictEqual(lsn2.lower, 0xabcd);
    assert.strictEqual(lsn2.raw, (0x1fn << 32n) | 0xabcdn);
  });

  it('should throw on invalid LSN strings', () => {
    assert.throws(() => parseLSN('invalid'));
    assert.throws(() => parseLSN('0/XYZ'));
  });

  it('should format LSN BigInt back to canonical uppercase hex', () => {
    const raw = (0x1fn << 32n) | 0xabcdn;
    assert.strictEqual(formatLSN(raw), '1F/ABCD');
    assert.strictEqual(formatLSN(0, 0x16b1978), '0/16B1978');
  });

  it('should construct a valid 34-byte StandbyStatusUpdate packet', () => {
    const lsn = '0/16B1978';
    const packet = buildStandbyStatusUpdate(lsn, true);

    assert.strictEqual(packet.length, 34);
    assert.strictEqual(packet[0], MSG_STANDBY_STATUS);

    const writeLsn = packet.readBigUInt64BE(1);
    const flushLsn = packet.readBigUInt64BE(9);
    const applyLsn = packet.readBigUInt64BE(17);
    const expectedRaw = 0x16b1978n;

    assert.strictEqual(writeLsn, expectedRaw);
    assert.strictEqual(flushLsn, expectedRaw);
    assert.strictEqual(applyLsn, expectedRaw);

    // replyRequested flag
    assert.strictEqual(packet[33], 1);

    const packetNoReply = buildStandbyStatusUpdate(lsn, false);
    assert.strictEqual(packetNoReply[33], 0);
  });

  it('should parse PrimaryKeepalive message correctly', () => {
    const buf = Buffer.alloc(18);
    buf.writeUInt8(MSG_KEEPALIVE, 0);
    buf.writeBigUInt64BE(0x16b1978n, 1); // serverWalEnd
    buf.writeBigInt64BE(123456789n, 9); // serverTime
    buf.writeUInt8(1, 17); // replyRequested

    const keepalive = parsePrimaryKeepalive(buf);
    assert.strictEqual(keepalive.serverWalEnd, '0/16B1978');
    assert.strictEqual(keepalive.replyRequested, true);
  });

  it('should parse XLogData message and JSON envelope correctly', () => {
    const mockEnvelope: Envelope = {
      cursor: '0/16B1978',
      committed_at_ms: 1727690096789,
      changes: [
        { feed: 'orders', op: 'I', key: '48921', tenant: 'acme' },
        { feed: 'orders', op: 'U', key: '48920', tenant: 'acme', old_tenant: 'legacy_acme' },
        { feed: 'orders', op: 'D', key: '1234' },
      ],
      truncate: ['feed_archived'],
    };

    const jsonBytes = Buffer.from(JSON.stringify(mockEnvelope), 'utf8');
    const buf = Buffer.alloc(25 + jsonBytes.length);

    buf.writeUInt8(MSG_XLOG_DATA, 0);
    buf.writeBigUInt64BE(0x16b1000n, 1); // walStart
    buf.writeBigUInt64BE(0x16b1978n, 9); // walEnd
    buf.writeBigInt64BE(999999n, 17);   // serverTime
    jsonBytes.copy(buf, 25);

    const xlog = parseXLogData(buf);
    assert.strictEqual(xlog.walStart, '0/16B1000');
    assert.strictEqual(xlog.walEnd, '0/16B1978');

    const decoded: Envelope = JSON.parse(xlog.walData.toString('utf8'));
    assert.strictEqual(decoded.cursor, '0/16B1978');
    assert.strictEqual(decoded.committed_at_ms, 1727690096789);
    assert.strictEqual(decoded.changes.length, 3);
    assert.strictEqual(decoded.changes[0].op, 'I');
    assert.strictEqual(decoded.changes[0].key, '48921');
    assert.strictEqual(decoded.changes[1].old_tenant, 'legacy_acme');
    assert.strictEqual(decoded.truncate[0], 'feed_archived');
  });
});
