import { EventEmitter } from 'events';
import { Client as PgClient, ClientConfig } from 'pg';
import { Envelope, ReplayFeedConfig } from './types';
import {
  buildStandbyStatusUpdate,
  formatLSN,
  MSG_KEEPALIVE,
  MSG_XLOG_DATA,
  parseLSN,
  parsePrimaryKeepalive,
  parseXLogData,
} from './protocol';

export class ReplayFeedClient extends EventEmitter {
  private config: ReplayFeedConfig;
  private repClient: PgClient | null = null;
  private isRunning = false;
  private lastLsn: string = '0/0';
  private standbyTimer: NodeJS.Timeout | null = null;
  private slotName: string;
  private pluginName: string;
  private feedName?: string;
  private standbyIntervalMs: number;

  constructor(config: ReplayFeedConfig = {}) {
    super();
    this.config = config;
    this.slotName = config.slotName || 'replayfeed_slot';
    this.pluginName = config.plugin || 'pg_replayfeed';
    this.feedName = config.feed;
    this.standbyIntervalMs = config.standbyIntervalMs || 10000;
  }

  /**
   * Returns the LSN of the most recently received envelope.
   */
  public getLastLsn(): string {
    return this.lastLsn;
  }

  /**
   * Creates a standard (non-replication) pg.Client for admin commands.
   */
  private createAdminClient(): PgClient {
    const baseConfig: ClientConfig = this.config.connectionString
      ? { connectionString: this.config.connectionString, ...this.config.clientConfig }
      : {
          host: this.config.host || 'localhost',
          port: this.config.port || 5432,
          user: this.config.user || 'postgres',
          password: this.config.password,
          database: this.config.database || 'postgres',
          ssl: this.config.ssl,
          ...this.config.clientConfig,
        };

    // Ensure replication flag is NOT set for admin commands
    delete (baseConfig as any).replication;
    return new PgClient(baseConfig);
  }

  /**
   * Ensures that the replication slot exists on the PostgreSQL server.
   * If the slot exists, returns its confirmed_flush_lsn.
   * If it does not exist, creates it and returns its consistent_point LSN.
   */
  public async ensureSlot(slot = this.slotName, plugin = this.pluginName): Promise<string> {
    const admin = this.createAdminClient();
    await admin.connect();
    try {
      try {
        const res = await admin.query(
          'SELECT pg_create_logical_replication_slot($1, $2)::text AS slot_info',
          [slot, plugin]
        );
        let slotInfo: string = res.rows[0]?.slot_info || '';
        // Format is "(slotname,0/1234)"
        slotInfo = slotInfo.replace(/^\(|\)$/g, '');
        const commaIdx = slotInfo.lastIndexOf(',');
        const lsn = commaIdx >= 0 ? slotInfo.slice(commaIdx + 1) : slotInfo;
        return lsn;
      } catch (err: any) {
        // SQLSTATE 42710: duplicate_object (slot already exists)
        if (err.code !== '42710') {
          throw err;
        }

        const res = await admin.query(
          'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1',
          [slot]
        );
        const lsn: string | null = res.rows[0]?.confirmed_flush_lsn;
        if (!lsn) {
          throw new Error(`Replication slot "${slot}" exists but confirmed_flush_lsn is null`);
        }
        return lsn;
      }
    } finally {
      await admin.end().catch(() => {});
    }
  }

  /**
   * Drops the logical replication slot.
   */
  public async dropSlot(slot = this.slotName): Promise<void> {
    const admin = this.createAdminClient();
    await admin.connect();
    try {
      await admin.query('SELECT pg_drop_replication_slot($1)', [slot]);
    } finally {
      await admin.end().catch(() => {});
    }
  }

  /**
   * Connects to PostgreSQL using replication=database and starts streaming.
   */
  public async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    let startLsn = this.config.startLsn;
    if (!startLsn && this.config.createSlotIfMissing !== false) {
      startLsn = await this.ensureSlot(this.slotName, this.pluginName);
    }
    if (!startLsn) {
      startLsn = '0/0';
    }
    this.lastLsn = startLsn;

    const repConfig: any = this.config.connectionString
      ? {
          connectionString: this.config.connectionString,
          ...this.config.clientConfig,
          replication: 'database',
        }
      : {
          host: this.config.host || 'localhost',
          port: this.config.port || 5432,
          user: this.config.user || 'postgres',
          password: this.config.password,
          database: this.config.database || 'postgres',
          ssl: this.config.ssl,
          ...this.config.clientConfig,
          replication: 'database',
        };

    const client = new PgClient(repConfig);
    this.repClient = client;

    client.on('error', (err) => {
      this.emit('error', err);
    });

    await client.connect();
    const conn = (client as any).connection;

    conn.on('copyData', ({ chunk }: { chunk: Buffer }) => {
      this.handleCopyData(chunk);
    });

    // Build START_REPLICATION query
    let sql = `START_REPLICATION SLOT "${this.slotName}" LOGICAL ${this.lastLsn}`;
    if (this.feedName) {
      const escapedFeed = this.feedName.replace(/'/g, "''");
      sql += ` (feed '${escapedFeed}')`;
    }

    this.isRunning = true;
    this.startStandbyTimer();
    this.emit('start');

    // Issue START_REPLICATION command
    client.query(sql).catch((err: any) => {
      if (this.isRunning) {
        this.emit('error', err);
      }
    });
  }

  /**
   * Internal message handler for incoming replication messages.
   */
  private handleCopyData(chunk: Buffer): void {
    if (!chunk || chunk.length === 0) {
      return;
    }

    const msgType = chunk[0];
    if (msgType === MSG_XLOG_DATA) {
      try {
        const xlog = parseXLogData(chunk);
        const payloadStr = xlog.walData.toString('utf-8');
        const envelope: Envelope = JSON.parse(payloadStr);

        this.lastLsn = envelope.cursor || xlog.walEnd;
        this.emit('envelope', envelope, this.lastLsn);
      } catch (err) {
        this.emit('error', new Error(`Failed to decode change envelope: ${err}`));
      }
    } else if (msgType === MSG_KEEPALIVE) {
      try {
        const keepalive = parsePrimaryKeepalive(chunk);
        if (keepalive.replyRequested) {
          this.ack(keepalive.serverWalEnd).catch((err) => this.emit('error', err));
        }
      } catch (err) {
        this.emit('error', new Error(`Failed to parse primary keepalive: ${err}`));
      }
    }
  }

  /**
   * Sends a Standby Status Update to PostgreSQL acknowledging that WAL up to `lsn`
   * has been flushed and applied. PostgreSQL can advance the replication slot and recycle WAL.
   */
  public async ack(lsn: string = this.lastLsn, replyRequested = false): Promise<void> {
    if (!this.repClient || !this.isRunning) {
      return;
    }

    try {
      const packet = buildStandbyStatusUpdate(lsn, replyRequested);
      const conn = (this.repClient as any).connection;
      if (conn && typeof conn.sendCopyFromChunk === 'function') {
        conn.sendCopyFromChunk(packet);
      }
    } catch (err) {
      this.emit('error', new Error(`Failed to send standby status update: ${err}`));
    }
  }

  /**
   * High-level streaming method: runs an ingestion loop calling the handler for each
   * envelope, and automatically acknowledges the LSN when handler resolves successfully.
   */
  public async stream(
    handler: (envelope: Envelope, lsn: string) => Promise<void> | void
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let isStopping = false;

      const onEnvelope = async (envelope: Envelope, lsn: string) => {
        if (isStopping) return;
        try {
          await handler(envelope, lsn);
          await this.ack(lsn);
        } catch (err) {
          isStopping = true;
          this.off('envelope', onEnvelope);
          this.off('error', onError);
          this.off('close', onClose);
          reject(err);
        }
      };

      const onError = (err: Error) => {
        isStopping = true;
        this.off('envelope', onEnvelope);
        this.off('error', onError);
        this.off('close', onClose);
        reject(err);
      };

      const onClose = () => {
        this.off('envelope', onEnvelope);
        this.off('error', onError);
        this.off('close', onClose);
        resolve();
      };

      this.on('envelope', onEnvelope);
      this.on('error', onError);
      this.on('close', onClose);

      this.start().catch((err) => {
        onError(err);
      });
    });
  }

  /**
   * Implements AsyncIterable for use with `for await (const { envelope, lsn } of client)`.
   */
  public async *[Symbol.asyncIterator](): AsyncIterableIterator<{
    envelope: Envelope;
    lsn: string;
  }> {
    const queue: { envelope: Envelope; lsn: string }[] = [];
    let error: Error | null = null;
    let notify: (() => void) | null = null;

    const onEnvelope = (envelope: Envelope, lsn: string) => {
      queue.push({ envelope, lsn });
      if (notify) {
        const cb = notify;
        notify = null;
        cb();
      }
    };

    const onError = (err: Error) => {
      error = err;
      if (notify) {
        const cb = notify;
        notify = null;
        cb();
      }
    };

    this.on('envelope', onEnvelope);
    this.on('error', onError);

    await this.start();

    try {
      while (this.isRunning) {
        if (error) {
          throw error;
        }
        if (queue.length > 0) {
          yield queue.shift()!;
          continue;
        }
        await new Promise<void>((res) => {
          notify = res;
        });
      }
    } finally {
      this.off('envelope', onEnvelope);
      this.off('error', onError);
    }
  }

  private startStandbyTimer(): void {
    this.stopStandbyTimer();
    this.standbyTimer = setInterval(() => {
      if (this.isRunning && this.lastLsn) {
        this.ack(this.lastLsn).catch((err) => this.emit('error', err));
      }
    }, this.standbyIntervalMs);
  }

  private stopStandbyTimer(): void {
    if (this.standbyTimer) {
      clearInterval(this.standbyTimer);
      this.standbyTimer = null;
    }
  }

  /**
   * Closes the replication connection and releases all resources.
   */
  public async close(): Promise<void> {
    this.isRunning = false;
    this.stopStandbyTimer();

    if (this.repClient) {
      const client = this.repClient;
      this.repClient = null;
      try {
        await client.end();
      } catch {}
    }

    this.emit('close');
  }
}
