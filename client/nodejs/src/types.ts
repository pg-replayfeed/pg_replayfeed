import { ClientConfig } from 'pg';

/**
 * Mutation operation type in a change record.
 * "I" = Insert, "U" = Update, "D" = Delete.
 */
export type Op = 'I' | 'U' | 'D';

/**
 * A single row key mutation inside an envelope.
 */
export interface Change {
  feed: string;
  op: Op;
  key: string;
  tenant?: string | null;
  old_tenant?: string | null;
}

/**
 * Transaction-atomic change envelope emitted by pg_replayfeed.
 * Each committed transaction touching one or more registered feeds emits exactly one envelope.
 */
export interface Envelope {
  /**
   * Commit Log Sequence Number (LSN) of the transaction, e.g. "0/16B1978".
   * This is checkpointed by the consumer to acknowledge processing.
   */
  cursor: string;

  /**
   * Millisecond Unix epoch timestamp when the transaction committed.
   */
  committed_at_ms: number;

  /**
   * List of key mutations in this transaction.
   */
  changes: Change[];

  /**
   * List of feeds that were truncated in this transaction (resnapshot marker).
   */
  truncate: string[];
}

/**
 * Configuration options for ReplayFeedClient.
 */
export interface ReplayFeedConfig {
  /**
   * Standard PostgreSQL connection string (e.g. postgres://user:pass@host:5432/db).
   * Overrides individual host/port/user fields if set.
   */
  connectionString?: string;

  /**
   * Database host (defaults to "localhost").
   */
  host?: string;

  /**
   * Database port (defaults to 5432).
   */
  port?: number;

  /**
   * Database user (defaults to "postgres").
   */
  user?: string;

  /**
   * Database password.
   */
  password?: string;

  /**
   * Database name.
   */
  database?: string;

  /**
   * SSL configuration passed to node-postgres.
   */
  ssl?: boolean | object;

  /**
   * Name of the PostgreSQL logical replication slot (defaults to "replayfeed_slot").
   */
  slotName?: string;

  /**
   * Name of the logical decoding plugin (defaults to "pg_replayfeed").
   */
  plugin?: string;

  /**
   * Optional feed filter. If specified, only changes for this feed are streamed.
   */
  feed?: string;

  /**
   * Starting LSN position (e.g. "0/16B1978").
   * If omitted, resumes from the slot's confirmed flush position or current WAL point.
   */
  startLsn?: string;

  /**
   * Automatically create the logical slot if it does not already exist (defaults to true).
   */
  createSlotIfMissing?: boolean;

  /**
   * Interval in milliseconds for sending standby status updates to PostgreSQL when idle.
   * Defaults to 10,000 ms (10 seconds).
   */
  standbyIntervalMs?: number;

  /**
   * Additional pg.Client options.
   */
  clientConfig?: Partial<ClientConfig>;
}
