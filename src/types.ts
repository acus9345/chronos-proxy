/**
 * ChronosProxy — Core TypeScript Domain Model
 *
 * Defines the data structures for TimelineFrames, Copy-on-Write (CoW) SQLite
 * binary page snapshots, logical table row deltas, and deterministic replay drift reports.
 */

export type HttpMethod =
  | 'GET'
  | 'POST'
  | 'PUT'
  | 'PATCH'
  | 'DELETE'
  | 'HEAD'
  | 'OPTIONS';

export type FrameProtocol = 'HTTP/1.1' | 'WS';

export interface LatencyBreakdown {
  /** Proxy pre-request SQLite snapshot capture duration in milliseconds */
  preSnapshotMs: number;
  /** Time to first byte from upstream target in milliseconds */
  upstreamTtfbMs: number;
  /** Response stream transfer duration in milliseconds */
  transferMs: number;
  /** Post-request SQLite snapshot + diff computation duration in milliseconds */
  postSnapshotMs: number;
  /** Total end-to-end wall time in milliseconds */
  totalMs: number;
}

export interface SqliteBinaryHeader {
  /** True if the file starts with "SQLite format 3\0" */
  validMagic: boolean;
  /** Page size in bytes (512..65536) */
  pageSize: number;
  /** File change counter at byte offset 24 (incremented on every SQLite commit) */
  changeCounter: number;
  /** Database size in pages at byte offset 28 */
  pageCount: number;
  /** Schema cookie at byte offset 40 */
  schemaCookie: number;
  /** Text encoding: 1 = UTF-8, 2 = UTF-16le, 3 = UTF-16be */
  textEncoding: 1 | 2 | 3;
  /** Write-Ahead Log (WAL) mode indicator (readVersion === 2 || writeVersion === 2) */
  walMode: boolean;
}

export interface PageChunkRef {
  /** Zero-based SQLite page index */
  pageIndex: number;
  /** SHA-256 hex digest of the page content */
  sha256: string;
  /** Byte offset in the SQLite file */
  byteOffset: number;
}

export type LogicalTableRows = Record<string, Record<string, unknown>[]>;

export interface SqliteSnapshot {
  /** Content-addressable snapshot ID (SHA-256 of page manifest + logical state) */
  snapshotId: string;
  /** Monotonic sequence index */
  seq: number;
  /** ISO-8601 timestamp when captured */
  capturedAt: string;
  /** Main database file size in bytes (excluding WAL sidecar bytes) */
  mainDbSizeBytes: number;
  /** Total captured bytes including any folded WAL frames */
  fileSizeBytes: number;
  /** Parsed SQLite3 100-byte header metadata */
  header: SqliteBinaryHeader;
  /** Ordered page chunk references enabling Copy-on-Write reconstruction */
  pages: PageChunkRef[];
  /** Number of newly allocated/dirty pages compared to parent snapshot */
  dirtyPageCount: number;
  /** Optional logical table rows for structured row-level diff inspection */
  logicalTables: LogicalTableRows;
}

export type MutationKind = 'INSERT' | 'UPDATE' | 'DELETE';

export interface ColumnValueChange {
  column: string;
  before: unknown;
  after: unknown;
}

export interface RowMutationDelta {
  table: string;
  kind: MutationKind;
  primaryKey: string;
  changes: ColumnValueChange[];
  rawBefore?: Record<string, unknown>;
  rawAfter?: Record<string, unknown>;
}

export interface SqliteStateDiff {
  fromSnapshotId: string;
  toSnapshotId: string;
  headerChanged: boolean;
  changeCounterDelta: number;
  pageCountDelta: number;
  dirtyPageIndices: number[];
  dirtyBytes: number;
  rowMutations: RowMutationDelta[];
}

export interface HttpMessageRecord {
  headers: Record<string, string | string[]>;
  bodyText: string;
  bodyBytes: number;
  bodyTruncated: boolean;
  contentType: string;
}

export interface TimelineFrame {
  /** Unique deterministic frame identifier (e.g., "frm_0001_9a8f2c") */
  id: string;
  /** Strictly increasing 1-based frame sequence number */
  seq: number;
  /** Wall-clock ISO timestamp */
  timestamp: string;
  /** High-resolution monotonic clock reading in nanoseconds */
  hrtimeNs: string;
  /** Protocol intercepted */
  protocol: FrameProtocol;
  /** HTTP method */
  method: HttpMethod;
  /** Request path + query string */
  path: string;
  /** Target upstream origin */
  upstreamOrigin: string;
  /** HTTP response status code */
  statusCode: number;
  /** Recorded request details (with sensitive fields redacted) */
  request: HttpMessageRecord;
  /** Recorded response details */
  response: HttpMessageRecord;
  /** Sub-millisecond latency breakdown */
  latency: LatencyBreakdown;
  /** Snapshot ID before the request hit the upstream server */
  preSnapshotId: string;
  /** Snapshot ID immediately after the upstream response completed */
  postSnapshotId: string;
  /** Computed database mutations caused specifically by this request */
  dbDiff: SqliteStateDiff;
  /** User-attached bookmark or annotation tag */
  tag?: string;
  /** Indicates whether this frame was generated via a time-travel replay */
  replayedFromFrameId?: string;
}

export interface JsonPropertyDiff {
  path: string;
  kind: 'added' | 'removed' | 'changed' | 'type_mismatch';
  recordedValue: unknown;
  replayedValue: unknown;
}

export interface ReplayDriftReport {
  originalFrameId: string;
  replayFrameId: string;
  replayedAt: string;
  /** True if status code, normalized JSON body, and DB row mutations are identical */
  deterministicMatch: boolean;
  statusCodeMatch: boolean;
  recordedStatus: number;
  replayedStatus: number;
  latencyDeltaMs: number;
  bodyDiffs: JsonPropertyDiff[];
  dbMutationDrift: {
    hasDrift: boolean;
    recordedMutationCount: number;
    replayedMutationCount: number;
    summary: string[];
  };
}

export interface ChronosProxyConfig {
  /** Port for the ChronosProxy listener (default: 8080) */
  listenPort: number;
  /** Hostname to bind (default: "127.0.0.1") */
  listenHost: string;
  /** Target upstream URL (e.g., "http://127.0.0.1:4000") */
  targetUrl: string;
  /** Path to the SQLite database file to snapshot & time-travel */
  sqlitePath: string;
  /** Maximum request/response body bytes to store per frame (default: 262144) */
  maxBodyBytes: number;
  /** Maximum frames retained in ring buffer (default: 1000) */
  maxFrames: number;
  /** Header and JSON field keys to automatically redact */
  redactKeys: string[];
}
