/**
 * ChronosProxy — Copy-on-Write (CoW) SQLite Binary Page & Logical Snapshot Engine
 *
 * Implements content-addressable page-level deduplication for SQLite3 files
 * alongside logical table state tracking, reference-counted page GC, and atomic
 * point-in-time restoration.
 */

import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import type {
  ColumnValueChange,
  LogicalTableRows,
  PageChunkRef,
  RowMutationDelta,
  SqliteBinaryHeader,
  SqliteSnapshot,
  SqliteStateDiff,
} from './types.js';

const SQLITE3_MAGIC = 'SQLite format 3\0';
const DEFAULT_PAGE_SIZE = 4096;

export class CoWSqliteEngine {
  private readonly dbPath: string;
  /** Content-addressable page store: sha256 -> raw page Buffer */
  private readonly pageStore = new Map<string, Buffer>();
  /** Snapshot history indexed by snapshotId */
  private readonly snapshots = new Map<string, SqliteSnapshot>();
  /** Ordered list of snapshot IDs */
  private readonly snapshotOrder: string[] = [];
  /** Optional logical sidecar state for instant row-level inspection */
  private currentLogicalState: LogicalTableRows = {};
  private seqCounter = 0;

  constructor(dbPath: string, initialLogicalState?: LogicalTableRows) {
    this.dbPath = path.resolve(dbPath);
    if (initialLogicalState) {
      this.currentLogicalState = structuredClone(initialLogicalState);
    }
  }

  /**
   * Parses the 100-byte SQLite3 binary file header per https://www.sqlite.org/fileformat2.html
   */
  public static parseBinaryHeader(buf: Buffer): SqliteBinaryHeader {
    if (buf.length < 100) {
      return {
        validMagic: false,
        pageSize: DEFAULT_PAGE_SIZE,
        changeCounter: 0,
        pageCount: Math.ceil(buf.length / DEFAULT_PAGE_SIZE),
        schemaCookie: 0,
        textEncoding: 1,
        walMode: false,
      };
    }

    const magic = buf.subarray(0, 16).toString('latin1');
    const validMagic = magic === SQLITE3_MAGIC;
    const rawPageSize = buf.readUInt16BE(16);
    const pageSize =
      rawPageSize === 1
        ? 65536
        : rawPageSize >= 512 && (rawPageSize & (rawPageSize - 1)) === 0
          ? rawPageSize
          : DEFAULT_PAGE_SIZE;

    const writeVersion = buf.readUInt8(18);
    const readVersion = buf.readUInt8(19);
    const changeCounter = buf.readUInt32BE(24);
    const headerPageCount = buf.readUInt32BE(28);
    const pageCount =
      headerPageCount > 0
        ? headerPageCount
        : Math.ceil(buf.length / pageSize);
    const schemaCookie = buf.readUInt32BE(40);
    const rawEncoding = buf.readUInt32BE(56);
    const textEncoding: 1 | 2 | 3 =
      rawEncoding === 2 ? 2 : rawEncoding === 3 ? 3 : 1;

    return {
      validMagic,
      pageSize,
      changeCounter,
      pageCount,
      schemaCookie,
      textEncoding,
      walMode: writeVersion === 2 || readVersion === 2,
    };
  }

  /**
   * Creates a valid minimal SQLite3 binary file header + page structure on disk
   * so tests and local demo databases have authentic SQLite3 binary page headers.
   */
  public static createSyntheticSqliteBuffer(
    logicalTables: LogicalTableRows,
    changeCounter: number,
    schemaCookie = 1,
    pageSize = DEFAULT_PAGE_SIZE
  ): Buffer {
    const payloadJson = Buffer.from(JSON.stringify(logicalTables), 'utf8');
    const totalPayloadWithHeader = 100 + 4 + payloadJson.length;
    const pageCount = Math.max(1, Math.ceil(totalPayloadWithHeader / pageSize));
    const totalBytes = pageCount * pageSize;
    const buf = Buffer.alloc(totalBytes, 0);

    buf.write(SQLITE3_MAGIC, 0, 'latin1');
    buf.writeUInt16BE(pageSize === 65536 ? 1 : pageSize, 16);
    buf.writeUInt8(2, 18);
    buf.writeUInt8(2, 19);
    buf.writeUInt8(64, 21);
    buf.writeUInt8(32, 22);
    buf.writeUInt8(32, 23);
    buf.writeUInt32BE(changeCounter >>> 0, 24);
    buf.writeUInt32BE(pageCount >>> 0, 28);
    buf.writeUInt32BE(schemaCookie >>> 0, 40);
    buf.writeUInt32BE(4, 44);
    buf.writeUInt32BE(1, 56);
    buf.writeUInt32BE(changeCounter >>> 0, 92);
    buf.writeUInt32BE(3045000, 96);

    buf.writeUInt32BE(payloadJson.length, 100);
    payloadJson.copy(buf, 104);

    return buf;
  }

  private extractLogicalTablesFromBuffer(buf: Buffer): LogicalTableRows | null {
    if (buf.length < 104) return null;
    const jsonLen = buf.readUInt32BE(100);
    if (jsonLen === 0 || 104 + jsonLen > buf.length) return null;
    try {
      const raw = buf.subarray(104, 104 + jsonLen).toString('utf8');
      if (!raw.startsWith('{')) return null;
      return JSON.parse(raw) as LogicalTableRows;
    } catch {
      return null;
    }
  }

  public applyLogicalMutation(
    mutator: (tables: LogicalTableRows) => void
  ): void {
    const nextState = structuredClone(this.currentLogicalState);
    mutator(nextState);
    this.currentLogicalState = nextState;

    let nextChangeCounter = 1;
    if (fs.existsSync(this.dbPath)) {
      const existingBuf = fs.readFileSync(this.dbPath);
      const header = CoWSqliteEngine.parseBinaryHeader(existingBuf);
      nextChangeCounter = header.changeCounter + 1;
    }

    const updatedBuf = CoWSqliteEngine.createSyntheticSqliteBuffer(
      this.currentLogicalState,
      nextChangeCounter
    );
    fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
    const tmpPath = `${this.dbPath}.chronos.tmp.${process.pid}`;
    fs.writeFileSync(tmpPath, updatedBuf);
    fs.renameSync(tmpPath, this.dbPath);
  }

  public getLogicalState(): LogicalTableRows {
    return structuredClone(this.currentLogicalState);
  }

  /**
   * Captures a Copy-on-Write page-level + logical snapshot of the SQLite file.
   * Tracks `mainDbSizeBytes` distinctly from optional WAL sidecar frames so
   * `restoreSnapshot()` never corrupts page alignment on the main database file.
   */
  public captureSnapshot(): SqliteSnapshot {
    let mainBuf: Buffer;
    let rawBuf: Buffer;
    if (fs.existsSync(this.dbPath)) {
      mainBuf = fs.readFileSync(this.dbPath);
      rawBuf = mainBuf;
      const walPath = `${this.dbPath}-wal`;
      if (fs.existsSync(walPath)) {
        const walBuf = fs.readFileSync(walPath);
        if (walBuf.length > 0) {
          rawBuf = Buffer.concat([mainBuf, walBuf]);
        }
      }
    } else {
      mainBuf = CoWSqliteEngine.createSyntheticSqliteBuffer(
        this.currentLogicalState,
        1
      );
      rawBuf = mainBuf;
      fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
      fs.writeFileSync(this.dbPath, mainBuf);
    }

    const header = CoWSqliteEngine.parseBinaryHeader(mainBuf);
    const pageSize = header.pageSize;
    const pages: PageChunkRef[] = [];

    const prevSnapshotId =
      this.snapshotOrder.length > 0
        ? this.snapshotOrder[this.snapshotOrder.length - 1]
        : undefined;
    const prevSnapshot = prevSnapshotId
      ? this.snapshots.get(prevSnapshotId)
      : undefined;

    let dirtyPageCount = 0;
    const totalPages = Math.max(1, Math.ceil(rawBuf.length / pageSize));

    for (let i = 0; i < totalPages; i++) {
      const byteOffset = i * pageSize;
      const pageSlice = rawBuf.subarray(
        byteOffset,
        Math.min(byteOffset + pageSize, rawBuf.length)
      );
      const pageHash = crypto
        .createHash('sha256')
        .update(pageSlice)
        .digest('hex');

      if (!this.pageStore.has(pageHash)) {
        this.pageStore.set(pageHash, Buffer.from(pageSlice));
      }

      const prevPageHash = prevSnapshot?.pages[i]?.sha256;
      if (prevPageHash !== pageHash) {
        dirtyPageCount++;
      }

      pages.push({
        pageIndex: i,
        sha256: pageHash,
        byteOffset,
      });
    }

    const extractedLogical = this.extractLogicalTablesFromBuffer(mainBuf);
    if (extractedLogical) {
      this.currentLogicalState = extractedLogical;
    }

    const manifestHash = crypto
      .createHash('sha256')
      .update(pages.map((p) => `${p.pageIndex}:${p.sha256}`).join('|'))
      .update(JSON.stringify(this.currentLogicalState))
      .digest('hex')
      .slice(0, 16);

    const existing = this.snapshots.get(manifestHash);
    if (existing) {
      return existing;
    }

    this.seqCounter++;
    const snapshot: SqliteSnapshot = {
      snapshotId: manifestHash,
      seq: this.seqCounter,
      capturedAt: new Date().toISOString(),
      mainDbSizeBytes: mainBuf.length,
      fileSizeBytes: rawBuf.length,
      header,
      pages,
      dirtyPageCount,
      logicalTables: structuredClone(this.currentLogicalState),
    };

    this.snapshots.set(manifestHash, snapshot);
    this.snapshotOrder.push(manifestHash);
    return snapshot;
  }

  /**
   * Atomically restores the SQLite database on disk and in-memory logical state
   * to the exact byte-for-byte state of `snapshotId`, truncating strictly to
   * `mainDbSizeBytes` so folded WAL sidecar pages do not corrupt the main DB file.
   */
  public restoreSnapshot(snapshotId: string): SqliteSnapshot {
    const snap = this.snapshots.get(snapshotId);
    if (!snap) {
      throw new Error(`Unknown SQLite snapshot ID: ${snapshotId}`);
    }

    const pageBuffers: Buffer[] = [];
    for (const pageRef of snap.pages) {
      const chunk = this.pageStore.get(pageRef.sha256);
      if (!chunk) {
        throw new Error(
          `Corrupt CoW page store: missing page chunk ${pageRef.sha256} (page ${pageRef.pageIndex})`
        );
      }
      pageBuffers.push(chunk);
    }

    const fullReconstructed = Buffer.concat(pageBuffers, snap.fileSizeBytes);
    const mainDbBytes = fullReconstructed.subarray(0, snap.mainDbSizeBytes);
    fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });

    const tempFile = `${this.dbPath}.restore.${process.pid}.${Date.now()}`;
    fs.writeFileSync(tempFile, mainDbBytes);
    fs.renameSync(tempFile, this.dbPath);

    for (const suffix of ['-wal', '-shm']) {
      const walOrShm = `${this.dbPath}${suffix}`;
      if (fs.existsSync(walOrShm)) {
        fs.unlinkSync(walOrShm);
      }
    }

    this.currentLogicalState = structuredClone(snap.logicalTables);
    return snap;
  }

  /**
   * Reference-counted Garbage Collector for CoW Snapshots & 4KB Page Chunks.
   * Evicts snapshots not in `retainedSnapshotIds` and frees any 4KB page chunk
   * whose reference count drops to zero.
   */
  public pruneSnapshots(retainedSnapshotIds: Set<string>): {
    prunedSnapshots: number;
    prunedPages: number;
  } {
    let prunedSnapshots = 0;
    for (const snapId of Array.from(this.snapshots.keys())) {
      if (!retainedSnapshotIds.has(snapId)) {
        this.snapshots.delete(snapId);
        prunedSnapshots++;
      }
    }

    for (let i = this.snapshotOrder.length - 1; i >= 0; i--) {
      if (!retainedSnapshotIds.has(this.snapshotOrder[i])) {
        this.snapshotOrder.splice(i, 1);
      }
    }

    const activePageHashes = new Set<string>();
    for (const snap of this.snapshots.values()) {
      for (const page of snap.pages) {
        activePageHashes.add(page.sha256);
      }
    }

    let prunedPages = 0;
    for (const pageHash of Array.from(this.pageStore.keys())) {
      if (!activePageHashes.has(pageHash)) {
        this.pageStore.delete(pageHash);
        prunedPages++;
      }
    }

    return { prunedSnapshots, prunedPages };
  }

  public diffSnapshots(
    fromSnapshotId: string,
    toSnapshotId: string
  ): SqliteStateDiff {
    const fromSnap = this.snapshots.get(fromSnapshotId);
    const toSnap = this.snapshots.get(toSnapshotId);

    if (!fromSnap || !toSnap) {
      throw new Error(
        `Cannot diff unknown snapshots (${fromSnapshotId} -> ${toSnapshotId})`
      );
    }

    const dirtyPageIndices: number[] = [];
    const maxPages = Math.max(fromSnap.pages.length, toSnap.pages.length);
    for (let i = 0; i < maxPages; i++) {
      const hashA = fromSnap.pages[i]?.sha256;
      const hashB = toSnap.pages[i]?.sha256;
      if (hashA !== hashB) {
        dirtyPageIndices.push(i);
      }
    }

    const rowMutations = CoWSqliteEngine.computeLogicalRowDiffs(
      fromSnap.logicalTables,
      toSnap.logicalTables
    );

    return {
      fromSnapshotId,
      toSnapshotId,
      headerChanged:
        fromSnap.header.changeCounter !== toSnap.header.changeCounter ||
        fromSnap.header.schemaCookie !== toSnap.header.schemaCookie,
      changeCounterDelta:
        toSnap.header.changeCounter - fromSnap.header.changeCounter,
      pageCountDelta: toSnap.header.pageCount - fromSnap.header.pageCount,
      dirtyPageIndices,
      dirtyBytes: dirtyPageIndices.length * toSnap.header.pageSize,
      rowMutations,
    };
  }

  public static computeLogicalRowDiffs(
    beforeTables: LogicalTableRows,
    afterTables: LogicalTableRows
  ): RowMutationDelta[] {
    const deltas: RowMutationDelta[] = [];
    const allTableNames = new Set([
      ...Object.keys(beforeTables),
      ...Object.keys(afterTables),
    ]);

    for (const table of Array.from(allTableNames).sort()) {
      const beforeRows = beforeTables[table] ?? [];
      const afterRows = afterTables[table] ?? [];

      const beforeByPk = new Map<string, Record<string, unknown>>();
      const afterByPk = new Map<string, Record<string, unknown>>();

      beforeRows.forEach((row, idx) => {
        const pk = CoWSqliteEngine.extractPrimaryKey(row, idx);
        beforeByPk.set(pk, row);
      });
      afterRows.forEach((row, idx) => {
        const pk = CoWSqliteEngine.extractPrimaryKey(row, idx);
        afterByPk.set(pk, row);
      });

      for (const [pk, afterRow] of afterByPk.entries()) {
        const beforeRow = beforeByPk.get(pk);
        if (!beforeRow) {
          const changes: ColumnValueChange[] = Object.keys(afterRow).map(
            (col) => ({
              column: col,
              before: undefined,
              after: afterRow[col],
            })
          );
          deltas.push({
            table,
            kind: 'INSERT',
            primaryKey: pk,
            changes,
            rawAfter: structuredClone(afterRow),
          });
        } else {
          const cols = new Set([
            ...Object.keys(beforeRow),
            ...Object.keys(afterRow),
          ]);
          const colChanges: ColumnValueChange[] = [];
          for (const col of cols) {
            if (!CoWSqliteEngine.deepEqual(beforeRow[col], afterRow[col])) {
              colChanges.push({
                column: col,
                before: beforeRow[col],
                after: afterRow[col],
              });
            }
          }
          if (colChanges.length > 0) {
            deltas.push({
              table,
              kind: 'UPDATE',
              primaryKey: pk,
              changes: colChanges,
              rawBefore: structuredClone(beforeRow),
              rawAfter: structuredClone(afterRow),
            });
          }
        }
      }

      for (const [pk, beforeRow] of beforeByPk.entries()) {
        if (!afterByPk.has(pk)) {
          const changes: ColumnValueChange[] = Object.keys(beforeRow).map(
            (col) => ({
              column: col,
              before: beforeRow[col],
              after: undefined,
            })
          );
          deltas.push({
            table,
            kind: 'DELETE',
            primaryKey: pk,
            changes,
            rawBefore: structuredClone(beforeRow),
          });
        }
      }
    }

    return deltas;
  }

  private static extractPrimaryKey(
    row: Record<string, unknown>,
    fallbackIndex: number
  ): string {
    for (const candidate of ['id', 'uuid', 'pk', 'key', 'code', 'slug']) {
      if (row[candidate] !== undefined && row[candidate] !== null) {
        return String(row[candidate]);
      }
    }
    return `row_${fallbackIndex}`;
  }

  private static deepEqual(a: unknown, b: unknown): boolean {
    if (Object.is(a, b)) return true;
    if (
      typeof a !== 'object' ||
      a === null ||
      typeof b !== 'object' ||
      b === null
    ) {
      return false;
    }
    return JSON.stringify(a) === JSON.stringify(b);
  }

  public getSnapshot(snapshotId: string): SqliteSnapshot | undefined {
    return this.snapshots.get(snapshotId);
  }

  public getAllSnapshots(): SqliteSnapshot[] {
    return this.snapshotOrder
      .map((id) => this.snapshots.get(id))
      .filter((s): s is SqliteSnapshot => Boolean(s));
  }

  public getStorageStats(): {
    uniquePagesStored: number;
    physicalStoreBytes: number;
    logicalSnapshotBytes: number;
    deduplicationRatio: number;
  } {
    let physicalStoreBytes = 0;
    for (const buf of this.pageStore.values()) {
      physicalStoreBytes += buf.length;
    }
    let logicalSnapshotBytes = 0;
    for (const snap of this.snapshots.values()) {
      logicalSnapshotBytes += snap.fileSizeBytes;
    }
    const deduplicationRatio =
      physicalStoreBytes > 0
        ? Number((logicalSnapshotBytes / physicalStoreBytes).toFixed(2))
        : 1;
    return {
      uniquePagesStored: this.pageStore.size,
      physicalStoreBytes,
      logicalSnapshotBytes,
      deduplicationRatio,
    };
  }
}
