/**
 * ChronosProxy — Deterministic JSON Structural Diff & Redaction Engine
 *
 * Compares recorded HTTP responses and database mutations against time-travel
 * replayed responses, filtering out volatile fields (timestamps, request IDs)
 * and redacting sensitive tokens before persistence.
 */

import type {
  JsonPropertyDiff,
  ReplayDriftReport,
  RowMutationDelta,
  TimelineFrame,
} from './types.js';

const DEFAULT_VOLATILE_JSON_KEYS = new Set([
  'timestamp',
  'requestedAt',
  'processedAt',
  'serverTime',
  'requestId',
  'traceId',
]);

export class DiffAndRedactionEngine {
  private readonly redactKeySet: Set<string>;

  constructor(redactKeys: string[]) {
    this.redactKeySet = new Set(redactKeys.map((k) => k.toLowerCase()));
  }

  /**
   * Redacts sensitive HTTP headers (`Authorization`, `Cookie`, `X-Api-Key`, etc.)
   * while preserving header scheme prefixes (e.g., `Bearer [REDACTED]`) for debugging.
   */
  public redactHeaders(
    headers: Record<string, string | string[] | undefined>
  ): Record<string, string | string[]> {
    const out: Record<string, string | string[]> = {};
    for (const [key, val] of Object.entries(headers)) {
      if (val === undefined) continue;
      if (this.shouldRedactKey(key)) {
        if (typeof val === 'string' && val.startsWith('Bearer ')) {
          out[key] = 'Bearer [REDACTED_BY_CHRONOS]';
        } else {
          out[key] = '[REDACTED_BY_CHRONOS]';
        }
      } else {
        out[key] = val;
      }
    }
    return out;
  }

  /**
   * Redacts sensitive keys inside JSON request/response payloads recursively.
   */
  public redactBodyText(bodyText: string, contentType: string): string {
    if (!bodyText || !contentType.toLowerCase().includes('json')) {
      return bodyText;
    }
    try {
      const parsed: unknown = JSON.parse(bodyText);
      const redacted = this.redactValueRecursive(parsed);
      return JSON.stringify(redacted, null, 2);
    } catch {
      return bodyText;
    }
  }

  private redactValueRecursive(val: unknown): unknown {
    if (Array.isArray(val)) {
      return val.map((item) => this.redactValueRecursive(item));
    }
    if (typeof val === 'object' && val !== null) {
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(val)) {
        if (this.shouldRedactKey(k)) {
          out[k] = '[REDACTED_BY_CHRONOS]';
        } else {
          out[k] = this.redactValueRecursive(v);
        }
      }
      return out;
    }
    return val;
  }

  private shouldRedactKey(key: string): boolean {
    const lower = key.toLowerCase();
    if (this.redactKeySet.has(lower)) return true;
    for (const sensitive of this.redactKeySet) {
      if (lower.includes(sensitive)) return true;
    }
    return false;
  }

  /**
   * Computes deep structural JSON property diffs between two JSON strings or objects.
   */
  public static diffJsonPayloads(
    recordedText: string,
    replayedText: string,
    ignoreVolatile = true
  ): JsonPropertyDiff[] {
    let objA: unknown;
    let objB: unknown;
    try {
      objA = recordedText ? JSON.parse(recordedText) : null;
    } catch {
      objA = recordedText;
    }
    try {
      objB = replayedText ? JSON.parse(replayedText) : null;
    } catch {
      objB = replayedText;
    }

    const diffs: JsonPropertyDiff[] = [];
    DiffAndRedactionEngine.walkDiff('$', objA, objB, ignoreVolatile, diffs);
    return diffs;
  }

  private static walkDiff(
    currentPath: string,
    a: unknown,
    b: unknown,
    ignoreVolatile: boolean,
    out: JsonPropertyDiff[]
  ): void {
    if (Object.is(a, b)) return;

    if (a === undefined || b === undefined) {
      out.push({
        path: currentPath,
        kind: a === undefined ? 'added' : 'removed',
        recordedValue: a,
        replayedValue: b,
      });
      return;
    }

    // Handle nullable primitive transitions vs structural type mismatches accurately
    const isStructA = typeof a === 'object' && a !== null;
    const isStructB = typeof b === 'object' && b !== null;
    if (
      isStructA !== isStructB ||
      Array.isArray(a) !== Array.isArray(b) ||
      (!isStructA && a !== null && b !== null && typeof a !== typeof b)
    ) {
      out.push({
        path: currentPath,
        kind: 'type_mismatch',
        recordedValue: a,
        replayedValue: b,
      });
      return;
    }

    if (!isStructA || !isStructB) {
      out.push({
        path: currentPath,
        kind: 'changed',
        recordedValue: a,
        replayedValue: b,
      });
      return;
    }

    if (Array.isArray(a) && Array.isArray(b)) {
      const maxLen = Math.max(a.length, b.length);
      for (let i = 0; i < maxLen; i++) {
        if (i >= a.length) {
          out.push({
            path: `${currentPath}[${i}]`,
            kind: 'added',
            recordedValue: undefined,
            replayedValue: b[i],
          });
        } else if (i >= b.length) {
          out.push({
            path: `${currentPath}[${i}]`,
            kind: 'removed',
            recordedValue: a[i],
            replayedValue: undefined,
          });
        } else {
          DiffAndRedactionEngine.walkDiff(
            `${currentPath}[${i}]`,
            a[i],
            b[i],
            ignoreVolatile,
            out
          );
        }
      }
      return;
    }

    const recA = a as Record<string, unknown>;
    const recB = b as Record<string, unknown>;
    const allKeys = new Set([...Object.keys(recA), ...Object.keys(recB)]);

    for (const key of Array.from(allKeys).sort()) {
      if (ignoreVolatile && DEFAULT_VOLATILE_JSON_KEYS.has(key)) {
        continue;
      }
      const nextPath = `${currentPath}.${key}`;
      if (!(key in recA)) {
        out.push({
          path: nextPath,
          kind: 'added',
          recordedValue: undefined,
          replayedValue: recB[key],
        });
      } else if (!(key in recB)) {
        out.push({
          path: nextPath,
          kind: 'removed',
          recordedValue: recA[key],
          replayedValue: undefined,
        });
      } else {
        DiffAndRedactionEngine.walkDiff(
          nextPath,
          recA[key],
          recB[key],
          ignoreVolatile,
          out
        );
      }
    }
  }

  /**
   * Compares an original recorded TimelineFrame against a time-travel replayed TimelineFrame.
   */
  public static buildReplayDriftReport(
    original: TimelineFrame,
    replayed: TimelineFrame
  ): ReplayDriftReport {
    const statusCodeMatch = original.statusCode === replayed.statusCode;
    const bodyDiffs = DiffAndRedactionEngine.diffJsonPayloads(
      original.response.bodyText,
      replayed.response.bodyText,
      true
    );

    const dbDriftSummaries = DiffAndRedactionEngine.compareRowMutations(
      original.dbDiff.rowMutations,
      replayed.dbDiff.rowMutations
    );

    const deterministicMatch =
      statusCodeMatch &&
      bodyDiffs.length === 0 &&
      dbDriftSummaries.length === 0;

    return {
      originalFrameId: original.id,
      replayFrameId: replayed.id,
      replayedAt: replayed.timestamp,
      deterministicMatch,
      statusCodeMatch,
      recordedStatus: original.statusCode,
      replayedStatus: replayed.statusCode,
      latencyDeltaMs: Number(
        (replayed.latency.totalMs - original.latency.totalMs).toFixed(2)
      ),
      bodyDiffs,
      dbMutationDrift: {
        hasDrift: dbDriftSummaries.length > 0,
        recordedMutationCount: original.dbDiff.rowMutations.length,
        replayedMutationCount: replayed.dbDiff.rowMutations.length,
        summary: dbDriftSummaries,
      },
    };
  }

  /**
   * Uses multiset frequency counting so duplicate row mutations in the same frame
   * are compared with exact cardinality.
   */
  private static compareRowMutations(
    recorded: RowMutationDelta[],
    replayed: RowMutationDelta[]
  ): string[] {
    const drift: string[] = [];
    const formatSig = (m: RowMutationDelta): string =>
      `${m.table}:${m.kind}:${m.primaryKey}:${JSON.stringify(m.changes)}`;

    const recCounts = new Map<string, number>();
    const repCounts = new Map<string, number>();

    for (const m of recorded) {
      const sig = formatSig(m);
      recCounts.set(sig, (recCounts.get(sig) ?? 0) + 1);
    }
    for (const m of replayed) {
      const sig = formatSig(m);
      repCounts.set(sig, (repCounts.get(sig) ?? 0) + 1);
    }

    for (const [sig, recCount] of recCounts.entries()) {
      const repCount = repCounts.get(sig) ?? 0;
      if (repCount < recCount) {
        drift.push(
          `Missing recorded DB mutation on replay (${recCount - repCount}x): ${sig}`
        );
      }
    }
    for (const [sig, repCount] of repCounts.entries()) {
      const recCount = recCounts.get(sig) ?? 0;
      if (repCount > recCount) {
        drift.push(
          `Unexpected new DB mutation on replay (${repCount - recCount}x): ${sig}`
        );
      }
    }
    return drift;
  }
}
