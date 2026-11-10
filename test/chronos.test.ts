/**
 * ChronosProxy — Automated Integration & Unit Test Suite
 *
 * Tests:
 *  1. SQLite3 100-byte binary header parser & Copy-on-Write 4KB page deduplication
 *  2. Atomic point-in-time SQLite database restoration (`restoreSnapshot`) + WAL alignment
 *  3. Reference-counted CoW Snapshot & Page Garbage Collection (`pruneSnapshots`)
 *  4. Logical table row diff computation (`INSERT`, `UPDATE`, `DELETE`)
 *  5. Recursive header & JSON payload secret redaction + nullable diff accuracy
 *  6. End-to-end HTTP reverse proxy interception, unredacted replay vault preservation,
 *     time-travel checkout, and deterministic replay drift detection.
 */

import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { CoWSqliteEngine } from '../src/sqlite-engine.js';
import { DiffAndRedactionEngine } from '../src/diff-engine.js';
import { startDemoEnvironment } from '../src/cli.js';

describe('ChronosProxy Core Engine & Time-Machine Suite', () => {
  const tempFiles: string[] = [];

  after(() => {
    for (const f of tempFiles) {
      if (fs.existsSync(f)) {
        fs.unlinkSync(f);
      }
    }
  });

  it('parses binary SQLite3 100-byte headers, deduplicates 4KB pages via CoW, and garbage-collects unreferenced pages', () => {
    const dbPath = path.join(os.tmpdir(), `chronos-cow-test-${Date.now()}.sqlite`);
    tempFiles.push(dbPath);

    const engine = new CoWSqliteEngine(dbPath, {
      users: [
        { id: 'usr_1', email: 'ada@analytical.org', credits: 500 },
        { id: 'usr_2', email: 'charles@babbage.org', credits: 250 },
      ],
    });

    const snap1 = engine.captureSnapshot();
    assert.equal(snap1.header.validMagic, true);
    assert.equal(snap1.header.pageSize, 4096);
    assert.equal(snap1.header.walMode, true);
    assert.equal(snap1.pages.length >= 1, true);

    const snap1Duplicate = engine.captureSnapshot();
    assert.equal(snap1Duplicate.snapshotId, snap1.snapshotId);

    engine.applyLogicalMutation((tables) => {
      tables.users[0].credits = 420;
      tables.users.push({ id: 'usr_3', email: 'grace@hopper.org', credits: 900 });
    });

    const snap2 = engine.captureSnapshot();
    assert.notEqual(snap2.snapshotId, snap1.snapshotId);
    assert.equal(snap2.header.changeCounter, snap1.header.changeCounter + 1);

    const diff = engine.diffSnapshots(snap1.snapshotId, snap2.snapshotId);
    assert.equal(diff.headerChanged, true);
    assert.equal(diff.changeCounterDelta, 1);
    assert.equal(diff.rowMutations.length, 2);

    // Restore back to snap1 and verify both logical and disk binary state rolled back
    engine.restoreSnapshot(snap1.snapshotId);
    const restoredLogical = engine.getLogicalState();
    assert.equal(restoredLogical.users.length, 2);
    assert.equal(restoredLogical.users[0].credits, 500);

    const diskHeader = CoWSqliteEngine.parseBinaryHeader(fs.readFileSync(dbPath));
    assert.equal(diskHeader.changeCounter, snap1.header.changeCounter);

    // Prune snap2 and verify unreferenced page chunks are reclaimed
    const gcResult = engine.pruneSnapshots(new Set([snap1.snapshotId]));
    assert.equal(gcResult.prunedSnapshots, 1);
    assert.equal(gcResult.prunedPages >= 1, true);
  });

  it('redacts sensitive headers and nested JSON credentials while computing structural diffs', () => {
    const redactor = new DiffAndRedactionEngine([
      'authorization',
      'cookie',
      'password',
      'client_secret',
    ]);

    const cleanHeaders = redactor.redactHeaders({
      authorization: 'Bearer sk_live_9988776655',
      'content-type': 'application/json',
      cookie: 'sid=secret_cookie_val',
    });

    assert.equal(cleanHeaders.authorization, 'Bearer [REDACTED_BY_CHRONOS]');
    assert.equal(cleanHeaders.cookie, '[REDACTED_BY_CHRONOS]');
    assert.equal(cleanHeaders['content-type'], 'application/json');

    const rawBody = JSON.stringify({
      user: 'ada',
      password: 'plaintext_password',
      nested: { client_secret: 'sec_xyz', publicKey: 'pub_123' },
    });
    const redactedBody = JSON.parse(
      redactor.redactBodyText(rawBody, 'application/json; charset=utf-8')
    );
    assert.equal(redactedBody.password, '[REDACTED_BY_CHRONOS]');
    assert.equal(redactedBody.nested.client_secret, '[REDACTED_BY_CHRONOS]');
    assert.equal(redactedBody.nested.publicKey, 'pub_123');

    // Structural diff ignoring volatile timestamps & handling null->primitive transition as 'changed'
    const diffs = DiffAndRedactionEngine.diffJsonPayloads(
      JSON.stringify({ balance: 100, note: null, timestamp: '2026-01-01T00:00:00Z' }),
      JSON.stringify({ balance: 85, note: 'settled', timestamp: '2026-11-11T05:00:00Z' }),
      true
    );
    assert.equal(diffs.length, 2);
    assert.equal(diffs[0].path, '$.balance');
    assert.equal(diffs[0].kind, 'changed');
    assert.equal(diffs[1].path, '$.note');
    assert.equal(diffs[1].kind, 'changed');
  });

  it('intercepts live HTTP traffic, correlates SQLite mutations, checks out past states, and verifies deterministic replay', async () => {
    const env = await startDemoEnvironment(0);
    tempFiles.push(env.dbPath);

    try {
      const baseUrl = `http://127.0.0.1:${env.proxyPort}`;

      const res1 = await fetch(`${baseUrl}/v1/transfers`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: 'Bearer live_secret_token',
        },
        body: JSON.stringify({
          fromAccount: 'acc_customer_09',
          toAccount: 'acc_merchant_01',
          amountCents: 12000,
          reason: 'INVOICE_881',
        }),
      });
      assert.equal(res1.status, 201);
      const frame1Id = res1.headers.get('x-chronos-frame-id');
      assert.ok(frame1Id);

      const res2 = await fetch(`${baseUrl}/v1/transfers`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          fromAccount: 'acc_customer_09',
          toAccount: 'acc_merchant_01',
          amountCents: 15000,
          reason: 'INVOICE_882',
        }),
      });
      assert.equal(res2.status, 201);
      const frame2Id = res2.headers.get('x-chronos-frame-id');
      assert.ok(frame2Id);

      const frames = env.proxy.getFrames();
      assert.equal(frames.length, 2);
      assert.equal(
        frames[0].request.headers.authorization,
        'Bearer [REDACTED_BY_CHRONOS]'
      );
      assert.equal(frames[0].dbDiff.rowMutations.length, 3);

      // Time-Travel Replay of Frame 1:
      const replayRes = await fetch(
        `${baseUrl}/__chronos/api/replay/${encodeURIComponent(frame1Id)}`,
        { method: 'POST' }
      );
      assert.equal(replayRes.status, 200);
      const replayPayload = (await replayRes.json()) as {
        driftReport: { deterministicMatch: boolean; bodyDiffs: unknown[] };
      };
      assert.equal(replayPayload.driftReport.deterministicMatch, true);
      assert.equal(replayPayload.driftReport.bodyDiffs.length, 0);
    } finally {
      await env.proxy.stop();
      await new Promise<void>((resolve) => env.upstreamServer.close(() => resolve()));
    }
  });
});
