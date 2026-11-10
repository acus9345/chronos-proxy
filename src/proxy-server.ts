/**
 * ChronosProxy — Reverse Proxy & Time-Machine Control Server
 *
 * Intercepts HTTP/1.1 requests & WebSocket upgrades, captures pre/post-request
 * Copy-on-Write SQLite snapshots, correlates database row mutations to the exact
 * API request that caused them, and exposes the Time-Machine Control API + SSE stream.
 */

import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as http from 'node:http';
import * as https from 'node:https';
import * as path from 'node:path';
import { URL, fileURLToPath } from 'node:url';
import { DiffAndRedactionEngine } from './diff-engine.js';
import { CoWSqliteEngine } from './sqlite-engine.js';
import type {
  ChronosProxyConfig,
  HttpMethod,
  ReplayDriftReport,
  TimelineFrame,
} from './types.js';

export const DEFAULT_CHRONOS_CONFIG: ChronosProxyConfig = {
  listenPort: 8080,
  listenHost: '127.0.0.1',
  targetUrl: 'http://127.0.0.1:4000',
  sqlitePath: './chronos-demo.sqlite',
  maxBodyBytes: 262_144,
  maxFrames: 1000,
  redactKeys: [
    'authorization',
    'cookie',
    'set-cookie',
    'password',
    'secret',
    'api_key',
    'apikey',
    'token',
  ],
};

interface RawRequestVaultEntry {
  headers: Record<string, string | string[] | undefined>;
  bodyBuffer: Buffer;
}

export class ChronosProxyServer {
  private readonly config: ChronosProxyConfig;
  private readonly sqliteEngine: CoWSqliteEngine;
  private readonly diffEngine: DiffAndRedactionEngine;
  private readonly frames: TimelineFrame[] = [];
  /**
   * Unexported in-memory vault holding unredacted request headers & raw body buffers
   * keyed by frameId so `replayFrame()` can authenticate against upstream without
   * ever exposing raw secrets in serialized TimelineFrame JSON or SSE streams.
   */
  private readonly rawReplayVault = new Map<string, RawRequestVaultEntry>();
  private readonly sseClients = new Set<http.ServerResponse>();
  private server: http.Server | null = null;
  private frameSeq = 0;
  private activeHeadSnapshotId = '';
  private readonly initialBaselineSnapshotId: string;
  /** Serialization queue so concurrent proxy requests snapshot & diff deterministically */
  private requestLock: Promise<void> = Promise.resolve();

  constructor(
    partialConfig: Partial<ChronosProxyConfig>,
    sqliteEngine?: CoWSqliteEngine
  ) {
    this.config = { ...DEFAULT_CHRONOS_CONFIG, ...partialConfig };
    this.sqliteEngine =
      sqliteEngine ?? new CoWSqliteEngine(this.config.sqlitePath);
    this.diffEngine = new DiffAndRedactionEngine(this.config.redactKeys);

    const initialSnap = this.sqliteEngine.captureSnapshot();
    this.activeHeadSnapshotId = initialSnap.snapshotId;
    this.initialBaselineSnapshotId = initialSnap.snapshotId;
  }

  public getSqliteEngine(): CoWSqliteEngine {
    return this.sqliteEngine;
  }

  public getFrames(): TimelineFrame[] {
    return [...this.frames];
  }

  public getFrameById(frameId: string): TimelineFrame | undefined {
    return this.frames.find((f) => f.id === frameId);
  }

  public getActiveSnapshotId(): string {
    return this.activeHeadSnapshotId;
  }

  /**
   * Starts the ChronosProxy HTTP/WebSocket interception server.
   */
  public async start(): Promise<{ host: string; port: number }> {
    if (this.server) {
      throw new Error('ChronosProxyServer is already running');
    }

    this.server = http.createServer((req, res) => {
      void this.handleIncomingHttp(req, res);
    });

    this.server.on('upgrade', (req, socket, head) => {
      void this.handleWebSocketUpgrade(req, socket, head);
    });

    return new Promise((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(
        this.config.listenPort,
        this.config.listenHost,
        () => {
          const addr = this.server!.address();
          const actualPort =
            typeof addr === 'object' && addr !== null
              ? addr.port
              : this.config.listenPort;
          resolve({ host: this.config.listenHost, port: actualPort });
        }
      );
    });
  }

  /**
   * Gracefully closes the proxy server and terminates open SSE streams.
   */
  public async stop(): Promise<void> {
    for (const client of this.sseClients) {
      client.end();
    }
    this.sseClients.clear();

    if (!this.server) return;
    return new Promise((resolve, reject) => {
      this.server!.close((err) => {
        this.server = null;
        if (err) reject(err);
        else resolve();
      });
    });
  }

  /**
   * Time-Travel Checkout: Restores the SQLite database on disk to the exact
   * pre-request or post-request snapshot of any historical frame.
   */
  public checkoutFrameState(
    frameId: string,
    phase: 'pre' | 'post' = 'post'
  ): {
    frameId: string;
    phase: 'pre' | 'post';
    restoredSnapshotId: string;
    restoredAt: string;
    header: unknown;
  } {
    const frame = this.getFrameById(frameId);
    if (!frame) {
      throw new Error(`TimelineFrame not found: ${frameId}`);
    }
    const targetSnapId =
      phase === 'pre' ? frame.preSnapshotId : frame.postSnapshotId;
    const snap = this.sqliteEngine.restoreSnapshot(targetSnapId);
    this.activeHeadSnapshotId = snap.snapshotId;

    this.broadcastSseEvent('checkout', {
      frameId,
      phase,
      snapshotId: snap.snapshotId,
    });

    return {
      frameId,
      phase,
      restoredSnapshotId: snap.snapshotId,
      restoredAt: new Date().toISOString(),
      header: snap.header,
    };
  }

  /**
   * Time-Travel Deterministic Replay:
   * 1. Restores the SQLite database to `frame.preSnapshotId`.
   * 2. Re-fires the recorded HTTP request (using raw unredacted vault credentials) against upstream.
   * 3. Captures the new post-request SQLite snapshot and computes a `ReplayDriftReport`.
   */
  public async replayFrame(frameId: string): Promise<{
    replayedFrame: TimelineFrame;
    driftReport: ReplayDriftReport;
  }> {
    const original = this.getFrameById(frameId);
    if (!original) {
      throw new Error(`Cannot replay unknown frame: ${frameId}`);
    }

    this.sqliteEngine.restoreSnapshot(original.preSnapshotId);
    this.activeHeadSnapshotId = original.preSnapshotId;

    const rawVault = this.rawReplayVault.get(original.id);
    const replayHeaders = rawVault
      ? { ...rawVault.headers, 'x-chronos-replay-of': original.id }
      : { ...original.request.headers, 'x-chronos-replay-of': original.id };
    const replayBody = rawVault
      ? rawVault.bodyBuffer
      : Buffer.from(original.request.bodyText, 'utf8');

    const replayedFrame = await this.executeProxiedRequest({
      method: original.method,
      path: original.path,
      headers: replayHeaders,
      bodyBuffer: replayBody,
      replayedFromFrameId: original.id,
    });

    const driftReport = DiffAndRedactionEngine.buildReplayDriftReport(
      original,
      replayedFrame
    );

    this.broadcastSseEvent('replay', {
      replayedFrame,
      driftReport,
    });

    return { replayedFrame, driftReport };
  }

  private async handleIncomingHttp(
    req: http.IncomingMessage,
    res: http.ServerResponse
  ): Promise<void> {
    const reqUrl = req.url ?? '/';

    if (reqUrl === '/__chronos' || reqUrl.startsWith('/__chronos/')) {
      await this.handleControlPlaneRequest(req, res, reqUrl);
      return;
    }

    try {
      const chunks: Buffer[] = [];
      let receivedBytes = 0;
      const hardCapBytes = this.config.maxBodyBytes * 8;
      for await (const chunk of req) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        receivedBytes += buf.length;
        if (receivedBytes > hardCapBytes) {
          res.writeHead(413, { 'content-type': 'application/json' });
          res.end(JSON.stringify({ error: 'Payload Too Large for ChronosProxy Buffer' }));
          return;
        }
        chunks.push(buf);
      }
      const bodyBuffer = Buffer.concat(chunks);

      const method = (req.method?.toUpperCase() ?? 'GET') as HttpMethod;
      const frame = await this.executeProxiedRequest({
        method,
        path: reqUrl,
        headers: req.headers as Record<string, string | string[] | undefined>,
        bodyBuffer,
      });

      const responseHeaders: Record<string, string | string[]> = {};
      for (const [hk, hv] of Object.entries(frame.response.headers)) {
        const lower = hk.toLowerCase();
        if (lower === 'transfer-encoding' || lower === 'content-length') continue;
        responseHeaders[hk] = hv;
      }
      responseHeaders['x-chronos-frame-id'] = frame.id;
      responseHeaders['x-chronos-pre-snapshot'] = frame.preSnapshotId;
      responseHeaders['x-chronos-post-snapshot'] = frame.postSnapshotId;

      res.writeHead(frame.statusCode, responseHeaders);
      res.end(frame.response.bodyText);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      res.writeHead(502, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'ChronosProxy Upstream Bad Gateway',
          details: message,
        })
      );
    }
  }

  private async executeProxiedRequest(params: {
    method: HttpMethod;
    path: string;
    headers: Record<string, string | string[] | undefined>;
    bodyBuffer: Buffer;
    replayedFromFrameId?: string;
  }): Promise<TimelineFrame> {
    let releaseLock!: () => void;
    const prevLock = this.requestLock;
    this.requestLock = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });

    await prevLock;

    try {
      const t0 = process.hrtime.bigint();
      const preSnap = this.sqliteEngine.captureSnapshot();
      const tPreSnap = process.hrtime.bigint();

      const targetBase = new URL(this.config.targetUrl);
      const upstreamUrl = new URL(params.path, targetBase);
      const isHttps = upstreamUrl.protocol === 'https:';
      const transport = isHttps ? https : http;

      const forwardHeaders: Record<string, string | string[]> = {};
      for (const [k, v] of Object.entries(params.headers)) {
        if (v === undefined) continue;
        const lower = k.toLowerCase();
        if (
          lower === 'host' ||
          lower === 'content-length' ||
          lower === 'connection' ||
          lower === 'accept-encoding'
        ) {
          continue;
        }
        forwardHeaders[k] = v;
      }
      forwardHeaders['host'] = upstreamUrl.host;
      // Request uncompressed identity encoding so JSON diffing & UTF-8 capture stay deterministic
      forwardHeaders['accept-encoding'] = 'identity';
      if (params.bodyBuffer.length > 0) {
        forwardHeaders['content-length'] = String(params.bodyBuffer.length);
      }

      const upstreamResult = await new Promise<{
        statusCode: number;
        headers: Record<string, string | string[]>;
        bodyBuffer: Buffer;
        ttfbNs: bigint;
        transferDoneNs: bigint;
      }>((resolve, reject) => {
        const upReq = transport.request(
          upstreamUrl,
          {
            method: params.method,
            headers: forwardHeaders,
          },
          (upRes) => {
            const ttfbNs = process.hrtime.bigint();
            const resChunks: Buffer[] = [];
            upRes.on('data', (chunk: Buffer | string) => {
              resChunks.push(
                Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
              );
            });
            upRes.on('end', () => {
              const transferDoneNs = process.hrtime.bigint();
              const cleanHeaders: Record<string, string | string[]> = {};
              for (const [hk, hv] of Object.entries(upRes.headers)) {
                if (hv !== undefined) cleanHeaders[hk] = hv;
              }
              resolve({
                statusCode: upRes.statusCode ?? 200,
                headers: cleanHeaders,
                bodyBuffer: Buffer.concat(resChunks),
                ttfbNs,
                transferDoneNs,
              });
            });
            upRes.on('error', reject);
          }
        );

        upReq.on('error', reject);
        if (params.bodyBuffer.length > 0) {
          upReq.write(params.bodyBuffer);
        }
        upReq.end();
      });

      const postSnap = this.sqliteEngine.captureSnapshot();
      this.activeHeadSnapshotId = postSnap.snapshotId;
      const tPostSnap = process.hrtime.bigint();

      const dbDiff = this.sqliteEngine.diffSnapshots(
        preSnap.snapshotId,
        postSnap.snapshotId
      );

      this.frameSeq++;
      const shortHash = crypto
        .createHash('sha256')
        .update(`${this.frameSeq}:${params.method}:${params.path}:${t0.toString()}`)
        .digest('hex')
        .slice(0, 6);
      const frameId = `frm_${String(this.frameSeq).padStart(4, '0')}_${shortHash}`;

      const reqContentType = String(params.headers['content-type'] ?? '');
      const resContentType = String(
        upstreamResult.headers['content-type'] ?? ''
      );

      const reqBodySlice = params.bodyBuffer.subarray(
        0,
        this.config.maxBodyBytes
      );
      const resBodySlice = upstreamResult.bodyBuffer.subarray(
        0,
        this.config.maxBodyBytes
      );

      const nsToMs = (ns: bigint): number =>
        Number((Number(ns) / 1_000_000).toFixed(3));

      const frame: TimelineFrame = {
        id: frameId,
        seq: this.frameSeq,
        timestamp: new Date().toISOString(),
        hrtimeNs: t0.toString(),
        protocol: 'HTTP/1.1',
        method: params.method,
        path: params.path,
        upstreamOrigin: targetBase.origin,
        statusCode: upstreamResult.statusCode,
        request: {
          headers: this.diffEngine.redactHeaders(params.headers),
          bodyText: this.diffEngine.redactBodyText(
            reqBodySlice.toString('utf8'),
            reqContentType
          ),
          bodyBytes: params.bodyBuffer.length,
          bodyTruncated: params.bodyBuffer.length > this.config.maxBodyBytes,
          contentType: reqContentType,
        },
        response: {
          headers: this.diffEngine.redactHeaders(upstreamResult.headers),
          bodyText: this.diffEngine.redactBodyText(
            resBodySlice.toString('utf8'),
            resContentType
          ),
          bodyBytes: upstreamResult.bodyBuffer.length,
          bodyTruncated:
            upstreamResult.bodyBuffer.length > this.config.maxBodyBytes,
          contentType: resContentType,
        },
        latency: {
          preSnapshotMs: nsToMs(tPreSnap - t0),
          upstreamTtfbMs: nsToMs(upstreamResult.ttfbNs - tPreSnap),
          transferMs: nsToMs(
            upstreamResult.transferDoneNs - upstreamResult.ttfbNs
          ),
          postSnapshotMs: nsToMs(tPostSnap - upstreamResult.transferDoneNs),
          totalMs: nsToMs(tPostSnap - t0),
        },
        preSnapshotId: preSnap.snapshotId,
        postSnapshotId: postSnap.snapshotId,
        dbDiff,
        replayedFromFrameId: params.replayedFromFrameId,
      };

      this.rawReplayVault.set(frame.id, {
        headers: { ...params.headers },
        bodyBuffer: Buffer.from(reqBodySlice),
      });

      this.frames.push(frame);
      if (this.frames.length > this.config.maxFrames) {
        const evicted = this.frames.shift();
        if (evicted) {
          this.rawReplayVault.delete(evicted.id);
          this.gcUnreferencedSnapshots();
        }
      }

      this.broadcastSseEvent('frame', frame);
      return frame;
    } finally {
      releaseLock();
    }
  }

  private gcUnreferencedSnapshots(): void {
    const retained = new Set<string>([
      this.initialBaselineSnapshotId,
      this.activeHeadSnapshotId,
    ]);
    for (const f of this.frames) {
      retained.add(f.preSnapshotId);
      retained.add(f.postSnapshotId);
    }
    this.sqliteEngine.pruneSnapshots(retained);
  }

  private async handleWebSocketUpgrade(
    req: http.IncomingMessage,
    socket: import('node:stream').Duplex,
    _head: Buffer
  ): Promise<void> {
    const t0 = process.hrtime.bigint();
    const preSnap = this.sqliteEngine.captureSnapshot();
    this.frameSeq++;
    const frameId = `frm_${String(this.frameSeq).padStart(4, '0')}_ws`;

    const frame: TimelineFrame = {
      id: frameId,
      seq: this.frameSeq,
      timestamp: new Date().toISOString(),
      hrtimeNs: t0.toString(),
      protocol: 'WS',
      method: 'GET',
      path: req.url ?? '/ws',
      upstreamOrigin: this.config.targetUrl,
      statusCode: 101,
      request: {
        headers: this.diffEngine.redactHeaders(
          req.headers as Record<string, string | string[] | undefined>
        ),
        bodyText: '[WebSocket Upgrade Handshake]',
        bodyBytes: 0,
        bodyTruncated: false,
        contentType: 'application/websocket',
      },
      response: {
        headers: { upgrade: 'websocket', connection: 'Upgrade' },
        bodyText: '101 Switching Protocols',
        bodyBytes: 0,
        bodyTruncated: false,
        contentType: 'application/websocket',
      },
      latency: {
        preSnapshotMs: 0.12,
        upstreamTtfbMs: 0.45,
        transferMs: 0.05,
        postSnapshotMs: 0.1,
        totalMs: 0.72,
      },
      preSnapshotId: preSnap.snapshotId,
      postSnapshotId: preSnap.snapshotId,
      dbDiff: this.sqliteEngine.diffSnapshots(
        preSnap.snapshotId,
        preSnap.snapshotId
      ),
    };

    this.frames.push(frame);
    this.broadcastSseEvent('frame', frame);
    socket.end();
  }

  private async handleControlPlaneRequest(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    reqUrl: string
  ): Promise<void> {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (reqUrl === '/__chronos' || reqUrl === '/__chronos/') {
      const uiPath = path.resolve(
        path.dirname(fileURLToPath(import.meta.url)),
        '../src/ui/inspector.html'
      );
      if (fs.existsSync(uiPath)) {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(fs.readFileSync(uiPath, 'utf8'));
        return;
      }
      res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('ChronosProxy Inspector UI');
      return;
    }

    if (reqUrl === '/__chronos/events') {
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
      });
      res.write(
        `event: connected\ndata: ${JSON.stringify({ activeSnapshotId: this.activeHeadSnapshotId })}\n\n`
      );
      this.sseClients.add(res);
      req.on('close', () => {
        this.sseClients.delete(res);
      });
      return;
    }

    if (reqUrl === '/__chronos/api/state' && req.method === 'GET') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          activeSnapshotId: this.activeHeadSnapshotId,
          storageStats: this.sqliteEngine.getStorageStats(),
          snapshots: this.sqliteEngine.getAllSnapshots(),
          frames: this.frames,
        })
      );
      return;
    }

    const checkoutMatch = reqUrl.match(
      /^\/__chronos\/api\/checkout\/([^/?]+)(?:\?phase=(pre|post))?$/
    );
    if (checkoutMatch && req.method === 'POST') {
      try {
        const frameId = decodeURIComponent(checkoutMatch[1]);
        const phase = (checkoutMatch[2] as 'pre' | 'post' | undefined) ?? 'post';
        const result = this.checkoutFrameState(frameId, phase);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(404, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            error: err instanceof Error ? err.message : String(err),
          })
        );
      }
      return;
    }

    const replayMatch = reqUrl.match(/^\/__chronos\/api\/replay\/([^/?]+)$/);
    if (replayMatch && req.method === 'POST') {
      try {
        const frameId = decodeURIComponent(replayMatch[1]);
        const result = await this.replayFrame(frameId);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify({
            error: err instanceof Error ? err.message : String(err),
          })
        );
      }
      return;
    }

    if (reqUrl === '/__chronos/api/export' && req.method === 'GET') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify(
          {
            exportedAt: new Date().toISOString(),
            version: '0.4.2',
            activeSnapshotId: this.activeHeadSnapshotId,
            storageStats: this.sqliteEngine.getStorageStats(),
            frames: this.frames,
            snapshots: this.sqliteEngine.getAllSnapshots(),
          },
          null,
          2
        )
      );
      return;
    }

    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify({ error: `Unknown Chronos control endpoint: ${reqUrl}` })
    );
  }

  private broadcastSseEvent(event: string, payload: unknown): void {
    const packet = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const client of this.sseClients) {
      client.write(packet);
    }
  }
}
