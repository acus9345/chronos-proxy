#!/usr/bin/env node
/**
 * ChronosProxy — Command-Line Interface (`chronos`)
 *
 * Commands:
 *   chronos start --target <url> --db <sqlite-path> [--port 8080]
 *   chronos demo [--port 8080]
 *   chronos checkout <frame-id> [--phase pre|post] [--proxy http://127.0.0.1:8080]
 *   chronos replay <frame-id> [--proxy http://127.0.0.1:8080]
 */

import * as http from 'node:http';
import * as os from 'node:os';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ChronosProxyServer } from './proxy-server.js';
import { CoWSqliteEngine } from './sqlite-engine.js';
import type { LogicalTableRows } from './types.js';

interface ParsedCliArgs {
  command: string;
  positional: string[];
  flags: Record<string, string | boolean>;
}

export function parseCliArgs(argv: string[]): ParsedCliArgs {
  const positional: string[] = [];
  const flags: Record<string, string | boolean> = {};
  let command = 'help';

  const args = argv.slice(2);
  if (args.length > 0 && !args[0].startsWith('-')) {
    command = args.shift()!;
  }

  for (let i = 0; i < args.length; i++) {
    const token = args[i];
    if (token.startsWith('--')) {
      const eqIdx = token.indexOf('=');
      if (eqIdx !== -1) {
        flags[token.slice(2, eqIdx)] = token.slice(eqIdx + 1);
      } else {
        const key = token.slice(2);
        const next = args[i + 1];
        if (next && !next.startsWith('-')) {
          flags[key] = next;
          i++;
        } else {
          flags[key] = true;
        }
      }
    } else if (token.startsWith('-')) {
      flags[token.slice(1)] = true;
    } else {
      positional.push(token);
    }
  }

  return { command, positional, flags };
}

function printBanner(): void {
  process.stdout.write(
    [
      '┌──────────────────────────────────────────────────────────────────────┐',
      '│ CHRONOS PROXY v0.4.2  //  LOCAL-FIRST API TIME MACHINE               │',
      '│ Copy-on-Write SQLite Page Snapshots + Deterministic HTTP/WS Replay   │',
      '└──────────────────────────────────────────────────────────────────────┘',
      '',
    ].join('\n') + '\n'
  );
}

function printUsage(): void {
  printBanner();
  process.stdout.write(
    `USAGE:
  chronos <command> [options]

COMMANDS:
  start                 Start ChronosProxy in front of an upstream HTTP/WS service
  demo                  Launch self-contained demo upstream ledger API + SQLite DB + Inspector
  checkout <frame-id>   Restore SQLite database on disk to a frame's pre or post snapshot
  replay <frame-id>     Time-travel DB to pre-frame state, replay request, and check drift
  help                  Show this usage screen

OPTIONS:
  --target <url>        Upstream server origin (default: http://127.0.0.1:4000)
  --db <path>           SQLite3 database file path (default: ./chronos-demo.sqlite)
  --port <number>       Proxy listen port (default: 8080)
  --host <address>      Proxy listen host (default: 127.0.0.1)
  --phase <pre|post>    Snapshot phase for checkout (default: post)
  --proxy <url>         Running ChronosProxy control URL (default: http://127.0.0.1:8080)
`
  );
}

/**
 * Starts a local demo upstream financial ledger service backed by a shared CoWSqliteEngine
 * so developers can immediately test time-travel state scrubbing out of the box.
 */
export async function startDemoEnvironment(listenPort = 8080): Promise<{
  proxy: ChronosProxyServer;
  upstreamServer: http.Server;
  proxyPort: number;
  upstreamPort: number;
  dbPath: string;
}> {
  const dbPath = path.join(
    os.tmpdir(),
    `chronos-demo-${process.pid}-${Date.now()}.sqlite`
  );

  const initialTables: LogicalTableRows = {
    accounts: [
      { id: 'acc_treasury', owner: 'Platform Treasury', balanceCents: 250000, status: 'ACTIVE' },
      { id: 'acc_merchant_01', owner: 'Kinetix Hardware Co.', balanceCents: 84500, status: 'ACTIVE' },
      { id: 'acc_customer_09', owner: 'M. Lindqvist', balanceCents: 32000, status: 'ACTIVE' },
    ],
    ledger_entries: [
      { id: 'ldg_1001', accountId: 'acc_merchant_01', deltaCents: 84500, reason: 'INITIAL_SETTLEMENT' },
    ],
  };

  const sqliteEngine = new CoWSqliteEngine(dbPath, initialTables);
  sqliteEngine.captureSnapshot();

  const upstreamServer = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const c of req) {
      chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
    }
    const rawBody = Buffer.concat(chunks).toString('utf8');
    let parsedBody: Record<string, unknown> = {};
    if (rawBody) {
      try {
        parsedBody = JSON.parse(rawBody) as Record<string, unknown>;
      } catch {
        parsedBody = {};
      }
    }

    const url = req.url ?? '/';
    res.setHeader('content-type', 'application/json');

    if (req.method === 'GET' && url === '/v1/accounts') {
      const state = sqliteEngine.getLogicalState();
      res.writeHead(200);
      res.end(
        JSON.stringify({
          accounts: state.accounts,
          ledgerCount: (state.ledger_entries ?? []).length,
        })
      );
      return;
    }

    if (req.method === 'POST' && url === '/v1/transfers') {
      const fromId = String(parsedBody.fromAccount ?? 'acc_customer_09');
      const toId = String(parsedBody.toAccount ?? 'acc_merchant_01');
      const amountCents = Number(parsedBody.amountCents ?? 4500);

      let transferResult: Record<string, unknown> = {};
      let statusCode = 201;

      sqliteEngine.applyLogicalMutation((tables) => {
        const accounts = tables.accounts ?? [];
        const fromAcc = accounts.find((a) => a.id === fromId);
        const toAcc = accounts.find((a) => a.id === toId);

        if (!fromAcc || !toAcc) {
          statusCode = 404;
          transferResult = { error: 'ACCOUNT_NOT_FOUND' };
          return;
        }
        if (Number(fromAcc.balanceCents) < amountCents) {
          statusCode = 422;
          transferResult = {
            error: 'INSUFFICIENT_FUNDS',
            availableCents: fromAcc.balanceCents,
            requestedCents: amountCents,
          };
          return;
        }

        fromAcc.balanceCents = Number(fromAcc.balanceCents) - amountCents;
        toAcc.balanceCents = Number(toAcc.balanceCents) + amountCents;

        const ledger = tables.ledger_entries ?? [];
        const newEntryId = `ldg_${1002 + ledger.length}`;
        ledger.push({
          id: newEntryId,
          accountId: toId,
          fromAccountId: fromId,
          deltaCents: amountCents,
          reason: String(parsedBody.reason ?? 'ORDER_CAPTURE'),
        });
        tables.ledger_entries = ledger;

        transferResult = {
          transferId: newEntryId,
          fromAccount: fromId,
          toAccount: toId,
          amountCents,
          remainingBalanceCents: fromAcc.balanceCents,
        };
      });

      res.writeHead(statusCode);
      res.end(JSON.stringify(transferResult));
      return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'ROUTE_NOT_FOUND', path: url }));
  });

  const upstreamPort = await new Promise<number>((resolve, reject) => {
    upstreamServer.once('error', reject);
    upstreamServer.listen(0, '127.0.0.1', () => {
      const addr = upstreamServer.address();
      resolve(typeof addr === 'object' && addr ? addr.port : 4000);
    });
  });

  const proxy = new ChronosProxyServer(
    {
      listenPort,
      listenHost: '127.0.0.1',
      targetUrl: `http://127.0.0.1:${upstreamPort}`,
      sqlitePath: dbPath,
    },
    sqliteEngine
  );

  const { port: proxyPort } = await proxy.start();
  return { proxy, upstreamServer, proxyPort, upstreamPort, dbPath };
}

async function runCli(): Promise<void> {
  const { command, positional, flags } = parseCliArgs(process.argv);

  if (command === 'help' || flags.help || flags.h) {
    printUsage();
    return;
  }

  if (command === 'start') {
    printBanner();
    const listenPort = Number(flags.port ?? 8080);
    const listenHost = String(flags.host ?? '127.0.0.1');
    const targetUrl = String(flags.target ?? 'http://127.0.0.1:4000');
    const sqlitePath = String(flags.db ?? './chronos-demo.sqlite');

    const proxy = new ChronosProxyServer({
      listenPort,
      listenHost,
      targetUrl,
      sqlitePath,
    });
    const bound = await proxy.start();
    process.stdout.write(
      `[CHRONOS] Listening on http://${bound.host}:${bound.port}\n` +
        `[CHRONOS] Inspector UI : http://${bound.host}:${bound.port}/__chronos\n` +
        `[CHRONOS] Upstream     : ${targetUrl}\n` +
        `[CHRONOS] SQLite CoW   : ${path.resolve(sqlitePath)}\n`
    );
    return;
  }

  if (command === 'demo') {
    printBanner();
    const requestedPort = Number(flags.port ?? 8080);
    const env = await startDemoEnvironment(requestedPort);
    process.stdout.write(
      `[DEMO] Upstream Ledger Service : http://127.0.0.1:${env.upstreamPort}\n` +
        `[DEMO] ChronosProxy Gateway    : http://127.0.0.1:${env.proxyPort}\n` +
        `[DEMO] Time-Machine UI         : http://127.0.0.1:${env.proxyPort}/__chronos\n` +
        `[DEMO] SQLite CoW Database     : ${env.dbPath}\n`
    );
    return;
  }

  if (command === 'checkout' || command === 'replay') {
    const frameId = positional[0];
    if (!frameId) {
      process.stderr.write(`Error: Missing <frame-id> for 'chronos ${command}'\n`);
      process.exitCode = 1;
      return;
    }
    const baseUrl = String(flags.proxy ?? 'http://127.0.0.1:8080');
    const phase = String(flags.phase ?? 'post');
    const endpoint =
      command === 'checkout'
        ? `${baseUrl}/__chronos/api/checkout/${encodeURIComponent(frameId)}?phase=${phase}`
        : `${baseUrl}/__chronos/api/replay/${encodeURIComponent(frameId)}`;

    const res = await fetch(endpoint, { method: 'POST' });
    const text = await res.text();
    process.stdout.write(`${text}\n`);
    if (!res.ok) process.exitCode = 1;
    return;
  }

  process.stderr.write(`Unknown command: ${command}\n\n`);
  printUsage();
  process.exitCode = 1;
}

function resolveRealPath(p: string): string {
  try {
    return fs.realpathSync(p);
  } catch {
    return path.resolve(p);
  }
}

const isMainModule =
  Boolean(process.argv[1]) &&
  resolveRealPath(process.argv[1]) === resolveRealPath(fileURLToPath(import.meta.url));

if (isMainModule) {
  runCli().catch((err) => {
    process.stderr.write(
      `Fatal CLI error: ${err instanceof Error ? err.message : String(err)}\n`
    );
    process.exitCode = 1;
  });
}
