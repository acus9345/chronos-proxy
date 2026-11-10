# ChronosProxy (`chronos-proxy`)

> **Local-First API Time Machine**: HTTP/1.1 & WebSocket reverse proxy with Copy-on-Write (CoW) 4KB SQLite binary page snapshots, sub-millisecond state scrubbing, and deterministic replay drift detection.

---

## Why ChronosProxy?

Debugging stateful backend bugs usually requires guessing what the database looked like before a failing request arrived, or writing brittle mocks that drift from production schemas.

**ChronosProxy** sits between your client and local backend (`http://127.0.0.1:8080 -> http://127.0.0.1:4000`) while monitoring your local `.sqlite` database file (`WAL` or rollback journal mode):

1. **Pre-Request CoW Snapshot (`T-0`)**: Before forwarding an incoming HTTP/WS request, Chronos parses the 100-byte SQLite3 binary header (`"SQLite format 3\0"`), slices the file into 4096-byte pages, hashes each page with SHA-256, and stores only newly mutated pages in a content-addressable page store.
2. **Upstream Execution & Stream Redaction**: Forwards the request with `Accept-Encoding: identity` for deterministic inspection, automatically redacting sensitive headers (`Authorization`, `Cookie`) and nested JSON secrets (`password`, `client_secret`, `token`) before writing to the frame buffer, while retaining raw credentials in an unexported in-memory replay vault (`rawReplayVault`).
3. **Post-Request CoW Snapshot & Row Correlation (`T+1`)**: Captures the post-response SQLite state, computes exact dirty 4KB page indices and logical table `INSERT` / `UPDATE` / `DELETE` row mutations, and attaches them directly to the `TimelineFrame`.
4. **Sub-Millisecond Time Travel & Deterministic Replay**: Scrub backward to any historical frame in the hardware-inspired Web Inspector (`/__chronos`) or CLI (`chronos checkout <frame-id>`), restore the on-disk `.sqlite` database atomically, or re-fire the request (`chronos replay <frame-id>`) to verify whether your backend produces the exact same HTTP status, JSON response, and database mutations.

---

## Architecture

```
Client / Browser / Test Suite
        │
        ▼  HTTP/1.1 or WebSocket (:8080)
┌───────────────────────────────────────────────────────────────────┐
│ ChronosProxyServer (src/proxy-server.ts)                          │
│  ├─ 1. Request Lock Queue (serializes DB snapshot boundaries)     │
│  ├─ 2. Pre-Request CoW Snapshot ──► CoWSqliteEngine (4KB Pages)   │
│  ├─ 3. Forward Request ───────────► Upstream Service (:4000)      │
│  ├─ 4. Post-Request CoW Snapshot ─► Page & Logical Row Diff       │
│  └─ 5. Redact Secrets & Broadcast ─► Inspector UI (/__chronos)    │
└───────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

```bash
# 1. Build TypeScript modules and run automated test suite
npm test

# 2. Launch self-contained demo (Upstream Ledger API + SQLite CoW DB + Proxy)
npm run demo

# 3. Or point ChronosProxy at your own upstream service & SQLite database
node dist/cli.js start \
  --target http://127.0.0.1:4000 \
  --db ./data/app.sqlite \
  --port 8080
```

Open **`http://127.0.0.1:8080/__chronos`** to launch the tactile Web Inspector.

---

## CLI Reference

| Command | Description |
| :--- | :--- |
| `chronos start --target <url> --db <file> [--port 8080]` | Start reverse proxy and SQLite CoW snapshot engine |
| `chronos demo [--port 8080]` | Start interactive local demo ledger API + SQLite DB + Inspector |
| `chronos checkout <frame-id> [--phase pre\|post]` | Atomically restore `.sqlite` file on disk to a frame's snapshot |
| `chronos replay <frame-id>` | Restore pre-request DB snapshot, re-fire HTTP request, and output drift report |

---

## Repository Structure

```
chronos-proxy/
├── package.json            # Package manifest & scripts
├── tsconfig.json           # Strict ES2022 / NodeNext TypeScript config
├── CODE_REVIEW.md          # Second-pass engineering code review & applied fixes
├── scripts/
│   └── build-ts.mjs        # Zero-dependency TypeScript-to-ESM builder & syntax checker
├── src/
│   ├── index.ts            # Public library barrel exports
│   ├── types.ts            # Core TypeScript domain interfaces
│   ├── sqlite-engine.ts    # SQLite3 binary header parser, 4KB CoW store, atomic restore & GC
│   ├── diff-engine.ts      # Recursive secret redaction, JSON structural diff, replay verifier
│   ├── proxy-server.ts     # Reverse proxy, mutex serialization, replay vault & SSE control plane
│   ├── cli.ts              # CLI entrypoint & self-contained demo ledger server
│   └── ui/
│       └── inspector.html  # Tactile hardware-inspired Time-Machine Web Inspector
└── test/
    └── chronos.test.ts     # Automated unit & integration test suite (node:test)
```
