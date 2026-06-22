# ts-wire-client

**A weir v1 wire-protocol producer in TypeScript/Node — built from the spec, no weir crate.**

## What it is

A dependency-free TypeScript/Node producer for the [weir](../../) durable
write-ahead-buffer daemon — no weir crate, no npm runtime deps, Node stdlib only
(`node:net`, `node:zlib`). The codec was written purely from
[`docs/wire_protocol.md`](../../../docs/wire_protocol.md) and the
[`docs/conformance/wire_v1_vectors.json`](../../../docs/conformance/wire_v1_vectors.json)
vectors. It speaks weir's `AF_UNIX` wire protocol directly, and runs `.ts` files
directly on stock Node — no build step.

The demo (`examples/request-logger.ts`) demonstrates the **async-integrator**
case: a Node `http` web backend that durably logs every HTTP request to weir from
its handler. Each request becomes a JSON event
(`{ seq, ts, method, path, status, latency_ms, ua, remote }`) pushed at
**Buffered** durability (ack after memory write, for low handler latency). The
payload is opaque to weir — a real deployment would point the daemon's sink at
ClickHouse/Postgres.

## How it works

### The frame

Every frame is a fixed **16-byte header**, then the payload, then a 4-byte
payload CRC32 (`encodeFrame` in `src/wire.ts`, using a Node `Buffer`):

```
Offset  Size  Field          Value
 0       4    magic          "WEIR"
 4       1    version        WIRE_VERSION = 1
 5       1    message_type    Push=0x01 / Ack=0x02 / Nack=0x03 / HealthCheck=0x04 / HealthCheckResponse=0x05
 6       1    durability      Sync=0x01 / Batched=0x02 / Buffered=0x03
 7       1    flags          reserved; must be 0 on write
 8       4    payload_len    u32 little-endian (writeUInt32LE)
12       4    header_crc32   CRC32 over bytes [0..12], little-endian
16     var    payload        payload_len bytes
16+n    4    payload_crc32  CRC32 over the payload bytes, little-endian
```

`crc(buf)` is `node:zlib`'s `crc32(buf) >>> 0` — IEEE / ISO-3309 CRC-32, the
zlib/PNG variant, **not** CRC-32C. The header CRC covers exactly bytes `[0..12]`;
the payload CRC covers exactly the payload. (The enums are `as const` objects, not
TS `enum`s, so the source stays within Node's strip-only type-stripping — see the
friction note below.)

### The Push → Ack round-trip

`WeirClient.push(payload, durability = Durability.Sync)` (in `src/client.ts`)
encodes a `Push` frame, writes it, and returns a `Promise<PushResult>` that
resolves on `Ack` and rejects with `NackError` on a Nack. `healthCheck()` sends a
zero-length `HealthCheck`. The client is serial: each request is queued FIFO and
matched to the next response.

### The codec (`src/wire.ts`)

`encodeFrame` / `decodeFrame` are inverses. `decodeFrame` is a reference decoder
requiring exactly one frame and follows the spec's mandatory order — magic,
truncation, version (before the header CRC), header CRC, field parsing
(`UnknownMessageType` / `UnknownDurability` / `ReservedFlagsSet`), the
payload-length cap (`PayloadTooLarge`), the exactly-one-frame length check
(`TruncatedFrame` / `TrailingBytes`), then the payload CRC
(`PayloadCrcMismatch`). Each failure throws a `DecodeError` whose `.tag` matches
the conformance vector names.

### Connection lifecycle & errors

`WeirClient.connect()` opens a single `node:net` connection to the Unix socket
path — no in-band handshake. Incoming bytes accumulate in a buffer and are framed
one response at a time: validate magic / version / header CRC, **cap the response
`payload_len` at `MAX_RESPONSE_PAYLOAD` (2 bytes)** as a desync guard, wait for
exactly `payload_len + 4` more bytes, verify the payload CRC, then dispatch. A
Nack rejects with `NackError` (`isTransient` is true only for `InternalError`;
all other reasons close the connection). A close mid-stream rejects every queued
request ("in-flight outcome unknown; retry on a fresh connection"), and an
optional per-request timeout destroys the socket on expiry.

## Conformance

`src/conformance.ts` runs the codec against all **30 canonical vectors** (17
valid frames + 13 rejection cases). For each it checks the decode outcome and —
for `"ok"` frames — the decoded `message_type`, `durability`, `flags`, and
payload, plus a **byte-exact re-encode** round-trip back to the vector `hex`. It
reads the cap and vectors from `docs/conformance/wire_v1_vectors.json` (resolved
relative to `src/`), overridable with a path arg or the `WEIR_CONFORMANCE_VECTORS`
environment variable. It is a self-executing script that exits non-zero on any
mismatch.

## Run it

```bash
cd demos/ts-wire-client

# Offline codec conformance — no daemon needed
node src/conformance.ts                 # -> conformance: 30/30 passed
#   (or via npm: npm run conformance)

# Live: start a daemon with a unique socket/wab/metrics port
weir-server --socket-path "$PWD/weir.sock" --wab-dir "$PWD/wab" \
            --metrics-port 19100 --sink-type noop &

# Smoke test (3 tiers + empty-Push rejection + a metrics cross-check)
node examples/live-smoke.ts "$PWD/weir.sock" 19100   # -> SMOKE OK

# HTTP request-logger demo — each request becomes a durable record
node examples/request-logger.ts "$PWD/weir.sock" 8787 &
curl -s localhost:8787/api/users
curl -s localhost:8787/health           # {"status":"up","logged":N,"dropped":0}

# Type-check (optional; runtime needs no build)
npm install && npm run typecheck
```

## Requirements

Zero runtime deps (TypeScript is a dev-only dependency for `npm run typecheck`).
Runs `.ts` directly on Node's native type-stripping and uses `node:zlib.crc32`.
**Requires Node 22.6+ · tested on Node 26.** (`node:zlib.crc32` needs ≥ 22.2;
native `.ts` type-stripping sets the 22.6 floor — hence `"engines": {"node":
">=22.6"}`.)

## Friction log (genuine, source-grounded)

- **Node native TS is strip-only.** Node runs `.ts` by erasing types but cannot
  emit the runtime code a TS `enum`, a `constructor(public x)` parameter property,
  or `namespace` needs (`ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX`). The source uses
  `as const` objects and explicit class fields instead; `tsconfig.json` sets
  `erasableSyntaxOnly: true` so `tsc` catches a violation before runtime does.
- **`node:zlib.crc32` is exactly the spec's CRC variant** — it matched every
  worked-example CRC on the first try. The spec naming the algorithm and giving
  hex worked-examples made this trivial to verify.

## Files

| File | What it holds |
|------|---------------|
| `src/wire.ts`     | The wire codec: `encodeFrame` / `decodeFrame`, constants, the `as const` `MessageType` / `Durability` / `NackReason` maps, name helpers, `crc()` (`node:zlib`), and `DecodeError` / `DecodeErrorTag`. |
| `src/client.ts`   | Async Unix-socket producer (`WeirClient`) over `node:net`: serial Push/HealthCheck → Ack/Nack with response framing, a 2-byte response cap, timeouts, and `WireError` / `NackError`. |
| `src/conformance.ts` | Self-executing harness running the 30 vectors through the codec (decode + fields + byte-exact re-encode for valid frames, tag-match for rejections). |
| `examples/request-logger.ts` | The demo: a `node:http` backend that fire-and-forget Pushes a JSON access-log event per request at Buffered durability. |
| `examples/live-smoke.ts` | End-to-end test against a running daemon: 3 durability tiers, empty-Push rejection, and a Prometheus metrics cross-check. |
| `package.json`    | ESM, `private`, zero runtime deps, TypeScript-only devDep, `engines.node >=22.6`; scripts run `.ts` directly via `node`. |
| `tsconfig.json`   | Type-check-only config (`noEmit`, `erasableSyntaxOnly`, `allowImportingTsExtensions`) for `tsc --noEmit`. |
</content>
