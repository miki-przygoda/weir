# weir TypeScript/Node wire client

A **dependency-free** TypeScript producer for the weir v1 wire protocol, built
purely from [`docs/wire_protocol.md`](../../../docs/wire_protocol.md) and the
[conformance vectors](../../../docs/conformance/wire_v1_vectors.json) — **no weir
crate, no npm runtime deps.** Node stdlib only: `node:net` (Unix socket) and
`node:zlib` (CRC-32).

It demonstrates a realistic **async-integrator** use case: a Node `http` web
backend that durably logs every request to weir from its handler.

## Layout

| File | What it is |
|------|-----------|
| `src/wire.ts` | Frame encode/decode + CRC-32. The reference codec — follows the spec's mandatory decode order and the exactly-one-frame contract. |
| `src/client.ts` | Async `WeirClient`: connect, `push()`, `healthCheck()`. Serial request/response over one connection, with response-payload cap + desync handling. |
| `src/conformance.ts` | Runs all 28 conformance vectors (decode + re-encode round-trip + rejection-tag match). |
| `examples/request-logger.ts` | The demo: a stdlib HTTP server that Pushes a JSON log event per request (Buffered durability). |
| `examples/live-smoke.ts` | End-to-end test against a running daemon: all 3 durability tiers, empty-Push rejection, and a metrics cross-check. |

## Requirements

- **Node >= 22.6** for native TypeScript (runs `.ts` directly, no build step).
  Verified on **Node v26**.
- For the live tests: a running `weir-server`.

## Run

### 1. Conformance (no daemon needed)

```sh
node src/conformance.ts
# -> conformance: 28/28 passed
```

### 2. Live smoke + the HTTP request-logger demo

Start a daemon (unique socket / wab / metrics port):

```sh
weir-server \
  --socket-path "$PWD/weir.sock" \
  --wab-dir "$PWD/wab" \
  --metrics-port 19100 \
  --sink-type noop
```

Smoke test:

```sh
node examples/live-smoke.ts "$PWD/weir.sock" 19100
# -> all PASS, SMOKE OK
```

Request logger — start it, then send it HTTP traffic:

```sh
node examples/request-logger.ts "$PWD/weir.sock" 8787 &
curl -s localhost:8787/api/users
curl -s localhost:8787/teapot
curl -s localhost:8787/health   # {"status":"up","logged":N,"dropped":0}
```

Each request becomes a durable record; you can confirm it landed:

```sh
curl -s localhost:19100/metrics | grep weir_records_accepted_total
strings wab/shard_00/seg_*.wab | grep '"method"'   # the JSON log events on disk
```

### 3. Type-check (optional)

```sh
npm install   # typescript + @types/node (dev only)
npm run typecheck
```

`tsconfig.json` sets `erasableSyntaxOnly: true`, which makes `tsc` enforce the
same constraint Node's runtime does (see friction #1) — so type-checking catches
non-erasable syntax before you hit it at runtime.

## Friction log (issues found while building)

1. **Node native TS is strip-only — `enum` and parameter properties crash at
   runtime.** Node 22+ runs `.ts` by stripping types but cannot emit the runtime
   code that a TS `enum`, a `constructor(public x)` parameter property, or
   `namespace` needs. The first run died with
   `ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX: TypeScript enum is not supported in
   strip-only mode`, then again on parameter properties. In **Node 26 the
   `--experimental-transform-types` escape hatch is gone** (`node: bad option`),
   so strip-only is the only mode. Fix: use `as const` objects + parameter-prop
   expansion (erasable syntax). This is a wire-client authoring gotcha worth a
   one-line note in the polyglot docs — the spec rightly says "no weir dep", and
   the most natural Node path (just run the `.ts`) silently constrains your TS
   subset.

2. **`zlib.crc32` is exactly the spec's CRC variant — frictionless.** Node's
   `node:zlib.crc32()` (>= 22.2) matched all worked-example CRCs on the first try
   (`header crc 3c7dad66`, `payload crc 3610a686`, empty `00000000`). The spec
   naming the algorithm "the same as zlib" and giving hex worked-examples made
   this trivial to verify. Strong docs win.

3. **The accepted-record metric reads zero / absent until the first push.** The
   counter is `weir_records_accepted_total{tier=...}` (HELP shows
   `weir_records_accepted`; Prometheus appends `_total`). A pre-traffic scrape
   won't contain the series yet, so a before/after diff must treat "absent" as 0.
   Minor, but a metrics-cross-checking integrator will hit it.

## Viability

**Strong + self-contained.** Zero runtime deps, runs `.ts` directly on stock
Node, passes 28/28 conformance vectors, and the demo is verified end-to-end
against a live daemon (HTTP request → wire → on-disk WAB segment). Good
`demos/` candidate for the polyglot-client story.
