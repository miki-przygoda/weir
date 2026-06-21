# weir-console

A small local web tool for looking **inside** and **operating** a weir
write-ahead-buffer (WAB) directory. Two views so far:

- **WAB Explorer** — a read-only forensics UI over the segments on disk: their
  lifecycle state, header/footer metadata, the records within (with CRC flags and
  payload previews), integrity/CRC verification, and the dead-letter store.
- **Ops Control Panel** — dead-letter management (requeue / drop, with
  preview → confirm → execute) plus a live status header.

A **Live** view is planned (nav placeholder for now).

This crate is **excluded from the root workspace** so its web dependencies
never touch the published crates' lockfile or the main CI. It depends on
`weir-wab` and `weir-core` by path and is `publish = false`.

## Run

```bash
cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <path-to-wab-dir>
```

Then open the URL it prints (default `http://127.0.0.1:8787`). Flags:
`--wab-dir <path>` (required), `--bind <addr>` (default `127.0.0.1:8787`),
`--static-dir <path>` (default the bundled `static/`). The server binds
localhost only by default, since it exposes on-disk record contents.

## Read-only, works on a stopped daemon

The Explorer **never writes, renames, or deletes** anything in the wab dir, and
it does **not** contact a running daemon — it reads the on-disk segments
directly through the `weir-wab` public read API. So it works against a
**stopped** weir's directory too, which is exactly what you want for
post-mortem forensics. Corruption (CRC mismatch, truncation, torn records) is
surfaced as first-class UI state, not an error.

## HTTP API

All endpoints are rooted at the configured `--wab-dir` and return JSON:

- `GET /api/wab/segments` — full inventory: every segment grouped by shard with
  state, size, header/footer meta, integrity verdict, and confirmed-sidecar
  meta, plus top-level totals.
- `GET /api/wab/segment?path=<rel>&offset=<n>&limit=<n>` — records in one
  segment: per-record index, length, CRC flag, hex/utf8 preview, and
  `terminated_cleanly`. A path that escapes the wab dir is rejected (400).
- `GET /api/wab/dead-letter` — the dead-letter store
  (`<wab-dir>/dead_letter/`); payloads only (there is no per-record reason on
  disk).
- `GET /api/wab/verify?path=<rel>` — run full sealed-segment verification for
  one segment; returns the structured result or the structured error.

## Ops Control Panel

The **Ops** tab manages the **dead-letter store** and shows a live status header. It
**shells out to `weir-ctl`** for every operation (`--json`), so it reuses the CLI's
tested behavior rather than re-implementing it.

- **Status header** — `weir-ctl metrics --json`: daemon up/down, sink health, accepted/
  ack/nack, queue depth, avg fsync ms, WAB + dead-letter bytes, and panic/fsync-failure
  alarms. A down daemon is shown plainly, not as an error.
- **Requeue all** — re-submits every dead-lettered record through the daemon's socket
  (at-least-once; the sink's idempotency key dedupes identical payloads). Shows a dry-run
  preview, a durability selector, then executes.
- **Drop all** — permanently deletes the dead-letter segments. Shows a dry-run preview
  and requires typing the segment count to confirm.
- When the daemon **appears live**, both actions require an extra confirmation.

### Ops flags

- `--metrics-addr <host:port>` — daemon `/metrics` for the status header (default
  `127.0.0.1:9185`).
- `--socket <path>` — daemon Unix socket used by requeue (default `/run/weir/weir.sock`).
- `--weir-ctl <path>` — the `weir-ctl` binary (default: next to this exe, then `PATH`).
- `--read-only` — disable all Ops mutations (requeue/drop + previews); status + listing
  remain.

### Ops HTTP API

- `GET /api/ops/status`, `GET /api/ops/dead-letter`
- `POST /api/ops/requeue/preview`, `POST /api/ops/requeue?durability=<sync|batched|buffered>`
- `POST /api/ops/drop/preview`, `POST /api/ops/drop`

> The console is unauthenticated and localhost-only by default; it both reveals record
> contents and can mutate the dead-letter store. Do not expose it. Use `--read-only` for
> shared/demo instances.

## Theme

`static/weir.css` is a **committed copy of `demo/weir.css`** (the shared "hr2"
theme), so the console is self-contained with no runtime cross-repo link. If the
demo theme changes and you want the console to match, re-copy it.
