# weir-console

A small local web tool for looking **inside** a weir write-ahead-buffer (WAB)
directory. The first (and currently only) view is the **WAB Explorer**: a
read-only forensics UI over the segments on disk — their lifecycle state,
header/footer metadata, the records within (with CRC flags and payload
previews), integrity/CRC verification, and the dead-letter store. An **Ops**
and a **Live** view are planned (nav placeholders for now).

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

## Theme

`static/weir.css` is a **committed copy of `demo/weir.css`** (the shared "hr2"
theme), so the console is self-contained with no runtime cross-repo link. If the
demo theme changes and you want the console to match, re-copy it.
