# weir-console — WAB Explorer (view D) — design spec

**Date:** 2026-06-21 · **Status:** approved design, pre-plan

## Goal

Build the first view of **`weir-console`**, a real local tool that surfaces a live
weir's state to a browser: the **WAB Explorer**, a read-only forensics UI for
looking *inside* a weir write-ahead-buffer directory — segments, their lifecycle,
the records within, integrity/CRC status, and the dead-letter store. It is the
visual proof that "your durable log is real and inspectable," and it exercises the
new (1.2.0) `weir-wab` public read API end-to-end.

## Context & decisions (from the brainstorm)

- `weir-console` is **one unified tool** with a shared Rust backend + one hr2-styled
  web frontend and three views: **Explorer (D)**, **Ops (E)**, **Live (G)**. This
  spec covers **D only** plus the minimal shell D needs; **E and G are out of scope
  here** (each gets its own spec/plan later).
- **Run model:** runs locally against a real weir; for the public site we later
  capture a recorded walkthrough / screenshots into a static asset (not part of this
  increment's core — noted under "Showcase output").
- **Build model:** this spec → implementation plan (writing-plans) → agents build
  under the plan, gated/reviewed.
- The Explorer is **strictly read-only** — it never mutates the wab dir.

## Architecture

A new crate **`tools/weir-console/`**, **excluded from the root workspace** (add
`exclude = ["tools/weir-console"]` to the root `Cargo.toml` `[workspace]`) so its web
deps never touch the published crates' lockfile or the main CI. It depends on
`weir-wab` and `weir-core` **by path**.

- **Backend** — a small async HTTP server (Rust). Modules:
  - `wab` — the Explorer's data source (this spec).
  - `ops`, `live` — created as empty stubs only (for the shell's nav); implemented in
    later specs.
- **Frontend** — static assets served by the backend: vanilla HTML/CSS/JS, reusing the
  hr2 theme via a **committed copy** at `tools/weir-console/static/weir.css` (derived
  from `demo/weir.css`; the README records the provenance), so the console is
  self-contained. No build step, no framework.

### Dependencies (tool crate only)
`tokio` (rt-multi-thread, macros), `axum` (HTTP + routing), `tower-http` (`ServeDir`
for static files), `serde` + `serde_json` (JSON responses), `weir-wab` (path),
`weir-core` (path). Dev: `tempfile` (fixtures). These are acceptable for a
`publish = false` local tool that's outside the published workspace.

### Configuration
CLI args via `clap` (already used by `weir-ctl`): `--wab-dir <path>`
(required; the weir wab directory to inspect), `--bind <addr>` (default
`127.0.0.1:8787`), `--static-dir <path>` (default the bundled `static/`). The server
is localhost-only by default (it exposes on-disk record contents).

## Backend — the `wab` module

Read-only JSON endpoints, all rooted at the configured `--wab-dir`. All segment reads
go through the **`weir-wab` public read API** — no re-implementation of the format.

### Endpoints

1. **`GET /api/wab/segments`** — the full inventory.
   - Uses `weir_wab::list_segment_files(wab_dir)` → `(PathBuf, SegmentState)` per file,
     grouped by shard (parse the `shard_NN` parent dir). The dead-letter dir is listed
     separately (see endpoint 3), not here.
   - For each segment include: `shard`, `file` (relative path), `state`
     (`"active" | "sealed" | "confirmed"`), `size_bytes`.
   - For readable segments, add `header` from `parse_segment_header` (the first
     `SEGMENT_HEADER_LEN` bytes): `{format_version, shard_id, created_at}`.
   - For **sealed** segments add `footer` from `parse_segment_footer` (the trailing
     `SEGMENT_FOOTER_LEN` bytes): `{record_count, data_bytes, file_crc32, sealed_at}`,
     and `integrity` from `verify_sealed_segment`: either `{"ok": true}` or
     `{"ok": false, "error": <SegmentVerifyError as a tagged string + fields>}`
     (e.g. `{"kind":"CrcMismatch","expected":"0x...","computed":"0x..."}`,
     `{"kind":"TrailingBytes"}`, `{"kind":"BadRecord"}`, `{"kind":"TooShort"}`, …).
   - For **confirmed** sidecars, add `confirmed` from `parse_confirmed`:
     `{sealed_at, record_count, drained_at}`.
   - Also return top-level totals: `{segments, sealed, active, confirmed, dead_letter,
     total_bytes}`.

2. **`GET /api/wab/segment?path=<rel>&offset=<n>&limit=<n>`** — records in one segment.
   - Validate `path` is inside `wab_dir` (reject `..`/absolute escapes → 400).
   - Open with `SegmentReader::open`; iterate, skipping `offset`, taking up to `limit`
     (default 100, max 1000). Per record: `{index, len, crc_ok, hex_preview,
     utf8_preview}` — `hex_preview`/`utf8_preview` capped (e.g. first 256 bytes;
     `utf8_preview` via `String::from_utf8_lossy`). A record that yields `Err`
     (CRC mismatch / oversized / torn) is reported as `{index, error: <kind>}` and
     iteration stops (matches the reader's contract).
   - Include `terminated_cleanly` (`true`/`false`/`null`) from
     `SegmentReader::terminated_cleanly()` after iteration, and `header` (as above).

3. **`GET /api/wab/dead-letter`** — the dead-letter store (`<wab_dir>/dead_letter/`).
   - List `*.wab.sealed` (+ any active `*.wab`) there via `list_segment_files`, and for
     each, the record count + bytes. Records are read the same way as endpoint 2.
   - **Note:** the dead-letter store holds **payloads only** — there is no per-record
     reason on disk (`dead_letter.rs::write_records` writes just `Payload`s). The UI
     shows the payloads and labels them dead-lettered; do not invent a reason field.

4. **`GET /api/wab/verify?path=<rel>`** — run `verify_sealed_segment` for one segment
   and return the full `SegmentVerification` (`{header, footer}`) or the structured
   `SegmentVerifyError`. (Powers the per-segment "verify" action + "verify all".)

### Read-only guarantee
The module opens files read-only and never writes, renames, or deletes. No daemon is
contacted (the Explorer reads the on-disk wab dir directly, so it works against a
stopped daemon's dir too — important for post-mortem forensics).

## Console shell (minimal, for D)

- An axum `Router`: `/api/wab/*` (above), `/` + static assets via `tower_http::ServeDir`
  pointing at the static dir, and a catch-all that serves `index.html`.
- `index.html` is the Explorer page; the top nav shows **Explorer** (active) plus
  disabled/"coming soon" **Ops** and **Live** tabs (placeholders for E/G).
- `main.rs`: parse args, build the router, bind, serve; print the URL.

## Frontend — the Explorer view

Static, vanilla, hr2-styled (uses the committed `static/weir.css` copy described in
Architecture, so the console is self-contained — no runtime cross-repo link). Layout:

- **Top bar:** the wab-dir path, the totals from `/api/wab/segments`, and a
  **"Verify all"** button (calls `/api/wab/verify` per sealed segment, paints badges).
- **Left pane:** a shard → segment tree. Each segment shows its name, a **state chip**
  (active / sealed / confirmed / dead-letter), and an **integrity badge** for sealed
  ones (✓ ok / ✗ + error kind). Clicking selects it.
- **Center pane:** the selected segment's detail — header + footer meta, the integrity
  verdict (with expected/computed on a CRC mismatch), and a **paginated record list**
  (`/api/wab/segment`): `index · len · CRC ✓/✗ · payload preview` with a **hex ⇄ utf8**
  toggle. A torn/`Err` record renders as an explicit error row; `terminated_cleanly`
  is shown as a clean-end vs torn-tail marker at the bottom.
- A **dead-letter** entry in the tree (from `/api/wab/dead-letter`) opens the same
  record view.

JS is plain `fetch` + DOM rendering; no framework, no bundler.

## Data flow

`browser ──fetch/JSON──▶ weir-console backend ──weir-wab read API──▶ on-disk wab dir`.
One direction; nothing writes.

## Error handling

- **Corruption is a first-class display, not an error.** Every `SegmentVerifyError`
  variant maps to a JSON `{kind, …}` and renders as a badge/row: `CrcMismatch`
  (expected vs computed), `TrailingBytes`, `BadRecord`, `TooShort`, `Header(..)`,
  truncation. This is the whole point of the tool — surface what the new read API
  detects.
- **Operational errors** (bad/missing `--wab-dir`, unreadable path, path-escape
  attempt) return a clear HTTP 4xx/5xx with a JSON `{error}` the frontend shows as a
  banner; the server does not panic.
- An empty or partially-populated wab dir renders cleanly (zero segments, no crash).

## Testing

- **Backend integration tests** over a **fixtures wab dir** built with `weir-wab`'s
  write side (and a couple of deliberately-corrupted files written by hand):
  fixtures = a clean **sealed** segment, an **active** segment, a **confirmed** sidecar,
  a **CRC-corrupted** sealed segment, a **truncated** segment, and a **dead-letter**
  segment. Tests assert:
  - `/api/wab/segments` returns the right state + header/footer/confirmed meta + totals,
    and `integrity.ok=false` with the right `kind` for the corrupted/truncated ones.
  - `/api/wab/segment` returns the right record count, `crc_ok` flags, previews,
    `terminated_cleanly`, and an error row for the corrupted record.
  - `/api/wab/dead-letter` lists the dead-letter records (payloads, no reason).
  - `/api/wab/verify` returns ok for the good segment and the structured error for the
    corrupted one.
  - path-escape (`..`, absolute) → 400; missing wab dir → clear error, no panic.
  (Drive the router in-process with `tower::ServiceExt::oneshot` — no real socket.)
- **Frontend smoke test:** a tiny check (node or a headless DOM stub, mirroring the
  existing demo harness) that the Explorer renders segment rows + a record list from
  mock JSON and the hex⇄utf8 toggle works — no real backend.

## Placement, workspace, deps

- `tools/weir-console/` with its own `Cargo.toml` (`publish = false`), `src/` (`main.rs`,
  `wab.rs`, stub `ops.rs`/`live.rs`), `static/` (`index.html`, `explorer.js`, `weir.css`),
  and `tests/`.
- Root `Cargo.toml`: add `exclude = ["tools/weir-console"]` to `[workspace]`.
- A short `tools/weir-console/README.md`: what it is, how to run
  (`cargo run -p weir-console -- --wab-dir <dir>` from the tool dir), the read-only note.

## Showcase output (noted, not core to this increment)

Once the Explorer works, capture a screenshot/recorded walkthrough (against a real
wab dir with some sealed + dead-letter + a deliberately-corrupted segment) as a static
asset for the demo site — the "look inside the durable log" visual. Tracked as a
follow-up, not blocking the tool.

## Out of scope (this spec)

- The **Ops (E)** and **Live (G)** views (own specs).
- Any **mutation** (e.g. dead-letter requeue) — that's an Ops/E concern.
- A **deployed/hosted** weir-console service.
- A CI job for the tool (it's excluded from the main workspace; a dedicated build/test
  job is an optional follow-up so it doesn't rot — noted, not required here).

## Success criteria

- `cargo run -p weir-console -- --wab-dir <real wab>` serves the Explorer at the bound
  URL; the tree shows every segment with correct state + integrity, the center pane
  shows header/footer meta + a paginated, CRC-flagged record list with payload previews,
  and a corrupted/truncated segment renders its structured error rather than failing.
- The backend is strictly read-only and never panics on a bad/empty/corrupt wab dir.
- Backend integration tests + a frontend smoke test pass; `cargo fmt`/`clippy` clean for
  the tool crate; the main workspace + its lockfile/CI are unaffected (tool is excluded).
