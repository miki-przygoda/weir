# weir-console — Ops Control Panel (view E) — design spec

**Date:** 2026-06-21 · **Status:** approved design, pre-plan

## Goal

Build the second view of **`weir-console`**: the **Ops Control Panel**, a browser GUI
for **dead-letter management** (requeue / drop, with a preview → confirm → execute
flow) fronted by a **live operational-status header**. It surfaces the *actions* that
neither the read-only WAB Explorer (view D) nor the Grafana/Prometheus stack covers —
getting dead-lettered records back into the pipeline, or discarding them — with the
guard-rails an operator-facing tool should have.

## Context & decisions (from the brainstorm)

- `weir-console` is one unified tool with three views: **Explorer (D)** — built —,
  **Ops (E)** — this spec —, and **Live (G)** — its own spec later. The nav's
  currently-disabled **Ops** tab activates here.
- **Scope:** dead-letter management (the mutating actions) **plus** a focused live-status
  header. NOT a Grafana clone (we deliberately chose Prometheus+Grafana for dashboards)
  and NOT general daemon control (the daemon has no admin/control HTTP API — only
  `/metrics` and the wire socket).
- **Mutations exposed:** **requeue** (re-submit dead-lettered records through the daemon)
  and **drop** (delete dead-letter segments). Requeue uses a simple confirm; **drop
  requires typing the segment count** from the preview to confirm (forces reading it).
- **Live-daemon safety:** mutations are **always permitted**, but when the daemon
  **appears live** both flows show a prominent warning banner and require an **extra
  explicit confirm**. (Requeue *requires* a live daemon — see below — so "block when
  live" was rejected.) Relies on `weir-ctl`'s existing TOCTOU hardening (acts only on
  `*.wab.sealed`, refuse-to-clobber).
- **Implementation:** **shell out to the sibling `weir-ctl` binary** with `--json` for
  every operation. `weir-ctl`'s dry-run (no `--yes`) powers the **preview**; `--yes`
  **commits**. Zero duplication of the durability-adjacent mutation logic; the console
  stays excluded from the published workspace.

### Why requeue needs a live daemon

`weir-ctl dl requeue` is **not** a file move: it reads each dead-letter segment's
records and **re-pushes them through the daemon's Unix socket** (re-ingesting them),
then deletes the segment once all its records are re-accepted. Re-delivery is
at-least-once (an identical payload is deduped by the sink's idempotency key); a sealed
segment with any unreadable record is **skipped wholesale** (never partially
re-delivered). So requeue inherently needs the daemon up. `dl drop` is a pure file
delete (daemon up or down).

## Architecture

Extend the existing `tools/weir-console/` crate (excluded from the root workspace,
`publish = false`, depends on `weir-wab` + `weir-core` by path). Ops shells out to
`weir-ctl` rather than linking it, so **no new crates** are added — the tool's existing
`tokio` dependency only gains the `process` feature.

- **Backend** — a new `ops` module + `/api/ops/*` routes added to the existing axum
  `Router`. Every operation runs the sibling `weir-ctl` binary via
  `tokio::process::Command` with `--json` and parses stdout (or the stderr error object
  on non-zero exit). The Explorer's `wab` module is untouched and stays read-only.
- **Frontend** — a new `ops.html` + `ops.js` under `static/`, reusing the committed
  hr2 `weir.css` and the existing console shell/nav. No new framework, no build step.

### The shell-out surface

`weir-ctl`'s relevant commands (verified against `crates/weir-ctl/src/main.rs`):

- `--json` is a **global** flag (any position).
- `weir-ctl metrics --addr <host:port> --json` — health summary as JSON.
- `weir-ctl dl list --wab-dir <dir> --json` — dead-letter segments (count + bytes);
  the `*.wab.sealed`-only actionable view.
- `weir-ctl dl drop --wab-dir <dir> [--yes] --json` — delete **ALL** dead-letter
  segments (whole-store; dry-run without `--yes`).
- `weir-ctl dl requeue --wab-dir <dir> --socket <path> --durability <sync|batched|buffered> [--yes] --json`
  — re-push **ALL** readable dead-letter records (whole-store; dry-run without `--yes`;
  durability default `batched`).

`weir-ctl` defaults: socket `/run/weir/weir.sock`, metrics addr `127.0.0.1:9185`.

### Ops endpoints

| Endpoint | `weir-ctl` invocation | Notes |
|---|---|---|
| `GET /api/ops/status` | `weir-ctl --json metrics --addr <addr>` | Daemon up/down derived from exit status; returns the health summary. |
| `GET /api/ops/dead-letter` | `weir-ctl --json dl list --wab-dir <dir>` | The actionable `*.wab.sealed` list the mutations operate on. |
| `POST /api/ops/requeue/preview` | `weir-ctl --json dl requeue --wab-dir <dir> --socket <path> --durability <d>` (no `--yes`) | Dry run: how many records/segments would requeue, how many skipped. |
| `POST /api/ops/requeue` | `… dl requeue … --durability <d> --yes` | Commit. Body: `{durability, live_confirmed}`. |
| `POST /api/ops/drop/preview` | `weir-ctl --json dl drop --wab-dir <dir>` (no `--yes`) | Dry run: M segments / R records / B bytes. |
| `POST /api/ops/drop` | `… dl drop … --yes` | Commit. Body: `{confirm_count, live_confirmed}`. |

The server constructs every argument itself (no user-supplied strings reach the command
line except the validated durability enum and the operator's confirm fields), so there
is no shell-injection surface; `Command` is invoked without a shell.

### Configuration (new CLI args, all additive)

- `--metrics-addr <host:port>` — daemon `/metrics` address for the status header
  (default `127.0.0.1:9185`, matching `weir-ctl`).
- `--socket <path>` — daemon Unix socket for requeue's re-push (default
  `/run/weir/weir.sock`).
- `--weir-ctl <path>` — the `weir-ctl` binary to invoke. **Default resolution:** look for
  `weir-ctl` next to the running `weir-console` executable (`std::env::current_exe`'s
  dir), then fall back to `weir-ctl` on `PATH`.
- `--read-only` — disable **all** `/api/ops/*` mutation endpoints (requeue/drop +
  their previews); status + dead-letter listing remain. A safety valve for shared/demo
  instances. The existing `--wab-dir`/`--bind` are unchanged.

The server remains **localhost-only by default** and is **unauthenticated** — the
README/startup banner state plainly that it must not be exposed (it both reveals record
contents and can mutate the store).

### Server self-check

On startup the server resolves `--weir-ctl` once and probes it
(`weir-ctl --version`); if it cannot be found it still starts (the Explorer works
without it) but logs a clear warning, and every `/api/ops/*` call returns a structured
`{error}` explaining that `weir-ctl` was not found and how to point `--weir-ctl` at it.

## Frontend — the Ops view

`ops.html` + `ops.js`, hr2-styled by reusing `weir.css` and the console shell. Layout:

- **Live-status header** — polls `GET /api/ops/status` every few seconds (the existing
  page-visibility-friendly cadence): a daemon **up/down** dot, a one-line shard summary,
  drain lag / stranded count, dead-letter count + bytes, **sink health**, and fsync
  p50/p99 — taken from the `weir-ctl metrics --json` summary. If the daemon is down the
  header shows it clearly and is not an error.
- **Dead-letter panel** — the `dl list` view (segments, records, bytes) plus two actions:
  - **Requeue all** → calls the requeue *preview* → a modal shows "would requeue R
    records from M segments (X corrupt segment(s) skipped)", a **durability selector**
    (default `batched`), and an at-least-once / sink-dedupe note → on confirm, calls the
    requeue commit → renders the per-result outcome.
  - **Drop all** → calls the drop *preview* → a modal shows "would delete M segments / R
    records / B bytes — **IRREVERSIBLE**" and a field to **type the segment count `M`**
    to enable the confirm button → on confirm, calls the drop commit → renders the
    outcome.
  - When `/api/ops/status` indicates the daemon is **live**, both modals show a
    prominent warning and require the operator to also tick an "I understand the daemon
    is running" box (`live_confirmed`) before the action proceeds.
- **Banner** for operational errors (`weir-ctl` missing, socket unreachable, dry-run
  failure) — the structured `{error}` rendered inline, never a crash.
- In `--read-only` mode the action buttons are absent/disabled with a "read-only" note;
  the status header + listing still render.

JS is plain `fetch` + DOM rendering; no framework.

## Data flow

```
browser ──fetch/JSON──▶ weir-console /api/ops/* ──spawn weir-ctl --json──▶ {daemon socket | /metrics | wab dir}
```

The console never opens the daemon socket or parses `/metrics` itself — `weir-ctl` is
the single execution path for both reads (status, list) and writes (requeue, drop), so
the console can't drift from the CLI's tested behavior.

## Error handling

- **`weir-ctl` not found / not executable** → every `/api/ops/*` returns
  `{error: "weir-ctl not found …"}` (HTTP 500) with remediation; the server does not
  panic and the Explorer keeps working.
- **`weir-ctl` non-zero exit** → its `--json` error object (emitted on stderr) is
  captured and surfaced verbatim in the response `{error}` (e.g. socket unreachable for
  requeue, unreadable segment). The HTTP status reflects the failure class.
- **Daemon down** for `status` → a normal `{daemon: "down"}` response, not an error.
- **`--read-only`** → mutation endpoints return HTTP 403 `{error: "read-only mode"}`.

## Testing

- **Backend integration tests** drive the axum router in-process
  (`tower::ServiceExt::oneshot`) with a **stub `weir-ctl`** — a tiny executable script
  written into a tempdir that the test points `--weir-ctl` at. The stub inspects its
  args and emits canned `--json` for each subcommand (and a non-zero exit + stderr JSON
  for the failure cases). Tests assert:
  - `GET /api/ops/status` parses the metrics summary; daemon-down maps cleanly.
  - `GET /api/ops/dead-letter` parses the `dl list` JSON.
  - requeue/drop **preview** invoke the stub **without `--yes`**, and **commit** invoke
    it **with `--yes`** (this guard — that a preview can never mutate — is the most
    important assertion); the constructed args include the right `--durability`,
    `--socket`, `--wab-dir`.
  - `--read-only` makes the four mutation endpoints return 403 and never spawns the stub.
  - a non-zero stub exit surfaces the stderr error JSON as a clean `{error}`, no panic.
  - a missing `weir-ctl` path yields the structured "not found" error.
- **Frontend smoke test** (`node --test`, mock JSON, mirroring the Explorer's
  `explorer.test.mjs`): the status header renders from a mock summary; the requeue
  preview→confirm renders and the confirm calls the commit endpoint; the drop confirm
  button stays disabled until the typed count matches; the live-daemon warning/checkbox
  gate appears when status says live.

## Placement, workspace, deps

- Add to `tools/weir-console/`: `src/ops.rs`, `static/ops.html`, `static/ops.js`,
  Ops routes in `src/server.rs`, the new CLI args in `src/main.rs`, and Ops backend
  tests under `tests/`. README updated with the Ops view, the new flags, the
  `weir-ctl`-dependency note, and the read-only/localhost warnings.
- **No new crates** beyond what the tool already has — only the `process` feature is
  added to the existing `tokio` dependency. The published workspace, its lockfile, and
  CI are unaffected (tool stays excluded).

## Out of scope (this spec)

- The **Live demo (view G)** — its own spec.
- Any daemon control beyond dead-letter (config edits, signals, shard reconfiguration) —
  the daemon exposes no admin API.
- Per-segment requeue/drop — `weir-ctl` operates whole-store; per-segment selection is a
  possible later `weir-ctl` enhancement, not a console-only feature.
- Authentication / non-localhost exposure / a hosted service.

## Success criteria

- `cargo run --manifest-path tools/weir-console/Cargo.toml -- --wab-dir <dir>` serves
  the Ops tab; the status header reflects a running daemon (or cleanly shows it down);
  **Requeue all** and **Drop all** each run a real `weir-ctl` dry-run preview, require
  the right confirmation (typed count for drop; extra confirm when live), and on commit
  invoke `weir-ctl … --yes` and report the outcome.
- A preview **never** passes `--yes`; `--read-only` disables every mutation.
- The backend never panics on a missing `weir-ctl`, a down daemon, or a `weir-ctl`
  failure; errors render as banners.
- Backend integration tests (stub `weir-ctl`) + the frontend smoke test pass;
  `cargo fmt`/`clippy --all-targets -- -D warnings` clean for the tool; the main
  workspace + its lockfile/CI are unaffected.
