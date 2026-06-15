# weir documentation

This directory is the source of truth for all weir documentation. It is
organised by user role:

- **Operators** — running weir in production — start at
  [Configuration](#operations).
- **Application developers** — pushing records from your service — start
  at [Getting started](#getting-started).
- **Client implementers** — writing a non-Rust client — start at
  [Protocol](#protocol).
- **Contributors** — modifying the daemon — start at [Architecture](#architecture).
- **Security reviewers** — auditing the design — start at
  [Security](#security).

A mdBook-rendered version of this directory is published to GitHub
Pages on every push to `main`; the in-repo Markdown is the canonical
source.

---

## Getting started

Two short docs to get you from zero to a running daemon with records flowing.

- [**install.md**](getting-started/install.md) — Build from source,
  container, or `cargo install` (planned). Verification + uninstall.
- [**quickstart.md**](getting-started/quickstart.md) — 5-minute hello
  world: run the daemon, push a record from a tiny Rust client,
  verify the metrics endpoint, troubleshoot common issues.

---

## Operations

Documentation for running weir in production.

- [**configuration.md**](operations/configuration.md) — Every config
  option: default, range, CLI flag, env var, TOML key, what it
  controls, and when to tune. Plus minimal-config and production-config
  examples. *(canonical reference; ~470 lines)*
- [**tcp-mtls.md**](operations/tcp-mtls.md) — The optional TCP + mutual-TLS
  listener for cross-host producers (behind the `tls` feature): certificate
  setup, config keys, and live cert reload on `SIGHUP`.
- [**monitoring.md**](monitoring.md) — Prometheus metrics at `/metrics`,
  the per-alert runbook, and the Grafana dashboard. Turnkey monitoring stack
  (weir + Prometheus + Grafana) under [`deploy/monitoring/`](https://github.com/miki-przygoda/weir/tree/main/deploy/monitoring).
- *Tuning guide* and *disaster-recovery runbook* — operator-facing guides.
  *(planned)*

---

## Protocol

For implementing a client in a non-Rust language, or understanding
the on-disk WAB layout.

- [**wire_protocol.md**](wire_protocol.md) — Frame layout (16-byte
  header + payload + CRC32), message types, validation order, nack
  payload formats, version negotiation.
- [**wab_format.md**](wab_format.md) — WAB segment binary format,
  `.confirmed` sidecar, crash-recovery algorithm, segment lifecycle
  (active → sealed → confirmed → deleted).
- [**conformance.md**](conformance.md) — Language-neutral conformance
  vectors (`conformance/wire_v1_vectors.json`): canonical hex frames plus
  the result a conformant decoder must produce for every message type, all
  nine Nack reasons, and each decode-rejection case. Run your codec against
  them to confirm byte-compatibility.
- *Writing a client* — practical client-implementation tutorial.
  *(planned)*

---

## Architecture

For contributors to the daemon, or anyone wanting to understand the
internal data flow and component responsibilities.

- [**architecture.md**](architecture.md) — Module-by-module breakdown:
  socket layer, queue, worker pool, WAB, drain, sink trait, metrics.
  Runtime boundary (where tokio ends and `std::thread` begins).
  Security-relevant design choices.
- [**benchmarks.md**](benchmarks.md) — Latest CI results, environment
  guide, per-version history, batch-tuning empirical sweep.

---

## Security

Three security-design docs cover the threat model, the most subtle
hardening detail (socket bind TOCTOU), and the container deployment
review. The reporting policy is at the repo root.

- [**security/threat-model.md**](security/threat-model.md) — Trust
  boundary, in-scope vs out-of-scope threats, operator-side
  assumptions, threat actors considered. *Start here.*
- [**security/socket-bind.md**](security/socket-bind.md) — Full TOCTOU
  analysis of the original socket bind sequence (lstat → check →
  remove → bind → chmod) and the hardened sequence (dirfd +
  AT_SYMLINK_NOFOLLOW + umask-tightened bind + inode-equality check).
  Documents the residual race window and the operator's responsibility.
- [**security/container.md**](security/container.md) — Production
  Dockerfile review: pinned UID, supply-chain considerations, what the
  image deliberately does **not** do, recommended `docker run`
  invocation with cap-drop and read-only filesystem.
- [**SECURITY.md**](https://github.com/miki-przygoda/weir/blob/main/SECURITY.md) — Vulnerability reporting policy (lives at repo root for GitHub's Security tab integration).

---

## Testing

The unit + system + load test suites in `crates/weir-server/tests/` are
the day-to-day coverage. The docs below describe the larger-grained
test infrastructure that runs separately:

- [**testing/test-audit.md**](testing/test-audit.md) — Verdicts on
  every test in `tests/system.rs` (KEEP / STRENGTHEN / DELETE /
  RENAME / REWRITE), with notes on what each guards against. The
  reference document when adding or removing a system test.
- [**testing/sink-integration.md**](testing/sink-integration.md) —
  How to run the MySQL + Postgres end-to-end tests against real
  databases via the docker-compose harness at
  `deploy/docker/test/` (one-command runner:
  `bash deploy/run-sink-integration-tests.sh`).
- [**testing/fuzzing.md**](testing/fuzzing.md) — `cargo-fuzz` setup
  and target catalogue. Currently two coverage-guided targets
  for the trust-boundary parsers (`wab_confirmed`,
  `envelope_parse`); needs nightly Rust.

---

## Benchmarks

- [**benchmarks.md**](benchmarks.md) — Hub doc.
- [**benchmarks/latest.md**](benchmarks/latest.md) — Most recent CI
  measurement.
- [**benchmarks/history.md**](benchmarks/history.md) — Per-version
  history.
- [**benchmarks/environments.md**](benchmarks/environments.md) — CI vs
  local-machine notes.
- [**benchmarks/batch-tuning.md**](benchmarks/batch-tuning.md) —
  Empirical sweep of `batch_size` × `batch_deadline_ms` that produced
  the current (256, 1ms) defaults.

---

## Doc conventions

If you're contributing to the docs:

- **One topic per file.** Cross-link aggressively; don't duplicate.
- **Balanced tone**: lead with a TL;DR for evaluators, follow with
  operator-grade detail. Each major heading should be skimmable.
- **Self-contained**: every doc should make sense loaded as the only
  context (this matters for both new readers and AI agents).
- **Concrete over hand-wavy**: code examples, real config values,
  specific file paths. Avoid "may" and "should" when "does" or "must"
  fits.
- **Cross-references use relative paths**: `../security/threat-model.md`,
  not `https://github.com/...`. The mdBook publish flow rewrites them
  to absolute URLs.
