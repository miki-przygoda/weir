# Contributing to weir

Thanks for your interest in weir. This guide covers how to build, test, and
submit changes. If you're new to the codebase, read
[`docs/architecture.md`](docs/architecture.md) first — it maps the pipeline
module by module — and [`docs/security/threat-model.md`](docs/security/threat-model.md)
before touching any trust-boundary code.

## The one invariant

weir exists to make one promise: **an ack is never a false ack.** A record
the daemon has acknowledged to a producer is durably recorded and will be
delivered to the sink at least once, even across a crash. Almost every design
choice — group fsync, the WAB, segment confirmation before reclaim,
crash-recovery replay — serves that promise.

When you change anything on the ingest → WAB → ack → drain path, the bar is:
*could this cause the daemon to ack a record it has not durably written, or to
drop a record it has acked?* If you can't rule that out, the change isn't ready.

## Prerequisites

- **Rust 1.88+** (edition 2024) — the declared MSRV (`rust-version` in
  `Cargo.toml`, enforced in CI). `rustup default stable` is enough.
- **A Unix host** (Linux or macOS) to **run** `weir-server` — it uses
  Unix-only socket APIs. The daemon still *builds* on Windows (CI compiles
  it there), but it is a non-functional stub: no Unix-socket listener, so it
  never serves. `weir-core` is genuinely cross-platform; `weir-client`
  compiles everywhere but its client type is Unix-only.
- **Docker** (with the `docker compose` plugin) — only for the optional
  sink-integration and monitoring suites below.

## Recommended: run CI locally before you push

The gate below is the quick subset to run while iterating. Before pushing, it is
worth running **everything CI runs**, in Docker, against the same toolchain — it
catches the red builds that cost a ten-minute round trip and Actions minutes to
discover, and it catches the ones this gate structurally cannot see.

[`deploy/ci-local/README.md`](deploy/ci-local/README.md) describes how, in about
a page: parse `.github/workflows/ci.yml` and execute its own `run:` steps rather
than restating them, so the local run cannot drift from CI. It also lists the
traps worth knowing before you start — the two kinds of `uses:` step and how
conflating them produces a runner that passes having checked nothing, plus the
arm64, stale-artifact and toolchain-version issues that bite in practice.

No tooling is committed for this, and that note explains why.

## The pre-PR gate

Run this before opening a PR. It is exactly what CI enforces (`.github/workflows/ci.yml`),
so running it locally first avoids a red CI round-trip:

```bash
# Formatting
cargo fmt --check

# Lints across the whole feature matrix (CI denies warnings on all three).
# A lint that only trips under clickhouse/tls must still be fixed.
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings

# The demo bundle's version banner is GENERATED from [workspace.package]
# version. CI's lint job regenerates it and fails on any diff, so a version
# bump that forgets this turns the whole lint job red — and every job that
# depends on lint (test, dst, load, build, monitoring, bench) then SKIPS.
./scripts/sync-demo-version.sh && git diff --exit-code demo/version.js

# Tests. weir-server's bin unit tests MUST run serially: socket::bind_hardened
# mutates the process-global umask around bind(2), and a directory created by
# another thread in that window loses its execute bit. See
# docs/security/socket-bind.md. Running them in parallel produces ~66 spurious
# PermissionDenied failures and leaves proptest-regressions/ unwritable.
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1

# The same three, across the full feature matrix (compiles + runs the
# clickhouse-sink and tls test code the default set never builds).
cargo test --workspace --exclude weir-server --all-features
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener --all-features
cargo test -p weir-server --bins --all-features -- --test-threads=1

# Dependency advisories, license, bans, and sources.
# Install once: cargo install cargo-deny
cargo deny check advisories bans licenses sources

# Minimum supported Rust version, and that Cargo.lock is consistent.
# --locked is what catches a lockfile edited without a matching manifest.
rustup toolchain install 1.88 --profile minimal   # once
rustup run 1.88 cargo check --workspace --all-features --locked

# Deterministic simulation sweep, and that the load suite still compiles.
cargo test -p weir-server --bin weir-server --features dst "wab::dst::" -- --test-threads=1
cargo test -p weir-server --test load --no-run

# The docs book, with link checking. THIS IS PART OF CI AND WAS MISSING HERE
# until 2026-08-31, when a PR went red on two link errors no local gate could
# have caught: a page linked from inside the book but absent from SUMMARY.md,
# and a link escaping the book root. Both are errors, not warnings.
# Install once (match .github/workflows/docs.yml exactly):
#   mdbook 0.4.40 and mdbook-linkcheck 0.7.7
mdbook build
```

All of the above must pass. CI builds `weir-server` on all five release targets:
Linux (x86_64 + aarch64), macOS (x86_64 + aarch64), **and Windows
(x86_64-pc-windows-msvc)** — so **cfg-gate any Unix-only code** or the Windows
build breaks. The Windows build is a non-functional stub (no Unix-socket
listener); the daemon runs only on Linux and macOS.

## Heavier suites (run when your change touches them)

These run in their own CI jobs; run them locally when relevant:

```bash
# Deterministic simulation sweep of the WAB durability invariants.
# Replays the pinned regression seeds in tests/dst_seeds/ plus a random sweep.
# A violated invariant prints a WEIR_DST_SEED you can replay. Run this for any
# change to the WAB, recovery, flusher, or drain.
WEIR_DST_SWEEP=300 cargo test -p weir-server --bin weir-server \
    --features dst "wab::dst::" -- --test-threads=1

# Load / benchmark scenarios (release build; emits BENCH: JSONL).
cargo test -p weir-server --test load --release -- --nocapture

# SQL sink end-to-end tests against real MySQL + Postgres (brings up and tears
# down a docker-compose stack; needs ports 33306 / 55432 free).
bash deploy/run-sink-integration-tests.sh

# Observability end-to-end: weir + Prometheus + Grafana stack smoke test.
deploy/monitoring/smoke-test.sh --teardown

# Fuzzing the trust-boundary parsers (needs nightly Rust + cargo-fuzz).
# Targets live in fuzz/fuzz_targets/ — see docs/testing/fuzzing.md.
cargo +nightly fuzz run envelope_parse
```

## Stability and what's frozen

weir follows [Semantic Versioning](https://semver.org/). The **v1 wire
protocol** is frozen and unchanged in 2.0. The **on-disk WAB segment format**
(`weir-wab`) gained a version 2 in 2.0 — segments are still written as v1 unless
compression is enabled, and readers accept both. The **public Rust API**
(`weir-core`, `weir-client`, `weir-sink-sdk`, `weir-wab`) is under SemVer: a
breaking change is a major, not a PR. The wire format has a language-neutral
conformance suite
([`docs/conformance.md`](docs/conformance.md)); if you touch the codec, the
vectors in `docs/conformance/wire_v1_vectors.json` must still pass unchanged.

**Done in 2.0:** `Sink::Record` / the `SinkRecord` trait was an
over-generalisation — the only implementation was the identity on `Payload`, and
every built-in sink used `type Record = Payload`, so the drain's generic
conversion was a no-op everywhere. Being part of the frozen `Sink` trait, it
could not be removed without a major version. 2.0 removes it: records are
`Payload`, `CommitResult` is no longer generic, and `commit` takes a
`SinkBatch` carrying the batch's dedup token.

For the design rationale behind the crate split and the configuration surface, see
the [Architecture doc](docs/architecture.md#workspace--crate-boundaries).

## Tests and commits

- **Demonstrate, don't assert.** A test for a fix should fail before the fix
  and pass after. Never write a test that locks in behaviour you believe is
  wrong — if you find a logic bug while adding coverage, flag it rather than
  encoding it.
- **One logical change per commit.** Use a conventional-commit-style subject
  (`fix(wab): …`, `docs(monitoring): …`, `test(dst): …`). Keep the working
  tree clean — no unrelated churn in a PR.
- **Update the docs in the same change.** `docs/` is the source of truth; if
  you change a config option, metric, or wire behaviour, update the relevant
  reference page in the same PR.
- **Note the verification you ran** in the PR description — which suites from
  the gate above, and any heavier suite you exercised.

## Reporting bugs and security issues

- **Bugs / features:** open a GitHub issue with a minimal reproduction and the
  weir version (or commit).
- **Security vulnerabilities:** do **not** open a public issue — follow
  [`SECURITY.md`](SECURITY.md) (private advisory or direct contact).

## License

weir is licensed under the Apache License 2.0 ([`LICENSE`](LICENSE)). By
contributing, you agree your contributions are licensed under the same terms.
