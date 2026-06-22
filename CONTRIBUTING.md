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

# Tests: default features, then the full matrix (compiles + runs the
# clickhouse-sink and tls test code the default set never builds).
cargo test
cargo test --all-features

# Dependency advisories, license, bans, and sources.
# Install once: cargo install cargo-deny
cargo deny check advisories bans licenses sources
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

## What's frozen at 1.0

weir is 1.0 under [Semantic Versioning](https://semver.org/). The **v1 wire
protocol**, the **on-disk WAB segment format** (`weir-wab`, `FORMAT_VERSION = 1`),
and the **public Rust API** (`weir-core`, `weir-client`, `weir-sink-sdk`,
`weir-wab`) are frozen — a breaking change to any is a 2.0 change, not a PR. The
wire format has a language-neutral conformance suite
([`docs/conformance.md`](docs/conformance.md)); if you touch the codec, the
vectors in `docs/conformance/wire_v1_vectors.json` must still pass unchanged.

**Known 2.0 cleanup (frozen for now):** `Sink::Record` / the `SinkRecord` trait in
`weir-sink-sdk` is an over-generalisation — the only implementation is the identity
on `Payload`, and every built-in sink uses `type Record = Payload`, so the drain's
generic conversion is a no-op everywhere. It's part of the frozen `Sink` trait, so
it can't be removed without a major version; it's a deliberate candidate to drop in
a hypothetical 2.0, not a bug to "fix" in a 1.x PR. (Rationale in
[`docs/architecture.md`](docs/architecture.md#design-notes).)

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

weir is MIT-licensed ([`LICENSE`](LICENSE)). By contributing, you agree your
contributions are licensed under the same terms.
