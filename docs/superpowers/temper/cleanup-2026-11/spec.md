# cleanup-2026-11: spec

Written by the head session on 5 Nov 2026 for an overnight temper run. This epic
builds nothing new: it is a stabilisation pass over the whole repository, run as
temper stage 4 (sweep, verify, careful fixes, regression check per fix, second
sweep) then stage 5 (docs, evidence, full gate, PR).

## Goal

Every real defect in weir at `main` 5e32265 (4.1.0 plus docs) that has an input
and a wrong result is found, proven, and fixed with a failing test first, without
any fix breaking something else; docs that say something the code does not do are
corrected. The result is one PR the owner can review and merge, and the base for
a 4.1.1.

## Scope

**The whole repository at the branch tip, not only the branch's diff.** The epic
branch starts equal to `main`, so `git diff main...epic/cleanup-2026-11` shows
only this folder: reviewers read the code itself.

Each review lens covers an area (paths from the repo root):

| Lens | Area |
|---|---|
| security | every crate; trust boundaries: the client socket and TCP/TLS listener, the admin socket, HTTP health/metrics, config and env, sink credentials, file permissions under `wab_dir` |
| failure paths and lifecycle | startup, shutdown, reload, signals, sink outages, disk full, partial writes, every `?`, `unwrap`, `expect` and ignored `Result` on a path a user can reach |
| test strength and docs | tests that cannot fail, assert nothing, or skip silently; docs, READMEs and `--help` that disagree with the code |
| durability and ack semantics | the ack path for every `Durability`; fsync order; the WAB, `.confirmed` sidecars, the drain ledger, dead letters, quarantine, restart and replay |
| wab and recovery | `crates/weir-server/src/wab/`, `crates/weir-wab/` |
| drain and dead letters | `crates/weir-server/src/drain/`, `worker.rs`, `queue.rs` |
| wire socket and client | `crates/weir-server/src/socket/`, `crates/weir-core/`, `crates/weir-client/`, `demos/*-wire-client` |
| config reload and startup | `crates/weir-server/src/config/`, `reload.rs`, `preflight.rs`, `lifecycle.rs`, `main.rs` |
| sinks | `crates/weir-server/src/sink/`, `crates/weir-sink-s3/`, `crates/weir-sink-sdk/` |
| status metrics and admin | `crates/weir-server/src/{status,metrics,admin}/`, `crates/weir-status/` |
| weir-ctl and attest | `crates/weir-ctl/`, `crates/weir-attest/`, `crates/weir-testkit/`, `crates/weir-rs/` |
| deploy ci and chaos harness | `deploy/`, `.github/`, `scripts/`, `chaos/` (code only: its power-cut campaign is never run), `fuzz/`, `demo/` |

## What a finding is

A finding has an input and the wrong result it produces, and a verifier
reproduces it on the branch. Style, naming, refactors, "could be cleaner" and
performance ideas without a wrong result are not findings.

## How a fix is made

1. A failing test that shows the wrong result.
2. The smallest change that makes it pass.
3. The quick gate green; nothing else red.
4. One line under `CHANGELOG.md` `## [Unreleased]`, in `### Fixed` (or
   `### Changed` for a visible behaviour change), in the file's style.
5. A fix that breaks anything else is reverted and rethought, never patched on top.

## Non-goals: these go to the owner, never into a fix

- Any change to the on-disk WAB format, the `.confirmed` sidecar format, the wire
  protocol (`WIRE_VERSION`), the `--json` output shapes, or a published crate's
  public API (semver; the `docs_drift` guards).
- Loosening any security default (peer UID check, socket permissions, mTLS on TCP).
- New dependencies or dependency upgrades (`cargo-deny` is the owner's call).
- D10, the explicit-missing `--config` WARN: a deferred owner decision
  (`docs/superpowers/specs/2026-10-04-safety-floor-design.md`), not a bug.
- Performance work, new features, refactors.
- Running the chaos power-cut campaign, anything needing root, real cloud
  accounts or real sinks outside the repo's docker test harness.
- Releasing: no version bump, no tag, no publish.

## Security notes

- Client frames, stored segments, sidecars, config files and env values are
  untrusted input: a panic reachable from any of them is critical.
- A secret (sink URL credentials, bearer token, crates.io token) must never reach
  a log, a `Debug` string, argv or `/status`.
- Two daemons must never own one `wab_dir`.

## Deferred questions

None block the run. Owner items found on the way go to the open items in
`log.md` and the PR body.
