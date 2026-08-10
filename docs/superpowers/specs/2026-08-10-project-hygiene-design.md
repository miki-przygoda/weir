# Project hygiene — parallel-safe tests, a real `create_dir_private`, and a config-doc guard

**Status:** Design, approved. No implementation yet.
**Date:** 2026-08-10
**Branch:** `v2/main-line`.

---

## 1. Why this exists

Three things, in descending order of how much they actually matter:

1. **`create_dir_private` does not deliver the guarantee its name makes.** It
   *requests* mode `0o700` from `mkdir(2)`, which masks the request by the
   process umask. A daemon started under a tighter umask gets WAB directories
   with the wrong modes. This is a latent production bug, not a test concern.
2. **The test suite cannot run in parallel**, so the documented gate carries
   `--test-threads=1` in three places. This is W3, found by the chaos harness
   and previously recorded as "deliberately not fixed — the fix is a design
   decision". A prototype (§2) shows most of it is not a design decision at all.
3. **Nothing enforces that a new config knob gets documented.** Today the
   configuration reference is perfect — 45 settable keys, 45 documented headings,
   zero drift either way — but by diligence rather than by construction.

Explicitly **out of scope**: replacing `bind_hardened`'s process-global umask
with a bind-into-a-private-directory-then-`renameat` sequence. That is a genuine
security redesign against the threat model in
`docs/security/socket-bind.md`, which says it "would need its own analysis". It
gets its own spec or it does not happen.

---

## 2. The prototype that scoped this

Rather than reason about how much of W3 is production versus test hygiene, the
fix was prototyped and measured.

| Configuration | Failures per parallel run |
|---|---|
| Baseline (`cargo test -p weir-server --bins`) | **73** |
| Enforcing `0o700` after test directory creation | **0–2** (2 of 5 runs fully green) |
| Serial (`-- --test-threads=1`) | 0 (377 passed) |

**W3 is therefore ~97% test-helper hygiene.** The prototype was reverted; only
its evidence is kept.

Two residual failures survived the prototype, and their causes are *not* the
same:

- `wab_bytes_tests::compute_wab_bytes_skips_dead_letter_and_quarantine` failed
  every time. Cause identified: the test does
  `create_dir_all(root/shard_00)`, so **`root` is created as an intermediate
  component** and gets the masked mode. The prototype enforced the mode on the
  leaf only. This is a flaw in the prototype, not a separate problem.
- `socket::tests::socket_bind_sets_mode_0600` and
  `socket::tests::run_exits_cleanly_after_shutdown_signal` failed once each
  across five runs. Cause **not** established.

That distinction drives §4.

---

## 3. Fix — `create_dir_private` guarantees its mode

`crates/weir-server/src/wab/mod.rs`:

```rust
pub(crate) fn create_dir_private(path: PathBuf) -> io::Result<()>
```

Today it is a single `DirBuilder::new().recursive(true).mode(0o700).create()`.
It becomes: create recursively as now, then **enforce `0o700` on every component
this call actually created**, walking from the shallowest created ancestor down
to `path`.

Two properties are load-bearing:

- **Enforce every created level, not just the leaf.** `recursive(true)` may
  create several directories; each is masked independently. Fixing only the leaf
  is what left the prototype's residual failure.
- **Never re-permission a directory that already existed.** The operator's
  `wab_dir` may legitimately be `0o750` with a shared group, and silently
  tightening it would be a surprising side effect of starting the daemon.
  Determine which components existed *before* creating, and enforce only on the
  rest.

On non-Unix the function keeps falling back to `create_dir_all` with no mode
handling, exactly as today.

---

## 4. Fix — test helpers use the same guarantee

Six `tmp_dir` helpers (`config/mod.rs`, `wab/{recovery,mod,segment}.rs`,
`drain/{dead_letter,mod}.rs`) and roughly forty scattered
`fs::create_dir_all(...).unwrap()` calls inside `#[cfg(test)]` modules currently
bypass `create_dir_private`. They route through **`create_dir_private` itself**,
not a parallel test-only helper, so a directory created while `bind_hardened`
holds its tightened umask still ends up traversable.

Reusing the production function rather than mirroring it is deliberate: a
separate test helper could drift from the real one, and then the suite would be
exercising a guarantee production does not make. The function is already
`pub(crate)`, so no visibility change is needed.

`main.rs`'s test module is included; the prototype missed it and that showed up
immediately.

### The two socket tests are deliberately not designed for here

Their cause is unknown, and the `compute_wab_bytes` failure demonstrates that a
residual failure can share a cause with the main fix rather than being separate.
Designing process isolation now risks building machinery for a problem that
dissolves once intermediate components are enforced.

**The plan must therefore: land §3 and §4, re-measure over ten runs, and only
then design a fix for whatever genuinely remains — with evidence.** If they do
survive, the likely candidates in order of preference are (a) they need
`umask_test_lock` and do not take it, (b) they need to be moved to an
integration-test target for process isolation, which costs widening the
deliberately narrow `src/lib.rs` facade.

---

## 5. Fix — config-doc drift guard

A test in `config/file.rs`'s test module, beside the lists it guards, asserting
**bidirectional** agreement between the canonical key lists and
`docs/operations/configuration.md`:

- every key in `BASE_SERVER_KEYS` (36) and `FEATURE_GATED_SERVER_KEYS` (9) has a
  `#### \`key\`` heading;
- every such heading corresponds to a real key.

The reverse direction matters as much as the forward one: a heading for a
removed knob tells an operator to set something the daemon will now reject as
unknown, which is worse than an undocumented knob.

The doc is located via `env!("CARGO_MANIFEST_DIR")`, the same pattern
`replay_pinned_regression_seeds` uses for `tests/dst_seeds`. No new test target.

**Feature-independent by construction:** `FEATURE_GATED_SERVER_KEYS` is a static
`&[(&str, &str)]` of (key, feature-name) pairs, fully populated regardless of
compiled features, so the test behaves identically under all three feature
configurations in the gate.

The failure message names the specific keys and the direction they are missing,
so it reads as an instruction. That is what made
`metrics_all_families_registered` useful when it caught two undocumented metrics
during the 2.0 work.

**This locks in a property that already holds** — 45/45, zero drift measured on
2026-08-10. It is insurance, not a repair.

---

## 6. Push `v2/main-line`

77 commits currently exist on one machine only. `git push -u origin
v2/main-line`, no PR.

Non-destructive: it creates a new remote branch and touches nothing existing.
Note it fires **no CI** — `.github/workflows/ci.yml` triggers on `push` to
`main` and on `pull_request`, so a branch push runs nothing. CI feedback arrives
only when a PR opens.

---

## 7. Success criteria

1. `cargo test -p weir-server --bins` passes **ten consecutive parallel runs**.
2. The three-command test gate in `CONTRIBUTING.md` and
   `.github/workflows/ci.yml` collapses back to plain `cargo test` /
   `cargo test --all-features`, and the comments explaining why it was split are
   removed.
3. `docs/security/socket-bind.md`'s "Known limitation" section is updated: the
   window still exists in production (unreachable, because no producer can
   connect before the socket is bound) but no longer forces serial tests. The
   section must **not** be deleted — the umask is still process-global, and the
   bind redesign is still the real fix.
4. The config-doc guard fails when a key is added to `BASE_SERVER_KEYS` without
   a doc heading. Demonstrate this by adding one temporarily and observing the
   failure, rather than trusting that it would.
5. Full gate green: fmt, clippy on all three feature configurations, the test
   suites, the 300-seed DST sweep, and `cargo deny`.

---

## 8. Risks

| Risk | Mitigation |
|---|---|
| Enforcing modes on pre-existing directories surprises an operator whose `wab_dir` is intentionally group-readable | §3 enforces only on components the call created; a test pins that a pre-existing `0o750` directory is left untouched |
| The two socket tests survive and the suite is still not parallel-safe | §4 makes re-measurement a required step; the gate stays serial until ten clean runs are demonstrated, rather than being collapsed optimistically |
| The doc guard's `CARGO_MANIFEST_DIR` path breaks if the crate moves | It fails loudly with the attempted path, like the DST seed loader; a silently-skipping guard would be worse than none |
| Collapsing the gate hides a genuine future regression behind parallel flakiness | Criterion 1 is ten consecutive runs, not one |
