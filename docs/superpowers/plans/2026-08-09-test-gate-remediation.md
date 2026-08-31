# Test Gate Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the documented pre-PR gate pass, and remove the flaky test that
`d522ce2` (the W2 shutdown-backoff fix) introduced on `feat/chaos-fault-injection`.

**Architecture:** Two independent fixes. Task 1 is a test-only change in the
drain: a test asserts a segment stays stranded, and W2 deliberately made that
outcome timing-dependent, so the test pins the timing instead of the outcome.
Task 2 changes the documented and CI gate commands so `weir-server`'s bin unit
tests run serially, because `socket::bind_hardened` mutates the **process-global**
umask and that is not fixable cheaply — and corrects two source comments that
currently claim it is harmless.

**Tech Stack:** Rust, cargo test, GitHub Actions.

## Global Constraints

- Branch: `feat/chaos-fault-injection`. Both fixes belong on this branch — Task 1
  is a regression it introduced, Task 2 is a blocker for everything after it.
- **No production behaviour change.** Task 1 touches only `#[cfg(test)]` code.
  Task 2 touches comments, `CONTRIBUTING.md`, `.github/workflows/ci.yml`,
  `docs/security/socket-bind.md`, and `CHANGELOG.md`.
- The verified working gate is exactly these three commands, in this order:
  ```bash
  cargo test --workspace --exclude weir-server
  cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
  cargo test -p weir-server --bins -- --test-threads=1
  ```
  Measured on 2026-08-09 at `7135951`: **246 + 55 + 363 = 664 passed, 0 failed.**
  The `--all-features` variants were measured the same day: **250** for the
  workspace command and **410** for the serial bin command, both `0 failed`.
- Do **not** attempt to make `bind_hardened` umask-free. `socket/mod.rs:373-377`
  documents why `fchmod` cannot work on Linux and why path-based `chmod` is
  symlink-swappable; `docs/security/socket-bind.md` carries the full analysis.
  Changing it is a security redesign, explicitly out of scope here.

---

### Task 1: Make the W2-regressed drain test deterministic

**Files:**
- Modify: `crates/weir-server/src/drain/mod.rs:2668-2705`

**Interfaces:**
- Consumes: `fast_config(PathBuf) -> DrainConfig` (`drain/mod.rs:1645`),
  `DrainConfig { health_poll_interval: Duration, .. }`, `run_drain`,
  `MockSink::with_responses`, `MAX_RETRIES` (`drain/mod.rs:58`, value `3`).
- Produces: nothing consumed by later tasks.

**Background — read before editing.** The test asserts `seg1.exists()`, i.e. that
a segment which exhausted its retries is *still stranded* when the drain exits.
Its own comment states the premise:

> `run_drain` drops the channel, so the drain never reaches the idle poll where
> the sink-recovery rescan runs

`d522ce2` invalidated that. Its CHANGELOG entry says so directly: *"because a
shutdown retry episode can now outlast `health_poll_interval`, a segment that
strands during shutdown may be picked up by the stranded-segment auto-resume and
delivered before the daemon exits."* `fast_config` sets
`health_poll_interval: 50ms` with `base_retry_delay: 1ms`. Under parallel test
load those 1/2/4 ms sleeps overshoot past 50 ms, the idle poll fires, seg1 is
resumed, takes `MockSink`'s default `Ok`, and is confirmed and **deleted**.

Measured: passes 5/5 alone, fails ~2 of 3 full parallel runs on this branch,
passes 3/3 on `main`.

The new behaviour is the improvement W2 intended. The fix is to stop the poll
from firing during this specific test, so the assertion is deterministic again.
The resume path itself stays covered by
`stranded_segment_resumes_when_sink_recovers` (`drain/mod.rs:2389`).

- [ ] **Step 1: Reproduce the flake**

Run the parallel suite excluding the umask-polluting socket tests, three times:

```bash
cd /Users/miki_przygoda/Projects/GitHub-Projects/weir
for i in 1 2 3; do
  cargo test -p weir-server --bin weir-server -- --skip socket:: 2>&1 \
    | grep -E "test result:|multiple_segments_second"
done
```

Expected: at least one run reports
`drain::tests::multiple_segments_second_processed_after_first_exhausts_retries`
and `test result: FAILED. 310 passed; 1 failed`.

If all three runs pass, run it two more times — the flake is load-dependent, not
deterministic. Do not proceed until you have seen it fail at least once.

- [ ] **Step 2: Pin the health poll so the rescan cannot fire**

In `crates/weir-server/src/drain/mod.rs`, inside
`multiple_segments_second_processed_after_first_exhausts_retries`, replace this
line:

```rust
        run_drain(rx, tx, sink, fast_config(dir.clone()), noop_metrics());
```

with:

```rust
        // The stranded-resume rescan runs at the idle health poll. This test
        // asserts seg1 is STILL stranded when the drain exits, so the poll must
        // not fire during the run. fast_config's 50 ms is short enough that
        // under parallel load the clamped shutdown backoff (d522ce2 / W2) lets
        // the poll fire, resume seg1, and delete it — the assertion below then
        // fails ~2 runs in 3. The resume path is covered separately by
        // `stranded_segment_resumes_when_sink_recovers`.
        let config = DrainConfig {
            health_poll_interval: Duration::from_secs(3600),
            ..fast_config(dir.clone())
        };
        run_drain(rx, tx, sink, config, noop_metrics());
```

- [ ] **Step 3: Also correct the stale comment below the call**

Still inside the same test, replace this comment block:

```rust
        // seg2 is delivered without being blocked by seg1 exhausting its retries
        // (a stranded segment doesn't stall the queue). seg1 stays stranded here
        // because run_drain drops the channel, so the drain never reaches the idle
        // poll where the sink-recovery rescan runs (that path is covered by
        // stranded_segment_resumes_when_sink_recovers).
```

with:

```rust
        // seg2 is delivered without being blocked by seg1 exhausting its retries
        // (a stranded segment doesn't stall the queue) — that is what this test
        // is for. seg1 stays stranded because the health poll is pinned above so
        // the sink-recovery rescan cannot run (that path is covered by
        // stranded_segment_resumes_when_sink_recovers).
```

The original comment's stated reason — "run_drain drops the channel, so the drain
never reaches the idle poll" — is no longer true after W2, and leaving it would
send the next reader down the wrong path.

- [ ] **Step 4: Verify the test passes in isolation**

```bash
cargo test -p weir-server --bin weir-server \
  drain::tests::multiple_segments_second_processed_after_first_exhausts_retries
```

Expected: `test result: ok. 1 passed; 0 failed`.

- [ ] **Step 5: Verify the flake is gone under load**

Five consecutive parallel runs, all of which must be clean:

```bash
for i in 1 2 3 4 5; do
  cargo test -p weir-server --bin weir-server -- --skip socket:: 2>&1 \
    | grep -E "test result:"
done
```

Expected: five lines, each `test result: ok. 311 passed; 0 failed; 0 ignored; 0 measured; 52 filtered out`.

If any run fails, the diagnosis is wrong — stop and re-investigate rather than
raising the timeout further.

- [ ] **Step 6: Verify the serial suite still passes**

```bash
cargo test -p weir-server --bins -- --test-threads=1
```

Expected: `test result: ok. 363 passed; 0 failed`.

- [ ] **Step 7: Commit**

```bash
git add crates/weir-server/src/drain/mod.rs
git commit -m "test(drain): pin the health poll so the stranded-segment assertion is deterministic

d522ce2 clamped the shutdown retry backoff to 250 ms instead of zeroing it.
Its own CHANGELOG entry names the side effect: a shutdown retry episode can
now outlast health_poll_interval, so a segment that strands during shutdown
may be picked up by the auto-resume and delivered before the daemon exits.

multiple_segments_second_processed_after_first_exhausts_retries asserts the
opposite — that seg1 is still on disk when the drain exits — and justified it
with 'the drain never reaches the idle poll'. That stopped being true. Under
parallel load the 1/2/4 ms retry sleeps overshoot fast_config's 50 ms poll,
seg1 resumes, takes MockSink's default Ok, and is deleted. Measured: 5/5 pass
alone, ~2 of 3 full parallel runs fail on this branch, 3/3 pass on main.

The behaviour is the improvement W2 intended, so the test now pins the timing
rather than asserting the old outcome: health_poll_interval is raised for this
test alone. The resume path stays covered by
stranded_segment_resumes_when_sink_recovers.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Make the documented pre-PR gate pass

**Files:**
- Modify: `CONTRIBUTING.md:35-62` (the pre-PR gate block)
- Modify: `.github/workflows/ci.yml:61-64`
- Modify: `crates/weir-server/src/socket/mod.rs:405-408` (comment)
- Modify: `crates/weir-server/src/socket/mod.rs:748-752` (comment)
- Modify: `docs/security/socket-bind.md` (append a section)
- Modify: `CHANGELOG.md` (`[Unreleased]`)

**Interfaces:**
- Consumes: nothing from Task 1. The two tasks are independent and may be done
  in either order; this one is listed second only because Task 1 is smaller.
- Produces: the three-command gate quoted in Global Constraints, which every
  subsequent plan's test steps rely on.

**Background — read before editing.** `cargo test` as documented **fails**, and
has done since before 1.3.1. Measured on 2026-08-09:

| Run | Result |
|---|---|
| `main`, `cargo test -p weir-server --bin weir-server` | 297 passed, **61 failed** |
| branch, same | 297 passed, **66 failed** |
| branch, `-- --test-threads=1` | **363 passed, 0 failed** |
| `main`, parallel but `--skip socket::` | 306 passed, 0 failed (3/3) |

`bind_hardened` tightens the process umask to `0o177` around `bind(2)`
(`socket/mod.rs:415-418`) so the socket inode is created at mode `0o600`. The
tightening is RAII-scoped to a single syscall, but umask is **process-global**:
any other thread creating a directory in that window gets `0o700 & !0o177 =
0o600`, which has no execute bit, so nothing can be created inside it. Roughly
60 `wab` / `drain` / `recovery` tests hit this. It also leaves
`crates/weir-server/proptest-regressions/` unwritable, so proptest cannot record
failing seeds — silently degrading the DST safety net.

Two comments in the source assert this is harmless. Both are wrong and both must
be corrected:

- `socket/mod.rs:405-408` — *"Every other file-creation path in weir specifies
  its mode bits explicitly (WAB segments 0o600, dirs 0o700), so the temporary
  tightening is invisible to those paths."* Explicit modes do **not** escape
  umask: `mkdir(2)` and `open(2)` both mask the requested mode. A segment created
  in the window via `WabSegment::create`'s `.mode(0o600)` (`segment.rs:53-62`)
  would land at `0o400`.
- `socket/mod.rs:748-752` — *"in production, bind_hardened is called once during
  single-threaded startup so the issue does not arise."* `main.rs` spawns the WAB
  flushers at line 292 and the workers at line 306; the socket bind happens
  inside the tokio block at line 507. Those threads are already running. In
  practice nothing is created in the window because no producer can connect
  before the socket exists — the process is idle, not single-threaded.

- [ ] **Step 1: Confirm the gate is currently broken**

```bash
cd /Users/miki_przygoda/Projects/GitHub-Projects/weir
cargo test -p weir-server --bin weir-server 2>&1 | grep -E "test result:" | tail -1
```

Expected: `test result: FAILED.` with roughly 60-70 failures. The exact count
varies run to run — it is a race.

- [ ] **Step 2: Confirm the replacement gate passes**

```bash
cargo test --workspace --exclude weir-server 2>&1 | grep -E "test result:" | awk '{p+=$4; f+=$6} END {print "passed="p" failed="f}'
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener 2>&1 | grep -E "test result:"
cargo test -p weir-server --bins -- --test-threads=1 2>&1 | grep -E "test result:"
```

Expected: `passed=246 failed=0`; then five result lines summing to 55 passed /
0 failed / 4 ignored; then `363 passed; 0 failed`.

- [ ] **Step 3: Replace the gate block in `CONTRIBUTING.md`**

Find the fenced block under `## The pre-PR gate` containing `cargo test` and
`cargo test --all-features`, and replace those two lines:

```bash
# Tests: default features, then the full matrix (compiles + runs the
# clickhouse-sink and tls test code the default set never builds).
cargo test
cargo test --all-features
```

with:

```bash
# Tests. weir-server's bin unit tests MUST run serially: socket::bind_hardened
# mutates the process-global umask around bind(2), and a directory created by
# another thread in that window loses its execute bit. See
# docs/security/socket-bind.md. Running them in parallel produces ~60 spurious
# PermissionDenied failures and leaves proptest-regressions/ unwritable.
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1

# The same three, across the full feature matrix (compiles + runs the
# clickhouse-sink and tls test code the default set never builds).
cargo test --workspace --exclude weir-server --all-features
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener --all-features
cargo test -p weir-server --bins --all-features -- --test-threads=1
```

- [ ] **Step 4: Verify the `--all-features` variants pass too**

```bash
cargo test --workspace --exclude weir-server --all-features 2>&1 | grep -E "test result:" | awk '{p+=$4; f+=$6} END {print "passed="p" failed="f}'
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener --all-features 2>&1 | grep -E "test result:"
cargo test -p weir-server --bins --all-features -- --test-threads=1 2>&1 | grep -E "test result:"
```

Expected: `failed=0` on the first, and `0 failed` on every result line of the
other two. If a `--all-features` run fails for an unrelated reason, fix that
before continuing — the gate must be green as written or it will be ignored.

- [ ] **Step 5: Update `.github/workflows/ci.yml`**

Replace these two steps in the `test:` job:

```yaml
      - run: cargo test
      # Build + run the clickhouse-sink and tls test code, which the default
      # feature set never compiles (S08). --all-features pulls in both.
      - run: cargo test --all-features
```

with:

```yaml
      # weir-server's bin unit tests run serially: socket::bind_hardened mutates
      # the process-global umask around bind(2), so a directory created by
      # another thread in that window loses its execute bit (~60 spurious
      # PermissionDenied failures). See docs/security/socket-bind.md.
      - run: cargo test --workspace --exclude weir-server
      - run: cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
      - run: cargo test -p weir-server --bins -- --test-threads=1
      # Build + run the clickhouse-sink and tls test code, which the default
      # feature set never compiles (S08). --all-features pulls in both.
      - run: cargo test --workspace --exclude weir-server --all-features
      - run: cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener --all-features
      - run: cargo test -p weir-server --bins --all-features -- --test-threads=1
```

The `test:` job has `timeout-minutes: 15`. Serial execution of the bin target
takes ~13 s locally versus ~1.5 s parallel, so the added cost is negligible; the
load tests in debug dominate at ~46 s and are unchanged.

- [ ] **Step 6: Correct the umask comment at `socket/mod.rs:405-408`**

Replace:

```rust
    // umask is process-global and applies to other threads briefly. Every
    // other file-creation path in weir specifies its mode bits explicitly
    // (WAB segments 0o600, dirs 0o700), so the temporary tightening is
    // invisible to those paths. A tighter umask is also a safer default.
```

with:

```rust
    // umask is process-global and applies to other threads for the duration of
    // the bind(2) below. Specifying mode bits explicitly does NOT escape it:
    // mkdir(2) and open(2) both mask the requested mode, so a directory created
    // by another thread in this window lands at 0o700 & !0o177 = 0o600 — no
    // execute bit, nothing can be created inside it — and a segment file at
    // 0o600 & !0o177 = 0o400. In the daemon nothing is created here because no
    // producer can connect before the socket exists, so the process is idle
    // (not single-threaded: the WAB flushers spawned at main.rs:292 and the
    // workers at main.rs:306 are already running). Under `cargo test` the
    // window is real and hit constantly, which is why weir-server's bin unit
    // tests run with --test-threads=1 — see CONTRIBUTING.md and
    // docs/security/socket-bind.md.
```

- [ ] **Step 7: Correct the test-module comment at `socket/mod.rs:748-752`**

Replace:

```rust
    // bind_hardened internally mutates the process umask. That makes
    // concurrent calls (e.g. parallel test execution) interleave their
    // save/restore and leak a tightened umask globally. The lock below
    // serialises the tests; in production, bind_hardened is called once
    // during single-threaded startup so the issue does not arise.
```

with:

```rust
    // bind_hardened internally mutates the process umask. The lock below
    // serialises these tests against each other, but it CANNOT protect the
    // ~300 other tests in this binary: any thread creating a directory while a
    // bind_hardened call holds umask 0o177 gets mode 0o600 and fails with
    // PermissionDenied. That is why the documented gate runs this target with
    // --test-threads=1 (CONTRIBUTING.md). In the daemon the window is harmless
    // because no producer can connect before the socket exists — but "called
    // once during single-threaded startup" is not the reason: the WAB flushers
    // (main.rs:292) and workers (main.rs:306) are already running by then.
```

- [ ] **Step 8: Record the limitation in `docs/security/socket-bind.md`**

Append this section to the end of the file:

```markdown
## Known limitation — the umask window is process-global

The bind sequence tightens the process umask to `0o177` around `bind(2)` so the
socket inode is created at mode `0o600` without a post-bind `chmod`. The
tightening is RAII-scoped to that single syscall, but umask is a **process**
attribute, not a thread attribute, so it applies to every thread for the
duration.

Specifying mode bits explicitly does not escape it — `mkdir(2)` and `open(2)`
both mask the requested mode. A directory created by another thread inside the
window lands at `0o700 & !0o177 = 0o600`, losing its execute bit; a WAB segment
lands at `0o600 & !0o177 = 0o400`.

**In the daemon this is not exploitable and not reachable.** The bind happens
before any producer can connect, so no records exist and no segment is being
created. The WAB flushers and workers are running by then, but idle.

**Under `cargo test` it is reachable and constant**, because ~300 unrelated tests
create directories concurrently with ~50 tests that call `bind_hardened`. That is
why `weir-server`'s bin unit tests are documented to run with
`--test-threads=1`.

Removing the umask dependency is a security redesign, not a cleanup: `fchmod`
operates on the sockfs object rather than the bound inode on Linux, and a
path-based `chmod` reintroduces the symlink-swap window analysed above. The
alternative worth evaluating if this is ever revisited is binding to a private
name inside the already-`0o700` parent directory and publishing it with
`renameat`, which would need its own analysis against the attack surface
described in this document.
```

- [ ] **Step 9: Add a CHANGELOG entry**

In `CHANGELOG.md`, inside the existing `## [Unreleased]` section, add this
subsection after the `### Reliability` block:

```markdown
### Testing

- **The documented pre-PR gate now passes.** `cargo test` had been failing on
  `main` with ~60 spurious `PermissionDenied` errors: `socket::bind_hardened`
  tightens the **process-global** umask to `0o177` around `bind(2)`, and any
  other thread creating a directory in that window gets mode `0o600` — no
  execute bit, so nothing can be created inside it. Explicit mode bits do not
  help; `mkdir(2)` and `open(2)` both mask the requested mode. It also left
  `proptest-regressions/` unwritable, so proptest could not record failing
  seeds. `CONTRIBUTING.md` and CI now run `weir-server`'s bin unit tests with
  `--test-threads=1` (~13 s versus ~1.5 s; the debug load tests dominate the job
  either way). Two source comments claiming the window was harmless — one
  asserting explicit modes escape umask, one asserting startup is
  single-threaded — are corrected, and `docs/security/socket-bind.md` records
  the limitation and what a real fix would have to analyse. No daemon behaviour
  change: the window is unreachable in production because no producer can
  connect before the socket exists.
```

- [ ] **Step 10: Run the full new gate end to end**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo test --workspace --exclude weir-server
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener
cargo test -p weir-server --bins -- --test-threads=1
cargo test --workspace --exclude weir-server --all-features
cargo test -p weir-server --lib --test system --test load --test load_tls --test tls_client --test tls_listener --all-features
cargo test -p weir-server --bins --all-features -- --test-threads=1
```

Expected: every command exits 0, with `0 failed` on every `test result:` line.

- [ ] **Step 11: Commit**

```bash
git add CONTRIBUTING.md .github/workflows/ci.yml crates/weir-server/src/socket/mod.rs docs/security/socket-bind.md CHANGELOG.md
git commit -m "test: run weir-server's bin tests serially so the documented gate passes

cargo test has been failing on main with ~60 spurious PermissionDenied
errors. socket::bind_hardened tightens the process-global umask to 0o177
around bind(2) so the socket inode is created at 0o600 without a post-bind
chmod. The tightening is RAII-scoped to one syscall, but umask is a process
attribute: any other thread creating a directory in that window gets
0o700 & !0o177 = 0o600, which has no execute bit, so nothing can be created
inside it. It also left proptest-regressions/ unwritable, so proptest could
not record failing seeds.

Two source comments claimed this was harmless and both were wrong. Explicit
mode bits do not escape umask — mkdir(2) and open(2) mask the requested mode,
so WabSegment::create's .mode(0o600) would land at 0o400. And startup is not
single-threaded: the WAB flushers (main.rs:292) and workers (main.rs:306) are
running well before the bind at main.rs:507. The window is unreachable in the
daemon for a different reason — no producer can connect before the socket
exists — and that is now what the comments say.

The gate and CI now run weir-server's bin unit tests with --test-threads=1
(~13 s versus ~1.5 s; the debug load tests dominate the job either way).
docs/security/socket-bind.md records the limitation and what a real fix would
have to analyse against the existing symlink-swap surface.

Measured before: 297 passed, 61 failed on main. After: 246 + 55 + 363 = 664
passed, 0 failed.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Self-review notes

**Coverage.** Task 1 closes the W2 regression; Task 2 closes the broken gate and
the two false comments. Both findings from the 2026-08-09 investigation are
addressed.

**Not addressed, deliberately.** The umask race itself is not fixed — only
worked around and documented. `docs/security/socket-bind.md` states what a real
fix (bind-private-then-`renameat`) would have to analyse. That is a security
redesign and belongs in its own spec.

**Ordering.** The two tasks are independent. Task 1 first is recommended only
because it is smaller; either order works, and Step 2 of Task 2 will show the
flake as noise if Task 1 has not landed yet — run Task 2's Step 2 commands more
than once if so.
