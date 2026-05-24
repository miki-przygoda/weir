# Test Roadmap — Systems Hardening Suite

This document tracks the next 10 tests to add to the integration and load
suites. Each one targets a gap that real production systems hit — not
theoretical edge cases. They are ordered roughly by "most likely to surface
a real bug today", with deployment-safety tests next and correctness
guarantees last.

Implementation rule: one test per commit, all committed to
`feat/systems-hardening`. Push the branch once all 10 are green and
lint-clean, then merge.

---

## Tests

### 1. Graceful shutdown under load
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Start a server. Spawn N threads all pushing Sync records in a tight loop.
After 2 s, send SIGTERM. Join all threads, categorising each result as
`Ok`, `Nack`, or `Io(UnexpectedEof)`. Assert zero silent drops: every
record that got an `Ok` back must be on disk; every record that didn't
get a response must have produced a clean `Io` error the client can
retry — not a silent half-write.

**Why it matters:**
Every rolling deploy exercises this path. If the server drops a Sync push
without responding, the producer has no way to know the record is gone.
This is the most dangerous failure mode for an append log: silent data
loss at shutdown.

**Key assertions:**
- Server exits within `shutdown_timeout_secs + 2 s` of receiving SIGTERM.
- No thread panics (all errors are `ClientError::Io`, never `unwrap` failures).
- WAB byte count on disk is consistent with the number of `Ok` results.

---

### 2. Slow / stalled client
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Open a raw `UnixStream`, write a complete Push frame, then hold the
connection open without reading the Ack for 30 s. Concurrently, a second
normal `WeirClient` pushes 50 records and asserts they all succeed within
5 s.

**Why it matters:**
OOMkilled processes, suspended VMs, and slow consumers all produce stalled
sockets. If the server's write path blocks on sending an Ack to a stalled
client, it will eventually stall the worker thread for that connection —
which is acceptable — but it must never stall *other* connections on
*other* workers. This test verifies connection isolation.

**Key assertions:**
- 50 concurrent pushes on a separate client all succeed within 5 s.
- Server process is still alive after the 30 s stall window.
- Server health-check passes after the stalled client disconnects.

---

### 3. Partial frame injection
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Open a raw `UnixStream`. Write a valid Push header, then write only the
first half of the declared payload, then abruptly close the connection.
Immediately after, connect a normal `WeirClient` and push a record
successfully.

**Why it matters:**
The server's frame-parsing loop maintains per-connection read state. A
connection that dies mid-frame must not leave that state machine in a
corrupt position. If it does, the server will mis-parse the *next*
connection's frames — causing either a nack storm or a silent corruption.
This is different from the proptest fuzz, which tests the decode
functions in isolation; here we test the live read loop.

**Key assertions:**
- No panic in the server process.
- The follow-up push on a fresh connection succeeds with `Ok`.
- Server health-check passes.

---

### 4. Disk full
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Create a tmpfs of a fixed small size (e.g. 1 MB) as the WAB directory.
Write records until the filesystem reports full. Assert the server sends
`Nack(InternalError)` for writes that cannot be flushed, rather than
panicking, hanging, or silently dropping the record.

**Why it matters:**
Disk-full is the most common late-night incident for append logs in
production. If the server crashes or hangs instead of nacking, the
producer has no signal to back off and the ops team wakes up to a dead
process with no error message. A graceful nack lets the producer retry
later or route to a different node.

**Implementation note:**
On Linux, use `unshare --mount` + `mount -t tmpfs -o size=1M` inside the
test process, or pre-fill the WAB dir with a large sparse file to consume
the quota. On macOS, use a RAM disk (`hdiutil attach -nomount ram://...`).
Gate with `#[cfg(target_os = "linux")]` if the tmpfs approach is
Linux-only.

**Key assertions:**
- At least one push returns `ClientError::Nack(NackReason::InternalError)`.
- Server process is still alive after the nack.
- After clearing space (deleting the filler file), subsequent pushes succeed.

---

### 5. WAB data integrity after crash
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Push N Sync records with known, unique payloads (e.g. 4-byte big-endian
sequence numbers). Kill the server with SIGKILL mid-batch. Restart it.
Read the raw WAB segment files and verify:
1. Every payload that received an `Ok` before the kill is present on disk.
2. No payload appears more than once (no duplicate writes).
3. No payload appears that was never sent (no corruption).

**Why it matters:**
The existing crash-restart test only checks that the server starts up and
accepts new records. It does not verify the *content* of what survived.
"Sync durability" is the core promise of the system: if you got an `Ok`,
the bytes are on disk. This test proves that promise at the byte level.

**Key assertions:**
- Set of payloads on disk == set of payloads that got `Ok` before kill.
- No duplicates. No phantoms.

---

### 6. Two instances binding the same socket
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Start a server normally. Attempt to start a second server instance
pointing at the exact same socket path. Assert the second instance exits
with a non-zero status code within a few seconds and does not clobber the
first instance's socket.

**Why it matters:**
Botched rollouts, double-starts from a broken init system, or a race
between a stopping and starting instance can all produce this situation.
If the second instance silently wins the socket, the first is now
unreachable — but callers think they're talking to a healthy server. The
second instance must fail loudly so the operator knows something is wrong.

**Key assertions:**
- Second process exits with a non-zero status within 5 s.
- First server is still accepting connections after the second fails.
- First server health-check passes.

---

### 7. File descriptor limit exhaustion
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Using `setrlimit(RLIMIT_NOFILE, ...)` (Linux/macOS), lower the fd ceiling
for the *test process* to a small value (e.g. 64), then attempt to open
more connections than the limit allows. Assert the server sends a clean
refusal (connection dropped at accept, not a server crash) and that
connections within the limit continue to work.

**Implementation note:**
This test modifies a process-wide resource limit; it must be run in
isolation or the limit must be restored after the test. Gate with
`#[cfg(unix)]`.

**Why it matters:**
Fd leaks are endemic. A server that crashes with `Too many open files`
instead of degrading gracefully takes everything down with it — including
the monitoring agent that would have paged the on-call engineer.

**Key assertions:**
- Server process does not crash.
- At least one connection within the original limit succeeds.
- After restoring the fd limit, new connections succeed.

---

### 8. Per-shard record ordering
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
With a single-shard server, push N records with unique sequential
payloads from a single producer. After all are acked, read the WAB segment
file(s) raw and assert that the payloads appear on disk in exactly the
order they were submitted.

**Why it matters:**
Consumers of an append log rely on ordering being preserved — it is the
fundamental contract. This is not tested anywhere today. The test is also a
useful regression guard: any change to the batching or queue path that
accidentally reorders records will be caught immediately.

**Key assertions:**
- Payloads on disk appear in submission order.
- No gaps (every submitted payload is present).

---

### 9. Batch deadline timer accuracy
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
Set `batch_deadline_ms = 20`. Push a single Buffered record from a slow
producer (one record, wait, measure round-trip). Assert the push completes
within `3 × batch_deadline_ms` (60 ms). Repeat 20 times, collecting
round-trip samples. Assert p99 < 100 ms.

**Why it matters:**
The batch deadline timer is what gives Buffered writes a bounded latency
guarantee even at low throughput. If Tokio's timer is being starved — for
example because the accept loop is spinning — the flush will be
arbitrarily delayed. A producer relying on `Buffered` for low latency
will see random multi-second spikes with no explanation.

**Key assertions:**
- Every sample completes within `3 × batch_deadline_ms`.
- p99 of all samples < `5 × batch_deadline_ms`.

---

### 10. Metrics monotonicity under repeated crash-restart
- [ ] Implemented
- [ ] Green on CI

**File:** `crates/weir-server/tests/system.rs`

**What it does:**
In a loop: push some records, SIGKILL the server, restart it, scrape the
Prometheus `/metrics` endpoint, assert `records_accepted` is ≥ the value
from the previous scrape. Repeat 5 times.

**Why it matters:**
Metrics that reset to zero on restart silently corrupt dashboards and
alert thresholds. An on-call engineer watching a counter that dips to zero
on every deploy will either start ignoring it (alert fatigue) or
misdiagnose a real incident. Monotonicity is a correctness property for
operational counters, not just a nice-to-have.

**Implementation note:**
Counters will legitimately reset to 0 after a restart since they are
in-process atomics — the test should assert that the *cumulative* total
across restarts (tracked by the test itself) is consistent, and flag
any restart that produces a counter *higher* than what was pushed in
that session (which would indicate phantom counts).

**Key assertions:**
- `records_accepted` after restart ≥ 0 (no negative values).
- `records_accepted` in a fresh session never exceeds the number of
  pushes made in that session.
- `records_nack` is always ≤ `records_accepted`.

---

## Progress

| # | Test | Status |
|---|------|--------|
| 1 | Graceful shutdown under load | pending |
| 2 | Slow / stalled client | pending |
| 3 | Partial frame injection | pending |
| 4 | Disk full | pending |
| 5 | WAB data integrity after crash | pending |
| 6 | Two instances — same socket | pending |
| 7 | fd limit exhaustion | pending |
| 8 | Per-shard record ordering | pending |
| 9 | Batch deadline timer accuracy | pending |
| 10 | Metrics monotonicity under crash-restart | pending |
