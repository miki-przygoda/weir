# Chaos Harness — Phase 1 (Spine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the spine of the chaos harness — load generator, recording sink, device-mapper plumbing, episode loop, quiescence detection, and the three-invariant verifier — proven by a 30-minute random-kill run that produces zero *false* violations.

**Architecture:** A standalone `chaos/` cargo project (own workspace, `publish = false`, path deps on `weir-client`), mirroring `fuzz/`. Two unprivileged Rust binaries (`loadgen`, `recorder`) observe from outside the fault zone; a root Python orchestrator owns the device-mapper stack and drives episodes; a Python verifier checks invariants against the two logs.

**Tech Stack:** Rust (edition 2024, MSRV 1.88), Python 3 (stdlib only — matches `deploy/avg_benchmarks.py` precedent), Linux device-mapper (`losetup`, `dmsetup`), ext4.

## Global Constraints

- **Platform:** Linux x86_64 only. Every task's code may assume Linux; no macOS or Windows compatibility shims.
- **Rust edition:** `2024`. **MSRV:** `1.88` (matches the weir workspace).
- **`chaos/` is NOT a weir workspace member.** It declares its own `[workspace] members = ["."]` so `cargo build` from the repo root never touches it.
- **`publish = false`** on the chaos package.
- **Python: stdlib only.** No pip installs. Matches existing repo tooling.
- **Record payloads MUST be newline-free ASCII.** The HTTP sink's NDJSON mode dead-letters records containing `0x0A` (`crates/weir-server/src/sink/http.rs:513-525`), which would manufacture false I1 violations.
- **Observers write outside the fault zone.** The recorder log and ledger checkpoints go to a host-filesystem path, never the device-mapper mount.
- **Values and semantics from a task's code blocks are binding; whitespace is not.** Transcribe the exact strings, numbers, signatures and test cases, then run `cargo fmt` (Rust) before committing. The blocks in this plan are hand-written and not all rustfmt-normalised, so a verbatim copy will fail `cargo fmt --check`.
- **Every task leaves `chaos/` building and its tests passing.** No task may forward-declare a target, module, or import whose file a later task creates.
- **Phase 1 does NOT implement:** the eBPF probe, `dm-flakey`, `dm-delay`, ENOSPC, read-only remount, dead-letter exhaustion, tier-aware invariants, or plots. Those are Phases 2–4. Phase 1 builds the dm stack *plumbing* (create/teardown) but injects no dm faults.
- **Branch:** `feat/chaos-fault-injection` (already created, spec already committed).

---

## File Structure

| File | Responsibility |
|------|----------------|
| `chaos/Cargo.toml` | Standalone workspace, two bins, path dep on `weir-client` |
| `chaos/README.md` | What it is, Linux/root requirements, how to run |
| `chaos/src/lib.rs` | Shared: record encode/decode, ledger types + serialisation |
| `chaos/src/bin/loadgen.rs` | Producer pool, ledger, latency stream |
| `chaos/src/bin/recorder.rs` | HTTP recording sink, durable append |
| `chaos/orchestrator/dm_stack.py` | Loopback + ext4 create/teardown |
| `chaos/orchestrator/quiescence.py` | `/metrics` scrape + three-signal drain-quiescence check |
| `chaos/orchestrator/run.py` | Episode loop, process lifecycle, random killer |
| `chaos/orchestrator/verify.py` | I1/I2/I3 invariants, duplicate rate |
| `chaos/orchestrator/report.py` | Episode log → markdown |
| `chaos/schedules/smoke.toml` | 30-minute Phase 1 schedule |

Rust and Python split along the privilege boundary, not along a technical layer: everything needing root is Python in `orchestrator/`, everything observing is unprivileged Rust.

---

## Task 1: Project scaffolding + record encoding

**Files:**
- Create: `chaos/Cargo.toml`
- Create: `chaos/src/lib.rs`
- Create: `chaos/README.md`
- Create: `chaos/.gitignore`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `chaos::Record { run_id: u64, seq: u64 }`
  - `chaos::encode_record(run_id: u64, seq: u64, size: usize) -> String`
  - `chaos::decode_record(line: &str) -> Option<Record>`

- [ ] **Step 1: Write the failing test**

Create `chaos/src/lib.rs`:

```rust
//! Shared types for the chaos harness: record identity and the outcome ledger.
//!
//! Linux-only, unpublished. See `docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md`.

/// A record's identity, carried in its payload and recovered by the recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    /// Derived from the schedule seed; distinguishes runs.
    pub run_id: u64,
    /// Monotonic per-run sequence number.
    pub seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_never_contains_a_newline() {
        let line = encode_record(7, 42, 128);
        assert_eq!(line.len(), 128, "encoded record must hit the requested size");
        assert!(
            !line.as_bytes().contains(&b'\n'),
            "payload must be newline-free: the NDJSON sink dead-letters records \
             containing 0x0A, which would look like a durability violation"
        );
        assert_eq!(decode_record(&line), Some(Record { run_id: 7, seq: 42 }));
    }

    #[test]
    fn rejects_a_size_too_small_to_hold_the_identity() {
        // 8 bytes cannot hold `{"run":7,"seq":42,"pad":""}`.
        let line = encode_record(7, 42, 8);
        assert!(line.len() > 8, "encoder must not truncate identity to hit size");
        assert_eq!(decode_record(&line), Some(Record { run_id: 7, seq: 42 }));
    }

    #[test]
    fn decode_rejects_junk() {
        assert_eq!(decode_record("not json"), None);
        assert_eq!(decode_record(""), None);
        assert_eq!(decode_record("{\"run\":1}"), None, "missing seq");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos && cargo test round_trips_and_never_contains_a_newline`
Expected: FAIL — `cannot find function encode_record in this scope`.

(If `chaos/Cargo.toml` does not exist yet, the failure is `could not find Cargo.toml` — create it in Step 3 and re-run.)

- [ ] **Step 3: Write minimal implementation**

Create `chaos/Cargo.toml`:

```toml
[package]
name = "weir-chaos"
version = "0.0.0"
publish = false
edition = "2024"
rust-version = "1.88"

# Standalone workspace so the chaos harness — which is Linux-only, needs root,
# and depends on device-mapper tooling — never enters the weir workspace build,
# the MSRV matrix, or the publish flow. Mirrors `fuzz/` for the same reason.
# Run everything from inside this directory.
[workspace]
members = ["."]

[dependencies]
# Path dep, deliberately: the harness must test the working tree. A git or
# crates.io dep would let a run report "durability verified" against a stale
# weir, which is worse than publishing no report at all.
weir-client = { path = "../crates/weir-client" }
weir-core = { path = "../crates/weir-core" }
```

> **No `[[bin]]` sections yet — deliberately.** Cargo resolves bin targets at
> load time, so declaring `loadgen`/`recorder` before their files exist makes
> every `cargo build` and bare `cargo test` in `chaos/` fail with "can't find
> bin". Tasks 3 and 4 each add their own `[[bin]]` block alongside the file it
> points at, so the project builds and its tests pass after **every** task
> rather than only after Task 4.

Append to `chaos/src/lib.rs` (above the `#[cfg(test)]` module):

```rust
/// Encodes a record's identity as newline-free ASCII JSON, padded to `size`.
///
/// The encoding is load-bearing. The HTTP sink's NDJSON batch mode
/// dead-letters any record containing an embedded `0x0A`
/// (`crates/weir-server/src/sink/http.rs:513-525`) rather than failing the
/// batch — so a binary payload that happened to contain a newline would be
/// acked, silently diverted, and never delivered. The verifier would then
/// report a durability violation that is really a harness bug.
///
/// If `size` is too small to hold the identity, the identity wins and the
/// returned string is longer than `size`. Truncating would corrupt the oracle.
#[must_use]
pub fn encode_record(run_id: u64, seq: u64, size: usize) -> String {
    let head = format!("{{\"run\":{run_id},\"seq\":{seq},\"pad\":\"");
    let tail = "\"}";
    let overhead = head.len() + tail.len();
    let pad_len = size.saturating_sub(overhead);
    let mut s = String::with_capacity(overhead + pad_len);
    s.push_str(&head);
    s.extend(std::iter::repeat_n('a', pad_len));
    s.push_str(tail);
    s
}

/// Recovers a record's identity from an encoded payload line.
///
/// Hand-rolled rather than pulling in `serde_json`: the format is fixed and
/// produced by `encode_record` in this same crate, so a dependency would buy
/// nothing. Returns `None` for anything that is not a well-formed record.
#[must_use]
pub fn decode_record(line: &str) -> Option<Record> {
    let run_id = extract_u64(line, "\"run\":")?;
    let seq = extract_u64(line, "\"seq\":")?;
    Some(Record { run_id, seq })
}

fn extract_u64(haystack: &str, key: &str) -> Option<u64> {
    let start = haystack.find(key)? + key.len();
    let rest = &haystack[start..];
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    rest[..end].parse().ok()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos && cargo test`
Expected: PASS — 3 tests.

- [ ] **Step 5: Add README and gitignore**

Create `chaos/README.md`:

```markdown
# chaos — real-kernel fault injection for weir

A standalone cargo project (not a weir workspace member) that runs weir against
a real device-mapper stack and verifies its durability claims from outside the
daemon, across crash and restart.

Design: [`docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md`](../docs/superpowers/specs/2026-07-25-chaos-fault-injection-design.md)

## Requirements

- **Linux.** Device-mapper and loopback devices. macOS is not supported and
  never will be: `F_BARRIERFSYNC` gives no power-loss guarantee, so a
  durability proof there would prove nothing.
- **root**, for the orchestrator only. The load generator and recorder run
  unprivileged by design — they are the observers and must not be able to
  corrupt what they measure.
- **Python 3** (stdlib only) and `losetup` / `dmsetup` / `mkfs.ext4`.

## Running

```bash
cd chaos
cargo build --release
sudo python3 orchestrator/run.py schedules/smoke.toml
```

Everything runs from inside this directory. `cargo build` at the weir repo root
does not build this project.
```

Create `chaos/.gitignore`:

```
target/
runs/
__pycache__/
```

- [ ] **Step 6: Verify the weir workspace is unaffected**

Run: `cd .. && cargo metadata --no-deps --format-version 1 | tr ',' '\n' | grep -c '"name":"weir-chaos"'`
Expected: `0` — the chaos project must not appear in the weir workspace.

- [ ] **Step 7: Commit**

```bash
git add chaos/
git commit -m "feat(chaos): scaffold standalone project and record encoding"
```

---

## Task 2: The outcome ledger

**Files:**
- Modify: `chaos/src/lib.rs`

**Interfaces:**
- Consumes: `Record` from Task 1.
- Produces:
  - `chaos::Outcome` — enum `Acked` / `Nacked(String)` / `Unknown`
  - `chaos::LedgerEntry { seq: u64, tier: char, outcome: Outcome, t_micros: u64, rtt_micros: u64 }`
  - `chaos::LedgerEntry::to_line(&self) -> String`
  - `chaos::LedgerEntry::from_line(&str) -> Option<LedgerEntry>`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `chaos/src/lib.rs`:

```rust
    #[test]
    fn ledger_entry_round_trips_every_outcome() {
        let cases = vec![
            Outcome::Acked,
            Outcome::Nacked("PayloadTooLarge".to_string()),
            Outcome::Unknown,
        ];
        for outcome in cases {
            let entry = LedgerEntry {
                seq: 99,
                tier: 'S',
                outcome: outcome.clone(),
                t_micros: 1_700_000_000_000_000,
                rtt_micros: 364,
            };
            let line = entry.to_line();
            assert!(!line.contains('\n'), "ledger lines must be single-line");
            let parsed = LedgerEntry::from_line(&line).expect("must parse back");
            assert_eq!(parsed, entry);
        }
    }

    #[test]
    fn ledger_nack_reason_with_spaces_survives_round_trip() {
        let entry = LedgerEntry {
            seq: 1,
            tier: 'B',
            outcome: Outcome::Nacked("some reason with spaces".to_string()),
            t_micros: 5,
            rtt_micros: 6,
        };
        assert_eq!(LedgerEntry::from_line(&entry.to_line()), Some(entry));
    }

    #[test]
    fn ledger_rejects_malformed_lines() {
        assert_eq!(LedgerEntry::from_line(""), None);
        assert_eq!(LedgerEntry::from_line("1 S"), None);
    }

    #[test]
    fn ledger_rejects_lines_the_encoder_would_never_emit() {
        // A kill -9 mid-write can truncate the final ledger line. Accepting a
        // truncated line as well-formed would let the verifier trust corrupt
        // ground truth — for an oracle, wrongly accepting beats nothing.
        assert_eq!(
            LedgerEntry::from_line("42 S 100 200 NACK"),
            None,
            "a NACK with no reason field is truncated, not an empty reason"
        );
        assert_eq!(
            LedgerEntry::from_line("42 S 100 200 ACK trailing garbage"),
            None,
            "ACK carries no reason; trailing content means corruption"
        );
        assert_eq!(
            LedgerEntry::from_line("42 S 100 200 UNK trailing garbage"),
            None,
            "UNK carries no reason; trailing content means corruption"
        );
        assert_eq!(LedgerEntry::from_line("42 S 100 200 WAT"), None, "unknown tag");
    }

    #[test]
    fn ledger_round_trips_an_empty_nack_reason() {
        // Distinct from the truncated case above: `to_line` emits a trailing
        // space, so the empty reason IS present as a sixth field.
        let entry = LedgerEntry {
            seq: 1,
            tier: 'S',
            outcome: Outcome::Nacked(String::new()),
            t_micros: 2,
            rtt_micros: 3,
        };
        assert_eq!(entry.to_line(), "1 S 2 3 NACK ");
        assert_eq!(LedgerEntry::from_line(&entry.to_line()), Some(entry));
    }

    #[test]
    fn ledger_round_trips_reasons_containing_newlines_and_backslashes() {
        // Phase 2+ puts sink error text here, and HTTP body excerpts contain
        // newlines. Escaping must be lossless, not lossy.
        for reason in [
            "line1\nline2",
            "carriage\r\nreturn",
            "back\\slash",
            "escaped-looking \\n literal",
            "ACK",
            "  leading and trailing  ",
            "multiple   consecutive   spaces",
        ] {
            let entry = LedgerEntry {
                seq: 9,
                tier: 'B',
                outcome: Outcome::Nacked(reason.to_string()),
                t_micros: 1,
                rtt_micros: 1,
            };
            let line = entry.to_line();
            assert!(
                !line.contains('\n') && !line.contains('\r'),
                "ledger line must stay single-line for reason {reason:?}, got {line:?}"
            );
            assert_eq!(
                LedgerEntry::from_line(&line),
                Some(entry),
                "round trip must be lossless for reason {reason:?}"
            );
        }
    }

    #[test]
    fn ledger_round_trips_numeric_boundaries() {
        let entry = LedgerEntry {
            seq: u64::MAX,
            tier: 'U',
            outcome: Outcome::Unknown,
            t_micros: u64::MAX,
            rtt_micros: u64::MAX,
        };
        assert_eq!(LedgerEntry::from_line(&entry.to_line()), Some(entry));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos && cargo test ledger_`
Expected: FAIL — `cannot find type Outcome in this scope`.

- [ ] **Step 3: Write minimal implementation**

Append to `chaos/src/lib.rs` (above the `#[cfg(test)]` module):

```rust
/// What the daemon told the producer about one record.
///
/// Exactly three outcomes, and the third is not a failure. `Unknown` records
/// are legitimately indeterminate — the connection died before a response — so
/// the verifier constrains neither their delivery nor their absence. Counting
/// them rather than reclassifying them is what keeps the oracle honest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The daemon returned Ack. This is a promise and the suite holds weir to it.
    Acked,
    /// The daemon returned Nack. It explicitly refused the record.
    Nacked(String),
    /// Pushed, but no response arrived. Either outcome conforms.
    Unknown,
}

/// One record's fate, as recorded by the load generator.
///
/// Serialised as a single space-delimited line. The Nack reason comes last so
/// it may contain spaces without needing quoting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerEntry {
    /// Per-run sequence number; pairs with the run id to identify the record.
    pub seq: u64,
    /// Durability tier: 'S' Sync, 'B' Batched, 'U' Buffered (unsynced).
    pub tier: char,
    /// What the daemon said.
    pub outcome: Outcome,
    /// Wall-clock microseconds since the Unix epoch at push time.
    pub t_micros: u64,
    /// Round-trip time of the push call, in microseconds.
    pub rtt_micros: u64,
}

impl LedgerEntry {
    /// Serialises to one line: `seq tier t_micros rtt_micros outcome [reason…]`.
    ///
    /// A Nack reason is escaped rather than stripped, so the line stays
    /// single-line **and** the round trip is lossless. Later phases put sink
    /// error text in this field, and HTTP body excerpts legitimately contain
    /// newlines — replacing them with spaces would silently corrupt the audit
    /// trail while looking like it worked.
    #[must_use]
    pub fn to_line(&self) -> String {
        debug_assert!(
            matches!(self.tier, 'S' | 'B' | 'U'),
            "tier must be S, B or U; {:?} would collide with the field separator \
             and corrupt the line",
            self.tier
        );
        let (tag, reason) = match &self.outcome {
            Outcome::Acked => ("ACK", String::new()),
            Outcome::Unknown => ("UNK", String::new()),
            Outcome::Nacked(r) => ("NACK", format!(" {}", escape_reason(r))),
        };
        format!(
            "{} {} {} {} {}{}",
            self.seq, self.tier, self.t_micros, self.rtt_micros, tag, reason
        )
    }

    /// Parses a line produced by [`LedgerEntry::to_line`].
    ///
    /// **Strict by design: rejects anything `to_line` would never emit.** This
    /// is the oracle's audit trail, and the harness kills the daemon mid-write
    /// by design — so a truncated final line is a realistic input, not a
    /// hypothetical one. Accepting `"42 S 100 200 NACK"` (no reason field) or
    /// `"42 S 100 200 ACK <garbage>"` as well-formed would let the verifier
    /// trust corrupt ground truth. For an oracle, wrongly accepting is worse
    /// than rejecting.
    #[must_use]
    pub fn from_line(line: &str) -> Option<Self> {
        let mut parts = line.splitn(6, ' ');
        let seq = parts.next()?.parse().ok()?;
        let tier = parts.next()?.chars().next()?;
        let t_micros = parts.next()?.parse().ok()?;
        let rtt_micros = parts.next()?.parse().ok()?;
        let tag = parts.next()?;
        // The 6th field: present iff the tag is NACK.
        let trailing = parts.next();
        let outcome = match (tag, trailing) {
            ("ACK", None) => Outcome::Acked,
            ("UNK", None) => Outcome::Unknown,
            ("NACK", Some(reason)) => Outcome::Nacked(unescape_reason(reason)),
            _ => return None,
        };
        Some(Self { seq, tier, outcome, t_micros, rtt_micros })
    }
}

/// Escapes a Nack reason so it survives a single-line format losslessly.
fn escape_reason(r: &str) -> String {
    let mut out = String::with_capacity(r.len());
    for c in r.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out
}

/// Inverse of [`escape_reason`].
///
/// An unrecognised escape is preserved verbatim rather than guessed at: this
/// text ends up in a report a human reads, so showing the raw bytes beats
/// inventing an interpretation.
fn unescape_reason(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}
```

> **Note for the Python verifier (Task 7).** It reads only `parts[4]` (the tag)
> and discards the reason, so it needs no unescaping. Escaping is
> Rust-side-only and does not change the field layout the Python parser
> depends on.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos && cargo test`
Expected: PASS — 11 tests (4 from Task 1 + 7 ledger tests).

- [ ] **Step 5: Commit**

```bash
git add chaos/src/lib.rs
git commit -m "feat(chaos): add outcome ledger with three-state record fate"
```

---

## Task 3: The recorder (durable recording sink)

**Files:**
- Create: `chaos/src/bin/recorder.rs`

**Interfaces:**
- Consumes: `chaos::decode_record` from Task 1.
- Produces: a binary accepting `--bind <addr> --log <path>`, serving `POST /ingest`, appending `run_id seq` lines and **fsyncing before responding 200**.

- [ ] **Step 1: Write the failing test**

First append the bin target to `chaos/Cargo.toml` (Task 1 deliberately left it
out so the project stayed buildable before this file existed):

```toml
[[bin]]
name = "recorder"
path = "src/bin/recorder.rs"
test = true
doc = false
```

Then create `chaos/src/bin/recorder.rs`:

```rust
//! The recording sink — the oracle's delivery log.
//!
//! weir posts committed batches here as NDJSON; this appends each record's
//! identity to a log and **fsyncs before returning 200**.
//!
//! The fsync is not incidental — it is the `Sink` contract. weir treats a 200
//! as "durably committed downstream" and becomes free to reclaim the segment.
//! A recorder that buffered in memory and lost records on its own crash would
//! manufacture false durability violations. The oracle must be at least as
//! durable as the thing it judges.
//!
//! Hand-rolled HTTP over `TcpListener`: the surface is one POST route, and a
//! framework dependency would buy nothing while adding a supply chain to a
//! component whose whole job is to be trustworthy.

use std::io::Write;

fn main() {
    eprintln!("not implemented");
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_one_line_per_ndjson_record() {
        let dir = std::env::temp_dir().join(format!("chaos-rec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let body = format!(
            "{}\n{}\n",
            weir_chaos::encode_record(3, 1, 64),
            weir_chaos::encode_record(3, 2, 64)
        );
        let written = log.append_ndjson(body.as_bytes()).unwrap();
        assert_eq!(written, 2);

        let mut contents = String::new();
        std::fs::File::open(&log_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "3 1\n3 2\n");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn skips_undecodable_lines_without_failing_the_batch() {
        let dir = std::env::temp_dir().join(format!("chaos-rec-junk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let body = format!("junk\n{}\n\n", weir_chaos::encode_record(4, 9, 64));
        let written = log.append_ndjson(body.as_bytes()).unwrap();
        assert_eq!(written, 1, "only the decodable record counts");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parses_content_length_from_a_request_head() {
        let head = "POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Length: 42\r\n\r\n";
        assert_eq!(content_length(head), Some(42));
        assert_eq!(content_length("POST / HTTP/1.1\r\n\r\n"), None);
    }

    // ── Socket-level tests ────────────────────────────────────────────────
    // The unit tests above exercise the log and the header parser in
    // isolation. These drive `handle` over a real loopback socket, because
    // every defect that actually threatens the oracle lives at that boundary:
    // a peer that stalls, a peer that lies about its length, a peer that
    // never sends a length at all.

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("chaos-rec-{tag}-{}-{n}", std::process::id()))
    }

    /// Drives one request through `handle` over loopback.
    ///
    /// `linger` keeps the client socket open after writing, so a
    /// short-body request genuinely stalls the server rather than hitting a
    /// clean EOF. `timeout` is applied to the server side.
    fn drive(
        request: Vec<u8>,
        linger: Duration,
        timeout: Duration,
    ) -> (std::io::Result<Response>, String) {
        let dir = unique_dir("drive");
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("delivered.log");
        let mut log = DeliveryLog::create(&log_path).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = std::thread::spawn(move || {
            let mut s = TcpStream::connect(addr).unwrap();
            let _ = s.write_all(&request);
            let _ = s.flush();
            if linger.is_zero() {
                // Half-close: FIN on the write side only. The server sees a
                // real EOF — so an incomplete request head is rejected
                // promptly instead of waiting out its timeout — while this
                // side can still read the response. A full `drop` here would
                // instead risk an RST that beats the server's response write,
                // which is what made these tests flaky.
                let _ = s.shutdown(Shutdown::Write);
            } else {
                // Simulate a HALF-OPEN peer: hold the connection open with no
                // FIN at all, which is what a process killed mid-POST leaves
                // behind. The server must time out rather than block forever.
                // Do NOT half-close here — that would hand the server a clean
                // EOF and the timeout would never be exercised.
                std::thread::sleep(linger);
            }
            let mut discard = Vec::new();
            let _ = s.read_to_end(&mut discard);
        });

        let (mut server_side, _) = listener.accept().unwrap();
        server_side.set_read_timeout(Some(timeout)).unwrap();
        server_side.set_write_timeout(Some(timeout)).unwrap();
        let result = handle(&mut server_side, &mut log);
        // Close the server side so the client's `read_to_end` sees EOF and the
        // join below cannot deadlock.
        drop(server_side);

        client.join().unwrap();
        let contents = std::fs::read_to_string(&log_path).unwrap_or_default();
        std::fs::remove_dir_all(&dir).ok();
        (result, contents)
    }

    fn post(body: &str) -> Vec<u8> {
        format!(
            "POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    #[test]
    fn post_records_every_decodable_record_then_acks() {
        let body = format!(
            "{}\n{}\n",
            weir_chaos::encode_record(3, 1, 64),
            weir_chaos::encode_record(3, 2, 64)
        );
        let (res, log) = drive(post(&body), Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Recorded(2));
        assert_eq!(log, "3 1\n3 2\n");
    }

    #[test]
    fn head_probe_is_answered_without_touching_the_log() {
        let req = b"HEAD /ingest HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Health);
        assert_eq!(log, "", "a health probe must not write a delivery");
    }

    #[test]
    fn post_without_content_length_is_refused_not_acked() {
        // Treating a missing length as a zero-length body would return 200 for
        // a delivery never read off the socket — a phantom ack, which is the
        // one thing the oracle must never produce.
        let req = b"POST /ingest HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(411));
        assert_eq!(log, "");
    }

    #[test]
    fn oversized_content_length_is_refused_before_allocating() {
        // Content-Length is peer-supplied. Allocating it blindly lets a corrupt
        // header abort the process (Rust aborts on allocation failure), killing
        // the oracle outright.
        let req = format!(
            "POST /ingest HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        )
        .into_bytes();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(413));
        assert_eq!(log, "");
    }

    #[test]
    fn a_non_post_method_is_refused() {
        let req = b"GET /ingest HTTP/1.1\r\nHost: x\r\n\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(405));
        assert_eq!(log, "");
    }

    #[test]
    fn a_truncated_body_times_out_rather_than_wedging_forever() {
        // Declares 1000 bytes, sends 10, holds the socket open. Without a read
        // timeout this blocks FOREVER — and because the accept loop is serial,
        // one such peer starves every subsequent delivery from every future
        // connection until the process is killed. This harness injects faults
        // that produce exactly this shape (a peer killed mid-POST leaves the
        // connection open with no FIN), and a lost delivery reads back as a
        // phantom durability violation.
        let req = b"POST /ingest HTTP/1.1\r\nContent-Length: 1000\r\n\r\n0123456789".to_vec();
        let start = std::time::Instant::now();
        let (res, log) = drive(
            req,
            Duration::from_millis(600),
            Duration::from_millis(150),
        );
        assert!(res.is_err(), "a stalled peer must error, not hang");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the stall must be bounded, took {:?}",
            start.elapsed()
        );
        assert_eq!(log, "", "nothing was durably delivered");
    }

    #[test]
    fn a_connection_closed_mid_headers_is_refused_not_acked() {
        let req = b"POST /ingest HTTP/1.1\r\nHost: x\r\n".to_vec();
        let (res, log) = drive(req, Duration::ZERO, Duration::from_secs(5));
        assert_eq!(res.unwrap(), Response::Rejected(0));
        assert_eq!(log, "");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos && cargo test --bin recorder`
Expected: FAIL — `cannot find type DeliveryLog in this scope`.

- [ ] **Step 3: Write minimal implementation**

Replace the `fn main()` stub in `chaos/src/bin/recorder.rs` with:

```rust
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::time::Duration;
// `Shutdown` is used only by the socket tests. `cargo clippy --all-targets`
// also builds the plain bin target, where an unconditional import would be
// an unused-import warning and fail under `-D warnings`.
#[cfg(test)]
use std::net::Shutdown;

/// Append-only log of delivered record identities, fsynced before each ack.
struct DeliveryLog {
    file: File,
}

impl DeliveryLog {
    fn create(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }

    /// Appends every decodable record in an NDJSON body. Returns the count
    /// written. Undecodable lines are skipped rather than failing the batch:
    /// weir would retry the whole segment, and a permanently-undecodable
    /// record would wedge the drain forever.
    fn append_ndjson(&mut self, body: &[u8]) -> std::io::Result<usize> {
        let mut out = String::new();
        let mut count = 0usize;
        for line in body.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Ok(text) = std::str::from_utf8(line) else {
                continue;
            };
            let Some(rec) = weir_chaos::decode_record(text) else {
                continue;
            };
            out.push_str(&format!("{} {}\n", rec.run_id, rec.seq));
            count += 1;
        }
        if !out.is_empty() {
            self.file.write_all(out.as_bytes())?;
            // Durable BEFORE the 200. See the module docs.
            self.file.sync_data()?;
        }
        Ok(count)
    }
}

/// Read/write timeout on an accepted connection.
///
/// Without this the recorder wedges **permanently**: `read_exact` on a raw
/// `TcpStream` blocks forever, and the accept loop below is serial, so one
/// half-open peer starves every subsequent delivery until the process is
/// killed. This harness injects faults that produce exactly that shape — a peer
/// killed mid-POST leaves the connection open with no FIN — and a delivery lost
/// that way reads back as a phantom durability violation. Ten seconds is six
/// orders of magnitude of headroom for a localhost POST, and bounds the
/// worst-case stall.
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Largest body the recorder will allocate for.
///
/// `Content-Length` is peer-supplied. Allocating it blindly lets a corrupt
/// header abort the process — Rust aborts on allocation failure — which would
/// kill the oracle itself. Sized far above the largest plausible NDJSON batch.
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// What the recorder did with one request.
///
/// Returned rather than swallowed so tests can assert on the decision instead
/// of scraping response bytes.
#[derive(Debug, PartialEq, Eq)]
enum Response {
    /// Health probe answered; nothing recorded.
    Health,
    /// Batch durably recorded; carries the record count.
    Recorded(usize),
    /// Refused without recording; carries the status code (`0` = peer vanished
    /// before sending a complete request head).
    Rejected(u16),
}

/// Extracts `Content-Length` from an HTTP request head. Case-insensitive.
fn content_length(head: &str) -> Option<usize> {
    head.lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
}

fn reject(stream: &mut TcpStream, code: u16, reason: &str) -> std::io::Result<Response> {
    write!(stream, "HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\n\r\n")?;
    stream.flush()?;
    Ok(Response::Rejected(code))
}

fn handle(stream: &mut TcpStream, log: &mut DeliveryLog) -> std::io::Result<Response> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            // Peer vanished mid-headers. Nothing to record, nothing to ack.
            return Ok(Response::Rejected(0));
        }
        let blank = line == "\r\n" || line == "\n";
        head.push_str(&line);
        if blank {
            break;
        }
    }

    // A HEAD probe is how the HTTP sink's health check works; answer it 200.
    if head.starts_with("HEAD") {
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
        stream.flush()?;
        return Ok(Response::Health);
    }
    if !head.starts_with("POST") {
        return reject(stream, 405, "Method Not Allowed");
    }

    // A POST with no Content-Length must NOT be acked. Treating it as a
    // zero-length body would return 200 for a delivery never read off the
    // socket — a phantom ack, the one thing the oracle must never produce.
    let Some(len) = content_length(&head) else {
        return reject(stream, 411, "Length Required");
    };
    if len > MAX_BODY_BYTES {
        return reject(stream, 413, "Payload Too Large");
    }

    let mut body = vec![0u8; len];
    reader.read_exact(&mut body)?;
    // Durable BEFORE the 200 — see the module docs. If this errors we return
    // without acking, weir classifies it transient, and retries the segment.
    let recorded = log.append_ndjson(&body)?;
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
    stream.flush()?;
    Ok(Response::Recorded(recorded))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut bind = "127.0.0.1:9900".to_string();
    let mut log_path = String::new();
    let mut i = 1;
    while i + 1 < args.len() {
        match args[i].as_str() {
            "--bind" => bind = args[i + 1].clone(),
            "--log" => log_path = args[i + 1].clone(),
            _ => {}
        }
        i += 2;
    }
    if log_path.is_empty() {
        eprintln!("usage: recorder --bind <addr> --log <path>");
        std::process::exit(2);
    }

    let mut log = DeliveryLog::create(Path::new(&log_path)).expect("open delivery log");
    let listener = TcpListener::bind(&bind).expect("bind recorder");
    eprintln!("recorder listening on {bind}, log at {log_path}");

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                // Bound every blocking read and write. Without this one stalled
                // peer wedges the whole recorder — see IO_TIMEOUT.
                if let Err(e) = s.set_read_timeout(Some(IO_TIMEOUT)) {
                    eprintln!("recorder: could not set read timeout: {e}");
                }
                if let Err(e) = s.set_write_timeout(Some(IO_TIMEOUT)) {
                    eprintln!("recorder: could not set write timeout: {e}");
                }
                match handle(&mut s, &mut log) {
                    Ok(Response::Rejected(code)) if code != 0 => {
                        eprintln!("recorder: refused a request with {code}");
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("recorder: request failed: {e}"),
                }
            }
            Err(e) => eprintln!("recorder: accept failed: {e}"),
        }
    }
}
```

Note: the `use std::io::Write;` already at the top of the file stays; add the other `use` lines shown above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos && cargo test`
Expected: PASS — 21 tests (11 from Tasks 1-2 + 3 recorder unit + 7 recorder socket tests).

- [ ] **Step 5: Verify it serves a real request**

```bash
cd chaos && cargo build --release
./target/release/recorder --bind 127.0.0.1:9900 --log /tmp/chaos-delivered.log &
sleep 1
printf '{"run":1,"seq":1,"pad":"aa"}\n{"run":1,"seq":2,"pad":"aa"}\n' > /tmp/body.ndjson
curl -s -X POST --data-binary @/tmp/body.ndjson http://127.0.0.1:9900/ingest -o /dev/null -w '%{http_code}\n'
cat /tmp/chaos-delivered.log
kill %1
```

Expected: `200`, then a log containing exactly `1 1` and `1 2` on separate lines.

- [ ] **Step 6: Commit**

```bash
git add chaos/Cargo.toml chaos/src/bin/recorder.rs
git commit -m "feat(chaos): add recording sink with durable-before-ack append"
```

---

## Task 4: The load generator

**Files:**
- Create: `chaos/src/bin/loadgen.rs`

**Interfaces:**
- Consumes: `chaos::{encode_record, LedgerEntry, Outcome}` from Tasks 1–2.
- Produces: a binary accepting `--socket <path> --ledger <path> --run-id <u64> --threads <n> --record-size <n> --tier <S|B|U> --duration-secs <n>`, appending `LedgerEntry` lines and reconnecting across daemon restarts.

- [ ] **Step 1: Write the failing test**

First append the bin target to `chaos/Cargo.toml` (same reason as Task 3 — the
declaration lands with the file it points at):

```toml
[[bin]]
name = "loadgen"
path = "src/bin/loadgen.rs"
test = true
doc = false
```

Then create `chaos/src/bin/loadgen.rs`:

```rust
//! The load generator — sustained producer pool and the outcome ledger.
//!
//! Runs unprivileged and OUTSIDE the fault zone. The suite kills the daemon,
//! never this process, so an in-memory ledger correctly models what the
//! producer *was told* — which is exactly what the durability claim is about.
//!
//! The ledger is flushed to disk continuously so a multi-day run survives an
//! operator mistake or an OOM.

fn main() {
    eprintln!("not implemented");
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_client_errors_into_ledger_outcomes() {
        use weir_client::ClientError;
        use weir_core::NackReason;

        // A Nack is an explicit refusal — weir said no, and I2 will hold it to that.
        assert!(matches!(
            classify(&ClientError::Nack(NackReason::PayloadTooLarge)),
            weir_chaos::Outcome::Nacked(_)
        ));

        // An I/O error means the response never arrived: legitimately indeterminate.
        let io = ClientError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "x"));
        assert_eq!(classify(&io), weir_chaos::Outcome::Unknown);

        // A protocol violation is equally indeterminate from the producer's view.
        assert_eq!(
            classify(&ClientError::Protocol("bad".into())),
            weir_chaos::Outcome::Unknown
        );
    }

    #[test]
    fn tier_chars_map_to_durability() {
        use weir_core::Durability;
        assert_eq!(tier_from_char('S'), Some(Durability::Sync));
        assert_eq!(tier_from_char('B'), Some(Durability::Batched));
        assert_eq!(tier_from_char('U'), Some(Durability::Buffered));
        assert_eq!(tier_from_char('x'), None);
    }

    /// A writer that always fails, to prove ledger write errors propagate.
    struct FailingWriter;

    impl std::io::Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk gone"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_entries_propagates_io_errors_instead_of_swallowing_them() {
        // The ledger IS the oracle. A silently-dropped ledger write loses the
        // record of what weir acked, which either invents a violation or hides
        // one. It must be loud.
        let entries = vec![weir_chaos::LedgerEntry {
            seq: 1,
            tier: 'S',
            outcome: weir_chaos::Outcome::Acked,
            t_micros: 1,
            rtt_micros: 2,
        }];
        let err = write_entries(&mut FailingWriter, &entries).expect_err("must propagate");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn write_entries_emits_one_newline_terminated_line_per_entry() {
        let entries = vec![
            weir_chaos::LedgerEntry {
                seq: 1,
                tier: 'S',
                outcome: weir_chaos::Outcome::Acked,
                t_micros: 10,
                rtt_micros: 20,
            },
            weir_chaos::LedgerEntry {
                seq: 2,
                tier: 'U',
                outcome: weir_chaos::Outcome::Unknown,
                t_micros: 11,
                rtt_micros: 21,
            },
        ];
        let mut buf: Vec<u8> = Vec::new();
        write_entries(&mut buf, &entries).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text, "1 S 10 20 ACK\n2 U 11 21 UNK\n");
    }

    /// A ledger sink that can be made to fail on append, on sync, or neither.
    #[derive(Default)]
    struct FlakySink {
        fail_append: bool,
        fail_sync: bool,
        written: Vec<u8>,
        synced: usize,
    }

    impl LedgerSink for FlakySink {
        fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            if self.fail_append {
                return Err(std::io::Error::other("append failed"));
            }
            self.written.extend_from_slice(bytes);
            Ok(())
        }
        fn sync(&mut self) -> std::io::Result<()> {
            if self.fail_sync {
                return Err(std::io::Error::other("sync failed"));
            }
            self.synced += 1;
            Ok(())
        }
    }

    fn sample_entry(seq: u64) -> weir_chaos::LedgerEntry {
        weir_chaos::LedgerEntry {
            seq,
            tier: 'S',
            outcome: weir_chaos::Outcome::Acked,
            t_micros: 1,
            rtt_micros: 2,
        }
    }

    #[test]
    fn flush_clears_pending_and_syncs_on_success() {
        let sink = Arc::new(Mutex::new(FlakySink::default()));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending = vec![sample_entry(1), sample_entry(2)];
        assert!(flush(&sink, &mut pending, &fatal));
        assert!(pending.is_empty(), "pending must be cleared on success");
        assert!(!fatal.load(Ordering::Relaxed));
        let s = sink.lock().unwrap();
        assert_eq!(s.synced, 1, "durability needs a sync, not just a write");
        assert_eq!(
            String::from_utf8(s.written.clone()).unwrap(),
            "1 S 1 2 ACK\n2 S 1 2 ACK\n"
        );
    }

    #[test]
    fn flush_sets_fatal_and_retains_pending_when_append_fails() {
        let sink = Arc::new(Mutex::new(FlakySink {
            fail_append: true,
            ..Default::default()
        }));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending = vec![sample_entry(1)];
        assert!(!flush(&sink, &mut pending, &fatal));
        assert!(
            fatal.load(Ordering::Relaxed),
            "a ledger entry that cannot be recorded must be fatal, never swallowed"
        );
        assert_eq!(
            pending.len(),
            1,
            "pending must survive a failed flush rather than vanish"
        );
    }

    #[test]
    fn flush_sets_fatal_when_only_the_sync_fails() {
        // Written but not durable is still a lost entry: this harness kills the
        // machine's processes by design, and an unsynced ledger tail goes with
        // them.
        let sink = Arc::new(Mutex::new(FlakySink {
            fail_sync: true,
            ..Default::default()
        }));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending = vec![sample_entry(1)];
        assert!(!flush(&sink, &mut pending, &fatal));
        assert!(fatal.load(Ordering::Relaxed));
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn flush_on_empty_pending_is_a_no_op() {
        let sink = Arc::new(Mutex::new(FlakySink {
            fail_append: true,
            ..Default::default()
        }));
        let fatal = Arc::new(AtomicBool::new(false));
        let mut pending: Vec<weir_chaos::LedgerEntry> = Vec::new();
        assert!(
            flush(&sink, &mut pending, &fatal),
            "nothing to write is not a failure"
        );
        assert!(!fatal.load(Ordering::Relaxed));
    }

    #[test]
    fn classify_maps_every_refusal_shape_to_nacked() {
        // The Nacked/Unknown split is load-bearing: invariant I2 later asserts a
        // nacked record never appears downstream, so a refusal misfiled as
        // Unknown silently weakens that check.
        use weir_client::ClientError;
        let refusals = [
            ClientError::VersionMismatch { daemon_version: 2 },
            ClientError::UnknownNack(0x0a),
            ClientError::PayloadTooLarge { len: 1, limit: 0 },
            ClientError::EmptyPayload,
            ClientError::NoDefaultDurability,
        ];
        for e in refusals {
            assert!(
                matches!(classify(&e), weir_chaos::Outcome::Nacked(_)),
                "{e:?} is an explicit refusal and must classify as Nacked"
            );
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos && cargo test --bin loadgen`
Expected: FAIL — `cannot find function classify in this scope`.

- [ ] **Step 3: Write minimal implementation**

Replace the `fn main()` stub in `chaos/src/bin/loadgen.rs` with:

```rust
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use weir_chaos::{encode_record, LedgerEntry, Outcome};
use weir_client::{ClientError, WeirClient};
use weir_core::Durability;

/// Read/write timeout on the producer's socket.
///
/// Without it `--duration-secs` is not a deadline at all: the loop checks the
/// clock only between iterations, so a daemon that accepts a connection and
/// then stops responding without closing it blocks `push` forever. That is
/// worse here than a plain hang — Task 8 asserts this process is *alive* before
/// each fault, and a wedged generator is alive while producing nothing, so load
/// would silently stop without tripping the guard. Generous relative to a
/// Sync-tier ack (hundreds of microseconds) even under injected slow-disk
/// faults.
const PUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// Maps a client error to a ledger outcome.
///
/// The distinction that matters: a `Nack` is weir *explicitly refusing* the
/// record, and invariant I2 holds it to that — a nacked record must never
/// appear downstream. Everything else means the response never arrived, so the
/// record's fate is genuinely indeterminate and the verifier constrains
/// nothing about it.
fn classify(err: &ClientError) -> Outcome {
    match err {
        ClientError::Nack(reason) => Outcome::Nacked(format!("{reason:?}")),
        ClientError::VersionMismatch { daemon_version } => {
            Outcome::Nacked(format!("VersionMismatch({daemon_version})"))
        }
        ClientError::UnknownNack(b) => Outcome::Nacked(format!("UnknownNack({b:#04x})")),
        // Local pre-send rejections are harness bugs, not daemon behaviour, but
        // they are still explicit refusals: no bytes reached the daemon.
        ClientError::PayloadTooLarge { .. } => Outcome::Nacked("LocalPayloadTooLarge".into()),
        ClientError::EmptyPayload => Outcome::Nacked("LocalEmptyPayload".into()),
        ClientError::NoDefaultDurability => Outcome::Nacked("NoDefaultDurability".into()),
        // Io / Protocol / any future variant: no answer arrived.
        _ => Outcome::Unknown,
    }
}

fn tier_from_char(c: char) -> Option<Durability> {
    match c {
        'S' => Some(Durability::Sync),
        'B' => Some(Durability::Batched),
        'U' => Some(Durability::Buffered),
        _ => None,
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

struct Args {
    socket: String,
    ledger: String,
    run_id: u64,
    threads: usize,
    record_size: usize,
    tier: char,
    duration_secs: u64,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let get = |name: &str, default: &str| -> String {
        argv.iter()
            .position(|a| a == name)
            .and_then(|i| argv.get(i + 1))
            .cloned()
            .unwrap_or_else(|| default.to_string())
    };
    Args {
        socket: get("--socket", "/tmp/weir/run/weir.sock"),
        ledger: get("--ledger", "./ledger.log"),
        run_id: get("--run-id", "1").parse().expect("--run-id must be u64"),
        threads: get("--threads", "4").parse().expect("--threads must be usize"),
        record_size: get("--record-size", "256").parse().expect("--record-size"),
        tier: get("--tier", "S").chars().next().unwrap_or('S'),
        duration_secs: get("--duration-secs", "60").parse().expect("--duration-secs"),
    }
}

fn main() {
    let args = parse_args();
    let durability = tier_from_char(args.tier).expect("--tier must be S, B, or U");

    let ledger = Arc::new(Mutex::new(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&args.ledger)
            .expect("open ledger"),
    ));
    let seq = Arc::new(AtomicU64::new(0));
    // Set when any thread cannot durably record an outcome. Every thread
    // watches it, so one ledger failure winds the whole generator down rather
    // than leaving the run producing records nobody is keeping track of.
    let fatal = Arc::new(AtomicBool::new(false));

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let mut handles = Vec::new();

    for _ in 0..args.threads {
        let ledger = Arc::clone(&ledger);
        let seq = Arc::clone(&seq);
        let fatal = Arc::clone(&fatal);
        let socket = args.socket.clone();
        let run_id = args.run_id;
        let record_size = args.record_size;
        let tier = args.tier;

        handles.push(std::thread::spawn(move || {
            let mut client: Option<WeirClient> = None;
            let mut pending: Vec<LedgerEntry> = Vec::with_capacity(256);

            while !fatal.load(Ordering::Relaxed) && Instant::now() < deadline {
                // Reconnect as needed. The daemon is killed repeatedly by
                // design, so a dead connection is expected, not exceptional.
                if client.is_none() {
                    match WeirClient::connect(&socket) {
                        Ok(c) => {
                            // Bound every push. See PUSH_TIMEOUT.
                            let _ = c.set_read_timeout(Some(PUSH_TIMEOUT));
                            let _ = c.set_write_timeout(Some(PUSH_TIMEOUT));
                            client = Some(c);
                        }
                        Err(_) => {
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                    }
                }

                let my_seq = seq.fetch_add(1, Ordering::Relaxed);
                let payload = encode_record(run_id, my_seq, record_size);
                let t0 = Instant::now();
                let t_micros = now_micros();

                let c = client.as_mut().expect("just ensured connected");
                let outcome = match c.push(&payload, durability) {
                    Ok(()) => Outcome::Acked,
                    Err(e) => {
                        let o = classify(&e);
                        if !e.is_recoverable() {
                            client = None;
                        }
                        o
                    }
                };

                pending.push(LedgerEntry {
                    seq: my_seq,
                    tier,
                    outcome,
                    t_micros,
                    rtt_micros: t0.elapsed().as_micros() as u64,
                });

                if pending.len() >= 256 && !flush(&ledger, &mut pending, &fatal) {
                    break;
                }
            }
            flush(&ledger, &mut pending, &fatal);
        }));
    }

    // A panicking thread must not be silent. It would drop its buffered ledger
    // entries — possibly including genuinely acked records — WITHOUT setting
    // `fatal`, so the run would report success while the oracle had quietly
    // lost evidence. That is the one failure mode that conceals a real
    // durability violation rather than inventing one.
    let mut panicked = 0usize;
    for h in handles {
        if h.join().is_err() {
            panicked += 1;
        }
    }

    let pushed = seq.load(Ordering::Relaxed);
    if fatal.load(Ordering::Relaxed) {
        eprintln!("loadgen: ABORTED after {pushed} records — ledger write failed");
        std::process::exit(1);
    }
    if panicked > 0 {
        eprintln!(
            "loadgen: ABORTED after {pushed} records — {panicked} producer thread(s) \
             panicked; their unflushed ledger entries are lost, so this run's \
             verification cannot be trusted"
        );
        std::process::exit(1);
    }
    eprintln!("loadgen: finished, {pushed} records pushed");
}

/// Serialises entries, one line each. Split from the I/O so the formatting and
/// the failure path are independently testable.
fn serialise(entries: &[LedgerEntry]) -> String {
    let mut buf = String::with_capacity(entries.len() * 48);
    for e in entries {
        buf.push_str(&e.to_line());
        buf.push('\n');
    }
    buf
}

/// Serialises entries to a writer. Generic over `Write` so the error path is
/// testable with a deliberately-failing writer.
///
/// Test-only: production writes go through [`flush`] via [`LedgerSink`]. Left
/// unconditional it would be `dead_code` in the plain bin build, which
/// `clippy --all-targets -- -D warnings` rejects.
#[cfg(test)]
fn write_entries<W: Write>(w: &mut W, entries: &[LedgerEntry]) -> std::io::Result<()> {
    w.write_all(serialise(entries).as_bytes())
}

/// The ledger's durable-append surface.
///
/// A trait rather than a concrete `File` so that [`flush`] — the function that
/// decides whether the whole run is still trustworthy — is testable without a
/// real filesystem. Leaving it untestable would mean the harness's own
/// fail-loud path is the one piece of it with no coverage.
trait LedgerSink {
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    fn sync(&mut self) -> std::io::Result<()>;
}

impl LedgerSink for std::fs::File {
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.write_all(bytes)
    }
    fn sync(&mut self) -> std::io::Result<()> {
        self.sync_data()
    }
}

/// Durably appends `pending` to the ledger. Returns false if the write failed,
/// having set `fatal`.
///
/// A ledger write failure is not recoverable and must not be swallowed: the
/// ledger is the oracle, so losing an entry either invents a durability
/// violation or hides a real one. `pending` is cleared only on success, so a
/// transient caller could retry without losing entries.
fn flush<S: LedgerSink>(
    ledger: &Arc<Mutex<S>>,
    pending: &mut Vec<LedgerEntry>,
    fatal: &Arc<AtomicBool>,
) -> bool {
    if pending.is_empty() {
        return true;
    }
    let mut sink = ledger.lock().expect("ledger mutex");
    let bytes = serialise(pending);
    let result = sink.append(bytes.as_bytes()).and_then(|()| sink.sync());
    match result {
        Ok(()) => {
            pending.clear();
            true
        }
        Err(e) => {
            eprintln!("loadgen: FATAL — could not durably record {} ledger entries: {e}", pending.len());
            fatal.store(true, Ordering::Relaxed);
            false
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos && cargo test`
Expected: PASS — 30 tests (21 from Tasks 1-3 + 9 loadgen tests).

- [ ] **Step 5: Verify it builds clean**

Run: `cd chaos && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: no output, exit 0.

- [ ] **Step 6: Commit**

```bash
git add chaos/Cargo.toml chaos/src/bin/loadgen.rs
git commit -m "feat(chaos): add load generator with reconnecting producer pool"
```

---

## Task 5: Device-mapper stack plumbing

**Files:**
- Create: `chaos/orchestrator/dm_stack.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (shells out to system tools).
- Produces:
  - `dm_stack.StorageStack(backing_file, size_mb, mount_point)` with `.setup()`, `.teardown()`, `.is_mounted()`

Phase 1 builds loopback + ext4 only. `dm-delay` and `dm-flakey` layers are Phase 2/3; the class exposes the seam but does not use it yet.

- [ ] **Step 1: Write the failing test**

Create `chaos/orchestrator/test_dm_stack.py`:

```python
"""Tests for the device-mapper stack plumbing.

The setup/teardown test needs root and Linux; it skips otherwise so the file
is still runnable on a dev machine.
"""
import os
import platform
import subprocess
import tempfile
import unittest

import dm_stack


def _needs_root_linux():
    return platform.system() != "Linux" or os.geteuid() != 0


class TestStorageStack(unittest.TestCase):
    def test_rejects_a_size_too_small_for_ext4(self):
        with self.assertRaises(ValueError):
            dm_stack.StorageStack("/tmp/x.img", size_mb=1, mount_point="/mnt/x")

    def test_names_are_derived_from_the_mount_point(self):
        s = dm_stack.StorageStack("/tmp/x.img", size_mb=128, mount_point="/mnt/weir-wab")
        self.assertEqual(s.name, "weir-wab")

    @unittest.skipIf(_needs_root_linux(), "needs root on Linux")
    def test_setup_then_teardown_leaves_nothing_behind(self):
        with tempfile.TemporaryDirectory() as tmp:
            img = os.path.join(tmp, "wab.img")
            mnt = os.path.join(tmp, "mnt")
            os.makedirs(mnt)
            s = dm_stack.StorageStack(img, size_mb=128, mount_point=mnt)
            s.setup()
            try:
                self.assertTrue(s.is_mounted())
                probe = os.path.join(mnt, "probe")
                with open(probe, "w") as f:
                    f.write("x")
                self.assertTrue(os.path.exists(probe))
            finally:
                s.teardown()
            self.assertFalse(s.is_mounted())
            out = subprocess.run(
                ["losetup", "-j", img], capture_output=True, text=True
            ).stdout
            self.assertEqual(out.strip(), "", "loop device must be detached")
            self.assertFalse(
                os.path.exists(img),
                "backing image must be removed — the test name promises nothing is left",
            )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 test_dm_stack.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'dm_stack'`.

- [ ] **Step 3: Write minimal implementation**

Create `chaos/orchestrator/dm_stack.py`:

```python
"""Loopback + ext4 storage stack for the chaos harness.

Phase 1 builds the plumbing only: a sparse backing file, a loop device, an
ext4 filesystem, and a mount. The dm-delay and dm-flakey layers that inject
faults land in Phases 2-3; this class owns the lifecycle they will slot into.

Linux and root only.
"""
import os
import subprocess
import sys

# ext4 needs a few MiB of metadata before it will even mkfs.
MIN_SIZE_MB = 16


def _run(cmd, check=True):
    """Runs a command, returning CompletedProcess.

    On failure with `check=True` this raises with stderr INCLUDED in the
    message. `subprocess.run(check=True)` raises `CalledProcessError`, whose
    `__str__` omits captured stderr entirely — so a real `mkfs.ext4` refusal
    would reach the orchestrator as an undiagnosable "returned non-zero exit
    status 1" and the operator would have no idea why the run died.
    """
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed (exit {proc.returncode}): {' '.join(cmd)}\n"
            f"stderr: {proc.stderr.strip()}"
        )
    return proc


class StorageStack:
    """A loopback-backed ext4 filesystem the WAB can live on.

    Args:
        backing_file: path to the sparse image file (on the HOST fs).
        size_mb: image size. Small on purpose — a small volume is what makes
            real ENOSPC reachable in Phase 3.
        mount_point: where to mount it. Must already exist.
    """

    def __init__(self, backing_file, size_mb, mount_point):
        if size_mb < MIN_SIZE_MB:
            raise ValueError(
                f"size_mb={size_mb} is below the {MIN_SIZE_MB} MiB ext4 floor"
            )
        self.backing_file = backing_file
        self.size_mb = size_mb
        self.mount_point = mount_point
        self.name = os.path.basename(mount_point.rstrip("/"))
        self.loop_device = None

    def setup(self):
        """Creates the image, attaches a loop device, mkfs, and mounts."""
        with open(self.backing_file, "wb") as f:
            f.truncate(self.size_mb * 1024 * 1024)

        out = _run(["losetup", "--find", "--show", self.backing_file]).stdout
        self.loop_device = out.strip()

        # -F: the device is fresh; don't prompt. -q: quiet.
        _run(["mkfs.ext4", "-F", "-q", self.loop_device])
        # -t ext4 explicitly, rather than relying on blkid auto-probe. Phases
        # 2-3 insert dm-delay/dm-flakey between the loop device and the
        # filesystem; being explicit makes a mis-stacked device fail fast here
        # instead of mounting something unexpected.
        _run(["mount", "-t", "ext4", self.loop_device, self.mount_point])
        # weir requires 0700 on its WAB dir, and create_dir_private's mode only
        # applies to directories it actually creates — not a pre-existing mount
        # point. Set it here.
        os.chmod(self.mount_point, 0o700)

    def teardown(self):
        """Unmounts, detaches the loop device, and removes the image.

        Every step tolerates already-undone state so teardown is idempotent and
        safe to call from a finally block after a partial setup.
        """
        _run(["umount", self.mount_point], check=False)
        if self.loop_device:
            detach = _run(["losetup", "--detach", self.loop_device], check=False)
            if detach.returncode != 0:
                # Do NOT clear `loop_device`, and do NOT remove the image.
                # Unlinking it would not free the device — the loop driver's
                # open fd keeps the inode alive — and it would make the orphan
                # UN-FINDABLE, because `losetup -j <path>` matches by path.
                # Leaving both in place keeps the leak visible to an operator.
                print(
                    f"WARNING: could not detach {self.loop_device}: "
                    f"{detach.stderr.strip()}. Leaving it and {self.backing_file} "
                    f"in place so `losetup -j {self.backing_file}` still finds it.",
                    file=sys.stderr,
                )
                return
            self.loop_device = None
        if os.path.exists(self.backing_file):
            os.remove(self.backing_file)

    def is_mounted(self):
        """True if mount_point is currently a mount point."""
        return os.path.ismount(self.mount_point)
```

- [ ] **Step 4: Run tests to verify they pass**

Run (dev machine): `cd chaos/orchestrator && python3 test_dm_stack.py`
Expected: PASS — 2 tests pass, 1 skipped ("needs root on Linux").

Run (target box): `cd chaos/orchestrator && sudo python3 test_dm_stack.py`
Expected: PASS — 3 tests, none skipped.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/dm_stack.py chaos/orchestrator/test_dm_stack.py
git commit -m "feat(chaos): add loopback+ext4 storage stack plumbing"
```

---

## Task 6: Drain-quiescence detection

**Files:**
- Create: `chaos/orchestrator/quiescence.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `quiescence.scrape(metrics_url) -> dict[str, float]`
  - `quiescence.wait_for_quiescence(metrics_url, timeout_s) -> (bool, str)` — `(True, "")` on quiescence, `(False, reason)` on timeout.

This is the highest-risk component in Phase 1: get it wrong and every episode reports false violations.

- [ ] **Step 1: Write the failing test**

Create `chaos/orchestrator/test_quiescence.py`:

```python
"""Tests for drain-quiescence detection.

Parsing is tested against real OpenMetrics text; the wait loop is tested with
an injected scrape function so no daemon is required.
"""
import unittest

import quiescence

SAMPLE = """\
# HELP weir_queue_depth Work queue occupancy
# TYPE weir_queue_depth gauge
weir_queue_depth 0.0
# HELP weir_wab_bytes_on_disk WAB directory size
# TYPE weir_wab_bytes_on_disk gauge
weir_wab_bytes_on_disk 4096.0
# HELP weir_drain_state Drain state
# TYPE weir_drain_state gauge
weir_drain_state{state="draining"} 1.0
weir_drain_state{state="retrying_transient"} 0.0
weir_drain_state{state="blocked_dead_letter_full"} 0.0
"""


class TestParse(unittest.TestCase):
    def test_parses_plain_and_labelled_gauges(self):
        m = quiescence.parse(SAMPLE)
        self.assertEqual(m["weir_queue_depth"], 0.0)
        self.assertEqual(m["weir_wab_bytes_on_disk"], 4096.0)
        self.assertEqual(m['weir_drain_state{state="draining"}'], 1.0)

    def test_ignores_help_and_type_lines(self):
        m = quiescence.parse(SAMPLE)
        self.assertNotIn("# HELP", m)
        self.assertEqual(len([k for k in m if k.startswith("weir_drain_state")]), 3)


class TestWait(unittest.TestCase):
    def test_quiesces_when_all_three_signals_settle(self):
        # Bytes must be STABLE across consecutive polls, not merely low.
        readings = [
            {"weir_queue_depth": 5.0, "weir_wab_bytes_on_disk": 900000.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
            {"weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
             'weir_drain_state{state="draining"}': 1.0},
        ]
        # Five readings, not four: stability is measured BETWEEN consecutive
        # polls, so the first stable reading only establishes the baseline.
        # Poll 1 sets last_bytes, poll 2 changes it, polls 3-5 are the three
        # stable comparisons `stable_polls=3` requires.
        it = iter(readings)
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=10, scrape_fn=lambda _: next(it),
            poll_interval_s=0, stable_polls=3,
        )
        self.assertTrue(ok, reason)

    def test_reports_stuck_when_drain_is_blocked(self):
        blocked = {
            "weir_queue_depth": 0.0, "weir_wab_bytes_on_disk": 8192.0,
            'weir_drain_state{state="draining"}': 0.0,
            'weir_drain_state{state="blocked_dead_letter_full"}': 1.0,
        }
        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=lambda _: blocked,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("blocked", reason)

    def test_reports_stuck_when_bytes_never_settle(self):
        counter = {"n": 0}

        def growing(_):
            counter["n"] += 1
            return {
                "weir_queue_depth": 0.0,
                "weir_wab_bytes_on_disk": 1000.0 * counter["n"],
                'weir_drain_state{state="draining"}': 1.0,
            }

        ok, reason = quiescence.wait_for_quiescence(
            "unused", timeout_s=0.01, scrape_fn=growing,
            poll_interval_s=0, stable_polls=3,
        )
        self.assertFalse(ok)
        self.assertIn("timeout", reason)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 test_quiescence.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'quiescence'`.

- [ ] **Step 3: Write minimal implementation**

Create `chaos/orchestrator/quiescence.py`:

```python
"""Drain-quiescence detection via weir's existing /metrics endpoint.

Verification must not run before the drain has caught up, or it reports
violations that are really timing artefacts. Three signals settle the question,
all already exported — no new instrumentation was needed:

1. `weir_wab_bytes_on_disk` STABLE. Its registered HELP text states it counts
   the active segment plus sealed segments awaiting drain, and excludes
   `.confirmed`, `dead_letter/` and `quarantine/` — so it falls to
   active-segment-only exactly when drain has caught up. Stability across
   consecutive polls matters more than any absolute threshold, because the
   active segment's size is workload-dependent.
2. `weir_queue_depth` at zero — nothing still in flight to the WAB.
3. `weir_drain_state{state="draining"}` == 1 — not retrying, not blocked.

A timeout REPORTS "stuck" rather than hanging. A drain that never quiesces is
itself a finding, and silently waiting forever would hide it.
"""
import time
import urllib.request

DRAINING = 'weir_drain_state{state="draining"}'
BLOCKED = 'weir_drain_state{state="blocked_dead_letter_full"}'
RETRYING = 'weir_drain_state{state="retrying_transient"}'


def parse(text):
    """Parses OpenMetrics text into {series_name: value}.

    Series with labels keep their full `name{labels}` form as the key, so
    `weir_drain_state{state="draining"}` is directly addressable.
    """
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        name, value = parts
        try:
            out[name] = float(value)
        except ValueError:
            continue
    return out


def scrape(metrics_url):
    """Fetches and parses /metrics."""
    with urllib.request.urlopen(metrics_url, timeout=5) as resp:
        return parse(resp.read().decode("utf-8"))


def wait_for_quiescence(
    metrics_url, timeout_s, scrape_fn=None, poll_interval_s=0.5, stable_polls=3
):
    """Blocks until the drain is quiesced or the timeout expires.

    Returns (True, "") on quiescence, (False, reason) otherwise. Never raises
    on a stuck drain — a stuck drain is a finding to report, not an exception
    to crash on.
    """
    scrape_fn = scrape_fn or scrape
    deadline = time.monotonic() + timeout_s
    last_bytes = None
    stable = 0

    while time.monotonic() < deadline:
        try:
            m = scrape_fn(metrics_url)
        except Exception:  # daemon may be mid-restart; keep polling
            if poll_interval_s:
                time.sleep(poll_interval_s)
            continue

        if m.get(BLOCKED, 0.0) == 1.0:
            return False, "drain is blocked (BlockedDeadLetterFull)"

        depth = m.get("weir_queue_depth", 1.0)
        wab_bytes = m.get("weir_wab_bytes_on_disk")
        draining = m.get(DRAINING, 0.0) == 1.0

        if wab_bytes is not None and wab_bytes == last_bytes and depth == 0.0 and draining:
            stable += 1
            if stable >= stable_polls:
                return True, ""
        else:
            stable = 0
        last_bytes = wab_bytes

        if poll_interval_s:
            time.sleep(poll_interval_s)

    return False, f"timeout after {timeout_s}s waiting for drain quiescence"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos/orchestrator && python3 test_quiescence.py`
Expected: PASS — 5 tests.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/quiescence.py chaos/orchestrator/test_quiescence.py
git commit -m "feat(chaos): add three-signal drain-quiescence detection"
```

---

## Task 7: The verifier (I1/I2/I3)

**Files:**
- Create: `chaos/orchestrator/verify.py`

**Interfaces:**
- Consumes: ledger lines from Task 2, delivery-log lines from Task 3.
- Produces:
  - `verify.LogTailer(path)` with `.read_new() -> list[str]` — returns only lines appended since the last call, never a partial trailing line
  - `verify.Accumulator(delivered_run_id)` with `.ingest(ledger_lines, delivered_lines)` and `.check() -> VerifyResult`
  - `verify.check(ledger, delivered) -> VerifyResult` with fields `ok`, `i1_missing`, `i2_leaked`, `unknown_count`, `acked_count`, `delivered_distinct`, `duplicate_rate`

Verification runs after **every** episode, so re-reading both logs each time is
O(n²) — by the late episodes of a 20-episode run those files hold millions of
records and each pass costs tens of seconds, which eats into the load window
(see Task 8). The tailer reads each byte exactly once.

- [ ] **Step 1: Write the failing test**

Create `chaos/orchestrator/test_verify.py`:

```python
"""Tests for the three durability invariants."""
import unittest

import verify


def ledger(**kw):
    """Builds a ledger dict: seq -> (tag, reason)."""
    return {int(k): v for k, v in kw.items()}


class TestInvariants(unittest.TestCase):
    def test_clean_run_passes(self):
        r = verify.check(
            ledger(**{"1": ("ACK", ""), "2": ("ACK", ""), "3": ("NACK", "PayloadTooLarge")}),
            [1, 2],
        )
        self.assertTrue(r.ok)
        self.assertEqual(r.i1_missing, [])
        self.assertEqual(r.i2_leaked, [])
        self.assertEqual(r.acked_count, 2)

    def test_i1_catches_an_acked_record_that_never_arrived(self):
        r = verify.check(ledger(**{"1": ("ACK", ""), "2": ("ACK", "")}), [1])
        self.assertFalse(r.ok)
        self.assertEqual(r.i1_missing, [2])

    def test_i2_catches_a_nacked_record_that_was_delivered(self):
        r = verify.check(ledger(**{"1": ("NACK", "EmptyPayload")}), [1])
        self.assertFalse(r.ok)
        self.assertEqual(r.i2_leaked, [1])

    def test_duplicates_are_conformant_and_measured(self):
        # At-least-once: delivering seq 1 three times is legal.
        r = verify.check(ledger(**{"1": ("ACK", "")}), [1, 1, 1])
        self.assertTrue(r.ok)
        self.assertEqual(r.delivered_distinct, 1)
        self.assertAlmostEqual(r.duplicate_rate, 3.0)

    def test_unknown_records_are_counted_not_constrained(self):
        # Delivered-or-not, both conform. Neither may fail the run.
        delivered_yes = verify.check(ledger(**{"1": ("UNK", "")}), [1])
        delivered_no = verify.check(ledger(**{"1": ("UNK", "")}), [])
        self.assertTrue(delivered_yes.ok)
        self.assertTrue(delivered_no.ok)
        self.assertEqual(delivered_yes.unknown_count, 1)
        self.assertEqual(delivered_no.unknown_count, 1)

    def test_duplicate_rate_is_zero_when_nothing_delivered(self):
        r = verify.check(ledger(), [])
        self.assertEqual(r.duplicate_rate, 0.0)


class TestLogTailer(unittest.TestCase):
    def test_returns_only_newly_appended_lines(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "log")
            with open(p, "w") as f:
                f.write("a\nb\n")
            t = verify.LogTailer(p)
            self.assertEqual(t.read_new(), ["a", "b"])
            self.assertEqual(t.read_new(), [], "no new data means no lines")
            with open(p, "a") as f:
                f.write("c\n")
            self.assertEqual(t.read_new(), ["c"])

    def test_withholds_a_partial_trailing_line_until_it_completes(self):
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "log")
            with open(p, "w") as f:
                f.write("full\npar")
            t = verify.LogTailer(p)
            # "par" has no newline yet: the writer is mid-append. Consuming it
            # would corrupt the oracle with a truncated record.
            self.assertEqual(t.read_new(), ["full"])
            with open(p, "a") as f:
                f.write("tial\n")
            self.assertEqual(t.read_new(), ["partial"])

    def test_missing_file_yields_nothing_rather_than_raising(self):
        t = verify.LogTailer("/nonexistent/path/log")
        self.assertEqual(t.read_new(), [])


class TestAccumulator(unittest.TestCase):
    def test_accumulates_across_episodes_without_rereading(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], ["7 1"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.acked_count, 1)

        # Second episode adds more; earlier state must persist.
        acc.ingest(["2 S 11 21 ACK"], ["7 2"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.acked_count, 2)
        self.assertEqual(r.delivered_distinct, 2)

    def test_filters_delivered_lines_by_run_id(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], ["7 1", "9 1", "junk"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.delivered_distinct, 1, "run 9's record must be ignored")

    def test_detects_a_violation_that_spans_episodes(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], [])
        self.assertFalse(acc.check().ok, "acked but undelivered")
        # Delivery arrives late — which is legal, and the violation clears.
        acc.ingest([], ["7 1"])
        self.assertTrue(acc.check().ok)

    def test_preserves_duplicate_counts_across_episodes(self):
        acc = verify.Accumulator(delivered_run_id=7)
        acc.ingest(["1 S 10 20 ACK"], ["7 1"])
        acc.ingest([], ["7 1", "7 1"])
        r = acc.check()
        self.assertTrue(r.ok)
        self.assertEqual(r.delivered_distinct, 1)
        self.assertAlmostEqual(r.duplicate_rate, 3.0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 test_verify.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'verify'`.

- [ ] **Step 3: Write minimal implementation**

Create `chaos/orchestrator/verify.py`:

```python
"""The oracle: three invariants checked against the ledger and delivery log.

I1 - every Acked record was delivered. Set CONTAINMENT, not equality:
     at-least-once delivery makes duplicates conformant. The duplicate rate is
     measured and reported, because "your sink must dedupe" is documented but
     "how much redelivery a crash costs" is not.

I2 - no Nacked record was ever delivered. A record weir refused must not
     silently appear downstream.

I3 - Unknown records are unconstrained but counted. Either outcome conforms.
     An oracle that quietly reclassifies its awkward cases is not an oracle.

Phase 1 treats all tiers alike. Tier- and fault-aware I1 (where Buffered may
lose records under simulated power loss) arrives in Phase 2 with dm-flakey.
"""
from dataclasses import dataclass, field


@dataclass
class VerifyResult:
    """Outcome of one episode's verification."""
    ok: bool
    i1_missing: list = field(default_factory=list)
    i2_leaked: list = field(default_factory=list)
    unknown_count: int = 0
    acked_count: int = 0
    delivered_distinct: int = 0
    duplicate_rate: float = 0.0

    def summary(self):
        if self.ok:
            return (
                f"PASS  acked={self.acked_count} distinct_delivered="
                f"{self.delivered_distinct} dup_rate={self.duplicate_rate:.3f} "
                f"unknown={self.unknown_count}"
            )
        return (
            f"FAIL  I1_missing={len(self.i1_missing)} I2_leaked={len(self.i2_leaked)} "
            f"acked={self.acked_count} unknown={self.unknown_count}"
        )


class LogTailer:
    """Reads only what has been appended to a file since the last call.

    Verification runs after every episode, so re-reading the whole log each
    time is O(n^2) — millions of records re-parsed twenty times. This reads
    each byte exactly once.

    A trailing line without a newline is WITHHELD and re-read next time: the
    writer is mid-append, and consuming a truncated record would corrupt the
    oracle. A missing file yields nothing rather than raising, because the
    recorder may not have received its first batch yet.
    """

    def __init__(self, path):
        self.path = path
        self.offset = 0

    def read_new(self):
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                chunk = f.read()
        except FileNotFoundError:
            return []
        if not chunk:
            return []
        # Keep only up to the last complete line; leave the remainder for later.
        cut = chunk.rfind(b"\n")
        if cut == -1:
            return []
        complete, self.offset = chunk[: cut + 1], self.offset + cut + 1
        return complete.decode("utf-8", errors="replace").splitlines()


def parse_ledger_line(line):
    """Parses one ledger line into (seq, tag). Returns None if malformed."""
    parts = line.rstrip("\n").split(" ", 5)
    if len(parts) < 5:
        return None
    try:
        seq = int(parts[0])
    except ValueError:
        return None
    return seq, parts[4]


def parse_delivered_line(line, run_id):
    """Parses one delivery line, keeping it only if it belongs to `run_id`."""
    parts = line.split()
    if len(parts) != 2:
        return None
    try:
        r, s = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    return s if r == run_id else None


class Accumulator:
    """Accumulated verification state across episodes.

    Holds the ledger outcome per seq and the delivery count per seq, both
    growing monotonically. `check()` runs the same pure invariants as the
    standalone `check()` function on the accumulated state.
    """

    def __init__(self, delivered_run_id):
        self.run_id = delivered_run_id
        self.ledger = {}
        self.delivered_counts = {}

    def ingest(self, ledger_lines, delivered_lines):
        """Folds newly-read lines into the accumulated state."""
        for line in ledger_lines:
            parsed = parse_ledger_line(line)
            if parsed:
                seq, tag = parsed
                self.ledger[seq] = (tag, "")
        for line in delivered_lines:
            seq = parse_delivered_line(line, self.run_id)
            if seq is not None:
                self.delivered_counts[seq] = self.delivered_counts.get(seq, 0) + 1

    def check(self):
        """Runs I1/I2/I3 against everything accumulated so far."""
        delivered = []
        for seq, count in self.delivered_counts.items():
            delivered.extend([seq] * count)
        return check(self.ledger, delivered)


def check(ledger, delivered):
    """Runs I1, I2 and I3. `ledger` is {seq: (tag, reason)}, `delivered` a list
    of seq values (duplicates intact)."""
    delivered_set = set(delivered)

    acked = {s for s, (tag, _) in ledger.items() if tag == "ACK"}
    nacked = {s for s, (tag, _) in ledger.items() if tag == "NACK"}
    unknown = {s for s, (tag, _) in ledger.items() if tag == "UNK"}

    i1_missing = sorted(acked - delivered_set)
    i2_leaked = sorted(nacked & delivered_set)

    distinct = len(delivered_set)
    dup_rate = (len(delivered) / distinct) if distinct else 0.0

    return VerifyResult(
        ok=not i1_missing and not i2_leaked,
        i1_missing=i1_missing,
        i2_leaked=i2_leaked,
        unknown_count=len(unknown),
        acked_count=len(acked),
        delivered_distinct=distinct,
        duplicate_rate=dup_rate,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos/orchestrator && python3 test_verify.py`
Expected: PASS — 7 tests.

- [ ] **Step 5: Commit**

```bash
git add chaos/orchestrator/verify.py chaos/orchestrator/test_verify.py
git commit -m "feat(chaos): add I1/I2/I3 verifier with duplicate-rate measurement"
```

---

## Task 8: Episode loop, random killer, and the smoke schedule

**Files:**
- Create: `chaos/orchestrator/run.py`
- Create: `chaos/schedules/smoke.toml`

**Interfaces:**
- Consumes: `dm_stack.StorageStack` (Task 5), `quiescence.wait_for_quiescence` (Task 6), `verify.{load_ledger,load_delivered,check}` (Task 7), the `loadgen` and `recorder` binaries (Tasks 3–4).
- Produces: `runs/<run_id>/episodes.jsonl` — one JSON object per episode.

- [ ] **Step 1: Write the failing test**

Create `chaos/orchestrator/test_run.py`:

```python
"""Tests for schedule parsing and the seeded random killer.

The episode loop itself needs root and a real daemon; it is exercised by the
Task 9 end-to-end gate, not here.
"""
import unittest

import run


class TestSchedule(unittest.TestCase):
    def test_parses_the_smoke_schedule(self):
        s = run.load_schedule("../schedules/smoke.toml")
        self.assertEqual(s["seed"], 0x5EED)
        self.assertGreater(s["episodes"], 0)
        self.assertIn("kill_random", s["faults"])

    def test_run_id_is_derived_from_the_seed(self):
        # Same seed must give the same run_id, so a replay cannot collide with
        # or be confused for a different run.
        self.assertEqual(run.run_id_from_seed(0x5EED), run.run_id_from_seed(0x5EED))
        self.assertNotEqual(run.run_id_from_seed(1), run.run_id_from_seed(2))


class TestSeededKiller(unittest.TestCase):
    def test_kill_delays_are_reproducible_from_the_seed(self):
        a = run.kill_delays(seed=42, count=10, lo=1.0, hi=5.0)
        b = run.kill_delays(seed=42, count=10, lo=1.0, hi=5.0)
        c = run.kill_delays(seed=43, count=10, lo=1.0, hi=5.0)
        self.assertEqual(a, b, "same seed must reproduce the schedule")
        self.assertNotEqual(a, c)
        self.assertTrue(all(1.0 <= d <= 5.0 for d in a))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 test_run.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'run'`.

- [ ] **Step 3: Write the schedule**

Create `chaos/schedules/smoke.toml`:

```toml
# Phase 1 smoke schedule — ~30 minutes, random kills only.
#
# The Phase 1 exit criterion is ZERO FALSE VIOLATIONS, not "it ran". A chaos
# harness that cries wolf is worse than no harness on a multi-day run, so this
# schedule exists to prove the oracle is trustworthy before any real fault
# class is added.

seed = 0x5EED
episodes = 20

# Seconds of steady-state load between kills, sampled from this range.
steady_lo_secs = 45.0
steady_hi_secs = 90.0

# Seconds to wait for drain quiescence after a restart before verifying.
quiescence_timeout_secs = 120.0

[load]
threads = 8
record_size = 256
tier = "S"

[weir]
shard_count = 4
batch_size = 64
batch_deadline_ms = 2
# Small segments so rotation actually happens inside a small volume. The
# config range is [4096, 4 GiB]; 8 MiB gives frequent seals without thrashing.
wab_segment_max_bytes = 8388608

[storage]
size_mb = 512

[faults]
kill_random = true
```

- [ ] **Step 4: Write minimal implementation**

Create `chaos/orchestrator/run.py`:

```python
#!/usr/bin/env python3
"""Chaos harness episode driver. Root, Linux only.

Owns every privileged operation: the storage stack, process lifecycle, and
fault injection. The load generator and recorder run unprivileged by design —
they are the observers and must not be able to corrupt what they measure.

Phase 1 injects one fault class (random SIGKILL). The dm-flakey, dm-delay,
ENOSPC, remount and dead-letter classes land in Phases 2-3 as additional
entries in the `[faults]` table and additional branches in `inject`.

Usage: sudo python3 run.py schedules/smoke.toml
"""
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time
import tomllib

import dm_stack
import quiescence
import verify

HERE = os.path.dirname(os.path.abspath(__file__))
CHAOS_ROOT = os.path.dirname(HERE)
WEIR_ROOT = os.path.dirname(CHAOS_ROOT)


def load_schedule(path):
    """Reads a schedule TOML relative to the orchestrator directory."""
    full = path if os.path.isabs(path) else os.path.join(HERE, path)
    with open(full, "rb") as f:
        return tomllib.load(f)


def run_id_from_seed(seed):
    """Derives a run id from the schedule seed.

    Deterministic, so a replayed seed produces the same run id and its records
    can never be confused with another run's.
    """
    return (seed * 2654435761) % (2**63)


def kill_delays(seed, count, lo, hi):
    """Seeded steady-state durations between kills. Reproducible by design."""
    rng = random.Random(seed)
    return [rng.uniform(lo, hi) for _ in range(count)]


class Daemon:
    """A weir-server process under test."""

    def __init__(self, binary, wab_dir, socket_path, metrics_port, cfg):
        self.binary = binary
        self.wab_dir = wab_dir
        self.socket_path = socket_path
        self.metrics_port = metrics_port
        self.cfg = cfg
        self.proc = None

    def start(self, sink_url):
        cmd = [
            self.binary,
            "--wab-dir", self.wab_dir,
            "--socket-path", self.socket_path,
            "--metrics-port", str(self.metrics_port),
            "--sink-type", "http",
            "--sink-url", sink_url,
            "--sink-http-batch", "ndjson",
            "--shard-count", str(self.cfg["shard_count"]),
            "--batch-size", str(self.cfg["batch_size"]),
            "--batch-deadline-ms", str(self.cfg["batch_deadline_ms"]),
            "--wab-segment-max-bytes", str(self.cfg["wab_segment_max_bytes"]),
        ]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        # Wait for the socket to appear rather than sleeping a fixed interval.
        for _ in range(200):
            if os.path.exists(self.socket_path):
                return
            if self.proc.poll() is not None:
                err = self.proc.stderr.read().decode(errors="replace")
                raise RuntimeError(f"weir-server exited during startup: {err}")
            time.sleep(0.05)
        raise RuntimeError("weir-server did not create its socket within 10s")

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.kill9()

    @property
    def metrics_url(self):
        return f"http://127.0.0.1:{self.metrics_port}/metrics"


def main():
    if os.geteuid() != 0:
        sys.exit("run.py must run as root (it owns loopback devices and mounts)")

    schedule_path = sys.argv[1] if len(sys.argv) > 1 else "../schedules/smoke.toml"
    sched = load_schedule(schedule_path)
    seed = sched["seed"]
    run_id = run_id_from_seed(seed)

    run_dir = os.path.join(CHAOS_ROOT, "runs", str(run_id))
    os.makedirs(run_dir, exist_ok=True)
    # Observers write HERE — the host filesystem, outside the fault zone.
    ledger_path = os.path.join(run_dir, "ledger.log")
    delivered_path = os.path.join(run_dir, "delivered.log")
    episodes_path = os.path.join(run_dir, "episodes.jsonl")

    mount_point = "/mnt/weir-wab"
    os.makedirs(mount_point, exist_ok=True)
    socket_dir = "/run/weir-chaos"
    os.makedirs(socket_dir, mode=0o700, exist_ok=True)

    stack = dm_stack.StorageStack(
        backing_file=os.path.join(run_dir, "wab.img"),
        size_mb=sched["storage"]["size_mb"],
        mount_point=mount_point,
    )

    loadgen_bin = os.path.join(CHAOS_ROOT, "target", "release", "loadgen")
    recorder_bin = os.path.join(CHAOS_ROOT, "target", "release", "recorder")
    weir_bin = os.path.join(WEIR_ROOT, "target", "release", "weir-server")
    for b in (loadgen_bin, recorder_bin, weir_bin):
        if not os.path.exists(b):
            sys.exit(f"missing binary: {b} — build it first")

    recorder = None
    loadgen = None
    daemon = None
    violations = 0

    try:
        stack.setup()

        recorder = subprocess.Popen(
            [recorder_bin, "--bind", "127.0.0.1:9900", "--log", delivered_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(0.5)

        daemon = Daemon(
            weir_bin, mount_point, os.path.join(socket_dir, "weir.sock"),
            9185, sched["weir"],
        )
        daemon.start("http://127.0.0.1:9900/ingest")

        delays = kill_delays(
            seed, sched["episodes"], sched["steady_lo_secs"], sched["steady_hi_secs"]
        )
        # The load must outlast the episodes. Steady-state sleeps are only part
        # of the wall clock: each episode also spends time on restart,
        # quiescence polling, and verification. Budget the worst case per
        # episode rather than a flat margin — if load stops early, the final
        # episodes verify an idle daemon and PASS vacuously, which is precisely
        # the false-confidence Phase 1 exists to rule out.
        per_episode_overhead = sched["quiescence_timeout_secs"] + 30
        total_secs = int(sum(delays) + per_episode_overhead * len(delays))

        loadgen = subprocess.Popen([
            loadgen_bin,
            "--socket", daemon.socket_path,
            "--ledger", ledger_path,
            "--run-id", str(run_id),
            "--threads", str(sched["load"]["threads"]),
            "--record-size", str(sched["load"]["record_size"]),
            "--tier", sched["load"]["tier"],
            "--duration-secs", str(total_secs),
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Read each log byte exactly once across the whole run.
        ledger_tail = verify.LogTailer(ledger_path)
        delivered_tail = verify.LogTailer(delivered_path)
        acc = verify.Accumulator(delivered_run_id=run_id)

        with open(episodes_path, "a") as ep_log:
            for i, delay in enumerate(delays):
                time.sleep(delay)

                # A dead load generator makes every subsequent verification
                # vacuous: an idle daemon trivially satisfies I1 and I2. Assert
                # liveness BEFORE the fault so a PASS always means something.
                if loadgen.poll() is not None:
                    print(
                        f"episode {i:3d}  ABORT — load generator exited "
                        f"(code {loadgen.returncode}) before the fault; "
                        "remaining episodes would pass vacuously",
                        flush=True,
                    )
                    violations += 1
                    ep_log.write(json.dumps({
                        "episode": i, "fault": "kill_random", "ok": False,
                        "quiesced": False, "abort_reason": "loadgen_exited",
                        "loadgen_exit_code": loadgen.returncode, "seed": seed,
                    }) + "\n")
                    ep_log.flush()
                    break

                daemon.kill9()
                daemon.start("http://127.0.0.1:9900/ingest")

                ok, reason = quiescence.wait_for_quiescence(
                    daemon.metrics_url, sched["quiescence_timeout_secs"]
                )

                # Give the recorder a moment to finish its final append.
                time.sleep(1.0)
                acc.ingest(ledger_tail.read_new(), delivered_tail.read_new())
                result = acc.check()

                if not result.ok or not ok:
                    violations += 1

                record = {
                    "episode": i,
                    "fault": "kill_random",
                    "steady_secs": round(delay, 2),
                    "quiesced": ok,
                    "quiescence_note": reason,
                    "ok": result.ok,
                    "acked": result.acked_count,
                    "delivered_distinct": result.delivered_distinct,
                    "duplicate_rate": round(result.duplicate_rate, 4),
                    "unknown": result.unknown_count,
                    "i1_missing": result.i1_missing[:50],
                    "i2_leaked": result.i2_leaked[:50],
                    "seed": seed,
                }
                ep_log.write(json.dumps(record) + "\n")
                ep_log.flush()

                status = result.summary()
                print(f"episode {i:3d}  quiesced={ok}  {status}", flush=True)
                if not result.ok:
                    print(
                        f"  REPRODUCER: sudo python3 run.py {schedule_path}  "
                        f"# seed={hex(seed)} episode={i}",
                        flush=True,
                    )
    finally:
        for p in (loadgen, recorder):
            if p and p.poll() is None:
                p.terminate()
        if daemon:
            daemon.stop()
        stack.teardown()

    print(f"\nrun {run_id} complete: {violations} violation(s) across {sched['episodes']} episodes")
    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd chaos/orchestrator && python3 test_run.py`
Expected: PASS — 3 tests.

- [ ] **Step 6: Commit**

```bash
git add chaos/orchestrator/run.py chaos/orchestrator/test_run.py chaos/schedules/smoke.toml
git commit -m "feat(chaos): add episode loop, seeded random killer, smoke schedule"
```

---

## Task 9: The report, and the Phase 1 exit gate

**Files:**
- Create: `chaos/orchestrator/report.py`
- Modify: `chaos/README.md`

**Interfaces:**
- Consumes: `runs/<run_id>/episodes.jsonl` from Task 8.
- Produces: `report.render(episodes, meta) -> str` (markdown), and a CLI writing `runs/<run_id>/report.md`.

- [ ] **Step 1: Write the failing test**

Create `chaos/orchestrator/test_report.py`:

```python
"""Tests for report rendering."""
import unittest

import report

EPISODES = [
    {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
     "acked": 1000, "delivered_distinct": 1000, "duplicate_rate": 1.02,
     "unknown": 3, "i1_missing": [], "i2_leaked": [], "seed": 24301},
    {"episode": 1, "fault": "kill_random", "ok": False, "quiesced": True,
     "acked": 2000, "delivered_distinct": 1998, "duplicate_rate": 1.01,
     "unknown": 5, "i1_missing": [17, 42], "i2_leaked": [], "seed": 24301},
]


class TestRender(unittest.TestCase):
    def test_headline_states_the_violation_count(self):
        out = report.render(EPISODES, {"weir_commit": "abc123", "kernel": "6.8.0"})
        self.assertIn("1 violation", out)
        self.assertIn("abc123", out)
        self.assertIn("6.8.0", out)

    def test_violations_are_listed_with_reproducers(self):
        out = report.render(EPISODES, {})
        self.assertIn("episode 1", out)
        self.assertIn("17", out)
        self.assertIn("seed", out.lower())

    def test_clean_run_says_so_plainly(self):
        out = report.render([EPISODES[0]], {})
        self.assertIn("0 violations", out)

    def test_duplicate_rate_is_reported(self):
        out = report.render(EPISODES, {})
        self.assertIn("Duplicate rate", out)

    def test_empty_run_does_not_crash(self):
        out = report.render([], {})
        self.assertIn("0 episodes", out)

    def test_an_aborted_run_says_so_prominently(self):
        # A vacuous-pass guard firing must never be buried in a table cell.
        aborted = [
            EPISODES[0],
            {"episode": 1, "fault": "kill_random", "ok": False, "quiesced": False,
             "abort_reason": "loadgen_exited", "loadgen_exit_code": 1, "seed": 24301},
        ]
        out = report.render(aborted, {})
        self.assertIn("aborted early", out)
        self.assertIn("loadgen_exited", out)
        self.assertIn("absent, not passing", out)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd chaos/orchestrator && python3 test_report.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'report'`.

- [ ] **Step 3: Write minimal implementation**

Create `chaos/orchestrator/report.py`:

```python
#!/usr/bin/env python3
"""Renders an episode log into a markdown report.

Phase 1 reports the essentials: what ran, what broke, and the duplicate rate.
Latency plots, resource curves and the tier x fault matrix arrive in Phases
2-4 as those measurements start existing.

A report that lists only successes is marketing. Limitations are a required
section, not an optional one.
"""
import json
import os
import sys


def render(episodes, meta):
    """Renders episodes (list of dicts) into markdown."""
    total = len(episodes)
    violations = [e for e in episodes if not e.get("ok", True)]
    unquiesced = [e for e in episodes if not e.get("quiesced", True)]

    lines = []
    lines.append("# weir chaos run — Phase 1 (spine)\n")

    lines.append("## Run metadata\n")
    for key in ("weir_commit", "kernel", "hardware", "filesystem", "seed", "duration"):
        if meta.get(key):
            lines.append(f"- **{key.replace('_', ' ').title()}:** {meta[key]}")
    lines.append("")

    verdict = f"{len(violations)} violation" + ("" if len(violations) == 1 else "s")
    lines.append("## Result\n")
    lines.append(f"**{total} episodes, {verdict}.**\n")
    if unquiesced:
        lines.append(
            f"{len(unquiesced)} episode(s) did not reach drain quiescence within "
            "the timeout. A drain that never quiesces is itself a finding.\n"
        )

    aborted = [e for e in episodes if e.get("abort_reason")]
    if aborted:
        lines.append(
            f"**Run aborted early at episode {aborted[0].get('episode')}: "
            f"`{aborted[0]['abort_reason']}`.** The run stopped because "
            "continuing would have produced passing episodes that verified an "
            "idle daemon. Every episode after this point is absent, not passing.\n"
        )

    if episodes:
        acked = sum(e.get("acked", 0) for e in episodes)
        distinct = sum(e.get("delivered_distinct", 0) for e in episodes)
        unknown = sum(e.get("unknown", 0) for e in episodes)
        rates = [e.get("duplicate_rate", 0.0) for e in episodes if e.get("duplicate_rate")]
        avg_dup = sum(rates) / len(rates) if rates else 0.0
        lines.append("## Totals\n")
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        lines.append(f"| Acked records | {acked} |")
        lines.append(f"| Distinct delivered | {distinct} |")
        lines.append(f"| Unknown (indeterminate) | {unknown} |")
        lines.append(f"| Duplicate rate (mean) | {avg_dup:.3f} |")
        lines.append("")
        lines.append(
            "Duplicate rate is delivered-over-distinct. At-least-once delivery makes "
            "duplicates conformant; this is what a crash actually costs a sink that "
            "must dedupe.\n"
        )

    lines.append("## Episodes\n")
    lines.append("| # | Fault | Quiesced | Verdict | Acked | Distinct | Dup rate | Unknown |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for e in episodes:
        lines.append(
            f"| {e.get('episode')} | {e.get('fault', '?')} | "
            f"{'yes' if e.get('quiesced') else 'NO'} | "
            f"{'PASS' if e.get('ok') else '**FAIL**'} | {e.get('acked', 0)} | "
            f"{e.get('delivered_distinct', 0)} | {e.get('duplicate_rate', 0.0):.3f} | "
            f"{e.get('unknown', 0)} |"
        )
    lines.append("")

    if violations:
        lines.append("## Violations\n")
        for e in violations:
            lines.append(f"### episode {e.get('episode')} — {e.get('fault', '?')}\n")
            if e.get("i1_missing"):
                lines.append(
                    f"**I1 — acked but never delivered** ({len(e['i1_missing'])} shown, "
                    f"truncated at 50): `{e['i1_missing']}`\n"
                )
            if e.get("i2_leaked"):
                lines.append(
                    f"**I2 — nacked but delivered** ({len(e['i2_leaked'])}): "
                    f"`{e['i2_leaked']}`\n"
                )
            lines.append(f"Reproducer: seed `{hex(e.get('seed', 0))}`, episode {e.get('episode')}\n")

    lines.append("## Limitations\n")
    lines.append(
        "- Phase 1 injects **random SIGKILL only**. Targeted mid-fsync kills, power "
        "loss, torn writes, disk-full, slow disk, read-only remount and dead-letter "
        "exhaustion are Phases 2-3 and are **not** covered by this run.\n"
        "- Invariant I1 is **not yet tier-aware**: all tiers are held to zero loss, "
        "which is correct for process-crash but will need relaxing for Buffered "
        "under power loss in Phase 2.\n"
        "- The seed reproduces the **schedule**, not the exact interleaving. Real "
        "kernel, real timing, real I/O — full determinism is not claimed.\n"
        "- Single host, single filesystem, single hardware configuration.\n"
    )
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: report.py <run_dir>")
    run_dir = sys.argv[1]
    episodes = []
    ep_path = os.path.join(run_dir, "episodes.jsonl")
    if os.path.exists(ep_path):
        with open(ep_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    episodes.append(json.loads(line))

    meta = {}
    try:
        import subprocess
        meta["weir_commit"] = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        meta["kernel"] = subprocess.run(
            ["uname", "-r"], capture_output=True, text=True
        ).stdout.strip()
    except Exception:
        pass
    if episodes:
        meta["seed"] = hex(episodes[0].get("seed", 0))

    out_path = os.path.join(run_dir, "report.md")
    with open(out_path, "w") as f:
        f.write(render(episodes, meta))
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd chaos/orchestrator && python3 test_report.py`
Expected: PASS — 5 tests.

- [ ] **Step 5: Run the full unit suite**

```bash
cd chaos && cargo test
cd orchestrator && for t in test_*.py; do python3 "$t" -q || exit 1; done
```
Expected: all Rust tests pass; all Python suites pass (the root-only dm_stack test skips off-Linux).

- [ ] **Step 6: Document the exit gate in the README**

Append to `chaos/README.md`:

```markdown
## Phase 1 exit gate

Phase 1 is complete when a 30-minute smoke run produces **zero violations**:

```bash
# From the weir repo root, build both sides first.
cargo build --release -p weir-server
cd chaos && cargo build --release
sudo python3 orchestrator/run.py schedules/smoke.toml
python3 orchestrator/report.py runs/<run_id>
```

Exit code 0 and `0 violations` in the report.

**The gate is zero FALSE violations, not "it ran".** Any violation at this
stage is far more likely to be a harness bug than a weir bug — Phase 1 injects
only random SIGKILL, which weir's existing system tests already cover. Treat a
Phase 1 violation as an oracle defect until proven otherwise: check that the
recorder fsynced before its 200, that quiescence really settled, and that no
record was NDJSON-dead-lettered for containing a newline.

A harness that cries wolf is worse than no harness on a multi-day run.
```

- [ ] **Step 7: Commit**

```bash
git add chaos/orchestrator/report.py chaos/orchestrator/test_report.py chaos/README.md
git commit -m "feat(chaos): add markdown report renderer and Phase 1 exit gate"
```

---

## Task 10: End-to-end gate on the target box

**Files:**
- Modify: `chaos/README.md` (record the result)

This task runs only on the Linux i9 box. It has no unit test — it *is* the test.

**Interfaces:**
- Consumes: everything from Tasks 1–9.
- Produces: a committed run report demonstrating the gate.

- [ ] **Step 1: Verify prerequisites on the box**

```bash
uname -r                          # expect >= 5.3
which losetup dmsetup mkfs.ext4   # all three present
python3 --version                 # expect >= 3.11 (tomllib is stdlib from 3.11)
```
Expected: all present. If `python3 < 3.11`, `tomllib` is unavailable — install `tomli` or upgrade; note which in the README.

- [ ] **Step 2: Build both sides**

```bash
cd <weir-repo> && cargo build --release -p weir-server
cd chaos && cargo build --release
```
Expected: both succeed.

- [ ] **Step 3: Run a 2-episode shakeout before the full smoke**

Temporarily set `episodes = 2` and `steady_lo_secs = 10.0` / `steady_hi_secs = 15.0` in a copy of the schedule:

```bash
sed -e 's/^episodes = 20/episodes = 2/' \
    -e 's/^steady_lo_secs = 45.0/steady_lo_secs = 10.0/' \
    -e 's/^steady_hi_secs = 90.0/steady_hi_secs = 15.0/' \
    schedules/smoke.toml > schedules/shakeout.toml
sudo python3 orchestrator/run.py schedules/shakeout.toml
```
Expected: exit 0, `0 violation(s)`. If violations appear, debug per the README's exit-gate guidance — treat as an oracle defect first.

- [ ] **Step 4: Run the full smoke schedule**

```bash
sudo python3 orchestrator/run.py schedules/smoke.toml
```
Expected: ~30 minutes, exit 0, `0 violation(s) across 20 episodes`.

- [ ] **Step 5: Generate and read the report**

```bash
python3 orchestrator/report.py runs/<run_id>
cat runs/<run_id>/report.md
```
Expected: a report with 20 PASS rows, a non-zero mean duplicate rate, and the Limitations section intact.

**Read the duplicate rate.** It is the first genuinely new fact this harness produces about weir — the README tells integrators their sink must dedupe but has never said how much redelivery a crash actually costs.

- [ ] **Step 6: Record the result and commit**

Append the headline numbers (episodes, violations, mean duplicate rate, kernel, weir commit) to `chaos/README.md` under a `## First run` heading, then:

```bash
git add chaos/README.md
git commit -m "docs(chaos): record Phase 1 exit-gate run result"
```

- [ ] **Step 7: Clean up the shakeout schedule**

```bash
rm -f chaos/schedules/shakeout.toml
```

---

## Phase 1 Done When

- `chaos/` builds and its unit tests pass on any machine (dm_stack's root test skipping off-Linux).
- A 30-minute smoke run on the i9 box exits 0 with zero violations.
- A report exists with a measured duplicate rate and an honest Limitations section.
- The oracle is trusted well enough to start adding real faults in Phase 2.
