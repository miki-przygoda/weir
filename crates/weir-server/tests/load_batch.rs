//! Wire-batching (A6) throughput for weir-server — `push_batch` against `push`.
//!
//! `tests/load.rs` measures one record per round trip; `tests/load_drain.rs`
//! measures the delivery side. Neither measures the thing A6 shipped, so the
//! only number the project had for wire batching was a model:
//!
//! > **~70,000 rec/s, roughly 100× over 653, ±40%** — and refuse a second
//! > significant figure until measured.
//! > — `docs/superpowers/specs/2026-09-13-a6-wire-batching-design.md` §9
//!
//! and §12 recorded why it stayed a model: *"no A6 throughput number has been
//! published, because the beast run that would justify one has not happened."*
//! This file is that run's harness.
//!
//! # Running locally
//!
//! ```sh
//! cargo test -p weir-server --test load_batch --release -- --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` is not optional. Every scenario times a whole daemon
//! against a wall clock, so two running at once measure each other's
//! contention rather than weir's throughput.
//!
//! # Why this is not in the CI `load` job
//!
//! `docs/benchmarks/environments.md` splits the surfaces: CI catches
//! order-of-magnitude regressions on a shared 2-vCPU runner, and any number a
//! claim may cite has to come from an operator-run capture on named hardware.
//! An A6 figure is the second kind — a 2-vCPU runner cannot distinguish 70k
//! rec/s from 40k — so this file is deliberately absent from the explicit
//! `--test` lists in `.github/workflows/ci.yml` and runs when an operator runs
//! it. That also keeps it out of `deploy/avg_benchmarks.py`'s averages, whose
//! scenario allowlist describes the ingest gate, not this.
//!
//! Being unrun in CI is not being unguarded: the `lint` job's
//! `cargo clippy --all-targets -- -D warnings` compiles every test target on
//! every push, so this file cannot rot against an API change without failing
//! the build. What CI does not do is *execute* it, which is the part that would
//! cost a minute per push to produce a number no claim may cite.
//!
//! # What the numbers mean
//!
//! Most scenarios push `TOTAL_RECORDS` 5-byte records down **one connection**
//! and time the whole transfer, so a rate is *records acked per second at one
//! producer*; the two `concurrent_*` scenarios instead spread the same total
//! across N connections on one shard and report N in `threads`. The 5-byte
//! payload matches `load.rs`'s
//! `baseline_single_thread_throughput_sync`, which is what makes
//! `push_single_durable` below a same-box, same-build, same-payload control for
//! the batched rows — the comparison `environments.md` requires and the one
//! earlier notes got wrong by reaching for a figure from another machine.
//!
//! `records_per_fsync` is read from the `weir_wab_group_commit_records`
//! histogram across the scenario, not modelled. It is the amortisation A6
//! exists to buy: `push` cannot raise it above ~1.0 on a serial connection
//! (each record is its own group commit), and `push_batch` should drive it to
//! `min(batch_n, batch_size)`. A batched row whose `records_per_fsync` is ~1.0
//! is a batched row that did not batch, and its rate means nothing.
//!
//! # Coverage caveats (known gaps)
//!
//! - **No concurrent *batching* producers.** A6's case is single-producer
//!   burst handoff (§8), so that is what the batched rows measure. The
//!   `concurrent_*` rows run concurrently but unbatched, because their job is
//!   to price the alternative A6 has to beat. Many producers each sending
//!   `push_batch` is measured nowhere, and the fsync amortisation would be
//!   shared differently if it were.
//! - **Noop sink.** Like `load.rs`, this is an ingest ceiling. §7.5 is explicit
//!   that on shipped defaults the end-to-end win is 1.0× — batching the wire
//!   does not speed up a per-record HTTP sink. Do not quote a row here as an
//!   end-to-end rate.
//! - **No drain pressure.** The WAB fills and is never drained against a real
//!   endpoint during the window. §8's warning — one drain thread, ingest now
//!   ~100× faster, `wab_max_bytes` defaulting to 0 — is a sizing concern this
//!   file measures the numerator of and nothing else.

#![cfg(unix)]

use std::time::{Duration, Instant};

use weir_core::Durability;
use weir_testkit::weir_server;

/// Records per scenario. Large enough that connection and daemon startup are
/// noise against the timed window, small enough that the unbatched `Durable`
/// control — the slowest row by two orders of magnitude — still finishes in
/// seconds rather than minutes.
const TOTAL_RECORDS: usize = 10_000;

/// Server-side group-commit batch size, fixed at the value §9 modelled so the
/// measurement answers the model that was actually published. `bench_preset`
/// would otherwise supply 64, which §9 costed separately as the "beast-native
/// cross-check at 64 records/fsync".
const GROUP_BATCH: usize = 256;

/// The 5-byte payload `load.rs` uses, so its `single_thread_sync` row and this
/// file's control are the same experiment at a different frame size.
const PAYLOAD: &[u8] = b"bench";

// ── Result reporting ───────────────────────────────────────────────────────
//
// Same `BENCH: {json}` line shape as tests/load.rs. `batch_records` is the
// wire batch size (1 for the unbatched control) and `records_per_fsync` is
// measured, so a published row carries the amortisation that explains its rate
// instead of needing this file re-read to interpret it.

fn emit(
    scenario: &str,
    threads: usize,
    batch_records: usize,
    total: usize,
    elapsed: Duration,
    per_fsync: f64,
) {
    let rps = total as f64 / elapsed.as_secs_f64();
    // Buffered acks before any fsync, so it is covered by no group commit and
    // the ratio is 0/0. `null` says "not applicable here"; a 0 would read as
    // "measured, and nothing was amortised", which is a different claim.
    let per_fsync = if per_fsync.is_finite() {
        format!("{per_fsync:.2}")
    } else {
        "null".to_string()
    };
    // Leading newline, deliberately. This file mandates `--test-threads=1`, and
    // under it libtest writes `test <name> ... ` with no trailing newline before
    // handing control to the test -- so an unprefixed line emits as
    // `test push_single_durable ... BENCH: {...}` and the project-wide
    // `grep '^BENCH: '` (deploy/run_bare_metal_bench.sh, the CI drain job,
    // avg_benchmarks.py) silently matches nothing. tests/load.rs never hit this
    // because nothing runs it single-threaded.
    println!(
        "\nBENCH: {{\"scenario\":\"{scenario}\",\"threads\":{threads},\
         \"batch_records\":{batch_records},\"total_records\":{total},\
         \"wall_ms\":{},\"throughput_rps\":{},\"records_per_fsync\":{per_fsync}}}",
        elapsed.as_millis(),
        rps as u64,
    );
}

fn parse_metric_f64(body: &str, prefix: &str) -> f64 {
    for line in body.lines() {
        if line.starts_with(prefix)
            && let Some(val) = line.split_whitespace().next_back()
            && let Ok(n) = val.parse()
        {
            return n;
        }
    }
    f64::NAN
}

// ── Scenario runner ────────────────────────────────────────────────────────

/// Runs one scenario end to end and emits its row.
///
/// `batch_n == 1` takes the `push` path rather than a one-record `push_batch`,
/// because the control has to be the API a producer would actually use for one
/// record — a `push_batch` of one measures A6's framing overhead, not the
/// baseline A6 claims to beat.
fn run(scenario: &str, tag: &str, batch_n: usize, durability: Durability) {
    let srv = weir_server!(tag)
        .bench_preset()
        .batch_size(GROUP_BATCH)
        .start();
    let mut client = srv.client();

    let fsync_stats = |body: &str| -> (f64, f64) {
        (
            parse_metric_f64(body, "weir_wab_group_commit_records_sum"),
            parse_metric_f64(body, "weir_wab_group_commit_records_count"),
        )
    };
    let (sum_before, count_before) = fsync_stats(&srv.scrape_metrics());

    let t0 = Instant::now();
    if batch_n == 1 {
        for _ in 0..TOTAL_RECORDS {
            client.push(PAYLOAD, durability).expect("push");
        }
    } else {
        let batch: Vec<&[u8]> = vec![PAYLOAD; batch_n];
        let full = TOTAL_RECORDS / batch_n;
        for _ in 0..full {
            let out = client.push_batch(&batch, durability).expect("push_batch");
            assert!(
                out.all_accepted(),
                "{scenario}: a record was not accepted; a throughput number over a \
                 partially-rejected batch is a rate for work that did not happen"
            );
        }
        let rest = TOTAL_RECORDS % batch_n;
        if rest > 0 {
            let out = client
                .push_batch(&batch[..rest], durability)
                .expect("push_batch remainder");
            assert!(
                out.all_accepted(),
                "{scenario}: remainder batch not accepted"
            );
        }
    }
    let elapsed = t0.elapsed();

    let (sum_after, count_after) = fsync_stats(&srv.scrape_metrics());
    let per_fsync = (sum_after - sum_before) / (count_after - count_before);

    emit(scenario, 1, batch_n, TOTAL_RECORDS, elapsed, per_fsync);
    srv.shutdown();
}

// ── Scenarios ──────────────────────────────────────────────────────────────

/// The control: one `Durable` record per round trip, one connection.
///
/// This is the denominator of every "N×" claim A6 makes. §9 quoted 653 rec/s
/// for it; that figure came from a different machine and a different build, so
/// it is re-measured here rather than reused.
#[test]
fn push_single_durable() {
    run("batch_single_durable", "b_sing_dur", 1, Durability::Durable);
}

/// The sweep. 16 and 64 sit below `GROUP_BATCH`, so their amortisation is
/// bounded by the wire batch; 256 matches it exactly; 1024 exceeds it, where
/// the group commit — not the frame — becomes the limit, and
/// `records_per_fsync` should pin to `GROUP_BATCH` rather than rise with the
/// batch. That last row is the one that says whether a bigger frame still buys
/// anything, which is what `max_batch_records`'s default of 1024 rests on.
#[test]
fn push_batch_durable_n16() {
    run("batch_durable_n16", "b_dur_n16", 16, Durability::Durable);
}

#[test]
fn push_batch_durable_n64() {
    run("batch_durable_n64", "b_dur_n64", 64, Durability::Durable);
}

#[test]
fn push_batch_durable_n256() {
    run("batch_durable_n256", "b_dur_n256", 256, Durability::Durable);
}

#[test]
fn push_batch_durable_n1024() {
    run(
        "batch_durable_n1024",
        "b_dur_n1k",
        1024,
        Durability::Durable,
    );
}

/// Batched `Buffered`, which §9 flagged as never measured while simultaneously
/// correcting the claim that `Buffered`'s 49,725 rec/s was a ceiling — it is a
/// latency reciprocal at one round trip per record, and batching removes the
/// round trip it is the reciprocal of. Whether the `Durable` batched rate can
/// exceed it is the question; this row is the other side of that comparison,
/// measured on the same box in the same run.
#[test]
fn push_batch_buffered_n256() {
    run(
        "batch_buffered_n256",
        "b_buf_n256",
        256,
        Durability::Buffered,
    );
}

/// The unbatched `Buffered` control, so the pair above is a ratio and not a
/// single number needing another file to interpret.
#[test]
fn push_single_buffered() {
    run(
        "batch_single_buffered",
        "b_sing_buf",
        1,
        Durability::Buffered,
    );
}

// ── Concurrent unbatched producers ─────────────────────────────────────────

/// `producers` threads pushing `Durable` down their own connections, all on one
/// shard, unbatched.
///
/// This is the configuration in which `push` amortises *without* A6, and it is
/// the measurement A6's premise had to survive: if enough concurrent producers
/// already drive `records_per_fsync` near `batch_size`, wire batching buys
/// nothing they do not already have. §1 established that coalescing begins only
/// once connections-per-shard exceeds 1 — hence `shard_count(1)`, which makes
/// every producer contend for the same group commit rather than spreading
/// across four and coalescing nothing.
fn run_concurrent(scenario: &str, tag: &str, producers: usize) {
    use std::{sync::Barrier, thread};

    let srv = weir_server!(tag)
        .bench_preset()
        .shard_count(1)
        .batch_size(GROUP_BATCH)
        .start();

    let fsync_stats = |body: &str| -> (f64, f64) {
        (
            parse_metric_f64(body, "weir_wab_group_commit_records_sum"),
            parse_metric_f64(body, "weir_wab_group_commit_records_count"),
        )
    };
    let (sum_before, count_before) = fsync_stats(&srv.scrape_metrics());

    let each = TOTAL_RECORDS / producers;
    // Connections are opened and clients built before the barrier, so the timed
    // window holds pushing and nothing else. Setup inside the window is what
    // makes `load.rs`'s `thundering_herd_*` scenarios swing ~8x run to run.
    let barrier = Barrier::new(producers);
    let elapsed = thread::scope(|s| {
        let handles: Vec<_> = (0..producers)
            .map(|_| {
                let barrier = &barrier;
                let srv = &srv;
                s.spawn(move || {
                    let mut client = srv.client();
                    barrier.wait();
                    let t0 = Instant::now();
                    for _ in 0..each {
                        client.push(PAYLOAD, Durability::Durable).expect("push");
                    }
                    t0.elapsed()
                })
            })
            .collect();
        // The slowest producer sets the wall clock: the run is not finished
        // until every record is acked, so taking the max is the only reading
        // under which `total / elapsed` is a rate the daemon actually sustained.
        handles
            .into_iter()
            .map(|h| h.join().expect("producer thread"))
            .max()
            .expect("at least one producer")
    });

    let (sum_after, count_after) = fsync_stats(&srv.scrape_metrics());
    let per_fsync = (sum_after - sum_before) / (count_after - count_before);

    emit(scenario, producers, 1, each * producers, elapsed, per_fsync);
    srv.shutdown();
}

/// The figure §12 published from macOS — 6.40 records per fsync at 8 producers
/// on 1 shard, against a gate of `0.6 × 256 ≈ 154` — re-measured here because
/// that number was explicitly recorded as a **lower bound**: a slower fsync
/// holds the group-commit window open longer and lets more records into it, and
/// beast's `fdatasync` is ~6x slower than the `F_BARRIERFSYNC` the macOS figure
/// was taken on. If concurrency alone were going to reach the gate anywhere, it
/// would reach it here.
#[test]
fn concurrent_durable_8p_1shard() {
    run_concurrent("batch_concurrent_8p_1shard", "b_c8", 8);
}

/// Four times the producers, to say whether the amortisation is *approaching* a
/// ceiling or merely climbing slowly. A single point cannot distinguish "8
/// producers is not enough" from "concurrency does not get you there", and that
/// distinction is the whole of A6's premise.
#[test]
fn concurrent_durable_32p_1shard() {
    run_concurrent("batch_concurrent_32p_1shard", "b_c32", 32);
}
