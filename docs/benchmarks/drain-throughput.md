# Drain throughput — the first delivery-side numbers

**Date:** 2026-09-02 · **Version:** 2.0.3 · **Suite:** `crates/weir-server/tests/load_drain.rs`

Until this release every published weir number described **ingest**: how fast the
daemon accepts records and gets them into the WAB. The load suite's own module
doc said so, under "Coverage caveats", and named this file's suite as the
follow-up.

That gap mattered because weir is a *buffer*. Ingest throughput says how fast the
buffer fills. Nothing said how fast it empties, which is the half that decides
whether it ever does.

These are the first delivery numbers the project has.

## Environment

| | |
|---|---|
| Machine | Apple M3 Max, 16 cores |
| OS | macOS 26.6.2 |
| Toolchain | rustc 1.97.1, `--release` |
| Sink | in-process HTTP/1.1 mock on loopback, answers in microseconds |
| Records | 256 bytes, `Durability::Buffered` |
| Segments | 64 KiB (`wab_segment_max_bytes`), 1 s idle seal |
| Daemon | `bench_preset`: 4 shards, 4 workers, batch 64 |

Numbers below are the **median of three consecutive runs**; the spread was under
±8% on every scenario.

## Results

| Scenario | Records | Median | Range over 3 runs |
|---|---:|---:|---|
| `drain_http_ndjson` — NDJSON batch framing | 1,500 | **7,457 rec/s** | 7,234 – 7,481 |
| `drain_http_per_record` — one POST per record (default) | 1,500 | **5,961 rec/s** | 5,695 – 6,674 |
| `drain_slow_sink_1ms_conc16` — 1 ms sink, concurrency 16 | 750 | **5,130 rec/s** | 5,005 – 5,236 |
| `ingest_during_sink_outage` — ingest while the sink is down | 2,000 | **~14,000 rec/s** | 133 – 166 ms total |

Every scenario measures a **known backlog**. The sink refuses with 503 during the
fill, so the buffer is full before timing starts and the measurement begins at a
real event — the sink answering. The first version of this suite simply filled
and then timed whatever was left, which for the faster modes was nothing at all:
it reported a `wall_ms` of 0 and a rate of nine billion.

## What the numbers say

**NDJSON batching wins about 1.25×, not an order of magnitude.** That is less
than the feature's framing suggests, and it is the most useful thing here. The
mock answers in microseconds, so per-request overhead is nearly all that batching
removes. The win should grow roughly with the sink's round-trip time, and against
a real endpoint across a network it should be far larger — but that is a
prediction, and this file does not measure it. **Do not quote 1.25× as the
value of NDJSON in production.**

**Concurrency is real but sub-linear.** A sink taking 1 ms per response caps
serial delivery at ~1,000 rec/s. weir sustained 5,130 rec/s at concurrency 16 —
so requests genuinely overlap, at roughly 5× rather than 16×. This is the
scenario that would catch a regression turning the HTTP sink serial, which no
ingest benchmark can see.

**Ingest is untouched by a total sink outage.** 2,000 records were accepted in
133–166 ms while every single delivery attempt was being refused. That
separation is weir's entire proposition and it is now asserted, not assumed.

**Delivery is the narrower half.** On the same machine, ingest through one client
thread ran roughly 2× the delivery rate, and `tests/load.rs`'s ingest baselines
are higher still. A weir deployment whose producers outrun ~6,000 rec/s to an
HTTP sink will grow its WAB, and `wab_max_bytes` — a *soft*, five-second-sampled
cap — is what stands between that and a full disk.

## What these numbers are not

- **Loopback only.** No network latency, no TLS to the sink, no DNS. A real HTTP
  sink over a WAN pays an RTT per batch that dominates everything here.
- **One sink type.** HTTP only. The SQL sinks batch very differently.
- **Not steady state.** Each scenario fills, then drains. Behaviour where ingest
  and drain run concurrently at matched rates is still unmeasured.
- **Not a gate.** The suite asserts correctness — including that no record is
  lost between the WAB and the sink across an outage — but the rates are reported
  for tracking, not enforced. Failing a throughput threshold on shared CI
  hardware produces flakes, not signal.
