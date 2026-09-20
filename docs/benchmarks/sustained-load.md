# Sustained load — what `Durable` does over hours, not seconds

Every `Durable` throughput figure weir publishes is a short run. `tests/load.rs`
pushes 500 records; `tests/load_batch.rs` pushes 10,000. Both finish in seconds,
both start against a disk that has been idle, and both report a number weir
**does not sustain**.

On beast, one producer at `Durable` measures **877 rec/s** in a short run and
**352 rec/s** averaged over four and a half hours. The gap is not drift, wear or
thermal throttling. It is an oscillation, and it belongs to the storage rather
than to weir.

## The shape

Throughput alternates between two discrete states and spends most of its time in
the slower one. Sampled every 5 s for 40 minutes:

| | fsync latency | throughput | share of time |
|---|---:|---:|---:|
| FAST | 1,210 µs | ~880 rec/s | 24% |
| SLOW | 5,557 µs | ~179 rec/s | **76%** |

There is almost nothing in between: the fsync range is 1,063–5,765 µs and the
distribution is two clusters, not a spread. The 4.6x latency ratio is the 4.9x
throughput ratio.

The cycle is regular. Across eight consecutive cycles the period is
**272 s mean, 270 s median, sd 10.3 s** (255–290 s). The FAST window is
strikingly fixed at **60–65 s**; all the variance lives in the SLOW window
(190–230 s).

```
FFSSSSSSSFFSSSSSSSFFSSSSSSFFFSSSSSSFFFSSSSSSFFFSSSSS   (30s per character)
```

## It is steady state, not decay

This matters for how a deployment should read it: the oscillation does not get
worse, and it does not recover while under load.

| run | duration | mean rec/s | slow share | quarterly means |
|---|---|---:|---:|---|
| 30 s samples | 90 min | 354 | 68% | 342 / 349 / 352 / 355 |
| 60 s samples | 179 min | 350 | 60% | 338 / 350 / 356 / 360 |

Four and a half hours, no downward trend — the 3-hour run's quarters rise
slightly. An operator sizing for `Durable` should use **~350 rec/s per producer
on this hardware**, and treat 877 as the cold-start number it is.

## It is the storage, not weir

Three controls, each removing one thing:

| configuration | mean rec/s | throughput spread | fsync |
|---|---:|---:|---|
| `Durable`, ext4 on SATA SSD (beast) | 352 | **5.14x** | 1,070–5,626 µs |
| `Durable`, **tmpfs** (beast, same kernel, same binary) | 46,761 | 1.55x | 0 µs |
| `Buffered`, ext4 on SATA SSD (beast) | 47,739 | 1.58x | — |
| `Durable`, APFS on NVMe (M3 Max, `F_BARRIERFSYNC`) | 6,137 | **1.17x** | 131–156 µs |

Remove the fsync (`Buffered`) or remove the disk (tmpfs) and the oscillation
disappears. Run the identical code against a different filesystem, device and
fsync primitive and it disappears — 44 minutes on macOS, spanning ~10 beast
cycles, produced a throughput spread of 1.17x and **zero** samples above twice
the median fsync, against beast's 76%.

Two consistency checks fall out of that table. `Durable` on tmpfs (46,761) lands
on `Buffered` on disk (47,739), which is what "the fsync is free" should look
like. And the macOS control writes *more* bytes per second than beast does, so
if the effect were driven by accumulated write volume it would appear there
sooner, not never.

**What is ruled out.** Not weir's code — it does not reproduce anywhere the
fsync-to-this-disk path is absent. Not segment rotation: `weir_wab_segments`
stays at 1 for the whole run. Not buffer pressure: the FAST and SLOW samples
have almost identical WAB-size ranges (0.4–13.4 MB against 1.0–13.7 MB). Not
heat: package temperature sat at 58 °C against a 100 °C critical.

**What is not established.** Which layer of the storage stack produces it. The
shape — a fixed ~60 s fast window recurring every ~270 s with a ~5x write-latency
penalty — is consistent with an SLC write cache filling and being reclaimed on a
consumer SATA SSD (this is a Samsung 850 EVO), but nothing here distinguishes
that from an ext4 journal effect or a device-level mapping flush. Testing a
second device on the same host would separate device from filesystem; beast has
an NVMe fitted but unmounted, and mounting it needs root.

## What to take from this

- **Size `Durable` from a sustained figure, not a benchmark.** On this hardware
  that is ~350 rec/s for one producer, not 877.
- **The number is a property of your storage.** It is not transferable between
  machines, and `environments.md` already forbids treating it as if it were. Run
  `research.rs`'s sustained experiments on the disk you will deploy on.
- **Wire batching sidesteps it.** The oscillation is per-fsync cost, and
  `push_batch` amortises hundreds of records into one fsync — see
  [wire-batching.md](wire-batching.md). A batched producer pays this once per
  batch rather than once per record.
- **`Buffered` is unaffected**, at both the throughput and the variance level.
  The tier that does not wait for an fsync does not inherit the disk's moods.

## Reproducing

```sh
# The curve, on whatever storage you care about:
cargo test -p weir-server --test research --release -- --ignored --nocapture \
  --test-threads=1 sustained_durable_throughput_over_time

# The controls:
WEIR_BENCH_WAB_DIR=/dev/shm  cargo test ... sustained_durable_throughput_over_time
                             cargo test ... sustained_buffered_throughput_over_time
```

`WEIR_RESEARCH_SECS` (default 1800) and `WEIR_RESEARCH_SAMPLE_SECS` (default 30)
control duration and resolution. Use 5 s sampling to resolve the period: at 30 s
the FAST-onset gaps quantise to 240 or 270 s and report a period the sampling
grid invented.
