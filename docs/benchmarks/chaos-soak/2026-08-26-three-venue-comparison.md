# Five soaks, three venues, two architectures, two on-disk formats

100,606,180 acked records across 1,226 `kill -9` crashes. Zero durability
violations. Zero records excused by the frontier exemption.

This consolidates the two bare-metal runs from 2026-08-22/23, the two
Docker/arm64 runs from 2026-08-25, and the Raspberry Pi run from 2026-08-25/26.
Read the scope limits at the bottom before quoting anything.

## The runs

| Run | Arch | Format | Venue | Duration | Kills | Acked |
|---|---|---|---|---|---|---|
| beast A | x86_64 | v1 | bare-metal NVMe | 10.01h | 438 | 25,266,567 |
| beast B | x86_64 | v1 | bare-metal NVMe | 5.02h | 227 | 13,390,247 |
| Mac office | aarch64 | **v2 (zstd)** | Docker Desktop VM | 3.42h | 111 | 41,059,463 |
| Mac overnight | aarch64 | **v2 (zstd)** | Docker Desktop VM | 2.05h | 46 | 15,910,149 |
| Pi 4B | aarch64 | v1 | real SD card | 8.84h | 404 | 4,979,754 |
| **Total** | | | | **29.3h** | **1,226** | **100,606,180** |

Throughput across these venues spans roughly **50x** — ~100 records/sec on the
Pi's SD card to ~4,800/sec in the Docker VM on compressible payloads.

## Every invariant, every run, zero

| Invariant | Meaning | All five runs |
|---|---|---|
| `i1_missing` | acked record never delivered — the crown invariant | **0** |
| `i2_leaked` | nacked record delivered anyway | **0** |
| `orphaned_delivered` | delivered with no ledger provenance | **0** |
| `ledger_conflicts` | ledger disagreed with itself | **0** |
| **`i1_exempt`** | **would-be I1 hits excused by the frontier** | **0** |
| ERROR log lines | | **0** |

`i1_exempt = 0` is the one that makes the rest mean something. I1 has an escape
hatch: records the load generator may not have flushed to its own ledger yet are
excused rather than counted as losses. A run can post `i1_missing = 0` while
quietly excusing thousands. Across 1,226 crashes, **nothing was excused** —
every acked record was held to the invariant with no relief.

The indeterminate-record identity `pushed − acked = unknown` holds exactly in
every run. Those are records in flight when the kill landed, whose ack never
came back; delivering them or not are both conformant, which is why distinct
delivered legitimately exceeds acked.

## The strongest result: recovery is deterministic, not merely correct

| Run | Truncations | Kills | Per kill |
|---|---|---|---|
| beast A | 1,752 | 438 | **4.000** |
| beast B | 908 | 227 | **4.000** |
| Mac office | 444 | 111 | **4.000** |
| Mac overnight | 184 | 46 | **4.000** |
| Pi 4B | 1,616 | 404 | **4.000** |
| **Total** | **4,904** | **1,226** | **4.000** |

`shard_count = 4` throughout. Every kill caught all four shards mid-write, and
recovery truncated each back to its last valid record — never a shard escaping
with a clean tail, never one needing a second pass.

**1,226 out of 1,226**, across:

- two CPU architectures (x86_64 and aarch64)
- two on-disk formats (v1 uncompressed and v2 zstd)
- three storage stacks (NVMe, a virtualised VM disk, an SD card)
- a ~50x throughput range

The *precision* is the finding. A ragged ratio would point at uneven load
distribution or nondeterministic recovery. This says the active segment always
ends mid-record under continuous load — structural, not probabilistic — and that
the truncate-the-torn-tail path is bit-for-bit identical regardless of
architecture, format or medium.

It is also worth naming what this does **not** cover: this is depth on one
recovery branch. Clean tails, partially-sealed sentinels, header corruption and
mid-file corruption (the quarantine path) each got zero executions, because
`kill -9` produces exactly one shape of damage.

## What format v2 changes: redelivery variance, not correctness

The duplicate rate is flat at **1.000** on every v1 run and sits at
**1.006–1.009** on both v2 runs. That is conformant — at-least-once permits
redelivery, and I2 is clean — but the distribution is the interesting part.
On the v2 office run, per-episode redelivery is **bimodal**:

```
 5 spike episodes (>5k redelivered), 50 trickle episodes, 56 completely clean

  ep 14  redelivered=  57,611
  ep 38  redelivered=  30,942
  ep 40  redelivered=  85,909
  ep 57  redelivered=  97,998
  ep 63  redelivered=  90,606

spikes carry 363,066 of 365,433 redeliveries — 99.4%
```

Five episodes out of 111 carry 99.4% of all redelivery. A crash usually costs
nothing in duplicates; occasionally it costs a large block.

The mechanism is segment-granularity replay: when a kill lands after a segment's
records were delivered but before its `.confirmed` sidecar is durable, the whole
segment replays. Compression means a segment holds far more logical records, so
each such event costs proportionally more. The daemon logs agree — 46 segment
rotations on the v2 office run against zero across the v1 bare-metal runs.

### This is a hypothesis, not a demonstrated result

The Pi was deliberately run at **v1** so it would isolate the architecture
variable, and it came back at a flat 1.000 — the right direction. But throughput
is confounded and the completed set does not resolve it:

| Run | Records/episode | Format | dup_rate |
|---|---|---|---|
| Pi 4B | ~12,300 | v1 | 1.000 |
| beast | ~58,000 | v1 | 1.000 |
| Mac | ~371,000 | **v2** | **1.006–1.009** |

The Mac varies *both* the format and records-per-episode. The clean experiment
is **v2 at low throughput** — a few hours on the Pi would settle it. Until then
segment-granularity replay is the best explanation available, not a proven one.

## Daemon logs account for themselves

Every WARN in every run falls into a known class with nothing left over. The Pi
run, as the fully-completed example:

| Class | Count |
|---|---|
| `truncated mid-length-field` | 1,616 |
| `WEIR_SINK_BEARER_TOKEN is unset` | 405 (one per daemon start = 404 kills + 1) |
| `shard_count/worker_count above recommended` | 405 |
| **Total WARN** | **2,426** |

`1,616 + 405 + 405 = 2,426`. Exact. **0 ERROR** lines in all five runs.

The `shard_count` warning is venue configuration, not a weir defect: the Pi has
4 cores, the Docker VM exposes 4, and beast reports 4 under `isolcpus`.

## What none of this covers

A clean 100-million-record result invites over-quoting. Stated plainly:

- **Power loss is untested.** Every fault in all five runs is `kill -9`, which
  does not lose the page cache. The headline contract "an ack is never a false
  ack" is therefore evidenced against *process* crashes only. Real power loss
  needs Phase 2 (`dm-flakey drop_writes`), which is blocked on injection code
  that has not been written. This is the single largest gap.
- **Only the `Sync` tier ran.** `Buffered` — which by design acks before fsync
  and explicitly does *not* uphold "ack ⇒ durable" — has zero chaos coverage.
  `Batched` is today identical to `Sync` in behaviour, so it is covered
  transitively.
- **The quarantine path was never entered.** Residue came back
  `quarantined = 0, dead_letter = 0` everywhere. That is not a pass: `kill -9`
  truncates *cleanly*, and quarantine exists for mid-file corruption. The
  quarantine tooling shipped in 2.0 still has no chaos coverage.
- **The WAB cap and growth warning never fired.** `nacked = 0` in every run and
  no growth WARN appeared, so neither path executed.
- **No number with a unit is quotable from the two Docker runs.** Storage is
  virtualised. The invariant results are venue-robust because the fault is a
  process kill; throughput, latency and byte figures are not.
- **The v2 runs used an unrealistically compressible payload**
  (`{"run":N,"seq":M,"pad":"aaaa…"}`). The v2 format path is genuinely
  exercised, but the compression ratio — and therefore the *magnitude* of the
  redelivery spikes — is not representative. The mechanism is real; its size on
  real data is unmeasured.
- **The two Mac runs were stopped by hand** and so have no final pass. Their
  per-episode verdicts stand; the end-of-run drained-state check at
  `frontier_slack = 0` is simply absent. beast A, beast B and the Pi run all
  completed on their deadlines with the final pass intact.

## Venue notes

**beast** — i9-9900K, kernel 7.0.0-28, 31 GiB, ext4 on a 2 GiB loop device.
Tuned for measurement (`isolcpus=2-7,10-15`, `nohz_full`, `mitigations=off`), so
runs launch under `taskset -c 0-15`.

**Mac** — Apple M3 Max, Docker Desktop (4 CPUs, 7.75 GiB) running `linux/arm64`
natively, ext4 on a 2 GiB loop device in a privileged container. **Docker
Desktop costs ~44x write amplification** — 33.3 GB/hour of physical writes for
~0.75 GB/hour of useful data, because loop → ext4 → overlayfs → VM disk image →
APFS each journal every fsync. Fine for invariants, a poor choice for anything
long.

**Pi 4B** — 4x Cortex-A72, 1.8 GiB RAM, Debian 13 trixie, ext4 on a 2 GiB loop
device on the SD card. Binaries were *transplanted* from the bookworm container
rather than compiled: glibc 2.36 → 2.41 is the compatible direction, and the Pi
has no internet, so building on-device was not an option. `quiescence_timeout`
was raised to 300s because SD fsync latency makes a bare-metal timeout produce
false findings. Temperature moved 61.3 → 63.7°C over 8.84h, `throttled=0x0`.

A footnote on feasibility: the verifier's oracle was replaced on 2026-08-24 with
a dense byte-array representation. At 41M records the previous dict-based
accumulator would have needed ~13.8 GB — past the Docker VM's 7.75 GiB — and the
office run would have died around episode 60. The dense one used 37 MB.
