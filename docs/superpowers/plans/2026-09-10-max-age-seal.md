# A genuine maximum segment age — Implementation Plan

**Goal:** Give the WAB a seal that fires a bounded time after a segment was
*created*, so a steady trickle of writes cannot hold a segment open forever.
Add it alongside the existing idle timer; change nothing about that timer.

**Why:** `wab_segment_max_age_secs` is named like a maximum age and behaves like
an idle timer. The clock restarts on every flush (`wab/mod.rs:683-685`), so a
producer writing one record a minute against a 300-second setting resets it four
times over and the segment grows toward `wab_segment_max_bytes` (256 MiB) exactly
as if the knob were `0` — roughly two years at 500 bytes a minute. Low volume is
the archetypal deployment for an S3 archive, a meter, a field station, a till,
which is precisely the population this trap selects for.

**Tech stack:** Rust 2024, MSRV 1.88. No new dependencies.

---

## The knob name

**`wab_segment_max_lifetime_secs`.** u64 seconds, `0` = off (matching the
existing knob's convention), range `0..=86_400` (same cap as
`wab_segment_max_age_secs` — a day is already past any useful seal interval).

Why this name over the alternatives:

- **A lifetime has one fixed origin and is never reset.** You can idiomatically
  speak of "resetting the age counter" on a thing; you cannot reset a lifetime
  without ending it. That asymmetry is the exact distinction between the two
  knobs, and it is carried by ordinary English rather than by a convention the
  operator has to learn.
- **It does not prefix-collide.** `wab_segment_max_a…` uniquely completes to the
  old knob and `wab_segment_max_l…` to the new one, so tab-completion, `grep`,
  and a hurried skim of a TOML file all separate them on the first differing
  character after the shared stem. A candidate like
  `wab_segment_max_age_since_create_secs` fails this: it *contains* the old key
  as a prefix, so `grep wab_segment_max_age_secs` would not match it but a
  regex-free eye scan would confuse them constantly.
- **The shared `wab_segment_max_` stem is deliberate**, not accidental. Both are
  seal thresholds on the same object; they sort adjacent in the configuration
  reference, which is where the two must be read *against each other*. Naming
  the new one something structurally different (`wab_segment_seal_interval_secs`)
  would split them in the reference and hide the comparison.

Rejected: `wab_segment_max_open_secs` (accurate — "open" is the real lifecycle
state name in `SegmentStateLabel` — but reads as a file-handle limit);
`wab_segment_seal_interval_secs` (implies periodicity; the seal is one-shot per
segment); renaming the old knob (a silent behaviour change for anyone relying on
idle semantics, explicitly out of scope).

**No name fully rescues a misnamed sibling.** The name does as much as a name
can; the documentation has to finish the job, so the configuration reference
gets an explicit side-by-side that names the *reference point* of each timer in
its first sentence.

## The interaction rule

**Both may be set; whichever comes due first seals the segment.** Confirmed
correct, for three reasons:

1. They are both *upper* bounds on delivery latency. Two upper bounds compose by
   taking the minimum — anything else would let one knob raise a latency the
   operator had already capped with the other.
2. Neither is a floor. There is no setting of either that an operator could
   intend as "do not seal before this", so an earlier seal can never violate an
   expressed intent.
3. It degrades correctly at the edges. Lifetime alone gives a hard delivery
   bound. Idle alone is today's behaviour, unchanged. Both give "seal when quiet,
   and in any case within the lifetime" — which is what an operator who sets
   both is asking for. A lifetime shorter than the idle interval simply makes
   the idle timer unreachable; that is legitimate, not a misconfiguration, so it
   is **not** rejected at startup.

Sealing is already idempotent-ish at the loop level: `seal_current()` returns
`Ok(None)` when there is no active segment, so a race between the two conditions
cannot double-seal.

## Where the clock lives

**In `ShardWriter`, stamped in `ensure_open`** — the single place in the tree
where a segment file is created (`segment.rs:656` is the only `self.active =
Some(..)` assignment). A new method `active_segment_age() -> Option<Duration>`
returns `None` when no segment is open.

The alternative — tracking creation time in the flusher loop next to
`last_write_at` — was rejected because the flusher cannot see every segment
birth. A segment also rotates *inside* `flush_batch` when a write crosses
`wab_segment_max_bytes`, and `flush_batch` returns `()`. The flusher would have
to infer creation from writes, which is how the existing timer ended up
measuring the wrong thing in the first place. Stamping at the creation point
makes the clock structurally unable to drift from the segment it describes.

The stale-timestamp question: three code paths clear `self.active` (seal, write
error, fsync error) and none of them clear the stamp. That is deliberate and
safe — `active_segment_age()` is gated on `self.active.is_some()`, so a stale
stamp is unobservable, and `ensure_open` overwrites it before the next segment
becomes visible. One field, one writer, no clearing to forget.

Real `Instant`, not the `BlockingClock` seam, matching the existing idle timer.
`Instant::now()` runs once per segment creation (not per record), and the age
comparison is behind a `segment_max_lifetime.is_some_and(..)` short-circuit, so
a deployment with the knob off pays one `Option` discriminant check per flush.

## Where it is checked

Two places in `flusher_thread`:

1. **The `recv_timeout` timeout branch**, beside the existing idle check. This is
   the one that fires for the trickle case — with a 1 ms batch deadline the
   flusher reaches this branch constantly.
2. **After the bottom-of-loop `flush_batch`.** A saturated producer never lets
   `recv_timeout` time out, so without this the bound would hold only for traffic
   that pauses — i.e. it would be a second idle timer with extra steps. Sealing
   here is safe: `flush_batch` has already fsynced and acked, which is exactly
   the state the shutdown seal path relies on.

Both call one shared helper so the metrics bump, the drain hand-off, and the
seal-failure retry behave identically however the seal was triggered.

---

## Tasks

- [ ] **Task 1 — Config knob.** `PartialConfig` + `Config` fields, `merge!` +
      `check_range(0, 86_400)`, `cli.rs` (`--wab-segment-max-lifetime-secs`),
      `env.rs` (`WEIR_WAB_SEGMENT_MAX_LIFETIME_SECS`), `file.rs` (`RawServer`
      field **and** `BASE_SERVER_KEYS`). Unit tests: default is `0`, and the
      range guard is pinned in `bounded_scalar_knobs_reject_out_of_range`.

- [ ] **Task 2 — The clock.** `ShardWriter::active_opened_at` +
      `active_segment_age()`; stamp in `ensure_open`. Unit test in
      `segment.rs`: age is `None` with no segment, `Some` after a write, and
      `None` again after `seal_current`.

- [ ] **Task 3 — The seal.** `WabConfig::segment_max_lifetime`, plumb through
      `spawn` → `flusher_thread`, extract the shared seal helper, add both
      check sites. Wire `main.rs` from the config knob.

- [ ] **Task 4 — System test.** `max_lifetime_seal_fires_for_a_producer_that_never_goes_idle`:
      a trickle that would defeat the idle timer must still deliver. Modelled on
      `idle_seal_does_not_fire_for_a_producer_that_never_goes_idle` — record the
      worst real gap between writes and assert only if the premise held, so a
      stalled runner reports a skip rather than a false failure. Falsify it
      before trusting it.

- [ ] **Task 5 — Docs.** `docs/operations/configuration.md`: the new knob's own
      section plus a side-by-side that makes the two timers impossible to
      confuse. `CHANGELOG.md` under `## [Unreleased]`.

- [ ] **Task 6 — Gate.** `cargo fmt --all --check`; `cargo clippy --all-targets
      --all-features -- -D warnings`; `cargo test -p weir-server --bins
      --all-features -- --test-threads=1`; `cargo test -p weir-server --test
      system`. Plus: confirm
      `idle_seal_does_not_fire_for_a_producer_that_never_goes_idle` (currently
      on `chore/research-fleet-findings`, not on `main`) still passes unchanged
      against this branch with the new knob unset.
