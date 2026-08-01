"""Drain-quiescence detection: has weir's drain caught up after a fault?

Verification must not run before the drain has caught up, or it reports
violations that are really timing artefacts.

This module has been wrong FOUR times, always by trying to infer drain state
from `/metrics` instead of measuring it directly:

1. Compared `weir_wab_bytes_on_disk` across a window shorter than the 5s
   period that gauge is recomputed on (`main.rs`'s background refresh task),
   so "unchanged" meant "not yet recomputed" and quiescence returned True
   almost immediately after every restart.
2. Widened the window but kept the same gauge — and `weir_wab_bytes_on_disk`
   (`compute_wab_bytes_on_disk`, `main.rs:58-91`) counts the OPEN,
   still-growing active segment plus sealed segments awaiting drain. Under
   continuous load that total changes on every genuine 5s recompute, so
   byte-exact stability across consecutive polls never occurred at all — the
   gate became unreachable in the other direction (0 of 800 simulated
   episodes quiesced).
3. Switched to `sealed_total == confirmed_total + quarantined_total`, all
   Counter families incremented at their transition sites — not
   timer-refreshed, so in theory immune to bug 1's staleness trap. But those
   counters are PER-PROCESS and reset to 0 on every daemon restart, and
   `recover_segment` (`crates/weir-server/src/wab/recovery.rs:497-503`) seals
   a segment during crash recovery WITHOUT incrementing `sealed`, while the
   drain's confirm path DOES increment `confirmed` when it later drains that
   same segment. After any restart the identity is permanently broken in the
   unsafe direction: `confirmed = sealed + shard_count`, so
   `sealed == confirmed + quarantined` can never hold again for the rest of
   the process's life. Observed live as `sealed(0) != confirmed(4)` on every
   post-restart episode — every episode timed out.
4. Reverted to the bytes gauge (bug 2's approach) rather than re-diagnose —
   which is inferential in the first place, and on a FRESHLY RESTARTED
   daemon every metric condition is trivially satisfied because nothing has
   happened yet: the gauge starts at 0 and reads "stable" instantly. A
   demonstrated ~1s false positive with four replay segments queued and
   undelivered — quiescence reported True while real backlog sat on disk.

Bytes-on-disk conflates two things that need to be judged separately: how
much is BUFFERED (workload-dependent, irrelevant to "has the drain caught
up") and whether SEALED WORK has reached a terminal state (exactly what
matters). No metrics-derived signal belongs in this function's primary
check; all four attempts above were fundamentally an IDENTITY computed from
counters or gauges, standing in for a STATE that is directly observable.

The fix: stop inferring, measure the filesystem. `run.py` runs as root and
owns the mount, so ground truth is a `glob`/`scandir` of the WAB directory
itself — immune to counter resets (bug 3), immune to the recovery-seal
asymmetry (bug 3), and immune to "nothing has happened yet" reading as
"caught up" (bug 4), because at T+50ms after a restart the replay backlog is
already physically on disk, sealed, with no `.confirmed` sidecar.

Quiescence requires ALL of the following to hold for `stable_polls`
consecutive polls:

1. Zero `*.wab.sealed` files lacking a corresponding `.confirmed` sidecar —
   sealed work that has not reached a terminal state (drained or
   quarantined; quarantined segments are moved into `quarantine/`, which is
   skipped by the scan, so nothing there can hold this open). This is the
   direct, on-disk analogue of what bug 3 tried to compute from counters.
2. Zero non-empty active `*.wab` files — records buffered in an open segment
   are acked and undelivered. `run.py` passes `--wab-segment-max-age-secs 2`
   so an idle open segment seals and drains; without that, this condition
   could never clear while the producer is paused (SIGSTOPped) waiting for
   quiescence.
3. The existing metric conditions, kept as necessary-but-not-sufficient
   companions — they catch cases the filesystem scan alone would not (a sink
   that is down but has strandeded nothing new yet, a queue backed up before
   its records reach the WAB at all):
   - `stranded_total == resumed_total` — no segment is still stranded. This
     is EQUALITY, not stability: an already-stranded segment that never
     resumes must keep failing this check for as long as it stays stranded —
     a counter that is merely not RISING is not good enough, or an outage
     that stranded a segment before polling ever started would satisfy every
     other condition while the segment sits there undelivered.
   - `weir_queue_depth == 0` — nothing still in flight to the WAB.
   - `weir_drain_state{state="draining"} == 1` — demoted to
     necessary-not-sufficient: its own registered HELP text says outright
     that state="draining" does NOT imply delivery progress (a segment
     stranded on a fully-down sink still reads draining).
   - `weir_sink_health{state="down"} != 1` — a down sink means nothing is
     actually draining, no matter what `drain_state` reads.
   - the immediate `BLOCKED` return (`drain_state{state="blocked_dead_letter_full"}`)
     — reported instantly, not folded into the stability window.

Someone will be tempted to go back to metrics because they are one HTTP call
instead of a directory walk. Don't. Every one of the four bugs above came
from treating a DERIVED, timer- or counter-based signal as if it were the
underlying state; the filesystem scan is the state.

A timeout REPORTS "stuck" rather than hanging. A drain that never quiesces is
itself a finding, and silently waiting forever would hide it.
"""
import collections
import os
import time
import urllib.request

DRAINING = 'weir_drain_state{state="draining"}'
BLOCKED = 'weir_drain_state{state="blocked_dead_letter_full"}'
#: `weir_sink_health` is a per-state gauge family (healthy/degraded/down);
#: this is the "down" member specifically.
SINK_DOWN = 'weir_sink_health{state="down"}'
#: Counters, so both carry the `_total` suffix.
STRANDED = "weir_drain_segments_stranded_total"
RESUMED = "weir_drain_segments_resumed_total"

#: On-disk extensions, mirroring `crates/weir-wab/src/format.rs`
#: (`EXT_ACTIVE`/`EXT_SEALED`/`EXT_CONFIRMED`). Duplicated here as string
#: literals because Python cannot import a Rust const — same tradeoff as
#: `run.py`'s `LEDGER_FLUSH_THRESHOLD`. If the on-disk layout changes, this
#: must change with it.
EXT_ACTIVE = ".wab"
EXT_SEALED = ".wab.sealed"
EXT_CONFIRMED = ".wab.confirmed"

#: Bytes written to a segment file at creation, before any record
#: (`crates/weir-wab/src/format.rs SEGMENT_HEADER_LEN`, written by
#: `WabSegment::create` at `crates/weir-server/src/wab/segment.rs:~85`,
#: before the writer's first `write_record` call). An active segment file
#: therefore always has at least this many bytes even with zero buffered
#: records, so "non-empty" means strictly more than this — not merely a
#: size greater than zero.
SEGMENT_HEADER_LEN = 24

#: Subdirectories weir owns for its own accounting, not drain backlog.
#: Mirrors the skip list in `wab::recover_open_segments` and
#: `wab::scan_unconfirmed_sealed`
#: (`crates/weir-server/src/wab/{recovery,mod}.rs`): `quarantine/` holds
#: segments parked for an operator after corruption; `dead_letter/` is owned
#: by the `DeadLetterWriter`.
RESERVED_SUBDIRS = frozenset({"quarantine", "dead_letter"})

#: Snapshot of on-disk WAB backlog: `scan_wab_residue`'s return type.
Residue = collections.namedtuple("Residue", ["unconfirmed_sealed", "nonempty_active"])


def scan_wab_residue(wab_dir):
    """Ground-truth scan of `wab_dir` for undrained backlog.

    Descends into every shard directory directly under `wab_dir` (skipping
    `RESERVED_SUBDIRS`) and counts:

    - `unconfirmed_sealed`: `*.wab.sealed` files with no matching
      `*.wab.confirmed` sidecar (sealed work not yet drained or
      quarantined — quarantined copies live under `quarantine/`, which this
      scan never descends into, so they cannot appear here).
    - `nonempty_active`: `*.wab` files larger than `SEGMENT_HEADER_LEN`,
      i.e. an open segment holding at least one buffered record.

    Raises whatever `os.scandir`/`os.stat` raise (e.g. `FileNotFoundError`
    if `wab_dir` does not exist, `PermissionError`) — the caller is
    responsible for treating a raise the same as a failed metrics scrape,
    not letting it crash the run.
    """
    unconfirmed_sealed = 0
    nonempty_active = 0
    with os.scandir(wab_dir) as it:
        shard_dirs = [
            e.path
            for e in it
            if e.is_dir(follow_symlinks=False) and e.name not in RESERVED_SUBDIRS
        ]
    for shard_dir in shard_dirs:
        with os.scandir(shard_dir) as it:
            entries = list(it)
        for entry in entries:
            name = entry.name
            if name.endswith(EXT_SEALED):
                if not entry.is_file(follow_symlinks=False):
                    continue
                base = name[: -len(EXT_SEALED)]
                confirmed_path = os.path.join(shard_dir, base + EXT_CONFIRMED)
                if not os.path.exists(confirmed_path):
                    unconfirmed_sealed += 1
            elif name.endswith(EXT_ACTIVE):
                if not entry.is_file(follow_symlinks=False):
                    continue
                if entry.stat().st_size > SEGMENT_HEADER_LEN:
                    nonempty_active += 1
    return Residue(unconfirmed_sealed=unconfirmed_sealed, nonempty_active=nonempty_active)


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
        head, sep, last = line.rpartition(" ")
        if not sep:
            continue
        try:
            value = float(last)
        except ValueError:
            continue
        # OpenMetrics permits an optional trailing timestamp (`name value ts`).
        # Taking the last field blindly would read the timestamp as the value
        # AND fold the real value into the key, so the canonical key silently
        # vanishes from the dict. If the remainder also ends in a number, the
        # last field was the timestamp. (Checking this way, rather than
        # splitting on the first space, keeps a label value containing a space
        # intact — weir emits none, but the parser should not depend on that.)
        head2, sep2, prev = head.rpartition(" ")
        if sep2:
            try:
                value = float(prev)
                head = head2
            except ValueError:
                pass
        out[head] = value
    return out


def scrape(metrics_url):
    """Fetches and parses /metrics."""
    with urllib.request.urlopen(metrics_url, timeout=5) as resp:
        return parse(resp.read().decode("utf-8"))


def wait_for_quiescence(
    metrics_url,
    timeout_s,
    wab_dir=None,
    scrape_fn=None,
    residue_fn=None,
    poll_interval_s=2.0,
    stable_polls=4,
):
    """Blocks until the drain is quiesced or the timeout expires.

    Returns (True, "") on quiescence, (False, reason) otherwise. Never raises
    on a stuck drain — a stuck drain is a finding to report, not an exception
    to crash on.

    `wab_dir` is the WAB directory `scan_wab_residue` (or an injected
    `residue_fn`) scans for on-disk backlog — the primary signal. `scrape_fn`
    and `residue_fn` default to the real HTTP scrape and the real filesystem
    scan respectively; tests inject fakes for both so no daemon or real mount
    is required.

    Every condition checked here (see the module docstring) is a snapshot
    property of a single poll, not a delta against the previous one, so
    `stable_polls` consecutive passing polls is a guard against a single
    flicker rather than a comparison across polls — there is no "last
    observed value" to track, unlike the bytes-gauge approach this replaced.
    """
    # Checked BEFORE reassigning residue_fn below, unlike the now-removed
    # GAUGE_REFRESH_SECS guard this module used to carry — that guard tested
    # `scrape_fn is None` AFTER `scrape_fn = scrape_fn or scrape` had already
    # reassigned it, so the check was permanently dead code. `wab_dir=None`
    # with the real scanner would otherwise silently fall back to scanning
    # `os.scandir`'s default (the current working directory) instead of the
    # WAB mount — a wrong-but-not-crashing answer that would fail quiet
    # rather than loud, so this raises instead.
    if residue_fn is None and wab_dir is None:
        raise ValueError(
            "wab_dir is required unless residue_fn is provided (e.g. by a test)"
        )
    scrape_fn = scrape_fn or scrape
    residue_fn = residue_fn or scan_wab_residue

    deadline = time.monotonic() + timeout_s
    stable = 0
    ok_polls = 0
    failed_polls = 0
    last_error = ""
    last_unmet = []

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        try:
            m = scrape_fn(metrics_url)
        except Exception as exc:  # daemon may be mid-restart; keep polling
            failed_polls += 1
            last_error = f"metrics scrape: {type(exc).__name__}: {exc}"
            # A failed poll must not leave `stable` as-is. Without this
            # reset, a window could straddle an observability gap and count
            # polls from either side of it as consecutive, when they were
            # never observed back-to-back.
            stable = 0
            if poll_interval_s:
                # Never sleep past the deadline — an unconditional sleep
                # overshoots badly whenever poll_interval_s is large relative
                # to timeout_s.
                time.sleep(min(poll_interval_s, max(0.0, deadline - time.monotonic())))
            continue

        if m.get(BLOCKED, 0.0) == 1.0:
            return False, "drain is blocked (BlockedDeadLetterFull)"

        try:
            residue = residue_fn(wab_dir)
        except Exception as exc:  # e.g. transient dirent error mid-write
            failed_polls += 1
            last_error = f"WAB residue scan: {type(exc).__name__}: {exc}"
            stable = 0
            if poll_interval_s:
                time.sleep(min(poll_interval_s, max(0.0, deadline - time.monotonic())))
            continue

        ok_polls += 1

        # Counters default to 0.0 when absent, and that is a genuine zero,
        # not a conservative guess: prometheus-client only emits a Family
        # member once it has been incremented at least once, so "absent"
        # means "never happened" — `0 == 0` correctly reports "nothing
        # stranded, nothing resumed" rather than blocking forever on a
        # daemon that has done nothing yet.
        stranded = m.get(STRANDED, 0.0)
        resumed = m.get(RESUMED, 0.0)

        # Gauges keep the conservative blocking default: absent means we
        # cannot tell, and the reading that blocks quiescence is the safe
        # one. In practice `drain_state` and `sink_health` are pre-initialised
        # by weir, so these defaults are a belt-and-braces fallback, not the
        # common case.
        depth = m.get("weir_queue_depth", 1.0)
        # Necessary, not sufficient — see the module docstring: a segment
        # stranded on a fully-down sink still reads "draining".
        draining = m.get(DRAINING, 0.0) == 1.0
        sink_down = m.get(SINK_DOWN, 1.0)

        no_unconfirmed_sealed = residue.unconfirmed_sealed == 0
        no_buffered_active = residue.nonempty_active == 0
        nothing_stranded = stranded == resumed

        # Record WHICH conditions are unmet, not just that some are. A timeout
        # reading "waiting for drain quiescence" and nothing else sends the
        # operator to read metrics by hand — the same diagnostic dead end that
        # made "every poll failed" indistinguishable from "the drain never
        # settled" before it was fixed. Only the last poll's state is kept:
        # that is the state the timeout actually fired on.
        unmet = []
        if not no_unconfirmed_sealed:
            unmet.append(f"unconfirmed_sealed={residue.unconfirmed_sealed}")
        if not no_buffered_active:
            unmet.append(f"nonempty_active_segments={residue.nonempty_active}")
        if not nothing_stranded:
            unmet.append(f"stranded({stranded:.0f}) != resumed({resumed:.0f})")
        if depth != 0.0:
            unmet.append(f"queue_depth={depth:.0f}")
        if not draining:
            unmet.append("drain_state is not 'draining'")
        if sink_down == 1.0:
            unmet.append("sink_health is 'down'")
        last_unmet = unmet

        if (
            no_unconfirmed_sealed
            and no_buffered_active
            and nothing_stranded
            and depth == 0.0
            and draining
            and sink_down != 1.0
        ):
            stable += 1
            if stable >= stable_polls:
                return True, ""
        else:
            stable = 0

        if poll_interval_s:
            time.sleep(min(poll_interval_s, max(0.0, deadline - time.monotonic())))

    # Distinguish a harness/config problem from a genuine finding. Both return
    # False, but on a multi-day run an identical reason string for "the URL was
    # wrong the whole time" and "the drain really never settled" costs hours of
    # misdirected investigation.
    if ok_polls == 0:
        return False, (
            f"timeout after {timeout_s}s: every poll failed "
            f"({failed_polls} attempts, last: {last_error}). This is a harness "
            "or configuration problem, not a weir finding."
        )
    detail = f"; unmet on the final poll: {', '.join(last_unmet)}" if last_unmet else ""
    return False, (
        f"timeout after {timeout_s}s waiting for drain quiescence "
        f"({ok_polls} successful polls, {failed_polls} failed){detail}"
    )
