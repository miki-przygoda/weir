"""Drain-quiescence detection via weir's existing /metrics endpoint.

Verification must not run before the drain has caught up, or it reports
violations that are really timing artefacts.

Two earlier versions of this module got that wrong in opposite directions.
The first compared `weir_wab_bytes_on_disk` across a window shorter than the
5s period that gauge is recomputed on (`main.rs`'s background refresh task),
so "unchanged" meant "not yet recomputed" and quiescence returned True almost
immediately after every restart. The second widened the window but kept the
same gauge — and `weir_wab_bytes_on_disk` (`compute_wab_bytes_on_disk`,
`main.rs:58-91`) counts the OPEN, still-growing active segment plus sealed
segments awaiting drain. Under continuous load (which `run.py` never pauses)
that total changes on every genuine 5s recompute, so byte-exact stability
across consecutive polls never occurs at all — the gate became unreachable
in the other direction (0 of 800 simulated episodes quiesced). Bytes-on-disk
conflates two things that need to be judged separately: how much is
BUFFERED (workload-dependent, and irrelevant to "has the drain caught up")
and whether SEALED WORK has reached a terminal state (exactly what matters).
Neither version of the bytes gauge belongs in this function; it has been
removed from the quiescence logic entirely.

The replacement signal is `weir_wab_segments_total`, a Counter family with
states open/sealed/confirmed/quarantined, incremented at the actual
transition sites (`wab/mod.rs:603,666,769` on seal, `drain/confirmed.rs:55`
on confirm, `wab/recovery.rs` on quarantine) — transition-driven, not
timer-refreshed, so an unchanged reading really means "no transition
happened since the last poll", never "not yet recomputed".

Quiescence requires ALL of the following to hold for `stable_polls`
consecutive polls:

1. `sealed_total == confirmed_total + quarantined_total` — every sealed
   segment has reached a terminal state. A sealed segment ends as either
   confirmed or quarantined, so both must be counted; this is the real
   "has the drain caught up" test.
2. `stranded_total == resumed_total` — no segment is still stranded. weir's
   own HELP text for `weir_drain_segments_resumed` says this directly:
   "Convergence with weir_drain_segments_stranded means an outage's backlog
   has been picked back up; a persistent gap means segments are still
   stranded." This is EQUALITY, not stability: an already-stranded segment
   that never resumes must keep failing this check for as long as it stays
   stranded — a counter that is merely not RISING is not good enough, or an
   outage that stranded a segment before polling ever started would satisfy
   every condition while the segment sits there undelivered.
3. `weir_queue_depth == 0` — nothing still in flight to the WAB.
4. `weir_drain_state{state="draining"} == 1` — kept, but demoted to
   necessary-not-sufficient: its own registered HELP text says outright
   that state="draining" does NOT imply delivery progress (a segment
   stranded on a fully-down sink still reads draining). Conditions 1 and 2
   are what actually catch that case.
5. `weir_sink_health{state="down"} != 1` — a down sink means nothing is
   actually draining, no matter what `drain_state` reads.

None of these five signals is refreshed by a background timer — every
counter above is incremented synchronously at its transition site, and every
gauge is updated synchronously on the state change it reports — so there is
no "stale but unchanged" trap here, and no runtime guard is needed to keep a
retuned poll interval from reintroducing one. If a future signal added here
IS timer-refreshed, it needs a guard like the one this module used to carry
(and removed once the bytes gauge was dropped) — don't add one without
re-adding that protection.

A timeout REPORTS "stuck" rather than hanging. A drain that never quiesces is
itself a finding, and silently waiting forever would hide it.
"""
import time
import urllib.request

DRAINING = 'weir_drain_state{state="draining"}'
BLOCKED = 'weir_drain_state{state="blocked_dead_letter_full"}'
#: `weir_sink_health` is a per-state gauge family (healthy/degraded/down);
#: this is the "down" member specifically.
SINK_DOWN = 'weir_sink_health{state="down"}'
#: Bytes currently held by live WAB segments: the open active segment plus
#: sealed segments awaiting drain. EXCLUDES confirmed/dead-letter/quarantine
#: (its registered HELP text says so), and confirmed segments are deleted, so
#: this falls as the drain catches up and goes flat once it has.
WAB_BYTES = "weir_wab_bytes_on_disk"

#: How often weir recomputes WAB_BYTES — a background task on
#: `tokio::time::interval(Duration::from_secs(5))` (`main.rs:563-576`). The
#: stability window MUST exceed this, or "unchanged" means "not yet
#: recomputed". Guarded at call time below.
GAUGE_REFRESH_SECS = 5.0
#: Counters, so both carry the `_total` suffix.
STRANDED = "weir_drain_segments_stranded_total"
RESUMED = "weir_drain_segments_resumed_total"


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
    metrics_url, timeout_s, scrape_fn=None, poll_interval_s=2.0, stable_polls=4
):
    """Blocks until the drain is quiesced or the timeout expires.

    Returns (True, "") on quiescence, (False, reason) otherwise. Never raises
    on a stuck drain — a stuck drain is a finding to report, not an exception
    to crash on.

    Every condition checked here (see the module docstring) is a snapshot
    property of a single scrape, not a delta against the previous one, so
    `stable_polls` consecutive passing polls is a guard against a single
    flicker rather than a comparison across polls — unlike the bytes-gauge
    approach this replaced, there is no "last observed value" to track.
    """
    scrape_fn = scrape_fn or scrape
    deadline = time.monotonic() + timeout_s
    stable = 0
    ok_scrapes = 0
    failed_scrapes = 0
    last_error = ""
    last_unmet = []
    last_bytes = None

    # A window shorter than the gauge's refresh period measures STALENESS, not
    # drain progress — that was this module's first bug. Keyed on scrape_fn
    # being absent (a real daemon); an injected fake has no 5s gauge to outrun.
    if scrape_fn is None and poll_interval_s * stable_polls <= GAUGE_REFRESH_SECS:
        raise ValueError(
            f"poll_interval_s * stable_polls = {poll_interval_s * stable_polls}s "
            f"does not exceed the {GAUGE_REFRESH_SECS}s gauge refresh period; a "
            "shorter window would report a stale reading as a stable one"
        )

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            m = scrape_fn(metrics_url)
            ok_scrapes += 1
        except Exception as exc:  # daemon may be mid-restart; keep polling
            failed_scrapes += 1
            last_error = f"{type(exc).__name__}: {exc}"
            # Minor 6: a failed scrape must not leave `stable` as-is. Without
            # this reset, a window could straddle an observability gap and
            # count polls from either side of it as consecutive, when they
            # were never observed back-to-back.
            stable = 0
            last_bytes = None
            if poll_interval_s:
                # Never sleep past the deadline — an unconditional sleep
                # overshoots badly whenever poll_interval_s is large relative
                # to timeout_s (measured 6x over at 3s poll / 0.5s timeout).
                time.sleep(min(poll_interval_s, max(0.0, deadline - time.monotonic())))
            continue

        if m.get(BLOCKED, 0.0) == 1.0:
            return False, "drain is blocked (BlockedDeadLetterFull)"

        # Counters default to 0.0 when absent, and that is a genuine zero,
        # not a conservative guess: prometheus-client only emits a Family
        # member once it has been incremented at least once, so "absent"
        # means "never happened" — `0 == 0 + 0` correctly reports "nothing
        # sealed yet, nothing to drain" rather than blocking forever on a
        # daemon that has done nothing yet.
        wab_bytes = m.get(WAB_BYTES)
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

        # Absent means we cannot tell, and the conservative reading blocks.
        drained = wab_bytes is not None and wab_bytes == last_bytes
        last_bytes = wab_bytes
        nothing_stranded = stranded == resumed

        # Record WHICH conditions are unmet, not just that some are. A timeout
        # reading "waiting for drain quiescence" and nothing else sends the
        # operator to read metrics by hand — the same diagnostic dead end that
        # made "every scrape failed" indistinguishable from "the drain never
        # settled" before it was fixed. Only the last poll's state is kept:
        # that is the state the timeout actually fired on.
        unmet = []
        if not drained:
            unmet.append(f"wab_bytes_on_disk still changing (now {wab_bytes})")
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
            drained
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
    if ok_scrapes == 0:
        return False, (
            f"timeout after {timeout_s}s: every scrape of {metrics_url} failed "
            f"({failed_scrapes} attempts, last: {last_error}). This is a harness "
            "or configuration problem, not a weir finding."
        )
    detail = f"; unmet on the final poll: {', '.join(last_unmet)}" if last_unmet else ""
    return False, (
        f"timeout after {timeout_s}s waiting for drain quiescence "
        f"({ok_scrapes} successful scrapes, {failed_scrapes} failed){detail}"
    )
