"""Drain-quiescence detection via weir's existing /metrics endpoint.

Verification must not run before the drain has caught up, or it reports
violations that are really timing artefacts. Several signals settle the
question, all already exported — no new instrumentation was needed:

1. `weir_wab_bytes_on_disk` STABLE. Its registered HELP text states it counts
   the active segment plus sealed segments awaiting drain, and excludes
   `.confirmed`, `dead_letter/` and `quarantine/` — so it falls to
   active-segment-only exactly when drain has caught up. Stability across
   consecutive polls matters more than any absolute threshold, because the
   active segment's size is workload-dependent.

   CAUTION: this gauge is recomputed by a background task on
   `tokio::time::interval(Duration::from_secs(5))` (`main.rs:563-576`), NOT on
   every scrape. A stability window shorter than that 5s refresh period is
   satisfied by the gauge simply not having been recomputed yet — "unchanged"
   then means "stale", never "drain caught up". `GAUGE_REFRESH_SECS` below
   records the real period, `wait_for_quiescence` refuses to run with a
   window that does not exceed it (see its docstring), and the defaults
   (`poll_interval_s=2.0, stable_polls=4`, an 8s window) are chosen so a
   5s timer necessarily ticks at least once inside it.
2. `weir_queue_depth` at zero — nothing still in flight to the WAB.
3. `weir_drain_state{state="draining"}` == 1. Kept as necessary-but-NOT-
   sufficient: its own registered HELP text says outright that
   state="draining" does NOT imply delivery progress — a segment stranded
   waiting on a fully-down sink still reads draining (`metrics/mod.rs:543-546`).
   That is exactly what signals 4 and 5 exist to catch.
4. `weir_sink_health{state="down"}` != 1 — a down sink means nothing is
   actually draining, no matter what `drain_state` reads.
5. `weir_drain_segments_stranded_total` STABLE across the same window as the
   bytes gauge, when present — a rising count means segments are being
   abandoned to the sink outage even while bytes-on-disk looks quiet.

A timeout REPORTS "stuck" rather than hanging. A drain that never quiesces is
itself a finding, and silently waiting forever would hide it.
"""
import time
import urllib.request

#: How often weir recomputes `weir_wab_bytes_on_disk` (main.rs:563-576).
GAUGE_REFRESH_SECS = 5.0

DRAINING = 'weir_drain_state{state="draining"}'
BLOCKED = 'weir_drain_state{state="blocked_dead_letter_full"}'
#: `weir_sink_health` is a per-state gauge family (healthy/degraded/down);
#: this is the "down" member specifically.
SINK_DOWN = 'weir_sink_health{state="down"}'
#: A counter, so the exposition name carries the `_total` suffix.
STRANDED = "weir_drain_segments_stranded_total"


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

    Raises ValueError before polling starts if `scrape_fn` is None (i.e. this
    is a real scrape against a live daemon, not an injected fake) and
    `poll_interval_s * stable_polls` does not exceed `GAUGE_REFRESH_SECS`. A
    window that fits inside one refresh period of `weir_wab_bytes_on_disk` can
    be satisfied by the gauge simply not having been recomputed yet, which
    measures staleness, not drain progress — this is exactly the bug the
    defaults above were chosen to avoid, and the guard keeps a later
    "optimisation" of the poll interval from silently reintroducing it. Tests
    inject a fake scraper with no 5s gauge to outrun, so they are exempt.
    """
    if scrape_fn is None and poll_interval_s * stable_polls <= GAUGE_REFRESH_SECS:
        raise ValueError(
            f"poll_interval_s ({poll_interval_s}) * stable_polls ({stable_polls}) "
            f"= {poll_interval_s * stable_polls}s does not exceed GAUGE_REFRESH_SECS "
            f"({GAUGE_REFRESH_SECS}s): a window this short measures gauge staleness, "
            "not drain progress, because weir_wab_bytes_on_disk may not have been "
            "recomputed even once within it."
        )
    scrape_fn = scrape_fn or scrape
    deadline = time.monotonic() + timeout_s
    last_bytes = None
    last_stranded = None
    stranded_ever_seen = False
    stable = 0
    ok_scrapes = 0
    failed_scrapes = 0
    last_error = ""

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
            if poll_interval_s:
                # Never sleep past the deadline — an unconditional sleep
                # overshoots badly whenever poll_interval_s is large relative
                # to timeout_s (measured 6x over at 3s poll / 0.5s timeout).
                time.sleep(min(poll_interval_s, max(0.0, deadline - time.monotonic())))
            continue

        if m.get(BLOCKED, 0.0) == 1.0:
            return False, "drain is blocked (BlockedDeadLetterFull)"

        depth = m.get("weir_queue_depth", 1.0)
        wab_bytes = m.get("weir_wab_bytes_on_disk")
        # Necessary, not sufficient — see the module docstring: a segment
        # stranded on a fully-down sink still reads "draining".
        draining = m.get(DRAINING, 0.0) == 1.0
        # Absent means we cannot tell whether the sink is down, and the
        # conservative reading is "not quiesced" — default to the value that
        # BLOCKS quiescence, not the one that would let it through.
        sink_down = m.get(SINK_DOWN, 1.0)
        stranded = m.get(STRANDED)
        if stranded is not None:
            stranded_ever_seen = True
        # Present: must be stable across the same window as the bytes gauge.
        # Absent: skip this sub-check (weir may not expose it) rather than
        # block forever — but stranded_ever_seen makes that weaker check
        # visible in the timeout reason below, instead of failing silently.
        stranded_stable = stranded is None or stranded == last_stranded

        if (
            wab_bytes is not None
            and wab_bytes == last_bytes
            and depth == 0.0
            and draining
            and sink_down != 1.0
            and stranded_stable
        ):
            stable += 1
            if stable >= stable_polls:
                return True, ""
        else:
            stable = 0
        last_bytes = wab_bytes
        last_stranded = stranded

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
    note = ""
    if not stranded_ever_seen:
        note = (
            f"; {STRANDED} was absent from every scrape, so the stranded-segment "
            "stability check was skipped"
        )
    return False, (
        f"timeout after {timeout_s}s waiting for drain quiescence "
        f"({ok_scrapes} successful scrapes, {failed_scrapes} failed){note}"
    )
