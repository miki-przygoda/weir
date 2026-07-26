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
        draining = m.get(DRAINING, 0.0) == 1.0

        if wab_bytes is not None and wab_bytes == last_bytes and depth == 0.0 and draining:
            stable += 1
            if stable >= stable_polls:
                return True, ""
        else:
            stable = 0
        last_bytes = wab_bytes

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
    return False, (
        f"timeout after {timeout_s}s waiting for drain quiescence "
        f"({ok_scrapes} successful scrapes, {failed_scrapes} failed)"
    )
