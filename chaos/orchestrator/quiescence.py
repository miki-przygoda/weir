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
