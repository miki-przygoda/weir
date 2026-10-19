"""Lifecycle fault kinds for the Foundations chaos schedules (spec §16).

Phase 1 cut the daemon with SIGKILL and Phase 2 with a dm-flakey power cut.
These kinds first PROVOKE a lifecycle path weir only has since Foundations —
a bounded SIGTERM shutdown, a forced second signal, a signal during
recovery, an admin pause — and then cut at a seeded point inside or after it.
The cut is what the oracle judges; the trigger decides what state it lands
on. Every trigger records whether it actually landed (its own canary), and
every power cut records I6, the injector's.

Stdlib only, like the rest of the orchestrator.
"""
import random

LEGACY_KINDS = ("kill_random", "power_loss")
TRIGGER_KINDS = ("sigterm_at", "double_sigterm", "sigterm_in_recovery",
                 "admin_pause_resume")
SHUTDOWN_TRIGGERS = ("sigterm_at", "double_sigterm")
CUT_MODES = ("power_loss", "kill")

_COMMON = frozenset({"kind", "cut", "cut_lo_ms", "cut_hi_ms"})
#: Every [faults] key a trigger reads. A key outside its kind's set is
#: refused — the same "nothing reads it, so the run lies" rule as
#: run.reject_unknown_schedule_keys, applied per kind.
TRIGGER_FAULT_KEYS = {
    "sigterm_at": _COMMON | {"stall_sink_probability"},
    "double_sigterm": _COMMON | {"stall_sink_probability",
                                 "second_signal_lo_ms", "second_signal_hi_ms"},
    "sigterm_in_recovery": _COMMON | {"recovery_delay_ms"},
    "admin_pause_resume": _COMMON | {"pause_hold_lo_ms", "pause_hold_hi_ms",
                                     "pause_cut_while_paused_probability"},
}
#: The keys a trigger cannot run without. There is no silent default for a
#: cut window: a default would decide what the run measured on its behalf.
REQUIRED_TRIGGER_KEYS = {
    "sigterm_at": frozenset({"cut", "cut_lo_ms", "cut_hi_ms"}),
    "double_sigterm": frozenset({"cut", "cut_lo_ms", "cut_hi_ms",
                                 "second_signal_lo_ms", "second_signal_hi_ms"}),
    "sigterm_in_recovery": frozenset({"cut", "cut_lo_ms", "cut_hi_ms",
                                      "recovery_delay_ms"}),
    "admin_pause_resume": frozenset({"cut", "cut_lo_ms", "cut_hi_ms",
                                     "pause_hold_lo_ms", "pause_hold_hi_ms"}),
}
#: The union, for run.SCHEDULE_KEYS["faults"]. It is a superset on purpose:
#: the table-level check admits any key some kind reads, and
#: validate_faults then narrows it to the keys THIS kind reads.
ALL_FAULT_KEYS = frozenset().union(*TRIGGER_FAULT_KEYS.values())
_RANGES = (("cut_lo_ms", "cut_hi_ms"),
           ("second_signal_lo_ms", "second_signal_hi_ms"),
           ("pause_hold_lo_ms", "pause_hold_hi_ms"))
_PROBABILITIES = ("stall_sink_probability", "pause_cut_while_paused_probability")
_NUMERIC = (frozenset(k for pair in _RANGES for k in pair)
            | frozenset(_PROBABILITIES) | {"recovery_delay_ms"})


def _is_number(v):
    # bool is an int subclass: `cut_hi_ms = true` must not load as 1 ms.
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_faults(faults):
    """Refuses an incoherent [faults] table. Raises ValueError."""
    kind = faults.get("kind", "kill_random")
    if kind in LEGACY_KINDS:
        extra = set(faults) - {"kind"}
        if extra:
            raise ValueError(
                f"[faults] keys {sorted(extra)} are lifecycle-trigger keys; "
                f"fault kind {kind!r} reads none of them")
        return
    if kind not in TRIGGER_KINDS:
        raise ValueError(
            f"unknown fault kind {kind!r}; expected one of "
            f"{LEGACY_KINDS + TRIGGER_KINDS}")
    unknown = set(faults) - TRIGGER_FAULT_KEYS[kind]
    if unknown:
        raise ValueError(
            f"[faults] key(s) {sorted(unknown)} are not read by {kind!r}: the run "
            f"would silently ignore them. Accepted: {sorted(TRIGGER_FAULT_KEYS[kind])}")
    missing = REQUIRED_TRIGGER_KEYS[kind] - set(faults)
    if missing:
        raise ValueError(f"[faults] for {kind!r} is missing {sorted(missing)}")
    if faults["cut"] not in CUT_MODES:
        raise ValueError(f"[faults].cut must be one of {CUT_MODES}, got {faults['cut']!r}")
    for key in sorted(_NUMERIC & set(faults)):
        if not _is_number(faults[key]):
            raise ValueError(f"[faults].{key} must be a number, got {faults[key]!r}")
    for lo, hi in _RANGES:
        if lo in faults and not (0 <= faults[lo] <= faults[hi]):
            raise ValueError(f"[faults] needs 0 <= {lo} <= {hi}")
    for p in _PROBABILITIES:
        if p in faults and not (0.0 <= faults[p] <= 1.0):
            raise ValueError(f"[faults].{p} must be within 0..1")
    if kind == "sigterm_in_recovery" and faults["recovery_delay_ms"] <= 0:
        raise ValueError("[faults].recovery_delay_ms must be > 0: without the "
                         "startup delay hook the recovering phase is too short to hit")


def cut_mode(sched):
    """How an episode ends: a dm-flakey power cut or a SIGKILL."""
    f = sched.get("faults") or {}
    kind = f.get("kind", "kill_random")
    if kind == "power_loss":
        return "power_loss"
    if kind in TRIGGER_KINDS:
        return f["cut"]
    return "kill"


def oracle_fault(kind, cut):
    """The fault the ORACLE must judge an episode under. A trigger that ends in
    a power cut is power loss to verify.check_counts, report.powerloss_verdict
    and final_pass's quarantine exemption; the trigger itself is recorded
    separately, under `trigger`."""
    if kind in LEGACY_KINDS:
        return kind
    return "power_loss" if cut == "power_loss" else "kill_random"


#: XORed into the schedule seed so the plan has its OWN random stream:
#: run.kill_delays draws from Random(seed), and sharing it would reshuffle
#: every historical steady-state sequence the moment a trigger draws.
FAULT_PLAN_SALT = 0xF0D5


def fault_plan(seed, count, kind, faults_cfg):
    """One step per episode: when to cut and what the trigger varies.

    Every step holds `cut_after_secs`, plus `stall_sink` for the shutdown
    triggers, `second_signal_secs` for `double_sigterm`, `pause_hold_secs`
    and `cut_while_paused` for `admin_pause_resume`, and `recovery_delay_ms`
    (copied from [faults], not drawn) for `sigterm_in_recovery`.

    Draw order is fixed (second signal, pause hold, cut-while-paused, stall,
    cut), so a seed reproduces its plan exactly. Pure.
    """
    rng = random.Random(seed ^ FAULT_PLAN_SALT)

    def secs(lo, hi):
        return rng.uniform(faults_cfg[lo], faults_cfg[hi]) / 1000.0

    plan = []
    for _ in range(count):
        step = {}
        if kind == "double_sigterm":
            step["second_signal_secs"] = secs("second_signal_lo_ms", "second_signal_hi_ms")
        if kind == "admin_pause_resume":
            step["pause_hold_secs"] = secs("pause_hold_lo_ms", "pause_hold_hi_ms")
            step["cut_while_paused"] = (
                rng.random() < faults_cfg.get("pause_cut_while_paused_probability", 0.0))
        if kind in SHUTDOWN_TRIGGERS:
            step["stall_sink"] = rng.random() < faults_cfg.get("stall_sink_probability", 0.0)
        if kind == "sigterm_in_recovery":
            # A constant, not a draw, so the draw order is unchanged.
            step["recovery_delay_ms"] = int(faults_cfg["recovery_delay_ms"])
        step["cut_after_secs"] = secs("cut_lo_ms", "cut_hi_ms")
        plan.append(step)
    return plan


def power_cut(stack, daemon, episode):
    """One dm-flakey power cut, KILL FIRST (the Phase 2 protocol; see the spec's
    "Protocol: kill first, then drop and remount" and README "The protocol is
    kill-first"). Moved verbatim from run.py's episode loop so the lifecycle
    triggers end in exactly the cut the 6,129-cut corpus was measured with.
    Returns the I6 canary verdict: "bit" / "did_not_bite" / "unexpected".

    PROTOCOL: kill first, then drop and remount. Superseded from the original
    "engage, then kill" — see the spec section above for the full
    kernel-layering argument; summary:

    drop_writes sits BELOW the page cache. A dropped write still leaves a
    correct, resident page behind, and kill -9 does not evict it — so the only
    way to actually lose a byte is drop_and_remount's umount/mount cycle, which
    forces the dirty page out (discarded by the lying disk while engaged) and
    then rebuilds the filesystem from whatever actually reached the platter.

    KILLING FIRST makes the lying window EXACTLY ZERO BY CONSTRUCTION — weir is
    already dead and cannot ack into it — rather than merely narrow. The
    superseded ordering left two subprocess round trips (dmsetup resume plus a
    table read-back) between engage and kill: 5-10ms at full rate, hundreds of
    acks, each one a false Durable-tier I1 violation once C2 made the injector
    actually bite.

    I6: a canary block, written before the fault and overwritten while it's
    engaged, converts the negative control from an INFERENCE (zero Buffered
    loss is merely "suspicious") into a MEASUREMENT — if the overwrite
    survives the umount/remount cycle, the injector did not bite THIS episode,
    independent of anything weir did.
    """
    import run  # lazy: run imports faults
    before = f"weir-chaos-canary ep={episode} phase=pre-fault\n".encode()
    during = f"weir-chaos-canary ep={episode} phase=engaged\n".encode()
    stack.write_canary(before)
    daemon.kill9()
    stack.engage_fault()
    try:
        # Overwritten HERE: inside the engaged window and still mounted, the
        # same conditions weir's own WAB writes are under.
        stack.write_canary(during)
        stack.drop_and_remount(wab_dir=daemon.wab_dir)
    finally:
        # Disengage even if drop_and_remount raised partway through (e.g. its
        # umount step failed while the fault was still live). A device left
        # engaged means the NEXT episode's steady-state load runs against a
        # lying disk, silently corrupting the run from that point on. Unlike
        # the superseded protocol, weir is already dead here, so there is no
        # timing pressure left on this cleanup — a redundant disengage on the
        # success path (drop_and_remount already disengaged) costs one
        # harmless extra round trip, not a false ack.
        stack.disengage_fault()
    return run.canary_verdict(before, during, stack.read_canary())


def cut(mode, stack, daemon, episode):
    """Ends an episode: a power cut (returns the canary) or SIGKILL (None)."""
    if mode == "power_loss":
        return power_cut(stack, daemon, episode)
    if mode == "kill":
        daemon.kill9()
        return None
    raise ValueError(f"unknown cut mode {mode!r}")
