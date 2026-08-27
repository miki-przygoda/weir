#!/usr/bin/env python3
"""Renders an episode log into a markdown report.

Phase 1 reports the essentials: what ran, what broke, and the duplicate rate.
Latency plots, resource curves and the tier x fault matrix arrive in Phases
2-4 as those measurements start existing.

A report that lists only successes is marketing. Limitations are a required
section, not an optional one.

I5: "violation" and "anomaly" are kept strictly separate here, mirroring
run.py's exit gate. A **violation** is a durability failure (I1/I2): weir
lost or leaked a record. An **anomaly** is the harness failing to observe
cleanly — a quiescence timeout, a dead observer, or a no-progress episode —
and must never be read as evidence of a weir defect by itself.

An **advisory** record is one whose verification ran on input the harness
cannot vouch for (a truncated ledger, a drain that was killed, a dead
recorder). Its failure is an anomaly, never a violation — but it is still
rendered as a failure, because silently downgrading it to a pass would be the
same disappearing act the stranded segments already performed.
"""
import json
import os
import subprocess
import sys

#: The `episode` value run.py writes for the end-of-run pass (D1). Every other
#: record carries an int, so nothing here may assume one.
FINAL = "final"

#: Survivor rows rendered in full before the list is elided. `run.py` already
#: truncates the stored list at 50; this keeps the report readable when a run
#: really does leave that many segments behind.
MAX_SURVIVORS_SHOWN = 20


def _dash(value):
    """Renders a missing value as an em dash rather than as a zero.

    An absent count and a count of zero are different claims, and a report
    that renders them identically is the kind that gets believed.
    """
    return "—" if value is None else value


def render_final_pass(final):
    """Renders the end-of-run record (D1) as its own section.

    Everything here is evidence that exists for exactly one moment in a run:
    after the producer stopped and weir's shutdown drain finished, and before
    `stack.teardown()` unmounted and deleted the filesystem it describes.
    """
    lines = ["## Final pass\n"]
    lines.append(
        "One verification pass after the last episode, with the producer "
        "stopped and weir's SIGTERM drain — a **full** drain, not a "
        "seal-and-exit — given a real chance to finish.\n"
    )

    advisory = final.get("advisory")
    slack = final.get("frontier_slack")
    if advisory:
        lines.append(
            f"**This check is ADVISORY.** It ran with the normal frontier slack "
            f"(`{slack}`) instead of zero, because the harness could not vouch "
            "for its own input:\n"
        )
        for reason in final.get("advisory_reasons", []):
            lines.append(f"- {reason}")
        lines.append("")
        lines.append(
            "An advisory failure is counted as an **anomaly, not a violation**: "
            "with a truncated ledger, an unfinished drain or a dead sink, "
            "\"acked but not delivered\" is not a statement about weir.\n"
        )
    else:
        lines.append(
            f"Checked at **`frontier_slack={slack}`** — zero exemption. This is "
            "the only moment in a run where that is a *true* statement rather "
            "than a stricter-than-reality one: the producer is stopped and both "
            "logs are complete, so nothing is legitimately still in flight.\n"
        )

    lines.append("| Teardown step | Result |")
    lines.append("|---|---|")
    lines.append(
        f"| Load generator exit code | {_dash(final.get('loadgen_exit_code'))}"
        f"{' (SIGKILLed)' if final.get('loadgen_forced_kill') else ''} |"
    )
    lines.append(
        f"| Daemon alive before shutdown | "
        f"{_yes_no(final.get('daemon_alive_before_stop'))} |"
    )
    lines.append(
        f"| Shutdown drain completed without a kill | "
        f"{_yes_no(final.get('daemon_clean_stop'))} |"
    )
    lines.append(
        f"| Recorder still answering | {_yes_no(final.get('recorder_alive'))} |"
    )
    m = final.get("final_metrics")
    if m:
        lines.append(
            f"| `/metrics` at shutdown | stranded={_dash(m.get('stranded'))}, "
            f"resumed={_dash(m.get('resumed'))}, "
            f"queue_depth={_dash(m.get('queue_depth'))} |"
        )
    elif final.get("final_metrics_error"):
        lines.append(f"| `/metrics` at shutdown | FAILED: {final['final_metrics_error']} |")
    if final.get("final_check_error"):
        lines.append(f"| Verification | DID NOT RUN: {final['final_check_error']} |")
    lines.append("")

    lines.append("### WAB post-mortem\n")
    residue = final.get("wab_residue")
    if residue is None:
        lines.append(
            "**The WAB directory was not scanned.** "
            f"{final.get('wab_scan_error', 'No reason recorded.')} Whatever weir "
            "left behind was deleted, unread, by teardown.\n"
        )
        return lines

    survivors = final.get("wab_survivors", [])
    count = final.get("wab_survivor_count", len(survivors))
    if not count:
        lines.append(
            "The WAB directory was empty of backlog after shutdown: no sealed "
            "segment without a `.confirmed` sidecar, no non-empty active "
            "segment, nothing quarantined, nothing dead-lettered.\n"
        )
        return lines

    lines.append(
        f"**{count} file(s) survived weir's shutdown drain.** Each one is an "
        "anomaly: a sealed segment with no `.confirmed` sidecar or a non-empty "
        "active segment is undelivered work; `quarantine/` means weir found "
        "corruption; `dead_letter/` means it gave up on records a healthy sink "
        "should have taken. Counted: "
        f"unconfirmed_sealed={residue.get('unconfirmed_sealed', 0)}, "
        f"nonempty_active={residue.get('nonempty_active', 0)}, "
        f"quarantined={residue.get('quarantined', 0)}, "
        f"dead_letter={residue.get('dead_letter', 0)}.\n"
    )
    lines.append("| Kind | Path | Bytes |")
    lines.append("|---|---|---|")
    for s in survivors[:MAX_SURVIVORS_SHOWN]:
        lines.append(f"| {s.get('kind', '?')} | `{s.get('path', '?')}` | {s.get('size', '?')} |")
    lines.append("")
    if count > len(survivors[:MAX_SURVIVORS_SHOWN]):
        lines.append(
            f"({count - len(survivors[:MAX_SURVIVORS_SHOWN])} more not shown.)\n"
        )
    return lines


def _yes_no(value):
    if value is None:
        return "—"
    return "yes" if value else "**NO**"


def _power_loss_records(records):
    """Episodes that actually injected power loss.

    Excludes the pre-fault abort record (MINOR, final review): `run.py`
    writes `"fault": kind` on it even though the run died BEFORE ever
    reaching `engage_fault()` — it never actually injected anything, and
    counting it here would render a Power-loss verdict section, and a
    canary/expected_loss figure, for a fault that never fired. Every other
    caller of `fault == "power_loss"` filtering in this module goes through
    here so the exclusion cannot drift between them.
    """
    return [
        r for r in records
        if r.get("fault") == "power_loss" and not r.get("abort_reason")
    ]


def powerloss_verdict(records):
    """pass | inconclusive | fail for a power-loss run.

    Any durability failure among this run's power-loss episodes fails this
    outright: an I1 miss (an acked record never delivered, outside the
    Buffered exemption — `kill -9` and power loss alike hold Durable to zero
    loss), an I2 leak (a nacked record delivered anyway), or a ledger
    conflict (corrupt oracle input, so the episode's result can't be trusted
    either way). Previously this checked `i1_missing` alone (MINOR, final
    review), so an I2 leak or ledger conflict could render `pass` here while
    the run's headline violation count already disagreed.

    A Buffered run that lost NOTHING across every power-loss episode is
    `inconclusive`, not `pass`. Under a correct power-loss model Buffered
    should lose something — it acks before fsync — so losing nothing
    suggests the injector never bit. Reporting that as success is how a
    chaos harness starts lying: a test that cannot fail proves nothing.

    C4: "lost nothing" is read from the LAST verified Buffered power-loss
    record (which — since I1's final-pass fix — includes the final pass when
    this run had one, the most authoritative measurement there is), not a
    sum across records. `expected_loss` is a currently-still-missing count,
    not a per-episode delta: a record lost in episode 0 stays in it for
    every later check, so summing would inflate the figure by roughly the
    number of episodes remaining after the loss.

    The suspicion rule is Buffered-only. Durable losing nothing is the
    contract being upheld, not evidence of a dead injector.

    A run with no power-loss episodes at all (e.g. a Phase 1 kill_random
    run) is a `pass`: this rule has nothing to say about it.
    """
    pl = _power_loss_records(records)
    if not pl:
        return "pass"
    if any(r.get("i1_missing") or r.get("i2_leaked") or r.get("ledger_conflicts")
           for r in pl):
        return "fail"
    buffered = [r for r in pl if r.get("tier") == "U"]
    if buffered and buffered[-1].get("expected_loss", 0) == 0:
        return "inconclusive"
    return "pass"


def _fault_kinds(records):
    """Which fault kind(s) this run actually injected.

    Excludes the pre-fault abort record (carries the SCHEDULED kind, never
    actually reached `engage_fault()`/`kill9()`) and the `None`/`"none"`
    placeholder the final pass used to carry before I1's final-review fix.
    Used to derive the report's title and its Limitations bullet (I2, final
    review) instead of a hardcoded "Phase 1", which used to contradict a
    Phase 2 (power-loss) run's own Power-loss verdict section two lines
    below it.
    """
    return {
        r.get("fault") for r in records
        if r.get("fault") not in (None, "none") and not r.get("abort_reason")
    }


def render(episodes, meta):
    """Renders episodes (list of dicts) into markdown."""
    fault_kinds = _fault_kinds(episodes)
    fault_episodes = [e for e in episodes if e.get("episode") != FINAL]
    final = next((e for e in episodes if e.get("episode") == FINAL), None)
    total = len(fault_episodes)
    # Advisory records are excluded: their input could not be vouched for, so
    # their failure is a harness finding, not a durability one. run.py's exit
    # gate makes the same split, and the two must not drift.
    violations = [
        e for e in episodes if not e.get("ok", True) and not e.get("advisory")
    ]
    # An episode aborted before the fault (a dead observer, `abort_reason`
    # set) never ran a quiescence wait at all, so it must not be counted
    # among episodes that waited and timed out — those are two different
    # findings with two different remedies.
    unquiesced = [
        e for e in episodes
        if not e.get("quiesced", True) and not e.get("abort_reason")
    ]
    no_progress_eps = [e for e in episodes if e.get("no_progress")]
    # An anomaly is anything that makes a record's verdict mean "the harness
    # didn't get a clean look", not "weir is broken": a quiescence timeout, an
    # observer dying (abort_reason), a no-progress episode, an advisory
    # verdict, or any of the teardown-time findings the final pass reports in
    # `anomaly_reasons` (a killed shutdown drain, surviving WAB files, a
    # failed last scrape). A record counts ONCE however many apply, because
    # run.py counts it once too.
    anomalies = [
        e for e in episodes
        if not e.get("quiesced", True) or e.get("abort_reason")
        or e.get("no_progress") or e.get("advisory") or e.get("anomaly_reasons")
    ]

    lines = []
    if fault_kinds == {"power_loss"}:
        phase_title = "Phase 2 (power loss)"
    elif "power_loss" in fault_kinds:
        phase_title = "Phase 2 (mixed faults)"
    else:
        phase_title = "Phase 1 (spine)"
    lines.append(f"# weir chaos run — {phase_title}\n")

    lines.append("## Run metadata\n")
    for key in ("weir_commit", "kernel", "hardware", "filesystem", "seed", "duration"):
        if meta.get(key):
            lines.append(f"- **{key.replace('_', ' ').title()}:** {meta[key]}")
    lines.append("")

    v_word = "violation" if len(violations) == 1 else "violations"
    a_word = "anomaly" if len(anomalies) == 1 else "anomalies"
    lines.append("## Result\n")
    e_word = "episode" if total == 1 else "episodes"
    final_phrase = " plus a final verification pass" if final else ""
    lines.append(
        f"**{total} {e_word}{final_phrase}, {len(violations)} {v_word}, "
        f"{len(anomalies)} {a_word}.**\n"
    )
    lines.append(
        "A violation is a durability failure (I1/I2): weir lost or leaked a "
        "record. An anomaly is the harness failing to observe an episode "
        "cleanly — a quiescence timeout, a dead observer, or no measurable "
        "progress — and is **not**, by itself, evidence of a weir defect.\n"
    )
    if final is None and episodes:
        # Never silently. The final pass is the only moment in a run when the
        # producer is stopped and the drain has finished, so its absence means
        # the run's most complete evidence was never collected — and, before
        # D1, deleted unread by `stack.teardown()`.
        lines.append(
            "**No final verification pass is recorded for this run.** Either it "
            "predates that phase, or the run died before reaching it. Nothing "
            "below reflects weir's shutdown drain, and no WAB post-mortem was "
            "taken before teardown deleted the filesystem.\n"
        )
    if unquiesced:
        lines.append(
            f"{len(unquiesced)} episode(s) did not reach drain quiescence within "
            "the timeout. A drain that never quiesces is itself a finding.\n"
        )
    if no_progress_eps:
        lines.append(
            f"{len(no_progress_eps)} episode(s) made no progress: the acked "
            "and/or delivered delta fell below the schedule's floor "
            "(`min_acked_per_episode` / `min_delivered_per_episode`). I1/I2 "
            "are set-containment checks that are vacuously satisfied when "
            "nothing was acked, so this is what catches a weir that refuses "
            "or delivers nothing instead of letting it read as a clean pass. "
            "Check the episode's `nacked`/`pushed` figures below: a run that "
            "is pushing but never acking is a very different finding from "
            "one where load itself stalled.\n"
        )

    aborted = [e for e in episodes if e.get("abort_reason")]
    if aborted:
        lines.append(
            f"**Run aborted early at episode {aborted[0].get('episode')}: "
            f"`{aborted[0]['abort_reason']}`.** The run stopped because "
            "continuing would have produced passing episodes that verified an "
            "idle daemon. Every episode after this point is absent, not passing.\n"
        )

    # The negative control (Task 7). Rendered only when this run actually
    # injected power loss — a Phase 1 kill_random report has nothing to say
    # here and must not grow a section about a rule that does not apply to it.
    # _power_loss_records excludes the pre-fault abort record (MINOR, final
    # review) — an aborted run that never reached engage_fault() must not
    # render this section at all.
    pl_records = _power_loss_records(episodes)
    if pl_records:
        verdict = powerloss_verdict(episodes)
        buffered_pl = [e for e in pl_records if e.get("tier") == "U"]
        # C4: the LAST verified record's expected_loss, not a sum — see
        # powerloss_verdict's docstring for why summing inflates it.
        buffered_loss = buffered_pl[-1].get("expected_loss", 0) if buffered_pl else 0
        lines.append("## Power-loss verdict\n")
        lines.append(
            f"**{verdict.upper()}.** Buffered `expected_loss` as of the last "
            f"verified power-loss record: **{buffered_loss}**.\n"
        )
        # I6: the canary measurement, when this run recorded one. Converts
        # "Buffered lost nothing" from an inference (maybe the injector never
        # bit) into a direct per-episode measurement of whether it did.
        canaries = [e.get("canary") for e in pl_records if e.get("canary")]
        if canaries:
            bit = sum(1 for c in canaries if c == "bit")
            did_not_bite = sum(1 for c in canaries if c == "did_not_bite")
            unexpected = sum(1 for c in canaries if c == "unexpected")
            detail = ""
            if did_not_bite:
                detail += f", did not bite in {did_not_bite}"
            if unexpected:
                detail += f", unexpected in {unexpected}"
            lines.append(
                f"**Canary:** bit in {bit}/{len(canaries)} episode(s){detail}. "
                "A known block is written before the fault and overwritten "
                "while it's engaged, then read back after the remount: if "
                "the OVERWRITE survived, the injector did not bite THIS "
                "episode — independent of anything weir did. This is the "
                "first calibration run's pass condition: every power-loss "
                "episode's canary should read `bit`.\n"
            )
        if verdict == "inconclusive":
            lines.append(
                "Buffered lost NOTHING across every power-loss episode in this "
                "run. Under a correct power-loss model Buffered should lose "
                "something — it acks after the in-memory write, before any "
                "fsync — so a result of exactly zero is read as **suspicious, "
                "not as success**: it suggests the injector was not actually "
                "active, not that weir is unusually durable. "
                "See `report.powerloss_verdict`.\n"
            )
        elif verdict == "fail":
            lines.append(
                "A Durable record was lost under power loss. Durable is held "
                "to zero loss under every fault class this harness injects, "
                "`kill -9` and power loss alike — this is a genuine "
                "durability violation, not the Buffered exemption.\n"
            )
        elif buffered_pl:
            lines.append(
                "Buffered lost at least one record somewhere in this run, and "
                "no Durable record was lost — the expected shape of a "
                "power-loss result: Buffered's documented contract paying "
                "out, Durable's held.\n"
            )
        else:
            lines.append(
                "No Durable record was lost under power loss — the contract "
                "held. (This run recorded no Buffered power-loss episodes, so "
                "the negative control does not apply.)\n"
            )

    # Totals as of the LAST verified episode, not a sum across episodes:
    # verify.Accumulator is cumulative, so summing would inflate every figure
    # by roughly episode-count/2 (20 episodes ending at 20,000 acked would
    # render as 210,000). Episodes without verification data (e.g. the abort
    # record written when an observer died before a check ran) are skipped
    # when picking the "last" one, so an abort never zeroes out real totals.
    verified = [e for e in episodes if "acked" in e]
    if verified:
        last = verified[-1]
        acked = last.get("acked", 0)
        distinct = last.get("delivered_distinct", 0)
        unknown = last.get("unknown", 0)
        dup_rate = last.get("duplicate_rate", 0.0)
        lines.append("## Totals\n")
        lines.append(
            "Run totals as of the last verified record (cumulative — NOT a sum "
            "across episodes). When a final pass ran, that is the record they "
            "come from: it is the most complete view of the run, taken after "
            "weir's shutdown drain delivered everything it still held.\n"
        )
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        lines.append(f"| Acked records | {acked} |")
        lines.append(f"| Distinct delivered | {distinct} |")
        lines.append(f"| Unknown (indeterminate) | {unknown} |")
        lines.append(
            f"| Deliveries per distinct record (1.000 = no redelivery) | "
            f"{dup_rate:.3f} |"
        )
        lines.append("")
        lines.append(
            "This is a **multiplicity factor**, not a percentage: 1.000 means "
            "no redelivery, 2.000 means every record arrived twice on "
            "average. At-least-once delivery makes duplicates conformant — "
            "this is what a crash actually costs a sink that has to dedupe, "
            "which weir's own docs require but never quantify.\n"
        )

    orphaned = len(last.get("orphaned_delivered", [])) if verified else 0
    conflicts = len(last.get("ledger_conflicts", [])) if verified else 0
    if orphaned or conflicts:
        lines.append("## Provenance anomalies\n")
        if orphaned:
            lines.append(
                f"{orphaned} delivered record(s) had no ledger entry. These are NOT "
                "durability violations and are excluded from the duplicate rate. The "
                "likeliest cause is a stale delivery log from an earlier run of this "
                "seed, since the run id derives from it.\n"
            )
        if conflicts:
            lines.append(
                f"{conflicts} sequence number(s) appeared under two different ledger "
                "tags. That is corruption of the oracle's own input, so the affected "
                "episodes fail — but it is a harness finding, not a weir one.\n"
            )

    if final is not None:
        lines.extend(render_final_pass(final))

    lines.append("## Episodes\n")
    lines.append(
        "Every count column is a per-episode DELTA of a cumulative total — "
        "acked, delivered, nacked and pushed alike, so the row is internally "
        "consistent and no column silently means something different from the "
        "one beside it. (Nacked/Pushed were cumulative here until D3, sitting "
        "next to deltas.) I1 exempt / Pending prov. are the frontier-exemption "
        "counts from the same check (see Provenance anomalies above): how many "
        "would-be I1/orphan hits were excused as not-yet-caught-up rather than "
        "lost. Expected loss (I3) is the Buffered-under-power_loss exemption's "
        "size (see the Power-loss verdict section above, when present) — 0 for "
        "every other tier/fault combination. The `final` row is the end-of-run "
        "pass, not an episode: no fault was injected and no quiescence wait "
        "ran, hence `n/a`.\n"
    )
    lines.append(
        "| # | Fault | Quiesced | Verdict | Acked Δ | Delivered Δ | Dup rate | "
        "Unknown | Nacked Δ | Pushed Δ | I1 exempt | Pending prov. | "
        "Expected loss | Notes |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for e in episodes:
        notes = []
        if e.get("no_progress"):
            notes.append("no-progress")
        if not e.get("quiesced", True):
            notes.append("quiescence timeout")
        if e.get("abort_reason"):
            notes.append(f"aborted ({e['abort_reason']})")
        # The final pass reports its teardown-time findings here — a killed
        # shutdown drain, surviving WAB files, a dead recorder. Without this
        # they would land in episodes.jsonl and never reach the report, which
        # is precisely the disappearing act D1 exists to stop.
        notes.extend(e.get("anomaly_reasons", []))
        if e.get("advisory") and not e.get("anomaly_reasons"):
            notes.append(
                "advisory: " + "; ".join(e.get("advisory_reasons", ["unspecified"]))
            )
        # I6: only flag a SURPRISING canary result — "bit" is the expected
        # outcome of every power-loss episode and would clutter every row.
        if e.get("fault") == "power_loss" and e.get("canary") not in (None, "bit"):
            notes.append(f"canary={e['canary']}")
        # Four verdicts, not two. A record with no durability violation but an
        # anomaly is NOT a clean pass — reading "PASS" beside a no-progress or
        # unquiesced row is exactly the false reassurance this report exists to
        # avoid. FAIL means weir lost or leaked a record; ADVISORY FAIL means
        # the check failed but ran on input the harness cannot vouch for, so it
        # is not attributable to weir; ANOMALY means the harness did not
        # observe cleanly.
        if not e.get("ok"):
            verdict = "ADVISORY FAIL" if e.get("advisory") else "**FAIL**"
        elif notes:
            verdict = "ANOMALY"
        else:
            verdict = "PASS"
        # No cumulative fallback for the deltas: showing a running total under
        # a "Δ" heading is the same silent mislabelling that inflated the
        # totals table by ~10x. If a delta is genuinely absent, say so.
        # i1_exempt/pending_provenance are already per-check counts (I3), not
        # cumulative, so no such caveat applies to them.
        # `quiesced` is tri-state: True, False, or ABSENT — the final pass ran
        # no quiescence wait, and rendering a missing key as "NO" would invent
        # a timeout that never happened.
        if "quiesced" in e:
            quiesced = "yes" if e["quiesced"] else "NO"
        else:
            quiesced = "n/a"
        lines.append(
            f"| {e.get('episode')} | {e.get('fault', '?')} | {quiesced} | "
            f"{verdict} | {_dash(e.get('acked_delta'))} | "
            f"{_dash(e.get('delivered_delta'))} | "
            f"{e.get('duplicate_rate', 0.0):.3f} | {e.get('unknown', 0)} | "
            f"{_dash(e.get('nacked_delta'))} | {_dash(e.get('pushed_delta'))} | "
            f"{_dash(e.get('i1_exempt'))} | {_dash(e.get('pending_provenance'))} | "
            f"{_dash(e.get('expected_loss'))} | "
            f"{', '.join(str(n) for n in notes) or '—'} |"
        )
    lines.append("")

    if violations:
        lines.append("## Violations\n")
        for e in violations:
            lines.append(f"### episode {e.get('episode')} — {e.get('fault', '?')}\n")
            if e.get("i1_missing"):
                lines.append(
                    f"**I1 — acked but never delivered** ({len(e['i1_missing'])} shown, "
                    f"truncated at 50): `{e['i1_missing']}`\n"
                )
            if e.get("i2_leaked"):
                lines.append(
                    f"**I2 — nacked but delivered** ({len(e['i2_leaked'])}): "
                    f"`{e['i2_leaked']}`\n"
                )
            lines.append(f"Reproducer: seed `{hex(e.get('seed', 0))}`, episode {e.get('episode')}\n")

    # I2 (final review): derived from what this run actually injected, not
    # hardcoded — a hardcoded "Phase 1 injects random SIGKILL only... power
    # loss... not covered by this run" printed directly under this same
    # run's own "## Power-loss verdict" section is a report contradicting
    # itself.
    if fault_kinds == {"power_loss"}:
        scope_bullet = (
            "- This run injects **simulated power loss** (`dm-flakey "
            "drop_writes`), not random SIGKILL. Targeted mid-fsync kills, "
            "torn writes, disk-full, slow disk, read-only remount and "
            "dead-letter exhaustion remain **out of scope** for this run.\n"
        )
    elif "power_loss" in fault_kinds:
        scope_bullet = (
            "- This run injects **random SIGKILL and simulated power loss** "
            "(`dm-flakey drop_writes`). Targeted mid-fsync kills, torn "
            "writes, disk-full, slow disk, read-only remount and "
            "dead-letter exhaustion remain **out of scope** for this run.\n"
        )
    else:
        scope_bullet = (
            "- This run injects **random SIGKILL only**. Simulated power "
            "loss, targeted mid-fsync kills, torn writes, disk-full, slow "
            "disk, read-only remount and dead-letter exhaustion are **not "
            "covered** by this run.\n"
        )
    lines.append("## Limitations\n")
    lines.append(
        scope_bullet +
        "- Invariant I1 is **tier- and fault-aware** (Phase 2): a Buffered ack "
        "(`tier=\"U\"`) is exempted from I1 ONLY under simulated power loss "
        "(`fault=\"power_loss\"`) — `kill -9` still holds every tier to zero "
        "loss. The exemption's size is reported as `expected_loss`, never "
        "silently dropped, and a Buffered power-loss run losing exactly "
        "nothing is flagged INCONCLUSIVE rather than read as a pass (see "
        "Power-loss verdict above, when present).\n"
        "- The seed reproduces the **schedule**, not the exact interleaving. Real "
        "kernel, real timing, real I/O — full determinism is not claimed.\n"
        "- Single host, single filesystem, single hardware configuration.\n"
    )
    return "\n".join(lines)


def load_episodes(run_dir):
    """Reads run_dir/episodes.jsonl into a list of dicts, `[]` if absent."""
    episodes = []
    ep_path = os.path.join(run_dir, "episodes.jsonl")
    if os.path.exists(ep_path):
        with open(ep_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    episodes.append(json.loads(line))
    return episodes


def commit_label(commit, porcelain):
    """Labels a commit with whether the tree that ran actually WAS that commit.

    A bare HEAD is a lie whenever the tree is dirty, and rsyncing a working
    tree onto a test box while leaving its .git behind makes that the normal
    case rather than the exception. The 2026-08-22 10h soak reported
    `b49f341` while running six modified files on top of it — including the
    recovery path the run was partly there to exercise. A durability result
    attributed to code that never ran is worse than one carrying no commit at
    all, because it looks citable.

    `porcelain` is `git status --porcelain --untracked-files=no` output:
    tracked modifications only, so ignored run output cannot flip the flag.
    """
    if not commit:
        return commit
    dirt = [ln for ln in porcelain.splitlines() if ln.strip()]
    if not dirt:
        return commit
    n = len(dirt)
    return f"{commit}-dirty ({n} tracked file{'' if n == 1 else 's'} modified)"


def gather_meta(episodes):
    """Best-effort run metadata: the weir commit, kernel, and the run's seed."""
    meta = {}
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        porcelain = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            capture_output=True, text=True,
        ).stdout
        meta["weir_commit"] = commit_label(commit, porcelain)
        meta["kernel"] = subprocess.run(
            ["uname", "-r"], capture_output=True, text=True
        ).stdout.strip()
    except Exception:
        pass
    if episodes:
        meta["seed"] = hex(episodes[0].get("seed", 0))
    return meta


def write_report(run_dir):
    """Renders run_dir/episodes.jsonl into run_dir/report.md. Returns its path.

    Shared by this module's CLI entry point and run.py, which calls this at
    the end of every run so rendering the report is no longer a manual step
    in the exit gate.
    """
    episodes = load_episodes(run_dir)
    meta = gather_meta(episodes)
    out_path = os.path.join(run_dir, "report.md")
    with open(out_path, "w") as f:
        f.write(render(episodes, meta))
    return out_path


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: report.py <run_dir>")
    out_path = write_report(sys.argv[1])
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
