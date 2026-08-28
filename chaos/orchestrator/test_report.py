"""Tests for report rendering."""
import unittest

import report

EPISODES = [
    {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
     "acked": 1000, "delivered_distinct": 1000, "acked_delta": 1000,
     "delivered_delta": 1000, "no_progress": False, "duplicate_rate": 1.02,
     "unknown": 3, "i1_missing": [], "i2_leaked": [], "seed": 24301},
    {"episode": 1, "fault": "kill_random", "ok": False, "quiesced": True,
     "acked": 2000, "delivered_distinct": 1998, "acked_delta": 1000,
     "delivered_delta": 998, "no_progress": False, "duplicate_rate": 1.01,
     "unknown": 5, "i1_missing": [17, 42], "i2_leaked": [], "seed": 24301},
]


def final_record(**overrides):
    """A clean end-of-run record, as `run.final_pass` writes it."""
    base = {
        "episode": "final", "fault": "none", "seed": 24301,
        "loadgen_exit_code": 0, "loadgen_forced_kill": False,
        "daemon_alive_before_stop": True, "daemon_clean_stop": True,
        "recorder_alive": True, "advisory": False, "frontier_slack": 0,
        "ok": True, "acked": 3000, "delivered_distinct": 3000,
        "acked_delta": 1000, "delivered_delta": 1002, "duplicate_rate": 1.0,
        "unknown": 0, "nacked": 0, "pushed": 3000, "nacked_delta": 0,
        "pushed_delta": 1000, "i1_missing": [], "i2_leaked": [],
        "i1_exempt": 0, "pending_provenance": 0,
        "final_metrics": {"stranded": 4.0, "resumed": 4.0, "queue_depth": 0.0},
        "wab_residue": {"unconfirmed_sealed": 0, "nonempty_active": 0,
                        "quarantined": 0, "dead_letter": 0},
        "wab_survivors": [], "wab_survivor_count": 0, "anomaly_reasons": [],
    }
    base.update(overrides)
    return base


class TestRender(unittest.TestCase):
    def test_headline_states_the_violation_count(self):
        out = report.render(EPISODES, {"weir_commit": "abc123", "kernel": "6.8.0"})
        self.assertIn("1 violation", out)
        self.assertIn("abc123", out)
        self.assertIn("6.8.0", out)

    def test_violations_are_listed_with_reproducers(self):
        out = report.render(EPISODES, {})
        self.assertIn("episode 1", out)
        self.assertIn("17", out)
        self.assertIn("seed", out.lower())

    def test_clean_run_says_so_plainly(self):
        out = report.render([EPISODES[0]], {})
        self.assertIn("0 violations", out)
        self.assertIn("0 anomalies", out)

    def test_duplicate_rate_is_reported_with_units_not_as_a_bare_mean(self):
        # I5 minor: the table cell used to say "Duplicate rate (mean)" with
        # no unit, which reads like a percentage. It is a multiplicity
        # factor, and since I1 (report totals from the last episode, not a
        # sum) there is no averaging left to call "(mean)" either.
        out = report.render(EPISODES, {})
        self.assertIn("Deliveries per distinct record", out)
        self.assertNotIn("(mean)", out)

    def test_totals_come_from_the_last_episode_not_a_sum(self):
        # I1: verify.Accumulator is cumulative. Summing per-episode figures
        # across episodes would inflate them (20 episodes ending at 20,000
        # acked would render as 210,000) — totals must reflect only the last
        # episode's cumulative counts.
        out = report.render(EPISODES, {})
        self.assertIn("2000", out)
        self.assertNotIn("3000", out, "totals must not sum acked across episodes")

    def test_empty_run_does_not_crash(self):
        out = report.render([], {})
        self.assertIn("0 episodes", out)

    def test_an_aborted_run_says_so_prominently(self):
        # A vacuous-pass guard firing must never be buried in a table cell.
        # "ok": True and "exit_code" (not "loadgen_exit_code") mirror what
        # run.py actually writes for an observer-death abort: no durability
        # check ran, so there is no violation, only an anomaly.
        aborted = [
            EPISODES[0],
            {"episode": 1, "fault": "kill_random", "ok": True, "quiesced": False,
             "abort_reason": "loadgen_exited", "exit_code": 1, "seed": 24301},
        ]
        out = report.render(aborted, {})
        self.assertIn("aborted early", out)
        self.assertIn("loadgen_exited", out)
        self.assertIn("absent, not passing", out)
        # I5: an observer dying is an anomaly, not a durability violation.
        self.assertIn("0 violations", out)
        self.assertIn("1 anomaly", out)

    def test_the_episode_table_renders_nacked_pushed_and_provenance_fields(self):
        # Important: the prose says "Check the episode's nacked/pushed figures
        # below" and claims the exempted count (i1_exempt/pending_provenance)
        # is reported — but until this fix the episode table carried none of
        # them. Whatever the prose points readers at must actually be there.
        episodes = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked": 5000, "delivered_distinct": 5000, "acked_delta": 5000,
             "delivered_delta": 5000, "no_progress": False, "duplicate_rate": 1.0,
             "unknown": 0, "nacked": 17, "pushed": 5017, "nacked_delta": 17,
             "pushed_delta": 5017, "i1_exempt": 3, "pending_provenance": 2,
             "i1_missing": [], "i2_leaked": [], "seed": 24301},
        ]
        out = report.render(episodes, {})
        self.assertIn("Nacked", out)
        self.assertIn("Pushed", out)
        self.assertIn("I1 exempt", out)
        self.assertIn("Pending prov", out)
        # The actual values from the episode record must appear, not just the
        # column headings.
        self.assertIn("| 17 |", out)
        self.assertIn("| 5017 |", out)
        self.assertIn("| 3 |", out)
        self.assertIn("| 2 |", out)

    def test_nacked_and_pushed_render_as_deltas_beside_the_other_deltas(self):
        # D3: they used to render CUMULATIVE totals under "(cum.)" headings,
        # sitting next to per-episode deltas in the same row. Internally
        # inconsistent, and exactly the mislabelling that inflated the totals
        # table by ~10x before it was fixed there.
        episodes = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked_delta": 5000, "delivered_delta": 5000, "duplicate_rate": 1.0,
             "unknown": 0, "nacked": 40, "pushed": 5040, "nacked_delta": 40,
             "pushed_delta": 5040, "seed": 1},
            {"episode": 1, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked_delta": 5000, "delivered_delta": 5000, "duplicate_rate": 1.0,
             "unknown": 0, "nacked": 81, "pushed": 10081, "nacked_delta": 41,
             "pushed_delta": 5041, "seed": 1},
        ]
        out = report.render(episodes, {})
        self.assertIn("Nacked Δ", out)
        self.assertIn("Pushed Δ", out)
        self.assertNotIn("(cum.)", out)
        self.assertIn("| 41 |", out)
        self.assertIn("| 5041 |", out)
        self.assertNotIn(
            "| 10081 |", out,
            "the cumulative total must not appear under a Δ heading",
        )


class TestFinalPassSection(unittest.TestCase):
    """D1's findings have to reach the report. Landing them in
    episodes.jsonl and nowhere else is the same disappearing act the
    stranded segments already performed."""

    def test_a_non_integer_episode_key_does_not_crash_the_report(self):
        out = report.render([EPISODES[0], final_record()], {})
        self.assertIn("| final |", out)

    def test_the_final_row_is_not_counted_as_an_episode(self):
        out = report.render(EPISODES + [final_record()], {})
        self.assertIn("2 episodes plus a final verification pass", out)
        out = report.render([EPISODES[0], final_record()], {})
        self.assertIn("1 episode plus a final verification pass", out)

    def test_the_final_pass_reports_the_zero_slack_claim_and_its_basis(self):
        out = report.render([EPISODES[0], final_record()], {})
        self.assertIn("frontier_slack=0", out)
        self.assertIn("only moment in a run", out)
        self.assertIn("0 violations", out)
        self.assertIn("0 anomalies", out)

    def test_the_final_row_shows_no_quiescence_verdict_rather_than_a_failed_one(self):
        # The final pass runs no quiescence wait. Rendering the missing key as
        # "NO" would invent a timeout that never happened.
        out = report.render([final_record()], {})
        self.assertIn("| n/a |", out)
        self.assertNotIn("did not reach drain quiescence", out)

    def test_totals_come_from_the_final_pass_when_there_is_one(self):
        out = report.render([EPISODES[0], final_record()], {})
        self.assertIn("| Acked records | 3000 |", out)

    def test_an_advisory_failure_is_an_anomaly_not_a_violation(self):
        # A truncated ledger, an unfinished drain or a dead sink all make
        # "acked but not delivered" a statement about the harness, not weir.
        final = final_record(
            ok=False, advisory=True, frontier_slack=2048,
            loadgen_exit_code=1,
            advisory_reasons=["the load generator exited dirty (code=1, "
                              "killed=False), so its ledger tail may be truncated"],
            anomaly_reasons=["loadgen_dirty_exit(code=1, killed=False)",
                             "advisory_check_failed"],
            i1_missing=[900, 901],
        )
        out = report.render([EPISODES[0], final], {})
        self.assertIn("0 violations", out)
        self.assertIn("1 anomaly", out)
        self.assertIn("ADVISORY", out)
        self.assertIn("ledger tail may be truncated", out)
        self.assertNotIn(
            "**FAIL**", out,
            "an advisory failure must not read as a durability violation",
        )

    def test_a_non_advisory_final_failure_is_a_violation(self):
        final = final_record(ok=False, i1_missing=[7])
        out = report.render([final], {})
        self.assertIn("1 violation", out)
        self.assertIn("**FAIL**", out)

    def test_surviving_wab_files_reach_the_report_with_paths_and_sizes(self):
        final = final_record(
            wab_residue={"unconfirmed_sealed": 1, "nonempty_active": 0,
                         "quarantined": 0, "dead_letter": 2},
            wab_survivors=[
                {"kind": "unconfirmed_sealed",
                 "path": "/mnt/weir-wab/wab/shard_02/seg_00000041.wab.sealed",
                 "size": 8388608},
                {"kind": "dead_letter",
                 "path": "/mnt/weir-wab/wab/dead_letter/dl_3.wab", "size": 1024},
            ],
            wab_survivor_count=3,
            anomaly_reasons=["wab_survivors=3"],
        )
        out = report.render([final], {})
        self.assertIn("WAB post-mortem", out)
        self.assertIn("seg_00000041.wab.sealed", out)
        self.assertIn("8388608", out)
        self.assertIn("dl_3.wab", out)
        self.assertIn("1 anomaly", out)
        self.assertIn("1 more not shown", out)

    def test_quarantined_segments_under_power_loss_are_not_called_anomalies(self):
        # The report must agree with the anomaly rule in run.final_pass. Saying
        # "each one is an anomaly" about files the harness just classified as
        # the expected outcome is the report/exit-code contradiction C5 exists
        # to prevent — the run exits 0 while its own report reads as a finding.
        final = final_record(
            fault="power_loss",
            wab_residue={"unconfirmed_sealed": 0, "nonempty_active": 0,
                         "quarantined": 2, "dead_letter": 0},
            wab_survivors=[
                {"kind": "quarantine",
                 "path": "/mnt/weir-wab/wab/quarantine/shard_00__seg_00000018.wab",
                 "size": 0},
                {"kind": "quarantine",
                 "path": "/mnt/weir-wab/wab/quarantine/shard_01__seg_00000018.wab",
                 "size": 0},
            ],
            wab_survivor_count=2,
            anomaly_reasons=[],
        )
        out = report.render([final], {})
        self.assertIn("WAB post-mortem", out)
        # Still listed — exempt from the alarm, not from the record.
        self.assertIn("shard_00__seg_00000018.wab", out)
        self.assertIn("shard_01__seg_00000018.wab", out)
        self.assertIn("0 anomalies", out)
        self.assertNotIn("Each one is an anomaly", out)

    def test_quarantined_segments_under_kill_random_are_still_anomalies(self):
        final = final_record(
            fault="kill_random",
            wab_residue={"unconfirmed_sealed": 0, "nonempty_active": 0,
                         "quarantined": 1, "dead_letter": 0},
            wab_survivors=[
                {"kind": "quarantine",
                 "path": "/mnt/weir-wab/wab/quarantine/shard_00__seg_00000018.wab",
                 "size": 0},
            ],
            wab_survivor_count=1,
            anomaly_reasons=["wab_survivors=1"],
        )
        out = report.render([final], {})
        self.assertIn("shard_00__seg_00000018.wab", out)
        self.assertIn("1 anomaly", out)

    def test_orphans_are_explained_by_flush_lag_not_by_a_stale_log(self):
        # The old text blamed "a stale delivery log from an earlier run of this
        # seed". That is impossible by construction — the harness refuses to
        # start when ledger.log, delivered.log OR episodes.jsonl is non-empty —
        # and it is contradicted by the evidence: over the 6h Buffered soak all
        # 9 orphan episodes of 397 were back to zero on the NEXT episode, which
        # a stale log would never do. The real cause is loadgen's buffered
        # ledger writes briefly outrunning the frontier window.
        final = final_record(orphaned_delivered=[11, 22, 33])
        out = report.render([final], {})
        self.assertIn("Provenance anomalies", out)
        self.assertIn("3 delivered record(s)", out)
        self.assertNotIn("stale delivery log", out)
        self.assertIn("frontier window", out)
        # The distinction that makes the number actionable at 3am.
        self.assertIn("Transient is benign; persistent is not", out)

    def test_a_killed_shutdown_drain_is_surfaced(self):
        final = final_record(
            daemon_clean_stop=False, advisory=True, frontier_slack=2048,
            advisory_reasons=["the shutdown drain overran its budget and the "
                              "daemon was killed, so undrained segments are expected"],
            anomaly_reasons=["daemon_kill_at_stop"],
        )
        out = report.render([final], {})
        self.assertIn("daemon_kill_at_stop", out)
        self.assertIn("Shutdown drain completed without a kill | **NO**", out)
        self.assertIn("1 anomaly", out)

    def test_a_run_with_no_final_pass_says_so(self):
        out = report.render(EPISODES, {})
        self.assertIn("No final verification pass is recorded", out)

    def test_a_clean_run_with_a_final_pass_does_not_warn(self):
        out = report.render([EPISODES[0], final_record()], {})
        self.assertNotIn("No final verification pass is recorded", out)

    def test_an_episode_whose_recorder_died_is_advisory_not_a_violation(self):
        # The loop's own advisory path: the recorder can die during the
        # quiescence wait, and every delivery after that is missing from the
        # log — so the episode's I1 result is about the harness.
        episodes = [
            dict(EPISODES[1], advisory=True, recorder_alive=False,
                 advisory_reasons=["recorder_exited"]),
        ]
        out = report.render(episodes, {})
        self.assertIn("0 violations", out)
        self.assertIn("1 anomaly", out)
        self.assertIn("recorder_exited", out)

    def test_missing_provenance_fields_render_as_a_dash_not_a_crash(self):
        # An abort record (or any episode written before these fields
        # existed) has none of nacked/pushed/i1_exempt/pending_provenance.
        episodes = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked": 100, "delivered_distinct": 100, "duplicate_rate": 1.0,
             "unknown": 0, "i1_missing": [], "i2_leaked": [], "seed": 1},
        ]
        out = report.render(episodes, {})
        self.assertIn("—", out)

    def test_an_aborted_episode_is_not_counted_as_a_quiescence_timeout(self):
        # Minor: an abort happens BEFORE the fault, so no quiescence wait
        # ever ran. run.py writes "quiesced": False on the abort record
        # (there being no meaningful value), which must not be read the same
        # way as "waited and timed out".
        aborted = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": False,
             "abort_reason": "loadgen_exited", "exit_code": 1, "seed": 24301},
        ]
        out = report.render(aborted, {})
        self.assertNotIn("did not reach drain quiescence", out)
        self.assertIn("aborted early", out)

    def test_no_progress_episode_is_an_anomaly_not_a_violation(self):
        # C2 + I5: a no-progress episode must read as an anomaly (the
        # harness observed nothing happening) and must NOT be mistaken for a
        # durability violation — a skeptic reading the report must not
        # conflate the two.
        episodes = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked": 5000, "delivered_distinct": 5000, "acked_delta": 5000,
             "delivered_delta": 5000, "no_progress": False, "duplicate_rate": 1.0,
             "unknown": 0, "nacked": 0, "pushed": 5000, "i1_missing": [],
             "i2_leaked": [], "seed": 24301},
            {"episode": 1, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked": 5000, "delivered_distinct": 5000, "acked_delta": 0,
             "delivered_delta": 0, "no_progress": True, "duplicate_rate": 1.0,
             "unknown": 0, "nacked": 40000, "pushed": 45000, "i1_missing": [],
             "i2_leaked": [], "seed": 24301},
        ]
        out = report.render(episodes, {})
        self.assertIn("0 violations", out)
        self.assertIn("1 anomaly", out)
        self.assertIn("made no progress", out)
        self.assertIn("evidence of a weir defect", out)
        self.assertNotIn("**FAIL**", out, "no_progress alone must not read as a durability FAIL")


class TestPowerLossVerdict(unittest.TestCase):
    """Task 7's negative control. A Buffered power-loss run that lost NOTHING
    across every episode is `inconclusive`, not `pass` — under a correct
    power-loss model Buffered should lose something (it acks before fsync),
    so losing nothing suggests the injector never bit. A test that cannot
    fail proves nothing."""

    def test_buffered_losing_nothing_is_INCONCLUSIVE_not_green(self):
        recs = [{"tier": "U", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": []} for _ in range(20)]
        self.assertEqual(report.powerloss_verdict(recs), "inconclusive")

    def test_buffered_losing_records_is_a_pass(self):
        recs = [{"tier": "U", "fault": "power_loss", "expected_loss": n,
                 "i1_missing": []} for n in (0, 14, 0, 31)]
        self.assertEqual(report.powerloss_verdict(recs), "pass")

    def test_any_durable_loss_is_a_fail_regardless(self):
        recs = [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": [7]}]
        self.assertEqual(report.powerloss_verdict(recs), "fail")

    def test_durable_losing_nothing_is_a_pass_not_inconclusive(self):
        # The suspicion rule applies to Buffered only. Durable losing nothing
        # is the contract being upheld, not evidence of a dead injector.
        recs = [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": []} for _ in range(20)]
        self.assertEqual(report.powerloss_verdict(recs), "pass")

    def test_no_power_loss_episodes_at_all_is_a_pass(self):
        # The rule does not apply to a run that never injected power loss —
        # a Phase 1 kill_random run must not be read as suspicious.
        recs = [{"tier": "D", "fault": "kill_random", "expected_loss": 0,
                 "i1_missing": []} for _ in range(20)]
        self.assertEqual(report.powerloss_verdict(recs), "pass")

    def test_an_empty_run_is_a_pass(self):
        self.assertEqual(report.powerloss_verdict([]), "pass")

    def test_a_mix_of_tiers_the_buffered_side_still_governs_when_nothing_is_lost(self):
        # If a run somehow carries both tiers' power-loss episodes and
        # Buffered lost nothing while Durable (correctly) lost nothing, the
        # Buffered suspicion still fires — durable-zero alone must not mask
        # buffered-zero being suspicious.
        recs = (
            [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
              "i1_missing": []} for _ in range(5)]
            + [{"tier": "U", "fault": "power_loss", "expected_loss": 0,
                "i1_missing": []} for _ in range(5)]
        )
        self.assertEqual(report.powerloss_verdict(recs), "inconclusive")

    def test_non_power_loss_records_are_ignored(self):
        # A kill_random episode losing nothing must not feed the suspicion
        # rule — it is not measuring the same thing at all.
        recs = (
            [{"tier": "U", "fault": "kill_random", "expected_loss": 0,
              "i1_missing": []} for _ in range(20)]
            + [{"tier": "U", "fault": "power_loss", "expected_loss": 5,
                "i1_missing": []}]
        )
        self.assertEqual(report.powerloss_verdict(recs), "pass")

    def test_the_zero_loss_check_reads_the_last_record_not_a_sum(self):
        # C4 (final review): expected_loss is a currently-still-missing
        # count, not a per-episode delta — a record lost in episode 0 stays
        # in it for every later check, so a naive sum would treat "lost once,
        # 199 episodes ago" the same as "actively losing every episode".
        # Only the LAST verified record's figure decides the verdict.
        recs = [
            {"tier": "U", "fault": "power_loss", "expected_loss": 14, "i1_missing": []},
            {"tier": "U", "fault": "power_loss", "expected_loss": 0, "i1_missing": []},
        ]
        self.assertEqual(
            report.powerloss_verdict(recs), "inconclusive",
            "the LAST record lost nothing; an old sum (14) must not paper "
            "over that",
        )

    def test_an_i2_leak_in_a_power_loss_episode_is_a_fail_not_a_silent_pass(self):
        # MINOR (final review): powerloss_verdict used to inspect only
        # i1_missing, so an I2 leak could render `pass` here while the run's
        # headline violation count already disagreed.
        recs = [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": [], "i2_leaked": [99]}]
        self.assertEqual(report.powerloss_verdict(recs), "fail")

    def test_a_ledger_conflict_in_a_power_loss_episode_is_a_fail_not_a_silent_pass(self):
        recs = [{"tier": "D", "fault": "power_loss", "expected_loss": 0,
                 "i1_missing": [], "ledger_conflicts": [(5, "ACK", "NACK")]}]
        self.assertEqual(report.powerloss_verdict(recs), "fail")

    def test_an_abort_record_does_not_count_as_a_power_loss_episode(self):
        # MINOR (final review): the pre-fault abort record carries the
        # SCHEDULED fault kind even though the run died before ever reaching
        # engage_fault().
        recs = [{"episode": 0, "fault": "power_loss", "ok": True,
                 "abort_reason": "loadgen_exited", "seed": 1}]
        self.assertEqual(report.powerloss_verdict(recs), "pass")


class TestPowerLossSection(unittest.TestCase):
    """Task 7's negative control has to actually reach report.md, not just
    report.powerloss_verdict() — a verdict function nothing calls would be
    exactly the disappearing act I5 exists to stop."""

    def _episode(self, n, tier, expected_loss, i1_missing=()):
        return {
            "episode": n, "fault": "power_loss", "tier": tier,
            "ok": not i1_missing, "quiesced": True,
            "acked": 1000 * (n + 1), "delivered_distinct": 1000 * (n + 1),
            "acked_delta": 1000, "delivered_delta": 1000,
            "no_progress": False, "duplicate_rate": 1.0, "unknown": 0,
            "i1_missing": list(i1_missing), "i2_leaked": [],
            "expected_loss": expected_loss, "seed": 24301,
        }

    def test_buffered_zero_loss_is_surfaced_as_inconclusive(self):
        episodes = [self._episode(0, "U", 0), self._episode(1, "U", 0)]
        out = report.render(episodes, {})
        self.assertIn("Power-loss verdict", out)
        self.assertIn("INCONCLUSIVE", out)
        self.assertIn("suspicious", out)

    def test_buffered_nonzero_loss_is_surfaced_as_pass_with_the_last_record(self):
        # C4 (final review): the headline used to SUM expected_loss across
        # episodes — a cumulative "currently still missing" count, not a
        # delta, so summing double(or more)-counts any record that stays
        # lost. It must read the LAST verified record instead, exactly like
        # the Totals table does.
        episodes = [self._episode(0, "U", 14), self._episode(1, "U", 31)]
        out = report.render(episodes, {})
        self.assertIn("Power-loss verdict", out)
        self.assertIn("PASS", out)
        self.assertIn(
            "**31**", out,
            "the LAST episode's cumulative figure, not a sum across episodes",
        )
        self.assertNotIn(
            "**45**", out,
            "45 = 14+31 would be the old (buggy) summed total",
        )

    def test_durable_loss_is_surfaced_as_fail(self):
        episodes = [self._episode(0, "D", 0, i1_missing=[7])]
        out = report.render(episodes, {})
        self.assertIn("Power-loss verdict", out)
        self.assertIn("FAIL", out)

    def test_a_kill_random_run_gets_no_power_loss_section_at_all(self):
        # The negative control does not apply to a run that never injected
        # power loss; the section must not appear and must not clutter a
        # Phase 1 report.
        episodes = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked_delta": 1000, "delivered_delta": 1000, "no_progress": False,
             "duplicate_rate": 1.0, "unknown": 0, "i1_missing": [], "i2_leaked": [],
             "seed": 1},
        ]
        out = report.render(episodes, {})
        self.assertNotIn(
            "## Power-loss verdict", out,
            "the Limitations bullet may still MENTION the section by name; "
            "only the section itself must be absent",
        )

    def test_limitations_no_longer_claims_i1_is_not_tier_aware(self):
        out = report.render([], {})
        self.assertNotIn("not yet tier-aware", out)
        self.assertIn("tier- and fault-aware", out)

    def test_an_aborted_run_that_never_injected_gets_no_power_loss_section(self):
        # MINOR (final review): the pre-fault abort record carries the
        # SCHEDULED fault kind, so this used to render a Power-loss verdict
        # section for a run that died before engage_fault() ever ran.
        episodes = [
            {"episode": 0, "fault": "power_loss", "ok": True, "quiesced": False,
             "abort_reason": "loadgen_exited", "exit_code": 1, "seed": 1},
        ]
        out = report.render(episodes, {})
        self.assertNotIn("## Power-loss verdict", out)

    def test_a_power_loss_run_gets_a_phase_2_title_and_a_consistent_limitations_bullet(self):
        # I2 (final review): the title used to be hardcoded "Phase 1
        # (spine)", and the Limitations bullet said power loss was "not
        # covered by this run" — printed directly under this run's own
        # "## Power-loss verdict" section.
        episodes = [self._episode(0, "U", 5)]
        out = report.render(episodes, {})
        self.assertIn("Phase 2", out)
        self.assertNotIn("Phase 1 (spine)", out)
        self.assertIn("Power-loss verdict", out)
        self.assertNotIn("not covered by this run", out)

    def test_a_kill_random_only_run_keeps_the_phase_1_title(self):
        episodes = [
            {"episode": 0, "fault": "kill_random", "ok": True, "quiesced": True,
             "acked_delta": 1000, "delivered_delta": 1000, "no_progress": False,
             "duplicate_rate": 1.0, "unknown": 0, "i1_missing": [], "i2_leaked": [],
             "seed": 1},
        ]
        out = report.render(episodes, {})
        self.assertIn("Phase 1 (spine)", out)
        self.assertIn("are **not", out)
        self.assertIn("covered** by this run", out)

    def test_expected_loss_has_its_own_episode_table_column(self):
        # I3 (final review): an episode that lost 12,000 Buffered records
        # used to render identically to one that lost none — I1 stays
        # silent (ok=True) for the exemption, and expected_loss was in
        # neither the episode table nor VerifyResult.summary().
        episodes = [self._episode(0, "U", 12000)]
        out = report.render(episodes, {})
        self.assertIn("Expected loss", out)
        self.assertIn("| 12000 |", out)


class TestCanarySection(unittest.TestCase):
    """I6: a canary block written before the fault and overwritten while it
    is engaged converts the negative control from an inference into a
    measurement — this has to actually reach report.md."""

    def _episode(self, n, canary):
        return {
            "episode": n, "fault": "power_loss", "tier": "U",
            "ok": True, "quiesced": True, "acked_delta": 100,
            "delivered_delta": 100, "no_progress": False,
            "duplicate_rate": 1.0, "unknown": 0, "i1_missing": [],
            "i2_leaked": [], "expected_loss": 0, "canary": canary,
            "seed": 1,
        }

    def test_canary_summary_reports_the_bite_count(self):
        episodes = [self._episode(0, "bit"), self._episode(1, "bit"),
                    self._episode(2, "did_not_bite")]
        out = report.render(episodes, {})
        self.assertIn("Canary", out)
        self.assertIn("2/3", out)

    def test_a_did_not_bite_canary_is_flagged_in_the_episode_notes(self):
        episodes = [self._episode(0, "did_not_bite")]
        out = report.render(episodes, {})
        self.assertIn("canary=did_not_bite", out)

    def test_an_unexpected_canary_is_flagged_too(self):
        episodes = [self._episode(0, "unexpected")]
        out = report.render(episodes, {})
        self.assertIn("canary=unexpected", out)

    def test_a_bit_canary_does_not_clutter_every_row(self):
        episodes = [self._episode(0, "bit")]
        out = report.render(episodes, {})
        self.assertNotIn("canary=bit", out)

    def test_no_canary_recorded_at_all_renders_no_canary_line(self):
        # kill_random episodes (and pre-canary records) carry no "canary"
        # key at all — the summary line must simply not appear, not crash.
        episodes = [
            {"episode": 0, "fault": "power_loss", "tier": "U", "ok": True,
             "quiesced": True, "acked_delta": 100, "delivered_delta": 100,
             "no_progress": False, "duplicate_rate": 1.0, "unknown": 0,
             "i1_missing": [], "i2_leaked": [], "expected_loss": 0, "seed": 1},
        ]
        out = report.render(episodes, {})
        self.assertNotIn("Canary:", out)


if __name__ == "__main__":
    unittest.main()


class TestCommitLabel(unittest.TestCase):
    """The reported commit must not claim a clean tree that did not run.

    The 2026-08-22 soak reported `b49f341` while running six modified files on
    top of it, so a green 25M-record result pointed at code that was never
    executed. These pin both directions.
    """

    def test_a_clean_tree_reports_the_bare_commit(self):
        self.assertEqual(report.commit_label("b49f341", ""), "b49f341")

    def test_whitespace_only_status_is_still_clean(self):
        self.assertEqual(report.commit_label("b49f341", "\n  \n"), "b49f341")

    def test_a_dirty_tree_is_marked_and_counted(self):
        porcelain = " M crates/weir-server/src/wab/recovery.rs\n M chaos/orchestrator/run.py\n"
        got = report.commit_label("b49f341", porcelain)
        self.assertIn("dirty", got)
        self.assertIn("b49f341", got)
        self.assertIn("2 tracked files modified", got)

    def test_one_modified_file_is_singular(self):
        got = report.commit_label("b49f341", " M chaos/orchestrator/run.py\n")
        self.assertIn("1 tracked file modified", got)
        self.assertNotIn("files", got)

    def test_a_missing_commit_is_left_alone_rather_than_labelled_dirty(self):
        # git absent or not a repo: say nothing rather than invent "-dirty".
        self.assertEqual(report.commit_label("", " M x.rs\n"), "")
