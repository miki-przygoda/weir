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
