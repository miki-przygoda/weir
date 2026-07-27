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
             "unknown": 0, "nacked": 17, "pushed": 5017, "i1_exempt": 3,
             "pending_provenance": 2, "i1_missing": [], "i2_leaked": [],
             "seed": 24301},
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
