"""Tests for the Foundations pre-release chaos gate (spec §16)."""
import json
import os
import tempfile
import unittest

import gate
import report


def cut(i, kind, **over):
    e = {"episode": i, "fault": "power_loss", "trigger": kind, "cut": "power_loss",
         "ok": True, "quiesced": True, "canary": "bit", "duplicate_rate": 1.01,
         "anomaly_reasons": [], "seed": 1}
    e.update(over)
    return e


def full_campaign(per=250):
    return {f"runs/{k}": [cut(i, k) for i in range(per)]
            + [{"episode": "final", "fault": "power_loss", "ok": True,
                "advisory": False, "anomaly_reasons": [], "duplicate_rate": 1.012}]
            for k in gate.GATE_TRIGGERS}


class TestCampaignGate(unittest.TestCase):
    def test_a_full_clean_campaign_passes(self):
        r = gate.campaign_gate(full_campaign())
        self.assertTrue(r["ok"], r["failures"])
        self.assertEqual(r["total_cuts"], 1000)
        self.assertEqual(r["duplicate_rate"]["sigterm_at"], 1.012)
        self.assertEqual(r["baseline_duplicate_rate"], 1.0)

    def test_too_few_cuts_in_one_schedule_fails_and_names_it(self):
        runs = full_campaign()
        runs["runs/sigterm_in_recovery"] = runs["runs/sigterm_in_recovery"][:150]
        r = gate.campaign_gate(runs)
        self.assertFalse(r["ok"])
        self.assertTrue(any("startup-signal" in f for f in r["failures"]))

    def test_too_few_cuts_in_total_fails(self):
        r = gate.campaign_gate(full_campaign(per=210))
        self.assertFalse(r["ok"])
        self.assertTrue(any("1000" in f for f in r["failures"]))

    def test_one_missed_canary_fails(self):
        runs = full_campaign()
        runs["runs/sigterm_at"][7]["canary"] = "did_not_bite"
        r = gate.campaign_gate(runs)
        self.assertFalse(r["ok"])
        self.assertEqual(r["canary_misses"], ["runs/sigterm_at#7"])

    def test_one_violation_fails(self):
        runs = full_campaign()
        runs["runs/double_sigterm"][3].update(ok=False, i1_missing=[42])
        self.assertFalse(gate.campaign_gate(runs)["ok"])

    def test_an_advisory_failure_is_an_anomaly_not_a_violation(self):
        runs = full_campaign()
        runs["runs/double_sigterm"][3].update(ok=False, advisory=True)
        r = gate.campaign_gate(runs)
        self.assertEqual(r["violations"], [])
        self.assertFalse(r["ok"], "anomalies fail the gate unless allowed")
        self.assertTrue(gate.campaign_gate(runs, allow_anomalies=True)["ok"])

    def test_aborted_and_kill_cut_episodes_are_not_cuts(self):
        runs = full_campaign()
        runs["runs/sigterm_at"].append(cut(999, "sigterm_at", cut="kill"))
        runs["runs/sigterm_at"].append(cut(998, "sigterm_at", abort_reason="loadgen_exited"))
        self.assertEqual(gate.campaign_gate(runs)["cuts"]["sigterm_at"], 250)

    def test_legacy_power_loss_runs_do_not_count_towards_the_gate(self):
        runs = full_campaign()
        runs["runs/legacy"] = [{"episode": i, "fault": "power_loss", "ok": True,
                                "canary": "bit"} for i in range(500)]
        self.assertEqual(gate.campaign_gate(runs)["total_cuts"], 1000)

    def test_a_run_without_a_final_pass_fails(self):
        runs = full_campaign()
        runs["runs/sigterm_at"] = runs["runs/sigterm_at"][:-1]
        r = gate.campaign_gate(runs)
        self.assertFalse(r["ok"])
        self.assertEqual(r["runs_without_final_pass"], ["runs/sigterm_at"])

    def _write_campaign(self, tmp):
        dirs = []
        for name, records in full_campaign().items():
            d = os.path.join(tmp, os.path.basename(name))
            os.makedirs(d)
            with open(os.path.join(d, "episodes.jsonl"), "w") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")
            dirs.append(d)
        return dirs

    def test_the_cli_reads_run_dirs_and_exits_by_the_verdict(self):
        with tempfile.TemporaryDirectory() as tmp:
            dirs = self._write_campaign(tmp)
            self.assertEqual(gate.main(dirs), 0)
            self.assertEqual(gate.main(dirs[:3]), 1)

    def test_the_cli_fails_a_run_dir_holding_a_crash_post_mortem(self):
        with tempfile.TemporaryDirectory() as tmp:
            dirs = self._write_campaign(tmp)
            with open(os.path.join(dirs[0], report.CRASH_POSTMORTEM_FILE), "w") as f:
                json.dump({"reason": "x", "wab_dir": None}, f)
            self.assertEqual(gate.main(dirs), 1)


if __name__ == "__main__":
    unittest.main()
