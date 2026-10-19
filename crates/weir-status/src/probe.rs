//! Probe predicates (spec §10, §12) and the exit-code table weir-ctl and
//! weir-readiness.sh share (spec §14). Each predicate runs the SAME rules
//! `evaluate` runs, so `/readyz` and the Conditions list cannot disagree.
use crate::codes::{ConditionCode, Severity};
use crate::condition::{Evaluation, Thresholds};
use crate::evidence::{Evidence, Phase};
use crate::rules::{Outcome, rule};
use serde::{Deserialize, Serialize};

/// A check selectable in `livez_checks` (spec §12), in catalogue order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivezCheck {
    ListenerDown,
    NoLiveShard,
    WorkerExited,
    DrainStopped,
    DrainWedged,
}

impl LivezCheck {
    pub const ALL: [LivezCheck; 5] = [
        LivezCheck::ListenerDown,
        LivezCheck::NoLiveShard,
        LivezCheck::WorkerExited,
        LivezCheck::DrainStopped,
        LivezCheck::DrainWedged,
    ];
    pub fn code(self) -> ConditionCode {
        match self {
            LivezCheck::ListenerDown => ConditionCode::ListenerDown,
            LivezCheck::NoLiveShard => ConditionCode::NoLiveShard,
            LivezCheck::WorkerExited => ConditionCode::WorkerExited,
            LivezCheck::DrainStopped => ConditionCode::DrainStopped,
            LivezCheck::DrainWedged => ConditionCode::DrainWedged,
        }
    }
    pub fn as_str(self) -> &'static str {
        self.code().as_str()
    }
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProbeCheck {
    pub code: ConditionCode,
    pub ok: bool,
    /// The public summary of a failing check (`None` when it passed, or
    /// when it failed for missing evidence).
    pub summary: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProbeVerdict {
    pub ok: bool,
    pub failing: Vec<ConditionCode>,
    pub checks: Vec<ProbeCheck>,
}

impl ProbeVerdict {
    fn passing() -> Self {
        ProbeVerdict {
            ok: true,
            failing: Vec::new(),
            checks: Vec::new(),
        }
    }
    fn pass(&mut self, code: ConditionCode) {
        self.checks.push(ProbeCheck {
            code,
            ok: true,
            summary: None,
        });
    }
    fn fail(&mut self, code: ConditionCode, summary: Option<String>) {
        self.ok = false;
        self.failing.push(code);
        self.checks.push(ProbeCheck {
            code,
            ok: false,
            summary,
        });
    }
    fn record(&mut self, code: ConditionCode, outcome: Outcome, fail_closed: bool) {
        match outcome {
            Outcome::Inactive => self.pass(code),
            Outcome::Active(f) => self.fail(code, Some(f.summary)),
            Outcome::Unevaluated if fail_closed => self.fail(code, None),
            Outcome::Unevaluated => self.pass(code),
        }
    }
}

/// The R-gated codes (spec §9.3), in catalogue order.
pub const READINESS_CODES: [ConditionCode; 5] = [
    ConditionCode::Starting,
    ConditionCode::ShuttingDown,
    ConditionCode::ListenerDown,
    ConditionCode::NoLiveShard,
    ConditionCode::WabCapRejecting,
];

/// `/readyz` (spec §10, owner decision 4): serving, not shutting down, not
/// cap-rejecting, at least one ingest-capable shard, no listener down.
/// Missing evidence fails the check (fail-closed).
pub fn readiness(e: &Evidence) -> ProbeVerdict {
    let t = Thresholds::DEFAULT;
    let mut v = ProbeVerdict::passing();
    for code in READINESS_CODES {
        v.record(code, rule(code, e, &t), true);
    }
    v
}

/// `/livez` (spec §12): only the selected checks, only while serving, and
/// only on positive evidence (a restart must never be ordered on a gap).
pub fn liveness(e: &Evidence, t: &Thresholds, checks: &[LivezCheck]) -> ProbeVerdict {
    let serving = e.phase == Some(Phase::Serving);
    let mut v = ProbeVerdict::passing();
    for c in checks {
        if serving {
            v.record(c.code(), rule(c.code(), e, t), false);
        } else {
            v.pass(c.code());
        }
    }
    v
}

/// `/startupz` (spec §10): 200 once the daemon has ever served; sticky.
pub fn startup(e: &Evidence) -> ProbeVerdict {
    let mut v = ProbeVerdict::passing();
    if e.ever_served == Some(true) {
        v.pass(ConditionCode::Starting);
    } else {
        let summary = match rule(ConditionCode::Starting, e, &Thresholds::DEFAULT) {
            Outcome::Active(f) => Some(f.summary),
            Outcome::Inactive | Outcome::Unevaluated => None,
        };
        v.fail(ConditionCode::Starting, summary);
    }
    v
}

pub const EXIT_READY: i32 = 0;
pub const EXIT_NOT_READY: i32 = 1;
pub const EXIT_UNREACHABLE: i32 = 2;
pub const EXIT_CRITICAL: i32 = 3;

/// The shared table (spec §14): 1 exactly when `/readyz` would be 503;
/// 3 when ready but a critical condition is active; else 0. (2, unreachable
/// or identity mismatch, is decided by the caller before any evidence exists.)
pub fn exit_code(readiness: &ProbeVerdict, eval: &Evaluation) -> i32 {
    if !readiness.ok {
        EXIT_NOT_READY
    } else if eval
        .conditions
        .iter()
        .any(|c| c.severity == Severity::Critical)
    {
        EXIT_CRITICAL
    } else {
        EXIT_READY
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::serving_evidence;
    use crate::*;
    use ConditionCode as C;

    const NOW: u64 = 1_800_000_000_000;
    fn with(f: impl FnOnce(&mut Evidence)) -> Evidence {
        let mut e = serving_evidence(NOW);
        f(&mut e);
        e
    }

    #[test]
    fn readiness_codes_and_livez_checks_are_the_catalogue_gates() {
        let r: Vec<ConditionCode> = CATALOGUE
            .iter()
            .filter(|e| e.gates.contains(&Gate::Readiness))
            .map(|e| e.code)
            .collect();
        assert_eq!(r, READINESS_CODES.to_vec());
        let l: Vec<ConditionCode> = CATALOGUE
            .iter()
            .filter(|e| e.gates.contains(&Gate::Liveness))
            .map(|e| e.code)
            .collect();
        assert_eq!(
            l,
            LivezCheck::ALL.iter().map(|c| c.code()).collect::<Vec<_>>()
        );
        for c in LivezCheck::ALL {
            assert_eq!(LivezCheck::parse(c.as_str()), Some(c));
            assert_eq!(
                serde_json::to_string(&c).unwrap(),
                format!("\"{}\"", c.as_str())
            );
        }
        assert_eq!(LivezCheck::parse("sink_down"), None);
    }

    #[test]
    fn readiness_truth_table() {
        assert!(readiness(&serving_evidence(NOW)).ok);
        let cases: Vec<(Evidence, ConditionCode)> = vec![
            (with(|e| e.phase = Some(Phase::Binding)), C::Starting),
            (
                with(|e| {
                    e.shutting_down = Some(true);
                    e.phase = Some(Phase::ShuttingDown)
                }),
                C::ShuttingDown,
            ),
            (
                with(|e| e.listeners.as_mut().unwrap()[0].state = ListenerState::Failed),
                C::ListenerDown,
            ),
            (
                with(|e| {
                    for s in e.shards.as_mut().unwrap() {
                        s.state = ShardState::Offline
                    }
                }),
                C::NoLiveShard,
            ),
            (
                with(|e| e.wab_cap.as_mut().unwrap().rejecting = true),
                C::WabCapRejecting,
            ),
        ];
        for (e, code) in cases {
            let v = readiness(&e);
            assert!(!v.ok, "{code:?}");
            assert_eq!(v.failing, vec![code]);
            assert!(
                v.checks
                    .iter()
                    .any(|c| c.code == code && !c.ok && c.summary.is_some())
            );
        }
    }

    #[test]
    fn readiness_ignores_sink_drain_pause_and_history() {
        let e = with(|e| {
            e.sink.as_mut().unwrap().health = Some(SinkHealthCode::Down);
            e.drain.as_mut().unwrap().exit = DrainExitCode::GaveUp;
            e.drain.as_mut().unwrap().paused = true;
            e.recent.as_mut().unwrap().fsync_failures_15m = Some(9);
            e.backlog.as_mut().unwrap().quarantine_bytes = 10;
        });
        assert!(readiness(&e).ok);
    }

    #[test]
    fn readiness_fails_closed_on_missing_evidence() {
        let v = readiness(&Evidence::empty(NOW, Source::Disk));
        assert!(!v.ok);
        assert_eq!(v.failing, READINESS_CODES.to_vec());
        assert!(v.checks.iter().all(|c| c.summary.is_none()));
    }

    #[test]
    fn liveness_is_opt_in_serving_only_and_fail_open() {
        let exited = with(|e| {
            let w = &mut e.workers.as_mut().unwrap()[0];
            w.running = false;
            w.exited_at_unix_ms = Some(NOW);
        });
        assert!(
            liveness(&exited, &Thresholds::DEFAULT, &[]).ok,
            "default livez_checks = []"
        );
        let v = liveness(&exited, &Thresholds::DEFAULT, &[LivezCheck::WorkerExited]);
        assert!(!v.ok);
        assert_eq!(v.failing, vec![C::WorkerExited]);
        let mut starting = exited.clone();
        starting.phase = Some(Phase::Recovering);
        assert!(
            liveness(&starting, &Thresholds::DEFAULT, &LivezCheck::ALL).ok,
            "only while serving"
        );
        let no_drain = with(|e| e.drain = None);
        assert!(
            liveness(&no_drain, &Thresholds::DEFAULT, &[LivezCheck::DrainWedged]).ok,
            "unknown is not dead: a restart must not be ordered on missing evidence"
        );
    }

    #[test]
    fn startup_is_sticky_once_served() {
        assert!(startup(&serving_evidence(NOW)).ok);
        let v = startup(&with(|e| {
            e.ever_served = Some(false);
            e.phase = Some(Phase::Recovering)
        }));
        assert!(!v.ok);
        assert_eq!(v.failing, vec![C::Starting]);
        assert!(
            v.checks[0]
                .summary
                .as_deref()
                .unwrap()
                .starts_with("startup phase recovering")
        );
        let later = with(|e| {
            e.phase = Some(Phase::ShuttingDown);
            e.shutting_down = Some(true)
        });
        assert!(startup(&later).ok, "sticky after serving");
    }

    #[test]
    fn exit_codes_and_the_parity_with_readyz() {
        let fixtures: Vec<Evidence> = vec![
            serving_evidence(NOW),
            with(|e| e.phase = Some(Phase::Handoff)),
            with(|e| e.wab_cap.as_mut().unwrap().rejecting = true),
            with(|e| e.drain.as_mut().unwrap().exit = DrainExitCode::GaveUp),
            with(|e| e.shards.as_mut().unwrap()[0].state = ShardState::Offline),
            with(|e| e.sink.as_mut().unwrap().health = Some(SinkHealthCode::Down)),
            Evidence::empty(NOW, Source::Disk),
        ];
        for e in &fixtures {
            let r = readiness(e);
            let eval = evaluate(e, &Thresholds::DEFAULT, &NoLatches);
            // parity: readiness == "no active R-gated condition and none unevaluated"
            let r_from_eval = !eval
                .conditions
                .iter()
                .any(|c| c.gates.contains(&Gate::Readiness))
                && !eval.unevaluated.iter().any(|c| READINESS_CODES.contains(c));
            assert_eq!(r.ok, r_from_eval, "{:?}", e.phase);
            let x = exit_code(&r, &eval);
            assert_eq!(x == EXIT_READY || x == EXIT_CRITICAL, r.ok);
        }
        let ok = serving_evidence(NOW);
        assert_eq!(
            exit_code(
                &readiness(&ok),
                &evaluate(&ok, &Thresholds::DEFAULT, &NoLatches)
            ),
            EXIT_READY
        );
        let crit = &fixtures[3];
        assert_eq!(
            exit_code(
                &readiness(crit),
                &evaluate(crit, &Thresholds::DEFAULT, &NoLatches)
            ),
            EXIT_CRITICAL
        );
        let not_ready = &fixtures[1];
        assert_eq!(
            exit_code(
                &readiness(not_ready),
                &evaluate(not_ready, &Thresholds::DEFAULT, &NoLatches)
            ),
            EXIT_NOT_READY
        );
    }
}
