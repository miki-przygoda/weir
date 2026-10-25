//! spec §14 parity: for identical Evidence, exit ∈ {0,3} ⇔ readiness().ok
//! (⇔ /readyz 200, since /readyz renders the same `readiness`).

use std::path::Path;
use weir_status::exit::StatusExit;
use weir_status::exposition::evidence_from_metrics;
use weir_status::{NoLatches, Thresholds, evaluate, exit_code, readiness};

/// A 4.1 daemon serving normally (metric names from spec §13). PR 1's
/// `evidence_from_metrics` builds `wab_cap` only when `weir_wab_cap_rejecting`,
/// `weir_wab_bytes_on_disk` and `weir_config_wab_max_bytes` are all present;
/// without all three, `wab_cap_rejecting` is unevaluated and readiness fails
/// closed, so this fixture would never be ready.
const SERVING_4_1: &str = "\
weir_build_info{version=\"4.1.0\",wire_version=\"1\"} 1
weir_startup_phase{phase=\"initializing\"} 0
weir_startup_phase{phase=\"recovering\"} 0
weir_startup_phase{phase=\"handoff\"} 0
weir_startup_phase{phase=\"binding\"} 0
weir_startup_phase{phase=\"serving\"} 1
weir_startup_phase{phase=\"shutting_down\"} 0
weir_ready 1
weir_live 1
weir_wab_shard_up{shard=\"0\"} 1
weir_worker_up{worker=\"0\"} 1
weir_wab_cap_rejecting 0
weir_wab_bytes_on_disk 0
weir_config_wab_max_bytes 1073741824
weir_drain_state{state=\"draining\"} 1
weir_sink_health{state=\"healthy\"} 1
weir_sink_info{sink_type=\"http\"} 1
";

fn fixtures() -> Vec<(String, String)> {
    let mut out = vec![
        ("inline:serving_4_1".to_string(), SERVING_4_1.to_string()),
        (
            "inline:cap_rejecting".to_string(),
            SERVING_4_1.replace("weir_wab_cap_rejecting 0", "weir_wab_cap_rejecting 1"),
        ),
        (
            "inline:shutting_down".to_string(),
            SERVING_4_1
                .replace(
                    "weir_startup_phase{phase=\"serving\"} 1",
                    "weir_startup_phase{phase=\"serving\"} 0",
                )
                .replace(
                    "weir_startup_phase{phase=\"shutting_down\"} 0",
                    "weir_startup_phase{phase=\"shutting_down\"} 1",
                ),
        ),
        (
            "inline:no_live_shard".to_string(),
            SERVING_4_1.replace(
                "weir_wab_shard_up{shard=\"0\"} 1",
                "weir_wab_shard_up{shard=\"0\"} 0",
            ),
        ),
    ];
    // Plus every exposition PR 1 captured for `evidence_from_metrics`
    // (PR 1 Task 1.9 names them `metrics_*.txt`, e.g. metrics_4_0_1.txt).
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if (name.starts_with("metrics_") || name.starts_with("exposition"))
                && name.ends_with(".txt")
            {
                out.push((name, std::fs::read_to_string(e.path()).unwrap()));
            }
        }
    }
    out
}

#[test]
fn exit_in_0_or_3_iff_readiness_ok_for_every_fixture() {
    let (mut seen_ok, mut seen_not_ok) = (false, false);
    for (name, body) in fixtures() {
        let e = evidence_from_metrics(&body, 1_700_000_000_000);
        let ev = evaluate(&e, &Thresholds::default(), &NoLatches);
        let r = readiness(&e);
        let exit = StatusExit::from_verdict(r.ok, &ev.conditions);
        assert_eq!(
            matches!(exit, StatusExit::Ready | StatusExit::ReadyWithCritical),
            r.ok,
            "{name}: exit {exit:?} disagrees with readiness().ok = {}",
            r.ok
        );
        assert_eq!(
            i32::from(exit.code()),
            exit_code(&r, &ev),
            "{name}: weir-ctl's StatusExit and PR 1's exit_code must be one table"
        );
        seen_ok |= r.ok;
        seen_not_ok |= !r.ok;
    }
    assert!(
        seen_ok,
        "a healthy 4.1 exposition must evaluate ready, or weir-ctl's metrics source never says READY"
    );
    assert!(
        seen_not_ok,
        "the cap-rejecting fixture must evaluate not ready"
    );
}
