//! Prometheus/OpenMetrics text → `Evidence` (spec §5.1 `exposition`), so
//! weir-ctl can run the same evaluator against a daemon it can only scrape,
//! including a pre-Foundations one. Label values in weir's own families
//! never contain `"` or `,`, so a simple split parser is exact for them; any
//! line it cannot parse is skipped, never guessed.
use crate::codes::{ConditionCode, Severity};
use crate::config::{ConfigValue, PublicConfig};
use crate::evidence::*;
use std::collections::BTreeMap;

struct Sample<'a> {
    name: &'a str,
    labels: Vec<(&'a str, &'a str)>,
    value: f64,
}

fn parse_line(line: &str) -> Option<Sample<'_>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (head, value) = line.rsplit_once(' ')?;
    let value: f64 = value.parse().ok()?;
    let (name, labels) = match head.find('{') {
        Some(i) => (&head[..i], parse_labels(head[i + 1..].strip_suffix('}')?)?),
        None => (head, Vec::new()),
    };
    Some(Sample {
        name,
        labels,
        value,
    })
}

fn parse_labels(body: &str) -> Option<Vec<(&str, &str)>> {
    let mut out = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let (key, after) = rest.split_once("=\"")?;
        let end = after.find('"')?;
        out.push((key, &after[..end]));
        rest = &after[end + 1..];
        rest = rest.strip_prefix(',').unwrap_or(rest);
    }
    Some(out)
}

struct Scrape<'a>(Vec<Sample<'a>>);

impl<'a> Scrape<'a> {
    fn parse(body: &'a str) -> Self {
        Scrape(body.lines().filter_map(parse_line).collect())
    }
    fn scalar(&self, name: &str) -> Option<f64> {
        self.0
            .iter()
            .find(|s| s.name == name && s.labels.is_empty())
            .map(|s| s.value)
    }
    fn labelled(&self, name: &str, label: &str) -> Vec<(&'a str, f64)> {
        self.0
            .iter()
            .filter(|s| s.name == name)
            .filter_map(|s| {
                s.labels
                    .iter()
                    .find(|(k, _)| *k == label)
                    .map(|(_, v)| (*v, s.value))
            })
            .collect()
    }
    fn one_hot(&self, name: &str, label: &str) -> Option<&'a str> {
        self.labelled(name, label)
            .into_iter()
            .find(|(_, v)| *v >= 1.0)
            .map(|(k, _)| k)
    }
}

fn ms(secs: f64) -> u64 {
    (secs * 1000.0) as u64
}

fn find<T: Copy>(all: &[T], name: fn(T) -> &'static str, s: &str) -> Option<T> {
    all.iter().copied().find(|v| name(*v) == s)
}

/// A named fn, not a closure: an elided lifetime in a closure parameter is a
/// fresh late-bound region, so a closure could not return a `&'a str` tied
/// to the sample.
fn label<'a>(x: &Sample<'a>, k: &str) -> Option<&'a str> {
    x.labels.iter().find(|(n, _)| *n == k).map(|(_, v)| *v)
}

/// Builds the evidence a scrape supports. Absent families stay `None`.
pub fn evidence_from_metrics(body: &str, now_unix_ms: u64) -> Evidence {
    let s = Scrape::parse(body);
    let now = now_unix_ms;
    let mut e = Evidence::empty(now, Source::Metrics);
    e.started_at_unix_ms = s.scalar("weir_start_time_seconds").map(ms);
    e.phase = s
        .one_hot("weir_startup_phase", "phase")
        .and_then(|p| find(Phase::ALL, Phase::as_str, p));
    if let Some(p) = e.phase {
        e.shutting_down = Some(p == Phase::ShuttingDown);
        if p == Phase::Serving {
            e.ever_served = Some(true);
        }
    }
    let serving = e.phase == Some(Phase::Serving);
    // Listener states are not exported. The daemon's own `weir_ready` verdict
    // covers listener_down: ready means no listener is down; anything else
    // stays unknown (and readiness fails closed on it).
    if s.scalar("weir_ready").is_some_and(|v| v >= 1.0) {
        e.listeners = Some(Vec::new());
    }

    let workers: Vec<(u16, bool)> = s
        .labelled("weir_worker_up", "worker")
        .into_iter()
        .filter_map(|(w, v)| Some((w.parse().ok()?, v >= 1.0)))
        .collect();
    if !workers.is_empty() {
        e.workers = Some(
            workers
                .iter()
                .map(|&(id, running)| WorkerEvidence {
                    id,
                    running,
                    exited_at_unix_ms: (!running && serving).then_some(now),
                })
                .collect(),
        );
    }
    let shard_up = s.labelled("weir_wab_shard_up", "shard");
    if !shard_up.is_empty() {
        let blocked = s.labelled("weir_wab_handoff_blocked_seconds", "shard");
        let ages = s.labelled("weir_wab_active_segment_age_seconds", "shard");
        let lookup =
            |set: &[(&str, f64)], id: &str| set.iter().find(|(k, _)| *k == id).map(|(_, v)| *v);
        let wc = workers.len() as u16;
        e.shards = Some(
            shard_up
                .iter()
                .filter_map(|&(id_s, up)| {
                    let id: u16 = id_s.parse().ok()?;
                    let state = if up >= 1.0 {
                        ShardState::Up
                    } else if e.phase.is_some_and(Phase::before_serving) {
                        ShardState::Starting
                    } else {
                        ShardState::Offline
                    };
                    Some(ShardEvidence {
                        id,
                        state,
                        reason: None,
                        since_unix_ms: now,
                        respawns: 0,
                        worker_running: wc == 0 || workers.iter().any(|&(w, r)| w == id % wc && r),
                        handoff_blocked_since_unix_ms: lookup(&blocked, id_s)
                            .filter(|v| *v > 0.0)
                            .map(|v| now.saturating_sub(ms(v))),
                        active_segment_age_secs: lookup(&ages, id_s),
                    })
                })
                .collect(),
        );
    }

    if let (Some(rej), Some(bytes), Some(max)) = (
        s.scalar("weir_wab_cap_rejecting"),
        s.scalar("weir_wab_bytes_on_disk"),
        s.scalar("weir_config_wab_max_bytes"),
    ) {
        let max = max as u64;
        e.wab_cap = Some(CapEvidence {
            max_bytes: max,
            low_water_bytes: max / 10 * 9,
            bytes_now: bytes as u64,
            rejecting: rej >= 1.0,
            first_scan_ok_unix_ms: None,
            last_scan_ok_unix_ms: None,
            growing_unbounded: None,
        });
    }

    if let Some(state) = s
        .one_hot("weir_drain_state", "state")
        .and_then(|v| find(DrainStateCode::ALL, DrainStateCode::as_str, v))
    {
        let paused = s.scalar("weir_drain_paused").is_some_and(|v| v >= 1.0);
        let exit = match state {
            DrainStateCode::Stopped if e.phase == Some(Phase::ShuttingDown) => {
                DrainExitCode::Stopped
            }
            DrainStateCode::Stopped => DrainExitCode::GaveUp,
            _ => DrainExitCode::Running,
        };
        let queue = s.labelled("weir_drain_pending_segments", "queue");
        let q = |name: &str| {
            queue
                .iter()
                .find(|(k, _)| *k == name)
                .map_or(0, |(_, v)| *v as u64)
        };
        let heartbeat = s.scalar("weir_drain_heartbeat_age_seconds");
        // `weir_drain_busy_seconds{kind}`: at most one kind is > 0 (the
        // safety floor's `publish_drain_busy`); all 0 means between operations.
        let busy = s
            .labelled("weir_drain_busy_seconds", "kind")
            .into_iter()
            .find(|(_, v)| *v > 0.0)
            .map(|(kind, v)| BusyEvidence {
                sink_call: kind == "sink_commit" || kind == "sink_probe",
                since_unix_ms: now.saturating_sub(ms(v)),
            });
        e.drain = Some(DrainEvidence {
            state,
            state_since_unix_ms: now,
            pause_requested: paused,
            paused,
            held: false,
            paused_since_unix_ms: None,
            paused_by_uid: None,
            generation: s.scalar("weir_drain_generation").map_or(0, |v| v as u64),
            observed_requeue: 0,
            in_flight: None,
            commit_in_flight_since_unix_ms: s
                .scalar("weir_drain_commit_in_flight_seconds")
                .filter(|v| *v > 0.0)
                .map(|v| now.saturating_sub(ms(v))),
            busy,
            last_progress_unix_ms: heartbeat.map(|v| now.saturating_sub(ms(v))),
            heartbeat_age_secs: heartbeat,
            pending_segments: q("live"),
            backlog_segments: q("backlog"),
            replay_total: 0,
            replay_remaining: s
                .scalar("weir_drain_replay_remaining_segments")
                .map_or(0, |v| v as u64),
            blocked_since_unix_ms: s
                .scalar("weir_dead_letter_blocked_duration_seconds")
                .filter(|v| *v > 0.0)
                .map(|v| now.saturating_sub(ms(v))),
            last_requeue: None,
            exit,
            last_panic: None,
        });
    }

    if let Some(h) = s
        .one_hot("weir_sink_health", "state")
        .and_then(|v| find(SinkHealthCode::ALL, SinkHealthCode::as_str, v))
    {
        let failures = s.labelled("weir_sink_last_failure_timestamp_seconds", "kind");
        let last = failures
            .iter()
            .filter(|(_, v)| *v > 0.0)
            .max_by(|a, b| a.1.total_cmp(&b.1));
        e.sink = Some(SinkEvidence {
            health: Some(h),
            health_since_unix_ms: None,
            last_success_unix_ms: s
                .scalar("weir_sink_last_success_timestamp_seconds")
                .filter(|v| *v > 0.0)
                .map(ms),
            last_failure_unix_ms: last.map(|(_, v)| ms(*v)),
            last_failure_kind: last
                .and_then(|(k, _)| find(SinkFailureKind::ALL, SinkFailureKind::as_str, k)),
            consecutive_failures: s
                .scalar("weir_sink_consecutive_failures")
                .map_or(0, |v| v as u64),
            last_error: None,
            probe_reason: None,
        });
    }

    if let Some(segments) = s.scalar("weir_wab_undelivered_segments") {
        e.backlog = Some(BacklogEvidence {
            undelivered_segments: segments as u64,
            undelivered_records: s
                .scalar("weir_wab_undelivered_records")
                .map_or(0, |v| v as u64),
            oldest_undelivered_age_secs: s
                .scalar("weir_wab_oldest_undelivered_age_seconds")
                .filter(|v| *v > 0.0),
            dead_letter_bytes: s
                .scalar("weir_dead_letter_bytes_on_disk")
                .map_or(0, |v| v as u64),
            dead_letter_max_bytes: s
                .scalar("weir_config_dead_letter_max_bytes")
                .map(|v| v as u64),
            quarantine_bytes: s
                .scalar("weir_quarantine_bytes_on_disk")
                .map_or(0, |v| v as u64),
            quarantine_files: 0,
            scanned_at_ms: now,
        });
    }

    // Only absolutes: a single scrape has no window, so the windowed fields
    // stay None (unevaluated) rather than reading as "no recent failures".
    e.recent = Some(RecentCounters {
        drain_panics_total: s.scalar("weir_drain_panics_total").map(|v| v as u64),
        recovery_segments_failed: s
            .scalar("weir_recovery_segments_failed_total")
            .map(|v| v as u64),
        recovery_quarantine_copy_failed: s
            .scalar("weir_recovery_quarantine_copy_failed_total")
            .map(|v| v as u64),
        ..RecentCounters::default()
    });

    if let Some(sink_type) = s.one_hot("weir_sink_info", "sink_type") {
        let mut view = BTreeMap::new();
        for (key, metric) in [
            ("wab_max_bytes", "weir_config_wab_max_bytes"),
            ("dead_letter_max_bytes", "weir_config_dead_letter_max_bytes"),
            ("wab_segment_max_bytes", "weir_config_wab_segment_max_bytes"),
            (
                "wab_segment_max_age_secs",
                "weir_config_segment_max_age_seconds",
            ),
            (
                "wab_segment_max_lifetime_secs",
                "weir_config_segment_max_lifetime_seconds",
            ),
            ("sink_max_retries", "weir_config_sink_max_retries"),
            (
                "shutdown_timeout_secs",
                "weir_config_shutdown_timeout_seconds",
            ),
            (
                "drain_shutdown_budget_secs",
                "weir_config_drain_shutdown_budget_seconds",
            ),
        ] {
            if let Some(v) = s.scalar(metric) {
                view.insert(key.to_string(), ConfigValue::U64(v as u64));
            }
        }
        if let Some(n) = e.shards.as_ref().map(Vec::len) {
            view.insert("shard_count".to_string(), ConfigValue::U64(n as u64));
        }
        if !workers.is_empty() {
            view.insert(
                "worker_count".to_string(),
                ConfigValue::U64(workers.len() as u64),
            );
        }
        e.config = Some(ConfigEvidence {
            sink_type: sink_type.to_string(),
            commit_timeout_secs: None,
            public: PublicConfig::from_view(&view),
            view,
        });
    }
    e
}

/// The daemon's own verdicts from `weir_condition_active` (spec §14 step 3).
/// `None` when the family is absent (a pre-Foundations daemon). Unknown codes
/// are kept as `ConditionCode::Unknown`.
pub fn active_conditions_from_metrics(body: &str) -> Option<Vec<(ConditionCode, Severity)>> {
    let s = Scrape::parse(body);
    let series: Vec<&Sample<'_>> =
        s.0.iter()
            .filter(|x| x.name == "weir_condition_active")
            .collect();
    if series.is_empty() {
        return None;
    }
    Some(
        series
            .iter()
            .filter(|x| x.value >= 1.0)
            .filter_map(|x| {
                let code = ConditionCode::parse_lossy(label(x, "code")?);
                let severity = match label(x, "severity")? {
                    "info" => Severity::Info,
                    "warning" => Severity::Warning,
                    "critical" => Severity::Critical,
                    _ => return None,
                };
                Some((code, severity))
            })
            .collect(),
    )
}

#[cfg(test)]
impl Evidence {
    fn with_recent_absolutes_only(mut self) -> Self {
        self.recent = Some(RecentCounters::default());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    const NOW: u64 = 1_800_000_000_000;
    const V401: &str = include_str!("../tests/fixtures/metrics_4_0_1_min.txt");

    /// What a 4.1 daemon serving with two shards exposes (the families this
    /// PR adds, plus the 4.0.1 ones the parser reads).
    const V41: &str = "\
weir_start_time_seconds 1799999400
weir_startup_phase{phase=\"initializing\"} 0
weir_startup_phase{phase=\"serving\"} 1
weir_wab_shard_up{shard=\"0\"} 1
weir_wab_shard_up{shard=\"1\"} 0
weir_worker_up{worker=\"0\"} 1
weir_worker_up{worker=\"1\"} 1
weir_wab_handoff_blocked_seconds{shard=\"0\"} 3.5
weir_wab_handoff_blocked_seconds{shard=\"1\"} 0
weir_wab_active_segment_age_seconds{shard=\"0\"} 12
weir_wab_cap_rejecting 1
weir_wab_bytes_on_disk 950
weir_config_wab_max_bytes 1000
weir_config_dead_letter_max_bytes 100
weir_config_segment_max_age_seconds 0
weir_config_segment_max_lifetime_seconds 0
weir_wab_undelivered_segments 3
weir_wab_undelivered_records 40
weir_wab_oldest_undelivered_age_seconds 61.5
weir_drain_state{state=\"draining\"} 0
weir_drain_state{state=\"stopped\"} 1
weir_drain_panics_total 10
weir_drain_busy_seconds{kind=\"sink_commit\"} 0
weir_drain_busy_seconds{kind=\"segment_read\"} 7.5
weir_sink_health{state=\"down\"} 1
weir_sink_health{state=\"healthy\"} 0
weir_sink_info{sink_type=\"http\"} 1
weir_condition_active{code=\"wab_cap_rejecting\",severity=\"critical\"} 1
weir_condition_active{code=\"noop_sink\",severity=\"warning\"} 0
weir_condition_active{code=\"tls_cert_expiring\",severity=\"warning\"} 1
# EOF
";

    #[test]
    fn a_4_0_1_scrape_yields_only_what_4_0_1_exposed() {
        let e = evidence_from_metrics(V401, NOW);
        assert_eq!(e.source, Source::Metrics);
        assert_eq!(e.phase, None);
        assert!(
            e.shards.is_none() && e.workers.is_none() && e.wab_cap.is_none() && e.backlog.is_none()
        );
        assert_eq!(
            e.sink.as_ref().unwrap().health,
            Some(SinkHealthCode::Healthy)
        );
        assert_eq!(e.drain.as_ref().unwrap().state, DrainStateCode::Draining);
        assert_eq!(e.config.as_ref().unwrap().sink_type, "noop");
        assert_eq!(e.recent.as_ref().unwrap().recovery_segments_failed, Some(1));
        assert_eq!(
            e.recent.as_ref().unwrap().ack_timeouts_5m,
            None,
            "no window from one scrape"
        );
        let ev = evaluate(&e, &Thresholds::DEFAULT, &NoLatches);
        assert!(ev.is_active(ConditionCode::NoopSink));
        assert!(ev.is_active(ConditionCode::RecoveryIncomplete));
        assert!(ev.unevaluated.contains(&ConditionCode::Starting));
        assert!(
            !readiness(&e).ok,
            "an old daemon's readiness is unknown, not ready"
        );
        assert_eq!(active_conditions_from_metrics(V401), None);
    }

    #[test]
    fn a_4_1_scrape_reconstructs_phase_shards_cap_backlog_and_drain() {
        let e = evidence_from_metrics(V41, NOW);
        assert_eq!(e.phase, Some(Phase::Serving));
        assert_eq!(e.ever_served, Some(true));
        assert_eq!(e.shutting_down, Some(false));
        assert_eq!(e.started_at_unix_ms, Some(1_799_999_400_000));
        let shards = e.shards.as_ref().unwrap();
        assert_eq!(shards[0].state, ShardState::Up);
        assert_eq!(shards[0].handoff_blocked_since_unix_ms, Some(NOW - 3_500));
        assert_eq!(shards[0].active_segment_age_secs, Some(12.0));
        assert_eq!(shards[1].state, ShardState::Offline);
        let cap = e.wab_cap.unwrap();
        assert!(cap.rejecting);
        assert_eq!(
            (cap.bytes_now, cap.max_bytes, cap.low_water_bytes),
            (950, 1000, 900)
        );
        assert_eq!(e.backlog.as_ref().unwrap().undelivered_records, 40);
        assert_eq!(
            e.drain.as_ref().unwrap().exit,
            DrainExitCode::GaveUp,
            "stopped while serving = gave up"
        );
        assert_eq!(e.recent.as_ref().unwrap().drain_panics_total, Some(10));
        assert_eq!(
            e.drain.as_ref().unwrap().busy,
            Some(BusyEvidence {
                sink_call: false,
                since_unix_ms: NOW - 7_500
            }),
            "the series > 0 wins"
        );
        let ev = evaluate(&e, &Thresholds::DEFAULT, &NoLatches);
        for c in [
            ConditionCode::WabCapRejecting,
            ConditionCode::ShardOffline,
            ConditionCode::DrainStopped,
            ConditionCode::SinkDown,
        ] {
            assert!(ev.is_active(c), "{c:?}");
        }
        assert!(ev.unevaluated.contains(&ConditionCode::WabScanStale));
        // Listener states are not exported and V41 has no `weir_ready 1`, so
        // listener_down is unknown and readiness fails closed on it too.
        assert_eq!(e.listeners, None);
        assert_eq!(
            readiness(&e).failing,
            vec![ConditionCode::ListenerDown, ConditionCode::WabCapRejecting]
        );
    }

    #[test]
    fn the_daemons_own_ready_verdict_vouches_for_its_listeners() {
        let e = evidence_from_metrics(
            "weir_ready 1\nweir_startup_phase{phase=\"serving\"} 1\n",
            NOW,
        );
        assert_eq!(e.listeners, Some(Vec::new()));
        assert!(
            !evaluate(&e, &Thresholds::DEFAULT, &NoLatches)
                .unevaluated
                .contains(&ConditionCode::ListenerDown)
        );
    }

    #[test]
    fn active_conditions_keep_unknown_codes_and_skip_inactive_ones() {
        let active = active_conditions_from_metrics(V41).unwrap();
        assert_eq!(
            active,
            vec![
                (ConditionCode::WabCapRejecting, Severity::Critical),
                (ConditionCode::Unknown, Severity::Warning),
            ]
        );
    }

    #[test]
    fn the_parser_ignores_comments_blank_lines_and_garbage() {
        let e = evidence_from_metrics(
            "# EOF\n\nnot a sample line\nweir_wab_cap_rejecting notanumber\n",
            NOW,
        );
        assert_eq!(
            e,
            Evidence::empty(NOW, Source::Metrics).with_recent_absolutes_only()
        );
    }

    #[test]
    fn captured_expositions_parse() {
        let old = evidence_from_metrics(include_str!("../tests/fixtures/metrics_4_0_1.txt"), NOW);
        assert_eq!(old.phase, None);
        assert!(old.sink.is_some() && old.drain.is_some() && old.config.is_some());
        assert!(
            !readiness(&old).ok,
            "an old daemon's readiness is unknown, not ready"
        );
        assert_eq!(
            active_conditions_from_metrics(include_str!("../tests/fixtures/metrics_4_0_1.txt")),
            None
        );
        let new = evidence_from_metrics(include_str!("../tests/fixtures/metrics_4_1.txt"), NOW);
        assert_eq!(new.phase, Some(Phase::Serving));
        assert!(
            new.shards.is_some()
                && new.workers.is_some()
                && new.wab_cap.is_some()
                && new.backlog.is_some()
                && new.listeners.is_some()
        );
        assert!(readiness(&new).ok, "{:?}", readiness(&new).failing);
        assert!(
            active_conditions_from_metrics(include_str!("../tests/fixtures/metrics_4_1.txt"))
                .is_some()
        );
    }
}
