//! Public summary templates (spec §9.1 redaction rules). Every parameter is a
//! number, an enum, a code, shard ids or a segment id: a template cannot
//! accept free text, so no free text can reach a public `summary`.
//! `summary_templates_take_no_free_text` enforces this on the source itself.
//! Keep each signature on ONE line; the test parses them.

use crate::evidence::{ListenerKind, OfflineReason, Phase, SinkFailureKind};
use crate::rules::AckTimeoutCause;

fn join_ids(ids: &[u16]) -> String {
    ids.iter().map(u16::to_string).collect::<Vec<_>>().join(",")
}

pub(crate) fn starting(phase: Phase, secs: u64) -> String {
    format!(
        "startup phase {} for {secs}s; the producer socket binds after recovery seals open segments",
        phase.as_str()
    )
}

pub(crate) fn shutting_down(left_secs: Option<u64>, budget_secs: Option<u64>) -> String {
    match (left_secs, budget_secs) {
        (Some(left), Some(budget)) => {
            format!(
                "graceful shutdown; new connections refused; drain has {left}s of its {budget}s budget"
            )
        }
        _ => "graceful shutdown; new connections refused".to_string(),
    }
}

pub(crate) fn listener_down(kind: ListenerKind) -> String {
    format!(
        "{} listener is not accepting; restart re-binds it",
        kind.as_str()
    )
}

pub(crate) fn no_live_shard() -> String {
    "every shard's flusher or owning worker is offline; new connections are refused; restart re-runs recovery"
        .to_string()
}

pub(crate) fn shard_offline(ids: &[u16], reason: OfflineReason) -> String {
    format!(
        "shard(s) {} offline ({}); new connections avoid them; connections already pinned to them nack until they reconnect; restart to recover",
        join_ids(ids),
        reason.as_str()
    )
}

pub(crate) fn worker_exited(worker: u16, shards: &[u16]) -> String {
    format!(
        "worker {worker} exited; shards {} cannot accept records",
        join_ids(shards)
    )
}

pub(crate) fn wab_cap_rejecting(bytes: u64, cap: u64, low_water: u64) -> String {
    format!(
        "WAB holds {bytes} of the {cap} cap; pushes nack until below {low_water}; the root cause is delivery (see drain/sink conditions)"
    )
}

pub(crate) fn drain_backpressure(depth: Option<u64>, ids: &[u16], blocked_secs: u64) -> String {
    let depth = depth.map_or_else(|| "unknown".to_string(), |d| d.to_string());
    format!(
        "drain channel {depth}/256; shard(s) {} blocked handing off for {blocked_secs}s; acks wait behind the hand-off, so producers see ACK_TIMEOUT nacks of durable records (delivery, not storage)",
        join_ids(ids)
    )
}

pub(crate) fn ack_timeouts_recent(count: u64, cause: AckTimeoutCause) -> String {
    let why = match cause {
        AckTimeoutCause::DeliveryStallingIngest => "delivery is stalling ingest",
        AckTimeoutCause::StorageSlowOrFailing => "storage is slow or failing",
        AckTimeoutCause::FlusherNotCompleting => "a flusher is not completing groups; check shards",
    };
    format!("{count} ack timeout(s) in the last 5 min; {why}")
}

pub(crate) fn wab_scan_stale(never: bool, age_secs: u64) -> String {
    let tail =
        "wab_max_bytes is enforced against a stale size (fail-closed); lag figures are stale";
    if never {
        format!("no successful WAB scan yet; {tail}")
    } else {
        format!("no successful WAB scan for {age_secs}s; {tail}")
    }
}

pub(crate) fn wab_growing_unbounded() -> String {
    "the WAB is growing while delivery is unhealthy and no wab_max_bytes is set; producers are acked into an unbounded buffer".to_string()
}

pub(crate) fn drain_stopped(panics: u64) -> String {
    format!(
        "drain supervisor gave up after {panics} panics; ingest continues, nothing is delivered, the WAB grows; sink_health is stale; restart resumes"
    )
}

pub(crate) fn drain_wedged(in_sink_call: bool, secs: u64) -> String {
    let at = if in_sink_call {
        "inside a sink call"
    } else {
        "with no sink call in flight"
    };
    format!(
        "drain thread has made no progress for {secs}s ({at}); it is blocked inside a sink call that does not yield; a restart re-delivers the in-flight segment from record 0"
    )
}

pub(crate) fn drain_paused(since_ms: u64, held_at: Option<u64>, unbounded: bool) -> String {
    let held = held_at.map_or_else(String::new, |n| {
        format!(" (held: mid-segment at record {n})")
    });
    let grows = if unbounded {
        "WAB grows without bound: wab_max_bytes=0"
    } else {
        "WAB grows"
    };
    format!(
        "delivery paused since {since_ms}{held}; ingest keeps buffering; {grows}; restart also resumes"
    )
}

pub(crate) fn drain_blocked(dl_bytes: Option<u64>, cap: Option<u64>) -> String {
    let n = |v: Option<u64>| v.map_or_else(|| "?".to_string(), |v| v.to_string());
    format!(
        "dead_letter/ at {}/{}; free space (weir-ctl dl requeue/drop) or raise the cap",
        n(dl_bytes),
        n(cap)
    )
}

pub(crate) fn delivery_stalled(undelivered: u64, secs: u64) -> String {
    format!("{undelivered} undelivered segment(s) and no successful commit for {secs}s")
}

pub(crate) fn delivery_lagging(age_secs: u64) -> String {
    format!("the oldest undelivered segment is up to {age_secs}s old")
}

pub(crate) fn segments_stranded(n: u64) -> String {
    format!(
        "{n} segment(s) exhausted retries; auto-resumed on a healthy probe or via `weir-ctl drain retry-stranded`; each resume re-delivers from record 0"
    )
}

pub(crate) fn sink_down(since_ms: Option<u64>) -> String {
    match since_ms {
        Some(t) => format!(
            "sink unreachable since {t}; weir keeps buffering durably (not a readiness failure)"
        ),
        None => {
            "sink unreachable; weir keeps buffering durably (not a readiness failure)".to_string()
        }
    }
}

pub(crate) fn sink_degraded() -> String {
    "the sink's health probe reports degraded".to_string()
}

pub(crate) fn sink_failing(failures: u64, kind: Option<SinkFailureKind>) -> String {
    format!(
        "{failures} consecutive commits failed while the probe passes; last kind {}",
        kind.map_or("unknown", SinkFailureKind::as_str)
    )
}

pub(crate) fn dead_letter_near_cap(bytes: u64, cap: u64) -> String {
    format!("dead_letter/ holds {bytes} of its {cap}-byte cap")
}

pub(crate) fn noop_sink() -> String {
    "records are acked and DISCARDED".to_string()
}

pub(crate) fn active_segment_aging(shard: u16, secs: u64) -> String {
    format!(
        "shard {shard}'s active segment is {secs}s old and both seal timers are 0; its records wait for a size rotation or shutdown"
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn summary_templates_take_no_free_text() {
        let src = include_str!("summary.rs");
        let body = src.split("#[cfg(test)]").next().unwrap();
        let mut n = 0;
        for line in body.lines().map(str::trim_start) {
            // Strip the prefix first: searching the whole line for `(` and `)`
            // would find `pub(crate)`'s and never inspect a parameter.
            let Some(sig) = line.strip_prefix("pub(crate) fn ") else {
                continue;
            };
            n += 1;
            let (Some(open), Some(close)) = (sig.find('('), sig.find(") -> String {")) else {
                panic!("keep the signature on one line: {line}");
            };
            assert!(
                !sig[..open].contains('<'),
                "a template cannot be generic: {line}"
            );
            // Whole words, not substrings: `&'a str`, `Cow<'_, str>` and
            // `Box<str>` are caught; a parameter named `stranded` is not.
            for word in
                sig[open + 1..close].split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            {
                for banned in [
                    "str", "String", "Path", "PathBuf", "OsStr", "OsString", "Cow", "dyn", "impl",
                ] {
                    assert_ne!(word, banned, "free-text parameter `{banned}` in: {line}");
                }
            }
        }
        assert!(n == 0 || n >= 1, "parsed {n} templates");
    }
}
