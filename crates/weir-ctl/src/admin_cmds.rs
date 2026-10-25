//! Mutating admin-socket commands: `reload` and `drain *` (spec §14). No
//! offline fallback: a mutation needs a daemon that proved who it is.

use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};

use crate::admin_client::{self, AdminTarget, CallError, instance_line};

/// A reload loads certificate files in `spawn_blocking`; generous but bounded.
pub(crate) const RELOAD_REPLY_TIMEOUT: Duration = Duration::from_secs(15);

const SIGHUP_HINT: &str = "`kill -HUP <pid>` uses the same code path";

/// The exit and the outcome word for a reload result. The daemon's wire
/// spells success `applied` (spec §5.10); the safety floor's report spelled
/// it `ok`. Both are accepted, `noop` too. Anything else, a missing outcome
/// included, exits 1: weir-ctl cannot vouch for a success it does not
/// recognise.
pub(crate) fn reload_outcome_exit(result: &Value) -> (u8, String) {
    match result.get("outcome").and_then(Value::as_str) {
        Some(o @ ("applied" | "ok" | "noop")) => (0, o.to_string()),
        Some(other) => (1, other.to_string()),
        None => (1, "missing".to_string()),
    }
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("?").to_string()
}

/// Renders spec §9.1's `items` / `not_reloaded` and the safety floor's
/// `changed` / `rejected` / `note`, whichever the daemon sent.
pub(crate) fn reload_item_lines(r: &Value) -> Vec<String> {
    let arr = |k: &str| {
        r.get(k)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    let mut out = Vec::new();
    for it in arr("items") {
        let detail = it
            .get("detail")
            .and_then(Value::as_str)
            .map(|d| format!(" ({d})"))
            .unwrap_or_default();
        out.push(format!("{}: {}{detail}", s(&it, "item"), s(&it, "outcome")));
    }
    for it in arr("not_reloaded") {
        let why = it
            .get("reason_code")
            .or_else(|| it.get("reason"))
            .and_then(Value::as_str)
            .unwrap_or("?");
        out.push(format!("{}: not reloaded ({why})", s(&it, "item")));
    }
    for c in arr("changed") {
        if let Some(name) = c.as_str() {
            out.push(format!("{name}: reloaded"));
        }
    }
    for rj in arr("rejected") {
        let (name, why) = match &rj {
            Value::Array(p) if p.len() == 2 => (
                p[0].as_str().unwrap_or("?").to_string(),
                p[1].as_str().unwrap_or("?").to_string(),
            ),
            Value::Object(_) => (s(&rj, "item"), s(&rj, "reason")),
            _ => continue,
        };
        out.push(format!("{name}: rejected ({why})"));
    }
    if let Some(n) = r.get("note").and_then(Value::as_str) {
        out.push(format!("note: {n}"));
    }
    out
}

/// Prints a failed admin call to `err` (under `--json` as the shared
/// `error_json` object) and returns its exit: 2 unreachable, no reply to a
/// sent request, identity mismatch or unusable reply; 1 an error response
/// from an authenticated daemon. A sent request without a reply is not
/// called unreachable: the daemon may have applied it.
pub(crate) fn report_call_error(
    verb: &str,
    e: &CallError,
    json: bool,
    err: &mut impl Write,
    unreachable_hint: Option<&str>,
) -> u8 {
    let (code, msg) = match e {
        CallError::Unreachable(w) => (
            2,
            match unreachable_hint {
                Some(h) => format!("{verb}: admin socket unreachable: {w}\n  hint: {h}"),
                None => format!("{verb}: admin socket unreachable: {w}"),
            },
        ),
        CallError::NoReply(w) => (
            2,
            format!(
                "{verb}: {w}, but the request was sent: it may have been applied; check \
                 `weir-ctl status`"
            ),
        ),
        CallError::Identity(w) => (2, format!("{verb}: identity mismatch: {w}")),
        CallError::Protocol(w) => (2, format!("{verb}: unusable reply: {w}")),
        CallError::Remote {
            code,
            message,
            retryable,
        } => (
            1,
            format!(
                "{verb} failed: {code}: {message}{}",
                if *retryable { " (retryable)" } else { "" }
            ),
        ),
    };
    if json {
        let _ = writeln!(err, "{}", crate::error_json(&msg));
    } else {
        let _ = writeln!(err, "weir-ctl: {msg}");
    }
    code
}

/// `weir-ctl reload`: one admin `reload` call. Exit 0 applied or noop, 1
/// failed (or an outcome weir-ctl does not recognise), 2 no usable admin
/// socket (with the SIGHUP hint) or no reply to the sent request.
pub(crate) fn reload_run(
    target: &AdminTarget,
    json: bool,
    out: &mut impl Write,
    err: &mut impl Write,
) -> u8 {
    match admin_client::call(target, "reload", json!({}), RELOAD_REPLY_TIMEOUT) {
        Ok(r) => {
            let (code, outcome) = reload_outcome_exit(&r.result);
            if json {
                crate::write_json_to(out, &r.result);
            } else {
                let _ = writeln!(out, "reload: {outcome} ({})", instance_line(&r.instance));
                for l in reload_item_lines(&r.result) {
                    let _ = writeln!(out, "  {l}");
                }
            }
            code
        }
        Err(e) => report_call_error("reload", &e, json, err, Some(SIGHUP_HINT)),
    }
}

pub(crate) fn cmd_reload(target: &AdminTarget, json: bool) -> ExitCode {
    ExitCode::from(reload_run(
        target,
        json,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainVerb {
    Pause,
    Resume,
    RetryStranded,
}

impl DrainVerb {
    pub(crate) fn wire(self) -> &'static str {
        match self {
            DrainVerb::Pause => "drain.pause",
            DrainVerb::Resume => "drain.resume",
            DrainVerb::RetryStranded => "drain.retry_stranded",
        }
    }
    /// The daemon's default `wait_ms` when weir-ctl sends none (spec §11).
    pub(crate) fn default_wait_ms(self) -> u64 {
        match self {
            DrainVerb::Pause | DrainVerb::Resume => 5_000,
            DrainVerb::RetryStranded => 10_000,
        }
    }
}

/// The admin protocol's `wait_ms` range is 0..=30000.
pub(crate) const MAX_WAIT_MS: u64 = 30_000;
pub(crate) const PAUSE_IN_MEMORY: &str = "in-memory only; a daemon restart resumes delivery";
/// PR 6 review follow-up. A strand holds its segment off for the backoff its
/// last retry would have served (the ledger's `not_before`, spec §4.6), and
/// the retry-stranded rescan honours it so a sink's Retry-After is never
/// violated. Such a segment is not counted as requeued; a later call picks it
/// up. The cool-down is the strand's backoff: `sink_retry_base_delay_ms`
/// doubled once per retry, or the sink's Retry-After, capped at 300 s.
pub(crate) const RETRY_COOL_DOWN_HINT: &str = "segments still inside their cool-down are skipped; \
     run it again once the cool-down has passed (sink_retry_base_delay_ms × 2^sink_max_retries \
     after the strand, or the sink's Retry-After, at most 300 s)";
/// Margin over the daemon-side wait for the reply to arrive.
const REPLY_MARGIN_MS: u64 = 5_000;

fn wait_ms(wait_secs: u64) -> u64 {
    wait_secs.saturating_mul(1000).min(MAX_WAIT_MS)
}

/// The `drain.*` args: `wait_ms` only when `--wait` was given, so the daemon
/// applies its own default otherwise.
pub(crate) fn drain_args(wait_secs: Option<u64>) -> Value {
    match wait_secs {
        None => json!({}),
        Some(s) => json!({"wait_ms": wait_ms(s)}),
    }
}

/// How long weir-ctl waits for the reply: the daemon-side wait plus a margin.
pub(crate) fn drain_reply_timeout(verb: DrainVerb, wait_secs: Option<u64>) -> Duration {
    let wait = wait_secs.map_or(verb.default_wait_ms(), wait_ms);
    Duration::from_millis(wait + REPLY_MARGIN_MS)
}

fn flag(v: &Value, k: &str) -> bool {
    v.get(k).and_then(Value::as_bool).unwrap_or(false)
}

/// The daemon's own `observed` flag when present. Otherwise the conservative
/// reading of the verb's result keys: unknown means "not yet" (exit 4).
pub(crate) fn drain_observed(verb: DrainVerb, result: &Value) -> bool {
    if let Some(b) = result.get("observed").and_then(Value::as_bool) {
        return b;
    }
    match verb {
        DrainVerb::Pause => flag(result, "paused") || flag(result, "held"),
        // PR 6's ResumeResult: `resumed` = observed no longer paused or held.
        DrainVerb::Resume => flag(result, "resumed"),
        DrainVerb::RetryStranded => flag(result, "completed"),
    }
}

/// The human lines for a `drain.*` result.
pub(crate) fn drain_lines(verb: DrainVerb, r: &Value, observed: bool) -> Vec<String> {
    let num = |k: &str| {
        r.get(k)
            .filter(|v| !v.is_null())
            .map_or_else(|| "?".to_string(), Value::to_string)
    };
    let mut out = Vec::new();
    match verb {
        DrainVerb::Pause => {
            out.push(if !observed {
                "drain.pause: requested; not in effect yet (it takes effect before the next commit batch)".to_string()
            } else if let Some(h) = r.get("held_segment").filter(|v| v.is_object()) {
                format!(
                    "drain.pause: paused mid-segment ({} at record {})",
                    h.get("segment").and_then(Value::as_str).unwrap_or("?"),
                    h.get("processed_records")
                        .map_or_else(|| "?".to_string(), Value::to_string)
                )
            } else {
                "drain.pause: paused".to_string()
            });
            for w in r
                .get("warnings")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(w) = w.as_str() {
                    out.push(format!("⚠ {w}"));
                }
            }
            out.push(PAUSE_IN_MEMORY.to_string());
        }
        DrainVerb::Resume => out.push(if !observed {
            "drain.resume: requested; the drain has not picked it up yet".to_string()
        } else if r.get("was_paused").and_then(Value::as_bool) == Some(false) {
            "drain.resume: delivery was not paused".to_string()
        } else {
            match r.get("paused_for_secs").and_then(Value::as_f64) {
                Some(s) => format!("drain.resume: delivery resumed (paused for {s:.0}s)"),
                None => "drain.resume: delivery resumed".to_string(),
            }
        }),
        DrainVerb::RetryStranded => {
            if observed {
                let mut l = format!(
                    "drain.retry_stranded: requeued {}, already queued {}",
                    num("requeued"),
                    num("already_queued")
                );
                if flag(r, "backoff_cut") {
                    l.push_str(", backoff cut");
                }
                if flag(r, "coalesced") {
                    l.push_str(" (coalesced with a retry already running)");
                }
                out.push(l);
                if let Some(e) = r.get("scan_error").and_then(Value::as_str) {
                    out.push(format!("⚠ the stranded-segment scan failed: {e}"));
                }
                if flag(r, "paused") {
                    out.push(
                        "⚠ delivery is paused: requeued segments wait for `weir-ctl drain resume`"
                            .to_string(),
                    );
                }
            } else {
                out.push(format!(
                    "drain.retry_stranded: requested (ticket {}); not completed within the wait",
                    num("ticket")
                ));
            }
            if let Some(n) = r.get("note").and_then(Value::as_str) {
                out.push(n.to_string());
            }
            out.push(RETRY_COOL_DOWN_HINT.to_string());
        }
    }
    if !observed {
        out.push("re-run `weir-ctl status` to see when it takes effect (exit 4)".to_string());
    }
    out
}

/// One `drain.*` call. Exit 0 observed, 4 requested but not in effect yet,
/// 1 an error response, 2 unreachable, no reply or an identity mismatch. No
/// offline fallback.
pub(crate) fn drain_run(
    verb: DrainVerb,
    wait_secs: Option<u64>,
    target: &AdminTarget,
    json: bool,
    out: &mut impl Write,
    err: &mut impl Write,
) -> u8 {
    match admin_client::call(
        target,
        verb.wire(),
        drain_args(wait_secs),
        drain_reply_timeout(verb, wait_secs),
    ) {
        Ok(r) => {
            let observed = drain_observed(verb, &r.result);
            if json {
                crate::write_json_to(out, &r.result);
            } else {
                for l in drain_lines(verb, &r.result, observed) {
                    let _ = writeln!(out, "{l}");
                }
                let _ = writeln!(out, "{}", instance_line(&r.instance));
            }
            if observed { 0 } else { 4 }
        }
        Err(e) => report_call_error(verb.wire(), &e, json, err, None),
    }
}

pub(crate) fn cmd_drain(
    verb: DrainVerb,
    wait_secs: Option<u64>,
    target: &AdminTarget,
    json: bool,
) -> ExitCode {
    ExitCode::from(drain_run(
        verb,
        wait_secs,
        target,
        json,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    #[test]
    fn reload_outcomes_map_to_the_documented_exits() {
        for (outcome, code) in [
            ("applied", 0),
            ("ok", 0),
            ("noop", 0),
            ("failed", 1),
            ("exploded", 1),
        ] {
            assert_eq!(
                reload_outcome_exit(&json!({"outcome": outcome})).0,
                code,
                "{outcome}"
            );
        }
        assert_eq!(
            reload_outcome_exit(&json!({})).0,
            1,
            "a missing outcome is not a success"
        );
    }

    #[test]
    fn reload_items_render_from_either_report_shape() {
        let spec_shape = json!({"outcome": "failed",
            "items": [{"item": "tls_certificates", "outcome": "failed", "detail": "bad key"}],
            "not_reloaded": [{"item": "config_file", "reason_code": "restart_required"}]});
        let l = reload_item_lines(&spec_shape);
        assert!(
            l.iter().any(|x| x == "tls_certificates: failed (bad key)"),
            "{l:?}"
        );
        assert!(
            l.iter()
                .any(|x| x == "config_file: not reloaded (restart_required)"),
            "{l:?}"
        );
        let floor_shape = json!({"outcome": "noop", "changed": [], "rejected": [["tls", "unreadable"]], "note": "nothing_reloadable"});
        let l = reload_item_lines(&floor_shape);
        assert!(l.iter().any(|x| x == "tls: rejected (unreadable)"), "{l:?}");
        assert!(l.iter().any(|x| x == "note: nothing_reloadable"), "{l:?}");
    }

    #[test]
    fn unreachable_reload_exits_2_with_the_sighup_hint() {
        let t = crate::admin_client::resolve_admin_target(
            Some(Path::new("/nonexistent/weir/wab.admin.sock")),
            None,
        );
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(reload_run(&t, false, &mut out, &mut err), 2);
        let err = String::from_utf8(err).unwrap();
        assert!(
            err.contains("`kill -HUP <pid>` uses the same code path"),
            "{err}"
        );
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(reload_run(&t, true, &mut out, &mut err), 2);
        let e: serde_json::Value = serde_json::from_slice(&err).unwrap();
        assert!(
            e["error"].as_str().unwrap().contains("unreachable"),
            "--json keeps error_json"
        );
    }

    /// A mutation whose request reached the daemon but whose reply did not
    /// come back may have been applied: the report says so and points at
    /// `weir-ctl status` instead of calling the socket unreachable. Same exit.
    #[cfg(unix)]
    #[test]
    fn a_sent_request_without_a_reply_may_have_been_applied() {
        use crate::admin_client::test_support::*;
        use std::io::BufRead;
        use std::os::unix::fs::PermissionsExt;
        let dir = private_dir("noreply");
        let wab = dir.join("wab");
        std::fs::create_dir_all(&wab).unwrap();
        let sock = dir.join("a.sock");
        let l = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600)).unwrap();
        // Reads each request and never answers, holding every connection open.
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let h = std::thread::spawn(move || {
            let mut held = Vec::new();
            for _ in 0..3 {
                let (s, _) = l.accept().unwrap();
                let mut req = String::new();
                std::io::BufReader::new(&s).read_line(&mut req).unwrap();
                held.push(s);
            }
            let _ = done_rx.recv();
        });
        let t = crate::admin_client::resolve_admin_target(Some(&sock), Some(&wab));
        for verb in ["drain.pause", "drain.resume", "reload"] {
            let e = crate::admin_client::call(&t, verb, json!({}), Duration::from_millis(300))
                .unwrap_err();
            let hint = (verb == "reload").then_some(SIGHUP_HINT);
            let mut err = Vec::new();
            assert_eq!(
                report_call_error(verb, &e, false, &mut err, hint),
                2,
                "{verb}"
            );
            let err = String::from_utf8(err).unwrap();
            assert!(
                err.contains("the request was sent")
                    && err.contains("may have been applied")
                    && err.contains("`weir-ctl status`"),
                "{verb}: {err}"
            );
            assert!(!err.contains("unreachable"), "{verb}: {err}");
            assert!(!err.contains("kill -HUP"), "{verb}: {err}");
            let mut err = Vec::new();
            assert_eq!(report_call_error(verb, &e, true, &mut err, hint), 2);
            let v: serde_json::Value = serde_json::from_slice(&err).unwrap();
            assert!(
                v["error"]
                    .as_str()
                    .unwrap()
                    .contains("may have been applied"),
                "--json keeps error_json: {v}"
            );
        }
        done_tx.send(()).unwrap();
        h.join().unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn reload_against_a_daemon_maps_failed_to_1_and_noop_to_0() {
        use crate::admin_client::test_support::*;
        for (outcome, code) in [("failed", 1u8), ("noop", 0u8)] {
            let dir = private_dir(&format!("rl{code}"));
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let inst = instance_for(&wab);
            let h = fake_admin(&dir.join("a.sock"), move |req| {
                json!({"v": 1, "id": req["id"].clone(), "instance": inst.clone(), "ok": true,
                       "result": {"trigger": "admin", "outcome": outcome, "items": []}})
            });
            let t =
                crate::admin_client::resolve_admin_target(Some(&dir.join("a.sock")), Some(&wab));
            let (mut out, mut err) = (Vec::new(), Vec::new());
            assert_eq!(reload_run(&t, false, &mut out, &mut err), code);
            let out = String::from_utf8(out).unwrap();
            assert!(out.starts_with(&format!("reload: {outcome}")), "{out}");
            assert_eq!(h.join().unwrap()["verb"], "reload");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn wait_maps_to_wait_ms_with_the_protocol_cap() {
        assert_eq!(
            drain_args(None),
            json!({}),
            "absent: the daemon's default applies"
        );
        assert_eq!(drain_args(Some(5)), json!({"wait_ms": 5000}));
        assert_eq!(drain_args(Some(0)), json!({"wait_ms": 0}));
        assert_eq!(drain_args(Some(99)), json!({"wait_ms": 30000}));
        assert_eq!(
            drain_reply_timeout(DrainVerb::RetryStranded, None),
            Duration::from_millis(15_000)
        );
        assert_eq!(
            drain_reply_timeout(DrainVerb::Pause, Some(99)),
            Duration::from_millis(35_000)
        );
    }

    #[test]
    fn observed_prefers_the_daemons_flag_and_falls_back_conservatively() {
        assert!(drain_observed(
            DrainVerb::Pause,
            &json!({"observed": true, "paused": false})
        ));
        assert!(!drain_observed(
            DrainVerb::Pause,
            &json!({"observed": false, "paused": true})
        ));
        assert!(drain_observed(
            DrainVerb::Pause,
            &json!({"paused": false, "held": true})
        ));
        assert!(!drain_observed(
            DrainVerb::Pause,
            &json!({"pause_requested": true})
        ));
        assert!(
            !drain_observed(DrainVerb::Resume, &json!({"was_paused": true})),
            "no observed flag: exit 4, not a guess"
        );
        assert!(drain_observed(
            DrainVerb::Resume,
            &json!({"was_paused": true, "resumed": true})
        ));
        assert!(!drain_observed(
            DrainVerb::Resume,
            &json!({"was_paused": true, "resumed": false})
        ));
        assert!(drain_observed(
            DrainVerb::RetryStranded,
            &json!({"completed": true})
        ));
        assert!(!drain_observed(
            DrainVerb::RetryStranded,
            &json!({"completed": false, "ticket": 3})
        ));
    }

    #[test]
    fn pause_always_says_it_is_in_memory_only() {
        for (observed, result) in [
            (
                true,
                json!({"paused": true, "held": true, "held_segment": {"segment": "shard_00/seg_00000007", "processed_records": 1200},
                          "warnings": ["wab_max_bytes is 0: the WAB grows without bound while paused"]}),
            ),
            (
                false,
                json!({"pause_requested": true, "paused": false, "held": false, "warnings": []}),
            ),
        ] {
            let l = drain_lines(DrainVerb::Pause, &result, observed);
            assert!(l.iter().any(|x| x == PAUSE_IN_MEMORY), "{l:?}");
            if observed {
                assert!(
                    l[0].contains("mid-segment") && l[0].contains("1200"),
                    "{l:?}"
                );
                assert!(
                    l.iter().any(|x| x.starts_with("⚠ wab_max_bytes is 0")),
                    "{l:?}"
                );
            } else {
                assert!(l[0].contains("not in effect yet"), "{l:?}");
            }
        }
    }

    #[test]
    fn retry_stranded_lines_report_counts_or_the_pending_ticket() {
        let done = drain_lines(
            DrainVerb::RetryStranded,
            &json!({"ticket": 4, "completed": true, "coalesced": false, "requeued": 3, "already_queued": 1,
                    "backoff_cut": true, "scan_error": null, "paused": true,
                    "note": "requeued segments restart from record 0"}),
            true,
        );
        assert!(
            done[0].contains("requeued 3")
                && done[0].contains("already queued 1")
                && done[0].contains("backoff cut"),
            "{done:?}"
        );
        assert!(
            done.iter().any(|x| x.contains("paused")),
            "a paused drain will not deliver them yet: {done:?}"
        );
        assert!(done.iter().any(|x| x.contains("record 0")), "{done:?}");
        let pending = drain_lines(
            DrainVerb::RetryStranded,
            &json!({"ticket": 5, "completed": false}),
            false,
        );
        assert!(
            pending[0].contains("ticket 5") && pending[0].contains("not completed"),
            "{pending:?}"
        );
    }

    /// PR 6 review follow-up: a segment still inside its per-segment
    /// cool-down (its `not_before`, spec §4.6) is skipped by the rescan, so
    /// the requeued count can be lower than the stranded count. The human
    /// output says so, and says when a second call can pick it up.
    #[test]
    fn retry_stranded_says_cooling_down_segments_are_skipped() {
        for (observed, result) in [
            (
                true,
                json!({"ticket": 1, "completed": true, "requeued": 0, "already_queued": 0}),
            ),
            (false, json!({"ticket": 2, "completed": false})),
        ] {
            let l = drain_lines(DrainVerb::RetryStranded, &result, observed);
            assert!(
                l.iter().any(|x| x == RETRY_COOL_DOWN_HINT),
                "observed={observed}: {l:?}"
            );
        }
        assert!(RETRY_COOL_DOWN_HINT.contains("cool-down"));
        assert!(RETRY_COOL_DOWN_HINT.contains("skipped"));
        assert!(RETRY_COOL_DOWN_HINT.contains("sink_retry_base_delay_ms"));
        for verb in [DrainVerb::Pause, DrainVerb::Resume] {
            let l = drain_lines(verb, &json!({"observed": true}), true);
            assert!(
                !l.iter().any(|x| x == RETRY_COOL_DOWN_HINT),
                "{verb:?}: {l:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn drain_run_exit_codes_against_a_daemon() {
        use crate::admin_client::test_support::*;
        let cases: [(DrainVerb, Option<u64>, serde_json::Value, u8); 4] = [
            (
                DrainVerb::Pause,
                Some(5),
                json!({"ok": true, "result": {"observed": true, "paused": true, "held": false, "warnings": []}}),
                0,
            ),
            (
                DrainVerb::Pause,
                None,
                json!({"ok": true, "result": {"observed": false, "paused": false, "held": false, "warnings": []}}),
                4,
            ),
            (
                DrainVerb::RetryStranded,
                Some(1),
                json!({"ok": true, "result": {"ticket": 9, "completed": false}}),
                4,
            ),
            (
                DrainVerb::Resume,
                None,
                json!({"ok": false, "error": {"code": "shutting_down", "message": "shutting down", "retryable": false}}),
                1,
            ),
        ];
        for (i, (verb, wait, body, code)) in cases.into_iter().enumerate() {
            let dir = private_dir(&format!("dr{i}"));
            let wab = dir.join("wab");
            std::fs::create_dir_all(&wab).unwrap();
            let inst = instance_for(&wab);
            let h = fake_admin(&dir.join("a.sock"), move |req| {
                let mut r = body.clone();
                r["v"] = json!(1);
                r["id"] = req["id"].clone();
                r["instance"] = inst.clone();
                r
            });
            let t =
                crate::admin_client::resolve_admin_target(Some(&dir.join("a.sock")), Some(&wab));
            let (mut out, mut err) = (Vec::new(), Vec::new());
            assert_eq!(
                drain_run(verb, wait, &t, false, &mut out, &mut err),
                code,
                "case {i}"
            );
            let req = h.join().unwrap();
            assert_eq!(req["verb"], verb.wire());
            assert_eq!(req["args"], drain_args(wait));
            if verb == DrainVerb::Pause {
                assert!(String::from_utf8(out).unwrap().contains(PAUSE_IN_MEMORY));
            }
            if code == 1 {
                assert!(
                    String::from_utf8(err)
                        .unwrap()
                        .contains("drain.resume failed: shutting_down")
                );
            }
            std::fs::remove_dir_all(&dir).ok();
        }
        // No socket: 2, no offline fallback.
        let t =
            crate::admin_client::resolve_admin_target(Some(Path::new("/nonexistent/x.sock")), None);
        let (mut out, mut err) = (Vec::new(), Vec::new());
        assert_eq!(
            drain_run(DrainVerb::Pause, None, &t, false, &mut out, &mut err),
            2
        );
    }
}
