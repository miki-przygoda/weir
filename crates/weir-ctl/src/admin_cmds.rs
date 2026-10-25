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
/// `error_json` object) and returns its exit: 2 unreachable, identity
/// mismatch or unusable reply; 1 an error response from an authenticated
/// daemon.
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
/// socket, with the SIGHUP hint.
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
}
