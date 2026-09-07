//! Known-answer tests against AWS's published SigV4 test suite.
//!
//! Vectors are vendored under `tests/vectors/` from
//! `awslabs/aws-c-auth/tests/aws-signing-test-suite/v4` — see that directory's
//! README for the retrieval date, the licence, and why seven cases are skipped.
//!
//! They are an **independent oracle**: nothing here recomputes an expectation.
//! The harness parses the fixture (splitting the request line, percent-decoding
//! the query back to values, honouring the `sign_body` and `omit_session_token`
//! context flags) but performs no canonicalisation of its own, so a bug in the
//! signer cannot be cancelled out by a matching bug here.
//!
//! Assertions are staged — canonical request, then string-to-sign, then the
//! signature — so a regression fails at the earliest divergent stage rather
//! than as an opaque `403 SignatureDoesNotMatch` against a live endpoint.
//!
//! This mirrors what weir already does for its own wire format
//! (`docs/conformance.md`): checked-in canonical vectors that cannot drift,
//! because implementation and expectation are asserted against the same files.

use weir_sink_s3::sigv4_test_hooks::{SigningParams, canonical_request, signature, string_to_sign};

const SKIP: &[&str] = &[
    "get-relative-normalized",
    "get-relative-relative-normalized",
    "get-slash-dot-slash-normalized",
    "get-slash-normalized",
    "get-slash-pointless-dot-normalized",
    "get-slashes-normalized",
    "get-space-normalized",
];

/// "2015-08-30T12:36:00Z" -> unix nanos.
///
/// days_from_civil (Hinnant) written independently of the module under test, so
/// the harness is not validated by the code it is checking.
fn parse_ts(s: &str) -> i64 {
    let y: i64 = s[0..4].parse().unwrap();
    let mo: i64 = s[5..7].parse().unwrap();
    let d: i64 = s[8..10].parse().unwrap();
    let h: i64 = s[11..13].parse().unwrap();
    let mi: i64 = s[14..16].parse().unwrap();
    let se: i64 = s[17..19].parse().unwrap();
    let yy = if mo <= 2 { y - 1 } else { y };
    let era = if yy >= 0 { yy } else { yy - 399 } / 400;
    let yoe = yy - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    ((days * 86_400) + h * 3600 + mi * 60 + se) * 1_000_000_000
}

/// Percent-decodes a query component. The fixture stores a wire-format request,
/// so decoding it back to values is fixture parsing -- not the signer's job,
/// which is to re-encode them canonically.
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap(), 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap()
}

#[test]
fn every_applicable_aws_vector_matches_at_every_stage() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors");
    let mut checked = 0usize;
    let mut failed: Vec<String> = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for e in entries {
        let p = e.path();
        if !p.is_dir() {
            continue;
        }
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        if SKIP.contains(&name.as_str()) {
            continue;
        }
        let req = std::fs::read_to_string(p.join("request.txt")).unwrap();
        let ctx: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(p.join("context.json")).unwrap())
                .unwrap();
        let exp_creq = std::fs::read_to_string(p.join("header-canonical-request.txt")).unwrap();
        let exp_sts = std::fs::read_to_string(p.join("header-string-to-sign.txt")).unwrap();
        let exp_sig = std::fs::read_to_string(p.join("header-signature.txt")).unwrap();

        let mut lines = req.split('\n');
        let start = lines.next().unwrap().trim_end_matches('\r');
        // The target may itself contain spaces (get-space-unnormalized), so
        // split on the FIRST and LAST space, not on every space.
        let first = start.find(' ').unwrap();
        let last = start.rfind(' ').unwrap();
        let method = start[..first].to_string();
        let target = start[first + 1..last].to_string();
        let (raw_path, raw_query) = match target.split_once('?') {
            Some((a, b)) => (a.to_string(), b.to_string()),
            None => (target.clone(), String::new()),
        };

        let mut headers: Vec<(String, String)> = Vec::new();
        let mut body = String::new();
        let mut in_body = false;
        for l in lines {
            let l = l.trim_end_matches('\r');
            if in_body {
                if !body.is_empty() {
                    body.push('\n');
                }
                body.push_str(l);
                continue;
            }
            if l.is_empty() {
                in_body = true;
                continue;
            }
            if l.starts_with(' ') || l.starts_with('\t') {
                if let Some(last) = headers.last_mut() {
                    last.1.push(' ');
                    last.1.push_str(l.trim());
                }
                continue;
            }
            let (k, v) = l.split_once(':').unwrap();
            headers.push((k.to_string(), v.to_string()));
        }

        let query: Vec<(String, String)> = if raw_query.is_empty() {
            vec![]
        } else {
            raw_query
                .split('&')
                .map(|kv| match kv.split_once('=') {
                    Some((k, v)) => (pct_decode(k), pct_decode(v)),
                    None => (pct_decode(kv), String::new()),
                })
                .collect()
        };

        let host = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("host"))
            .map(|(_, v)| v.trim().to_string())
            .unwrap_or_default();
        let extra: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| !k.eq_ignore_ascii_case("host"))
            .cloned()
            .collect();

        let creds = &ctx["credentials"];
        let sign_body = ctx["sign_body"].as_bool().unwrap_or(false);
        let payload: Vec<u8> = if sign_body {
            body.clone().into_bytes()
        } else {
            Vec::new()
        };
        // `omit_session_token` means the token is attached AFTER signing, so it
        // must not appear in the canonical request.
        let omit = ctx["omit_session_token"].as_bool().unwrap_or(false);
        let token = if omit {
            None
        } else {
            creds["token"]
                .as_str()
                .map(std::string::ToString::to_string)
        };

        let params = SigningParams {
            method: &method,
            uri_path: &raw_path,
            query: &query,
            host: &host,
            extra_headers: &extra,
            payload: &payload,
            access_key_id: creds["access_key_id"].as_str().unwrap(),
            secret_access_key: creds["secret_access_key"].as_str().unwrap(),
            session_token: token.as_deref(),
            region: ctx["region"].as_str().unwrap(),
            service: ctx["service"].as_str().unwrap(),
            sign_payload_header: sign_body,
            timestamp_nanos: parse_ts(ctx["timestamp"].as_str().unwrap()),
        };

        let got_creq = canonical_request(&params);
        if got_creq.trim_end() != exp_creq.trim_end() {
            failed.push(format!(
                "{name}: CANONICAL\n--got--\n{got_creq}\n--want--\n{exp_creq}"
            ));
            continue;
        }
        let got_sts = string_to_sign(&params, &got_creq);
        if got_sts.trim_end() != exp_sts.trim_end() {
            failed.push(format!(
                "{name}: STS\n--got--\n{got_sts}\n--want--\n{exp_sts}"
            ));
            continue;
        }
        let got_sig = signature(&params);
        if got_sig.trim_end() != exp_sig.trim_end() {
            failed.push(format!("{name}: SIG got={got_sig} want={exp_sig}"));
            continue;
        }
        checked += 1;
    }

    eprintln!("PASSED {checked}, FAILED {}", failed.len());
    for f in failed.iter().take(3) {
        eprintln!("\n{f}");
    }
    assert!(failed.is_empty(), "{} vector(s) failed", failed.len());
    // 38 cases upstream minus the 7 that require path normalization.
    assert_eq!(
        checked, 31,
        "expected all 31 applicable vectors to run; a change in the vendored \
         set or the skip list needs a matching change here"
    );
}
