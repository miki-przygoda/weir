//! Built-in sink implementations + a re-export of the [`weir_sink_sdk`] contract.
//!
//! The `Sink` trait, its error/record/result types, and the idempotency contract
//! live in the published [`weir_sink_sdk`] crate so third parties can implement
//! sinks without depending on the daemon. This module re-exports them and houses
//! weir's own built-in sinks (feature-gated — see crate `[features]`):
//!
//! - [`noop::NoopSink`] — always compiled; accepts all records, forwards nothing.
//!   The default when `sink_type = "noop"`. Useful for soak-testing the pipeline.
//! - [`http::HttpSink`] — feature `http-sink`; POSTs each record to a configurable
//!   URL with transient/permanent error classification.
//! - [`mysql::MySqlSink`] — feature `mysql-sink`; writes a whole batch with one
//!   multi-row `INSERT`. The IOPS-compression sink: N records → 1 statement.
//! - [`postgres::PostgresSink`] — feature `postgres-sink`; Postgres counterpart,
//!   `ON CONFLICT DO NOTHING` for idempotency.
//! - [`clickhouse::ClickHouseSink`] — feature `clickhouse-sink`; HTTP
//!   `INSERT … FORMAT RowBinary` with a sha256 `insert_deduplication_token`.

#[cfg(feature = "clickhouse-sink")]
pub mod clickhouse;
#[cfg(feature = "http-sink")]
pub mod http;
#[cfg(feature = "mysql-sink")]
pub mod mysql;
pub mod noop;
#[cfg(feature = "postgres-sink")]
pub mod postgres;
#[cfg(feature = "_sql-sink")]
mod sql_common;

pub use weir_sink_sdk::{CommitResult, Sink, SinkError, SinkHealth, SinkRecord};

/// Redacts credentials from a URL for `Debug`/log output. Scrubs BOTH the
/// userinfo password (`scheme://user:<redacted>@host…`) AND the values of
/// known credential query parameters (`?password=<redacted>`, `?token=…`, …).
/// Leaves anything else untouched — it locates the secret substrings rather
/// than validating the URL, so debug formatting can never fail and a malformed
/// URL is returned as-is (after query scrubbing).
///
/// The query-parameter pass matters because credentials are commonly carried in
/// the query string (`?password=…`, presigned-URL `?sig=…`, `?api_key=…`), and
/// that part of the URL surfaces both in the startup INFO log and — embedded in
/// reqwest's transport-error string — in the drain's per-retry `warn!`. The
/// userinfo-only redaction missed those entirely (S31 follow-up).
///
/// Lives here (always compiled) rather than in the feature-gated `sql_common`
/// so the config layer's `RedactedUrl` Debug impl can reuse one implementation
/// regardless of which sink features are built in (F59). The SQL sinks delegate
/// to it via `sql_common::redact_password`.
pub(crate) fn redact_url_password(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        // No scheme (e.g. a bare host or a non-URL string): still scrub any
        // credential query parameters that may be present.
        return redact_query_credentials(url);
    };
    let scheme = &url[..scheme_end];
    let rest = &url[scheme_end + 3..];
    // The userinfo lives in the authority, which ends at the first '/', '?', or
    // '#'. Restrict the search there so a '@' in the path/query can't be mistaken
    // for the userinfo/host separator.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    // The remainder (path?query#fragment) gets the query-credential pass.
    let after_authority = redact_query_credentials(&rest[authority_end..]);

    // The userinfo/host boundary is the LAST '@' in the authority: a password may
    // contain unencoded '@' characters, so the FIRST '@' would split mid-password
    // and leak the tail. user:password splits at the FIRST ':' (a username carries
    // no unencoded ':'). Absent either, the authority is left as-is.
    let redacted_authority = match authority.rfind('@') {
        Some(at) => match authority[..at].find(':') {
            Some(colon) => format!("{}:<redacted>{}", &authority[..colon], &authority[at..]),
            None => authority.to_string(),
        },
        None => authority.to_string(),
    };

    format!("{scheme}://{redacted_authority}{after_authority}")
}

/// Replaces the values of known credential query parameters with `<redacted>`
/// in a `…?query#fragment` (or bare-query) substring. Pairs whose key is not a
/// known credential name, and the fragment, are left untouched. Conservative by
/// design: only a fixed denylist of unambiguously-secret keys is scrubbed, so
/// benign diagnostic params survive in logs.
fn redact_query_credentials(s: &str) -> String {
    let Some(q) = s.find('?') else {
        return s.to_string();
    };
    let head = &s[..q];
    let after_q = &s[q + 1..];
    // Keep the fragment (and anything after it) verbatim.
    let (query, frag) = match after_q.find('#') {
        Some(h) => (&after_q[..h], &after_q[h..]),
        None => (after_q, ""),
    };
    let redacted = query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((key, _)) if is_credential_query_key(key) => format!("{key}=<redacted>"),
            _ => pair.to_string(),
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{head}?{redacted}{frag}")
}

/// True for query-parameter keys that carry credentials and must be redacted.
/// Case-insensitive. Intentionally a narrow denylist of clearly-secret names so
/// benign params (ids, keys-as-in-partition-key) stay visible for debugging.
fn is_credential_query_key(key: &str) -> bool {
    const CREDENTIAL_KEYS: &[&str] = &[
        "password",
        "passwd",
        "pwd",
        "token",
        "access_token",
        "secret",
        "api_key",
        "apikey",
        "sig",
        "signature",
    ];
    CREDENTIAL_KEYS.iter().any(|k| key.eq_ignore_ascii_case(k))
}

/// Replaces ASCII/Unicode control characters with `.` for safe logging.
///
/// Downstream sink response bodies are interpolated into log lines and
/// dead-letter reason strings. Without this, a hostile or compromised endpoint
/// could embed newlines (forging extra log records) or terminal escape sequences
/// in its body and have the daemon emit them verbatim (S29). Only the HTTP and
/// ClickHouse sinks read a response body, so it is compiled only for those.
#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
pub(crate) fn sanitize_log_excerpt(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '.' } else { c })
        .collect()
}

/// Upper bound on how many response-body bytes a sink will buffer. Success bodies
/// are discarded and error bodies are truncated to a short excerpt, so 64 KiB is
/// ample for any legitimate endpoint while capping peak memory at
/// `concurrency × 64 KiB` against a hostile/compromised downstream (S28).
#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
pub(crate) const RESPONSE_BODY_CAP: usize = 64 * 1024;

/// Reads at most `cap` bytes of a response body, then stops — dropping the rest
/// (and the connection, which is fine: a partially-read response is not pooled).
/// Bounds memory regardless of what the downstream sends; the success path
/// discards the body and the error path only needs a short excerpt (S28).
#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
pub(crate) async fn read_body_capped(mut resp: reqwest::Response, cap: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    while buf.len() < cap {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let take = (cap - buf.len()).min(chunk.len());
                buf.extend_from_slice(&chunk[..take]);
                if take < chunk.len() {
                    break; // hit the cap mid-chunk
                }
            }
            Ok(None) => break, // end of body
            Err(_) => break,   // read error — return what we have so far
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_password_covers_adversarial_inputs() {
        // No userinfo → unchanged.
        assert_eq!(
            redact_url_password("https://host/path"),
            "https://host/path"
        );
        // Simple user:pass.
        assert_eq!(
            redact_url_password("https://u:p@host/path"),
            "https://u:<redacted>@host/path"
        );
        // Password containing '@' must split at the LAST '@', not leak the tail.
        assert_eq!(
            redact_url_password("https://u:p@ss@host/db"),
            "https://u:<redacted>@host/db"
        );
        // A '@' in the path must not be mistaken for the userinfo separator.
        assert_eq!(
            redact_url_password("https://host/p@th"),
            "https://host/p@th"
        );
        // Username only (no password) → unchanged (nothing to redact).
        assert_eq!(
            redact_url_password("https://user@host/x"),
            "https://user@host/x"
        );
        // The literal password must never survive in the output.
        let red = redact_url_password("postgres://admin:sup3rS3cret@db:5432/app");
        assert!(!red.contains("sup3rS3cret"), "password leaked: {red}");
        assert!(red.contains("<redacted>"));
        // A URL embedded in surrounding text — the exact shape of a reqwest
        // transport error ("...for url (<url>)"), which the HTTP sink runs through
        // this helper (S31). The password must be redacted even mid-string.
        let red = redact_url_password(
            "error sending request for url (https://u:sup3rS3cret@host/ingest)",
        );
        assert!(!red.contains("sup3rS3cret"), "password leaked: {red}");
        assert!(red.contains("<redacted>"));
        assert!(red.contains("host/ingest"), "host diagnostic lost: {red}");
    }

    #[test]
    fn redact_url_password_scrubs_query_string_credentials() {
        // Credentials in the query string (the gap the userinfo-only redaction
        // missed): the value must be replaced and the host/path must survive.
        for (input, secret) in [
            ("https://host/ingest?password=sup3rS3cret", "sup3rS3cret"),
            ("https://host/ingest?token=t0p-s3cret", "t0p-s3cret"),
            ("https://host/ingest?api_key=ak_live_999", "ak_live_999"),
            ("https://host/ingest?x=1&signature=deadbeef&y=2", "deadbeef"),
            // Presigned-URL style sig=.
            ("https://bucket.s3/obj?sig=AbC123Xyz", "AbC123Xyz"),
        ] {
            let red = redact_url_password(input);
            assert!(!red.contains(secret), "query credential leaked: {red}");
            assert!(red.contains("<redacted>"), "no redaction marker: {red}");
            assert!(
                red.contains("host") || red.contains("bucket"),
                "host lost: {red}"
            );
        }
        // Benign query params must survive (narrow denylist, not blanket).
        let red = redact_url_password("https://host/ingest?region=eu&partition_key=orders&x=1");
        assert_eq!(
            red,
            "https://host/ingest?region=eu&partition_key=orders&x=1"
        );
        // Case-insensitive key match, and other pairs are preserved verbatim.
        let red = redact_url_password("https://host/i?Region=eu&PassWord=hunter2&tag=z");
        assert!(!red.contains("hunter2"), "case-insensitive miss: {red}");
        assert!(
            red.contains("Region=eu") && red.contains("tag=z"),
            "benign pairs lost: {red}"
        );
        // Both userinfo AND query credentials in one URL → both scrubbed.
        let red = redact_url_password("https://u:pw1@host/ingest?token=pw2");
        assert!(!red.contains("pw1") && !red.contains("pw2"), "leak: {red}");
        // A fragment after the query is left intact.
        let red = redact_url_password("https://host/i?token=secret#section");
        assert!(
            !red.contains("secret") && red.contains("#section"),
            "fragment handling: {red}"
        );
        // No query string → unchanged.
        assert_eq!(
            redact_url_password("https://host/path"),
            "https://host/path"
        );
    }

    /// Coverage gap (T05/T09 / S28): read_body_capped returns at most `cap` bytes
    /// regardless of how large a body the server sends, stopping once the cap is
    /// reached (the network-read memory bound). Uses a tiny cap + a body well
    /// past it, so no large allocation is needed to prove the branch.
    #[cfg(feature = "http-sink")]
    #[tokio::test]
    async fn read_body_capped_stops_at_cap_on_oversized_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut tmp = [0u8; 1024];
                let _ = sock.read(&mut tmp).await; // consume the request
                let body = vec![b'Z'; 1000]; // far past the 16-byte cap below
                let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.flush().await;
            }
        });
        let resp = reqwest::Client::new()
            .get(format!("http://{addr}/"))
            .send()
            .await
            .unwrap();
        let bytes = read_body_capped(resp, 16).await;
        assert_eq!(
            bytes.len(),
            16,
            "read_body_capped must stop at the cap, not buffer the whole 1000-byte body"
        );
    }

    #[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]
    #[test]
    fn sanitize_log_excerpt_strips_control_chars() {
        assert_eq!(
            sanitize_log_excerpt("ok\n2026 INFO forged log line\r\x1b[31m"),
            "ok.2026 INFO forged log line..[31m"
        );
        // Normal text and non-control Unicode pass through untouched.
        assert_eq!(sanitize_log_excerpt("plain café"), "plain café");
    }
}
