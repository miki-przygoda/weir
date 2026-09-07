//! AWS Signature Version 4, header-based, for S3 requests.
//!
//! Hand-rolled rather than taken from `aws-sdk-s3`: the official SDK adds 68
//! crates to weir-server's tree and, at default features, pulls `aws-lc-sys`
//! plus a second `rustls` — against a workspace that deliberately unified on
//! `ring`. Correctness is bought back with AWS's own published test vectors
//! (`tests/sigv4_vectors.rs`) rather than with trust.
//!
//! # Scope
//!
//! - **SigV4 only** — no SigV4a (multi-region access points).
//! - **Header-based only** — no presigned URLs; this sink only PUTs and HEADs.
//! - **The payload hash is always computed.** `UNSIGNED-PAYLOAD` is not used:
//!   the batch is already in memory and providers differ in what they accept.
//! - **Paths are encoded but never normalized.** S3 keys may legitimately
//!   contain `.`, `..` and `//` segments, so collapsing them would sign a
//!   different key than the one being written. This is the behaviour the test
//!   suite's `*-unnormalized` cases pin, and why its `*-normalized` twins are
//!   excluded.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::time::utc_from_unix_nanos;

type HmacSha256 = Hmac<Sha256>;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// Everything needed to sign one request.
pub struct SigningParams<'a> {
    /// Uppercase HTTP method.
    pub method: &'a str,
    /// The object key / path, **unencoded**, with a leading `/`.
    /// [`uri_encode_path`] is applied here — not by the caller.
    pub uri_path: &'a str,
    /// Query parameters, **unencoded**. Canonicalised (encoded and sorted) here.
    pub query: &'a [(String, String)],
    /// Authority for the `host` header — **including the port** when it is not
    /// the scheme default (the MinIO integration test signs `127.0.0.1:19000`).
    pub host: &'a str,
    /// Headers to sign beyond the required set. `host` and the `x-amz-*`
    /// headers are added by [`sign`]; do not pass them here.
    pub extra_headers: &'a [(String, String)],
    /// Request body.
    pub payload: &'a [u8],
    /// Access key id.
    pub access_key_id: &'a str,
    /// Secret access key.
    pub secret_access_key: &'a str,
    /// Session token, for temporary credentials.
    pub session_token: Option<&'a str>,
    /// Signing region, e.g. `us-east-1`.
    pub region: &'a str,
    /// `"s3"` in production; the AWS test suite uses `"service"`.
    pub service: &'a str,
    /// Whether to sign `x-amz-content-sha256`.
    ///
    /// **Always `true` for S3**, which requires it. It is a separate flag
    /// rather than being inferred from `service` because that is what the
    /// signing rule actually keys on: the AWS test suite's
    /// `post-x-www-form-urlencoded` case signs for `service` (not `s3`) and
    /// still expects the header, because its context sets `sign_body`. Gating
    /// on the service name passes 29 of 31 vectors and is wrong on both of the
    /// cases that distinguish the two rules.
    pub sign_payload_header: bool,
    /// Signing instant, unix nanoseconds.
    pub timestamp_nanos: i64,
}

/// The signing result: the authorization value plus **the exact header set to
/// attach to the request**.
///
/// Returning the headers rather than only the signature is deliberate. S3
/// requires `host`, `x-amz-date`, `x-amz-content-sha256` and (with temporary
/// credentials) `x-amz-security-token` to be *signed*, so a caller cannot
/// assemble them after the fact — it would need the date formatter and the
/// payload hash, which are internal. Handing back the full list makes signing a
/// set that differs from what is sent unrepresentable.
pub struct Signed {
    /// The `Authorization` header value.
    pub authorization: String,
    /// Every header to attach, `Authorization` included.
    pub headers: Vec<(String, String)>,
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    use std::fmt::Write as _;
    bytes.as_ref().iter().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

fn sha256_hex(data: &[u8]) -> String {
    hex(Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AWS `UriEncode`: percent-encode every byte outside `A-Za-z0-9-._~` with
/// **uppercase** hex. `encode_slash` is false for a path (where `/` separates
/// segments) and true for query components.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char);
            }
            b'/' if !encode_slash => out.push('/'),
            other => {
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

/// AWS `UriEncode` for a request path, preserving `/` as a segment separator.
///
/// S3 waives *double*-encoding and *normalization*; it does **not** waive
/// encoding. Skipping this is not cosmetic. `sink_s3_prefix` is
/// operator-supplied free text, so a space in it makes the signer sign
/// `/has space/` while the HTTP client sends `/has%20space/` — a permanent
/// `403 SignatureDoesNotMatch` with no diagnosis path. A `#` or `?` is worse:
/// the client truncates the path there, and signing the URL it parsed makes
/// both sides agree on the **wrong key**.
pub fn uri_encode_path(path: &str) -> String {
    uri_encode(path, false)
}

/// The canonical query string: each key and value URI-encoded, pairs sorted by
/// encoded key then encoded value, joined with `&`.
fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// `YYYYMMDD'T'HHMMSS'Z'`.
fn amz_date(nanos: i64) -> String {
    let t = utc_from_unix_nanos(nanos);
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year, t.month, t.day, t.hour, t.minute, t.second
    )
}

/// AWS `Trim()`: collapse runs of ASCII whitespace to a single space, then trim.
///
/// Deliberately not `str::trim()`, which strips Unicode whitespace such as
/// U+00A0 that AWS's `Trim` does not. The collapse applies **inside quotes**
/// too — the suite's `get-header-value-trim` expects `my-header2:"a b c"` from
/// `"a   b   c"`. This is the signing copy only; the original value is sent.
fn canonical_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    let mut pending_space = false;
    for c in v.chars() {
        if matches!(c, ' ' | '\t' | '\r' | '\n') {
            pending_space = true;
        } else {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            out.push(c);
        }
    }
    out
}

/// Lowercase names, canonical values, duplicate names merged comma-joined in
/// original request order, sorted by name.
fn normalised_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    let mut merged: Vec<(String, Vec<String>)> = Vec::new();
    for (k, v) in headers {
        let k = k.to_ascii_lowercase();
        let v = canonical_value(v);
        match merged.iter_mut().find(|(name, _)| *name == k) {
            Some((_, vals)) => vals.push(v),
            None => merged.push((k, vec![v])),
        }
    }
    let mut out: Vec<(String, String)> = merged
        .into_iter()
        .map(|(k, vals)| (k, vals.join(",")))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// `;`-joined lowercase header names, sorted, each appearing once.
fn signed_headers_list(headers: &[(String, String)]) -> String {
    normalised_headers(headers)
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";")
}

/// The full header set that is both signed and sent.
fn required_headers(p: &SigningParams<'_>) -> Vec<(String, String)> {
    let mut h = vec![
        ("host".to_string(), p.host.to_string()),
        ("x-amz-date".to_string(), amz_date(p.timestamp_nanos)),
    ];
    if p.sign_payload_header {
        h.push(("x-amz-content-sha256".to_string(), sha256_hex(p.payload)));
    }
    if let Some(tok) = p.session_token {
        h.push(("x-amz-security-token".to_string(), tok.to_string()));
    }
    h.extend(p.extra_headers.iter().cloned());
    h
}

/// The SigV4 canonical request.
pub fn canonical_request(p: &SigningParams<'_>) -> String {
    let headers = required_headers(p);
    let canonical_headers: String = normalised_headers(&headers)
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        p.method,
        uri_encode_path(p.uri_path),
        canonical_query(p.query),
        canonical_headers,
        signed_headers_list(&headers),
        sha256_hex(p.payload),
    )
}

/// `<YYYYMMDD>/<region>/<service>/aws4_request`.
fn credential_scope(p: &SigningParams<'_>) -> String {
    let d = &amz_date(p.timestamp_nanos)[..8];
    format!("{d}/{}/{}/aws4_request", p.region, p.service)
}

/// The SigV4 string-to-sign, given an already-built canonical request.
pub fn string_to_sign(p: &SigningParams<'_>, canonical: &str) -> String {
    format!(
        "{ALGORITHM}\n{}\n{}\n{}",
        amz_date(p.timestamp_nanos),
        credential_scope(p),
        sha256_hex(canonical.as_bytes()),
    )
}

/// `kDate → kRegion → kService → kSigning`.
fn signing_key(p: &SigningParams<'_>) -> Vec<u8> {
    let d = &amz_date(p.timestamp_nanos)[..8];
    let k_date = hmac_sha256(
        format!("AWS4{}", p.secret_access_key).as_bytes(),
        d.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, p.region.as_bytes());
    let k_service = hmac_sha256(&k_region, p.service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// The hex signature alone. Exposed because the vector suite's
/// `header-signature.txt` holds exactly this, not the full header.
pub fn signature(p: &SigningParams<'_>) -> String {
    let canonical = canonical_request(p);
    let sts = string_to_sign(p, &canonical);
    hex(hmac_sha256(&signing_key(p), sts.as_bytes()))
}

/// Signs a request, returning the authorization value and every header to
/// attach.
pub fn sign(p: &SigningParams<'_>) -> Signed {
    let sig = signature(p);
    let mut headers = required_headers(p);
    let authorization = format!(
        "{ALGORITHM} Credential={}/{}, SignedHeaders={}, Signature={sig}",
        p.access_key_id,
        credential_scope(p),
        signed_headers_list(&headers),
    );
    headers.push(("authorization".to_string(), authorization.clone()));
    Signed {
        authorization,
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_values_collapse_internal_whitespace_even_inside_quotes() {
        assert_eq!(canonical_value("  value1  "), "value1");
        assert_eq!(canonical_value("\"a   b   c\""), "\"a b c\"");
        assert_eq!(canonical_value("a\t\tb"), "a b");
    }

    #[test]
    fn duplicate_header_names_merge_into_one_comma_joined_line() {
        let h = vec![
            ("My-Header1".to_string(), "value2".to_string()),
            ("My-Header1".to_string(), "value1".to_string()),
        ];
        let n = normalised_headers(&h);
        assert_eq!(n.len(), 1, "duplicates must merge into one line");
        assert_eq!(n[0].1, "value2,value1", "merged in original request order");
        assert_eq!(
            signed_headers_list(&h),
            "my-header1",
            "a merged name appears once in SignedHeaders"
        );
    }

    #[test]
    fn uri_encoding_preserves_slashes_and_escapes_everything_else() {
        assert_eq!(uri_encode_path("/a/b.ndjson"), "/a/b.ndjson");
        assert_eq!(uri_encode_path("/example space/"), "/example%20space/");
        // The suite's get-utf8 case.
        assert_eq!(uri_encode_path("/\u{1234}"), "/%E1%88%B4");
        // Hive partition keys: '=' IS escaped. S3 percent-decodes, so the
        // stored key is still `dt=2026-09-06` and partition discovery works.
        assert_eq!(uri_encode_path("/dt=2026-09-06/x"), "/dt%3D2026-09-06/x");
    }

    #[test]
    fn percent_escapes_use_uppercase_hex() {
        // Only the two digits FOLLOWING a '%' are escape hex. Literal a-f
        // letters elsewhere in the key are untouched and must stay that way.
        let encoded = uri_encode_path("/dt=x/caf\u{e9}");
        let b = encoded.as_bytes();
        for (i, c) in b.iter().enumerate() {
            if *c == b'%' {
                for d in &b[i + 1..i + 3] {
                    assert!(
                        !d.is_ascii_lowercase(),
                        "escape digits must be uppercase hex, got {encoded}"
                    );
                }
            }
        }
        assert!(
            encoded.contains("%3D") && encoded.contains("%C3%A9"),
            "{encoded}"
        );
        assert!(
            encoded.contains("caf"),
            "literal letters must not be uppercased"
        );
    }

    #[test]
    fn paths_are_not_normalized() {
        // S3 keys may contain '.', '..' and '//' segments; collapsing them
        // would sign a different key than the one being written.
        assert_eq!(uri_encode_path("/a/./b"), "/a/./b");
        assert_eq!(uri_encode_path("/a/../b"), "/a/../b");
        assert_eq!(uri_encode_path("//a"), "//a");
    }

    #[test]
    fn the_canonical_query_is_encoded_and_sorted_by_key() {
        let q = vec![
            ("Param2".to_string(), "value2".to_string()),
            ("Param1".to_string(), "value1".to_string()),
        ];
        assert_eq!(canonical_query(&q), "Param1=value1&Param2=value2");
    }

    #[test]
    fn the_payload_hash_header_is_signed_only_when_asked_for() {
        let base = |sign_payload_header| SigningParams {
            method: "GET",
            uri_path: "/",
            query: &[],
            host: "example.amazonaws.com",
            extra_headers: &[],
            payload: b"",
            access_key_id: "AKIDEXAMPLE",
            secret_access_key: "secret",
            session_token: None,
            region: "us-east-1",
            service: "s3",
            sign_payload_header,
            timestamp_nanos: 1_440_938_160 * 1_000_000_000,
        };
        assert!(canonical_request(&base(true)).contains("x-amz-content-sha256"));
        assert!(
            !canonical_request(&base(false)).contains("x-amz-content-sha256"),
            "the header must be opt-in: the AWS vector suite has cases on both \
             sides of this rule and they key on sign_body, not the service name"
        );
    }
}
