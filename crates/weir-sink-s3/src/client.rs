//! S3 HTTP transport: `PutObject`, `HeadBucket`, and error classification.

use crate::creds::CredentialChain;
use crate::redact::{sanitize_log_excerpt, truncate};
use crate::sigv4::{SigningParams, sign};

/// Whether a failed request should be retried or dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Classification {
    /// Strand the segment and retry it.
    Transient,
    /// Dead-letter the batch.
    Permanent,
}

/// Classifies an S3 error response.
///
/// # The rule
///
/// **Dead-letter on record-level rejection; strand on connection-level
/// rejection.** A record is dead-lettered only when *that record* is the
/// problem. Anything about the credential, the endpoint or the bucket applies
/// equally to every record in the backlog, so it strands — which is recoverable
/// and visible (`WeirSegmentStranded` fires, the `wab_max_bytes` cap engages) —
/// rather than dead-lettering, which silently displaces acked data.
///
/// # Why this departs from the ClickHouse sink
///
/// `weir-server`'s `sink/clickhouse.rs:214` maps auth 4xx to permanent. This
/// sink does not, deliberately. A day-one IAM misconfiguration, an expired IRSA
/// token, a rotated key and a signing bug all present as `403`; dead-lettering
/// an entire backlog over any of them converts a recoverable operator error
/// into displaced data. The ClickHouse precedent is arguably wrong for the same
/// reason, but changing it is a separate matter.
pub(crate) fn classify(status: u16, s3_code: Option<&str>) -> Classification {
    if matches!(
        s3_code,
        Some(
            "AccessDenied"
                | "SignatureDoesNotMatch"
                | "ExpiredToken"
                | "TokenRefreshRequired"
                | "InvalidAccessKeyId"
                | "RequestTimeTooSkewed"
                | "NoSuchBucket"
                | "SlowDown"
        )
    ) {
        return Classification::Transient;
    }
    // 400 is NOT uniformly a record-level fault, which an earlier version
    // assumed. S3 answers 400 for transport corruption, for facts about the
    // bucket, for credential problems, and -- worst -- for headers this sink
    // itself adds from config. Each of those applies to every record in the
    // backlog, so each must strand.
    if matches!(
        s3_code,
        Some(
            // Transport: the body did not arrive intact. Retrying re-sends it.
            "RequestTimeout"
                | "BadDigest"
                | "IncompleteBody"
                | "XAmzContentSHA256Mismatch"
                // A fact about the bucket, not the record.
                | "InvalidBucketName"
                // Sibling of ExpiredToken; an STS token problem, not a record one.
                | "InvalidToken"
                // Produced by sink_s3_storage_class / sink_s3_sse / sink_s3_sse_kms_key_id
                // -- headers THIS SINK adds. A config typo must not dead-letter
                // a backlog; that is the same argument this module makes for 403.
                | "InvalidArgument"
                | "InvalidStorageClass"
        )
    ) || s3_code.is_some_and(|c| c.starts_with("KMS."))
    {
        // SSE-KMS burns a GenerateDataKey call per object and KMS has a
        // per-region rate quota, so KMS.ThrottlingException is the expected
        // response to throughput -- backpressure wearing a 400.
        return Classification::Transient;
    }
    match status {
        // Genuinely record-level: these are facts about the bytes just sent,
        // and the same bytes will be rejected again.
        //   400 -- a malformed request not matched above
        //   411 -- missing Content-Length (unreachable via reqwest)
        //   413 -- EntityTooLarge: this batch is too big for the endpoint
        400 | 411 | 413 => Classification::Permanent,
        // Everything else -- 5xx, throttling, auth, missing bucket, and any
        // status this code does not recognise. Providers diverge (R2, B2,
        // Ceph), and stranding an unknown response is recoverable while
        // dead-lettering it is not.
        _ => Classification::Transient,
    }
}

/// Extracts `<Code>…</Code>` from an S3 error body.
///
/// Searches outside `<Message>` first: some providers quote a different error
/// code inside the human-readable message, and taking the first `<Code>`
/// anywhere would classify on the quoted one. Falls back to a plain search so a
/// body this heuristic does not fit still yields something.
pub(crate) fn s3_error_code(body: &str) -> Option<&str> {
    fn first_code(s: &str) -> Option<(usize, usize)> {
        let start = s.find("<Code>")? + "<Code>".len();
        let end = s[start..].find("</Code>")? + start;
        Some((start, end))
    }
    // Strip the Message element, then look there first.
    if let Some(m0) = body.find("<Message>")
        && let Some(m1) = body[m0..]
            .find("</Message>")
            .map(|i| i + m0 + "</Message>".len())
    {
        let outside = format!("{}{}", &body[..m0], &body[m1..]);
        if let Some((s, e)) = first_code(&outside) {
            // Map back into `body` by searching for the exact code text.
            let code = &outside[s..e];
            if let Some(p) = body.find(&format!("<Code>{code}</Code>")) {
                let s = p + "<Code>".len();
                return Some(&body[s..s + code.len()]);
            }
        }
    }
    let (s, e) = first_code(body)?;
    Some(&body[s..e])
}

/// A transport-level or HTTP-level failure, already classified.
#[derive(Debug)]
pub(crate) struct S3Error {
    pub(crate) classification: Classification,
    pub(crate) message: String,
}

impl S3Error {
    fn transient(message: impl Into<String>) -> Self {
        Self {
            classification: Classification::Transient,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Where objects go and how to reach them.
#[derive(Debug, Clone)]
pub(crate) struct Endpoint {
    /// Scheme + authority, no trailing slash, e.g. `https://s3.us-east-1.amazonaws.com`
    /// or `http://127.0.0.1:19000`.
    pub(crate) base: String,
    pub(crate) bucket: String,
    pub(crate) region: String,
    /// `true` puts the bucket in the path rather than the hostname. Required by
    /// MinIO and most self-hosted gateways.
    pub(crate) force_path_style: bool,
}

impl Endpoint {
    /// The `(url, host, signed_path)` triple for a key.
    ///
    /// All three are derived from one encoded path so the signature and the
    /// request can never disagree — the failure mode `sigv4::uri_encode_path`
    /// exists to prevent.
    fn resolve(&self, key: &str) -> (String, String, String) {
        let (scheme, authority) = match self.base.split_once("://") {
            Some((s, a)) => (s, a.trim_end_matches('/')),
            None => ("https", self.base.trim_end_matches('/')),
        };
        if self.force_path_style {
            let path = format!("/{}/{}", self.bucket, key);
            let encoded = crate::sigv4::uri_encode_path(&path);
            (
                format!("{scheme}://{authority}{encoded}"),
                authority.to_string(),
                path,
            )
        } else {
            let host = format!("{}.{authority}", self.bucket);
            let path = format!("/{key}");
            let encoded = crate::sigv4::uri_encode_path(&path);
            (format!("{scheme}://{host}{encoded}"), host, path)
        }
    }
}

/// Signs and sends S3 requests.
#[derive(Debug)]
pub(crate) struct S3Client {
    http: reqwest::Client,
    creds: std::sync::Arc<CredentialChain>,
    endpoint: Endpoint,
    /// Extra headers attached to every `PutObject` (storage class, SSE).
    put_headers: Vec<(String, String)>,
}

impl S3Client {
    pub(crate) fn new(
        http: reqwest::Client,
        creds: std::sync::Arc<CredentialChain>,
        endpoint: Endpoint,
        put_headers: Vec<(String, String)>,
    ) -> Self {
        Self {
            http,
            creds,
            endpoint,
            put_headers,
        }
    }

    /// Current wall-clock, unix seconds. Used only for signing and credential
    /// expiry — never for an object key, which must survive a replay.
    fn now_unix() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or_default()
    }

    async fn send(
        &self,
        method: &str,
        key: &str,
        body: Vec<u8>,
        extra: Vec<(String, String)>,
    ) -> Result<(u16, String), S3Error> {
        let now = Self::now_unix();
        let creds = self
            .creds
            .resolve(now)
            .await
            .map_err(|e| S3Error::transient(format!("credentials: {e}")))?;
        let (url, host, path) = self.endpoint.resolve(key);

        let signed = sign(&SigningParams {
            method,
            uri_path: &path,
            query: &[],
            host: &host,
            extra_headers: &extra,
            payload: &body,
            access_key_id: &creds.access_key_id,
            secret_access_key: creds.secret_access_key.expose(),
            session_token: creds
                .session_token
                .as_ref()
                .map(crate::redact::SecretString::expose),
            region: &self.endpoint.region,
            service: "s3",
            sign_payload_header: true,
            timestamp_nanos: now * 1_000_000_000,
        });

        let mut req = match method {
            "PUT" => self.http.put(&url),
            "HEAD" => self.http.head(&url),
            other => self.http.request(
                other
                    .parse()
                    .map_err(|_| S3Error::transient("bad method"))?,
                &url,
            ),
        };
        for (k, v) in &signed.headers {
            req = req.header(k, v);
        }
        let resp = req
            .body(body)
            .send()
            .await
            .map_err(|e| S3Error::transient(sanitize_log_excerpt(&e.to_string())))?;
        let status = resp.status().as_u16();
        // HEAD responses carry no body; PUT errors carry an XML one.
        //
        // A body that fails mid-read is a TRANSPORT failure, not an empty body.
        // Swallowing it would hand `classify` a `None` error code and, for a
        // 400, dead-letter the batch on the strength of evidence that was
        // erased rather than absent.
        match resp.text().await {
            Ok(text) => Ok((status, text)),
            Err(e) => Err(S3Error::transient(format!(
                "HTTP {status} but the response body could not be read: {}",
                sanitize_log_excerpt(&e.to_string())
            ))),
        }
    }

    /// Writes one object.
    pub(crate) async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: &'static str,
    ) -> Result<(), S3Error> {
        let mut extra = vec![("content-type".to_string(), content_type.to_string())];
        extra.extend(self.put_headers.iter().cloned());
        let (status, body_text) = self.send("PUT", key, body, extra).await?;
        if (200..300).contains(&status) {
            return Ok(());
        }
        let code = s3_error_code(&body_text);
        Err(S3Error {
            classification: classify(status, code),
            message: format!(
                "PutObject {key} -> HTTP {status}{}: {}",
                code.map(|c| format!(" {c}")).unwrap_or_default(),
                truncate(&sanitize_log_excerpt(&body_text), 256)
            ),
        })
    }

    /// Probes the bucket. Returns the raw status so [`crate::S3Sink::health`]
    /// can distinguish 403 from 404.
    pub(crate) async fn head_bucket(&self) -> Result<u16, S3Error> {
        let (status, _) = self.send("HEAD", "", Vec::new(), Vec::new()).await?;
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_errors_and_throttling_are_transient() {
        for s in [500u16, 502, 503, 504, 408, 429] {
            assert_eq!(classify(s, None), Classification::Transient, "status {s}");
        }
    }

    #[test]
    fn slow_down_is_transient() {
        // S3's own backpressure signal arrives as 503 SlowDown.
        assert_eq!(classify(503, Some("SlowDown")), Classification::Transient);
    }

    #[test]
    fn transport_level_400s_strand_rather_than_dead_letter() {
        // Each of these is a 400 that says the bytes did not arrive intact, or
        // says something about the bucket or the credential. An earlier version
        // mapped all 400s to Permanent and dead-lettered acked records on them.
        for code in [
            "RequestTimeout",
            "BadDigest",
            "IncompleteBody",
            "XAmzContentSHA256Mismatch",
            "InvalidBucketName",
            "InvalidToken",
        ] {
            assert_eq!(
                classify(400, Some(code)),
                Classification::Transient,
                "400 {code} must strand"
            );
        }
    }

    #[test]
    fn a_400_caused_by_this_sinks_own_config_headers_strands() {
        // sink_s3_storage_class / sink_s3_sse / sink_s3_sse_kms_key_id become
        // request headers. A typo in one must not dead-letter a backlog -- that
        // is the same argument this module makes for 403, applied to a header
        // the sink itself adds.
        for code in ["InvalidArgument", "InvalidStorageClass"] {
            assert_eq!(
                classify(400, Some(code)),
                Classification::Transient,
                "{code}"
            );
        }
    }

    #[test]
    fn kms_throttling_is_backpressure_wearing_a_400() {
        // SSE-KMS burns a GenerateDataKey call per object and KMS has a
        // per-region rate quota, so this is the expected response to throughput
        // -- the one case where a 400 means "slow down", not "bad request".
        assert_eq!(
            classify(400, Some("KMS.ThrottlingException")),
            Classification::Transient
        );
        assert_eq!(
            classify(400, Some("KMS.KeyUnavailableException")),
            Classification::Transient
        );
    }

    #[test]
    fn only_request_shape_faults_are_permanent() {
        // Dead-lettering is for bytes the downstream will never accept.
        // EntityTooLarge is the real one: this batch is too big for the
        // endpoint, and the same bytes will be rejected again.
        for (s, code) in [
            (400u16, Some("InvalidRequest")),
            (413, Some("EntityTooLarge")),
            (411, None),
        ] {
            assert_eq!(
                classify(s, code),
                Classification::Permanent,
                "status {s} code {code:?}"
            );
        }
    }

    #[test]
    fn auth_failures_strand_rather_than_dead_letter() {
        // A 403 is a fact about the credential, not the record: every record in
        // the backlog gets the same answer and none is malformed. Dead-lettering
        // would turn a recoverable operator error into displaced data. This
        // departs from clickhouse.rs:214 deliberately.
        for (s, code) in [
            (401u16, None),
            (403, Some("AccessDenied")),
            (403, Some("SignatureDoesNotMatch")),
            (403, Some("ExpiredToken")),
            (403, Some("InvalidAccessKeyId")),
            (404, Some("NoSuchBucket")),
        ] {
            assert_eq!(
                classify(s, code),
                Classification::Transient,
                "status {s} code {code:?}"
            );
        }
    }

    #[test]
    fn clock_skew_is_transient_despite_being_a_403() {
        // The one 403 that is not an authorisation decision. NTP may resync;
        // dead-lettering a backlog over drift is the wrong trade.
        assert_eq!(
            classify(403, Some("RequestTimeTooSkewed")),
            Classification::Transient
        );
    }

    #[test]
    fn an_unrecognised_status_strands_rather_than_dead_letters() {
        // Providers diverge (R2, B2, Ceph). A stranded segment is recoverable
        // by an operator; a dead-lettered one needs a requeue.
        assert_eq!(classify(418, None), Classification::Transient);
        assert_eq!(classify(451, Some("WhoKnows")), Classification::Transient);
    }

    #[test]
    fn a_code_quoted_inside_the_message_does_not_win() {
        // Some providers quote a different code in the human-readable message.
        // Taking the first <Code> anywhere would classify on the quoted one.
        let body = "<Error><Message>saw <Code>AccessDenied</Code> earlier</Message>\
                    <Code>InvalidStorageClass</Code></Error>";
        assert_eq!(s3_error_code(body), Some("InvalidStorageClass"));
    }

    #[test]
    fn the_s3_error_code_is_extracted_from_a_real_error_body() {
        let body = "<?xml version=\"1.0\"?><Error><Code>NoSuchBucket</Code>\
                    <Message>The specified bucket does not exist</Message></Error>";
        assert_eq!(s3_error_code(body), Some("NoSuchBucket"));
        assert_eq!(s3_error_code("not xml"), None);
        assert_eq!(s3_error_code("<Code>unterminated"), None);
    }

    fn endpoint(force_path_style: bool, base: &str) -> Endpoint {
        Endpoint {
            base: base.to_string(),
            bucket: "weir-test".to_string(),
            region: "us-east-1".to_string(),
            force_path_style,
        }
    }

    #[test]
    fn path_style_puts_the_bucket_in_the_path_and_keeps_the_port_in_host() {
        // MinIO needs path style, and the signed host must carry the port or
        // every request fails with SignatureDoesNotMatch.
        let (url, host, path) = endpoint(true, "http://127.0.0.1:19000").resolve("a/b.ndjson");
        assert_eq!(url, "http://127.0.0.1:19000/weir-test/a/b.ndjson");
        assert_eq!(host, "127.0.0.1:19000");
        assert_eq!(path, "/weir-test/a/b.ndjson");
    }

    #[test]
    fn virtual_host_style_puts_the_bucket_in_the_hostname() {
        let (url, host, path) =
            endpoint(false, "https://s3.us-east-1.amazonaws.com").resolve("a/b.ndjson");
        assert_eq!(
            url,
            "https://weir-test.s3.us-east-1.amazonaws.com/a/b.ndjson"
        );
        assert_eq!(host, "weir-test.s3.us-east-1.amazonaws.com");
        assert_eq!(path, "/a/b.ndjson");
    }

    #[test]
    fn the_url_is_encoded_while_the_signed_path_is_not() {
        // The signer applies uri_encode_path itself, so handing it an
        // already-encoded path would double-encode. The URL must be encoded and
        // the signed path must not -- that asymmetry is the whole contract.
        let (url, _, path) = endpoint(true, "http://h").resolve("dt=2026-09-06/x.ndjson");
        assert!(
            url.ends_with("/weir-test/dt%3D2026-09-06/x.ndjson"),
            "{url}"
        );
        assert_eq!(path, "/weir-test/dt=2026-09-06/x.ndjson");
    }
}
