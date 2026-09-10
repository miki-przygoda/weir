//! S3-API object-storage sink for the [weir] daemon.
//!
//! Writes each commit batch as one object, framed as NDJSON by default so
//! Athena, DuckDB, Spark and Glue can read the bucket with no weir-specific
//! tooling. Targets the S3 **API**, not AWS specifically: set
//! [`S3SinkConfig::endpoint`] and MinIO, Cloudflare R2, Backblaze B2 and Ceph
//! all work on the same path.
//!
//! # Replay-stable, collision-free object keys
//!
//! weir's drain is at-least-once: after a crash it re-commits a byte-identical
//! batch. This sink derives the object key from values that are invariant under
//! that replay — the owning WAB segment's creation time for the partition, and
//! the batch's first [`RecordId`](weir_sink_sdk::RecordId) plus its record count
//! for the filename — so a replayed batch overwrites its own object with
//! identical bytes instead of creating a duplicate.
//!
//! The filename is deliberately **not** the batch's
//! [`DedupToken`](weir_sink_sdk::DedupToken). A `DedupToken` is a pure content
//! hash, so two batches carrying identical bytes share one; since an S3 key is
//! last-write-wins, a token-named object is destroyed by the next batch of the
//! same repetitive records. `RecordId` mixes the record's WAB coordinate into
//! the digest, which is the uniqueness a content hash cannot carry. See the
//! `key` module for the full argument.
//!
//! **Precondition:** these guarantees hold only while `sink_max_batch_size` is
//! constant for the life of the bucket. The drain re-reads it on every call, so
//! changing it re-splits batches at different boundaries and produces different
//! keys.
//!
//! [weir]: https://github.com/miki-przygoda/weir
#![deny(missing_docs)]

pub(crate) mod client;
pub(crate) mod creds;
pub(crate) mod framing;
pub(crate) mod key;
pub(crate) mod redact;
pub(crate) mod sigv4;
pub(crate) mod time;

use std::sync::atomic::{AtomicBool, Ordering};

use weir_sink_sdk::{CommitResult, Sink, SinkBatch, SinkError, SinkHealth};

pub use framing::{Compression, Framing};

use client::{Classification, Endpoint, S3Client};
use creds::CredentialChain;
use key::PartitionTemplate;
use redact::SecretString;

/// A failure delivering a batch to S3.
#[derive(Debug)]
pub struct S3SinkError {
    message: String,
    transient: bool,
}

impl std::fmt::Display for S3SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for S3SinkError {}

impl SinkError for S3SinkError {
    fn is_transient(&self) -> bool {
        self.transient
    }
}

/// How to reach the bucket and what to write into it.
///
/// `Debug` redacts the secret access key: this struct is formatted into the
/// daemon's startup `INFO` log.
#[derive(Debug, Clone)]
pub struct S3SinkConfig {
    /// Scheme and authority, no trailing slash. Defaults to the AWS regional
    /// endpoint when unset; set it for MinIO, R2, B2 or Ceph.
    pub endpoint: Option<String>,
    /// Target bucket. Required.
    pub bucket: String,
    /// Signing region.
    pub region: String,
    /// Put the bucket in the path rather than the hostname. Required by MinIO
    /// and most self-hosted gateways.
    pub force_path_style: bool,
    /// Key prefix, no leading `/`.
    pub prefix: String,
    /// Partition template — `%Y`, `%m`, `%d`, `%H` only, rendered in UTC.
    pub partition: String,
    /// Record layout inside an object.
    pub framing: Framing,
    /// Object body compression.
    pub compression: Compression,
    /// Static access key id. Prefer the environment, IRSA, or an instance role.
    pub access_key_id: Option<String>,
    /// Static secret access key.
    pub secret_access_key: Option<String>,
    /// `x-amz-storage-class`, e.g. `STANDARD_IA`. Omitted when `None`.
    pub storage_class: Option<String>,
    /// `x-amz-server-side-encryption`, e.g. `AES256` or `aws:kms`.
    pub sse: Option<String>,
    /// `x-amz-server-side-encryption-aws-kms-key-id`, with `sse = "aws:kms"`.
    pub sse_kms_key_id: Option<String>,
    /// Records per `commit`, from `sink_max_batch_size`.
    pub max_batch_size: usize,
    /// Per-request timeout.
    pub timeout: std::time::Duration,
}

/// Writes weir batches to S3-compatible object storage.
#[derive(Debug)]
pub struct S3Sink {
    client: S3Client,
    prefix: String,
    partition: PartitionTemplate,
    framing: Framing,
    compression: Compression,
    max_batch_size: usize,
    /// Latches the one-time warning for a batch with no `RecordId`s.
    warned_missing_ids: AtomicBool,
}

impl S3Sink {
    /// Builds a sink.
    ///
    /// # Errors
    ///
    /// If the prefix or partition template contains path text a URL parser
    /// would rewrite — `.` or `..` segments, an interior `//`, `#`, `?`, a
    /// backslash, or a control character. Rejecting these at construction is
    /// deliberate: each would make weir sign one key and write another.
    pub fn new(config: S3SinkConfig) -> Result<Self, S3SinkError> {
        let invalid = |e: key::TemplateError, what: &str| S3SinkError {
            message: format!("sink_s3_{what}: {e}"),
            transient: false,
        };
        key::validate_path_text(&config.prefix).map_err(|e| invalid(e, "prefix"))?;
        // An endpoint is a scheme and an authority, never a path. `Endpoint::resolve`
        // treats everything after "://" as the authority, so "http://host/s3gw"
        // would produce the Host header "host/s3gw" and sign "/bucket/key" while
        // sending "/s3gw/bucket/key" -- a guaranteed 403 SignatureDoesNotMatch
        // that classifies as transient and strands forever, with nothing in the
        // message pointing at the endpoint. The prefix and partition are
        // validated for exactly this failure; the endpoint was the gap.
        if let Some(ep) = &config.endpoint {
            let authority = ep.split_once("://").map_or(ep.as_str(), |(_, a)| a);
            if authority.trim_end_matches('/').contains('/') {
                return Err(S3SinkError {
                    message: format!(
                        "sink_s3_endpoint must be a scheme and host only (optionally with a \
                         port), with no path component: got {ep:?}. A path here would be \
                         signed and sent differently, giving a permanent \
                         SignatureDoesNotMatch."
                    ),
                    transient: false,
                });
            }
            if authority.is_empty() {
                return Err(S3SinkError {
                    message: format!("sink_s3_endpoint has no host: {ep:?}"),
                    transient: false,
                });
            }
        }
        let partition =
            PartitionTemplate::parse(&config.partition).map_err(|e| invalid(e, "partition"))?;

        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            // Never follow redirects. reqwest strips `authorization` across
            // hosts but NOT `x-amz-*`, so a 307 or 301 (S3 answers both for a
            // wrong-region or renamed bucket) would forward the session token
            // AND the record body to another host, return 200, and have this
            // sink report the batch committed -- after which the drain deletes
            // the segment, with nothing in the configured bucket. A redirect
            // must surface as a status this sink classifies, not be chased.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| S3SinkError {
                message: format!("could not build the HTTP client: {e}"),
                transient: false,
            })?;

        let static_creds = match (&config.access_key_id, &config.secret_access_key) {
            (Some(id), Some(secret)) => Some((id.clone(), SecretString::new(secret.clone()))),
            _ => None,
        };
        let creds = CredentialChain::new(static_creds, &config.region, http.clone());

        let base = config
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", config.region));

        let mut put_headers = Vec::new();
        if let Some(sc) = &config.storage_class {
            put_headers.push(("x-amz-storage-class".to_string(), sc.clone()));
        }
        if let Some(sse) = &config.sse {
            put_headers.push(("x-amz-server-side-encryption".to_string(), sse.clone()));
        }
        if let Some(k) = &config.sse_kms_key_id {
            put_headers.push((
                "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
                k.clone(),
            ));
        }

        Ok(Self {
            client: S3Client::new(
                http,
                creds,
                Endpoint {
                    base,
                    bucket: config.bucket,
                    region: config.region,
                    force_path_style: config.force_path_style,
                },
                put_headers,
            ),
            prefix: config.prefix,
            partition,
            framing: config.framing,
            compression: config.compression,
            max_batch_size: config.max_batch_size,
            warned_missing_ids: AtomicBool::new(false),
        })
    }

    /// The object name for a batch.
    ///
    /// Falls back to the `DedupToken` only when the batch carries no
    /// `RecordId`s, which the drain never produces — it means a batch built by
    /// `SinkBatch::new` or `From<Vec<Payload>>`, i.e. a sink author's test. The
    /// fallback warns once rather than proceeding silently, because a
    /// token-derived name collides across distinct batches of identical
    /// records and an S3 key is last-write-wins.
    fn batch_name(&self, batch: &SinkBatch, created_at: i64) -> String {
        match batch.record_ids().and_then(<[_]>::first) {
            Some(first) => key::batch_name(created_at, &first.to_hex(), batch.len()),
            None => {
                if !self.warned_missing_ids.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "s3 sink: batch carries no RecordIds, falling back to the dedup token \
                         for the object name. Two batches of byte-identical records then share \
                         a key and the second overwrites the first. The drain always supplies \
                         RecordIds, so this indicates a hand-built batch."
                    );
                }
                key::batch_name(created_at, &batch.dedup_token().to_hex(), batch.len())
            }
        }
    }
}

impl Sink for S3Sink {
    type Error = S3SinkError;

    async fn commit(&self, batch: SinkBatch) -> Result<CommitResult, S3SinkError> {
        // 0 partitions under 1970-01-01. Reachable only from a hand-built batch
        // (the drain always supplies it), and warned about for the same reason
        // the missing-RecordId path is: silently mis-partitioning is worse than
        // a noisy one-time line.
        let created_at = match batch.segment_created_at() {
            Some(t) => t,
            None => {
                if !self.warned_missing_ids.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "s3 sink: batch carries no segment creation time; partitioning under \
                         1970-01-01. The drain always supplies one, so this indicates a \
                         hand-built batch."
                    );
                }
                0
            }
        };
        let name = self.batch_name(&batch, created_at);
        let object_key = key::object_key(
            &self.prefix,
            &self.partition,
            created_at,
            &name,
            &framing::extension(self.framing, self.compression),
        );

        let framed = framing::frame(batch.into_records(), self.framing, self.compression);

        // Every record was rejected by framing. Upload nothing: an empty object
        // is noise in the bucket, and for a compressed framing it would not even
        // be empty -- zstd and gzip of no input are non-empty frame headers.
        if framed.body.is_empty() {
            // The empty check is on the BODY, not the record count: zstd and
            // gzip of no input are non-empty frame headers, so an empty-count
            // check would upload a valid but meaningless object. The converse
            // invariant -- empty body implies nothing committed -- holds for
            // both framings today and is asserted so a future framing that
            // breaks it fails here rather than silently dropping records from
            // the CommitResult.
            debug_assert!(
                framed.committed.is_empty(),
                "an empty body must mean nothing was committed"
            );
            return Ok(CommitResult::new(Vec::new(), framed.dead_lettered));
        }

        match self
            .client
            .put_object(
                &object_key,
                framed.body,
                framing::content_type(self.framing),
            )
            .await
        {
            Ok(()) => Ok(CommitResult::new(framed.committed, framed.dead_lettered)),
            Err(e) => Err(S3SinkError {
                message: e.message,
                transient: e.classification == Classification::Transient,
            }),
        }
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    async fn health(&self) -> SinkHealth {
        match self.client.head_bucket().await {
            Ok(s) if (200..300).contains(&s) => SinkHealth::Healthy,
            // A least-privilege IAM policy commonly grants s3:PutObject without
            // s3:ListBucket, so HeadBucket 403s while writes succeed perfectly.
            // Reporting Down would be wrong -- and before the drain fix in
            // ae392bc, Degraded was a STICKIER failure than Down, which would
            // have stranded such a deployment's backlog for the life of the
            // process.
            // AWS documents HeadBucket as answering "a generic 400 Bad Request,
            // 403 Forbidden, or 404 Not Found" when the bucket is missing OR the
            // caller lacks permission, with no body to distinguish them -- and
            // HeadBucket requires s3:ListBucket, which is exactly what a
            // least-privilege PutObject-only policy omits. Mapping only 403 to
            // Degraded left the same deployment reporting Down on 400 or 404, and
            // the drain never rescans a Down sink, so its stranded backlog would
            // sit until a restart. All three are therefore Degraded: PutObject is
            // the authority on whether delivery works.
            Ok(s @ (400 | 403 | 404)) => SinkHealth::Degraded(format!(
                "HeadBucket returned HTTP {s}, which AWS uses for both a missing bucket and \
                 a denied one. Delivery may still work: HeadBucket needs s3:ListBucket, which \
                 a least-privilege PutObject-only policy does not grant. Commit success is \
                 the real signal."
            )),
            Ok(s) => SinkHealth::Down(format!("HeadBucket returned HTTP {s}")),
            Err(e) => SinkHealth::Down(e.message),
        }
    }
}

#[doc(hidden)]
/// Internal seams exposed for the vendored SigV4 vector suite
/// (`tests/sigv4_vectors.rs`), which is a separate crate and cannot reach
/// `pub(crate)` items. Not a public API: no stability guarantee, and it may
/// change in a patch release.
pub mod sigv4_test_hooks {
    pub use crate::sigv4::{
        Signed, SigningParams, canonical_request, sign, signature, string_to_sign, uri_encode_path,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use weir_core::Payload;

    fn cfg() -> S3SinkConfig {
        S3SinkConfig {
            endpoint: Some("http://127.0.0.1:19000".to_string()),
            bucket: "weir-test".to_string(),
            region: "us-east-1".to_string(),
            force_path_style: true,
            prefix: "archive".to_string(),
            partition: "dt=%Y-%m-%d/hour=%H".to_string(),
            framing: Framing::Ndjson,
            compression: Compression::Zstd,
            access_key_id: Some("id".to_string()),
            secret_access_key: Some("secret".to_string()),
            storage_class: None,
            sse: None,
            sse_kms_key_id: None,
            max_batch_size: 100,
            timeout: std::time::Duration::from_secs(30),
        }
    }

    #[test]
    fn a_valid_config_builds() {
        assert!(S3Sink::new(cfg()).is_ok());
    }

    #[test]
    fn a_prefix_that_would_be_rewritten_by_a_url_parser_is_rejected_at_construction() {
        // Not at first commit: by then records are already acked and waiting.
        for bad in ["a/../b", "a/./b", "a//b", "arch#ive", "dt=?x"] {
            let mut c = cfg();
            c.prefix = bad.to_string();
            let e = S3Sink::new(c).expect_err(&format!("{bad:?} must be rejected"));
            assert!(!e.is_transient(), "a config error is permanent");
            assert!(e.to_string().contains("sink_s3_prefix"), "{e}");
        }
    }

    #[test]
    fn an_endpoint_with_a_path_component_is_rejected() {
        // Endpoint::resolve treats everything after "://" as the authority, so a
        // path here signs "/bucket/key" while sending "/gw/bucket/key" -- a
        // permanent SignatureDoesNotMatch that classifies as transient and
        // strands forever, with nothing naming the endpoint.
        for bad in [
            "http://127.0.0.1:19000/s3gw",
            "https://gw.example/prefix/",
            "https://",
        ] {
            let mut c = cfg();
            c.endpoint = Some(bad.to_string());
            let e = S3Sink::new(c).expect_err(&format!("{bad:?} must be rejected"));
            assert!(!e.is_transient());
            assert!(e.to_string().contains("sink_s3_endpoint"), "{e}");
        }
        // A bare host, and a host with a port, are both fine.
        for good in [
            "http://127.0.0.1:19000",
            "https://s3.example.com",
            "https://h/",
        ] {
            let mut c = cfg();
            c.endpoint = Some(good.to_string());
            assert!(S3Sink::new(c).is_ok(), "{good:?} must be accepted");
        }
    }

    #[test]
    fn an_invalid_partition_template_is_rejected_at_construction() {
        let mut c = cfg();
        c.partition = "dt=%Y/%s".to_string();
        let e = S3Sink::new(c).expect_err("unknown specifier must be rejected");
        assert!(e.to_string().contains("sink_s3_partition"), "{e}");
    }

    #[test]
    fn the_secret_never_appears_in_the_config_debug_output() {
        // S3SinkConfig is Debug-formatted into the startup INFO log.
        let c = S3SinkConfig {
            secret_access_key: Some("wJalrXUtnFEMI".to_string()),
            ..cfg()
        };
        let sink = S3Sink::new(c).expect("builds");
        let rendered = format!("{sink:?}");
        assert!(!rendered.contains("wJalrXUtnFEMI"), "leaked: {rendered}");
    }

    #[test]
    fn max_batch_size_is_reported_from_config() {
        // The drain reads this on every call, which is why the value must be
        // constant for the life of the bucket -- it decides batch boundaries,
        // and those decide object keys.
        let sink = S3Sink::new(cfg()).expect("builds");
        assert_eq!(sink.max_batch_size(), 100);
    }

    #[test]
    fn the_batch_name_uses_the_record_id_not_the_dedup_token() {
        use weir_sink_sdk::{DedupToken, RecordId};
        let sink = S3Sink::new(cfg()).expect("builds");
        let p = Payload::from(&b"heartbeat"[..]);
        let ids = vec![RecordId::for_record("shard_00/seg_00000001.wab", 1, &p)];
        let token = DedupToken::for_payloads(std::slice::from_ref(&p));
        let batch = SinkBatch::with_segment_context(vec![p], token, ids.clone(), 0);

        let name = sink.batch_name(&batch, 0);
        assert!(name.contains(&ids[0].to_hex()), "{name}");
        assert!(
            !name.contains(&token.to_hex()),
            "the dedup token collides across distinct batches of identical records"
        );
        assert!(
            name.ends_with("-1"),
            "the record count is part of the name: {name}"
        );
    }

    #[test]
    fn the_name_carries_the_pre_framing_count_not_the_object_line_count() {
        // The name is chosen at lib.rs:281, before framing runs at :290, so a
        // record NDJSON cannot represent is dead-lettered *after* the count is
        // baked in. The object then holds fewer lines than its key claims.
        //
        // This is the right trade -- the alternative is naming the object after
        // framing, which would make the key depend on record content and break
        // the replay-stability the whole scheme rests on -- but it is a trap for
        // anyone validating an archive by parsing counts out of key names, so
        // docs/sinks/s3.md says so and this pins it.
        use weir_sink_sdk::{DedupToken, RecordId};
        let sink = S3Sink::new(cfg()).expect("builds");
        let good = Payload::from(&b"clean"[..]);
        let bad = Payload::from(&b"has\nnewline"[..]);
        let records = vec![good.clone(), bad.clone()];
        let token = DedupToken::for_payloads(&records);
        let ids = vec![
            RecordId::for_record("shard_00/seg_00000001.wab", 0, &good),
            RecordId::for_record("shard_00/seg_00000001.wab", 1, &bad),
        ];
        let batch = SinkBatch::with_segment_context(records.clone(), token, ids, 0);

        let name = sink.batch_name(&batch, 0);
        assert!(
            name.ends_with("-2"),
            "the name must carry the count weir was handed (2): {name}"
        );

        let framed = crate::framing::frame(records, Framing::Ndjson, Compression::None);
        assert_eq!(
            framed.dead_lettered.len(),
            1,
            "the newline record is rejected"
        );
        assert_eq!(
            framed.body.iter().filter(|b| **b == b'\n').count(),
            1,
            "the object holds one line, while its key says two"
        );
    }

    #[test]
    fn the_name_does_not_cover_records_after_the_first() {
        // The boundary of the append-only property, asserted so it cannot be
        // quietly oversold. `batch_name` uses `.first()`, so altering any record
        // but the first -- while keeping the first and the count -- reproduces
        // the original key exactly. An S3 key is last-write-wins, so anyone
        // holding s3:PutObject on the prefix can replace the object's contents
        // without the name changing.
        //
        // weir itself never does this: its records come from an append-only WAB
        // and a replay re-derives the same bytes. The property is "weir never
        // rewrites an object with different records", NOT "an altered batch
        // cannot occupy the original key". docs/sinks/s3.md states both halves;
        // three separate readers built designs on the stronger, false one.
        use weir_sink_sdk::{DedupToken, RecordId};
        let sink = S3Sink::new(cfg()).expect("builds");
        let first = Payload::from(&b"record-one"[..]);
        let id = RecordId::for_record("shard_00/seg_00000001.wab", 0, &first);

        // Both batches carry a *correct* second RecordId covering their own
        // second record -- the ids differ between the two calls. The name is
        // identical anyway, because only the first one is read.
        let name_of = |second: &[u8]| {
            let altered = Payload::from(second);
            let ids = vec![
                id,
                RecordId::for_record("shard_00/seg_00000001.wab", 1, &altered),
            ];
            let records = vec![first.clone(), altered];
            let token = DedupToken::for_payloads(&records);
            sink.batch_name(&SinkBatch::with_segment_context(records, token, ids, 0), 0)
        };

        assert_eq!(
            name_of(b"original"),
            name_of(b"TAMPERED"),
            "the key covers only the first RecordId and the count, so changing a \
             later record leaves the name identical. If this assertion ever \
             fails the property got STRONGER and docs/sinks/s3.md should be \
             updated to claim it -- but do not claim it while this passes."
        );
    }

    #[test]
    fn two_batches_of_identical_records_get_different_names() {
        // The failure this design exists to prevent, asserted at the sink level
        // rather than only in the key module.
        use weir_sink_sdk::{DedupToken, RecordId};
        let sink = S3Sink::new(cfg()).expect("builds");
        let p = Payload::from(&b"heartbeat"[..]);
        let token = DedupToken::for_payloads(std::slice::from_ref(&p));
        let mk = |index: u64| {
            SinkBatch::with_segment_context(
                vec![p.clone()],
                token,
                vec![RecordId::for_record("shard_00/seg_00000001.wab", index, &p)],
                0,
            )
        };
        assert_ne!(
            sink.batch_name(&mk(1), 0),
            sink.batch_name(&mk(2), 0),
            "byte-identical records at different WAB coordinates must not share an object"
        );
    }
}
