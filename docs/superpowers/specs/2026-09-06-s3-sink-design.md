# S3 Sink (`weir-sink-s3`) — Design Spec

- **Status:** Approved (design), pre-implementation
- **Date:** 2026-09-06
- **Target version:** 2.1.0 (additive `weir-sink-sdk` change → minor bump)
- **Author:** Mikolaj (with Claude Code)
- **Base:** `main` — *after* `fix/sweep-truth-up` lands (7 commits, currently unpushed)

## 1. Goal

Add an **S3 sink** as weir's sixth drain target and its **first out-of-tree
sink** — a separately published crate, `weir-sink-s3`, that implements
`weir-sink-sdk` without depending on the daemon.

It targets the **S3 API, not AWS specifically**: a configurable endpoint means
MinIO, Cloudflare R2, Backblaze B2 and Ceph work with the same code path, and
CI can prove the sink end-to-end against MinIO in docker-compose with no cloud
account.

Two things make this the right next sink rather than just another one:

1. **The semantics fit.** Sealed WAB segments are already immutable and
   content-addressable. That is the shape of an object-store write, and it
   yields the strongest idempotency story of any weir sink — see §3.
2. **It proves the SDK.** `weir-sink-sdk` was extracted so third parties could
   build sinks without the daemon's dependency tree. Nothing has ever tested
   that claim. This crate is that test, and it is written under the same
   constraint a third party would face.

## 2. Decisions (locked in brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Scope | **S3 only**, done to the standard the SQL sinks are held to | Kinesis/SQS become cheap follow-ups once the credential + signing layer exists. |
| Target | **S3 API, any provider** (configurable endpoint) | Far wider reach than AWS-only, and it makes CI free — MinIO in docker-compose, no AWS account. |
| S3 client | **Hand-rolled SigV4** over the existing `reqwest` + `sha2` + `hmac` stack | +1 crate vs +68 (official SDK) or +39 (`rust-s3`). Keeps the ring-only unification. Full control of transient-vs-permanent classification. See §2.1. |
| Placement | **New published crate** `weir-sink-s3` + optional `s3-sink` feature in `weir-server` | Usable via `--sink-type s3` *and* proves the SDK is externally implementable. Keeps ~1,100 LOC of signing out of the 33k-LOC daemon. |
| Object body | **NDJSON default**, framing configurable | Athena / DuckDB / Spark / Glue read the bucket directly, zero weir-specific tooling. Length-prefixed mode for binary payloads. |
| Object key | **WAB-coordinate filename under a segment-derived time partition** | Replay-stable, partition-prunable, *and* collision-free. See §3 — the load-bearing decision, and the one an early draft got wrong. |
| Feature default | **Opt-in** (not in `default`) | Consistent with `clickhouse-sink`. Promote later if it earns it. |

### 2.1 Why not the official AWS SDK — measured, not assumed

Against `weir-server`'s actual dependency tree (210 crates, all features —
counting rule at the end of this section):

| Approach | Net **new** crates | `aws-lc-sys` C build |
|---|---|---|
| `aws-sdk-s3` + `aws-config`, default features | +68 (after remediation) | **yes**, plus *two* rustls versions (0.23/aws-lc-rs and 0.21/ring) |
| `aws-sdk-s3`, hand-assembled ring-only | +68 | no |
| `rust-s3` | +39 | no |
| **Hand-rolled SigV4** | **+1** (`hmac`) | no |

The workspace made a deliberate, documented choice to unify on `ring` and drop
the `aws-lc-sys` cmake/C build (`weir-server/Cargo.toml`, the `rustls`
dependency comment). The AWS SDK fights that choice: neither `aws-config` nor
`aws-sdk-s3` exposes a `rustls-ring` feature. They offer `rustls-aws-lc` (the
default, via `default-https-client`) or `tls-rustls`, which routes to
`legacy-rustls-ring` and pins **rustls 0.21**. A clean ring-only tree is
reachable only by adding `aws-smithy-http-client` as a *direct* dependency with
`rustls-ring` and constructing the HTTP client by hand — an assembly weir would
then own and have to CI-guard against regression forever.

`sha2`, `base64`, `percent-encoding`, `reqwest` and `rustls`-on-`ring` are all
already in the tree. **`hmac` is the one genuinely new crate.** The tree does
carry `hmac 0.13.0` (via `postgres-protocol`, so only when `postgres-sink` is
enabled), but this crate pins `hmac 0.12` to pair with the `sha2 0.10` that
`weir-sink-sdk` uses — a different node. Do not describe it as "already there".

Two more dependencies were checked and cost nothing:

- **`percent-encoding 2.3.2`** is already in the tree via `reqwest`, so taking
  it as a direct dependency adds **zero** crates. §5 needs it for `UriEncode`.
- **`flate2 1.1.9`** is already in the tree via `mysql_async` (the `mysql-sink`
  feature, which is on by default), so `gzip` survives the first cut at no cost.
  It is *not* present under `--no-default-features --features s3-sink`; the
  manifest therefore declares it explicitly rather than relying on that path.

Counting rule for the table above: **unique `name + version` pairs on normal
(non-dev, non-build) edges**. By that rule `weir-server --all-features` is
**210** crates. Deduplicating by bare name instead gives 202 — the two figures
differ because the tree legitimately carries two `sha2` versions.

## 3. The object key — the load-bearing decision

### 3.1 Wall-clock partitioning is silently wrong

The obvious key scheme is Hive-partitioned by wall-clock so query engines can
partition-prune:

```
{prefix}/dt=2026-09-06/hour=14/{token}.ndjson.zst
```

**This breaks weir's core guarantee.** The drain is at-least-once: after a
crash-restart it re-commits a *byte-identical* batch. Wall-clock has moved, so
the replayed batch lands under a **different partition path** with the same
filename. The bucket now holds two objects with identical content, both visible
to every query.

weir acked those records once. The bucket reports them twice. That is not a
false ack under the project's definition, but it is the same outcome for the
user — and it happens precisely at the moment weir is supposed to be at its
most trustworthy.

### 3.2 Pure content-addressing is insufficient

```
{prefix}/{token[0:2]}/{token[2:4]}/{token}.ndjson.zst
```

Replay-stable, but forfeits partition pruning entirely: every Athena query
scans the whole bucket. For an archive sink that is the dominant read pattern,
this is a real cost, not a theoretical one.

### 3.3 The partition comes from the segment header

The WAB segment header stores **`created_at` — unix nanoseconds, header bytes
`[8..16]` LE** (`crates/weir-wab/src/format.rs:343`). It is written once when
the segment is created, lives on disk, and is therefore **identical across a
replay**.

`SegmentReader::header()` is already public
(`crates/weir-wab/src/lib.rs:181`) and the drain already holds an open reader at
`crates/weir-server/src/drain/mod.rs:1143` — though in `process_segment`, a
different function from the batch construction site at `:1374`, so the value
travels as a new parameter rather than a field access.

That settles the **partition path**. It does not settle the filename.

### 3.3a The filename must NOT be the `DedupToken` — it would destroy data

An earlier draft of this spec named the object after the batch's `DedupToken`.
**That is wrong, and its failure is worse than the duplication §3.1 avoids.**

`DedupToken` is a pure content hash (`weir-sink-sdk/src/lib.rs:349-357`; the
drain builds it at `drain/mod.rs:1367`). The SDK's own documentation on
`RecordId` — a type that exists *because of* this — states the consequence at
`lib.rs:405-414`:

> It is therefore a pure function of the payload bytes, and two batches carrying
> identical bytes are — correctly — the same token. […] a producer emitting
> repetitive records (a heartbeat, a status ping, any fixed-shape event) hands
> the downstream the same key for genuinely distinct events […] weir acked those
> records and wrote them to disk; they are then dropped downstream by weir's own
> header.

An S3 bucket is the most literal "correctly-implemented idempotent endpoint"
there is: same key means last-write-wins. And the partition contributes **no**
uniqueness — at `hour=%H` granularity every object in an hour shares one path.

**The failure needs no crash.** A heartbeat producer at the default
`sink_max_batch_size = 100` emits batches of 100 byte-identical payloads. Every
batch hashes to the same token, lands on the same key, and **overwrites the
previous one**. weir acks 360,000 records in an hour; the bucket holds 100.
That is silent loss of acked records — the failure the daemon exists to prevent.

### 3.3b The filename is the batch's first `RecordId` plus its length

`RecordId` mixes the record's WAB coordinate — segment name plus index — into
the digest (`weir-sink-sdk/src/lib.rs:428-437`), which is exactly the uniqueness
`DedupToken` cannot carry. It is **already on `SinkBatch`**, populated by the
drain on every commit (`drain/mod.rs:1374`) and exposed at `lib.rs:547`.

It is also replay-stable, for a reason worth pinning: `read_index += 1` at
`drain/mod.rs:1199` runs **before** the `if read_index <= skip { continue; }`
guard at `:1209`, so the index is the record's absolute ordinal within the
segment regardless of resume state.

So the key is:

```
{prefix}/dt=2026-09-06/hour=14/{created_at:016x}-{first_record_id_hex}-{count}.ndjson.zst
         └── segment created_at ──┘└ same, full ns ┘└─ WAB coordinate ─┘ └ batch size ┘
```

**The full-nanosecond `created_at` in the *name* was added during
implementation**, after the MinIO integration test caught a collision the unit
tests could not. A `RecordId` is `sha256(segment_name ++ index ++ payload)`, and
segment names restart at `seg_00000001` in every fresh WAB directory — so two
daemon runs on *different* WAB dirs produce identical `RecordId`s for their
first batches. A pod redeployed without a PVC, a wiped WAB, or a fresh container
writing to the same bucket and prefix within one partition hour would silently
overwrite the previous deployment's objects. That is the ephemeral-volume case
`phase4-k8s-deployment.md` warned about, arriving through the key scheme rather
than through fsync. `created_at` distinguishes segment *instances* where the
name cannot, and being on disk it stays replay-invariant.

The record count is in the name because the start coordinate alone still
collides when `sink_max_batch_size` changes: a resized batch beginning at the
same index would overwrite an object holding different content. With the count
present, a resize yields a distinct key and degrades to plain duplication —
which the at-least-once contract already absorbs — instead of an overwrite.

Every component is replay-invariant, so a crash-replayed batch writes the same
key with byte-identical bytes: an idempotent overwrite. And because the
coordinate is unique per batch, **no two distinct batches can collide**. No
dedup support is required from the downstream at all, which is true of no other
weir sink.

**Fallback.** `record_ids()` is `None` for a batch built by `SinkBatch::new` or
`From<Vec<Payload>>` — never from the drain, only from a sink author's test. The
sink then logs a one-time `WARN` and falls back to the `DedupToken`, naming the
collision risk. It does not silently proceed.

**Requeue.** `weir-ctl dl requeue` and `quarantine requeue` re-push through the
daemon socket into a *new* segment, so the segment name changes and the key
changes. Under this scheme that yields a clean duplicate object, which
at-least-once absorbs. Under the rejected `DedupToken` scheme it was a coin-flip
between duplicating and overwriting, depending on the hour.

### 3.4 The `weir-sink-sdk` change

`SinkBatch` gains a private field and an accessor:

```rust
pub struct SinkBatch {
    records: Vec<Payload>,
    dedup_token: DedupToken,
    record_ids: Option<Vec<RecordId>>,
    /// Owning segment's creation time (unix nanoseconds), from the WAB segment
    /// header. `None` from `SinkBatch::new`, so a batch built by a 1.x-era
    /// caller or a sink author's test is unchanged.
    segment_created_at: Option<i64>,
}

impl SinkBatch {
    /// Creation time of the WAB segment this batch was read from, unix
    /// nanoseconds. Stable across a crash-replay — unlike wall-clock — so a
    /// sink may safely derive a partition path or object key from it.
    pub fn segment_created_at(&self) -> Option<i64> { … }
}
```

This follows the pattern `record_ids` already established exactly: private
field, `None` from `new()`, populated by the drain through a dedicated
constructor. Fields are private, so this is **additive and non-breaking** —
a minor bump.

Two consequences worth writing down:

- **There are three struct-literal sites, not two.** `SinkBatch::new`
  (`lib.rs:509`), `with_record_ids` (`:536`) and
  `impl From<Vec<Payload>> for SinkBatch` (`:597`). Missing the third is a
  compile error.
- **`SinkBatch` derives `PartialEq, Eq`.** Adding a field is compile-compatible
  but changes equality: a batch from `new()` no longer compares equal to one
  from `with_segment_context()` carrying the same records. Call it out in the
  changelog for sink authors who assert on batch equality in their tests.

The drain switches to a superseding constructor at
`crates/weir-server/src/drain/mod.rs:1374`:

```rust
pub fn with_segment_context(
    records: Vec<Payload>,
    dedup_token: DedupToken,
    record_ids: Vec<RecordId>,
    segment_created_at: i64,
) -> Self
```

fed by `reader.header().created_at` from the reader already open at
`drain/mod.rs:1143`. It keeps `with_record_ids`' length-mismatch assertion.

`with_record_ids` stays and is **not** `#[deprecated]` — it remains the right
constructor for a sink author's tests, and firing a deprecation warning in
downstream crates over an internal plumbing change would be hostile. Its doc
comment gains a pointer to the new constructor.

**Rejected alternative:** having the S3 sink read the segment header itself.
The sink receives a `SinkBatch`, not a path — and giving sinks filesystem
access to the WAB would be a far larger contract change than adding a field.

### 3.5 `sink_max_batch_size` must be constant — not merely stable

The drain sizes each sub-batch by `sink_max_batch_size`, and re-reads it on
**every** call (`drain/mod.rs:1166`) rather than capturing it per segment. So
the requirement is stronger than the "stable across a restart" that `DedupToken`
documents: the value must be **constant for the life of the bucket**. A
third-party sink returning a varying `max_batch_size()` would shift batch
boundaries mid-run, not just across a restart.

Under §3.3b the consequence is bounded — a changed size yields a distinct key,
so the outcome is duplicate objects rather than an overwrite, and at-least-once
already absorbs duplicates. It still deserves a blunt warning in the
configuration reference and in the daemon's startup log, because the duplicates
are silent and permanent.

## 4. Crate layout and module boundaries

New workspace member `crates/weir-sink-s3`, published.

Dependencies: `weir-sink-sdk`, `weir-core`, `reqwest` (rustls-tls, no default
features), `sha2`, `zstd`, `tokio`, `thiserror`, `tracing`, `percent-encoding`
and `flate2` — all already in the workspace tree — plus **`hmac`**, the one
genuinely new crate (§2.1).

| Module | Responsibility | I/O |
|---|---|---|
| `sigv4.rs` | Canonical request → string-to-sign → `Authorization` header | **none** |
| `creds.rs` | Credential chain + expiry/refresh | HTTP |
| `framing.rs` | `SinkBatch` → `(body, dead_lettered)`; NDJSON / length-prefixed; optional zstd | **none** |
| `key.rs` | Object key from prefix + partition template + `created_at` + token | **none** |
| `client.rs` | `PutObject` / `HeadBucket`; status → transient/permanent | HTTP |
| `lib.rs` | `S3Sink: Sink`, `S3SinkConfig`, `S3SinkError` | — |

Four of six modules are pure functions with no network and no clock. That split
is what makes owning SigV4 tractable: the part that is hard to get right is the
part that is trivial to test exhaustively.

## 5. SigV4 — what we own, and how it is proven

`sigv4.rs` implements AWS Signature Version 4 for `s3` requests:
canonical request → SHA-256 → string-to-sign → HMAC key derivation chain
(`date → region → service → aws4_request`) → `Authorization` header.

**Proof strategy:** known-answer tests against **AWS's published
`aws-sig-v4-test-suite`** vectors, checked into the repo. Each vector supplies a
raw request and the expected canonical request, string-to-sign, and
authorization header — so a regression fails at the earliest divergent stage
rather than as an opaque `403 SignatureDoesNotMatch`.

This is the same pattern weir already uses for its own wire format
(`docs/conformance.md`): canonical vectors, checked in, cannot drift from the
implementation because both are asserted against the same fixtures.

### 5.1 `sigv4.rs` owns URI encoding — it is not the caller's job

AWS's `UriEncode` is **not** waived for S3. S3 waives *double*-encoding and
*normalization*; it still requires every byte outside `A-Za-z0-9-._~` to be
percent-encoded with **uppercase** hex, `/` excepted inside an object key.

An early draft delegated this to the caller ("`uri_path` is already
percent-encoded") on the grounds of the dependency budget. That was wrong twice
over: `percent-encoding 2.3.2` is already in the tree via `reqwest`, so it costs
nothing (§2.1), and the delegation created a live hazard. `sink_s3_prefix` is
free text that nothing validates, so a space or non-ASCII byte there means the
signer signs `/has space/` while `reqwest` sends `/has%20space/` — a permanent
`403 SignatureDoesNotMatch` with no diagnosis path. A `#` or `?` is worse: the
path is silently truncated, and if the caller signs the URL it parsed, both
sides agree and the batch is written **to the wrong key with a valid
signature**.

`sigv4.rs` therefore exports `uri_encode_path`, and the client builds both the
canonical URI and the request URL from that one string. Hive keys are safe
either way — `=` and `/` survive unencoded — but the sink must not depend on
that.

### 5.2 Header canonicalisation

Beyond lowercasing, sorting and trimming, AWS requires two steps an early draft
omitted, both of which the vendored suite exercises:

- **Collapse runs of ASCII whitespace to a single space**, including inside
  quoted values (`get-header-value-trim` expects `my-header2:"a b c"`).
- **Merge duplicate header names** into one comma-joined line in original
  request order, appearing once in `SignedHeaders`
  (`get-header-key-duplicate`).

Use explicit ASCII whitespace handling, not `str::trim()` — the latter strips
Unicode whitespace such as U+00A0, which AWS's `Trim()` would not.

### 5.3 The signed header set is an input, not an output

S3 **requires** `x-amz-content-sha256` to be signed, alongside `host`,
`x-amz-date`, and `x-amz-security-token` when a session token is present. An
early draft had `sign()` *return* the date and payload hash, which is circular:
the caller cannot assemble the headers it must sign without the helpers that
produce them.

`sign()` therefore owns the required set. `SigningParams` takes `host` (with
the port, for the MinIO test's `127.0.0.1:19000`) plus any caller extras, and
`Signed` returns the **exact list of headers to attach**. Signing a set that
differs from what is sent becomes unrepresentable.

This matters beyond ergonomics: a malformed signature returns `403`, and §9
must not dead-letter on that — see §9.

### 5.4 Scope limits, deliberate

- **SigV4 only.** No SigV4a (multi-region access points) — out of scope, §16.
- **Header-based signing only.** No presigned URLs; the sink only PUTs.
- **`UNSIGNED-PAYLOAD` is not used.** The body hash is computed — the batch is
  already in memory, and providers differ in what they accept.
- **No path normalization.** Correct for S3: keys may legitimately contain `.`,
  `..` and `//` segments.

### 5.5 The vector suite needs an explicit exclusion list

The suite cannot be applied wholesale, and a plan that gates on "all of it"
gates on something impossible. Roughly 16 of the 38 cases in the maintained
mirror (`awslabs/aws-c-auth/tests/aws-signing-test-suite/v4`) exercise path
handling; the `*-normalized` variants require RFC 3986 normalization that an S3
signer **must not** do. Those are excluded by name, with that rule cited as the
reason.

Note also that the mirror's fixture filenames are `request.txt`,
`header-canonical-request.txt`, `header-string-to-sign.txt`,
`header-signature.txt` — not the `.req`/`.creq`/`.sts`/`.authz` of the retired
zip on the IAM docs site. Confirm the layout of whatever is actually fetched
before writing the case collector.

## 6. Credentials

`creds.rs` resolves in this order, first match wins:

1. **Static config** — `sink_s3_access_key_id` / `sink_s3_secret_access_key`.
2. **Environment** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`.
3. **Web identity (IRSA / GKE / EKS)** — `AWS_WEB_IDENTITY_TOKEN_FILE` +
   `AWS_ROLE_ARN` → `sts:AssumeRoleWithWebIdentity`.
4. **IMDSv2** — token `PUT` then credential `GET`. IMDSv1 is **not**
   implemented (it is the SSRF-prone one).

Returns `Credentials { access_key_id, secret_access_key, session_token,
expires_at }`. Temporary credentials (3, 4) are refreshed when within 5 minutes
of expiry. A refresh failure is **transient** — it must strand the segment, not
dead-letter it.

**Not implemented:** `~/.aws/credentials` profile parsing and SSO. Both are
developer-workstation conveniences; a daemon runs with env vars, IRSA, or an
instance role. Revisit only on real demand.

Static secrets are stored in a wrapper whose `Debug` impl redacts.

**The daemon's redaction helpers are not reachable.** `redact_url_password`
(`crates/weir-server/src/sink/mod.rs:50`) and `sanitize_log_excerpt` (`:135`,
additionally gated `#[cfg(any(feature = "http-sink", feature = "clickhouse-sink"))]`)
are both `pub(crate)` — deliberately, since they are internal to the daemon. `weir-sink-s3` therefore carries its own copies, each with a
comment naming the original and the finding it came from (S31 for URL
redaction, S29 for log-forging). This is the first concrete cost of the
out-of-tree placement, and it is worth paying: promoting them to a public
surface would commit weir to maintaining a log-hygiene API it never intended to
publish. If a third sink crate ever needs them, extracting a small
`weir-sink-util` crate is the answer — not widening `weir-sink-sdk`.

## 7. Object body — framing and compression

`framing.rs` turns a `SinkBatch` into a body plus a dead-letter list.

**`Ndjson` (default)** — records joined by `\n`, one per line. A record
containing `\n` or `\r` cannot be framed this way and is **dead-lettered** with
reason `"record contains a newline; NDJSON framing cannot represent it"` —
exactly the behaviour `sink/http.rs` already has in its Ndjson mode. Content
type `application/x-ndjson`.

**`LengthPrefixed`** — `u64` LE length + bytes per record, binary-safe, no
dead-lettering on content. Content type `application/octet-stream`. Requires a
weir-aware reader; `weir-wab`'s `SegmentReader` is *not* it (different framing),
so the format is documented in the crate docs.

**Extensions** are `.ndjson` and `.weirbin` respectively, then the compression
suffix — so a key ends `.ndjson.zst` or `.weirbin.gz`. `.weirbin` is
deliberately not `.bin`: the framing is weir-specific and the name should say
so rather than imply a format a reader might guess wrong.

**Empty payloads** are legal records and are preserved: an empty record is an
empty NDJSON line, or a `0` length prefix. An empty *batch* never reaches
`commit` (the drain does not emit one), and if one did it is a no-op success —
no object is written, since an empty object would pollute the bucket with
nothing to read.

**Compression** — `none` | `zstd` | `gzip`, appended to the key as `.zst` /
`.gz`. Default **`zstd`**: already in the tree for WAB format v2, and both
Athena and Spark read it. `gzip` is offered for engines that don't.

Compression happens *after* framing, over the whole body — so the object is a
compressed NDJSON file, which is what query engines expect.

## 8. Config surface

Following the established `sink_<type>_<key>` convention
(`config/mod.rs:213-238`). All available as `WEIR_*` env vars and TOML keys.

| Key | Default | Notes |
|---|---|---|
| `sink_s3_bucket` | — | **required** |
| `sink_s3_region` | `us-east-1` | signing region |
| `sink_s3_endpoint` | *(AWS)* | set for MinIO / R2 / B2 / Ceph |
| `sink_s3_force_path_style` | `false` | `true` for MinIO and most self-hosted |
| `sink_s3_prefix` | `""` | key prefix, no leading `/` |
| `sink_s3_partition` | `dt=%Y-%m-%d/hour=%H` | see below; `""` disables partitioning |
| `sink_s3_framing` | `ndjson` | `ndjson` \| `length-prefixed` |
| `sink_s3_compression` | `zstd` | `none` \| `zstd` \| `gzip` |
| `sink_s3_access_key_id` | — | prefer env/IRSA/IMDS |
| `sink_s3_secret_access_key` | — | redacted in all output |
| `sink_s3_storage_class` | — | e.g. `STANDARD_IA`; omitted if unset |
| `sink_s3_sse` | — | `AES256` or `aws:kms` |
| `sink_s3_sse_kms_key_id` | — | with `sse = "aws:kms"` |

`sink_url` is **not** reused — an S3 target is a bucket plus region plus
optional endpoint, and cramming that into one URL makes the MinIO and
path-style cases ambiguous. `sink_timeout_secs`, `sink_max_batch_size`,
`sink_max_retries` and `sink_retry_base_delay_ms` apply unchanged.

**Partition template** is a deliberately tiny syntax — `%Y`, `%m`, `%d`, `%H`
only, everything else literal — validated at startup with a clear error. Not
strftime: a full implementation invites `%s`-style templates that would
reintroduce the replay-instability of §3.1.

**Always rendered in UTC.** `created_at` is unix nanoseconds, and a local-time
render would make the key depend on the host's `TZ` — so a daemon restarted
after a timezone or DST change would replay a batch into a different partition
and duplicate the object, which is exactly the §3.1 failure by another route.
UTC is not configurable, and the reference says so.

## 9. Error handling and classification

Mirrors `sink/clickhouse.rs:214` (`status_is_transient`), which the repo
already pins with an explicit test.

**The rule: dead-letter on record-level rejection, strand on connection-level
rejection.** A record is dead-lettered only when *that record* is the problem.
Anything about the credential, the endpoint, or the bucket applies equally to
every record in the backlog, so it strands — which is recoverable and visible
(`WeirSegmentStranded` fires, the `wab_max_bytes` cap engages) — rather than
dead-lettering, which silently displaces acked data.

**Transient** (strand the segment, retry): 500, 502, 503 (including S3
`SlowDown`), 504, 408, 429; connect / DNS / TLS / reset / timeout; credential
refresh failure; **and every authentication and authorization failure** — 401,
403 `AccessDenied`, `SignatureDoesNotMatch`, `ExpiredToken`,
`TokenRefreshRequired`, `RequestTimeTooSkewed`; plus 404 `NoSuchBucket`.

**Permanent** (dead-letter): 400 `InvalidRequest`, 411, 413 — request-shape
faults that will not change on retry.

**Unrecognised status or S3 error code → transient.** Providers diverge (R2, B2,
Ceph). Stranding an unknown response is recoverable; dead-lettering it is not.

This **departs from the ClickHouse sink** (`clickhouse.rs:214`), which maps auth
4xx to permanent. The departure is deliberate. A day-one credential
misconfiguration, an expired IRSA token, a rotated key, or a signing bug all
present as `403`; dead-lettering the entire backlog over any of them converts a
recoverable operator error into displaced data. The ClickHouse precedent is
arguably wrong for the same reason, but changing it is a separate matter, not
this PR's.

A `PutObject` is all-or-nothing, so `CommitResult.committed` is the entire
batch on success. `dead_lettered` carries **only** records rejected by framing
(§7) — never a network outcome.

Response bodies are run through `sanitize_log_excerpt`-equivalent control-char
stripping before entering logs or dead-letter reasons (the S29 log-forging
defence; the helper is `pub(crate)` in the daemon, so the crate carries its own
copy with a comment pointing at the original).

## 10. Health

`health()` issues `HeadBucket`.

- `200` → `Healthy`
- `403` → `Degraded("bucket exists but HeadBucket is denied")` — a common,
  legitimate least-privilege IAM policy grants `s3:PutObject` without
  `s3:ListBucket`. Writes still work, so this must not read as `Down`.
- `404` → `Down("no such bucket")`
- other / transport error → `Down(reason)`

The `403 → Degraded` mapping is deliberate and directly informed by the
`fix/sweep-truth-up` finding that `Degraded` was a *stickier* failure than
`Down`. That fix must be on the base branch before this sink ships, or a
least-privilege IAM setup would strand its backlog permanently.

## 11. Server wiring and feature gating

- `weir-server` gains `s3-sink = ["dep:weir-sink-s3"]`, **not** in `default`.
- `SinkType::S3` added to the enum at `config/mod.rs:74`, with the same
  built-without-the-feature error arm the other sinks have.
- A construction arm in `main.rs` beside the existing five
  (`main.rs:615-791`).
- `weir_sink_info{sink_type="s3"}` via the existing `as_str()`.

Publish order becomes:
`core → wab → sink-sdk → sink-s3 → rs → client → server → ctl`.

`weir-rs` is in the list because it is a published facade with an optional
`weir-sink-sdk` dependency, so it must follow `sink-sdk`. `weir-testkit` stays
`publish = false`. The workspace version bump to `2.1.0` and the
`[workspace.dependencies]` pins must land in the **same commit** as the new
crate: `weir-sink-s3` needs `weir-sink-sdk 2.1.0` for `segment_created_at`, and
a path dependency hides that locally while an external user resolving `^2.0.5`
from crates.io would fail to compile.

## 12. Object sizing — the small-object trap

`sink_max_batch_size` defaults to **100** (`config/mod.rs:721`, range
1..10,000). One object per commit batch means that at defaults, a 500-byte
record produces **~50 KB objects**. A producer at 10k records/sec would create
360,000 objects an hour — expensive to PUT, and pathological for every query
engine, all of which prefer objects in the tens-to-hundreds of MB.

Three responses, all in scope:

1. The configuration reference gets a **sizing section** recommending
   `sink_max_batch_size = 10000` for S3 and explaining the trade.
2. The sink logs a startup **`WARN`** when `sink_max_batch_size < 1000`, naming
   the resulting approximate object size and the cost implication.
3. That warning **also** restates the §3.5 stability requirement, because the
   operator raising the value is exactly the person who needs to know it must
   never change again.

An object-size-driven batcher (accumulate across commits until N MB) is
explicitly **out of scope** — it would decouple the object from the
`DedupToken`'s batch and destroy the §3 idempotency property. Sizing is the
operator's knob.

## 13. Testing

**Unit (no network):**
- `sigv4` — the applicable `aws-sig-v4-test-suite` cases with the §5.5
  exclusion list, staged assertions; plus `uri_encode_path` against the
  `get-space-unnormalized` and `get-utf8` expectations, and header
  canonicalisation against `get-header-value-trim` and
  `get-header-key-duplicate`.
- `key` — template parsing; partition rendering from a fixed `created_at`; and
  the two properties that matter:
  1. **the same `(created_at, record_id, count)` always yields the same key**
     — replay stability;
  2. **two batches with byte-identical records yield different keys** — the
     §3.3a collision guard. This is the assertion an early draft's design
     failed.
  Assert a rendered value for an instant whose UTC and local dates differ, to
  pin UTC-ness. Do **not** mutate `TZ`: the code never reads it, so such a test
  proves nothing, and `set_var` races sibling tests in the same binary — which
  here includes `creds`, and `creds` reads `AWS_*` from the environment.
- `framing` — NDJSON round-trip; newline records dead-lettered, not silently
  dropped; length-prefixed round-trip; empty-payload handling; compression
  round-trip.
- `creds` — chain precedence; expiry/refresh boundary; a refresh failure is
  classified transient.
- `client` — status/error-code → classification table, including the
  `RequestTimeTooSkewed` exception (§9).

**Integration (MinIO, `--ignored`):**
A `minio` service joins `deploy/docker/test/docker-compose.yml` alongside
MySQL/Postgres/ClickHouse, driven by the existing
`deploy/run-sink-integration-tests.sh` with a healthcheck the runner waits on.

- `s3_sink_end_to_end` — push → seal → drain → object present with expected
  key, bytes, and content type.
- `s3_sink_replay_is_an_idempotent_overwrite` — **headline test 1.** Commit a
  batch, kill the daemon before the segment is confirmed, restart, let it
  replay, then assert the bucket holds **exactly one** object and its bytes are
  unchanged.
- `s3_sink_distinct_batches_of_identical_records_produce_distinct_objects` —
  **headline test 2, and the one that catches the §3.3a failure.** Push two
  separate batches of byte-identical records within the same partition hour,
  then assert the bucket holds **two** objects. The first test alone is not
  enough: "exactly one object" is equally true when the design is destroying
  data, which is precisely how an early draft's `DedupToken` key scheme would
  have passed review.

  Both are needed. Test 1 pins replay stability; test 2 pins collision freedom.
  A key scheme satisfying only one of them is broken.
- `s3_sink_least_privilege_iam_is_degraded_not_down` — a MinIO policy granting
  PutObject but not ListBucket must keep delivering (§10).
- `s3_sink_transient_failure_strands_and_resumes` — stop MinIO mid-drain,
  restart it, assert the backlog resumes.

**Harness note:** `WeirServer::kill_ungracefully` and `restart_in_place` take
`&mut self`, so the crash tests bind `let mut handle = weir_server!(…)`.

**Conformance:** the SigV4 vectors are wired into the same CI job that runs the
wire-format vectors, so both drift-guards live together.

## 14. Documentation

- `docs/operations/configuration.md` — every key from §8, plus the sizing
  section from §12 and the §3.5 stability warning.
- `docs/getting-started/integrating.md` — a short "archive to S3 / MinIO"
  walk-through.
- New `docs/sinks/s3.md` — key layout, the replay-idempotency property and its
  precondition, IAM policy minimum, provider notes (AWS / MinIO / R2 / B2),
  and how to point Athena or DuckDB at the bucket. It must also state that
  **`weir-ctl dl requeue` and `quarantine requeue` produce duplicate objects**:
  both re-push through the daemon socket into a new segment, so the key changes
  by design. `quarantine requeue` already warns that it re-sends records that
  reached the sink; for S3 that surfaces as a second object rather than a
  downstream-side dedup.
- `README.md` — sink count and the crate table.
- `CHANGELOG.md` — under 2.1.0, with the `SinkBatch` addition called out
  explicitly for sink authors.
- `docs/explorations/parked-future-directions.md` — mark the S3 sink resolved,
  and record that the credential + signing layer now exists, which is what
  makes Kinesis/SQS cheap.

## 15. Version

**2.1.0.** The `weir-sink-sdk` change is additive (private field, new accessor,
new constructor; `SinkBatch` construction was already closed), so no major bump
is needed. `weir-sink-s3` starts at `2.1.0` to match the workspace version line.

## 16. Out of scope

- **Multipart upload.** `sink_max_batch_size` caps at 10,000 records; a single
  `PutObject` handles up to 5 GiB. Revisit only if a real batch approaches it.
- **SigV4a** (multi-region access points).
- **Presigned URLs**, `GetObject`, listing, lifecycle management. This is a
  sink; it writes.
- **`~/.aws/credentials` profiles and SSO** (§6).
- **Parquet / Avro output.** Both need a schema; weir payloads are opaque
  bytes. A downstream compaction job is the right place for that.
- **Object-size-driven batching** (§12) — it would break §3.
- **Kinesis, SQS, CloudWatch Logs.** Deliberately deferred; this crate's
  `sigv4` and `creds` modules are the reusable half when they come.

## 17. Risks

| Risk | Mitigation |
|---|---|
| SigV4 correctness — failures are opaque `403`s | AWS's published vectors as staged known-answer tests (§5); MinIO end-to-end in CI |
| Provider divergence (R2/B2 differ from AWS in edge cases) | Path-style toggle; document tested providers; classify unknown errors as **transient** so an unrecognised response strands rather than dead-letters |
| Small-object explosion | Startup `WARN` + sizing docs (§12) |
| Operator changes `sink_max_batch_size`, silently duplicating objects | Blunt warning in the reference and in the startup log (§3.5, §12) |
| Credential refresh races the drain's single-threaded runtime | Refresh 5 min ahead of expiry; refresh failure is transient, never permanent |
| A published crate is a permanent API commitment | Keep the public surface to `S3Sink`, `S3SinkConfig`, `S3SinkError`; everything else `pub(crate)` |

## 18. Acceptance criteria

1. `cargo test -p weir-sink-s3` passes, including the applicable SigV4 vector
   cases (§5.5's exclusion list applied and justified in the fixtures README).
2. `deploy/run-sink-integration-tests.sh` brings up MinIO and all five
   `--ignored` S3 tests pass — **both**
   `s3_sink_replay_is_an_idempotent_overwrite` **and**
   `s3_sink_distinct_batches_of_identical_records_produce_distinct_objects`.
   Either alone is insufficient: the first passes while the design destroys
   data, the second passes while it duplicates on replay.
3. `cargo build -p weir-server --features s3-sink` produces a daemon that
   accepts `--sink-type s3` and delivers to MinIO.
4. `cargo tree -p weir-server --features s3-sink` shows **no `aws-lc-sys`** and
   exactly one `rustls` version.
4b. The workspace version is `2.1.0` and every `[workspace.dependencies]` pin
   matches it, so `weir-sink-s3` resolves `weir-sink-sdk 2.1.0` from crates.io
   rather than a `2.0.5` that lacks `segment_created_at`.
5. `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo fmt --all --check` are clean.
6. `cargo publish --dry-run` succeeds for `weir-sink-s3`.
7. A 1.x-era sink implementation still compiles against the new
   `weir-sink-sdk` — the additive change is proven non-breaking.
8. Docs from §14 are written; no published claim contradicts the code.
