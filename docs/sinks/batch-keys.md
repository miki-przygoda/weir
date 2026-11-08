# Batch keys

The ClickHouse sink and the HTTP sink's NDJSON mode send one deduplication key
per commit batch:

| Sink | Where the key goes | Form |
|---|---|---|
| ClickHouse | the `insert_deduplication_token` query parameter | `<64 hex>` |
| HTTP, `sink_http_batch = "ndjson"` | the `Idempotency-Key` header | `sha256:<64 hex>` |

HTTP per-record mode (`sink_http_batch = "none"`, the default) is different: it
sends each record's own `RecordId` as `Idempotency-Key`, one per request.

## What the key is

The key names where the batch sits in the buffer, not what it contains. It is
the SHA-256, in lowercase hex, of the ASCII string

```text
{created_at:016x}-{first_record_id_hex}-{count}
```

- `created_at` is the creation time in the segment's header, in unix
  nanoseconds, read as an unsigned 64-bit number and written as 16 lowercase
  hex digits.
- `first_record_id_hex` is the batch's first
  [`RecordId`](https://docs.rs/weir-sink-sdk/latest/weir_sink_sdk/struct.RecordId.html)
  as 64 lowercase hex digits. A `RecordId` mixes in the segment's shard
  directory and file name, the same `created_at`, and the record's index
  within the segment.
- `count` is the number of records in the batch, in decimal, with no padding.

That string is exactly the [S3 sink's](s3.md#what-an-object-looks-like) object name for the
same batch, without the extension. For example, a three-record batch from a
segment created at `1788704730000000000` whose first `RecordId` is
`1111…1111` (64 hex digits) has the preimage
`18d2c17997b60400-1111…1111-3` and the key
`f9f2be833231eb8c64a5eeafe1c97eceffa5b02da9f473bd617aa5de6639397d`.

The code is `crates/weir-server/src/sink/batch_key.rs`, and the
`batch_key_known_answer` test pins it.

## What that buys

- **A replay presents the same key.** After a crash or a transient error the
  drain re-reads the same segment and re-sends the same batch, so ClickHouse
  or the endpoint recognises it.
- **Identical bytes in different segments get different keys.** A producer
  that sends the same heartbeat every 30 s fills each timed-seal segment with
  one byte-identical record. Each of those batches has its own segment, so each
  has its own key, and each is inserted.
- **Sub-batches of one segment get different keys**, because each starts at a
  different record.

The key still names the batch, so it moves when `sink_max_batch_size` changes
between two delivery attempts. See the note under
[`sink_max_batch_size`](../operations/configuration.md#sink_max_batch_size).

## Batches without segment context

A `SinkBatch` built with `SinkBatch::new` or `From<Vec<Payload>>`, such as one in
a sink author's test, has no `RecordId`s or no segment `created_at`. For such a
batch the key falls back to the batch's `DedupToken`, a hash of its payload
bytes. The drain always supplies both, so every batch it delivers uses the key
above.

## Changed after 4.1.0

Through 4.1.0 both sinks sent the batch's `DedupToken`, a pure hash of the
payload bytes. Two distinct batches with byte-identical records shared a key,
so ClickHouse dropped the second as a duplicate block and an idempotent HTTP
endpoint replayed its cached response, while weir confirmed and deleted the
segment. After upgrading, a batch that was in flight at the upgrade is re-sent
once under the new key, so ClickHouse or the endpoint may see it twice, once.
