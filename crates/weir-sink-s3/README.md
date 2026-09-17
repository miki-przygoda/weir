# weir-sink-s3

S3-API object-storage sink for the [weir](https://github.com/miki-przygoda/weir)
daemon.

Writes each commit batch as one object, framed as NDJSON by default so Athena,
DuckDB, Spark and Glue can read the bucket with no weir-specific tooling. Targets
the S3 **API** rather than AWS specifically — set an endpoint and MinIO,
Cloudflare R2, Backblaze B2 and Ceph all work on the same path. Object keys are
derived from values invariant under replay (the segment's creation time, the
batch's first `RecordId` and its record count), so weir's at-least-once drain
re-committing a byte-identical batch overwrites its own object instead of
creating a duplicate.

See the [workspace README](https://github.com/miki-przygoda/weir).
