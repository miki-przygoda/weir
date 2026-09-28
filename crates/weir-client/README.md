# weir-client

Synchronous client for the [weir](https://github.com/miki-przygoda/weir) daemon.

Connects over a Unix socket (or TCP + mutual TLS behind the `tls` feature) and
returns typed errors. One blocking round-trip per call, no async runtime required.

Four producer calls: `push` (one record, one ack), `push_tracked` (returns a
`RecordCoordinate` naming where the record landed), `push_batch` (many records in
one frame; the reply is a bitmap, returned as a `BatchOutcome` — a set bit is a
durability promise, a clear bit means retry) and `health_check`. Ships
`push_simple`, `health_check`, and `push_tls` examples.

See the [workspace README](https://github.com/miki-przygoda/weir) and the
[quickstart](https://github.com/miki-przygoda/weir/blob/main/docs/getting-started/quickstart.md).
