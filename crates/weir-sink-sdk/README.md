# weir-sink-sdk

The `Sink` trait and error/result contract for building sinks for the
[weir](https://github.com/miki-przygoda/weir) daemon.

Implement `Sink` (and `SinkError`) to commit batches of records to a database,
HTTP endpoint, object store, etc. The drain retries transient failures with
backoff and dead-letters permanent ones. Implement and unit-test a sink against
this stable trait independent of the daemon internals; `BasicSinkError` and an
`Infallible` impl are provided for quick error types.

See the [workspace README](https://github.com/miki-przygoda/weir).
