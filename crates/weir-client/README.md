# weir-client

Synchronous client for the [weir](https://github.com/miki-przygoda/weir) daemon.

Connects over a Unix socket (or TCP + mutual TLS behind the `tls` feature), sends
Push / HealthCheck frames, and returns typed errors. One blocking round-trip per
call, no async runtime required. Ships `push_simple`, `health_check`, and
`push_tls` examples.

See the [workspace README](https://github.com/miki-przygoda/weir) and the
[quickstart](https://github.com/miki-przygoda/weir/blob/main/docs/getting-started/quickstart.md).
