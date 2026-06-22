# weir-core

Shared types, wire protocol, and error types for [weir](https://github.com/miki-przygoda/weir)
— a durable write-ahead-buffer daemon.

Cross-platform. Contains the v1 wire types (`Envelope`, `Header`, `Durability`,
`NackReason`, `Payload`) and the `MAX_PAYLOAD_HARD_CAP`. Frozen at 1.0 under
SemVer; the wire format has a language-neutral conformance suite.

See the [workspace README](https://github.com/miki-przygoda/weir) for the full
project and the [wire protocol docs](https://github.com/miki-przygoda/weir/blob/main/docs/wire_protocol.md).
