# weir-core

Shared types, wire protocol, and error types for [weir](https://github.com/miki-przygoda/weir)
— a durable write-ahead-buffer daemon.

Cross-platform. Contains the wire types — `Envelope`, `Header`, `MessageType`,
`Durability`, `NackReason`, `Payload`, `RecordCoordinate` — the `batch` module's
`PushBatch`/`AckBatch` codec, and `MAX_PAYLOAD_HARD_CAP`.

**Two version numbers, and only one of them is frozen.** `WIRE_VERSION` is still
`1` and the v1 wire format is frozen: it has a language-neutral conformance suite,
and growth within v1 is additive (`PushTracked`/`AckTracked`, `PushBatch`/
`AckBatch`), so an older decoder is *correct* to reject an addition it does not
implement. The **crate's** SemVer is not frozen and is well past 1.0 — this crate's
public Rust API is what has forced every major bump so far (2.0 collapsed
`Durability` to two variants, 3.0 added `MessageType` variants, 4.0 made
`MessageType` `#[non_exhaustive]`).

See the [workspace README](https://github.com/miki-przygoda/weir) for the full
project and the [wire protocol docs](https://github.com/miki-przygoda/weir/blob/main/docs/wire_protocol.md).
