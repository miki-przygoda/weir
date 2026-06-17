# weir Wire Protocol — Conformance Vectors

This directory ships a language-neutral conformance suite for the weir v1 wire
protocol: [`conformance/wire_v1_vectors.json`](conformance/wire_v1_vectors.json).
If you are writing a weir client or daemon in another language, run your codec
against these vectors to confirm it is byte-compatible with the reference
implementation. The format itself is specified in
[`wire_protocol.md`](wire_protocol.md); this file describes only the vectors.

## What's in the file

```json
{
  "wire_version": 1,
  "max_payload_hard_cap": 16777216,
  "description": "...",
  "vectors": [ { ... }, ... ]
}
```

Each entry in `vectors` is one test case:

| Field          | Present when      | Meaning                                                              |
|----------------|-------------------|---------------------------------------------------------------------|
| `name`         | always            | Stable identifier for the case.                                     |
| `notes`        | always            | Human-readable description.                                         |
| `hex`          | always            | The input buffer, lowercase hex (no `0x`, no separators).          |
| `decode`       | always            | `"ok"`, or the name of the rejection error (see below).            |
| `message_type` | `decode == "ok"`  | Decoded message type: `Push` / `Ack` / `Nack` / `HealthCheck` / `HealthCheckResponse`. |
| `durability`   | `decode == "ok"`  | Decoded durability tier: `Sync` / `Batched` / `Buffered`.          |
| `flags`        | `decode == "ok"`  | Decoded flags byte (always `0` in v1).                             |
| `payload_hex`  | `decode == "ok"`  | Decoded payload bytes, lowercase hex (empty string for none).     |

### The two kinds of vector

- **`decode == "ok"`** — `hex` is *exactly one valid frame*. A conformant decoder
  must accept it and produce the stated `message_type`, `durability`, `flags`, and
  payload. Encoding those fields back must reproduce `hex` byte-for-byte
  (encode/decode are inverses).
- **`decode == "<Error>"`** — `hex` must be **rejected**, and the error must be the
  named one. The rejection tags are:

  | Tag                  | Cause                                                          |
  |----------------------|----------------------------------------------------------------|
  | `BadMagic`           | First four bytes are not `WEIR`.                              |
  | `VersionMismatch`    | Version byte ≠ 1.                                             |
  | `UnknownMessageType` | `message_type` byte has no known variant.                    |
  | `UnknownDurability`  | `durability` byte has no known variant.                      |
  | `HeaderCrcMismatch`  | Header CRC does not cover bytes `[0..12]`.                    |
  | `PayloadCrcMismatch` | Trailing CRC does not match the payload.                      |
  | `TruncatedFrame`     | Buffer shorter than `16 + payload_len + 4`.                   |
  | `PayloadTooLarge`    | Declared `payload_len` exceeds `max_payload_hard_cap`.       |
  | `ReservedFlagsSet`   | `flags` byte is nonzero.                                      |
  | `TrailingBytes`      | Buffer longer than the single frame it declares (G18).       |

Your decoder need not use these exact names internally — only map each vector's
tag to your equivalent rejection. The ordering guarantees that make some of these
deterministic (e.g. version is checked before the header CRC, the payload cap
before the frame-length check) are documented in
[`wire_protocol.md`](wire_protocol.md#frame-decode-order-server-side); the vectors
encode the observable result, not the internal order.

## Coverage

The suite covers every message type, **all nine** Nack reason bytes
(`0x01`–`0x09`) as decodable Nack frames, and one rejection vector per
`DecodeError` variant, including the boundary cases (empty payloads, the
payload-cap boundary, a truncated header vs. a truncated payload, and trailing
bytes after a complete frame).

## CRC algorithm

Both CRC fields are **IEEE / ISO-3309 CRC-32** — the zlib/PNG/Ethernet variant,
*not* CRC-32C (Castagnoli). The vectors are generated with Python's
`zlib.crc32` and verified against the Rust `crc32fast` crate, so a passing
vector means two independent CRC implementations agree on the bytes. See
[`wire_protocol.md`](wire_protocol.md#crc32-algorithm) for the full parameter
table.

## Running the suite

**Rust (reference implementation):**

```sh
cargo test -p weir-core --test conformance
```

`crates/weir-core/tests/conformance.rs` loads this JSON, decodes every vector
with weir-core, and checks the result — and, for `"ok"` vectors, that re-encoding
round-trips to the same bytes.

**Other languages:** a ready-to-run reference harness ships at
[`conformance/run_vectors.py`](conformance/run_vectors.py) (stdlib-only Python).
It includes a small reference codec that passes all vectors, so it doubles as a
worked non-Rust implementation:

```bash
python3 docs/conformance/run_vectors.py   # "28/28 vectors passed"; non-zero exit on mismatch
```

To validate **your own** client, replace its `decode_frame()` / `encode_frame()`
with adapters over your codec and run it unchanged. (Or, from scratch: load the
JSON, hex-decode each `hex`, run your decoder, and assert the outcome matches
`decode` — and the header/payload fields when `"ok"`.)

## Regenerating the vectors (maintainers)

The vectors are generated, never hand-edited, by
[`conformance/gen_vectors.py`](conformance/gen_vectors.py):

```sh
python3 docs/conformance/gen_vectors.py > docs/conformance/wire_v1_vectors.json
cargo test -p weir-core --test conformance   # confirm weir still agrees
```

After an *intentional* wire change, regenerate the file and re-run the Rust
suite; the test deliberately fails if the bytes drift so the change is never
silent. A breaking change to any `"ok"` vector requires a `WIRE_VERSION` bump.
