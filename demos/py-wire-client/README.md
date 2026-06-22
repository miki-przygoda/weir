# py-wire-client

**A weir v1 wire-protocol producer in Python — built from the spec, no weir crate.**

## What it is

A from-scratch, stdlib-only Python producer for the [weir](../../) durable
write-ahead-buffer daemon. It speaks weir's `AF_UNIX` wire protocol directly:
encode a frame, write it to the socket, read back the Ack. No Rust, no weir
crates imported — the codec was written purely from
[`docs/wire_protocol.md`](../../docs/wire_protocol.md) and the
[`docs/conformance/wire_v1_vectors.json`](../../docs/conformance/wire_v1_vectors.json)
vectors, the way a polyglot developer would.

The example (`examples/produce.py`) demonstrates **event logging**: it health-checks
the daemon, then pushes a handful of JSON `signup` events
(`{"event": "signup", "user": N}`) at Sync durability. The payload is opaque to
weir — JSON is the producer's choice; an MCU would push packed binary, a SIEM
client would push something else. weir just durably buffers the bytes.

## How it works

### The frame

Every frame is a fixed **16-byte header**, then the payload, then a 4-byte
payload CRC32 (`encode_frame` in `src/weir_wire/codec.py`):

```
Offset  Size  Field          Value
 0       4    magic          b"WEIR"
 4       1    version        WIRE_VERSION = 1
 5       1    message_type    Push=0x01 / Ack=0x02 / Nack=0x03 / HealthCheck=0x04 / HealthCheckResponse=0x05
 6       1    durability      Sync=0x01 / Batched=0x02 / Buffered=0x03
 7       1    flags          reserved; must be 0 on write
 8       4    payload_len    u32 little-endian (struct "<I")
12       4    header_crc32   CRC32 over bytes [0..12], little-endian
16     var    payload        payload_len bytes
16+n    4    payload_crc32  CRC32 over the payload bytes, little-endian
```

`_crc32(data)` is `zlib.crc32(data) & 0xFFFFFFFF` — IEEE / ISO-3309 CRC-32, the
zlib/PNG variant, **not** CRC-32C. The header CRC covers exactly bytes `[0..12]`
(magic through `payload_len`); the payload CRC covers exactly the payload.

### The Push → Ack round-trip

`WeirClient.push(payload, durability=Durability.SYNC)` (in
`src/weir_wire/client.py`) encodes a `Push` frame, `sendall`s it, then reads the
response frame and returns a `PushResult(acked, durability_used)`. An `Ack`
response means the record is accepted; a `Nack` raises `NackError`. The client
guards locally against zero-length payloads (weir Nacks those with
`EmptyPayload`) and over-cap payloads before sending.

### The codec (`codec.py`)

`encode_frame(...)` / `decode_frame(buf, max_payload_bytes=...)` are inverses.
`decode_frame` follows the spec's mandatory decode order — magic, then version
(before the header CRC, so a v2 frame is `VersionMismatch` not a CRC error),
then header CRC, then field parsing (`UnknownMessageType` / `UnknownDurability` /
`ReservedFlagsSet`), then the payload-length cap (`PayloadTooLarge`), then the
exactly-one-frame length check (`TruncatedFrame` / `TrailingBytes`), then the
payload CRC (`PayloadCrcMismatch`). Each failure raises `DecodeError` whose
`.tag` matches the conformance vector names.

### Connection lifecycle & errors

`WeirClient.connect()` opens a single `socket.AF_UNIX` / `SOCK_STREAM`
connection — there is no in-band handshake; you connect and send. Responses are
framed by hand: `_read_response_frame()` reads the 16-byte header, takes
`payload_len`, then reads exactly `payload_len + 4` more bytes and hands that
single frame to `decode_frame` (it never feeds a multi-frame buffer to one
decode call). A `Nack` surfaces as `NackError`, which decodes the first payload
byte via `NackReason` and exposes `.retryable` (true only for `InternalError`
and unknown reasons; the eight permanent reasons are not retryable). A
mid-stream socket close raises `ConnectionClosed` ("in-flight outcome unknown;
retry on a fresh connection").

## Conformance

`tests/test_conformance.py` runs the codec against all **30 canonical vectors**
(17 valid frames + 13 rejection cases). For every vector it checks the decode
outcome and — for `"ok"` frames — the decoded `message_type`, `durability`,
`flags`, and payload; for the valid frames it also re-encodes and asserts the
bytes round-trip **byte-for-byte** to the vector's `hex`. The vectors are read
from `docs/conformance/wire_v1_vectors.json` (resolved relative to the repo),
overridable with the `WEIR_CONFORMANCE_VECTORS` environment variable. It is a
plain script (a `__main__` guard, not pytest) and exits non-zero on any
mismatch.

## Run it

```bash
# Offline codec conformance — no daemon needed (run from the repo root)
python3 demos/py-wire-client/tests/test_conformance.py     # -> "30/30 vectors passed"

# Live: start an isolated daemon, then validate against it
bash demos/py-wire-client/scripts/run_daemon.sh            # writes daemon.pid; socket = weir.sock
python3 demos/py-wire-client/tests/test_live.py weir.sock  # -> "11/11 live tests passed"
python3 demos/py-wire-client/examples/produce.py weir.sock # the signup-event demo
kill "$(cat demos/py-wire-client/daemon.pid)"              # stop YOUR daemon
```

`test_live.py` exercises the happy path (health check, Push at all three
durability tiers, 50 pipelined Pushes) and every server-enforced rejection
(EmptyPayload, BadPayloadCrc, BadMagic, ReservedFlagsSet, UnknownMessage,
PayloadTooLarge), plus the connection close after a permanent Nack. Both the
conformance and live runners self-bootstrap `sys.path` — no install step.

## Requirements

Stdlib only (`socket`, `zlib`, `struct`, `enum`, `dataclasses`) — no
`pyproject.toml`, no third-party packages. **Requires Python 3.8+ · tested on
3.14.4.**

## Files

| Path | What it holds |
|------|---------------|
| `src/weir_wire/codec.py`      | The wire codec: `encode_frame` / `decode_frame`, the `Frame` dataclass, `DecodeError` tags, and the `MessageType` / `Durability` / `NackReason` enums (CRC via `zlib.crc32`). |
| `src/weir_wire/client.py`     | `WeirClient`: AF_UNIX producer with `push()` (Push→Ack) and `health_check()`; surfaces rejections as `NackError` / `ConnectionClosed`. |
| `src/weir_wire/__init__.py`   | Package facade re-exporting the public API. |
| `examples/produce.py`         | End-to-end demo: health-check, then push five JSON `signup` events; takes an optional socket-path arg. |
| `tests/test_conformance.py`   | Runs the codec against the 30 canonical vectors (decode + fields for all, byte-exact re-encode for valid frames); plain script. |
| `tests/test_live.py`          | Validates the client against a running daemon: happy path, pipelining, and every rejection path. |
| `scripts/run_daemon.sh`       | Launches an isolated `weir-server` (socket `weir.sock`, `wab/` dir, metrics port), writes `daemon.pid`. |
</content>
</invoke>
