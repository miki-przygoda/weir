# weir C wire client — embedded telemetry producer

A dependency-free **C** producer for the [weir](../../) durable
write-ahead-buffer daemon. It speaks the weir **v1 wire protocol** over a
Unix socket, built entirely from `docs/wire_protocol.md` and the
`docs/conformance/wire_v1_vectors.json` vectors — **no weir crate is
linked**. Just the C standard library and POSIX sockets.

The use case is embedded-systems telemetry: a sensor node emitting compact
fixed-width binary records (the kind of payload an MCU actually pushes — not
JSON) and streaming them to weir for durable buffering.

## What's here

| File | Role |
|------|------|
| `weir_wire.h` / `weir_wire.c` | The wire codec: CRC-32 (ISO-3309), Push / HealthCheck encode, Ack / Nack / HealthCheckResponse decode. Zero dependencies. |
| `weir_conn.h` / `weir_conn.c` | Blocking POSIX `AF_UNIX` transport with correct stream framing (read 16-byte header, take `payload_len`, read exactly `payload_len + 4`). |
| `telemetry.c` | The demo producer: HealthCheck probe + a stream of 16-byte sensor records at a chosen durability tier. |
| `conformance.c` | Self-test: re-encodes every "ok" Push/HealthCheck vector and demands **byte-exact** equality with the published bytes; decodes every response vector; rejects the rejection vectors. |
| `negative.c` | **Live** error-path coverage: crafts malformed frames and confirms a running daemon Nacks with the spec-mandated reason byte. |

## CRC-32

The two CRC fields are **CRC-32 / ISO-3309** (zlib / PNG / Ethernet —
the algorithm `crc32fast::hash` and `zlib.crc32` expose), **not** CRC-32C.
Implemented as a lazily-built 256-entry reflected table
(`weir_crc32` in `weir_wire.c`), ~30 lines, no dependency.

## Build

```sh
make            # builds: conformance, telemetry, negative
```

Strict flags are on by default
(`-std=c11 -O2 -Wall -Wextra -Wpedantic -Wconversion -Wshadow`) and the tree
builds **warning-clean** under Apple clang.

## Run the conformance self-test (no daemon needed)

```sh
./conformance /path/to/weir/docs/conformance/wire_v1_vectors.json
```

Expected: `vectors scanned: 28   checks passed: 109   failed: 0` →
`RESULT: PASS — C codec is wire-compatible with weir v1.`

The strongest check here is byte-exact re-encoding: the C encoder reproduces
`push_hello_sync` and every other Push/HealthCheck vector to the byte.

## Run against a live daemon

Start a daemon with a private socket (replace paths as needed):

```sh
weir-server \
  --socket-path /tmp/weir-demo/weir.sock \
  --wab-dir     /tmp/weir-demo/wab \
  --metrics-port 19102
```

Stream telemetry (HealthCheck probe, then N records at a durability tier):

```sh
./telemetry /tmp/weir-demo/weir.sock 8 sync
./telemetry /tmp/weir-demo/weir.sock 5 batched
./telemetry /tmp/weir-demo/weir.sock 3 buffered
```

Verify durability + counts from the daemon's metrics:

```sh
curl -s http://127.0.0.1:19102/metrics | grep weir_records_ack_total
```

Exercise the error paths against the live daemon:

```sh
./negative /tmp/weir-demo/weir.sock
# -> 5 PASS: EmptyPayload, BadPayloadCrc, BadMagic, ReservedFlagsSet, UnknownMessage
```

## Telemetry record format (16 bytes, little-endian)

```
u32 node_id
u32 seq
i16 temp_centi_c     (centidegrees C, 2350 == 23.50 C)
u16 humidity_basis   (0.01% units, 4512 == 45.12%)
u32 uptime_ms
```

The payload is opaque to weir — this layout is the producer's choice. A real
consumer/sink would decode it symmetrically.

## Notes / gotchas surfaced while building

- **`sun_path` is ~104 bytes.** Any C Unix-socket client hits this hard limit
  long before a path looks unreasonable; the demo's test socket path was
  already 78 of 104. `weir_connect` guards it with an explicit
  `ENAMETOOLONG`. The wire spec's "Socket setup" section documents the
  `0o600` mode and absolute-path requirement but not this platform limit —
  worth a one-line note for non-Rust producers.
- **The daemon closes the connection after a permanent error.** `negative.c`
  therefore uses a fresh connection per malformed frame.
- **HealthCheck, not empty Push, for liveness.** A zero-length Push is
  rejected with `Nack(EmptyPayload)`; the codec refuses to build one.
