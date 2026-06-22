# c-wire-client

**A weir v1 wire-protocol producer in C — built from the spec, no weir crate.**

## What it is

A dependency-free C producer for the [weir](../../) durable
write-ahead-buffer daemon — just the C standard library and POSIX sockets, no
weir crate linked. The codec was written entirely from
[`docs/wire_protocol.md`](../../docs/wire_protocol.md) and the
[`docs/conformance/wire_v1_vectors.json`](../../docs/conformance/wire_v1_vectors.json)
vectors. It speaks weir's `AF_UNIX` wire protocol directly.

The demo (`telemetry.c`) demonstrates **embedded-systems telemetry**: a sensor
node emitting compact 16-byte fixed-width binary records — the kind of payload an
MCU actually pushes, not JSON — and streaming them to weir for durable buffering.
Each record is little-endian `u32 node_id`, `u32 seq`, `i16 temp_centi_c`,
`u16 humidity_basis`, `u32 uptime_ms`. The payload is opaque to weir; this layout
is the producer's choice and a real sink would decode it symmetrically.

## How it works

### The frame

Every frame is a fixed **16-byte header**, then the payload, then a 4-byte
payload CRC32 (`write_header` + `weir_encode_push` in `weir_wire.c`):

```
Offset  Size  Field          Value
 0       4    magic          'W' 'E' 'I' 'R'
 4       1    version        WEIR_WIRE_VERSION = 1
 5       1    message_type    WEIR_MSG_PUSH=0x01 / ACK=0x02 / NACK=0x03 / HEALTHCHECK=0x04 / HEALTHCHECK_RESPONSE=0x05
 6       1    durability      WEIR_DUR_SYNC=0x01 / BATCHED=0x02 / BUFFERED=0x03
 7       1    flags          hardcoded 0x00 (reserved; must be zero on write)
 8       4    payload_len    u32 little-endian (put_u32_le)
12       4    header_crc32   CRC32 over bytes [0..12], little-endian
16     var    payload        payload_len bytes
16+n    4    payload_crc32  CRC32 over the payload bytes, little-endian
```

`weir_crc32` is a lazily-built 256-entry reflected lookup table (poly
`0xEDB88320`, init/xorout `0xFFFFFFFF`) — IEEE / ISO-3309 CRC-32, the zlib/PNG
variant, **not** CRC-32C — in ~30 lines with no dependency. The header CRC covers
exactly bytes `[0..12]`; the payload CRC covers exactly the payload.

### The Push → Ack round-trip

A producer does `weir_encode_push(dur, payload, len, out, cap, &out_len)` →
`weir_send_all(fd, out, out_len)` → `weir_recv_response(fd, &resp)` (in
`weir_conn.c`). `weir_encode_push` refuses a zero-length payload
(`WEIR_ERR_EMPTY_PAYLOAD`) and an over-cap payload (`WEIR_ERR_PAYLOAD_TOO_LARGE`)
before building anything. The response is examined via `resp.hdr.message_type`,
`resp.is_nack`, and `resp.nack_reason`.

### The codec (`weir_wire.c` / `.h`)

`weir_encode_push` and `weir_encode_healthcheck` build frames;
`weir_decode_resp_header` validates a 16-byte response header in the spec's
order — magic (`WEIR_ERR_BAD_MAGIC`), version (`WEIR_ERR_BAD_VERSION`), header CRC
over `[0..12]` (`WEIR_ERR_BAD_HEADER_CRC`), then caps the response `payload_len`
at `WEIR_MAX_RESPONSE_PAYLOAD` (2 bytes) so a desynced peer can never make the
client allocate an attacker-chosen buffer. Diagnostic helpers
(`weir_nack_reason_str`, `weir_msg_type_str`, `weir_result_str`) name every byte.

### Connection lifecycle & errors

`weir_connect(socket_path)` opens an `AF_UNIX` / `SOCK_STREAM` socket and guards
the `sun_path` length (returns `ENAMETOOLONG` rather than silently truncating) —
no in-band handshake. `weir_recv_response` frames the response by hand: `read_exact`
the 16-byte header, decode it, then `read_exact` exactly `payload_len + 4` more
bytes into a small fixed buffer and verify the payload CRC. A peer close mid-read
returns `WEIR_ERR_SHORT_READ`; `telemetry.c` treats it as "outcome unknown, retry
on a fresh connection." Nacks are split into transient (`WEIR_NACK_INTERNAL_ERROR`,
connection stays open → continue) vs permanent (connection closed → stop). One
implementation detail: a *response* payload-CRC mismatch reuses
`WEIR_ERR_BAD_HEADER_CRC` rather than a dedicated code (see `weir_conn.c`).

## Conformance

`conformance.c` runs **offline** against the canonical vectors. For each `"ok"`
Push or HealthCheck vector it re-encodes via `weir_encode_push` /
`weir_encode_healthcheck` and demands **byte-exact** equality with the published
bytes; for `"ok"` response vectors (Ack / Nack / HealthCheckResponse) it decodes
the header, checks the type, and verifies the payload CRC; for rejection vectors
it asserts the matching decode error (the offline codec checks the decode-side
errors it can see — BadMagic, VersionMismatch, HeaderCrcMismatch, truncation —
and leaves the server-policy rejections to the live `negative.c`). It parses the
JSON itself (no env var) and is given the vectors path on `argv[1]`; the
Makefile's `check` target passes
`../../docs/conformance/wire_v1_vectors.json` by default.

## Run it

```bash
cd demos/c-wire-client
make                                # builds: conformance, telemetry, negative

# Offline codec conformance — no daemon needed
make check                          # -> "vectors scanned: 30 ... RESULT: PASS"
#   (override the path: make check VECTORS=/abs/path/to/wire_v1_vectors.json)

# Live: start a daemon with a private socket
weir-server --socket-path /tmp/weir-demo/weir.sock \
            --wab-dir /tmp/weir-demo/wab --metrics-port 19102 &

# Stream telemetry: HealthCheck probe, then N records at a durability tier
./telemetry /tmp/weir-demo/weir.sock 8 sync
./telemetry /tmp/weir-demo/weir.sock 5 batched

# Exercise the error paths against the live daemon (5 PASS)
./negative /tmp/weir-demo/weir.sock
```

`negative.c` hand-crafts malformed frames (empty Push, bad payload CRC, bad
magic, reserved flags, client-sends-a-daemon-type) on a fresh connection each and
asserts the spec-mandated Nack reason byte.

## Requirements

C11 + POSIX sockets, no dependencies. Builds warning-clean under
`-std=c11 -O2 -Wall -Wextra -Wpedantic -Wconversion -Wshadow`. **Requires a C11
compiler · tested with clang 21.**

## Files

| File | What it holds |
|------|---------------|
| `weir_wire.h` / `weir_wire.c` | The wire codec: `#define` constants, the `weir_msg_type` / `weir_durability` / `weir_nack_reason` / `weir_result` enums, the lazily-built CRC-32 table, `weir_encode_push` / `weir_encode_healthcheck` / `weir_decode_resp_header`, and the `*_str` diagnostics. |
| `weir_conn.h` / `weir_conn.c` | Blocking POSIX `AF_UNIX` / `SOCK_STREAM` transport: `weir_connect` (with `sun_path` guard), `weir_send_all`, and the read-exact framed `weir_recv_response` with payload-CRC verification. |
| `telemetry.c`  | The demo producer: HealthCheck probe then a stream of 16-byte little-endian sensor records at a chosen durability tier. CLI: `telemetry <socket> [count] [sync\|batched\|buffered]`. |
| `conformance.c`| Offline self-test: byte-exact re-encode of every `"ok"` Push/HealthCheck vector, decode of response vectors, and rejection assertions. |
| `negative.c`   | Live error-path coverage: hand-crafted malformed frames asserting the daemon's Nack reason. |
| `Makefile`     | Targets `conformance` (codec only), `telemetry`, `negative`, and `check` (runs the offline conformance suite). |
</content>
