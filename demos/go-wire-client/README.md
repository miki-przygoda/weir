# go-wire-client

**A weir v1 wire-protocol producer in Go — built from the spec, no weir crate.**

## What it is

A from-scratch, stdlib-only Go producer for the [weir](../../) durable
write-ahead-buffer daemon. It speaks weir's `AF_UNIX` wire protocol directly and
does **not** link the Rust `weir-client` — the codec was written purely from
[`docs/wire_protocol.md`](../../docs/wire_protocol.md) and the
[`docs/conformance/wire_v1_vectors.json`](../../docs/conformance/wire_v1_vectors.json)
vectors.

The demonstration here is **adversarial protocol conformance** rather than a
business domain: `main.go` is a live harness that crafts a frame per Nack reason
and edge case, sends each to a running daemon, and checks both the observed Nack
reason byte **and** the connection-close behavior against the spec. The goal is
to prove the wire spec is sufficient to build a wire-compatible producer in
another language — and that the daemon honors its written contract on every
case.

## How it works

### The frame

Every frame is a fixed **16-byte header**, then the payload, then a 4-byte
payload CRC32 (`EncodeFrame` in `wire.go`):

```
Offset  Size  Field          Value
 0       4    magic          "WEIR"
 4       1    version        WireVersion = 1
 5       1    message_type    MsgPush=0x01 / MsgAck=0x02 / MsgNack=0x03 / MsgHealthCheck=0x04 / MsgHealthCheckResponse=0x05
 6       1    durability      Sync=0x01 / Batched=0x02 / Buffered=0x03
 7       1    flags          reserved; must be 0 on write
 8       4    payload_len    u32 little-endian (encoding/binary)
12       4    header_crc32   CRC32 over bytes [0..12], little-endian
16     var    payload        payload_len bytes
16+n    4    payload_crc32  CRC32 over the payload bytes, little-endian
```

`crc(b)` is `crc32.Checksum(b, crc32.IEEETable)` — IEEE / ISO-3309 CRC-32, the
zlib/PNG variant, **not** CRC-32C. The header CRC covers exactly bytes `[0..12]`;
the payload CRC covers exactly the payload. `EncodePush` / `EncodeHealthCheck`
are convenience encoders over `EncodeFrame`.

### The Push → Ack round-trip

`Client.PushRaw(frame)` (in `client.go`) writes a pre-encoded frame and reads
back one response, returned as a `Response` with `IsAck` / `IsNack` /
`NackReason` (and `DaemonWireVersion` when a `VersionMismatch` Nack carries the
daemon's version byte). Callers encode with `EncodePush(payload, durability)` and
call `PushRaw`. An `Ack` means the record is accepted; a Nack is surfaced as data
on the `Response`, not as a Go error.

### The codec (`wire.go`)

`EncodeFrame` / `DecodeFrame` are inverses. `DecodeFrame` requires the buffer to
be exactly one frame and follows the spec's mandatory order — truncation check,
magic, version (before the header CRC), header CRC, field parsing
(`ErrUnknownMessageType` / `ErrUnknownDurability` / `ErrReservedFlagsSet`), the
payload-length cap (`ErrPayloadTooLarge`), the frame-length check
(`ErrTruncatedFrame` / `ErrTrailingBytes`), then the payload CRC
(`ErrPayloadCrcMismatch`). Each sentinel error's `.Error()` string is exactly the
conformance vector tag. `NackReason.Permanent()` encodes the open/closed rule
(only `InternalError` is transient).

### Connection lifecycle & errors

`Dial(socketPath)` opens a single `net.Dial("unix", ...)` connection with a 5s
timeout — no in-band handshake. Responses are framed by hand in `readFrame()`:
read the 16-byte header, verify magic / version / header CRC *before* trusting
`payload_len`, cap the response `payload_len` at the 16 MiB hard cap, then read
exactly `payload_len + 4` more bytes and run that single buffer through
`DecodeFrame`. A mid-stream close surfaces as `io.EOF`; the harness treats it as
the documented "in-flight outcome unknown, retry on a fresh connection" and, for
permanent errors, verifies the daemon actually closes the connection.

## Conformance

`conformance_test.go` runs the codec against all **30 canonical vectors** (17
valid frames + 13 rejection cases). It first asserts the file's `wire_version`
and `max_payload_hard_cap` match the client's constants, then for each vector
checks the decode outcome and — for valid frames — the decoded `message_type`,
`durability`, `flags`, and payload, plus a **byte-exact re-encode** back to the
vector `hex`. Vectors are read from `docs/conformance/wire_v1_vectors.json`
(resolved relative to the demo dir, two levels up), overridable with the
`WEIR_CONFORMANCE_VECTORS` environment variable. It runs as a standard `go test`
— no daemon needed.

## Run it

```bash
# Offline codec conformance — no daemon needed (run from the demo dir)
cd demos/go-wire-client
go test ./...                              # 30/30 conformance vectors

# Live: start the daemon yourself with a unique socket/wab/port
weir-server --socket-path ./weir.sock --wab-dir ./wab --metrics-port 19009 &
go build -o weir-go-client .
./weir-go-client -socket ./weir.sock       # 15 adversarial cases, all PASS

# Live probes of corners the docs are quieter about (gated on WEIR_SOCKET)
WEIR_SOCKET=./weir.sock go test -run TestProbe -v
```

The live harness covers every Nack reason, the happy path at all three
durability tiers, HealthCheck, and pipelining — including the connection-close
semantics for permanent errors. `probe_test.go` adds live probes (short-header
blocking, lenient HealthCheck, stream framing of trailing bytes, Ack durability
filler, header-validation-before-dispatch, the effective-cap discoverability
gap), skipped unless `WEIR_SOCKET` is set.

## Requirements

Stdlib only (`net`, `encoding/binary`, `hash/crc32`, `encoding/json`) — the
`go.mod` has no `require` block, zero external dependencies. **Tested on go
1.26.3** (the `go 1.26.3` directive in `go.mod`).

## Files

| File | What it holds |
|------|---------------|
| `wire.go`             | The wire codec: constants, `MessageType` / `Durability` / `NackReason` enums, the IEEE CRC-32 helper, `EncodeFrame` / `EncodePush` / `EncodeHealthCheck`, and the strict one-frame `DecodeFrame` with conformance-tagged sentinel errors. |
| `client.go`           | Synchronous Unix-socket producer (`Client`): `Dial` / `Close`, framed `readFrame` / `readResponse` (header-verify-before-payload), `PushRaw` Push→Ack, and Ack/Nack classification. |
| `main.go`             | Adversarial live harness (`-socket` flag, default `weir.sock`): sends happy-path and deliberately-malformed frames and checks each Nack reason + connection-close behavior; exits non-zero on any failure. |
| `conformance_test.go` | Offline `go test` decoding every canonical vector (decode + fields for all, byte-exact re-encode for valid frames). |
| `probe_test.go`       | Live-daemon `go test` probes (gated on `WEIR_SOCKET`) for protocol corners. |
| `go.mod`              | Module `weir-wire-client`, `go 1.26.3`, zero external dependencies. |
</content>
