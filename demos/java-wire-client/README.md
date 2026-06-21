# java-wire-client

**A weir v1 wire-protocol producer in Java — built from the spec, no weir crate.**

## What it is

A self-contained, stdlib-only JDK producer for the [weir](../../) durable
write-ahead-buffer daemon — no Maven, no Gradle, no third-party jars, and no
dependency on any weir crate. The codec was written straight from
[`docs/wire_protocol.md`](../../docs/wire_protocol.md) and validated against the
[`docs/conformance/wire_v1_vectors.json`](../../docs/conformance/wire_v1_vectors.json)
vectors. It speaks weir's `AF_UNIX` wire protocol directly, exactly what a
skeptical enterprise team would write to integrate a JVM service with weir
without pulling in a Rust toolchain.

The demo (`AuditEventProducer`) demonstrates **enterprise audit logging**: it
streams structured JSON audit events (logins, RBAC grants, data exports) to weir
at **Sync** durability — so an `Ack` means "on stable storage", the guarantee a
compliance auditor cares about. weir is the durable write-ahead buffer in front
of a downstream SIEM / audit store.

## How it works

### The frame

Every frame is a fixed **16-byte header**, then the payload, then a 4-byte
payload CRC32 (`Frame.encode()` in `Frame.java`, using a little-endian
`ByteBuffer`):

```
Offset  Size  Field          Value
 0       4    magic          "WEIR"
 4       1    version        WIRE_VERSION = 1
 5       1    message_type    PUSH=0x01 / ACK=0x02 / NACK=0x03 / HEALTH_CHECK=0x04 / HEALTH_CHECK_RESPONSE=0x05
 6       1    durability      SYNC=0x01 / BATCHED=0x02 / BUFFERED=0x03
 7       1    flags          reserved; must be 0 on write (encode() rejects nonzero)
 8       4    payload_len    u32 little-endian
12       4    header_crc32   CRC32 over bytes [0..12], little-endian
16     var    payload        payload_len bytes
16+n    4    payload_crc32  CRC32 over the payload bytes, little-endian
```

The CRC is `java.util.zip.CRC32` — IEEE / ISO-3309 CRC-32, the zlib/PNG variant,
**not** CRC-32C. The header CRC covers exactly bytes `[0..12]`; the payload CRC
covers exactly the payload.

### The Push → Ack round-trip

`WeirClient.push(payload, durability)` (in `WeirClient.java`) builds a `Push`
frame, writes it, reads the response, and interprets it: `ACK` returns the frame;
`NACK` throws `ProtocolException.nack(reason, rawByte)`. An empty payload is
guarded client-side (the daemon would Nack `EmptyPayload`). `healthCheck()` sends
a zero-length `HEALTH_CHECK` and verifies a `HEALTH_CHECK_RESPONSE`.

### The codec (`Wire.java` / `Frame.java`)

`Wire` holds the frozen constants and the `MessageType` / `Durability` /
`NackReason` enums (each with a `code` byte and `fromByte`). `Frame.encode()` /
`Frame.decode(byte[])` are inverses. `decode` mirrors the server's mandatory
order — length-aware magic check, version (before the header CRC), header CRC,
field parsing (`UNKNOWN_MESSAGE_TYPE` / `UNKNOWN_DURABILITY` /
`RESERVED_FLAGS_SET`), the payload cap (`PAYLOAD_TOO_LARGE`), the
exactly-one-frame length check (`TRUNCATED_FRAME` / `TRAILING_BYTES`), then the
payload CRC (`PAYLOAD_CRC_MISMATCH`). Each `Frame.DecodeError` carries the exact
conformance vector name string.

### Connection lifecycle & errors

`WeirClient.connect(socketPath)` opens a `java.net.UnixDomainSocketAddress` +
`java.nio.channels.SocketChannel` (`AF_UNIX SOCK_STREAM`, JDK 16+, pure stdlib,
no JNI) — no in-band handshake. `readResponse()` frames the reply by hand: read
16 header bytes, validate magic/version before trusting `payload_len`, **cap the
response `payload_len` at `MAX_RESPONSE_PAYLOAD` (2 bytes)** before allocating,
then read exactly `payload_len + 4` more bytes and re-validate the whole frame
through `Frame.decode`. A Nack surfaces as `ProtocolException` carrying the
`NackReason`; `isRetryable()` is true only for `INTERNAL_ERROR`. A premature
close throws an `IOException` noting the in-flight outcome is unknown.

## Conformance

`ConformanceRunner` runs the codec against all **28 canonical vectors** (17 valid
frames + 11 rejection cases). For each it checks the decode outcome and — for
`"ok"` frames — the decoded `message_type`, `durability`, `flags`, and payload;
for the **4 client-emittable** valid frames (the 3 Pushes + the HealthCheck) it
also re-encodes and asserts the bytes round-trip **byte-for-byte** (daemon→client
frames are decode-only and skipped for re-encode). Vectors come from `argv[0]`,
else the `WEIR_CONFORMANCE_VECTORS` environment variable, else the default
`../../docs/conformance/wire_v1_vectors.json`; the tiny stdlib `MiniJson` reader
parses the file. It runs via a `main()` and exits non-zero on any mismatch.

## Run it

```bash
cd demos/java-wire-client
javac -d out $(find src -name '*.java')

# Offline codec conformance — no daemon needed
java -cp out dev.weir.client.ConformanceRunner ../../docs/conformance/wire_v1_vectors.json
# -> 28/28 vectors passed (4 encode round-trips verified)

# Live: start a daemon with a unique socket/wab/port
SOCK=$PWD/weir.sock
weir-server --socket-path "$SOCK" --wab-dir "$PWD/wab" --metrics-port 19104 &

java -cp out dev.weir.client.LiveSmokeTest "$SOCK"        # 13/13 checks
java -cp out dev.weir.client.AuditEventProducer "$SOCK" 7 # 7/7 durable acks
```

`LiveSmokeTest` covers the happy path at all three durability tiers plus
rejection paths (`ReservedFlagsSet`, `PayloadTooLarge`, the client-side empty-Push
guard). `AuditEventProducer`'s event-count arg is optional (defaults to 5).

## Requirements

JDK standard library only (`UnixDomainSocketAddress`, `java.util.zip.CRC32`,
`ByteBuffer`). **Requires JDK 16+** (for the `AF_UNIX` domain-socket API)
**· tested on OpenJDK 25.** The codec alone (`Frame` / `Wire` / `Hex` /
`MiniJson`) is plain Java with no version-specific APIs.

## Files

| File | What it holds |
|------|---------------|
| `Wire.java`              | Frozen v1 constants + the `MessageType` / `Durability` / `NackReason` enums (each with a `code` byte and `fromByte`). |
| `Frame.java`            | Single-frame `encode()` / `decode(byte[])` (CRC32 IEEE, little-endian `ByteBuffer`) plus the `DecodeError` verdict enum with vector-name strings. |
| `WeirClient.java`       | Synchronous `AutoCloseable` producer over an `AF_UNIX` `SocketChannel`: `connect` / `push` / `healthCheck`; does its own stream framing and caps response payloads at 2 bytes. |
| `AuditEventProducer.java` | The demo: streams JSON audit events at Sync durability. CLI: `<socket> [event-count]`. |
| `ConformanceRunner.java`| Offline validator running the codec against the 28 vectors (decode + fields for all, byte-exact re-encode for the 4 client-emittable frames). |
| `LiveSmokeTest.java`    | Live happy + rejection paths against a running daemon. CLI: `<socket>`. |
| `ProtocolException.java`| Unchecked wire exception carrying the `DecodeError` / `NackReason` / raw byte, with `isRetryable()`. |
| `Hex.java`, `MiniJson.java` | Tiny stdlib-only hex codec and JSON reader for the vector runner. |
</content>
