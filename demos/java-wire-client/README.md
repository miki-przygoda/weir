# weir Java wire client (stdlib-only)

A self-contained **Java producer** for the [weir](../../) v1 wire protocol,
implemented straight from `docs/wire_protocol.md` and validated against the 28
canonical conformance vectors and a live daemon.

**Zero dependencies.** Only the JDK standard library:
- `java.net.UnixDomainSocketAddress` + `java.nio.channels.SocketChannel` for the
  `AF_UNIX SOCK_STREAM` transport (JDK 16+).
- `java.util.zip.CRC32` for the header / payload CRC (IEEE / ISO-3309 — the
  variant the spec mandates, **not** CRC-32C).
- `java.nio.ByteBuffer` for little-endian framing.

No Maven/Gradle, no third-party jars, **no dependency on any weir crate** — this
is a clean polyglot implementation of the wire, exactly what a skeptical
enterprise team would write to integrate a JVM service with weir without
pulling in a Rust toolchain.

## Domain: enterprise event logging

`AuditEventProducer` streams structured JSON audit events (logins, RBAC grants,
data exports) to weir at **Sync** durability — so an `Ack` means "on stable
storage", the guarantee a compliance auditor cares about. weir is the durable
write-ahead buffer in front of a downstream SIEM / audit store.

## Layout

| File | Role |
|------|------|
| `Wire.java` | Frozen v1 constants + `MessageType` / `Durability` / `NackReason` enums |
| `Frame.java` | Encode/decode for one frame; mirrors the server-side decode order |
| `WeirClient.java` | Sync producer over a Unix domain socket; does its own stream framing |
| `ProtocolException.java` | Decode verdicts + Nack outcomes; `isRetryable()` per the spec's open/closed table |
| `AuditEventProducer.java` | The enterprise-event-logging demo (happy path) |
| `LiveSmokeTest.java` | Happy + rejection paths against a live daemon |
| `ConformanceRunner.java` | Validates the codec against all 28 vectors (+ encode round-trips) |
| `Hex.java`, `MiniJson.java` | Tiny stdlib-only hex / JSON helpers for the vector runner |

## Build

```sh
javac -d out $(find src -name '*.java')
```

## Run the conformance suite (no daemon needed)

```sh
java -cp out dev.weir.client.ConformanceRunner resources/wire_v1_vectors.json
# -> 28/28 vectors passed (4 encode round-trips verified)
```

## Run against a live daemon

```sh
SOCK=$PWD/weir.sock
weir-server --socket-path "$SOCK" --wab-dir "$PWD/wab" --metrics-port 19104 &

java -cp out dev.weir.client.LiveSmokeTest "$SOCK"        # 13/13 checks
java -cp out dev.weir.client.AuditEventProducer "$SOCK" 7 # 7/7 durable acks
```

## What it proves

- **Byte-exact encoder.** Re-encoding the `push_hello_sync`, `push_batched`,
  `push_buffered`, and `healthcheck` vectors produces byte-identical buffers —
  CRC and little-endian layout are correct.
- **Full decode coverage.** All 10 rejection tags (BadMagic, VersionMismatch,
  UnknownMessageType, UnknownDurability, HeaderCrcMismatch, ReservedFlagsSet,
  PayloadTooLarge, TruncatedFrame, PayloadCrcMismatch, TrailingBytes) decode to
  the exact verdict the vectors expect.
- **Live interop.** Real `Ack` at all three durability tiers; real
  `Nack(ReservedFlagsSet)` and `Nack(PayloadTooLarge)` decoded off the wire;
  server-side metrics confirm `weir_records_accepted_total` and the matching
  `weir_records_nack_total{reason=...}` counters.

Built and verified on **OpenJDK 25** (macOS). Requires JDK 16+ for the Unix
domain socket API; the codec itself (`Frame`/`Wire`/`Hex`) is plain Java with no
version-specific APIs.
