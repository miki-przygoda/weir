# weir-go-client — a polyglot weir producer (pure Go, no Rust deps)

A from-scratch **Go** implementation of the weir v1 wire protocol, built only
from `docs/wire_protocol.md` and `docs/conformance/wire_v1_vectors.json` — it
does **not** link the Rust `weir-client`. Stdlib only (`hash/crc32`,
`encoding/binary`, `net`, `encoding/json`).

Goal: prove the wire spec is sufficient to build a wire-compatible producer in
another language, and adversarially test **every Nack reason** and edge case
against a live daemon.

## What's here

| File | Purpose |
|------|---------|
| `wire.go` | Frame encode/decode codec + CRC-32 (IEEE/ISO-3309) + enums |
| `client.go` | Synchronous producer over a Unix socket (framed read) |
| `main.go` | Adversarial **live** harness: crafts a frame per Nack reason + edges, checks reason byte **and** connection-close behavior |
| `conformance_test.go` | Runs all 28 vectors through the codec (decode + re-encode round-trip). Reads the canonical `docs/conformance/wire_v1_vectors.json` (resolved relative to the repo, or via `WEIR_CONFORMANCE_VECTORS`) — no vendored copy. |
| `probe_test.go` | Live probes of corners the docs are quieter about |

## Run

```bash
go test ./...                         # offline: 28/28 conformance vectors

# Live (start the daemon yourself with a unique socket/wab/port):
weir-server --socket-path ./weir.sock --wab-dir ./wab --metrics-port 19009 &
go build -o weir-go-client .
./weir-go-client -socket ./weir.sock  # 15 adversarial cases, all PASS

WEIR_SOCKET=./weir.sock go test -run TestProbe -v   # live probes
```

## Results

- **28/28** conformance vectors pass (decode + round-trip encode).
- **15/15** live adversarial cases pass: every Nack reason
  (`BadMagic`, `VersionMismatch`, `BadHeaderCrc`, `PayloadTooLarge`,
  `BadPayloadCrc`, `EmptyPayload`, `UnknownMessage`×3 paths, `ReservedFlagsSet`),
  the happy path at all 3 durability tiers, HealthCheck, and pipelining —
  including the documented **connection-close** semantics for permanent errors.
- Daemon `weir_records_nack_total{reason=...}` metrics matched the harness 1:1.

The spec was sufficient to build this with zero reference to Rust source. The
live daemon matched its written contract on every case tried.
