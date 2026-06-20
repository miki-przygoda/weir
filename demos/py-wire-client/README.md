# py-wire-client — a pure-Python weir producer

A from-scratch, **stdlib-only** (`socket`, `struct`, `zlib`, `enum`,
`dataclasses`) Python implementation of the weir v1 wire protocol. No Rust, no
weir crates imported — it talks to the daemon over its `AF_UNIX` socket using
the framing defined in `docs/wire_protocol.md`.

This was built the way a **polyglot developer** would: working *only* from
`docs/wire_protocol.md` and `docs/conformance/wire_v1_vectors.json`, never
looking at the Rust source or the shipped reference codec.

## What's here

| Path | What |
|------|------|
| `src/weir_wire/codec.py`  | Frame encode/decode (the wire codec, from the spec). |
| `src/weir_wire/client.py` | `WeirClient`: connect, `push()`, `health_check()` over the Unix socket. |
| `tests/test_conformance.py` | Runs MY codec against the shipped `wire_v1_vectors.json` (28 vectors). |
| `tests/test_live.py`      | Runs the producer against a live daemon (happy path + every rejection path). |
| `examples/produce.py`     | Tiny end-to-end demo. |
| `scripts/run_daemon.sh`   | Launch an isolated weir-server (writes `daemon.pid`). |

## Run it

```bash
# 1. start a daemon (isolated socket / wab / metrics)
bash scripts/run_daemon.sh

# 2. codec conformance — no daemon needed
python3 tests/test_conformance.py        # -> "28/28 vectors passed"

# 3. live validation against the running daemon
python3 tests/test_live.py weir.sock     # -> "11/11 live tests passed"

# 4. demo
python3 examples/produce.py weir.sock

# 5. stop YOUR daemon
kill "$(cat daemon.pid)"
```

## Results (verified)

- **28/28** conformance vectors pass — decode outcomes, decoded fields, and
  byte-for-byte re-encode round-trips.
- Encoder reproduces the `wire_protocol.md` worked examples **byte-for-byte**
  (the `"hello"`/Sync Push `57454952…86a61036` and the Ack `…c9474b3a00000000`).
- **11/11** live tests pass against a real `weir-server`: health check, Push at
  all three durability tiers, 50 pipelined Pushes, and every server-enforced
  rejection (EmptyPayload, BadPayloadCrc, BadMagic, ReservedFlagsSet,
  UnknownMessage, PayloadTooLarge) plus the post-permanent-Nack connection close.
- Daemon metrics confirm the records landed: `weir_records_accepted_total`
  counts match, and the payloads are visible in `wab/shard_00/seg_00000001.wab`.

## Notes on building from the spec

The wire doc is unusually complete for a polyglot author: a full field table, a
mandatory decode order, exact CRC parameters (with an explicit "not CRC-32C"
warning that saved a classic mistake), byte-exact worked examples, and a
machine-readable conformance suite. A working client fell out in one pass. A few
friction points are recorded in the build notes that accompany this project.
