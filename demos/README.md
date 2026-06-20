# demos

A small, **curated** set of standalone example projects built on weir — the
strongest picks from the agent development sweeps, kept here *in the repo* as
runnable reference integrations.

> Not to be confused with [`../demo/`](../demo/) (singular) — that's the
> self-contained **interactive website bundle** (the landing-page simulation +
> crate/example pages). This `demos/` directory holds *real, runnable projects*.

## Policy

- **Maximum 5 projects.** This is a curated showcase, not a dumping ground —
  only genuinely strong, self-contained examples earn a slot. The full library
  of every project the sweeps produce lives outside the repo in the gitignored
  `.workdocs/projects/` catalog; good ones get *promoted* here deliberately.
- **Polyglot first.** Non-Rust wire clients (built only from the spec +
  conformance vectors) are the highest-value demos — they prove weir is
  approachable from any language — so they get priority for the slots.
- **Self-contained.** Each project stands alone: no path-dependency on the weir
  crates. Rust producers/sinks that need `path = "../../crates/..."` stay in the
  catalog instead, to keep this directory build-decoupled from the workspace.
- **Layout is language-idiomatic** (`demos/<project>/`): Python uses a `src/`
  layout, Go keeps its module flat at the project root, etc. Each project carries
  its own `README.md` with run instructions.

## Conformance vectors

Demos that validate against the wire protocol read the **canonical**
`docs/conformance/wire_v1_vectors.json` (resolved relative to the repo, or via the
`WEIR_CONFORMANCE_VECTORS` env var) — never a vendored copy, so they can't drift
from the source of truth.

## Current demos (2 / 5)

| Project | Lang | What it is |
|---------|------|------------|
| [`py-wire-client/`](py-wire-client/) | Python (stdlib) | A from-scratch weir v1 producer + codec built only from the spec. 28/28 conformance vectors; runnable `examples/produce.py` against a local daemon (`scripts/run_daemon.sh`). |
| [`go-wire-client/`](go-wire-client/) | Go (stdlib) | A from-scratch weir v1 producer + codec. 28/28 conformance vectors + a 15-case adversarial live harness covering every Nack reason and the connection-close semantics. |

Run instructions are in each project's own README. The offline conformance suites
need no daemon (`python3 tests/test_conformance.py`, `go test ./...`); the live
harnesses start a daemon with an isolated socket/wab/port.
