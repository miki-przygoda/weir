# demos

A small, **curated** set of standalone example projects built on weir — the
strongest picks from the agent development sweeps, kept here *in the repo* as
runnable reference integrations.

> Not to be confused with [`../demo/`](../demo/) (singular) — that's the
> self-contained **interactive website bundle** (the landing-page simulation +
> crate/example pages). This `demos/` directory holds *real, runnable projects*.

## What this is — a 5-language wire-conformance showcase

These five demos implement the **weir v1 wire protocol from the spec alone** —
each in a different language, with **no dependency on any weir crate** — and every
one reproduces all **29 conformance vectors byte-exact**. Together they're the
proof that weir's wire is a complete, language-neutral contract: you can talk to a
weir daemon from anything that can open a socket and compute a CRC32.

## Policy

- **Maximum 5 projects — currently full (5 / 5).** A curated showcase, not a
  dumping ground; only genuinely strong, self-contained examples earn a slot. The
  full library of every project the sweeps produce lives outside the repo in the
  gitignored `.workdocs/projects/` catalog; good ones get *promoted* here
  deliberately. (The slate filled out to a full 5-language spread as a deliberate
  exception — to swap one in, retire one first.)
- **Polyglot, self-contained.** Each is a from-spec wire client with no
  `path = "../../crates/..."` dependency, so this directory stays build-decoupled
  from the Rust workspace. Rust producers/sinks/tools that need the crates stay in
  the catalog instead.
- **Layout is language-idiomatic** (`demos/<project>/`): Python/TS use a `src/`
  layout, Go/Java/C keep their conventional structure. Each carries its own
  `README.md` with run instructions.

## Conformance vectors

Each demo validates against the **canonical** `docs/conformance/wire_v1_vectors.json`
— resolved relative to the repo, never a vendored copy, so they can't drift from
the source of truth. Override the path with the `WEIR_CONFORMANCE_VECTORS` env var
(or, for the C client's `make`, `make check VECTORS=/path/to/vectors.json`).

## The five demos (5 / 5)

| Project | Lang | What it is | Offline conformance |
|---------|------|------------|---------------------|
| [`py-wire-client/`](py-wire-client/) | Python (stdlib) | From-scratch producer + codec; runnable `examples/produce.py` + `scripts/run_daemon.sh`. | `python3 tests/test_conformance.py` |
| [`go-wire-client/`](go-wire-client/) | Go (stdlib) | Producer + codec + a 15-case adversarial live harness (every Nack reason + connection-close). | `go test ./...` |
| [`c-wire-client/`](c-wire-client/) | C (C11, POSIX) | Zero-dep, warning-clean (`-Wall -Wextra -Wpedantic -Wconversion`); embedded/systems angle. | `make check` |
| [`java-wire-client/`](java-wire-client/) | Java (JDK 21+) | Stdlib-only (`UnixDomainSocketAddress` + `CRC32`); enterprise/JVM angle. | `javac -d out $(find src -name '*.java') && java -cp out dev.weir.client.ConformanceRunner` |
| [`ts-wire-client/`](ts-wire-client/) | TypeScript/Node | Zero runtime deps; runs `.ts` on stock Node; end-to-end HTTP→wire→WAB example. | `node src/conformance.ts` |

Run instructions are in each project's own README. The offline conformance suites
need no daemon; the live harnesses start a daemon with an isolated socket/wab/port.
