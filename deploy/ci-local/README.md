# Running CI locally before you push

**Recommended, not shipped.** This directory holds no tooling — only this note.
It describes an approach that works, the traps it hits, and why the
implementation is not in the repository.

## Why bother

A CI round-trip costs GitHub Actions minutes and roughly ten minutes of waiting.
Most red builds are a formatting slip, a clippy lint, or a stale generated file —
things a local run catches in seconds. Running CI locally first is the cheapest
way to raise PR quality and lower spend at the same time.

`CONTRIBUTING.md`'s pre-PR gate covers the common cases and is the right thing to
run while iterating. The technique below covers *everything CI runs*, which is a
different and larger set — see the next section for why that difference bites.

## The one design rule that matters: parse the workflow, do not restate it

The obvious approach is a shell script listing CI's commands. **Do not do that.**
It drifts, silently, and then lies to you.

That is not a hypothetical in this repository:

- `CONTRIBUTING.md` described its gate as *"exactly what CI enforces"* while
  omitting four jobs, including the docs build. A green local gate was followed
  by a red PR **twice in one day**.
- Nothing ran the polyglot conformance clients, so a durability-tier rename
  broke **all five** of them and shipped in 2.0.0 unnoticed.

Both are the same bug: a claim about what is checked, drifting from what is
actually checked. A hand-maintained local mirror reproduces it in a new file.

Instead, read `.github/workflows/ci.yml` and execute the `run:` steps it
contains. A step added to CI then appears locally with no edit. About 100 lines
of Python with `yaml` does it.

## The trap that makes this worth writing down

GitHub Actions steps come in two kinds, and conflating them produces a runner
that reports success having checked nothing:

- **Setup actions** install a tool — `actions/setup-go`, `actions/setup-node`,
  `dtolnay/rust-toolchain`. Locally these are no-ops *provided the tool is
  present*. Probe for it and fail loudly if it is missing; a stale image that
  silently skips is worse than no runner.
- **Working actions** do the job themselves. `EmbarkStudios/cargo-deny-action`
  is not a "cargo-deny installer" — it *runs the check*, with its arguments in
  the step's `with:` block.

A prototype of this runner classified `cargo-deny-action` as setup, confirmed
`cargo-deny` was on the PATH, and reported the `advisories` job **green having
never run cargo-deny**. Treat any action you have not classified as an error
that fails the run, and take its arguments from `with:` rather than
hardcoding them.

## Environment traps, all hit in practice

**Docker on Apple Silicon builds an arm64 image.** The native Rust target is
then `aarch64-unknown-linux-gnu` and x86_64 is the cross one — the reverse of
GitHub's runners. Install both targets and both cross linkers.

**Bind-mounting the repo leaks host build artifacts into the container.** A
`make check` in `demos/c-wire-client` reused a `Mach-O 64-bit executable arm64`
left by a macOS run and died with `Exec format error`. Point
`CARGO_TARGET_DIR` at a volume outside the tree, and expect non-Cargo build
systems to need a clean step.

**The distro Go is too old.** `demos/go-wire-client/go.mod` requires `go 1.26.3`;
Debian's `golang-go` cannot parse it (`invalid go version`). CI uses
`actions/setup-go@v5` with `go-version: stable`, so match that rather than
apt-installing Go.

**`mdbook-linkcheck` publishes an x86_64 binary only.** On arm64 it must be
compiled. `mdbook` itself does ship an aarch64 build. Pin both to the versions
in `.github/workflows/docs.yml` or the docs job checks something else.

**`cargo install cargo-deny` takes ~40 minutes** in a fresh Rust image and is one
network hiccup away from failing the build. Fetch the prebuilt release binary.

**Python buffers stdout when it is not a TTY**, so a runner's progress output
vanishes until the process ends. Use `PYTHONUNBUFFERED=1` or `docker run -it`.

## What cannot run locally, and should be said rather than skipped

| Job | Why |
|---|---|
| `monitoring` | Needs `docker compose` inside the container. Run it on the host instead: `deploy/monitoring/smoke-test.sh --teardown` |
| `build` | Cross-compiles to macOS and `windows-msvc`, which need those SDKs. The Linux targets build fine locally; the other three only really prove out on GitHub's runners. |

List them explicitly in whatever you build. A job missing from both the run set
and the "cannot run" list is a silent gap — the exact failure this whole note is
about.

## Why the implementation is not committed

It worked, and it is still not here on purpose.

It is opinionated tooling with a real maintenance cost: it must track the
workflow's action set, the image must track CI's toolchain versions, and every
new `uses:` action needs classifying. When that maintenance lapses, the runner
becomes another thing claiming to check what it no longer checks — which is the
problem it was built to solve.

The technique is the durable part. Build it if it saves you time, keep it out of
the tree unless someone owns it, and hold it to the same standard as anything
else here: **a check that cannot fail is worse than no check at all.**
