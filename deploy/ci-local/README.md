# Running CI locally before you push

A CI round-trip costs GitHub Actions minutes and roughly ten minutes of waiting.
Most red builds are a formatting slip, a clippy lint, or a stale generated file —
things a local run catches in seconds.

**Use [`act`](https://nektosact.com).** It reads `.github/workflows/ci.yml` and
runs the jobs in Docker, executing the *actions themselves* rather than a
transcription of their `run:` steps.

```bash
brew install act                                   # or: gh extension install nektos/gh-act

act -W .github/workflows/ci.yml -l                 # list jobs and their stage order
act -W .github/workflows/ci.yml -j lint            # one job
act -W .github/workflows/ci.yml -j conformance     # a job and its dependencies
act -W .github/workflows/ci.yml                    # everything runnable
```

Runner images are pinned in [`.actrc`](../../.actrc) at the repo root, so a
fresh clone works with no configuration. Without it act prompts for an image on
first run and dies with `level=fatal msg=EOF` under any non-interactive
invocation.

`-j <job>` runs that job's `needs:` chain too, so asking for `sink-integration`
rebuilds `lint` and `test` first. Ask for the job you actually want to check.

## The one design rule that matters: run the workflow, do not restate it

The obvious approach is a shell script listing CI's commands. **Do not do that.**
It drifts, silently, and then lies to you.

That is not hypothetical in this repository:

- `CONTRIBUTING.md` described its gate as *"exactly what CI enforces"* while
  omitting four jobs, including the docs build. A green local gate was followed
  by a red PR **twice in one day**.
- Nothing ran the polyglot conformance clients, so a durability-tier rename
  broke **all five** of them and shipped in 2.0.0 unnoticed.

Both are the same bug: a claim about what is checked, drifting from what is
actually checked. A hand-maintained local mirror reproduces it in a new file.

That argument also condemns the halfway house. A runner that parses the workflow
but supplies its own toolchain still has an image to keep in step, and *that*
drifts instead — see [The runner this replaced](#the-runner-this-replaced)
below, which is this repository's own worked example. act has neither problem:
`actions/setup-go@v5` installs Go, `dtolnay/rust-toolchain` installs rustup, and
a version bump in the workflow takes effect locally with no edit anywhere.

## What genuinely cannot run locally

Not a limitation of act — a limitation of Linux containers. Say these out loud
rather than skipping them silently; a job missing from both the run set and this
table is the gap that bites.

| Job | Why |
|---|---|
| `windows` | Builds `weir-client --features tls` and checks the published libraries for `x86_64-pc-windows-msvc`, which needs the MSVC toolchain. Both steps compile happily on Linux and would report green **while proving nothing about Windows** — the exact false pass this table exists to stop. |
| `build` (macOS targets) | Cross-compiles to `x86_64-apple-darwin` and `aarch64-apple-darwin`, which need the macOS SDK. The two Linux targets do build locally. |

`sink-integration` and `monitoring` **do** run. Both shell out to
`docker compose`, and act binds `/var/run/docker.sock` into the runner and
starts it with `network="host"`, so compose services come up as siblings and
their published ports resolve from inside the job. Verified by bringing up MinIO
from within a runner container and reaching it on `127.0.0.1:19000`.

**When a CI job is split or renamed, re-check this table.** The `windows` row is
the worked example: 2.0.5 dropped the Windows `weir-server` target and moved the
mTLS-client guarantee into a job of its own. The new job inherited no "cannot
run" entry, so the previous local runner listed it as runnable and it passed — on
the wrong operating system.

## Traps worth knowing

**Architecture.** On Apple Silicon act runs arm64 containers; GitHub's runners
are amd64. `--container-architecture linux/amd64` matches CI exactly but runs
under qemu — fine for `lint` or `conformance`, punishing for the Rust `test`
job. Native arm64 is the right default; reach for amd64 only when chasing a
failure you suspect is architecture-specific.

**act copies the repo; it does not bind it.** The working tree is copied into a
Docker volume mounted at the repo's own path, so the container's
`target/`, `~/.cargo` and any `Swatinem/rust-cache` "Cleaning ..." output refer
to the copy. Your host build cache is untouched — and, more usefully, host build
artifacts cannot leak *in*. A `Mach-O 64-bit executable arm64` left in
`demos/c-wire-client` by a macOS build is invisible to the container, where a
bind mount would hand it to `make check` and produce `Exec format error`.

**`cargo-deny` reads a cached crates.io index.** A local run can pass on a stale
index while CI fails on a crate yanked since the last fetch — this bit a real PR.
Refresh before trusting a local pass: `cargo deny fetch`, or `cargo update -w`
without `--offline`.

**The first run is slow.** act clones each action and the image pulls ~500 MB.
Subsequent runs reuse both.

## The approach this replaced, and why it is worth recording

Everything in this directory except this file is gitignored, and was before act
arrived: the previous approach was a local, untracked runner — a Python script
that parsed the workflow and executed its `run:` steps inside a hand-built image
carrying the toolchain. It was left untracked deliberately, on the grounds that
opinionated tooling nobody owns becomes another thing claiming to check what it
no longer checks.

That judgement was right, and the evidence arrived before act did. The image
apt-installed Debian's `golang-go` — `go1.19.8` — while
`demos/go-wire-client/go.mod` requires `go 1.26.3`. So the polyglot conformance
step it existed to run had become a step that **always failed**, in a repository
where the Go client is fine, and a real PR had to distinguish that false failure
from a genuine one by hand. CI uses `actions/setup-go@v5` with
`go-version: stable` and never had the problem.

That is the rot warned about at the top of this file, one level down: not a
restated command list, but a restated *toolchain*. Parsing the workflow closes
the first hole and leaves the second wide open. act closes both, which is why
this file now recommends it rather than a technique.

If you still keep a personal runner in this directory, it stays ignored. But a
check that fails when the code is fine trains you to ignore checks, which is the
same failure as a check that cannot fail — so if the image needs maintaining,
delete it instead.
