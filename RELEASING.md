# Releasing weir

The order below is not a preference. Each step exists because skipping it broke a
previous release, and the commit that broke it is named.

## What gets published

**Nine crates.** `weir-testkit` is `publish = false` (test harness only) and is
the sole workspace member that stays off crates.io.

**A published version can never be replaced** — only yanked. So everything a user
sees on crates.io and docs.rs, including each crate's README, has to be right
*before* the publish, not after. 4.0.0 shipped `weir-attest` and `weir-sink-s3`
with READMEs added in the release commit itself for exactly this reason.

## Publish order

Dependencies must exist on crates.io before the crates that require them, so the
order is a topological sort of the intra-workspace `[dependencies]`:

```
weir-core → weir-wab → weir-sink-sdk → weir-attest → weir-client
          → weir-ctl → weir-rs → weir-sink-s3 → weir-server
```

**Any valid topological order works** — only "dependencies before dependents"
matters. `weir-wab` and `weir-sink-sdk` are interchangeable here, for instance,
since both depend on `weir-core` alone. This is the order the snippet below
prints, so the two agree.

Derive it rather than trusting this list if a crate gains a weir dependency:

```sh
# prints a valid order, and names anything that is publish = false
python3 - <<'PY'
import pathlib, tomllib
crates = {}
for d in sorted(pathlib.Path("crates").iterdir()):
    m = d / "Cargo.toml"
    if not m.exists(): continue
    t = tomllib.loads(m.read_text())
    if t["package"].get("publish") is False: continue
    crates[t["package"]["name"]] = [k for k in (t.get("dependencies") or {}) if k.startswith("weir")]
order, seen = [], set()
def visit(n):
    if n in seen: return
    seen.add(n)
    for d in crates.get(n, []): visit(d)
    order.append(n)
for n in crates: visit(n)
print(" → ".join(order))
PY
```

## The version bump

**Bump all seven occurrences in `Cargo.toml` in one commit**: `[workspace.package]
version` *and* the six intra-workspace `[workspace.dependencies]` pins. 3.0.0
bumped the first and forgot the pins, which needed the follow-up `952863a`.

```sh
sed -i '' 's/version = "OLD"/version = "NEW"/g' Cargo.toml   # expect 7 replacements
```

**Never hand-edit `demo/version.js`.** It is generated, and the `lint` job fails
if it drifts:

```sh
./scripts/sync-demo-version.sh
```

**Refresh all THREE lockfiles**, not just the root one:

```sh
cargo metadata --format-version 1 >/dev/null
(cd chaos && cargo metadata --format-version 1 >/dev/null)
(cd fuzz  && cargo metadata --format-version 1 >/dev/null)
```

`chaos/` and `fuzz/` are separate Cargo workspaces whose locks pin the weir path
deps **by version**, so a workspace bump leaves them stale. Their CI steps run
`--locked`, so a stale harness lock **fails the `harnesses` job**. Before
`--locked` was added, cargo silently rewrote them in CI and both sat at `3.0.0`
for the whole of 4.0.0.

**CHANGELOG:** promote `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, leave a
fresh empty `[Unreleased]`, and add the compare link at the bottom:

```
[X.Y.Z]: https://github.com/miki-przygoda/weir/compare/vPREV...vX.Y.Z
```

## Order of operations

1. **Land every content change first.** A README fix after the publish is a fix
   nobody on crates.io will ever see.
2. **Bump** (above), open a PR, and get CI green — all of it, including `windows`.
3. **Merge to `main`.**
4. **Publish, from `main`, in the order above.** `cargo publish -p <crate>`, and
   wait for each to appear in the index before the next: the following crate
   cannot resolve it otherwise.
5. **Tag only after every crate is published.** `git tag vX.Y.Z && git push
   origin vX.Y.Z` — this is what triggers `.github/workflows/release.yml`, which
   builds the binary artefacts. Tagging first advertises a release whose crates
   are not yet installable.
6. **Verify**: `cargo install weir-ctl --version X.Y.Z` from a clean registry
   cache, the docs.rs build, and the GitHub Pages deploy (`Publish docs` runs on
   every push to `main` and serves `main:/docs` through mdBook with link
   checking).

## Before you start

`gh` holds two accounts in its keyring and **the active one drifts**. Only
`miki-przygoda` can write to these repos:

```sh
gh auth status          # confirm "Active account: true" is miki-przygoda
```
