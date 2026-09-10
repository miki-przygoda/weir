# Platform support

Which parts of weir run where, what is tested, and — the part that was
previously written down nowhere — what is **untested**.

The short version: the daemon is Unix-only and the CI test suite runs on Linux
alone. Everything else on this page is detail on those two sentences.

## Daemon (`weir-server`)

| Platform | Builds | Tested | Released binary | Durability |
|---|---|---|---|---|
| `x86_64-unknown-linux-gnu` | yes | **full suite, every push** | yes | `fdatasync` — a real flush |
| `aarch64-unknown-linux-gnu` | yes (cross) | no | yes | as above |
| `x86_64-apple-darwin` | yes | build only | yes | `F_BARRIERFSYNC` — see below |
| `aarch64-apple-darwin` | yes | build + startup smoke | yes | as above |
| Other Unix (BSD, illumos, Android) | **compiles, then refuses every connection** | no | no | untested |
| Windows | **no ingest path at all** | n/a | no, dropped in 2.0.5 | n/a |

Every released binary is built `--features tls`, so it has the TCP + mutual-TLS
listener. The `aarch64-apple-darwin` and `x86_64-unknown-linux-gnu` artifacts
additionally run a startup smoke test that fails the release if the binary was
built without that feature.

### Linux is the only tested platform

`cargo test` runs on `ubuntu-latest` and nowhere else
(`.github/workflows/ci.yml`, the `test` job). The macOS targets are **built**
and one of them is started once; no test in the suite has ever executed on
macOS in CI. That is a deliberate cost trade rather than a statement about
macOS, but it means a macOS-specific regression would reach a release
unnoticed, and you should treat macOS as a development platform rather than a
production one.

### Other Unix platforms fail closed, which looks like a hang

Android, the BSDs and illumos are all `cfg(unix)`, so `mod socket` compiles and
the daemon starts. It will then **refuse every connection**.

`peer_uid` is implemented for `target_os = "linux"` (`SO_PEERCRED`) and
`target_os = "macos"` (`getpeereid`); every other target takes the
`#[cfg(not(any(...)))]` arm and returns `Unsupported`
(`crates/weir-server/src/socket/peer.rs:44-49`). The accept loop treats a failed
credential lookup as untrusted and drops the connection
(`crates/weir-server/src/socket/mod.rs:184-195`) — correctly, since it cannot
prove who the peer is — and `peer_uid_check` defaults to `true`
(`crates/weir-server/src/config/mod.rs:749`). Every connection is refused, and
`weir_connection_rejected_peer_uid_total` climbs.

Setting `peer_uid_check = false` gets past it and gives up the defence-in-depth
that check exists for. Nothing else about those platforms is tested either.

### Windows has no ingest path

`mod socket` is `#[cfg(unix)]`, the TCP + mTLS listener is
`#[cfg(all(unix, feature = "tls"))]`, and `main.rs`'s `#[cfg(not(unix))]` arm
awaits shutdown and nothing else. A Windows `weir-server.exe` cannot accept a
record by any route.

Releases up to and including v2.0.4 shipped that binary anyway. **2.0.5 removed
it.** It was also the only release target with no smoke test — the smoke test
asserts the binary was built with `tls`, which on Windows it cannot be, which is
why the gap survived four releases.

If you have a Windows producer, run the daemon on Linux and connect over the
[TCP + mutual-TLS listener](operations/tcp-mtls.md). That configuration is
supported and CI-enforced; see the next section.

## Libraries and CLI

| Crate | Unix | Windows | Enforced by |
|---|---|---|---|
| `weir-core` | yes | **yes** | `windows` CI job, `cargo check --all-features` |
| `weir-wab` | yes | **yes** | same |
| `weir-sink-sdk` | yes | **yes** | same |
| `weir-sink-s3` | yes | yes (no platform-specific code) | not separately gated |
| `weir-client`, TCP + mTLS transport | yes | **yes** | `windows` CI job builds `weir-client --features tls` |
| `weir-client`, Unix-socket transport | yes | no — `WeirClient::connect` is `#[cfg(unix)]` | — |
| `weir-server` | Unix only | no | — |
| `weir-ctl` | Unix only — it drives the Unix-socket client | no | — |

A Rust developer on Windows can depend on `weir-core`, `weir-wab` and
`weir-sink-sdk` for the wire codec, the on-disk format, or to write a sink, and
on `weir-client --features tls` to produce records against a remote daemon. The
`windows` CI job exists to keep that true: it is the only thing that would catch
an ungated `libc::` or `std::os::unix` import landing in one of them.

## Durability by platform

A `Durable` ack is one fsync, so the tier's meaning is the platform's flush
primitive — not a weir setting. This is the single largest cross-platform
difference in the system, and it is a difference in *kind*, not in speed.

| Platform | WAB record path | Drain confirm | Power-loss safe |
|---|---|---|---|
| Linux | `fdatasync` — the device is told to flush | `fdatasync` | **yes**, on honest storage |
| macOS | `F_BARRIERFSYNC` — an ordering **barrier**, not a flush | `F_FULLFSYNC` | **no, at any tier** |

On macOS `File::sync_all` is `F_FULLFSYNC`, which does flush; the WAB record
path deliberately uses `F_BARRIERFSYNC`
(`crates/weir-server/src/wab/segment.rs:672-685`), which orders writes without
forcing the drive to commit them. Data can therefore still be in the drive's
volatile cache when a `Durable` ack returns. **macOS is not power-loss safe at
any durability tier.** The daemon warns about this at startup.

Measured on one M3 Max, for scale: 36 µs for plain `fsync(2)`, 238 µs for
`F_BARRIERFSYNC`, 3,965 µs for `F_FULLFSYNC`. Comparing a macOS latency figure
against a Linux one compares two different operations —
[environments.md](benchmarks/environments.md) has the full rule.

## Released binaries

Attached to each version tag on the
[releases page](https://github.com/miki-przygoda/weir/releases):

- `weir-server-x86_64-unknown-linux-gnu`
- `weir-server-aarch64-unknown-linux-gnu`
- `weir-server-x86_64-apple-darwin`
- `weir-server-aarch64-apple-darwin`

Plus the Docker image, which is Linux. There is no `weir-ctl` release artifact —
build it from source, or use the Docker image.

## Untested and unsupported

Stated explicitly because "not mentioned" reads as "probably fine":

- **Any Unix other than Linux and macOS.** Compiles; refuses every connection at
  default config, as described above.
- **musl / Alpine.** No target is built or tested. Nothing is known to be wrong;
  nothing has been checked.
- **32-bit targets.** Untested.
- **Network and shared filesystems for the WAB** (NFS, SMB, EFS, virtiofs).
  Untested, and unwise independently of testing: weir's durability argument
  rests on `fsync` semantics a network filesystem does not necessarily provide.
- **Windows `weir-server`.** Not merely untested — it has no ingest path.
- **macOS in production.** Builds and ships, is not power-loss safe, and no test
  in the suite runs there.

If you need one of these, the productive path is to run the daemon on Linux and
reach it over the [TCP + mutual-TLS listener](operations/tcp-mtls.md).
