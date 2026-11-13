## What changed

Two small behaviour changes for weir 4.2.0.

**D10: an explicit `--config` that does not exist refuses to start.**

- `weir-server --config <path>` with no file at `<path>` now fails in config
  loading with the new `ConfigError::ConfigFileMissing { path }`. It exits 1
  and prints `config file <path> (passed with --config) does not exist`. This
  happens before the env layer is read and before preflight, recovery, the WAB
  lock or any socket bind, so nothing is created on disk. The message names
  only the path, never file contents or env values.
- A missing *default* file (no `--config`, `/etc/weir/weir.toml` absent) is
  still not an error. The startup INFO `config source` line shows
  `loaded=false`. The only WARN left is the NOOP SINK one, logged when
  `sink_type` resolves to `noop`. The old "will become a startup error in a
  future minor release" sentence and the plain missing-file WARN are gone.
- `deploy/docker/Dockerfile`: the CMD is now `["weir-server"]`, so env-only
  containers keep working. To use a file, bind-mount it at
  `/etc/weir/weir.toml`, and pass `--config` only for a file at another path.
- `deploy/systemd/weir.service` keeps `--config /etc/weir/weir.toml`, so a
  systemd install without that file fails fast and the error shows in the
  journal.

**Batch error: `push_batch` over the record cap reports a record count.**

- New `ClientError::TooManyRecords { count, limit }`. `push_batch` returns it
  for more than `MAX_BATCH_RECORDS_HARD_CAP` (2048) records, where it used to
  return `PayloadTooLarge { len: <record count>, .. }`. It is local and
  recoverable: no bytes are sent and the client is not poisoned. It displays as
  `too many records: 2049 exceeds the 2048-record batch hard cap`.
- `PayloadTooLarge` now means bytes only: a single record, or a whole batch
  body, over the 16 MiB hard cap. `ClientError` is `#[non_exhaustive]`, so the
  new variant is additive.

**Docs:**

- `docs/operations/configuration.md` (a missing config file) and
  `docs/architecture.md` (the config section, plus `ConfigFileMissing` in the
  `ConfigError` variant list) describe D10.
- `docs/getting-started/install.md` covers the Docker CMD and the systemd
  unit's `--config`.
- `docs/getting-started/integrating.md` lists `TooManyRecords` among the
  recoverable errors, and says to split a batch to the daemon's
  `max_batch_records` (default 1024), not to `limit`.
- `docs/wire_protocol.md` adds a client checklist item for the batch record
  count.
- `CHANGELOG.md` has the upgrade notes under Unreleased.

**CI:** `.github/workflows/docker.yml`'s smoke job tags the image
`weir-server:local` and runs the Docker acceptance tests CONFIG-STARTUP-05 and
-08 with `WEIR_ACCEPTANCE_DOCKER=1`. A guard test keeps that wiring in place.

## Why

- **D10:** a mistyped `--config` path used to fall back quietly to env and
  defaults. With no sink configured, those defaults are the `noop` sink, which
  acks and discards every record, so a durable pipeline turned into a silent
  discard. Once the Docker CMD stops passing `--config`, the hard error no
  longer breaks env-only containers.
- **Batch error:** reporting a record count as a byte `len` against a
  2048 `limit` made the error look like a size problem.

Owner decisions (10 Nov): ship with #77 as 4.2.0; Docker CMD
`["weir-server"]`; a missing implicit default is logged at INFO (with the NOOP
SINK WARN when the sink is noop); `TooManyRecords { count, limit }`; the
systemd unit keeps its explicit `--config`.

## How it was tested

- **Unit tests** in `weir-server` config:
  - a missing explicit file is the error, whatever the sink, and the env layer
    is not read;
  - the error's Display and Debug;
  - a missing default with a real sink gives no WARN;
  - a missing default with the noop sink gives exactly one NOOP SINK WARN.

  Unit tests in `weir-client`:
  - 2049 records give `TooManyRecords { count: 2049, limit: 2048 }`, nothing is
    sent, and the next push on the same client succeeds;
  - Display and classification.
- **Acceptance tests**: `acceptance_config_startup.rs` and
  `acceptance_client_batch.rs`, 17 passing and 0 failing. CONFIG-STARTUP-05 and
  -08 need the `weir-server:local` image and run only with
  `WEIR_ACCEPTANCE_DOCKER=1`, which `docker.yml` now sets. They could not run
  in the build box, where `docker build` fails at `apt-get update` behind the
  proxy, so GitHub CI is their first run.
- **Guard tests**: `docs_drift` checks that the architecture config section
  states D10 and that the integration guide lists every variant
  `is_recoverable` returns true for. Both `docs_drift` (35 passed) and
  `demo_drift` (8 passed) were rerun after this docs commit.
- **Full gate at the MVP checkpoint (ee210c1)**: under the owner's
  native-steps rule (13 Nov), every native step had rc=0:
  - fmt
  - clippy-default, clippy-all, clippy-nodefault, clippy-s3
  - demo-version
  - test-ws, test-s3, test-srv, test-srv-bins, test-ws-all, test-srv-all,
    test-srv-bins-all
  - load-norun
  - deny
  - dst-300
  - build-x86_64-unknown-linux-gnu (`TLS smoke OK`)

  These steps were not run in the box and are left to this PR's GitHub CI:
  - msrv (no 1.88 toolchain in the box)
  - the six act-* steps (no act)
  - windows
  - the three cross builds
- **Flows, run by hand on the debug binary**:
  - A missing `--config` exits 1 with the message and leaves an empty dir.
  - An env-only start logs the INFO line, plus the NOOP SINK WARN with the noop
    sink.
  - A user transient unit with the shipped `ExecStart` fails `status=1` with
    the error in the journal.
  - Against a running daemon, `push_batch` of 2049 records returns
    `TooManyRecords`, and the next `push` succeeds.

  The Docker image and the system unit themselves were not exercised in the
  box.
- **Stabilise**: two sweeps. The first sweep's four findings were fixed:
  - the Docker tests' CI wiring;
  - default-path acceptance tests now skip when `/etc/weir/weir.toml` exists;
  - architecture.md's D10 text;
  - integrating.md's recoverable list.

  The second sweep found nothing severe.

## Open items (minor, for a follow-up)

- `crates/weir-server/tests/acceptance_config_startup.rs:293`:
  `config_startup_01`'s reference run passes no `--config`, so it reads the
  host's `/etc/weir/weir.toml`. It fails falsely where weir is installed (the
  file is 0640 root:weir) or the file is malformed. Fix: point it at a scratch
  config, or skip as 06/07/10 do.
- **The 2048-vs-1024 batch cap.** A batch the client accepts (up to 2048
  records) is Nacked by a daemon on the default `max_batch_records = 1024`,
  which also closes the connection, and the client is then poisoned.
  integrating.md and wire_protocol.md now say to split to the daemon's
  `max_batch_records`. The `TooManyRecords` rustdoc
  (`crates/weir-client/src/client.rs`) still just says "split the batch and
  retry". Whether the client should learn the daemon's cap is a design question
  for the owner.
- Fixed in this docs pass: `docs/architecture.md`'s `ConfigError` variant list
  now includes `ConfigFileMissing`.
