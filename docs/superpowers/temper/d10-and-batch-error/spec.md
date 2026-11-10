# d10-and-batch-error: spec

Written by the head with the owner on 10 Nov 2026. Two small behaviour changes
for weir 4.2.0, released together with PR #77 (cleanup-2026-11). The branch is
stacked on `epic/cleanup-2026-11` and is rebased onto `main` once #77 merges.

## Goal

1. **D10.** A mistyped `--config` path can no longer turn a durable pipeline
   into a silent discard: an explicit `--config` that doesn't exist refuses to
   start. Env-only container deployments keep working.
2. **Batch error.** `WeirClient::push_batch` with too many records reports a
   record count, not a byte length.

## Behaviour

### D10: a missing config file

| `--config` | File exists | Result |
|---|---|---|
| passed explicitly | yes | read it (unchanged) |
| passed explicitly | no | **refuse to start**: a config error naming the path and saying it was passed with `--config`; the process exits with weir's existing config-error exit status |
| not passed (implicit default `/etc/weir/weir.toml`) | yes | read it (unchanged) |
| not passed | no | start; log the config source at **INFO**; if the resolved `sink_type` is `noop`, log the existing **NOOP SINK WARN** (records acked and discarded) |

- The explicit/implicit distinction already exists (`config::cli` tracks
  `explicit`; `ConfigSource { path, explicit, found }`).
- The error happens in config loading, before preflight, recovery or any
  socket bind: nothing is touched on disk.
- The error text never prints file contents (security-r3-02 still holds).
- `deploy/docker/Dockerfile` CMD becomes `["weir-server"]` (the implicit
  default), with the comment above it updated: bind-mount
  `/etc/weir/weir.toml` to use a file; pass `--config` only for another path.
- `deploy/systemd/weir.service` keeps `--config /etc/weir/weir.toml`
  (owner, 10 Nov): a systemd install is file-based, so it fails fast on a
  missing file.
- `missing_config_file_warning` loses its explicit branch and the "will become
  a startup error" sentence.

### Batch error

- New variant `ClientError::TooManyRecords { count: usize, limit: usize }`,
  returned by `push_batch` (and every batch entry point that checks the record
  count) when `records.len() > MAX_BATCH_RECORDS_HARD_CAP`.
- It is recoverable and local: no bytes are sent and the connection stays
  usable (`is_recoverable() == true`, `is_poisoned() == false`), exactly as
  the old `PayloadTooLarge` return for this case was.
- Display: `too many records: {count} exceeds the {limit}-record batch hard cap`.
- `PayloadTooLarge { len, limit }` now means bytes only (a single record over
  the hard cap, or a whole batch body over it, wire-socket-and-client-r3-01).
- `ClientError` is `#[non_exhaustive]`, so the new variant is additive.

## User flows

1. `weir-server --config /etc/weir/missing.toml` → exits non-zero at once with
   `config file /etc/weir/missing.toml (passed with --config) does not exist`;
   no WAB, socket or lock file is created.
2. `docker run weir-server:local` with only `WEIR_*` env (no bind mount) →
   starts, INFO line names the default path as absent; with no sink configured,
   the NOOP SINK WARN appears.
3. `systemctl start weir` on a host without `/etc/weir/weir.toml` → the unit
   fails with the D10 error in the journal.
4. A client calls `push_batch` with 2,049 one-byte records → gets
   `TooManyRecords { count: 2049, limit: 2048 }`; the next `push` on the same
   client succeeds.

## Contracts between parts

- `weir_client::ClientError::TooManyRecords { count: usize, limit: usize }`
  (public, additive).
- Config error for D10: the existing `ConfigError` type, a new or existing
  variant, whose Display matches flow 1; exit status unchanged from other
  config errors.

## Non-goals

- No change to the wire protocol, WAB format, `--json` shapes or any other
  `ClientError` variant's meaning beyond `PayloadTooLarge` now being bytes-only.
- No change to how an existing config file is parsed.
- No new config knob (no `WEIR_CONFIG_OPTIONAL`).
- No version bump in this epic; 4.2.0 is released after it (and #77) merge.

## Upgrade notes (CHANGELOG `### Changed`)

- An explicit `--config` file that doesn't exist now stops startup. The
  systemd unit passes one; container users who relied on the image's old
  `--config` CMD are unaffected (the CMD no longer passes it).
- Callers that matched `ClientError::PayloadTooLarge` to catch an over-long
  batch must also match `ClientError::TooManyRecords`.

## Security notes

- The D10 error prints the path only, never file contents or env values.

## Deferred questions

None.
