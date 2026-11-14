# d10-and-batch-error: behaviour

Written from `spec.md` only. Each behaviour is given/when/then.

## config-startup: D10 config-file startup behaviour (spec flows 1-3)

### CONFIG-STARTUP-01: an explicit `--config` that doesn't exist refuses to start
- Given no file exists at `/etc/weir/missing.toml`
- When `weir-server --config /etc/weir/missing.toml` is run
- Then the process exits at once with weir's existing config-error exit status (non-zero)
- And the error reads `config file /etc/weir/missing.toml (passed with --config) does not exist`

### CONFIG-STARTUP-02: the D10 error touches nothing on disk or network
- Given an explicit `--config` path that doesn't exist
- When `weir-server` is started with it
- Then the error is raised in config loading, before preflight, recovery or any socket bind
- And no WAB, socket or lock file is created

### CONFIG-STARTUP-03: the D10 error prints the path only
- Given an explicit `--config` path that doesn't exist and `WEIR_*` environment variables set
- When `weir-server` refuses to start
- Then the error names the path and says it was passed with `--config`
- And it prints no file contents and no env values

### CONFIG-STARTUP-04: an explicit `--config` that exists is read as before
- Given a valid config file at the path passed with `--config`
- When `weir-server --config <path>` is run
- Then the file is read and parsed exactly as before, and the server starts

### CONFIG-STARTUP-05: the implicit default file is read when present
- Given `--config` is not passed and `/etc/weir/weir.toml` exists
- When `weir-server` is run
- Then `/etc/weir/weir.toml` is read as before, and the server starts

### CONFIG-STARTUP-06: a missing implicit default still starts, logging the source at INFO
- Given `--config` is not passed and `/etc/weir/weir.toml` does not exist
- When `weir-server` is run with configuration from `WEIR_*` env only
- Then the server starts
- And an INFO log line names the config source, with the default path as absent
- And no "will become a startup error" warning is logged

### CONFIG-STARTUP-07: a missing implicit default with a noop sink logs the NOOP SINK WARN
- Given `--config` is not passed, `/etc/weir/weir.toml` does not exist, and no sink is configured (resolved `sink_type` is `noop`)
- When `weir-server` is run
- Then the server starts
- And the existing NOOP SINK WARN (records acked and discarded) is logged

### CONFIG-STARTUP-08: the Docker image starts env-only without a bind mount
- Given the image built from `deploy/docker/Dockerfile`, whose CMD is `["weir-server"]` with no `--config`
- When `docker run weir-server:local` is run with only `WEIR_*` env and no bind mount
- Then the container starts, the INFO line names the default path as absent
- And with no sink configured, the NOOP SINK WARN appears

### CONFIG-STARTUP-09: the Dockerfile comment explains how to supply a file
- Given `deploy/docker/Dockerfile`
- When its CMD and the comment above it are read
- Then the CMD is `["weir-server"]`
- And the comment says to bind-mount `/etc/weir/weir.toml` to use a file and to pass `--config` only for another path

### CONFIG-STARTUP-10: the systemd unit fails fast on a missing file
- Given `deploy/systemd/weir.service`, which still passes `--config /etc/weir/weir.toml`, and a host without `/etc/weir/weir.toml`
- When `systemctl start weir` is run
- Then the unit fails, with the D10 error in the journal

### CONFIG-STARTUP-11: no new config knob
- Given any configuration
- When weir's config options are listed
- Then there is no option (such as `WEIR_CONFIG_OPTIONAL`) to make an explicit missing `--config` optional

## client-batch: the client's batch record-count error (spec flow 4)

### CLIENT-BATCH-01: too many records returns TooManyRecords
- Given a connected `WeirClient` and `MAX_BATCH_RECORDS_HARD_CAP` of 2048
- When `push_batch` is called with 2,049 one-byte records
- Then it returns `ClientError::TooManyRecords { count: 2049, limit: 2048 }`

### CLIENT-BATCH-02: every batch entry point checks the record count the same way
- Given any client batch entry point that checks the record count
- When it is called with `records.len() > MAX_BATCH_RECORDS_HARD_CAP`
- Then it returns `ClientError::TooManyRecords { count: records.len(), limit: MAX_BATCH_RECORDS_HARD_CAP }`, not `PayloadTooLarge`

### CLIENT-BATCH-03: a batch at the cap is not rejected for its count
- Given a connected `WeirClient`
- When `push_batch` is called with exactly `MAX_BATCH_RECORDS_HARD_CAP` small records
- Then it does not return `TooManyRecords`

### CLIENT-BATCH-04: TooManyRecords is local and recoverable
- Given `push_batch` has returned `TooManyRecords`
- When the error and connection are inspected
- Then no bytes were sent to the server
- And `is_recoverable()` is `true` and `is_poisoned()` is `false`

### CLIENT-BATCH-05: the client stays usable after TooManyRecords
- Given `push_batch` on a client has returned `TooManyRecords { count: 2049, limit: 2048 }`
- When the next `push` is made on the same client
- Then it succeeds

### CLIENT-BATCH-06: TooManyRecords Display text
- Given `ClientError::TooManyRecords { count: 2049, limit: 2048 }`
- When it is formatted with Display
- Then it reads `too many records: 2049 exceeds the 2048-record batch hard cap`

### CLIENT-BATCH-07: PayloadTooLarge means bytes only
- Given a batch within the record cap that holds a single record over the byte hard cap, or whose whole body is over it
- When `push_batch` is called
- Then it returns `ClientError::PayloadTooLarge { len, limit }` with byte lengths
- And `PayloadTooLarge` is never returned for a record-count overflow

### CLIENT-BATCH-08: the new variant is additive
- Given `ClientError` is `#[non_exhaustive]`
- When the public API is compared with the previous release
- Then `TooManyRecords { count: usize, limit: usize }` is a new public variant and no other variant's meaning changes beyond `PayloadTooLarge` being bytes-only
