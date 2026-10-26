# weir-status

The `weir.status/v1` status model for [weir](https://github.com/miki-przygoda/weir):
the Conditions catalogue, the pure evaluator, the readiness/liveness/startup
predicates and the Prometheus-exposition parser. The daemon and `weir-ctl`
share it so that every consumer computes the same answer from the same facts.
It performs no I/O and reads no clock.

`weir-server` builds an `Evidence` snapshot from its live state and calls
`evaluate` behind `/status`, `/readyz`, `/livez`, `/startupz`, the admin
socket's `status` verb and the `weir_condition_active{code,severity}` gauge.
`weir-ctl status` calls the same evaluator on the admin document, on a
`/metrics` scrape (`evidence_from_metrics`, which also works against a daemon
older than 4.1.0), or on facts read from disk when no daemon answers. A Rust
program that wants to read a daemon's health the way weir does can depend on
this crate instead of re-implementing the rules.

What it holds:

- **The catalogue** (`CATALOGUE`, `ConditionCode`, `Severity`, `Affects`,
  `Gate`): the 30 codes the daemon evaluates, from `starting` to
  `admin_unavailable`, plus the two only `weir-ctl` emits offline
  (`daemon_unreachable`, `recovery_pending`). Each has a severity, what it
  affects (ingest, delivery or durability), the probes it gates, and a runbook
  anchor in `docs/monitoring.md`.
- **The evaluator**: `evaluate(&Evidence, &Thresholds, &dyn SinceLookup) ->
  Evaluation`. Missing evidence makes a condition *unevaluated*, never
  healthy.
- **Probe predicates**: `readiness`, `liveness` (with the `LivezCheck`s
  selected by the daemon's `livez_checks` key) and `startup`, each running the
  same rules as `evaluate`, so `/readyz` and the conditions list cannot
  disagree.
- **The exit-code table** `weir-ctl status` and `weir-readiness.sh` share
  (`StatusExit`, `exit_code`): 0 ready, 1 not ready (exactly when `/readyz`
  answers 503), 2 unreachable, 3 ready with a critical condition.
- **The documents**: `AdminStatusDoc` (the admin socket's view) and
  `PublicStatusDoc` (what `GET /status` serves), a separate type with no field
  that could carry free text, built from the admin one by `public`.
- **The admin socket protocol** (`admin` module, version 1): the JSON-lines
  request and response envelope, the verbs (`status`, `reload`,
  `drain.pause`, `drain.resume`, `drain.retry_stranded`), error codes, and
  `default_socket_path` (`<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`).

**What is stable.** The JSON document is versioned by its `schema` field
(`"weir.status/v1"`), and keys are only ever added within v1. A condition code
a reader does not know parses as `ConditionCode::Unknown` and is shown
verbatim, never dropped, so an older `weir-ctl` can read a newer daemon. The
Rust API follows the workspace's SemVer like every other weir crate; it is not
frozen.

Depends on `serde` and `serde_json` only; no async runtime.

See the [workspace README](https://github.com/miki-przygoda/weir) and the
[monitoring guide](https://github.com/miki-przygoda/weir/blob/main/docs/monitoring.md)
for the `/status` keys and a runbook per condition.
