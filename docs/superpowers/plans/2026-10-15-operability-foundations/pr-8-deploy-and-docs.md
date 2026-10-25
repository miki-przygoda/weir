# Operability Foundations — PR 8: Deploy defaults, alerts and runbooks, and the docs_drift guards that hold them

> Part of the [Operability Foundations Implementation Plan](README.md). Read its Global Constraints, Decisions and Execution layout first. Task numbers are global across the plan.

## PR 8: Deploy defaults, alerts and runbooks, and the docs_drift guards that hold them

> **Scope (spec §16 PR table, row 8):** deploy (readiness script, Docker HEALTHCHECK, compose grace, systemd), alerts and promtool, runbooks, docs_drift guards. Size M.
> **Depends on:** all of PRs 1-7 ("PR 8 after all", §16 Dependencies). Concretely it reads: the `weir-status` crate, its `ALL` catalogue and golden documents (PR 1); the `weir_condition_active`, `weir_ready`, `weir_startup_phase`, `weir_drain_paused`, `weir_wab_oldest_undelivered_age_seconds`, `weir_wab_handoff_blocked_seconds`, `weir_drain_channel_depth`, `weir_drain_channel_capacity` metric families (PRs 1-6); `drain_shutdown_budget_secs` and its default (PR 4); `/livez`, `/readyz`, `/startupz`, `/status` (PR 5); `admin_socket_enabled`, `admin_socket_path`, the `admin_unavailable` condition (PR 6); `livez_checks` (PR 5); `weir-ctl status` and its exit-code table (PR 7).
> **Behaviour change (release-noted, spec §17 items 6 and 7):** *deploy defaults.* The Docker HEALTHCHECK probes `GET /livez` instead of opening a TCP connection; compose gets `stop_grace_period: 45s`; systemd `TimeoutStopSec` goes 35 → 45; `weir-readiness.sh` no longer gates on sink health or lifetime counters, exit 1 now means exactly "`/readyz` is 503", and exit 3 needs `--fail-on-critical`. `WeirSegmentQuarantined` now keys on quarantine bytes (it keeps firing across a restart until the quarantine is dealt with). No daemon code changes; wire protocol, on-disk format and every existing metric are untouched.
> **Merge gate (spec §16 "Docs and alerts"):** `promtool check rules` + `promtool test rules` green, every new rule pinned in both directions; every new docs_drift guard **verified by mutation** (each task below has the mutation step); `act -W .github/workflows/ci.yml -j lint`, `-j test`, `-j monitoring`, `act -W .github/workflows/docker.yml -j smoke` and `act -W .github/workflows/docs.yml -j build` green locally before pushing (memory: the cargo gate misses the container jobs); all remote checks green including Windows before merge. The security review of redaction and the admin socket is a PR 5 / PR 6 gate; PR 8 re-checks that the threat model states what they built (Task 8.12).

**Integrator reconciliations (applied in this section).** PR 4 Task 4.14 already moved `TimeoutStopSec` to 45, added `stop_grace_period: 45s` and declared the docs_drift consts `SYSTEMD_UNIT`, `SYSTEMD_README` and `COMPOSE` with a narrower grace guard; PR 4 Task 4.2 declared `CI_YML`. Task 8.1 therefore reuses those consts (redeclaring them would not compile), **replaces** PR 4's `the_shutdown_grace_follows_the_shutdown_bound_everywhere` and its `help_default` helper with the stricter guard below, and its red phase lists only what PR 4 left out. Task 8.1 also replaces, never adds beside, PR 4's text in weir.service, the compose file, the systemd README and configuration.md's `#### Shutdown grace period` (PR 4 Task 4.13's section, under the title CROSS-14 settled on), so each file keeps one `stop_grace_period` and one grace section. PR 5 Task 5.11 and PR 6 Task 6.17 write the threat-model rows and actors under the exact titles Task 8.12 pins, so Task 8.12 adds the guard and rewrites only the "Stuck pipeline on shutdown" row. The condition accessors are PR 1's `weir_status::entry(code) -> Option<&'static CatalogueEntry>` (fields `severity`, `affects`, `gates`, `runbook`), not methods on `ConditionCode`.

**Reconciliation rules used in this PR.** Line numbers are from 3adc5f5. PRs 1-7 will have moved some of them; locate by the quoted text, not the number. Where this plan names a `weir-status` item, it uses the spec's §5.1 names; if PR 1 shipped a different spelling, change only the call sites named in the task (the assertions do not change) and record the rename in the PR body. Verified against 3adc5f5: the threat-model rows "Stale socket files" (non-Linux undecidable probe → refused) and "Another instance taking over" (once-a-second integrity check) already carry the safety floor's wording and are not edited here; container.md's Signal-handling first paragraph predates the safety floor, whose SIGHUP paragraph is kept.

**Test commands.** docs_drift: `cargo test -p weir-server --test docs_drift -- --test-threads=1 <name>`. Deploy tests: `cargo test -p weir-server --test deploy -- --test-threads=1 <name>`. Alert rules: `docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/r:ro" -w /r prom/prometheus:v3.14.0 test rules weir-alerts_test.yml` (the exact invocation in ci.yml's `lint` job). All commands run from the repository root.

**Commits.** Conventional subjects, **no trailers** (no Co-Authored-By, no session lines), and never the CI-skip token in a message.

---

### Task 8.1: One shutdown grace formula in every deploy file, guarded

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

The bounded shutdown (PR 4) needs `shutdown_timeout_secs + drain_shutdown_budget_secs + 5` seconds before SIGKILL (spec §7: 30 + 10 + 5 = 45 s at the defaults). Today `weir.service` says 35, compose has no `stop_grace_period` at all (Docker's default is 10 s), and the formula lives nowhere. This task writes the guard first, then makes every file satisfy it.

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs` (add consts after line 32; add helpers and one test at the end of the file)
- Modify: `deploy/systemd/weir.service:65-71` (the "Graceful shutdown (bounded)" comment block and `TimeoutStopSec=45` that PR 4 Task 4.14 wrote there)
- Modify: `deploy/systemd/weir.toml:55-57` (shutdown budget comment and keys)
- Modify: `deploy/docker/docker-compose.yml` (replace PR 4 Task 4.14's `# Bounded shutdown:` comment and `stop_grace_period: 45s` under `restart: unless-stopped`, line 13; the file keeps exactly one `stop_grace_period` key)
- Modify: `deploy/docker/Dockerfile:117-123` (STOPSIGNAL comment)
- Modify: `deploy/docker/weir.toml.example:118-123` (the `shutdown_timeout_secs` comment)
- Modify: `docs/operations/configuration.md` (`### Shutdown`, lines 996-1022 at 3adc5f5: replace the "When to tune" paragraph of `shutdown_timeout_secs` as PR 4 Task 4.13 left it, and rewrite the body of PR 4 Task 4.13's `#### Shutdown grace period` section; no second grace section)
- Modify: `deploy/systemd/README.md:127-141` (the section PR 4 Task 4.14 retitled "Graceful shutdown: `TimeoutStopSec`, `shutdown_timeout_secs` and `drain_shutdown_budget_secs`")
- Modify: `docs/security/container.md:109-116` ("Signal handling (`STOPSIGNAL SIGTERM`)" first paragraph)

**Interfaces:**
- Consumes: `drain_shutdown_budget_secs` resolved in `Config::resolve` as `merge!(drain_shutdown_budget_secs).unwrap_or(10)` (PR 4; same shape as `merge!(shutdown_timeout_secs).unwrap_or(30)` at `config/mod.rs:912`). PR 4 Task 4.1 writes exactly that literal (`merge!(drain_shutdown_budget_secs).unwrap_or(10)`); if it is ever replaced by a named constant, `config_default` below fails with a message saying so.
- Produces (test-private, `crates/weir-server/tests/docs_drift.rs`):
  - `const CONFIG_MOD_SRC: &str`, `const SYSTEMD_TOML: &str`, `const DOCKER_IMAGE: &str`, `const CONTAINER_MD: &str` (plus PR 4's existing `SYSTEMD_UNIT`, `SYSTEMD_README`, `COMPOSE`, reused)
  - `const GRACE_FORMULA: &str = "shutdown_timeout_secs + drain_shutdown_budget_secs + 5";`
  - `fn config_default(key: &str) -> u64`
  - `fn toml_int(toml: &str, key: &str) -> Option<u64>`
  - `#[test] fn the_shutdown_grace_formula_holds_in_every_deploy_file()`, which replaces PR 4 Task 4.14's `the_shutdown_grace_follows_the_shutdown_bound_everywhere` (deleted here together with its `help_default` helper)
- Produces (deploy contract): `TimeoutStopSec=45`, `stop_grace_period: 45s`, `drain_shutdown_budget_secs = 10` in `deploy/systemd/weir.toml`.

- [ ] **Step 1: Write the failing test.** Delete PR 4 Task 4.14's `the_shutdown_grace_follows_the_shutdown_bound_everywhere` and `help_default` (this guard supersedes them; keep PR 4's `SYSTEMD_UNIT`, `SYSTEMD_README` and `COMPOSE` consts, which the test below uses). Add after the existing `const CONFIG_CLI_SRC` (line 32):

```rust
const CONFIG_MOD_SRC: &str = include_str!("../src/config/mod.rs");
const SYSTEMD_TOML: &str = include_str!("../../../deploy/systemd/weir.toml");
const DOCKER_IMAGE: &str = include_str!("../../../deploy/docker/Dockerfile");
const CONTAINER_MD: &str = include_str!("../../../docs/security/container.md");

/// What a process manager must wait, after SIGTERM, before it SIGKILLs weir.
/// Spec §7: connection drain + joins (~1 s) + drain budget + 2 s exit grace,
/// rounded up.
const GRACE_FORMULA: &str = "shutdown_timeout_secs + drain_shutdown_budget_secs + 5";
```

Append at the end of the file:

```rust
/// The default `Config::resolve` applies to `key`: the integer literal in the
/// `.unwrap_or(<n>)` that follows `merge!(<key>)` in src/config/mod.rs.
fn config_default(key: &str) -> u64 {
    let call = format!("merge!({key})");
    let at = CONFIG_MOD_SRC.find(&call).unwrap_or_else(|| {
        panic!("src/config/mod.rs no longer resolves {key} with `{call}`; update this parser")
    });
    let rest = &CONFIG_MOD_SRC[at..];
    let open = rest
        .find(".unwrap_or(")
        .unwrap_or_else(|| panic!("`{call}` has no `.unwrap_or(<default>)`"))
        + ".unwrap_or(".len();
    assert!(
        open < call.len() + 80,
        "the first `.unwrap_or(` after `{call}` is {open} bytes away, so it belongs to \n\
         another statement; {key}'s default is no longer an inline literal"
    );
    let close = rest[open..].find(')').expect("unterminated unwrap_or") + open;
    let lit = rest[open..close].trim().replace('_', "");
    lit.parse().unwrap_or_else(|_| {
        panic!(
            "the default of {key} is {lit:?}, not an integer literal; if it is now a \n\
             named constant, make this parser read the constant"
        )
    })
}

/// `key = <integer>` on an uncommented line of one of the flat `[server]`
/// TOML files this repository ships.
fn toml_int(toml: &str, key: &str) -> Option<u64> {
    toml.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#'))
        .find_map(|l| {
            let (k, v) = l.split_once('=')?;
            if k.trim() != key {
                return None;
            }
            v.split('#').next()?.trim().replace('_', "").parse().ok()
        })
}

/// Every place an operator learns how long to give weir to stop says the same
/// thing, and the shipped units actually give it that long.
///
/// Shutdown became bounded in two terms: the connection drain
/// (`shutdown_timeout_secs`) and the drain's delivery budget
/// (`drain_shutdown_budget_secs`). Before this, `weir.service` carried
/// `TimeoutStopSec=35` sized for the first term only, the compose file had no
/// `stop_grace_period` (Docker SIGKILLs after 10 s), and nothing tied either
/// to the config they ship with. A grace shorter than the bound is
/// crash-safe, but it turns every stop into a crash: the drain gets no budget
/// and every active segment waits for recovery.
///
/// Each unit is checked against the config file it actually runs with, so
/// raising `shutdown_timeout_secs` in `deploy/systemd/weir.toml` without
/// raising `TimeoutStopSec` fails here.
#[test]
fn the_shutdown_grace_formula_holds_in_every_deploy_file() {
    let shutdown = config_default("shutdown_timeout_secs");
    let budget = config_default("drain_shutdown_budget_secs");
    let default_grace = shutdown + budget + 5;
    let grace_for = |toml: &str| {
        toml_int(toml, "shutdown_timeout_secs").unwrap_or(shutdown)
            + toml_int(toml, "drain_shutdown_budget_secs").unwrap_or(budget)
            + 5
    };

    let mut problems = Vec::new();

    // weir.service runs with deploy/systemd/weir.toml.
    let unit = grace_for(SYSTEMD_TOML);
    if !SYSTEMD_UNIT
        .lines()
        .any(|l| l.trim() == format!("TimeoutStopSec={unit}"))
    {
        problems.push(format!(
            "deploy/systemd/weir.service: needs `TimeoutStopSec={unit}` \
             ({GRACE_FORMULA} with deploy/systemd/weir.toml's values)"
        ));
    }

    // The compose file bind-mounts deploy/docker/weir.toml.example.
    let compose = grace_for(CONFIG_EXAMPLE);
    if !COMPOSE
        .lines()
        .any(|l| l.trim() == format!("stop_grace_period: {compose}s"))
    {
        problems.push(format!(
            "deploy/docker/docker-compose.yml: needs `stop_grace_period: {compose}s` \
             ({GRACE_FORMULA} with deploy/docker/weir.toml.example's values)"
        ));
    }

    for (file, text) in [
        ("deploy/systemd/weir.service", SYSTEMD_UNIT),
        ("deploy/systemd/weir.toml", SYSTEMD_TOML),
        ("deploy/systemd/README.md", SYSTEMD_README),
        ("deploy/docker/docker-compose.yml", COMPOSE),
        ("deploy/docker/Dockerfile", DOCKER_IMAGE),
        ("deploy/docker/weir.toml.example", CONFIG_EXAMPLE),
        ("docs/operations/configuration.md", CONFIGURATION),
        ("docs/security/container.md", CONTAINER_MD),
    ] {
        if !text.contains(GRACE_FORMULA) {
            problems.push(format!("{file}: does not state `{GRACE_FORMULA}`"));
        }
    }

    for (file, text) in [
        ("docs/operations/configuration.md", CONFIGURATION),
        ("deploy/systemd/README.md", SYSTEMD_README),
    ] {
        if !text.contains(&format!("**{default_grace} s**")) {
            problems.push(format!(
                "{file}: does not state the default grace as `**{default_grace} s**`"
            ));
        }
    }

    // The figures this replaced. A paragraph left behind beside the new one
    // contradicts it.
    for (file, text, stale) in [
        ("deploy/systemd/weir.toml", SYSTEMD_TOML, "TimeoutStopSec (35s)"),
        ("deploy/systemd/README.md", SYSTEMD_README, "(in `weir.service`, **35**)"),
        ("deploy/systemd/weir.service", SYSTEMD_UNIT, "TimeoutStopSec=35"),
    ] {
        if text.contains(stale) {
            problems.push(format!("{file}: still says {stale:?}"));
        }
    }

    // PR 4 Task 4.14 already added one `stop_grace_period`; a second one is a
    // duplicate mapping key, which compose refuses to load. Nothing in CI
    // parses this compose file, and the `.any(..)` check above still passes
    // with two, so count them.
    let grace_keys = COMPOSE
        .lines()
        .filter(|l| l.trim_start().starts_with("stop_grace_period:"))
        .count();
    if grace_keys != 1 {
        problems.push(format!(
            "deploy/docker/docker-compose.yml: {grace_keys} `stop_grace_period:` keys \
             (compose rejects a duplicate key)"
        ));
    }

    assert!(
        problems.is_empty(),
        "the shutdown grace has drifted:\n  {}\n\n\
         A grace below the bound turns a stop into a crash: the drain gets no \n\
         budget and the active segments wait for recovery.",
        problems.join("\n  ")
    );
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 the_shutdown_grace_formula_holds_in_every_deploy_file`
  Expected: FAIL. PR 4 already set `TimeoutStopSec=45` and `stop_grace_period: 45s` and states the formula in weir.service, the systemd README and configuration.md, so the panic lists what PR 4 left out: at least `deploy/systemd/weir.toml`, `deploy/docker/docker-compose.yml`, `deploy/docker/Dockerfile`, `deploy/docker/weir.toml.example` and `docs/security/container.md` "does not state `shutdown_timeout_secs + drain_shutdown_budget_secs + 5`", and `deploy/systemd/weir.toml: still says "TimeoutStopSec (35s)"` if its old comment survived. Step 3's text for the files PR 4 already edited (weir.service, the systemd README, configuration.md and docker-compose.yml) **replaces** PR 4's block in each, at the anchors Step 3 quotes; it never inserts beside it.

- [ ] **Step 3: Implement.**

`deploy/systemd/weir.service`, replace PR 4 Task 4.14's block (from `# Graceful shutdown (bounded): on SIGTERM weir goes lame duck` through `TimeoutStopSec=45`, as PR 4 Task 4.14 left it) with:

```ini
# Graceful shutdown is BOUNDED. After SIGTERM weir:
#   closes its listeners at once; an idle connection is closed at once, and a
#     connection in the middle of a frame finishes it and gets its ack, for
#     at most shutdown_timeout_secs                        (30 s in weir.toml)
#   joins workers and flushers: active segments sealed and fsynced, each
#     flusher waiting at most 1 s to hand its last segment to the drain
#   lets the drain deliver for at most drain_shutdown_budget_secs (10 s)
#   gives the drain thread 2 s to exit
# so systemd must wait at least
#   shutdown_timeout_secs + drain_shutdown_budget_secs + 5   = 45 s
# before it escalates to SIGKILL. Raise TimeoutStopSec whenever you raise either
# key in weir.toml. What the drain has not delivered when its budget runs out
# stays sealed in the WAB and is delivered after the next start.
KillSignal=SIGTERM
TimeoutStopSec=45
```

`deploy/systemd/weir.toml`, replace the three lines starting `# Graceful-shutdown budget. Keep this BELOW the unit's TimeoutStopSec (35s) so` with (first run `grep -n '^drain_shutdown_budget_secs' deploy/systemd/weir.toml`; if PR 4 already added the key, keep its line and drop the duplicate below):

```toml
# Graceful-shutdown budgets. The unit's TimeoutStopSec must be at least
# shutdown_timeout_secs + drain_shutdown_budget_secs + 5 (45 s with these
# values); raise it whenever you raise either key.
shutdown_timeout_secs      = 30
drain_shutdown_budget_secs = 10
```

`deploy/docker/docker-compose.yml`, replace PR 4 Task 4.14's block (from `    # Bounded shutdown: shutdown_timeout_secs (30) + drain_shutdown_budget_secs` through `    stop_grace_period: 45s`, under `restart: unless-stopped`) with the block below, so the `weir` service keeps exactly one `stop_grace_period` key (the guard counts them):

```yaml

    # weir's shutdown is bounded at
    #   shutdown_timeout_secs + drain_shutdown_budget_secs + 5
    # (30 + 10 + 5 = 45 s with weir.toml.example). Docker's own default is 10 s,
    # after which it SIGKILLs: crash-safe, but the drain then gets no budget and
    # every active segment waits for recovery. Raise this with either key.
    stop_grace_period: 45s
```

`deploy/docker/Dockerfile`, replace the comment above `STOPSIGNAL SIGTERM` (lines 117-122) with:

```dockerfile
# Weir shuts down on SIGTERM, within a bound: listeners close at once, an idle
# connection is closed at once and a connection in the middle of a frame
# finishes it and gets its ack (for at most shutdown_timeout_secs), active
# segments are sealed, and the drain gets drain_shutdown_budget_secs to deliver
# before the rest is left sealed for replay. Give it at least
#   shutdown_timeout_secs + drain_shutdown_budget_secs + 5
# seconds (45 s at the defaults): `docker stop -t 45`, compose
# `stop_grace_period: 45s`, Kubernetes `terminationGracePeriodSeconds: 45`.
# (The formula must stay on one line: a docs_drift test searches for it.)
# SIGHUP never stops it: `docker kill --signal=HUP` reloads TLS material when
# tcp_bind is set and is a counted no-op otherwise.
```

`deploy/docker/weir.toml.example`, replace the line `# Set <= the orchestrator's terminationGracePeriodSeconds minus a few seconds.` with:

```toml
# The orchestrator must wait at least
# shutdown_timeout_secs + drain_shutdown_budget_secs + 5 seconds before it
# SIGKILLs (Kubernetes terminationGracePeriodSeconds, compose stop_grace_period,
# `docker stop -t`): 45 s with the defaults.
```

`docs/operations/configuration.md`, in `#### \`shutdown_timeout_secs\``, replace the paragraph starting `**When to tune**: deployments with long-running batched producers` (as PR 4 Task 4.13 left it, ending with its pointer to the grace section) with:

```markdown
**When to tune**: deployments with long-running batched producers
should raise this to match their batch flush interval, and raise the
process manager's grace by the same amount (see
[Shutdown grace period](#shutdown-grace-period)).
```

then rewrite PR 4 Task 4.13's grace section, which sits after `#### \`drain_shutdown_budget_secs\`` at the end of `### Shutdown`. Its title is `#### Shutdown grace period` (anchor `#shutdown-grace-period`; older drafts of PR 4 titled it `#### Shutdown, grace periods and duplicates`, in which case retitle it here). Replace everything from that heading up to the `---` that precedes `### Dead-letter queue` with the block below. This is a rewrite of PR 4's section, not a second one: the numbered list becomes the Phase/Bound table, and the Grace period, second-SIGTERM, Duplicates and Decommissioning paragraphs are PR 4's, carried over (where PR 4's merged wording of Duplicates or Decommissioning differs from the copy below, keep PR 4's).

```markdown
#### Shutdown grace period

SIGTERM (or SIGINT) runs a bounded sequence, set by two keys:

| Phase | Bound |
|---|---|
| Lame duck: listeners closed, socket file unlinked, `/readyz` 503; a connection in the middle of a frame finishes it and gets its ack, an idle connection is closed at once | `shutdown_timeout_secs` |
| Workers and flushers join: queues flushed, active segments sealed and fsynced, each flusher's last hand-off to the drain | the queue flush, one fsync per shard, and at most 1 s of waiting for the drain |
| The drain delivers what it can | `drain_shutdown_budget_secs` |
| The drain thread exits | 2 s |

The daemon then exits 0, with one log line naming how many segments are left
for replay.

**Grace period.** Give the process manager at least

`shutdown_timeout_secs + drain_shutdown_budget_secs + 5`

seconds before it SIGKILLs: **45 s** at the defaults. The shipped units
already do (`TimeoutStopSec=45` in `deploy/systemd/weir.service`,
`stop_grace_period: 45s` in `deploy/docker/docker-compose.yml`). Elsewhere:
Kubernetes `terminationGracePeriodSeconds: 45`, ECS `stopTimeout: 45`,
`docker stop -t 45`. Docker's own default is 10 s. What the drain has not
delivered when its budget runs out stays sealed and unconfirmed in the WAB
and is delivered after the next start. A SIGKILL before the bound is
crash-safe too, but the drain gets no budget at all and the active segments
are left to recovery.

A **second** SIGTERM at any point forces: open connections are aborted and the
drain deadline becomes "now". The worker and flusher joins (the seal) still
run, so no acked record is lost. A third signal only logs. Exit status is 0 in
every case.

**Duplicates.** For the one segment abandoned at the deadline (at most one per
shutdown, forced or not): its committed prefix is re-delivered on replay; a
commit cancelled at the deadline may already have been applied by the sink;
and records the sink rejected permanently in that prefix are dead-lettered
again, counting against `dead_letter_max_bytes`. A segment a flusher did not
hand off was never started and adds no duplicates. This is the same window as
a crash.

**Decommissioning** a host: stop the producers, wait for
`weir_wab_undelivered_segments` to reach 0 (an empty backlog in
`weir-ctl status`), then stop the daemon; or raise
`drain_shutdown_budget_secs` (and the grace) for that one stop.
```

Then make sure exactly one grace section remains and nothing points at the old title (PR 4 Task 4.1's `drain_shutdown_budget_secs` text referred to it by name):
`grep -c '^#### Shutdown grace period$' docs/operations/configuration.md` → `1`.
`grep -n 'Shutdown, grace periods and duplicates' docs/operations/configuration.md` → no output; for any hit, change the reference to `[Shutdown grace period](#shutdown-grace-period)`.

`deploy/systemd/README.md`, replace the whole section `## Graceful shutdown: \`TimeoutStopSec\`, \`shutdown_timeout_secs\` and \`drain_shutdown_budget_secs\`` (PR 4 Task 4.14's title; lines 127-141 before PR 4) with:

```markdown
## Graceful shutdown: `TimeoutStopSec`

On `systemctl stop` (or a host shutdown), systemd sends `SIGTERM`. weir closes
its listeners and unlinks its socket at once, seals the open WAB segments,
gives the drain a delivery budget, and exits. An idle connection is closed at
once; a connection in the middle of a frame finishes it and gets its ack. Two
keys in `weir.toml` bound that:

- **`shutdown_timeout_secs`** (default **30**): how long a connection in the
  middle of a frame gets to finish it and receive its ack.
- **`drain_shutdown_budget_secs`** (default **10**): how long the drain keeps
  delivering once every segment is sealed. What it does not deliver stays
  sealed in the WAB and is delivered after the next start.

`TimeoutStopSec` must be at least
`shutdown_timeout_secs + drain_shutdown_budget_secs + 5`, which is
**45 s** with the shipped `weir.toml`, and is what `weir.service` sets. If you
raise either key, raise `TimeoutStopSec` by the same amount; a CI test fails
if the shipped files disagree. A second
`systemctl kill --signal=SIGTERM weir` forces a prompt exit (open connections
aborted, the drain stopped); the seal still runs, so no acked record is lost.
```

`docs/security/container.md`, replace the first paragraph under `### Signal handling (\`STOPSIGNAL SIGTERM\`)` (from `Made explicit so orchestrators` through `the recent deadlock fix in PR #10.`) with:

```markdown
Made explicit so orchestrators that override Docker's default
(e.g. Kubernetes' `terminationGracePeriodSeconds` flow, which sends a
configurable signal) still get the clean shutdown path. Shutdown is
bounded: listeners close at once (the socket file is unlinked and `/readyz`
answers 503), an idle connection is closed at once and a connection in the
middle of a frame finishes it and gets its ack (for at most
`shutdown_timeout_secs`), the active segments are sealed and fsynced, and the
drain gets `drain_shutdown_budget_secs` before the rest is left sealed for
replay. Give the container `shutdown_timeout_secs + drain_shutdown_budget_secs + 5`
seconds (45 s at the defaults) before the SIGKILL: `docker stop -t 45`,
compose `stop_grace_period: 45s` (the shipped compose file sets it),
Kubernetes `terminationGracePeriodSeconds: 45`. Docker's own default is 10 s.
```

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 the_shutdown_grace_formula_holds_in_every_deploy_file` → PASS.
  Then the whole file: `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.

- [ ] **Step 5: Verify by mutation** (spec §16: every docs_drift guard is verified by mutation). One at a time, run the Step 4 command after each and revert with `git checkout -- <file>`:
  1. `deploy/systemd/weir.service`: `TimeoutStopSec=45` → `TimeoutStopSec=35`. Expected FAIL: `needs \`TimeoutStopSec=45\`` and `still says "TimeoutStopSec=35"`.
  2. `deploy/systemd/weir.toml`: `shutdown_timeout_secs      = 30` → `= 60`. Expected FAIL: `needs \`TimeoutStopSec=75\``.
  3. `deploy/docker/docker-compose.yml`: delete the `stop_grace_period: 45s` line. Expected FAIL: `needs \`stop_grace_period: 45s\`` and `0 \`stop_grace_period:\` keys`.
  4. `deploy/systemd/README.md`: `**45 s**` → `**40 s**`. Expected FAIL: `does not state the default grace as \`**45 s**\``.
  5. `deploy/docker/docker-compose.yml`: duplicate the `stop_grace_period: 45s` line (what inserting beside PR 4's block would leave). Expected FAIL: `2 \`stop_grace_period:\` keys`.
  Record the five outcomes in the PR body under "Guards verified by mutation".

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/docs_drift.rs deploy/systemd/weir.service deploy/systemd/weir.toml deploy/systemd/README.md deploy/docker/docker-compose.yml deploy/docker/Dockerfile deploy/docker/weir.toml.example docs/operations/configuration.md docs/security/container.md`
  `git commit -m "fix(deploy): give the bounded shutdown its 45 s grace in every unit, and guard the formula"`

---

### Task 8.2: The container HEALTHCHECK probes `/livez`, executed for real

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Today's check is `exec 3</dev/tcp/127.0.0.1/9185 || exit 1` in three places: it passes for anything that accepts a TCP connection. The new check sends `GET /livez` and requires a 200 status line (spec §12). This task also creates the `tests/deploy.rs` target (and wires it into CI) that later tasks extend, because a shell command nobody executes is how the current one shipped.

**Files:**
- Create: `crates/weir-server/tests/deploy.rs`
- Test: `crates/weir-server/tests/docs_drift.rs` (one const, one helper, two tests)
- Modify: `deploy/docker/Dockerfile:125-134` (HEALTHCHECK comment and instruction)
- Modify: `deploy/docker/docker-compose.yml:45-60` (`healthcheck:` block)
- Modify: `deploy/monitoring/docker-compose.yml:32-37` (`healthcheck:` in `x-weir-base`)
- Modify: `.github/workflows/ci.yml:219` and `:224` (add `--test deploy` to both `cargo test -p weir-server --lib --test system ...` lines)
- Modify: `.github/workflows/docker.yml` (smoke job: wait for `healthy` after the `/metrics` check, around line 120)
- Modify: `docs/security/container.md:185-202` (`### Healthcheck`)

**Interfaces:**
- Consumes: `GET /livez` → `200 ok\n` with `livez_checks = []` (PR 5, spec §10); status line `HTTP/1.1 200 OK` (the existing writer, `metrics/server.rs:113`, which PR 5's router keeps for 200s). `weir_testkit::weir_server!`, `WeirServer::metrics_port: u16`.
- Produces:
  - Test target `crates/weir-server/tests/deploy.rs` (`#![cfg(unix)]`) with test-private helpers later tasks reuse:
    - `struct Stub { port: u16, .. }`, `fn Stub::start(routes: Vec<Route>) -> Stub`, `fn Stub::silent() -> Stub`
    - `struct Route { path: &'static str, status: u16, content_type: &'static str, body: String }`, `fn route(path: &'static str, status: u16, content_type: &'static str, body: impl Into<String>) -> Route` (`"*"` matches any path)
    - `const TEXT: &str`, `const OPENMETRICS: &str`
    - `fn run_with_deadline(cmd: &mut Command, deadline: Duration) -> (Option<ExitStatus>, String)`
    - `fn image_healthcheck(port: u16) -> (bool, String)`
  - docs_drift: `const MONITORING_COMPOSE: &str` (and reuses PR 4 Task 4.2's `CI_YML`, not redeclared), `const LIVEZ_HEALTHCHECK: &str`, `fn healthcheck_command(src: &str, anchor: &str, compose: bool) -> String`, tests `every_container_healthcheck_probes_livez_the_same_way`, `the_deploy_tests_run_in_ci`.
  - Deploy contract: the canonical command
    `exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\r\n\r\n' >&3 && read -r l <&3 && [[ $l == *' 200 '* ]]`
    with `--start-period=10s` / `start_period: 10s`.

- [ ] **Step 1: Write the failing tests.**

Create `crates/weir-server/tests/deploy.rs`:

```rust
//! The deploy artefacts, executed: the container HEALTHCHECK command and
//! `deploy/systemd/weir-readiness.sh`.
//!
//! Both are shell, so nothing in the Rust build ever ran them, and both
//! shipped with semantics nobody had executed. The HEALTHCHECK passed for
//! anything that accepted a TCP connection on 9185. The readiness script
//! stayed NOT READY forever after one recovered flusher panic, because it
//! gated on lifetime counters. Every test here runs the shipped text, read
//! from the file operators install, never a copy of it.
//!
//! Needs `bash`, and for the readiness script `curl` and `awk` (the script's
//! documented dependencies).

#![cfg(unix)]

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use weir_testkit::weir_server;

const DOCKERFILE: &str = include_str!("../../../deploy/docker/Dockerfile");
const TEXT: &str = "text/plain; charset=utf-8";
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

// ── A canned HTTP server ──────────────────────────────────────────────────────

#[derive(Clone)]
struct Route {
    path: &'static str,
    status: u16,
    content_type: &'static str,
    body: String,
}

/// `path` matches exactly after the query is stripped; `"*"` matches any path.
fn route(
    path: &'static str,
    status: u16,
    content_type: &'static str,
    body: impl Into<String>,
) -> Route {
    Route {
        path,
        status,
        content_type,
        body: body.into(),
    }
}

/// A loopback HTTP/1.x server answering from a fixed route table, one
/// connection at a time, closing after each response. `None` routes means
/// accept and close at once without writing a byte.
struct Stub {
    port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Stub {
    fn start(routes: Vec<Route>) -> Stub {
        Stub::spawn(Some(routes))
    }

    /// Accepts every connection and closes it without a response: a port that
    /// is open but is not an HTTP server.
    fn silent() -> Stub {
        Stub::spawn(None)
    }

    fn spawn(routes: Option<Vec<Route>>) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the stub");
        listener.set_nonblocking(true).expect("non-blocking accept");
        let port = listener.local_addr().expect("stub address").port();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => match &routes {
                        Some(table) => serve_one(stream, table),
                        None => drop(stream),
                    },
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(_) => break,
                }
            }
        });
        Stub { port, stop, thread: Some(thread) }
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn serve_one(mut stream: TcpStream, routes: &[Route]) {
    // An accepted socket inherits O_NONBLOCK from the listener on macOS.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < 8192 {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    let text = String::from_utf8_lossy(&head);
    let target = text.split_whitespace().nth(1).unwrap_or("/");
    let path = target.split('?').next().unwrap_or(target);
    let hit = routes
        .iter()
        .find(|r| r.path == path)
        .or_else(|| routes.iter().find(|r| r.path == "*"));
    let (status, ct, body) = match hit {
        Some(r) => (r.status, r.content_type, r.body.as_str()),
        None => (404, TEXT, "not found\n"),
    };
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ct}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

/// Runs `cmd`, killing it at `deadline`. Returns its status (`None` when it
/// had to be killed) and its stdout followed by its stderr.
fn run_with_deadline(cmd: &mut Command, deadline: Duration) -> (Option<ExitStatus>, String) {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let end = Instant::now() + deadline;
    let status = loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            break Some(s);
        }
        if Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut out = String::new();
    if let Some(mut o) = child.stdout.take() {
        let _ = o.read_to_string(&mut out);
    }
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut out);
    }
    (status, out)
}

// ── The image HEALTHCHECK ─────────────────────────────────────────────────────

/// Runs the Dockerfile's HEALTHCHECK command, as shipped, against `port`
/// instead of 9185. A check that hangs past Docker's own `--timeout=3s`
/// counts as failed, as it does in Docker.
fn image_healthcheck(port: u16) -> (bool, String) {
    let from = DOCKERFILE
        .find("\nHEALTHCHECK ")
        .expect("the Dockerfile has a HEALTHCHECK instruction");
    let marker = "\"/bin/bash\", \"-c\", \"";
    let start = DOCKERFILE[from..]
        .find(marker)
        .expect("the HEALTHCHECK is the exec form [\"CMD\", \"/bin/bash\", \"-c\", ...]")
        + from
        + marker.len();
    let end = DOCKERFILE[start..].find("\"]").expect("unterminated HEALTHCHECK") + start;
    // JSON string escapes: `\\r` in the Dockerfile is `\r` for bash's printf.
    let cmd = DOCKERFILE[start..end]
        .replace("\\\\", "\\")
        .replace("/127.0.0.1/9185", &format!("/127.0.0.1/{port}"));
    let (status, out) =
        run_with_deadline(Command::new("bash").arg("-c").arg(cmd), Duration::from_secs(3));
    (status.is_some_and(|s| s.success()), out)
}

#[test]
fn the_image_healthcheck_passes_against_a_live_daemon() {
    let srv = weir_server!("hc_livez").start();
    let (ok, out) = image_healthcheck(srv.metrics_port);
    assert!(ok, "the Dockerfile HEALTHCHECK failed against a healthy daemon:\n{out}");
}

#[test]
fn the_image_healthcheck_fails_when_livez_says_not_live() {
    let stub = Stub::start(vec![
        route("/livez", 503, TEXT, "not live: no_live_shard\n"),
        route("*", 200, OPENMETRICS, "# EOF\n"),
    ]);
    let (ok, out) = image_healthcheck(stub.port);
    assert!(!ok, "the HEALTHCHECK passed although /livez answered 503:\n{out}");
}

#[test]
fn the_image_healthcheck_fails_against_a_port_that_only_accepts() {
    let stub = Stub::silent();
    let (ok, out) = image_healthcheck(stub.port);
    assert!(
        !ok,
        "the HEALTHCHECK passed against a port that accepts and says nothing; \n\
         that is exactly what the TCP-connect check it replaced could not tell:\n{out}"
    );
}
```

Add to `crates/weir-server/tests/docs_drift.rs`, after the Task 8.1 consts:

```rust
const MONITORING_COMPOSE: &str = include_str!("../../../deploy/monitoring/docker-compose.yml");
// CI_YML is PR 4 Task 4.2's const; reused, not redeclared.

/// The container liveness check, spec §12: one HTTP/1.0 request to /livez
/// over bash's /dev/tcp (the image ships no curl or wget), healthy only on a
/// 200 status line.
const LIVEZ_HEALTHCHECK: &str = "exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\\r\\n\\r\\n' >&3 && read -r l <&3 && [[ $l == *' 200 '* ]]";
```

and at the end of the file:

```rust
/// The bash command of the first `["CMD", "/bin/bash", "-c", "<cmd>"]` after
/// `anchor`, with JSON/YAML `\\` escapes undone. For a compose file, `$$` is
/// compose's escape for a literal `$`; a lone `$` would be interpolated by
/// compose as an (empty) variable, so it is rejected here rather than undone.
fn healthcheck_command(src: &str, anchor: &str, compose: bool) -> String {
    let from = src
        .find(anchor)
        .unwrap_or_else(|| panic!("no {anchor:?} in this file"));
    let marker = "\"/bin/bash\", \"-c\", \"";
    let start = src[from..]
        .find(marker)
        .expect("the healthcheck is not the exec form [\"CMD\", \"/bin/bash\", \"-c\", ...]")
        + from
        + marker.len();
    let end = src[start..].find("\"]").expect("unterminated healthcheck") + start;
    let cmd = src[start..end].replace("\\\\", "\\");
    if compose {
        assert!(
            !cmd.replace("$$", "").contains('$'),
            "a compose healthcheck uses a bare `$` ({cmd:?}); compose interpolates \n\
             it as an empty variable, so `[[ $l == ... ]]` becomes `[[  == ... ]]` \n\
             and the container is never healthy. Write `$$`."
        );
        cmd.replace("$$", "$")
    } else {
        cmd
    }
}

/// The image, the compose file and the monitoring stack probe liveness with
/// the same command, and it is the /livez one.
///
/// The three copies drifted once already in the other direction: the compose
/// file's comment explains a `wget` probe that failed with "not found" in a
/// curl-less image and restart-looped a healthy container. A copy that still
/// opens a bare TCP connection reports healthy for anything listening on 9185.
#[test]
fn every_container_healthcheck_probes_livez_the_same_way() {
    for (file, cmd) in [
        (
            "deploy/docker/Dockerfile",
            healthcheck_command(DOCKER_IMAGE, "\nHEALTHCHECK ", false),
        ),
        (
            "deploy/docker/docker-compose.yml",
            healthcheck_command(COMPOSE, "healthcheck:", true),
        ),
        (
            "deploy/monitoring/docker-compose.yml",
            healthcheck_command(MONITORING_COMPOSE, "healthcheck:", true),
        ),
    ] {
        assert_eq!(
            cmd, LIVEZ_HEALTHCHECK,
            "{file}: the healthcheck command differs from the canonical /livez probe"
        );
    }
    assert!(
        DOCKER_IMAGE.contains("--start-period=10s"),
        "deploy/docker/Dockerfile: the HEALTHCHECK needs --start-period=10s (spec §12)"
    );
    for (file, src) in [
        ("deploy/docker/docker-compose.yml", COMPOSE),
        ("deploy/monitoring/docker-compose.yml", MONITORING_COMPOSE),
    ] {
        assert!(src.contains("start_period: 10s"), "{file}: needs `start_period: 10s`");
    }
}

/// `tests/deploy.rs` runs on both `cargo test -p weir-server` lines in CI.
///
/// ci.yml names its test targets one by one, so a new target that is not
/// added there compiles locally and never runs in CI: "a drift guard is
/// theatre until wired".
#[test]
fn the_deploy_tests_run_in_ci() {
    let lines: Vec<&str> = CI_YML
        .lines()
        .filter(|l| l.contains("cargo test -p weir-server --lib --test system"))
        .collect();
    assert_eq!(
        lines.len(),
        2,
        "expected the default-features and --all-features weir-server test lines \n\
         in ci.yml, found {}",
        lines.len()
    );
    for line in lines {
        let words: Vec<&str> = line.split_whitespace().collect();
        assert!(
            words.windows(2).any(|w| w == ["--test", "deploy"]),
            "ci.yml line does not run --test deploy:\n  {line}"
        );
    }
}
```

- [ ] **Step 2: Run them to verify they fail.**
  `cargo test -p weir-server --test deploy -- --test-threads=1` → `the_image_healthcheck_fails_when_livez_says_not_live` and `the_image_healthcheck_fails_against_a_port_that_only_accepts` FAIL ("the HEALTHCHECK passed ..."): the shipped command only connects. `the_image_healthcheck_passes_against_a_live_daemon` passes already (it pins the new command against the real daemon).
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 every_container_healthcheck_probes_livez_the_same_way the_deploy_tests_run_in_ci` → both FAIL: `deploy/docker/Dockerfile: the healthcheck command differs`, and `ci.yml line does not run --test deploy`.

- [ ] **Step 3: Implement.**

`deploy/docker/Dockerfile`, replace lines 125-134 (the `# Lightweight LIVENESS check` comment through the `CMD [...]` line) with:

```dockerfile
# LIVENESS: GET /livez over bash's /dev/tcp (the image has no curl or wget).
# /livez answers 200 whenever the runtime serves HTTP: from a few milliseconds
# after start, through WAB recovery, and while shutting down. It fails only on
# the checks selected in `livez_checks` (default none), each a state a restart
# fixes. It never fails on sink health, a paused, retrying or blocked drain, or
# a long recovery, so Docker never restarts weir for a slow sink. Readiness
# ("can it take ingest now?") is GET /readyz; probe it from outside
# (deploy/systemd/weir-readiness.sh, or a Kubernetes readinessProbe). The port
# is the default metrics_port: change 9185 here if you change it.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/bin/bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\\r\\n\\r\\n' >&3 && read -r l <&3 && [[ $l == *' 200 '* ]]"]
```

`deploy/docker/docker-compose.yml`, replace the `healthcheck:` block (lines 45-60) with:

```yaml
    healthcheck:
      # Liveness: GET /livez, the same command as the Dockerfile's HEALTHCHECK.
      # The image ships ONLY ca-certificates (no curl or wget), so the request is
      # written by hand over bash's /dev/tcp. `$$l` is compose's escape for the
      # shell's `$l`; a bare `$l` would be interpolated by compose as an empty
      # variable and the container would never become healthy. /livez fails
      # only on the `livez_checks` you select (default none), never on sink
      # health or a slow recovery.
      test: ["CMD", "/bin/bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\\r\\n\\r\\n' >&3 && read -r l <&3 && [[ $$l == *' 200 '* ]]"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 10s
```

`deploy/monitoring/docker-compose.yml`, in `x-weir-base`, replace the `healthcheck:` block (lines 32-37) with:

```yaml
  healthcheck:
    # GET /livez, as the Dockerfile's HEALTHCHECK (`$$l` escapes compose).
    test: ["CMD", "/bin/bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\\r\\n\\r\\n' >&3 && read -r l <&3 && [[ $$l == *' 200 '* ]]"]
    interval: 5s
    timeout: 3s
    retries: 5
    start_period: 10s
```

`.github/workflows/ci.yml`, both test lines become (the second keeps its trailing `--all-features`):

```yaml
      - run: cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test deploy --test load --test load_tls --test tls_client --test tls_listener
```
```yaml
      - run: cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test deploy --test load --test load_tls --test tls_client --test tls_listener --all-features
```

`.github/workflows/docker.yml`, in the `smoke` job's script, insert after the `/metrics` check loop (after the `done` that closes `for metric in ...`, before `# Confirm WAB segments were written.`):

```bash
          # The image's own HEALTHCHECK (GET /livez) must report healthy: this is
          # the only place the shipped command runs inside the shipped image.
          echo "Waiting for the HEALTHCHECK to report healthy…"
          for i in $(seq 1 40); do
            status=$(docker inspect -f '{{.State.Health.Status}}' weir-smoke)
            [ "$status" = healthy ] && break
            if [ "$i" -eq 40 ]; then
              echo "ERROR: container health is '$status' after 40 s"
              docker inspect -f '{{json .State.Health}}' weir-smoke
              docker logs weir-smoke
              exit 1
            fi
            sleep 1
          done
          echo "Container healthy."
```

(`{{...}}` without a leading `$` is not an Actions expression, so it reaches `docker inspect` unchanged.)

`docs/security/container.md`, replace the body of `### Healthcheck` (lines 187-202) with:

```markdown
`HEALTHCHECK` sends `GET /livez` to the metrics port (9185) over bash's
`/dev/tcp` virtual file (no curl or wget in the image) and is healthy only on
a `200` status line. `/livez` answers 200 whenever the daemon's runtime is
serving HTTP: a few milliseconds after start, all through WAB recovery, and
while shutting down. It fails only on the checks you select with
`livez_checks` (default: none), each a state a restart fixes
(`no_live_shard`, `listener_down`, `worker_exited`, and the opt-ins
`drain_stopped`, `drain_wedged`). It never fails because the sink is down or
the drain is paused, retrying or blocked, so Docker never restarts weir for a
slow downstream.

It is a liveness check, not readiness. "Can this daemon take ingest now?" is
`GET /readyz`; see [Health and status endpoints](../monitoring.md#health-and-status-endpoints).

The defaults (10 s interval, 3 s timeout, 10 s start period, 3 retries) suit
`docker run` and compose. The command hard-codes port 9185: if you change
`metrics_port`, override `HEALTHCHECK` (or compose's `healthcheck.test`) with
the new port. Kubernetes ignores the image's `HEALTHCHECK`; use the probe
recipe in monitoring.md instead.
```

- [ ] **Step 4: Run to verify they pass.**
  `cargo test -p weir-server --test deploy -- --test-threads=1` → 3 PASS.
  `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.

- [ ] **Step 5: Verify by mutation.** Revert each with `git checkout -- <file>` after its run.
  1. `deploy/docker/docker-compose.yml`: `$$l` → `$l`. `every_container_healthcheck_probes_livez_the_same_way` FAILS with "uses a bare `$`".
  2. `deploy/monitoring/docker-compose.yml`: `/livez` → `/metrics`. Same test FAILS ("differs from the canonical /livez probe").
  3. `deploy/docker/Dockerfile`: `*' 200 '*` → `*' 503 '*`. `the_image_healthcheck_passes_against_a_live_daemon` FAILS and `every_container_healthcheck_probes_livez_the_same_way` FAILS.
  4. `.github/workflows/ci.yml`: remove `--test deploy` from the `--all-features` line. `the_deploy_tests_run_in_ci` FAILS.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/deploy.rs crates/weir-server/tests/docs_drift.rs deploy/docker/Dockerfile deploy/docker/docker-compose.yml deploy/monitoring/docker-compose.yml .github/workflows/ci.yml .github/workflows/docker.yml docs/security/container.md`
  `git commit -m "fix(deploy): the container healthcheck asks /livez instead of opening a TCP connection"`

---
### Task 8.3: `weir-readiness.sh` on `/readyz` semantics (HTTP path)

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

The script today (149 lines) is a liveness check over `weir-ctl health` plus readiness gates on `weir_sink_health`, `weir_drain_state` and two lifetime counters (lines 94-131). Spec §12: readiness means "can accept ingest", decided by `/readyz`; the old gates become WARN lines; exit codes align with `weir-ctl status` (0/1/2/3). This task rewrites the script with the HTTP-only decision path; Task 8.4 adds the `weir-ctl status` path in front of it.

**Reconciled with the spec.** §12 says "otherwise use `GET /readyz` plus `/status`". `/status` is JSON, and the script must not grow a `jq` dependency (it runs on bare hosts that have only bash, curl and awk). The active conditions come instead from `weir_condition_active{code,severity}` on `/metrics`, which the daemon's 5 s poll task publishes from the same `weir_status::evaluate` call (§5.11 step 6). The verdict itself still comes from `/readyz`, which is evaluated live on every request, so exit 1 is exactly "`/readyz` is 503".

**Files:**
- Modify (rewrite): `deploy/systemd/weir-readiness.sh` (all 149 lines)
- Test: `crates/weir-server/tests/deploy.rs` (append helpers and tests)

**Interfaces:**
- Consumes: `GET /readyz` → `200` `text/plain` body `ok\n`, or `503` body `not ready: <code>[, <code>]\n` (PR 5, spec §10); a pre-Foundations daemon answers every path with `200` and `Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8` (`metrics/server.rs:113-118`); `weir_condition_active{code="<code>",severity="<info|warning|critical>"}` with value 0 or 1 for every code (PR 1, spec §13); `weir_sink_info{sink_type="..."} 1` (existing).
- Consumes from Task 8.2: `Stub`, `route`, `TEXT`, `OPENMETRICS`, `run_with_deadline`.
- Produces (operator contract, documented in Task 8.4):
  - `weir-readiness.sh [--addr HOST:PORT] [--ctl PATH] [--admin-socket PATH] [--wab-dir DIR] [--fail-on-critical] [--allow-noop] [--socket PATH (ignored)]`
  - exit 0 READY, 1 NOT READY (iff `/readyz` 503), 2 DEAD, 3 READY with an active critical condition and `--fail-on-critical`
  - output lines `READY: ...`, `NOT READY: <codes>`, `DEAD: ...` (stderr), `WARN: <code> (<severity>) - see docs/monitoring.md#cond-<code>`
- Produces (tests, `deploy.rs`): `const READINESS: &str`, `fn readiness(port: u16, extra: &[&str], env: &[(&str, &str)]) -> (i32, String)`, `fn exposition(active: &[(&str, &str)]) -> String`, `fn ready_daemon(active: &[(&str, &str)]) -> Stub`.

- [ ] **Step 1: Write the failing tests.** Append to `crates/weir-server/tests/deploy.rs`:

```rust
// ── weir-readiness.sh ─────────────────────────────────────────────────────────

const READINESS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../deploy/systemd/weir-readiness.sh");

/// Runs the shipped readiness script against `127.0.0.1:<port>` with no
/// weir-ctl (`--ctl` points nowhere; a later `--ctl` in `extra` wins). Returns
/// the exit code and stdout+stderr.
fn readiness(port: u16, extra: &[&str], env: &[(&str, &str)]) -> (i32, String) {
    assert!(
        Command::new("curl")
            .arg("--version")
            .stdout(Stdio::null())
            .status()
            .is_ok_and(|s| s.success()),
        "these tests need curl on PATH: it is the script's documented dependency"
    );
    let mut cmd = Command::new("bash");
    cmd.arg(READINESS)
        .arg("--addr")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--ctl")
        .arg("/nonexistent/weir-ctl")
        .args(extra);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let (status, out) = run_with_deadline(&mut cmd, Duration::from_secs(20));
    let status = status.unwrap_or_else(|| panic!("weir-readiness.sh ran past 20 s:\n{out}"));
    (status.code().expect("weir-readiness.sh was killed by a signal"), out)
}

/// A metrics body with `active` as `(code, severity)` conditions at 1, one
/// inactive condition at 0, and a sink type.
fn exposition(active: &[(&str, &str)]) -> String {
    let mut s = String::from("# TYPE weir_condition_active gauge\n");
    for (code, severity) in active {
        s.push_str(&format!(
            "weir_condition_active{{code=\"{code}\",severity=\"{severity}\"}} 1\n"
        ));
    }
    s.push_str("weir_condition_active{code=\"starting\",severity=\"info\"} 0\n");
    s.push_str("# TYPE weir_sink_info gauge\nweir_sink_info{sink_type=\"http\"} 1\n# EOF\n");
    s
}

/// A daemon whose /readyz says ok, with `active` conditions on /metrics.
fn ready_daemon(active: &[(&str, &str)]) -> Stub {
    Stub::start(vec![
        route("/readyz", 200, TEXT, "ok\n"),
        route("/metrics", 200, OPENMETRICS, exposition(active)),
    ])
}

#[test]
fn readiness_is_ready_when_readyz_answers_200() {
    let stub = ready_daemon(&[]);
    let (code, out) = readiness(stub.port, &[], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("READY: /readyz ok, sink=http"), "{out}");
}

#[test]
fn readiness_is_not_ready_exactly_when_readyz_answers_503() {
    let stub = Stub::start(vec![
        route("/readyz", 503, TEXT, "not ready: wab_cap_rejecting, no_live_shard\n"),
        route("/metrics", 200, OPENMETRICS, exposition(&[])),
    ]);
    let (code, out) = readiness(stub.port, &[], &[]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("NOT READY: wab_cap_rejecting, no_live_shard"), "{out}");
}

/// The behaviour change: sink, drain and history conditions are reported, but
/// a daemon that can take ingest is READY. The old script exited 1 on each of
/// these, and stayed at 1 forever after one recovered flusher panic.
#[test]
fn a_sink_outage_is_a_warning_not_a_readiness_failure() {
    let stub = ready_daemon(&[
        ("sink_down", "warning"),
        ("drain_blocked_dead_letter_full", "critical"),
        ("flusher_respawned_recent", "warning"),
    ]);
    let (code, out) = readiness(stub.port, &[], &[]);
    assert_eq!(code, 0, "{out}");
    for c in ["sink_down", "drain_blocked_dead_letter_full", "flusher_respawned_recent"] {
        assert!(
            out.contains(&format!("WARN: {c} (")) && out.contains(&format!("#cond-{c}")),
            "no WARN line with a runbook link for {c}:\n{out}"
        );
    }
}

#[test]
fn a_critical_condition_turns_ready_into_exit_3_only_on_request() {
    let stub = ready_daemon(&[("shard_offline", "critical")]);
    assert_eq!(readiness(stub.port, &[], &[]).0, 0);
    let (code, out) = readiness(stub.port, &["--fail-on-critical"], &[]);
    assert_eq!(code, 3, "{out}");

    let warning_only = ready_daemon(&[("sink_down", "warning")]);
    let (code, out) = readiness(warning_only.port, &["--fail-on-critical"], &[]);
    assert_eq!(code, 0, "a warning must never yield exit 3:\n{out}");
}

#[test]
fn allow_noop_silences_only_the_noop_warning() {
    let stub = ready_daemon(&[("noop_sink", "warning"), ("sink_down", "warning")]);
    let (_, loud) = readiness(stub.port, &[], &[]);
    assert!(loud.contains("WARN: noop_sink"), "{loud}");
    let (code, quiet) = readiness(stub.port, &["--allow-noop"], &[]);
    assert_eq!(code, 0, "{quiet}");
    assert!(!quiet.contains("noop_sink"), "{quiet}");
    assert!(quiet.contains("WARN: sink_down"), "{quiet}");
}

#[test]
fn readiness_is_dead_when_nothing_listens() {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        l.local_addr().expect("addr").port()
    };
    let (code, out) = readiness(port, &[], &[]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("DEAD"), "{out}");
}

#[test]
fn readiness_is_dead_for_an_http_server_that_is_not_weir() {
    let html = Stub::start(vec![route("*", 200, "text/html", "<html>hello</html>")]);
    let (code, out) = readiness(html.port, &[], &[]);
    assert_eq!(code, 2, "{out}");

    // text/plain, but not a /readyz body.
    let plain = Stub::start(vec![route("*", 200, TEXT, "hello\n")]);
    let (code, out) = readiness(plain.port, &[], &[]);
    assert_eq!(code, 2, "{out}");
}

#[test]
fn a_daemon_without_readyz_counts_as_ready_when_it_answers_with_weir_metrics() {
    // Before the health routes, every path returned the metrics.
    let older = Stub::start(vec![route("*", 200, OPENMETRICS, exposition(&[]))]);
    let (code, out) = readiness(older.port, &[], &[]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("has no /readyz"), "{out}");

    let not_weir = Stub::start(vec![route("*", 200, OPENMETRICS, "# TYPE up gauge\nup 1\n# EOF\n")]);
    let (code, out) = readiness(not_weir.port, &[], &[]);
    assert_eq!(code, 2, "{out}");
}
```

- [ ] **Step 2: Run them to verify they fail.**
  `cargo test -p weir-server --test deploy -- --test-threads=1`
  Expected: `readiness_is_ready_when_readyz_answers_200`, `readiness_is_not_ready_exactly_when_readyz_answers_503`, `a_sink_outage_is_a_warning_not_a_readiness_failure`, `a_critical_condition_turns_ready_into_exit_3_only_on_request`, `allow_noop_silences_only_the_noop_warning` and `a_daemon_without_readyz_counts_as_ready_when_it_answers_with_weir_metrics` FAIL with `left: 2` (the old script runs `weir-ctl health` first, which `/nonexistent/weir-ctl` fails, so it exits DEAD; `--fail-on-critical` is also an unknown arg → 2). The two DEAD tests pass already and pin that the rewrite keeps them.

- [ ] **Step 3: Implement.** Replace `deploy/systemd/weir-readiness.sh` with:

```bash
#!/usr/bin/env bash
# weir-readiness.sh — can this weir daemon accept ingest right now?
#
# READY means exactly what the daemon's own GET /readyz means: recovery is
# done, a producer listener is bound, the daemon is not shutting down, the
# wab_max_bytes cap is not rejecting pushes, and at least one shard can take
# records. Sink health, drain state, dead-letter and quarantine, and fsync or
# flusher-panic history never gate it: weir keeps buffering durably while the
# sink is down, and a recovered panic is history, not a present failure. They
# are printed as WARN lines instead.
#
# Exit codes (the same table as `weir-ctl status`):
#   0  READY
#   1  NOT READY, if and only if /readyz answers 503 (the codes are printed)
#   2  DEAD: unreachable, not a weir daemon, or (via weir-ctl) an identity mismatch
#   3  READY, but a critical condition that does not gate readiness is active
#      (shard_offline, drain_stopped, ...). Only with --fail-on-critical;
#      without it the script exits 0 and prints WARN lines.
#
# How it decides:
#   1. GET /readyz. No answer is DEAD. A daemon from before the health routes
#      existed answers every path with its metrics; reachable then counts as
#      ready, and the script says so.
#   2. A 503 is NOT READY. A 200 is READY, and the active conditions come from
#      weir_condition_active on /metrics (refreshed every 5 s by the daemon,
#      from the same evaluator /readyz and /status use).
#
# Usage:
#   weir-readiness.sh [--addr HOST:PORT] [--ctl PATH] [--admin-socket PATH]
#                     [--wab-dir DIR] [--fail-on-critical] [--allow-noop]
#
#   --socket PATH is accepted and ignored: readiness no longer round-trips the
#   producer socket, because /readyz already reports whether it is bound.
#
# For cron + alerting, a systemd timer, or a bare-metal deploy gate. Kubernetes
# should probe /startupz, /readyz and /livez directly (docs/monitoring.md).
# Requires bash, curl and awk, which the container image does not ship; its
# HEALTHCHECK probes /livez with bash alone.
set -euo pipefail

ADDR="127.0.0.1:9185"
CTL="$(command -v weir-ctl || true)"
ADMIN_SOCKET=""
WAB_DIR=""
FAIL_ON_CRITICAL=0
ALLOW_NOOP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --addr)             ADDR="$2"; shift 2 ;;
    --ctl)              CTL="$2"; shift 2 ;;
    --admin-socket)     ADMIN_SOCKET="$2"; shift 2 ;;
    --wab-dir)          WAB_DIR="$2"; shift 2 ;;
    --socket)           shift 2 ;;
    --fail-on-critical) FAIL_ON_CRITICAL=1; shift ;;
    --allow-noop)       ALLOW_NOOP=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

fail_dead() { echo "DEAD: $*" >&2; exit 2; }

body_file="$(mktemp)"
trap 'rm -f "$body_file"' EXIT

# ── 1. /readyz: reachable? which daemon? ────────────────────────────────────
# No -f: a 503 is an answer, not a failure. %{content_type} is empty when the
# server sends none.
meta="$(curl -sS --max-time 3 -o "$body_file" -w '%{http_code} %{content_type}' \
  "http://${ADDR}/readyz" 2>/dev/null)" \
  || fail_dead "/readyz not reachable at http://${ADDR}/readyz"
code="${meta%% *}"
ctype="${meta#* }"
body="$(cat "$body_file")"

case "$ctype" in
  application/openmetrics-text*)
    # A daemon without the health routes answers every path with its metrics.
    grep -q 'weir_' "$body_file" \
      || fail_dead "http://${ADDR} serves metrics with no weir_* series - wrong target?"
    echo "READY: the daemon at ${ADDR} has no /readyz (an older weir), so answering is all it can report; upgrade it for a real verdict"
    exit 0 ;;
  text/plain*) ;;
  *) fail_dead "http://${ADDR}/readyz answered HTTP ${code} as '${ctype:-no content type}' - not a weir daemon?" ;;
esac

if [[ "$code" == 503 && "$body" == "not ready:"* ]]; then
  ready=0
elif [[ "$code" == 200 && "$body" == ok* ]]; then
  ready=1
else
  fail_dead "http://${ADDR}/readyz answered HTTP ${code} with '${body:0:80}' - not a weir daemon?"
fi

# ── 2. The verdict from /readyz ─────────────────────────────────────────────
if [[ "$ready" -eq 0 ]]; then
  echo "NOT READY: ${body#not ready: }"
  exit 1
fi

# Ready. Report what does not gate readiness but an operator should see.
metrics="$(curl -fsS --max-time 3 "http://${ADDR}/metrics" 2>/dev/null)" \
  || fail_dead "/readyz answered but /metrics did not at http://${ADDR}/metrics"

# Lines look like: weir_condition_active{code="sink_down",severity="warning"} 1
# A gauge may print as 1 or 1.0, so any fraction is stripped before comparing,
# and the label order is not assumed.
active="$(awk '$1 ~ /^weir_condition_active\{/ { v = $2; sub(/\..*/, "", v); if (v + 0 >= 1) print $1 }' <<<"$metrics" \
  | sed -E -e 's/.*code="([a-z0-9_]+)".*severity="([a-z]+)".*/\1 \2/' \
           -e 's/.*severity="([a-z]+)".*code="([a-z0-9_]+)".*/\2 \1/')"

critical=0
while read -r cond sev; do
  if [[ -z "${cond:-}" ]]; then continue; fi
  if [[ "$cond" == noop_sink && "$ALLOW_NOOP" -eq 1 ]]; then continue; fi
  echo "WARN: ${cond} (${sev}) - see docs/monitoring.md#cond-${cond}"
  if [[ "$sev" == critical ]]; then critical=1; fi
done <<<"$active"

sink_type="$(grep -oE 'weir_sink_info\{sink_type="[a-z0-9_]+"\} 1' <<<"$metrics" \
  | sed -E 's/.*sink_type="([a-z0-9_]+)".*/\1/' || true)"

if [[ "$critical" -eq 1 && "$FAIL_ON_CRITICAL" -eq 1 ]]; then
  echo "READY, but a critical condition is active (exit 3: --fail-on-critical)"
  exit 3
fi
echo "READY: /readyz ok, sink=${sink_type:-?}"
exit 0
```

`ADMIN_SOCKET`, `WAB_DIR` and `CTL` are parsed here and used from Task 8.4.

- [ ] **Step 4: Run to verify they pass.**
  `cargo test -p weir-server --test deploy -- --test-threads=1` → all PASS (3 from Task 8.2, 8 here).
  `bash -n deploy/systemd/weir-readiness.sh` → no output.
  If `shellcheck` is installed: `shellcheck deploy/systemd/weir-readiness.sh` → no findings (it is not a CI dependency; nothing is added for it).

- [ ] **Step 5: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 6: Commit.**
  `git add deploy/systemd/weir-readiness.sh crates/weir-server/tests/deploy.rs`
  `git commit -m "fix(deploy): weir-readiness.sh reports /readyz, and sink or panic history only warns"`

---

### Task 8.4: `weir-readiness.sh` defers to `weir-ctl status`, and the docs say what it now means

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

When a `weir-ctl` with a `status` subcommand is installed, its exit code is the verdict (spec §12 "If weir-ctl is present, run `weir-ctl status` and map its exit code"): it reads the same evaluator through the identity-checked admin socket or `/status`, and its table is the script's table. `/readyz` still runs first, so an unreachable daemon is DEAD without asking weir-ctl (weir-ctl's offline fallback would otherwise turn a dead daemon into "NOT READY: daemon_unreachable", exit 1).

**Reconciled with the spec.** §12 names `weir-ctl status --json`. The script maps only the exit code and relays the human output, so it runs `weir-ctl status` without `--json` and needs no JSON parser. An older `weir-ctl` without `status` exits 2 on `status --help` (clap's unknown-subcommand status); the script detects that and falls back to `/readyz`.

**Files:**
- Modify: `deploy/systemd/weir-readiness.sh` (insert the weir-ctl block between section 1 and section 2; update the header's "How it decides")
- Test: `crates/weir-server/tests/deploy.rs` (append)
- Modify: `deploy/systemd/README.md:172-193` (`## Readiness probe (optional)`)
- Modify: `docs/getting-started/install.md:229-230` (the `weir.service` bullet's `TimeoutStopSec` clause) and `:238-239` (the `weir-readiness.sh` bullet)

**Interfaces:**
- Consumes: `weir-ctl status [--admin-socket P] [--addr H:P] [--wab-dir D]` exiting 0 ready / 1 not ready (including a dead daemon whose WAB was read from disk) / 2 no daemon answered and no disk facts, or identity mismatch / 3 ready with a critical condition (PR 7, spec §14, decision Q9); `weir-ctl status --help` exits 0 on a weir-ctl that has the subcommand (clap). Live daemon `/readyz` (PR 5).
- Consumes from earlier tasks: `readiness`, `ready_daemon`, `exposition`, `Stub`, `route`, `TEXT`, `OPENMETRICS`, `weir_server!`.
- Produces (tests): `fn stub_ctl(tag: &str, has_status: bool) -> std::path::PathBuf`.
- Produces (operator contract): weir-ctl is invoked as `weir-ctl status --addr <ADDR> [--admin-socket <P>] [--wab-dir <D>]`; its exit 0/1/2/3 map to the script's 0/1/2/(3 with `--fail-on-critical`, else 0); any other exit is DEAD; `--allow-noop` removes lines mentioning `noop_sink` from its relayed output.

- [ ] **Step 1: Write the failing tests.** Append to `crates/weir-server/tests/deploy.rs` (and add `use std::os::unix::fs::PermissionsExt;` and `use std::path::PathBuf;` to the imports):

```rust
/// A fake `weir-ctl` that echoes its argv and then prints `$STUB_OUT` and
/// exits `$STUB_RC` for `status`. `has_status: false` imitates a weir-ctl
/// from before the subcommand existed: clap exits 2 on `status --help`.
fn stub_ctl(tag: &str, has_status: bool) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("weir-deploy-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("stub dir");
    let path = dir.join("weir-ctl");
    let help_rc = if has_status { 0 } else { 2 };
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\n\
             if [ \"$1\" = status ] && [ \"$2\" = --help ]; then exit {help_rc}; fi\n\
             if [ \"$1\" = status ]; then echo \"args: $*\"; printf '%s\\n' \"$STUB_OUT\"; exit \"$STUB_RC\"; fi\n\
             exit 64\n"
        ),
    )
    .expect("write stub weir-ctl");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    path
}

#[test]
fn weir_ctl_status_decides_the_verdict_when_installed() {
    let stub = ready_daemon(&[]);
    let ctl = stub_ctl("verdict", true);
    let ctl = ctl.to_str().expect("utf-8 temp path");
    let addr = format!("127.0.0.1:{}", stub.port);
    // (weir-ctl exit, script exit, script exit with --fail-on-critical)
    for (rc, plain, strict) in [("0", 0, 0), ("1", 1, 1), ("2", 2, 2), ("3", 0, 3), ("4", 2, 2)] {
        let env = [("STUB_RC", rc), ("STUB_OUT", "verdict from weir-ctl")];
        let (code, out) = readiness(stub.port, &["--ctl", ctl], &env);
        assert_eq!(code, plain, "weir-ctl exited {rc}:\n{out}");
        assert!(out.contains(&format!("args: status --addr {addr}")), "{out}");
        assert!(out.contains("verdict from weir-ctl"), "weir-ctl's output was not relayed:\n{out}");
        let (code, out) = readiness(stub.port, &["--ctl", ctl, "--fail-on-critical"], &env);
        assert_eq!(code, strict, "weir-ctl exited {rc}, --fail-on-critical:\n{out}");
    }
}

#[test]
fn weir_ctl_gets_the_admin_socket_and_wab_dir_it_was_given() {
    let stub = ready_daemon(&[]);
    let ctl = stub_ctl("args", true);
    let (code, out) = readiness(
        stub.port,
        &["--ctl", ctl.to_str().unwrap(), "--admin-socket", "/x/wab.admin.sock", "--wab-dir", "/x/wab"],
        &[("STUB_RC", "0"), ("STUB_OUT", "READY")],
    );
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains(&format!(
            "args: status --addr 127.0.0.1:{} --admin-socket /x/wab.admin.sock --wab-dir /x/wab",
            stub.port
        )),
        "{out}"
    );
}

#[test]
fn allow_noop_filters_weir_ctl_output_too() {
    let stub = ready_daemon(&[]);
    let ctl = stub_ctl("noop", true);
    let env = [("STUB_RC", "0"), ("STUB_OUT", "READY\nwarning noop_sink: records are acked and DISCARDED")];
    let (code, out) = readiness(stub.port, &["--ctl", ctl.to_str().unwrap(), "--allow-noop"], &env);
    assert_eq!(code, 0, "{out}");
    assert!(!out.contains("noop_sink"), "{out}");
    assert!(out.contains("READY"), "{out}");
}

#[test]
fn an_unreachable_daemon_is_dead_without_asking_weir_ctl() {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        l.local_addr().expect("addr").port()
    };
    let ctl = stub_ctl("dead", true);
    // A weir-ctl that would answer "not ready" from disk must not turn a dead
    // daemon into exit 1.
    let (code, out) = readiness(
        port,
        &["--ctl", ctl.to_str().unwrap()],
        &[("STUB_RC", "1"), ("STUB_OUT", "NOT READY: daemon_unreachable")],
    );
    assert_eq!(code, 2, "{out}");
    assert!(!out.contains("args: status"), "weir-ctl was consulted for a dead daemon:\n{out}");
}

#[test]
fn an_older_weir_ctl_without_status_falls_back_to_readyz() {
    let stub = Stub::start(vec![
        route("/readyz", 503, TEXT, "not ready: starting\n"),
        route("/metrics", 200, OPENMETRICS, exposition(&[])),
    ]);
    let ctl = stub_ctl("old", false);
    let (code, out) = readiness(
        stub.port,
        &["--ctl", ctl.to_str().unwrap()],
        &[("STUB_RC", "0"), ("STUB_OUT", "unused")],
    );
    assert_eq!(code, 1, "the verdict must come from /readyz:\n{out}");
    assert!(out.contains("NOT READY: starting"), "{out}");
}

#[test]
fn the_readiness_script_reports_a_live_daemon_ready() {
    let srv = weir_server!("readiness_live").start();
    // wait_ready returns once the socket accepts; `serving` is stored a moment later.
    let deadline = Instant::now() + Duration::from_secs(5);
    let (code, out) = loop {
        let (code, out) = readiness(srv.metrics_port, &[], &[]);
        if code == 0 || Instant::now() >= deadline {
            break (code, out);
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("READY: /readyz ok"), "{out}");
}
```

- [ ] **Step 2: Run them to verify they fail.**
  `cargo test -p weir-server --test deploy -- --test-threads=1`
  Expected: `weir_ctl_status_decides_the_verdict_when_installed` FAILS (`weir-ctl exited 1: left: 0, right: 1`; the script never runs weir-ctl), `weir_ctl_gets_the_admin_socket_and_wab_dir_it_was_given` and `allow_noop_filters_weir_ctl_output_too` FAIL on the missing `args:` / relayed output. `an_unreachable_daemon_is_dead_without_asking_weir_ctl`, `an_older_weir_ctl_without_status_falls_back_to_readyz` and `the_readiness_script_reports_a_live_daemon_ready` pass already and pin the ordering and the end-to-end path.

- [ ] **Step 3: Implement.** In `deploy/systemd/weir-readiness.sh`, insert between the `fi` that closes the `ready=0 / ready=1` block and the `# ── 2. The verdict from /readyz` line:

```bash
# ── 2. weir-ctl, when installed, decides ────────────────────────────────────
# `weir-ctl status` reads the same evaluator through the admin socket (with its
# identity checks) or /status, and exits with the table above, so its exit code
# is the verdict. /readyz has already answered, so a dead daemon never reaches
# here: weir-ctl's offline fallback would report it NOT READY instead of DEAD.
# A weir-ctl from before `status` existed exits 2 on `status --help` (clap);
# the /readyz verdict below is used for it.
if [[ -n "$CTL" && -x "$CTL" ]] && "$CTL" status --help >/dev/null 2>&1; then
  args=(status --addr "$ADDR")
  if [[ -n "$ADMIN_SOCKET" ]]; then args+=(--admin-socket "$ADMIN_SOCKET"); fi
  if [[ -n "$WAB_DIR" ]]; then args+=(--wab-dir "$WAB_DIR"); fi
  rc=0
  out="$("$CTL" "${args[@]}" 2>&1)" || rc=$?
  if [[ "$ALLOW_NOOP" -eq 1 ]]; then out="$(grep -v 'noop_sink' <<<"$out" || true)"; fi
  if [[ -n "$out" ]]; then printf '%s\n' "$out"; fi
  case "$rc" in
    0) exit 0 ;;
    1) exit 1 ;;
    3) if [[ "$FAIL_ON_CRITICAL" -eq 1 ]]; then exit 3; fi
       echo "WARN: a critical condition is active (pass --fail-on-critical to exit 3)"
       exit 0 ;;
    *) fail_dead "weir-ctl status exited ${rc} (unreachable, or the daemon failed weir-ctl's identity check)" ;;
  esac
fi
```

Renumber the following section header to `# ── 3. The verdict from /readyz (no weir-ctl) ───────────────────────────────`, and replace the header's "How it decides" block with:

```bash
# How it decides:
#   1. GET /readyz. No answer is DEAD. A daemon from before the health routes
#      existed answers every path with its metrics; reachable then counts as
#      ready, and the script says so.
#   2. If a weir-ctl with a `status` subcommand is installed (PATH, or --ctl),
#      its exit code is the verdict; its output is relayed. It adds the admin
#      socket's identity checks and the admin view's reasons.
#   3. Otherwise a 503 is NOT READY, and a 200 is READY with the active
#      conditions read from weir_condition_active on /metrics (refreshed every
#      5 s by the daemon, from the same evaluator /readyz and /status use).
```

`deploy/systemd/README.md`, replace the whole `## Readiness probe (optional)` section with:

````markdown
## Readiness probe (optional)

`weir-readiness.sh` answers one question: can this daemon accept ingest right
now? The verdict is the daemon's own `GET /readyz`: recovery done, a producer
listener bound, not shutting down, the `wab_max_bytes` cap not rejecting, and
at least one shard up. When a `weir-ctl` with a `status` subcommand is
installed, the script asks `weir-ctl status` instead, which reads the same
evaluator through the admin socket and checks that it is talking to the daemon
that owns this `wab_dir`.

| Exit | Meaning |
|---|---|
| `0` | READY |
| `1` | NOT READY, exactly when `/readyz` answers 503; the failing codes are printed |
| `2` | DEAD: unreachable, not a weir daemon, or an identity mismatch |
| `3` | READY, but a critical condition that does not gate readiness is active; only with `--fail-on-critical` |

Sink health, a blocked or paused drain, dead-letter and quarantine, and fsync
or flusher-panic history do not make it NOT READY. weir keeps buffering
durably through all of them, and the old gates on lifetime counters stayed NOT
READY until a restart after one recovered panic. They are printed as `WARN:`
lines naming the condition and its runbook. `--allow-noop` silences the
`noop_sink` warning.

```bash
sudo install -m0755 weir-readiness.sh /usr/local/bin/weir-readiness.sh
weir-readiness.sh --addr 127.0.0.1:9185                       # the verdict
weir-readiness.sh --addr 127.0.0.1:9185 --fail-on-critical    # exit 3 on a critical condition
```

`--socket` is still accepted and ignored. The script needs `bash`, `curl` and
`awk` (standard on a bare-metal host, absent from the container image, whose
`HEALTHCHECK` probes `/livez` with bash alone). Run it from cron, a systemd
timer or a deploy gate. For Kubernetes, probe `/startupz`, `/readyz` and
`/livez` directly: see [Health and status
endpoints](../../docs/monitoring.md#health-and-status-endpoints). To reach the
port remotely set `metrics_bind = "0.0.0.0"` **and firewall it**: the listener
is unauthenticated.
````

`docs/getting-started/install.md`, replace the two-line bullet starting `- \`weir-readiness.sh\` — a liveness+readiness probe` with:

```markdown
- `weir-readiness.sh` — a readiness check: the daemon's own `/readyz` (or
  `weir-ctl status` when installed), with the same exit codes as `weir-ctl status`.
```

and, in the `weir.service` bullet above it, replace `(\`TimeoutStopSec\` aligned to the` / `  daemon's \`shutdown_timeout_secs\`)` with:

```markdown
(`TimeoutStopSec=45`: at least
  `shutdown_timeout_secs + drain_shutdown_budget_secs + 5`)
```

- [ ] **Step 4: Run to verify they pass.**
  `cargo test -p weir-server --test deploy -- --test-threads=1` → all PASS (17).
  `bash -n deploy/systemd/weir-readiness.sh` → no output.

- [ ] **Step 5: Verify the pin by mutation.** In the script, change `1) exit 1 ;;` in the weir-ctl `case` to `1) exit 0 ;;`; `weir_ctl_status_decides_the_verdict_when_installed` FAILS. Move the weir-ctl block above section 1; `an_unreachable_daemon_is_dead_without_asking_weir_ctl` FAILS. Revert both.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add deploy/systemd/weir-readiness.sh deploy/systemd/README.md docs/getting-started/install.md crates/weir-server/tests/deploy.rs`
  `git commit -m "feat(deploy): weir-readiness.sh defers to weir-ctl status when it is installed"`

---
### Task 8.5: `WeirNotReady` and `WeirConditionCritical`, with runbooks, and a guard that alert condition codes are real

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Two rules read the daemon's own verdicts (spec §16 "Docs and alerts"). `WeirNotReady` uses the exact expression the spec gives; its promtool test includes the inactive phase series present at 0 and a serving case. `WeirConditionCritical` pages once per active critical code that has no dedicated rule. A docs_drift guard checks every `weir_condition_active{code...}` matcher in the rules names a real code (spec §9.3 Contract).

**Files:**
- Modify: `deploy/prometheus/weir-alerts.yml` (new group `weir-health` between `weir-durability` and `weir-drain`, i.e. before line 107)
- Test: `deploy/prometheus/weir-alerts_test.yml` (append three tests)
- Test: `crates/weir-server/tests/docs_drift.rs` (one helper, one test)
- Modify: `docs/monitoring.md` (new `### Readiness and conditions` subsection under `## Alerts & runbook`, after the Durability subsection, i.e. before `### Drain / dead-letter`)

**Interfaces:**
- Consumes: gauges `weir_ready` (0/1, PR 5), `weir_startup_phase{phase}` one-hot over `initializing|recovering|handoff|binding|serving|shutting_down`, every label set pre-created (PR 1/4), `weir_condition_active{code,severity}` (PR 1). `weir_status::ALL: &'static [weir_status::ConditionCode]` and `ConditionCode: serde::Serialize` as a snake_case string (PR 1, spec §5.1). PR 1 Task 1.1 exports it as `weir_status::ALL` (30 codes; the offline-only two are in `weir_status::OFFLINE_ONLY`).
- Produces: alert rules `WeirNotReady` (critical, `for: 5m`), `WeirConditionCritical` (critical, `for: 2m`, labels `job, instance, code`); runbook anchors `#weirnotready`, `#weirconditioncritical`; docs_drift `fn wire_name(v: serde_json::Value) -> String`, `fn condition_code_names() -> Vec<String>`, test `every_condition_code_an_alert_names_is_real`.

- [ ] **Step 1: Write the failing tests.** Append to `deploy/prometheus/weir-alerts_test.yml`:

```yaml
  - interval: 1m
    name: serving but not ready fires WeirNotReady - the inactive phases are present at 0
    input_series:
      - series: 'weir_ready{job="weir",instance="w:9185"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="initializing"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="recovering"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="handoff"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="binding"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="serving"}'
        values: "1x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="shutting_down"}'
        values: "0x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirNotReady
        exp_alerts:
          - exp_labels:
              instance: "w:9185"
              job: "weir"
              severity: "critical"
            exp_annotations:
              summary: "weir on w:9185 is serving but NOT READY for new ingest"
              description: "/readyz has answered 503 for 5 minutes outside startup and shutdown: new connections are refused or pushes nack. The failing check is in `curl -s <host>:9185/readyz?verbose` or `weir-ctl status`: wab_cap_rejecting (delivery is behind and the WAB hit wab_max_bytes), no_live_shard or listener_down (restart after capturing the logs)."
              runbook: "docs/monitoring.md#weirnotready"

  - interval: 1m
    name: a long recovery and a shutdown are not-ready by design and do not fire
    input_series:
      - series: 'weir_ready{job="weir",instance="r:9185"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="r:9185",phase="recovering"}'
        values: "1x10"
      - series: 'weir_startup_phase{job="weir",instance="r:9185",phase="serving"}'
        values: "0x10"
      - series: 'weir_ready{job="weir",instance="s:9185"}'
        values: "0x10"
      - series: 'weir_startup_phase{job="weir",instance="s:9185",phase="shutting_down"}'
        values: "1x10"
      - series: 'weir_startup_phase{job="weir",instance="s:9185",phase="serving"}'
        values: "0x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirNotReady
        exp_alerts: []

  - interval: 1m
    name: a ready daemon and a short not-ready blip do not fire WeirNotReady
    input_series:
      - series: 'weir_ready{job="weir",instance="w:9185"}'
        values: "1 1 1 0 0 0 1 1 1 1 1"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="serving"}'
        values: "1x10"
      - series: 'weir_startup_phase{job="weir",instance="w:9185",phase="shutting_down"}'
        values: "0x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirNotReady
        exp_alerts: []

  - interval: 1m
    name: a critical condition fires per code - not for codes with their own rule, not for warnings
    input_series:
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="shard_offline",severity="critical"}'
        values: "1x10"
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="drain_stopped",severity="critical"}'
        values: "1x10"
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="sink_down",severity="warning"}'
        values: "1x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirConditionCritical
        exp_alerts:
          - exp_labels:
              code: "shard_offline"
              instance: "w:9185"
              job: "weir"
              severity: "critical"
            exp_annotations:
              summary: "weir on w:9185 reports critical condition shard_offline"
              description: "The daemon's own condition evaluator reports shard_offline, a critical condition with no dedicated rule in this file. `weir-ctl status` on the host shows it with its evidence, and the admin view adds the reason. Runbook for this code: docs/monitoring.md#cond-shard_offline."
              runbook: "docs/monitoring.md#weirconditioncritical"

  - interval: 1m
    name: no active critical condition does not fire WeirConditionCritical
    input_series:
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="shard_offline",severity="critical"}'
        values: "0x10"
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="no_live_shard",severity="critical"}'
        values: "0 0 1 0 0 0 0 0 0 0 0"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirConditionCritical
        exp_alerts: []
```

Append to `crates/weir-server/tests/docs_drift.rs`:

```rust
/// The wire name of a serde unit variant (`"shard_offline"`).
fn wire_name(v: serde_json::Value) -> String {
    v.as_str()
        .unwrap_or_else(|| panic!("expected a unit variant serialized as a string, got {v}"))
        .to_string()
}

/// Every condition code the daemon can emit, as the `code` label spells it.
fn condition_code_names() -> Vec<String> {
    let names: Vec<String> = weir_status::ALL
        .iter()
        .map(|c| wire_name(serde_json::to_value(c).expect("serialize a ConditionCode")))
        .collect();
    assert!(
        names.len() >= 30,
        "weir_status::ALL has {} codes; the catalogue has 30, so this is no longer \n\
         reading it",
        names.len()
    );
    names
}

/// Every `code` matcher on `weir_condition_active` in the alert rules names a
/// code the daemon emits.
///
/// A rule selecting `code="sink_unreachable"` when the code is `sink_down`
/// matches no series and never fires; `promtool test rules` cannot see that,
/// because its tests feed the series the rule author believed in. Spec §9.3:
/// "every weir_condition_active{code=...} reference in an alert names a real
/// code".
#[test]
fn every_condition_code_an_alert_names_is_real() {
    let known = condition_code_names();
    let mut referenced = Vec::new();
    for (i, _) in ALERTS.match_indices("weir_condition_active{") {
        let body = &ALERTS[i + "weir_condition_active{".len()..];
        let selector = &body[..body.find('}').expect("unterminated selector")];
        for matcher in selector.split(',') {
            let m = matcher.trim();
            let Some(rest) = m.strip_prefix("code") else { continue };
            let value = rest
                .trim_start_matches(['=', '!', '~'])
                .trim()
                .trim_matches('"');
            referenced.extend(value.split('|').map(str::to_string));
        }
    }
    assert!(
        !referenced.is_empty(),
        "no rule in weir-alerts.yml selects weir_condition_active by code; \n\
         WeirConditionCritical and WeirAdminUnavailable both do"
    );
    let unknown: Vec<&String> = referenced.iter().filter(|c| !known.contains(c)).collect();
    assert!(
        unknown.is_empty(),
        "weir-alerts.yml selects condition codes the daemon never emits: {unknown:?}\n\
         known codes: {known:?}"
    );
}
```

- [ ] **Step 2: Run them to verify they fail.**
  `docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/r:ro" -w /r prom/prometheus:v3.14.0 test rules weir-alerts_test.yml`
  Expected: `FAILED` for the two firing tests (`alertname: WeirNotReady, time: 10m, exp:[...], got:[]` and the same for `WeirConditionCritical`).
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 every_condition_code_an_alert_names_is_real` → FAIL: "no rule in weir-alerts.yml selects weir_condition_active by code".

- [ ] **Step 3: Implement.** In `deploy/prometheus/weir-alerts.yml`, insert before `  # ── Drain / dead-letter` (line 107):

```yaml
  # ── Readiness and conditions — the daemon's own verdicts ────────────────────
  - name: weir-health
    rules:
      # weir_ready is /readyz as a gauge (1 = ready). It is 0 by design while
      # starting and while shutting down, so those phases are excluded: a long
      # recovery shows on weir_startup_phase, and a stop is not an incident.
      # weir_startup_phase is one-hot with every phase pre-created, so the
      # inactive phases are PRESENT at 0: the `== 1` inside the `unless` is
      # load-bearing, without it any instance with the family would be excluded.
      - alert: WeirNotReady
        expr: |
          weir_ready{job=~"weir"} == 0
          unless on(instance, job)
          (weir_startup_phase{job=~"weir", phase=~"initializing|recovering|handoff|binding|shutting_down"} == 1)
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "weir on {{ $labels.instance }} is serving but NOT READY for new ingest"
          description: "/readyz has answered 503 for 5 minutes outside startup and shutdown: new connections are refused or pushes nack. The failing check is in `curl -s <host>:9185/readyz?verbose` or `weir-ctl status`: wab_cap_rejecting (delivery is behind and the WAB hit wab_max_bytes), no_live_shard or listener_down (restart after capturing the logs)."
          runbook: "docs/monitoring.md#weirnotready"

      # One alert per active critical condition code. The three codes excluded
      # here have their own rules (WeirDrainStopped, WeirDrainBlocked,
      # WeirFsyncFailure), so they would otherwise page twice. `max by` drops
      # the series' own severity label before the rule sets it.
      - alert: WeirConditionCritical
        expr: |
          max by (job, instance, code) (
            weir_condition_active{job=~"weir", severity="critical", code!~"drain_stopped|drain_blocked_dead_letter_full|fsync_failures_recent"}
          ) == 1
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "weir on {{ $labels.instance }} reports critical condition {{ $labels.code }}"
          description: "The daemon's own condition evaluator reports {{ $labels.code }}, a critical condition with no dedicated rule in this file. `weir-ctl status` on the host shows it with its evidence, and the admin view adds the reason. Runbook for this code: docs/monitoring.md#cond-{{ $labels.code }}."
          runbook: "docs/monitoring.md#weirconditioncritical"

```

In `docs/monitoring.md`, insert before `### Drain / dead-letter`:

```markdown
### Readiness and conditions

#### WeirNotReady
The daemon reached `serving`, but `/readyz` has answered 503 for 5 minutes:
new connections are refused or pushes nack. Startup and shutdown are
excluded, because not-ready is their normal state; a daemon stuck in a startup
phase shows on `weir_startup_phase` and `/startupz` instead.
**Respond:** `curl -s 127.0.0.1:9185/readyz?verbose` (or `weir-ctl status`)
names the failing check. `wab_cap_rejecting`: delivery is behind and the WAB
reached `wab_max_bytes`; follow the drain and sink conditions, because raising
the cap only buys time. `no_live_shard` or `listener_down`: capture the logs,
then restart. See [Conditions](#conditions) for each code.

#### WeirConditionCritical
The daemon's own evaluator reports a critical condition that no dedicated rule
covers (`drain_stopped`, `drain_blocked_dead_letter_full` and
`fsync_failures_recent` have their own). The `code` label names it, and one
alert fires per code.
**Respond:** open that code's runbook under [Conditions](#conditions)
(`#cond-<code>`), and run `weir-ctl status` on the host: the admin view adds
the reason (panic message, error text) that the public `/status` leaves out.
```

- [ ] **Step 4: Run to verify they pass.**
  The promtool command from Step 2 → `SUCCESS`. Also `... check rules weir-alerts.yml` → `SUCCESS: 24 rules found` (22 at 3adc5f5, which PRs 1-7 do not change, + 2).
  `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS, including `every_alert_rule_has_a_unit_test` (both rules pinned both ways) and `every_alert_runbook_anchor_resolves_to_a_heading`.

- [ ] **Step 5: Verify by mutation.** Revert each after its run.
  1. In `WeirNotReady`, delete `== 1` after the `weir_startup_phase` selector. promtool: the "serving but not ready" test FAILS (got `[]`): the serving instance is excluded because its inactive phase series exist.
  2. In `WeirConditionCritical`, change `code!~"drain_stopped|...` to `code!~"drain_stoped|...`. `every_condition_code_an_alert_names_is_real` FAILS naming `drain_stoped`, and promtool FAILS (an extra `drain_stopped` alert).
  3. Delete `#### WeirNotReady` from monitoring.md. `every_alert_runbook_anchor_resolves_to_a_heading` FAILS.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add deploy/prometheus/weir-alerts.yml deploy/prometheus/weir-alerts_test.yml crates/weir-server/tests/docs_drift.rs docs/monitoring.md`
  `git commit -m "feat(alerts): WeirNotReady and WeirConditionCritical read the daemon's own verdicts"`

---

### Task 8.6: `WeirDrainPausedLong`, `WeirOldestUndeliveredAge`, `WeirDrainBackpressure`, `WeirAdminUnavailable`

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

**Reconciled with the spec.** §16 lists the four rules and fixes only `WeirDrainPausedLong` (1 h) and `WeirDrainBackpressure` (on `weir_wab_handoff_blocked_seconds`). The other thresholds are chosen here and marked `[TUNE]`: `WeirOldestUndeliveredAge` at 3600 s for 15 m (the `delivery_lagging` condition fires at 900 s; the page waits four times longer). `WeirAdminUnavailable` is written on `weir_condition_active{code="admin_unavailable"}`, not on `weir_admin_socket_up == 0`, because `weir_admin_socket_up` is also 0 when `admin_socket_enabled = false`, and that is not an incident.

**Files:**
- Modify: `deploy/prometheus/weir-alerts.yml` (two rules appended to `weir-drain`, one to `weir-ingest`, one to `weir-security`)
- Test: `deploy/prometheus/weir-alerts_test.yml` (append)
- Modify: `docs/monitoring.md` (four `#### ` runbook sections)

**Interfaces:**
- Consumes: gauges `weir_drain_paused` (0/1, PR 2/4), `weir_wab_oldest_undelivered_age_seconds` (poll task's lag scan, PR 1; an upper bound, from the oldest unconfirmed sealed segment header's `created_at`), `weir_wab_handoff_blocked_seconds{shard}` (seconds the shard's flusher has currently been blocked handing off, 0 when not blocked; PR 3), `weir_condition_active{code="admin_unavailable",severity="warning"}` (PR 1/6). Task 8.5's `every_condition_code_an_alert_names_is_real` checks the new `code=` matcher.
- Produces: rules `WeirDrainPausedLong` (warning, `for: 1h`), `WeirOldestUndeliveredAge` (warning, `for: 15m`), `WeirDrainBackpressure` (warning, `for: 10m`), `WeirAdminUnavailable` (warning, `for: 5m`); anchors `#weirdrainpausedlong`, `#weiroldestundeliveredage`, `#weirdrainbackpressure`, `#weiradminunavailable`.

- [ ] **Step 1: Write the failing tests.** Append to `deploy/prometheus/weir-alerts_test.yml`:

```yaml
  - interval: 1m
    name: a pause held over an hour fires WeirDrainPausedLong
    input_series:
      - series: 'weir_drain_paused{job="weir",instance="w:9185"}'
        values: "1x70"
    alert_rule_test:
      - eval_time: 65m
        alertname: WeirDrainPausedLong
        exp_alerts:
          - exp_labels:
              instance: "w:9185"
              job: "weir"
              severity: "warning"
            exp_annotations:
              summary: "weir delivery on w:9185 has been paused for over an hour"
              description: "An operator paused the drain (`weir-ctl drain pause`) and it is still paused. Ingest keeps buffering into the WAB, which grows until wab_max_bytes makes pushes nack, or without bound if wab_max_bytes is 0. Resume with `weir-ctl drain resume`; a restart also resumes delivery."
              runbook: "docs/monitoring.md#weirdrainpausedlong"

  - interval: 1m
    name: a pause under an hour, and no pause, do not fire
    input_series:
      - series: 'weir_drain_paused{job="weir",instance="p:9185"}'
        values: "1x50"
      - series: 'weir_drain_paused{job="weir",instance="w:9185"}'
        values: "0x50"
    alert_rule_test:
      - eval_time: 50m
        alertname: WeirDrainPausedLong
        exp_alerts: []

  - interval: 1m
    name: undelivered data older than an hour fires WeirOldestUndeliveredAge
    input_series:
      - series: 'weir_wab_oldest_undelivered_age_seconds{job="weir",instance="w:9185"}'
        values: "4000+60x30"
    alert_rule_test:
      - eval_time: 30m
        alertname: WeirOldestUndeliveredAge
        exp_alerts:
          - exp_labels:
              instance: "w:9185"
              job: "weir"
              severity: "warning"
            exp_annotations:
              summary: "weir on w:9185 holds undelivered data older than an hour"
              description: "The oldest sealed segment the sink has not confirmed was created over an hour ago (an upper bound on the oldest record's age). Delivery is behind or stuck: `weir-ctl status` shows which (delivery_stalled, sink_down, sink_failing, drain_paused, segments_stranded, or delivery_lagging with commits succeeding)."
              runbook: "docs/monitoring.md#weiroldestundeliveredage"

  - interval: 1m
    name: a young backlog, and a backlog that caught up, do not fire
    input_series:
      - series: 'weir_wab_oldest_undelivered_age_seconds{job="weir",instance="w:9185"}'
        values: "100+0x30"
      - series: 'weir_wab_oldest_undelivered_age_seconds{job="weir",instance="c:9185"}'
        values: "4000x10 0x20"
    alert_rule_test:
      - eval_time: 30m
        alertname: WeirOldestUndeliveredAge
        exp_alerts: []

  - interval: 1m
    name: a hand-off blocked for minutes fires WeirDrainBackpressure
    input_series:
      - series: 'weir_wab_handoff_blocked_seconds{job="weir",instance="w:9185",shard="0"}'
        values: "30x20"
      - series: 'weir_wab_handoff_blocked_seconds{job="weir",instance="w:9185",shard="1"}'
        values: "0x20"
    alert_rule_test:
      - eval_time: 15m
        alertname: WeirDrainBackpressure
        exp_alerts:
          - exp_labels:
              instance: "w:9185"
              job: "weir"
              severity: "warning"
            exp_annotations:
              summary: "weir on w:9185: delivery is stalling ingest (drain hand-off blocked)"
              description: "WAB flushers keep waiting to hand sealed segments to the drain because its channel is full. Acks wait behind the hand-off, so producers see ACK_TIMEOUT nacks of records that ARE durable. This is a delivery problem, not a storage one: check sink commit latency and the sink and drain conditions in `weir-ctl status`."
              runbook: "docs/monitoring.md#weirdrainbackpressure"

  - interval: 1m
    name: no hand-off block, and one brief block, do not fire WeirDrainBackpressure
    input_series:
      - series: 'weir_wab_handoff_blocked_seconds{job="weir",instance="w:9185",shard="0"}'
        values: "0x20"
      - series: 'weir_wab_handoff_blocked_seconds{job="weir",instance="b:9185",shard="0"}'
        values: "0 0 8 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
    alert_rule_test:
      - eval_time: 20m
        alertname: WeirDrainBackpressure
        exp_alerts: []

  - interval: 1m
    name: an enabled admin socket that could not bind fires WeirAdminUnavailable
    input_series:
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="admin_unavailable",severity="warning"}'
        values: "1x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirAdminUnavailable
        exp_alerts:
          - exp_labels:
              instance: "w:9185"
              job: "weir"
              severity: "warning"
            exp_annotations:
              summary: "weir admin socket is unavailable on w:9185"
              description: "admin_socket_enabled is on but the daemon could not bind the admin socket (most often a parent directory that is group- or world-writable, or owned by another user). weir-ctl drain and weir-ctl reload do not work until it is fixed; kill -HUP still reloads TLS, and weir-ctl status falls back to HTTP and disk. The reason code is on /status under admin.reason_code."
              runbook: "docs/monitoring.md#weiradminunavailable"

  - interval: 1m
    name: a bound or deliberately disabled admin socket does not fire WeirAdminUnavailable
    input_series:
      - series: 'weir_condition_active{job="weir",instance="w:9185",code="admin_unavailable",severity="warning"}'
        values: "0x10"
      - series: 'weir_admin_socket_up{job="weir",instance="w:9185"}'
        values: "0x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirAdminUnavailable
        exp_alerts: []
```

- [ ] **Step 2: Run them to verify they fail.**
  promtool `test rules` (command in the header) → `FAILED`: the four firing tests report `got:[]` for `WeirDrainPausedLong`, `WeirOldestUndeliveredAge`, `WeirDrainBackpressure`, `WeirAdminUnavailable`.

- [ ] **Step 3: Implement.** In `deploy/prometheus/weir-alerts.yml`:

Append to the `weir-drain` group's `rules:` (after `WeirDeadLettered`):

```yaml

      # A pause is in-memory and deliberate, so its first hour is not an
      # incident. Past that it is usually a forgotten `weir-ctl drain pause`,
      # and with wab_max_bytes = 0 the WAB grows without bound meanwhile.
      - alert: WeirDrainPausedLong
        expr: weir_drain_paused{job=~"weir"} == 1
        for: 1h
        labels: { severity: warning }
        annotations:
          summary: "weir delivery on {{ $labels.instance }} has been paused for over an hour"
          description: "An operator paused the drain (`weir-ctl drain pause`) and it is still paused. Ingest keeps buffering into the WAB, which grows until wab_max_bytes makes pushes nack, or without bound if wab_max_bytes is 0. Resume with `weir-ctl drain resume`; a restart also resumes delivery."
          runbook: "docs/monitoring.md#weirdrainpausedlong"

      # [TUNE] to your delivery SLO. The gauge is an UPPER bound: it is the age
      # of the oldest unconfirmed sealed segment, from its header's creation
      # time, not of its newest record. The delivery_lagging condition fires
      # at 900 s; this pages at four times that.
      - alert: WeirOldestUndeliveredAge
        expr: weir_wab_oldest_undelivered_age_seconds{job=~"weir"} > 3600
        for: 15m
        labels: { severity: warning }
        annotations:
          summary: "weir on {{ $labels.instance }} holds undelivered data older than an hour"
          description: "The oldest sealed segment the sink has not confirmed was created over an hour ago (an upper bound on the oldest record's age). Delivery is behind or stuck: `weir-ctl status` shows which (delivery_stalled, sink_down, sink_failing, drain_paused, segments_stranded, or delivery_lagging with commits succeeding)."
          runbook: "docs/monitoring.md#weiroldestundeliveredage"
```

Append to the `weir-ingest` group's `rules:` (after `WeirQueueSaturation`):

```yaml

      # weir_wab_handoff_blocked_seconds{shard} is how long that shard's flusher
      # has been waiting to hand a sealed segment to the drain (0 when it is
      # not). The acks of the batch behind it wait too, so producers see
      # ACK_TIMEOUT nacks for records that are already durable. max_over_time
      # bridges the gaps between hand-offs: under steady backpressure each
      # wait ends when the drain takes one segment, and the next begins at the
      # next rotation. A single short block (< 2 m of scrapes) never fires.
      - alert: WeirDrainBackpressure
        expr: max by (job, instance) (max_over_time(weir_wab_handoff_blocked_seconds{job=~"weir"}[2m])) > 5
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: "weir on {{ $labels.instance }}: delivery is stalling ingest (drain hand-off blocked)"
          description: "WAB flushers keep waiting to hand sealed segments to the drain because its channel is full. Acks wait behind the hand-off, so producers see ACK_TIMEOUT nacks of records that ARE durable. This is a delivery problem, not a storage one: check sink commit latency and the sink and drain conditions in `weir-ctl status`."
          runbook: "docs/monitoring.md#weirdrainbackpressure"
```

Append to the `weir-security` group's `rules:` (after `WeirTlsHandshakeFailures`):

```yaml

      # Fires only when the admin socket is ENABLED and could not be bound: the
      # admin_unavailable condition. weir_admin_socket_up alone is also 0 when
      # admin_socket_enabled = false, which is a choice, not an incident.
      - alert: WeirAdminUnavailable
        expr: max by (job, instance) (weir_condition_active{job=~"weir", code="admin_unavailable"}) == 1
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "weir admin socket is unavailable on {{ $labels.instance }}"
          description: "admin_socket_enabled is on but the daemon could not bind the admin socket (most often a parent directory that is group- or world-writable, or owned by another user). weir-ctl drain and weir-ctl reload do not work until it is fixed; kill -HUP still reloads TLS, and weir-ctl status falls back to HTTP and disk. The reason code is on /status under admin.reason_code."
          runbook: "docs/monitoring.md#weiradminunavailable"
```

In `docs/monitoring.md`: append to `### Drain / dead-letter` (after `#### WeirDeadLettered`'s text, before `### Ingest / backpressure`):

```markdown
#### WeirDrainPausedLong
An operator paused delivery with `weir-ctl drain pause` over an hour ago, and
it is still paused. Ingest keeps buffering, so the WAB grows until
`wab_max_bytes` starts nacking pushes, or without bound when the cap is 0.
**Respond:** if the pause is still wanted, silence the alert for the planned
window. Otherwise `weir-ctl drain resume`. A restart also resumes delivery:
a pause lives in memory only.

#### WeirOldestUndeliveredAge
The oldest sealed segment the sink has not confirmed was created over an hour
ago. That is an upper bound on the oldest record's age: it is the segment's
creation time.
**Respond:** `weir-ctl status` tells stuck from slow. Stuck:
`delivery_stalled`, `sink_down`, `sink_failing`, `drain_paused` or
`segments_stranded`; follow that condition's runbook. Slow: only
`delivery_lagging`, with commits succeeding, means ingest outruns the sink;
compare `rate(weir_sink_commit_records_total[5m])` with the ingest rate and add
sink capacity. `[TUNE]` the hour to your delivery objective.
```

Append to `### Ingest / backpressure` (after `#### WeirQueueSaturation`):

```markdown
#### WeirDrainBackpressure
WAB flushers have spent the last ten minutes repeatedly waiting to hand sealed
segments to the drain, whose 256-slot channel is full. The acks of the batch
behind each wait cannot resolve until it ends, so producers see `ACK_TIMEOUT`
nacks for records that are already durable: a delivery problem that looks
like a storage one.
**Respond:** check sink commit latency (`weir_sink_commit_duration_seconds`)
and the sink and drain conditions in `weir-ctl status`. A paused drain
(`drain_paused`) produces exactly this. Producers' retries of the nacked
records will be delivered twice, which at-least-once allows.
```

Append to `### Security / connection` (after `#### WeirTlsHandshakeFailures`):

```markdown
#### WeirAdminUnavailable
`admin_socket_enabled` is on, but the daemon could not bind the admin socket.
The reason is on `/status` under `admin.reason_code`: `parent_insecure` (the
parent directory is group- or world-writable, or owned by a uid other than the
daemon's or root; the daemon refuses rather than let another user plant a
fake control socket there), `path_too_long`, `in_use` (another live socket
already answers at that path) or `bind_failed`.
**Respond:** make the parent directory private (`chmod go-w <dir>`, and
`chown <daemon user> <dir>` if another user owns it), or set
`admin_socket_path` to a private directory, then restart. Until then `weir-ctl drain` and `weir-ctl reload` do not work;
`kill -HUP` still reloads TLS, and `weir-ctl status` falls back to HTTP and
disk. To run without the admin socket on purpose, set
`admin_socket_enabled = false`; that does not fire this alert.
```

- [ ] **Step 4: Run to verify they pass.**
  promtool `check rules` → `SUCCESS: 28 rules found` (Task 8.5's 24 + 4); `test rules` → `SUCCESS`.
  `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.

- [ ] **Step 5: Verify by mutation.** Revert each after its run.
  1. `WeirDrainBackpressure`: change `[2m]` to `[30m]`. The "one brief block" test FAILS: the 8 s spike is held for 30 minutes, past the 10 m `for`. This proves the quiet test pins the window.
  2. `WeirAdminUnavailable`: change the expr to `weir_admin_socket_up{job=~"weir"} == 0`. The "deliberately disabled" test FAILS.
  3. `WeirDrainPausedLong`: `for: 1h` → `for: 30m`. The "under an hour" test FAILS.

- [ ] **Step 6: Commit.**
  `git add deploy/prometheus/weir-alerts.yml deploy/prometheus/weir-alerts_test.yml docs/monitoring.md`
  `git commit -m "feat(alerts): paused delivery, old undelivered data, hand-off backpressure and an unbindable admin socket"`

---

### Task 8.7: `WeirSegmentQuarantined` keys on quarantine bytes

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Today it fires on `increase(weir_recovery_segments_quarantined_total[10m]) > 0`. That counter is per process: a restart resets it, and the alert goes quiet while the quarantined files are still there. `weir_quarantine_bytes_on_disk` survives a restart (its help text says so, `metrics/mod.rs:745-752`) and, from PR 1, is refreshed by the 5 s poll task (spec §13 "Cadence change only"). Spec §16: "`WeirSegmentQuarantined` switched to bytes". The rule now keeps firing until the quarantine is dealt with, the same reasoning the file already applies to `WeirAttestVerifyFailed`.

**Files:**
- Modify: `deploy/prometheus/weir-alerts.yml:56-63` (`WeirSegmentQuarantined`)
- Test: `deploy/prometheus/weir-alerts_test.yml` (the "one durability event fires its rule" test, lines ~204-259, and "a quiet daemon trips no durability rule", lines ~261-289; plus one new test)
- Modify: `docs/monitoring.md:108-124` (`#### WeirSegmentQuarantined`, first paragraph)

**Interfaces:**
- Consumes: `weir_quarantine_bytes_on_disk` (existing gauge; written by recovery, the drain, and from PR 1 the 5 s poll task).
- Produces: `WeirSegmentQuarantined` with `expr: weir_quarantine_bytes_on_disk{job=~"weir"} > 0`, severity unchanged (critical), new summary and description text.

- [ ] **Step 1: Write the failing test.** Append to `deploy/prometheus/weir-alerts_test.yml`:

```yaml
  - interval: 1m
    name: a quarantine left on disk keeps firing after a restart reset the counter
    input_series:
      # The process restarted: its counter starts again at 0, the files remain.
      - series: 'weir_recovery_segments_quarantined_total{job="weir",instance="w:9185"}'
        values: "0x10"
      - series: 'weir_quarantine_bytes_on_disk{job="weir",instance="w:9185"}'
        values: "4096x10"
    alert_rule_test:
      - eval_time: 10m
        alertname: WeirSegmentQuarantined
        exp_alerts:
          - exp_labels:
              instance: "w:9185"
              job: "weir"
              severity: "critical"
            exp_annotations:
              summary: "weir holds quarantined segment data on w:9185"
              description: "<wab_dir>/quarantine/ is not empty: recovery or the drain moved a segment there (or, for mid-file corruption, a copy of it) because its bytes are bad (a header it cannot parse, in recovery a newer format too; a CRC mismatch; a truncated footer), a media-class read error outlasted three strand cycles and 10 minutes, or its .confirmed sidecar failed verification. A data-integrity event. The byte gauge survives restarts, so this keeps firing until quarantine/ is empty: `weir-ctl quarantine list` and `inspect`, `requeue --yes` (it deletes each fully recovered segment), then move out what it leaves, desynced or header-corrupt segments and the .wab.confirmed sidecars it ignores (see docs/operations/quarantine-recovery.md)."
              runbook: "docs/monitoring.md#weirsegmentquarantined"
```

In the existing test `one durability event fires its rule`, replace the input series

```yaml
      - series: 'weir_recovery_segments_quarantined_total{job="weir",instance="w:9185"}'
        values: "0 0 1 1 1 1 1 1"
```
with
```yaml
      - series: 'weir_quarantine_bytes_on_disk{job="weir",instance="w:9185"}'
        values: "0 0 4096 4096 4096 4096 4096 4096"
```
and its `WeirSegmentQuarantined` `exp_annotations` `summary`/`description` with the two strings above. In `a quiet daemon trips no durability rule`, replace the `weir_recovery_segments_quarantined_total` series with:
```yaml
      - series: 'weir_quarantine_bytes_on_disk{job="weir",instance="w:9185"}'
        values: "0x15"
```

- [ ] **Step 2: Run it to verify it fails.**
  promtool `test rules` → `FAILED`: the new restart test reports `got:[]` (the counter is flat), and the edited firing test reports `got:[]` (no counter series any more).

- [ ] **Step 3: Implement.** Replace the `WeirSegmentQuarantined` rule with:

```yaml
      # On the BYTES in quarantine/, not the per-process counter: a restart
      # resets weir_recovery_segments_quarantined_total and the counter rule
      # went quiet while the quarantined files were still there. The gauge
      # survives restarts and covers every quarantine path (recovery and the
      # drain). Like WeirAttestVerifyFailed, it keeps firing until the incident
      # is resolved, not for a window after it began.
      - alert: WeirSegmentQuarantined
        expr: weir_quarantine_bytes_on_disk{job=~"weir"} > 0
        for: 0m
        labels: { severity: critical }
        annotations:
          summary: "weir holds quarantined segment data on {{ $labels.instance }}"
          description: "<wab_dir>/quarantine/ is not empty: recovery or the drain moved a segment there (or, for mid-file corruption, a copy of it) because its bytes are bad (a header it cannot parse, in recovery a newer format too; a CRC mismatch; a truncated footer), a media-class read error outlasted three strand cycles and 10 minutes, or its .confirmed sidecar failed verification. A data-integrity event. The byte gauge survives restarts, so this keeps firing until quarantine/ is empty: `weir-ctl quarantine list` and `inspect`, `requeue --yes` (it deletes each fully recovered segment), then move out what it leaves, desynced or header-corrupt segments and the .wab.confirmed sidecars it ignores (see docs/operations/quarantine-recovery.md)."
          runbook: "docs/monitoring.md#weirsegmentquarantined"
```

In `docs/monitoring.md`, replace the first paragraph of `#### WeirSegmentQuarantined` (from `Crash recovery found a segment` through `(failing disk / bad RAM).`) with:

```markdown
`<wab_dir>/quarantine/` holds data. Recovery or the drain moved a segment there
(or, for mid-file corruption, a copy of it) because its bytes are bad (header,
CRC, a truncated footer), a media-class read error persisted for three strand
cycles and 10 minutes, or its `.confirmed` sidecar failed verification. A
data-integrity event; CRC mismatches usually mean storage corruption (failing
disk / bad RAM). A *sealed* segment from a newer weir is parked instead
([WeirSegmentParked](#weirsegmentparked)); crash recovery still quarantines an
active segment whose header it cannot parse, newer format included.
The rule reads `weir_quarantine_bytes_on_disk`, which survives a restart and
counts every file in `quarantine/`, so it fires until the directory is empty:
`requeue --yes` deletes the segments it fully recovers; move out what it
leaves (desynced or header-corrupt segments, and `.wab.confirmed` sidecars,
which `weir-ctl quarantine` ignores) once you have kept a copy.
```

- [ ] **Step 4: Run to verify it passes.** promtool `test rules` → `SUCCESS`. `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.

- [ ] **Step 5: Verify by mutation.** Put the old counter expression back: the restart test FAILS. Revert.

- [ ] **Step 6: Commit.**
  `git add deploy/prometheus/weir-alerts.yml deploy/prometheus/weir-alerts_test.yml docs/monitoring.md`
  `git commit -m "fix(alerts): WeirSegmentQuarantined keeps firing across a restart while quarantine holds data"`

---

### Task 8.8: The ack-timeout and queue-saturation runbooks check the drain hand-off first

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Spec §1: a full drain channel makes flushers block at the rotation hand-off before acks resolve, so producers see `ACK_TIMEOUT` for durable records, "routinely misdiagnosed as a storage problem". The `WeirAckTimeout` alert today says "flusher may be wedged ... Check fsync latency". Spec §16: rewrite both runbooks to check hand-off blocking and channel depth first. A guard pins the order.

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs` (two helpers, one test)
- Modify: `deploy/prometheus/weir-alerts.yml:47-54` (`WeirAckTimeout` summary and description) and the `WeirQueueSaturation` description
- Modify: `deploy/prometheus/weir-alerts_test.yml` (the `WeirAckTimeout` and `WeirQueueSaturation` `exp_annotations`)
- Modify: `docs/monitoring.md:101-106` (`#### WeirAckTimeout`) and `:365-371` (`#### WeirQueueSaturation`)

**Interfaces:**
- Consumes: `weir_wab_handoff_blocked_seconds{shard}` (PR 3), `weir_drain_channel_depth`, `weir_drain_channel_capacity` (PR 1/3), the `ack_timeouts_recent` and `drain_backpressure` conditions (PR 1).
- Produces: docs_drift `fn monitoring_section(heading: &str) -> &'static str`, `fn alert_annotation(rule: &str, key: &str) -> &'static str`, test `ack_timeout_and_queue_saturation_runbooks_check_the_drain_hand_off_first`.

- [ ] **Step 1: Write the failing test.** Append to `crates/weir-server/tests/docs_drift.rs`:

```rust
/// The body under `#### <heading>` in docs/monitoring.md, up to the next
/// heading of any level.
fn monitoring_section(heading: &str) -> &'static str {
    let marker = format!("\n#### {heading}\n");
    let start = MONITORING
        .find(&marker)
        .unwrap_or_else(|| panic!("docs/monitoring.md has no `#### {heading}`"))
        + marker.len();
    let rest = &MONITORING[start..];
    let end = ["\n## ", "\n### ", "\n#### "]
        .iter()
        .filter_map(|m| rest.find(m))
        .min()
        .unwrap_or(rest.len());
    &rest[..end]
}

/// The double-quoted value of `<key>:` in the rule named `rule`.
fn alert_annotation(rule: &str, key: &str) -> &'static str {
    let at = ALERTS
        .find(&format!("- alert: {rule}\n"))
        .unwrap_or_else(|| panic!("weir-alerts.yml has no rule {rule}"));
    let rest = &ALERTS[at..];
    let needle = format!("{key}: \"");
    let start = rest
        .find(&needle)
        .unwrap_or_else(|| panic!("{rule} has no {key} annotation"))
        + needle.len();
    let end = rest[start..].find("\"\n").expect("unterminated annotation") + start;
    &rest[start..end]
}

/// Both runbooks send the operator to the drain hand-off before storage.
///
/// When the drain channel is full, flushers block handing sealed segments to
/// the drain before the acks of the batch behind resolve, so producers get
/// ACK_TIMEOUT for records that are durable. The old text said "flusher may
/// be wedged, check fsync latency", which sent every such incident to the
/// disk. Order matters here, not just presence: a runbook that mentions the
/// hand-off after three paragraphs of fsync triage still misdirects.
#[test]
fn ack_timeout_and_queue_saturation_runbooks_check_the_drain_hand_off_first() {
    for rule in ["WeirAckTimeout", "WeirQueueSaturation"] {
        for (place, text) in [
            ("docs/monitoring.md", monitoring_section(rule)),
            ("weir-alerts.yml description", alert_annotation(rule, "description")),
        ] {
            let storage = text
                .find("fsync")
                .unwrap_or_else(|| panic!("{rule} ({place}) no longer mentions fsync at all"));
            for signal in ["weir_wab_handoff_blocked_seconds", "weir_drain_channel_depth"] {
                match text.find(signal) {
                    Some(i) if i < storage => {}
                    Some(_) => panic!("{rule} ({place}): {signal} comes after the fsync triage"),
                    None => panic!("{rule} ({place}): does not mention {signal}"),
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 ack_timeout_and_queue_saturation_runbooks_check_the_drain_hand_off_first` → FAIL: `WeirAckTimeout (docs/monitoring.md): does not mention weir_wab_handoff_blocked_seconds`.

- [ ] **Step 3: Implement.** In `deploy/prometheus/weir-alerts.yml`, `WeirAckTimeout`:

```yaml
          summary: "weir ack timed out on {{ $labels.instance }} — check the drain hand-off before storage"
          description: "A record's ack did not resolve within the timeout and the producer got ACK_TIMEOUT. Often this is delivery, not storage: with the drain channel full, flushers wait to hand sealed segments to the drain before the acks behind them resolve, so durable records are nacked. Check weir_wab_handoff_blocked_seconds and weir_drain_channel_depth first; only if both are idle, check fsync latency and the flusher threads."
```

`WeirQueueSaturation` (summary unchanged):

```yaml
          description: "The work queue is backing up and producers see slower acks. Check weir_wab_handoff_blocked_seconds and weir_drain_channel_depth first: a full drain channel stalls the flushers, and the queue backs up behind them. If both are idle, the WAB flushers cannot keep pace with ingest (usually storage pressure); correlate with fsync latency."
```

In `deploy/prometheus/weir-alerts_test.yml`, set the `WeirAckTimeout` expectation in `one durability event fires its rule` to `summary: "weir ack timed out on w:9185 — check the drain hand-off before storage"` and the description above; set the `WeirQueueSaturation` expectation in `a saturated queue fires` to the new description.

In `docs/monitoring.md`, replace `#### WeirAckTimeout`'s body with:

```markdown
A record's ack did not resolve within the timeout, and the producer got an
`ACK_TIMEOUT` nack. The common cause is **not** storage: when the drain's
channel is full, a flusher waits to hand a sealed segment to the drain before
the acks of the batch behind it resolve, so records that are already durable
are nacked.
**Respond, in this order:**
1. `weir_wab_handoff_blocked_seconds` above 0, or `weir_drain_channel_depth`
   at or near `weir_drain_channel_capacity` (256): delivery is stalling
   ingest. Follow [WeirDrainBackpressure](#weirdrainbackpressure). The
   records were durable, and producers' retries will deliver them twice.
2. Both idle: look at storage. Correlate with `WeirFsyncLatencyHigh` and
   `weir_queue_depth`; spiking fsync latency means a slow or failing disk.
3. A `WeirFlusherPanic` shortly after: treat it as a flusher panic. A hung
   daemon may need a restart.

`weir-ctl status` shows the `ack_timeouts_recent` condition with the cause it
infers from the same signals.
```

and `#### WeirQueueSaturation`'s body with:

```markdown
The work queue between the sockets and the WAB workers is backing up, and
producers see slower acks.
**Respond, in this order:**
1. `weir_wab_handoff_blocked_seconds` above 0, or `weir_drain_channel_depth`
   near its capacity: a full drain channel stalls the flushers and the queue
   backs up behind them. That is a delivery problem; follow
   [WeirDrainBackpressure](#weirdrainbackpressure).
2. Both idle: the flushers cannot keep pace with ingest, usually storage.
   Correlate with fsync latency. If fsync is the bottleneck you are at the
   storage durability ceiling: a faster disk, more shards (one flusher per
   disk), or latency-tolerant traffic on the `Buffered` tier. `[TUNE]` the
   threshold to your `batch_size × shard_count` and burst profile.
```

- [ ] **Step 4: Run to verify it passes.** The Step 2 command → PASS; promtool `test rules` → `SUCCESS`; `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.

- [ ] **Step 5: Verify by mutation.** In monitoring.md's `#### WeirAckTimeout`, move item 2 above item 1: the test FAILS (`comes after the fsync triage`). Revert.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/docs_drift.rs deploy/prometheus/weir-alerts.yml deploy/prometheus/weir-alerts_test.yml docs/monitoring.md`
  `git commit -m "docs(monitoring): ack-timeout and queue-saturation runbooks check the drain hand-off before storage"`

---
### Task 8.9: The Conditions catalogue in monitoring.md, guarded against the code

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Every condition carries `runbook: docs/monitoring.md#cond-<code>` (spec §9.3), and the readiness script and `WeirConditionCritical` link there too. Spec §9.3 Contract: every code in `ALL`, plus the offline-only codes, has a `#### cond-<code>` heading, and docs_drift extends the runbook-anchor guard. This task writes the section and a guard that derives the code list, severity, `affects` and gates from `weir-status` itself.

It also fixes a latent bug in the existing anchor guard: its slug function drops `_`, but GitHub and mdBook both keep it (mdBook's `normalize_id` keeps alphanumerics, `_` and `-`). No alert anchor has an underscore today, so it never mattered; every `cond-<code>` anchor has one.

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs:436-480` (`every_alert_runbook_anchor_resolves_to_a_heading`: use a shared slug helper) and a new test at the end
- Modify: `docs/monitoring.md` (new `## Conditions` section between `## Alerts & runbook` and `## Capacity: surviving a sink outage`, i.e. before the `---` at ~561)

**Interfaces:**
- Consumes (PR 1 Task 1.1): `weir_status::ALL: &'static [ConditionCode]`; `weir_status::entry(ConditionCode) -> Option<&'static CatalogueEntry>` with `CatalogueEntry { code, severity: Severity, affects: Affects, gates: &'static [Gate], runbook: &'static str }`; `Severity`, `Affects`, `Gate` serialize as snake_case strings (they are fields of the serde `Condition`). PR 1's `Gate` is `#[serde(rename_all = "snake_case")]`, so the gates column uses `readiness` and `liveness`.
- Consumes from Task 8.5: `wire_name`, `condition_code_names`.
- Produces: docs_drift `fn heading_slugs(doc: &str) -> Vec<String>`, `const OFFLINE_ONLY_CODES: [&str; 2]`, `fn catalogue_row(c: &weir_status::ConditionCode) -> [String; 4]`, test `every_condition_has_a_catalogue_row_and_a_runbook_heading`; anchors `docs/monitoring.md#conditions` and `#cond-<code>` for all 32 codes.

- [ ] **Step 1: Write the failing test.** In `crates/weir-server/tests/docs_drift.rs`, add above `every_alert_runbook_anchor_resolves_to_a_heading`:

```rust
/// The anchor of every `#### ` heading in `doc`, as GitHub and mdBook build
/// it: lowercased, punctuation dropped except `-` and `_`, spaces to `-`.
fn heading_slugs(doc: &str) -> Vec<String> {
    doc.lines()
        .filter_map(|l| l.strip_prefix("#### "))
        .map(|h| {
            h.trim()
                .to_ascii_lowercase()
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | ' '))
                .collect::<String>()
                .replace(' ', "-")
        })
        .collect()
}
```

and replace the `let headings: Vec<String> = MONITORING ... .collect();` block at the top of `every_alert_runbook_anchor_resolves_to_a_heading` with `let headings = heading_slugs(MONITORING);`.

Append at the end of the file:

```rust
/// Codes only `weir-ctl status` emits, from offline evidence (spec §9.3).
const OFFLINE_ONLY_CODES: [&str; 2] = ["daemon_unreachable", "recovery_pending"];

/// `[code, severity, affects, gates]` as the catalogue table must show them.
fn catalogue_row(c: &weir_status::ConditionCode) -> [String; 4] {
    // PR 1's catalogue: `entry(code)` carries severity, affects and gates.
    let e = weir_status::entry(*c).unwrap_or_else(|| panic!("{c:?} has no catalogue entry"));
    let gates: Vec<String> = e
        .gates
        .iter()
        .map(|g| wire_name(serde_json::to_value(g).expect("serialize a Gate")))
        .collect();
    [
        wire_name(serde_json::to_value(c).expect("serialize a ConditionCode")),
        wire_name(serde_json::to_value(e.severity).expect("serialize a Severity")),
        wire_name(serde_json::to_value(e.affects).expect("serialize an Affects")),
        if gates.is_empty() { "none".to_string() } else { gates.join(", ") },
    ]
}

/// Every condition the daemon or weir-ctl can report has a row in the
/// catalogue table that agrees with the code, and a `#### cond-<code>`
/// runbook heading; every row names a real code.
///
/// The runbook link is printed by `weir-ctl status`, by the readiness script
/// and by WeirConditionCritical, so a code without a heading sends an operator
/// to a page that scrolls nowhere. The severity, `affects` and gates columns
/// are compared with `weir_status` itself, because a table that says a
/// condition does not gate readiness when it does is worse than no table.
#[test]
fn every_condition_has_a_catalogue_row_and_a_runbook_heading() {
    let section = MONITORING
        .split("\n## Conditions\n")
        .nth(1)
        .expect("docs/monitoring.md must keep its `## Conditions` section")
        .split("\n## ")
        .next()
        .unwrap();
    let rows: Vec<Vec<String>> = section
        .lines()
        .filter(|l| l.starts_with("| `"))
        .map(|l| {
            l.split('|')
                .map(|cell| cell.trim().trim_matches('`').to_string())
                .filter(|cell| !cell.is_empty())
                .collect()
        })
        .collect();
    let slugs = heading_slugs(MONITORING);
    let daemon_codes = condition_code_names();
    let mut problems = Vec::new();

    for c in weir_status::ALL {
        let want = catalogue_row(c);
        let code = &want[0];
        if OFFLINE_ONLY_CODES.contains(&code.as_str()) {
            continue; // checked below; their gates cell carries "(weir-ctl only)"
        }
        match rows.iter().find(|r| r.first() == Some(code)) {
            None => problems.push(format!("{code}: no row in the catalogue table")),
            Some(r) if r.len() < 4 || r[..4] != want[..] => problems.push(format!(
                "{code}: the table says {:?}, weir_status says {want:?}",
                &r[..r.len().min(4)]
            )),
            Some(_) => {}
        }
        if !slugs.contains(&format!("cond-{code}")) {
            problems.push(format!("{code}: no `#### cond-{code}` heading"));
        }
    }
    for code in OFFLINE_ONLY_CODES {
        if !rows.iter().any(|r| r.first().map(String::as_str) == Some(code)) {
            problems.push(format!("{code} (weir-ctl only): no row"));
        }
        if !slugs.contains(&format!("cond-{code}")) {
            problems.push(format!("{code} (weir-ctl only): no `#### cond-{code}` heading"));
        }
    }
    for r in &rows {
        let code = &r[0];
        if !daemon_codes.contains(code) && !OFFLINE_ONLY_CODES.contains(&code.as_str()) {
            problems.push(format!("{code}: a row for a code nothing emits"));
        }
    }

    assert!(
        problems.is_empty(),
        "docs/monitoring.md's Conditions catalogue disagrees with weir_status:\n  {}",
        problems.join("\n  ")
    );
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 every_condition_has_a_catalogue_row_and_a_runbook_heading` → FAIL: "docs/monitoring.md must keep its `## Conditions` section". (If an earlier PR already added a section, it fails listing the codes without rows or headings instead.)
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 every_alert_runbook_anchor_resolves_to_a_heading` → PASS (no current anchor has `_`, so the slug change is behaviour-neutral for them).

- [ ] **Step 3: Implement.** Insert into `docs/monitoring.md`, before the `---` that precedes `## Capacity: surviving a sink outage` (if PR 1 or PR 5 already started a `## Conditions` section, merge into it: keep its prose, make its table and headings match the text below):

```markdown
---

## Conditions

The daemon evaluates a fixed catalogue of **conditions** from its own state:
one pure evaluator (the `weir-status` crate) behind `/status`, `/readyz`,
`/livez`, the admin `status` verb, the `weir_condition_active{code,severity}`
gauge, `weir-readiness.sh` and an offline `weir-ctl status`. Each has a code,
a severity, what it affects (ingest, delivery or durability), and whether it
gates readiness (`/readyz` 503) or may be selected for liveness
(`livez_checks`, evaluated only while serving). Missing evidence makes a
condition *unevaluated*, listed separately on `/status`; never read that as
healthy.

| Code | Severity | Affects | Gates |
|---|---|---|---|
| `starting` | info | ingest | readiness |
| `shutting_down` | info | ingest | readiness |
| `listener_down` | critical | ingest | readiness, liveness |
| `no_live_shard` | critical | ingest | readiness, liveness |
| `shard_offline` | critical | ingest | none |
| `worker_exited` | critical | ingest | liveness |
| `wab_cap_rejecting` | critical | ingest | readiness |
| `drain_backpressure` | warning | ingest | none |
| `ack_timeouts_recent` | warning | ingest | none |
| `wab_scan_stale` | warning | ingest | none |
| `wab_growing_unbounded` | warning | durability | none |
| `drain_stopped` | critical | delivery | liveness |
| `drain_wedged` | critical | delivery | liveness |
| `drain_paused` | warning | delivery | none |
| `drain_blocked_dead_letter_full` | critical | delivery | none |
| `delivery_stalled` | warning | delivery | none |
| `delivery_lagging` | warning | delivery | none |
| `segments_stranded` | warning | delivery | none |
| `sink_down` | warning | delivery | none |
| `sink_degraded` | info | delivery | none |
| `sink_failing` | warning | delivery | none |
| `dead_letter_near_cap` | warning | delivery | none |
| `noop_sink` | warning | delivery | none |
| `active_segment_aging` | info | delivery | none |
| `fsync_failures_recent` | critical | durability | none |
| `flusher_respawned_recent` | warning | ingest | none |
| `quarantine_nonempty` | warning | durability | none |
| `recovery_incomplete` | critical | durability | none |
| `tls_reload_failed` | warning | ingest | none |
| `admin_unavailable` | warning | ingest | none |
| `daemon_unreachable` | critical | ingest | none (weir-ctl only) |
| `recovery_pending` | info | durability | none (weir-ctl only) |

Sink, drain, pause, lag, dead-letter, quarantine and fsync or panic history
never gate readiness: weir keeps accepting and buffering durably through all
of them. A newer daemon may report a code an older `weir-ctl` does not know;
it is shown verbatim, never dropped.

### Ingest and lifecycle

#### cond-starting
**Means:** a startup phase (`initializing`, `recovering`, `handoff`,
`binding`); the producer socket binds once recovery has sealed the open
segments. Health, metrics and the admin socket already answer.
**Respond:** nothing while it progresses; `weir-ctl status` shows the phase
and the replay remaining. A long `recovering` is a large WAB or a slow disk:
check the log.

#### cond-shutting_down
**Means:** a graceful shutdown is under way: listeners closed, new
connections refused, the drain on its `drain_shutdown_budget_secs` budget.
**Respond:** nothing. If unexpected, find who sent the SIGTERM.

#### cond-listener_down
**Means:** a configured producer listener (Unix or TCP) failed, or closed
outside shutdown. Selectable in `livez_checks`.
**Respond:** read the bind or accept error in the log, then restart; a
restart re-binds.

#### cond-no_live_shard
**Means:** no shard can accept: every flusher is offline or its owning worker
exited. New connections are refused. Selectable in `livez_checks`.
**Respond:** capture the panic or counter-scan error from the log, then
restart; recovery re-runs.

#### cond-shard_offline
**Means:** some shards are offline (respawn budget exhausted, counter scan
failed, flusher exited). New connections avoid them; connections already
pinned to them nack until they reconnect.
**Respond:** capture the log (`grep "flusher panicked"`), then restart.

#### cond-worker_exited
**Means:** a worker thread exited outside shutdown; the shards it owns cannot
accept records. Selectable in `livez_checks`.
**Respond:** capture the log, then restart.

#### cond-wab_cap_rejecting
**Means:** the WAB reached `wab_max_bytes`; pushes nack until it falls below
the low-water mark. The root cause is delivery.
**Respond:** follow the drain and sink conditions. Raising `wab_max_bytes`
buys time only.

#### cond-drain_backpressure
**Means:** the drain channel is full, or a shard has been blocked handing a
sealed segment to the drain for over a second. Acks wait behind the hand-off,
so producers see `ACK_TIMEOUT` for durable records.
**Respond:** a delivery problem, not storage: see
[WeirDrainBackpressure](#weirdrainbackpressure).

#### cond-ack_timeouts_recent
**Means:** `weir_ack_timeout_total` rose in the last 5 minutes. The summary
names the inferred cause: delivery (with `drain_backpressure`), storage
(fsync failures or slow group commits), or a flusher not completing groups.
**Respond:** follow the inferred cause; see [WeirAckTimeout](#weiracktimeout).

#### cond-wab_scan_stale
**Means:** no successful WAB size scan for 30 s (or none yet since start).
The cap is enforced against the last size, fail-closed, and the lag figures
are stale.
**Respond:** look for scan errors in the log (permissions or I/O on
`wab_dir`).

#### cond-wab_growing_unbounded
**Means:** the WAB is growing with `wab_max_bytes = 0` while delivery is not
healthy (including while paused).
**Respond:** fix delivery, or set `wab_max_bytes` so a full disk becomes
nacks instead of a crash.

### Delivery

#### cond-drain_stopped
**Means:** the drain supervisor gave up after repeated panics. Ingest
continues, nothing is delivered, the WAB grows, and `sink_health` is stale.
Selectable in `livez_checks`.
**Respond:** the admin view of `weir-ctl status` shows the last panic
message; capture it, then restart. See [WeirDrainStopped](#weirdrainstopped).

#### cond-drain_wedged
**Means:** the drain thread is blocked inside a sink call that does not
yield (a commit older than `commit_timeout` + 15 s, or no progress for
`health_poll_interval` + 15 s). Selectable in `livez_checks`.
**Respond:** restart; the in-flight segment is re-delivered from record 0.
Investigate the sink.

#### cond-drain_paused
**Means:** an operator paused delivery (`weir-ctl drain pause`); it stopped
before the next commit batch, possibly mid-segment (`held`). Ingest keeps
buffering. If the drain panics and respawns while paused, it stays paused,
and on resume it continues past the records already committed: nothing in
the held segment is re-delivered within this run.
**Respond:** `weir-ctl drain resume` when done. A restart also resumes, but
replays the held segment from record 0. With `wab_max_bytes = 0` the WAB
grows without bound meanwhile.

#### cond-drain_blocked_dead_letter_full
**Means:** `dead_letter/` reached `dead_letter_max_bytes`; all delivery waits.
**Respond:** `weir-ctl dl requeue` or `weir-ctl dl drop`, or raise the cap.
See [WeirDrainBlocked](#weirdrainblocked).

#### cond-delivery_stalled
**Means:** there is undelivered data and no commit has succeeded for 300 s,
and delivery is not paused.
**Respond:** check the sink conditions; see
[WeirDrainNotConfirming](#weirdrainnotconfirming).

#### cond-delivery_lagging
**Means:** the oldest undelivered data is over 900 s old (an upper bound).
**Respond:** compare the ingest rate with the commit rate; see
[WeirOldestUndeliveredAge](#weiroldestundeliveredage).

#### cond-segments_stranded
**Means:** segments exhausted their retries since the last successful commit.
They resume automatically once their cool-down has passed and the sink's
probe no longer reports it down.
**Respond:** fix the sink, or `weir-ctl drain retry-stranded`. Within this run
a resumed segment continues past the records already committed (the drain
keeps a resume cursor pinned to the file); after a restart it replays from
record 0.

#### cond-sink_down
**Means:** the sink's health probe reports it unreachable. weir keeps
buffering durably; this is not a readiness failure.
**Respond:** fix the sink, and watch the WAB against `wab_max_bytes`. The
admin view shows the probe's reason.

#### cond-sink_degraded
**Means:** the sink's health probe reports it degraded.
**Respond:** watch it; see [WeirSinkDegraded](#weirsinkdegraded).

#### cond-sink_failing
**Means:** three or more consecutive commits failed while the probe passes.
**Respond:** the admin view shows the last error (redacted). Common causes:
credentials, schema, records the sink rejects.

#### cond-dead_letter_near_cap
**Means:** `dead_letter/` is at 80% or more of `dead_letter_max_bytes`.
**Respond:** requeue or drop dead-letters before the drain blocks.

#### cond-noop_sink
**Means:** `sink_type = "noop"`: records are acked and DISCARDED.
**Respond:** configure a real sink unless this is deliberate (soak tests).

#### cond-active_segment_aging
**Means:** an active segment is over an hour old and both seal timers are 0,
so its records are durable but will not be delivered until it fills.
**Respond:** set `wab_segment_max_age_secs` or `wab_segment_max_lifetime_secs`.

### Durability

#### cond-fsync_failures_recent
**Means:** `fdatasync` failed in the last 15 minutes. Windowed: it clears on
its own, unlike the readiness script's old lifetime-counter gate.
**Respond:** storage; see [WeirFsyncFailure](#weirfsyncfailure).

#### cond-flusher_respawned_recent
**Means:** a flusher panicked and was respawned in the last 15 minutes, and
every shard is up.
**Respond:** capture the panic from the log and report it.

#### cond-quarantine_nonempty
**Means:** `quarantine/` holds data.
**Respond:** `weir-ctl quarantine list`, `inspect`, `requeue`; see
[quarantine recovery](operations/quarantine-recovery.md), and
[WeirSegmentQuarantined](#weirsegmentquarantined) for what `requeue` leaves
behind.

#### cond-recovery_incomplete
**Means:** in this process, recovery failed a segment or failed to copy one
into quarantine. The segment was left in place.
**Respond:** read the recovery errors in the log, fix the cause (space,
permissions), then restart.

### Config and control surface

#### cond-tls_reload_failed
**Means:** the last TLS reload failed; the previous certificates are still
served. Sticky until a reload succeeds.
**Respond:** fix the files, then `weir-ctl reload` or `kill -HUP`.

#### cond-admin_unavailable
**Means:** the admin socket is enabled but could not be bound.
**Respond:** see [WeirAdminUnavailable](#weiradminunavailable).

### Reported by weir-ctl only

#### cond-daemon_unreachable
**Means:** `weir-ctl status` could not reach the daemon by admin socket or
HTTP, and built its verdict from the disk alone.
**Respond:** check the process (`systemctl status weir`, container state).

#### cond-recovery_pending
**Means:** `weir-ctl status`, reading the disk, found active segments the
next start must recover.
**Respond:** nothing; informational.
```

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.
  `mdbook build` (from the repository root, as `docs.yml` does) → no broken-link warnings for the new anchors.

- [ ] **Step 5: Verify by mutation.** Revert each after its run.
  1. Delete the `#### cond-noop_sink` heading: FAIL `noop_sink: no \`#### cond-noop_sink\` heading`.
  2. Change `wab_cap_rejecting`'s gates cell to `none`: FAIL `the table says [.., "none"], weir_status says [.., "readiness"]`.
  3. Add a row `| \`sink_unreachable\` | warning | delivery | none |`: FAIL `a row for a code nothing emits`.
  4. Revert `heading_slugs` to drop `_`: the test FAILS for every `cond-` heading (proving the fix is load-bearing).

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/docs_drift.rs docs/monitoring.md`
  `git commit -m "docs(monitoring): the Conditions catalogue, guarded against weir-status"`

---

### Task 8.10: Health and status endpoints, the probe recipe, and the `/status` top-level keys guard

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Spec §16 asks for a docs_drift guard on the `/status` top-level keys, and §12 gives the probe recipe (docs only; manifests are SP-O). The guard compares the documented keys with PR 1's golden documents, which are themselves pinned by `weir-status` tests, so a key added to the type without a documentation row fails here.

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs` (two consts, one helper, one test)
- Modify: `docs/monitoring.md` (new `## Health and status endpoints` section right after `## Setup`, i.e. before the `---` preceding `## Alerts & runbook`)
- Modify: `docs/explorations/phase4-k8s-deployment.md:121-127` (`### 3.3 Probes`)

**Interfaces:**
- Consumes: golden files `crates/weir-status/tests/golden/status_v1_public.json` (PR 5 Task 5.2) and `crates/weir-status/tests/golden/status_v1_admin.json` (PR 1 Task 1.8) (spec §16 "Golden `status_v1_{public,admin}.json`"). Top-level keys per spec §9.1.
- Consumes (PR 5): routes and response bodies of spec §10.
- Produces: docs_drift `const STATUS_GOLDEN_PUBLIC: &str`, `const STATUS_GOLDEN_ADMIN: &str`, `fn top_level_keys(doc: &str) -> std::collections::BTreeSet<String>`, test `the_status_reference_lists_every_top_level_key`; anchors `#health-and-status-endpoints`, `#probe-recipe`, `#the-status-document`.

- [ ] **Step 1: Write the failing test.** Add after the other consts in `docs_drift.rs`:

```rust
const STATUS_GOLDEN_PUBLIC: &str =
    include_str!("../../weir-status/tests/golden/status_v1_public.json");
const STATUS_GOLDEN_ADMIN: &str =
    include_str!("../../weir-status/tests/golden/status_v1_admin.json");
```

and append:

```rust
fn top_level_keys(doc: &str) -> std::collections::BTreeSet<String> {
    serde_json::from_str::<serde_json::Value>(doc)
        .expect("a golden status document is valid JSON")
        .as_object()
        .expect("a status document is a JSON object")
        .keys()
        .cloned()
        .collect()
}

/// docs/monitoring.md's `/status` reference has one row per top-level key of
/// the document, marked with the views that carry it, and no row for a key
/// the document does not have.
///
/// `weir.status/v1` is additive-only and `weir-ctl status --json` pins its
/// shape, so its keys are a contract operators script against. The goldens
/// are the serialized types (the weir-status tests pin them), so this follows
/// the code, not a copy of it.
#[test]
fn the_status_reference_lists_every_top_level_key() {
    let section = MONITORING
        .split("\n### The `/status` document\n")
        .nth(1)
        .expect("docs/monitoring.md must keep its \"The `/status` document\" heading");
    let section = ["\n## ", "\n### "]
        .iter()
        .filter_map(|m| section.find(m))
        .min()
        .map_or(section, |end| &section[..end]);

    let mut documented_admin = std::collections::BTreeSet::new();
    let mut documented_public = std::collections::BTreeSet::new();
    for line in section.lines().filter(|l| l.starts_with("| `")) {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        let key = cells[1].trim_matches('`').to_string();
        documented_admin.insert(key.clone());
        if cells[2].contains("public") {
            documented_public.insert(key);
        }
    }

    for (golden, view) in [(STATUS_GOLDEN_PUBLIC, "public"), (STATUS_GOLDEN_ADMIN, "admin")] {
        let v: serde_json::Value = serde_json::from_str(golden).expect("golden JSON");
        assert_eq!(v["schema"], "weir.status/v1", "the {view} golden's schema");
        assert_eq!(v["view"], view, "the {view} golden's view");
    }
    assert_eq!(
        documented_admin,
        top_level_keys(STATUS_GOLDEN_ADMIN),
        "the `/status` reference rows (left) differ from the admin document's \n\
         top-level keys (right)"
    );
    assert_eq!(
        documented_public,
        top_level_keys(STATUS_GOLDEN_PUBLIC),
        "the rows marked `public` (left) differ from the public document's \n\
         top-level keys (right); an admin-only key marked public tells operators \n\
         a field is on the unauthenticated listener when it is not, and the \n\
         reverse hides one that is"
    );
    assert!(MONITORING.contains("`\"weir.status/v1\"`"));
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 the_status_reference_lists_every_top_level_key` → FAIL: `docs/monitoring.md must keep its "The `/status` document" heading` (or, if PR 5 documented the endpoints, a set difference naming the missing keys).

- [ ] **Step 3: Implement.** Insert into `docs/monitoring.md`, after the `## Setup` section and before the `---` that precedes `## Alerts & runbook` (if PR 5 already added a section with this heading, keep its prose and make its subsections match):

````markdown
---

## Health and status endpoints

The metrics listener (`metrics_bind:metrics_port`, default `127.0.0.1:9185`)
serves four more paths. They match exactly (case-sensitive, no trailing-slash
folding) and accept `GET` and `HEAD`; any other method gets `405`. Every other
path still returns the metrics, byte for byte as before. None of these routes
reads a body or changes anything, and their responses carry
`Cache-Control: no-store`.

| Path | `200` when | `503` when |
|---|---|---|
| `/livez` | always with the default `livez_checks = []`, or no selected check is active | a check selected in `livez_checks` is active: `not live: <codes>` |
| `/readyz` | serving, not shutting down, the `wab_max_bytes` cap not rejecting, at least one shard up, no listener down | any of those fails: `not ready: <codes>` |
| `/startupz` | the daemon has reached `serving` once (sticky) | still starting: `starting: phase=<p> since=<s>s replay=<n>` |
| `/status` | always: the public `weir.status/v1` JSON document | never (`500` with an empty body on a serialization error) |

`?verbose` on `/livez` and `/readyz` prints one line per check with its public
summary. Probes and scrapes use separate connection pools, and a client must
send its request head within 1 s.

### Probe recipe

Kubernetes (fragments of a pod spec; the manifests themselves are future work):

```yaml
terminationGracePeriodSeconds: 45   # shutdown_timeout_secs + drain_shutdown_budget_secs + 5
containers:
  - name: weir
    startupProbe:
      httpGet: { path: /startupz, port: 9185 }
      periodSeconds: 5
      timeoutSeconds: 2
      failureThreshold: 120   # 10 minutes: size it to your longest recovery
    readinessProbe:
      httpGet: { path: /readyz, port: 9185 }
      periodSeconds: 5
      timeoutSeconds: 2
      failureThreshold: 2
    livenessProbe:
      httpGet: { path: /livez, port: 9185 }
      periodSeconds: 10
      timeoutSeconds: 2
      failureThreshold: 6     # a minute of probe-pool pressure before a restart
```

Set `metrics_bind = "0.0.0.0"` so kubelet can reach the port, and restrict
9185 to kubelet and Prometheus with a NetworkPolicy: the listener is
unauthenticated. Liveness is opt-in. With `livez_checks = []`, `/livez` fails
only when the process stops answering. The recommended set restarts only on
states that leave ingest dead and that a restart fixes:
`livez_checks = ["no_live_shard", "listener_down", "worker_exited"]`.
`drain_stopped` and `drain_wedged` are deliberate opt-ins.

Docker: the image's `HEALTHCHECK` probes `/livez` with bash alone; see
[container.md](security/container.md#healthcheck).

Bare metal and cron: `deploy/systemd/weir-readiness.sh` exits `1` exactly when
`/readyz` answers 503, with the exit codes of `weir-ctl status`.

### The `/status` document

`GET /status` returns `weir.status/v1` as JSON with `"view": "public"`. The
admin socket's `status` verb (and so `weir-ctl status`) returns the same
document with `"view": "admin"`, which adds the keys marked admin-only below
and free-text detail inside the shared ones (error messages, panic payloads,
paths, uids). The public view cannot carry free text: it is a separate type
with no field that could hold it. Times are unix milliseconds, ages are
seconds, and an unknown value is `null`. Keys are only ever added within v1.

| Key | Views | Contents |
|---|---|---|
| `schema` | public, admin | `"weir.status/v1"` |
| `view` | public, admin | `"public"` or `"admin"` |
| `version` | public, admin | the weir version |
| `wire_version` | public, admin | the wire protocol version |
| `generated_at_ms` | public, admin | when this document was built |
| `started_at_ms` | public, admin | process start |
| `uptime_secs` | public, admin | seconds since start |
| `pid` | admin | process id |
| `instance` | admin | `wab_dir`, `socket_path`, and the `wab_dir` (dev, ino) `weir-ctl` checks |
| `verdict` | public, admin | `ready`, `live`, `startup_complete`, the selected `livez_checks`, the failing codes |
| `conditions` | public, admin | the active conditions (see [Conditions](#conditions)) |
| `unevaluated` | public, admin | codes whose evidence is missing; never healthy by default |
| `startup` | public, admin | phase, since when, replay total and remaining |
| `lifecycle` | public, admin | shutting down, since when, forced, the drain deadline |
| `ingest` | public, admin | listener states, queue depth, the `wab_max_bytes` cap |
| `shards` | public, admin | per shard: state, reason, respawns, worker, hand-off blocked since, active segment age |
| `drain` | public, admin | state, pause, in-flight segment, queues, channel depth, heartbeat age, last requeue, exit |
| `sink` | public, admin | type, probe health, last success and failure, consecutive failures |
| `backlog` | public, admin | undelivered segments and records, oldest undelivered age (an upper bound), dead-letter and quarantine bytes |
| `reload` | public, admin | the last reload report |
| `admin` | public, admin | admin protocol version, verbs, socket state and reason code |
| `config` | public, admin | effective numeric settings; the admin view has the full redacted configuration |
````

In `docs/explorations/phase4-k8s-deployment.md`, replace the three paragraphs under `### 3.3 Probes` (`**Liveness probe:**`, `**Readiness probe:**`, `**Startup probe:**`) with:

```markdown
**Superseded.** The daemon now serves `/livez`, `/readyz`, `/startupz` and
`/status` on the metrics port; health answers within milliseconds of start,
before recovery. The probe recipe (periods, thresholds, the recommended
`livez_checks`, `terminationGracePeriodSeconds: 45`) is in
[monitoring.md](../monitoring.md#probe-recipe). The analysis below this
section predates those routes.
```

- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS. `mdbook build` → no warnings for the new anchors.

- [ ] **Step 5: Verify by mutation.** Revert each.
  1. Delete the `| \`backlog\` |` row: FAIL, the left set lacks `backlog`.
  2. Change `pid`'s views cell to `public, admin`: FAIL on the public comparison.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/docs_drift.rs docs/monitoring.md docs/explorations/phase4-k8s-deployment.md`
  `git commit -m "docs(monitoring): health routes, the probe recipe and the /status reference, guarded"`

---
### Task 8.11: The Foundations config keys are documented under the spellings the parser reads

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Four keys arrive in PRs 4-6 (spec §13 table). The existing `the_wab_dir_lock_key_is_in_the_configuration_reference` (docs_drift.rs:795-825) shows the pattern: check the spellings against the parser, then the reference, then the example file that "claims to list every option". This task generalises it to the four new keys and documents whatever the guard finds missing.

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs` (one const, one struct, one helper, one test; the existing `wab_dir_lock` test is left unchanged)
- Modify: `docs/operations/configuration.md`: add only what the guard reports missing, to the existing `#### \`drain_shutdown_budget_secs\`` (PR 4), `#### \`livez_checks\`` (PR 5) and `#### \`admin_socket_enabled\`` / `#### \`admin_socket_path\`` (PR 6) sections; a `### Health and admin` section only if one of those sections is absent. Also `#### \`wab_max_bytes\`` (append the pause paragraph, spec §17 "pause's disk warning").
- Modify: `deploy/docker/weir.toml.example` (after the `shutdown_timeout_secs` entry, line ~123)

**Interfaces:**
- Consumes: PR 4 (`drain_shutdown_budget_secs`, `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`, `--drain-shutdown-budget-secs`, range 0..=3600, default 10); PR 5 (`livez_checks`, `WEIR_LIVEZ_CHECKS`, `--livez-checks a,b`, subset of `no_live_shard|listener_down|worker_exited|drain_stopped|drain_wedged`, default `[]`); PR 6 (`admin_socket_enabled`, `WEIR_ADMIN_SOCKET_ENABLED`, no CLI flag, default `true`; `admin_socket_path`, `WEIR_ADMIN_SOCKET_PATH`, `--admin-socket-path`). Each key listed in `BASE_SERVER_KEYS` of `src/config/file.rs` (lines 99-127) and read in `src/config/env.rs` / `src/config/cli.rs` as string literals, as `WEIR_WAB_DIR_LOCK` and `--wab-dir-lock` are.
- Produces: docs_drift `const CONFIG_FILE_SRC: &str`, `struct KeySpelling { key: &'static str, env: &'static str, cli: Option<&'static str> }`, `const FOUNDATIONS_KEYS: [KeySpelling; 4]`, `const LIVEZ_SELECTABLE: [&str; 5]`, `fn configuration_section(key: &str) -> Option<&'static str>`, test `the_foundations_config_keys_are_documented_under_the_spellings_the_parser_reads`.

- [ ] **Step 1: Write the failing test.** Add after the other consts:

```rust
const CONFIG_FILE_SRC: &str = include_str!("../src/config/file.rs");
```

Append:

```rust
/// A config key and the spellings an operator types for it.
struct KeySpelling {
    key: &'static str,
    env: &'static str,
    /// `None` where the spec gives the key no flag (`admin_socket_enabled`).
    cli: Option<&'static str>,
}

/// The keys Foundations added (spec §13).
const FOUNDATIONS_KEYS: [KeySpelling; 4] = [
    KeySpelling {
        key: "drain_shutdown_budget_secs",
        env: "WEIR_DRAIN_SHUTDOWN_BUDGET_SECS",
        cli: Some("--drain-shutdown-budget-secs"),
    },
    KeySpelling {
        key: "admin_socket_enabled",
        env: "WEIR_ADMIN_SOCKET_ENABLED",
        cli: None,
    },
    KeySpelling {
        key: "admin_socket_path",
        env: "WEIR_ADMIN_SOCKET_PATH",
        cli: Some("--admin-socket-path"),
    },
    KeySpelling {
        key: "livez_checks",
        env: "WEIR_LIVEZ_CHECKS",
        cli: Some("--livez-checks"),
    },
];

/// The checks `livez_checks` accepts (spec §12).
const LIVEZ_SELECTABLE: [&str; 5] = [
    "no_live_shard",
    "listener_down",
    "worker_exited",
    "drain_stopped",
    "drain_wedged",
];

/// The body of `#### \`<key>\`` in configuration.md, up to the next heading or
/// rule, or `None` when there is no such heading.
fn configuration_section(key: &str) -> Option<&'static str> {
    let marker = format!("\n#### `{key}`\n");
    let start = CONFIGURATION.find(&marker)? + marker.len();
    let rest = &CONFIGURATION[start..];
    let end = ["\n#### ", "\n### ", "\n## ", "\n---"]
        .iter()
        .filter_map(|m| rest.find(m))
        .min()
        .unwrap_or(rest.len());
    Some(&rest[..end])
}

/// Each Foundations key is parsed under the spellings the docs give, has its
/// own section in the configuration reference naming them, and is in the
/// example file that claims to list every option.
///
/// The parser is checked first, so a rename in the code fails here instead
/// of leaving the docs naming a key the daemon ignores (an unknown TOML key
/// only warns). `admin_socket_path` is also what `weir-ctl` reads from
/// `WEIR_ADMIN_SOCKET_PATH`, so a drifted spelling breaks the client too.
#[test]
fn the_foundations_config_keys_are_documented_under_the_spellings_the_parser_reads() {
    let mut problems = Vec::new();
    for k in &FOUNDATIONS_KEYS {
        if !CONFIG_FILE_SRC.contains(&format!("\"{}\"", k.key)) {
            problems.push(format!("{}: not a known TOML key in src/config/file.rs", k.key));
        }
        if !CONFIG_ENV_SRC.contains(&format!("\"{}\"", k.env)) {
            problems.push(format!("{}: src/config/env.rs does not read {}", k.key, k.env));
        }
        if let Some(flag) = k.cli {
            if !CONFIG_CLI_SRC.contains(&format!("\"{flag}\"")) {
                problems.push(format!("{}: src/config/cli.rs does not parse {flag}", k.key));
            }
        }
        match configuration_section(k.key) {
            None => problems.push(format!(
                "{}: no `#### `{}`` section in docs/operations/configuration.md",
                k.key, k.key
            )),
            Some(section) => {
                if !section.contains(&format!("`{}`", k.env)) {
                    problems.push(format!("{}: its section does not name `{}`", k.key, k.env));
                }
                match k.cli {
                    Some(flag) if !section.contains(&format!("`{flag}")) => problems
                        .push(format!("{}: its section does not name `{flag}`", k.key)),
                    None if !section.contains("**CLI**: none") => problems
                        .push(format!("{}: its section must say `**CLI**: none`", k.key)),
                    _ => {}
                }
            }
        }
        for needle in [k.key, k.env] {
            if !CONFIG_EXAMPLE.contains(needle) {
                problems.push(format!("deploy/docker/weir.toml.example: missing {needle}"));
            }
        }
    }
    if let Some(livez) = configuration_section("livez_checks") {
        for check in LIVEZ_SELECTABLE {
            if !livez.contains(&format!("`{check}`")) {
                problems.push(format!("livez_checks: its section does not list `{check}`"));
            }
        }
    }
    // Spec §17: "pause's disk warning". A pause delivers nothing while ingest
    // keeps writing, and the cap is the only thing between that and a full disk.
    if !configuration_section("wab_max_bytes")
        .is_some_and(|s| s.contains("**While delivery is paused**"))
    {
        problems.push(
            "wab_max_bytes: its section does not warn that the WAB grows while \
             delivery is paused"
                .to_string(),
        );
    }
    assert!(
        problems.is_empty(),
        "the Foundations config keys are not documented as the parser reads them:\n  {}",
        problems.join("\n  ")
    );
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 the_foundations_config_keys_are_documented_under_the_spellings_the_parser_reads`
  Expected: FAIL listing at least `deploy/docker/weir.toml.example: missing livez_checks` and `... missing WEIR_LIVEZ_CHECKS` (PR 4 added drain_shutdown_budget_secs, PR 6 the commented admin keys; PR 5 does not touch the example file), and `wab_max_bytes: its section does not warn that the WAB grows while delivery is paused`, plus any section PRs 4-6 left out. Any `src/config/*.rs` line in the list means a PR shipped a different spelling than spec §13: stop and reconcile with that PR's author before changing either side.

- [ ] **Step 3: Implement.** For each problem reported, add the matching block below; where a section already exists, keep its text and add only the missing line.

`docs/operations/configuration.md`, under `### Shutdown` after `#### \`shutdown_timeout_secs\``'s text and before `#### Shutdown grace period` (PR 4 Task 4.13's section, rewritten by Task 8.1); PR 4 Task 4.1 already wrote this section, so this block is reference text for a missing line only:

```markdown
#### `drain_shutdown_budget_secs`

- **Type**: u64 (seconds)
- **Default**: `10`
- **Range**: 0–3600
- **CLI**: `--drain-shutdown-budget-secs <n>`
- **Env**: `WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`
- **TOML**: `drain_shutdown_budget_secs`

How long the drain keeps delivering after a SIGTERM, counted from the moment
every flusher has sealed its last segment. Whole segments are delivered
until the budget runs out; then the in-flight segment is abandoned without a
`.confirmed` sidecar, and it and everything behind it replay at the next
start.

**Duplicates.** At most one segment per shutdown is abandoned mid-delivery.
On replay its already-committed prefix is delivered again (one more batch, if
the sink applied a commit the deadline cancelled), and records that prefix
dead-lettered are dead-lettered again, counting twice against
`dead_letter_max_bytes`. This is the same window a crash has. A segment a
flusher could not hand off before its 1 s limit was never started, so it adds
no duplicates.

`0` delivers nothing more at shutdown. Raise it, up to 3600, to drain more on
a planned stop, and raise the process manager's grace by the same amount
([Shutdown grace period](#shutdown-grace-period)).
```

`docs/operations/configuration.md`, only if one of these three sections is absent (PR 5 Task 5.11 writes `#### \`livez_checks\`` under `### Metrics endpoint`, and PR 6 Task 6.17 writes both admin sections after `wab_dir_lock`; where a section exists, add only the line the guard reports): a new section after `### Metrics endpoint`'s last key (`metrics_max_connections`) and before the `---` that precedes `### Shutdown`, with just the missing sections:

```markdown
---

### Health and admin

#### `livez_checks`

- **Type**: list of check names
- **Default**: `[]`
- **Values**: any of `no_live_shard`, `listener_down`, `worker_exited`, `drain_stopped`, `drain_wedged`
- **CLI**: `--livez-checks no_live_shard,listener_down`
- **Env**: `WEIR_LIVEZ_CHECKS` (comma-separated)
- **TOML**: `livez_checks`

The conditions that make `GET /livez` answer 503. With the default `[]`,
`/livez` fails only when the process stops answering. Every check is a state
a restart fixes, and each is evaluated only while the daemon is serving, so
`/livez` never fails during startup, shutdown, a sink outage, a retry, a
blocked or a paused drain. Recommended: `no_live_shard`, `listener_down`,
`worker_exited` (ingest is dead). `drain_stopped` and `drain_wedged` restart
weir over a delivery fault; select them deliberately. See [Health and status
endpoints](../monitoring.md#health-and-status-endpoints).

#### `admin_socket_enabled`

- **Type**: bool
- **Default**: `true`
- **CLI**: none
- **Env**: `WEIR_ADMIN_SOCKET_ENABLED`
- **TOML**: `admin_socket_enabled`

Serves the admin control socket (Unix only) that `weir-ctl status`,
`weir-ctl reload` and `weir-ctl drain pause|resume|retry-stranded` talk to.
`false` turns it off; `weir-ctl status` then falls back to HTTP and disk, and
`kill -HUP` still reloads TLS.

#### `admin_socket_path`

- **Type**: path
- **Default**: `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`, e.g. `/var/lib/weir/wab.admin.sock`
- **Rules**: absolute, no `..`, at most 103 bytes, not under `wab_dir`, not the same socket as `socket_path`
- **CLI**: `--admin-socket-path <path>`
- **Env**: `WEIR_ADMIN_SOCKET_PATH` (`weir-ctl` reads the same variable)
- **TOML**: `admin_socket_path`

The socket is created mode 0600, and every connection must come from the
daemon's own uid or root, whatever `peer_uid_check` says. The default sits
beside `wab_dir`, not in the producer socket's directory, because that
directory is shared with producers by design (the image's `/run/weir`
volume), and producers often run as the daemon's uid. Its parent directory
must not be group- or world-writable and must be owned by the daemon's uid or
root: the daemon refuses to bind there. With
an explicit path that refusal, or any bind failure, stops startup; with the
derived default it disables the socket, logs an ERROR and raises the
`admin_unavailable` condition. On Kubernetes with a read-only root
filesystem, set it to a private `emptyDir` that is not mounted into producer
containers.
```

`docs/operations/configuration.md`, at the end of `#### \`wab_max_bytes\`` (after its `**When to tune**` paragraph, before the `---` that precedes `#### \`wab_compression\``), append. This one is not guard-driven; spec §17 lists it ("pause's disk warning"), and no earlier PR adds it to configuration.md (PR 6 and PR 7 carry it only in the `drain.pause` reply):

```markdown
**While delivery is paused** (`weir-ctl drain pause`) the drain delivers
nothing and the WAB grows at the ingest rate. With a cap, pushes start to
nack at the cap (`wab_cap_rejecting`, `/readyz` 503); with `wab_max_bytes = 0`
the WAB grows until the disk fills. A pause lives in memory only: a restart
resumes delivery.
```

`deploy/docker/weir.toml.example`, after the `shutdown_timeout_secs = 30` line (first run `grep -nE '^(# )?(drain_shutdown_budget_secs|admin_socket_enabled|admin_socket_path|livez_checks)' deploy/docker/weir.toml.example` and skip any key already present: a duplicate key makes the file, which compose mounts as the live config, fail to parse):

```toml

# How long the drain keeps delivering after SIGTERM, counted once every flusher
# has sealed its last segment. What is left replays at the next start.
# Range: 0-3600. Env: WEIR_DRAIN_SHUTDOWN_BUDGET_SECS
drain_shutdown_budget_secs = 10

# The admin control socket (Unix only) behind `weir-ctl status`, `reload` and
# `drain pause|resume|retry-stranded`: mode 0600, daemon uid or root only.
# Env: WEIR_ADMIN_SOCKET_ENABLED
admin_socket_enabled = true

# Default: <parent(wab_dir)>/<basename(wab_dir)>.admin.sock, here
# /var/lib/weir/wab.admin.sock. Absolute, at most 103 bytes, outside wab_dir,
# in a directory that is not group- or world-writable and is owned by the
# daemon user or root.
# Env: WEIR_ADMIN_SOCKET_PATH (weir-ctl reads it too)
# admin_socket_path = "/var/lib/weir/wab.admin.sock"

# Checks that make GET /livez answer 503. Default [] (fails only when the
# process stops answering). Recommended: no_live_shard, listener_down,
# worker_exited; also drain_stopped, drain_wedged.
# Env: WEIR_LIVEZ_CHECKS (comma-separated)
# livez_checks = ["no_live_shard", "listener_down", "worker_exited"]
```

(`admin_socket_path` and `livez_checks` stay commented: the default path is derived from `wab_dir`, and the shipped default for liveness is `[]`. `livez_checks` is a TOML list: PR 5 Task 5.3 maps `Option<Vec<String>>`.)

- [ ] **Step 4: Run to verify it passes.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS (including Task 8.1's guard, which reads PR 4's `drain_shutdown_budget_secs = 10` from the example file).
  `cargo test -p weir-server --test system -- --test-threads=1` → PASS (the system suite parses configs shaped like the example; a TOML error in it would surface in the compose smoke test of Task 8.14).

- [ ] **Step 5: Verify by mutation.** Revert each.
  1. In configuration.md, `WEIR_LIVEZ_CHECKS` → `WEIR_LIVEZ_CHECK`: FAIL `livez_checks: its section does not name \`WEIR_LIVEZ_CHECKS\``.
  2. Delete `` `drain_wedged` `` from the `livez_checks` Values line and paragraph: FAIL.
  3. Delete the `# admin_socket_enabled = true` line (PR 6's) from the example: FAIL `missing admin_socket_enabled` (PR 6's comment above it spells only the upper-case `WEIR_ADMIN_SOCKET_ENABLED`, which does not match).
  4. In `#### \`wab_max_bytes\``, change `**While delivery is paused**` to `While delivery is paused`: FAIL `wab_max_bytes: its section does not warn`.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/docs_drift.rs docs/operations/configuration.md deploy/docker/weir.toml.example`
  `git commit -m "docs(config): the Foundations keys, guarded against the spellings the parser reads"`

---

### Task 8.12: The threat model names the health listener and the admin socket

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Spec §17 lists explicit `threat-model.md` edits, and §16 asks for a guard on "the presence of the HTTP-listener and admin-socket rows and actors". PR 5 ("threat model") and PR 6 may have added rows already; this task pins the row and actor titles below and rewrites the text that went stale. The "Stale socket files" row already states the stricter probe outcome (threat-model.md:87); PR 6 appends the admin-socket sentence. The "still looks live" residual window is rewritten below (lame duck). The same stale shutdown claim is fixed in socket-bind.md and container.md, and tcp-mtls.md gets the admin `reload` verb (spec §17 "tcp-mtls.md: reload"; no earlier PR edits that passage).

**Files:**
- Test: `crates/weir-server/tests/docs_drift.rs` (one const, one test)
- Modify: `docs/security/threat-model.md` (Unix socket bullets lines 25-31; In-scope table rows "Stale socket files" line 87 and "Stuck pipeline on shutdown" line 94, plus three new rows; the residual-window bullets at lines 185-194 and 217-222; "Threat actors considered" lines 295-314)
- Modify: `docs/security/socket-bind.md:274-278` (the shutting-down sentences under "Side effects")
- Modify: `docs/security/container.md:164-166` (the Kubernetes `RollingUpdate` lock-overlap bound)
- Modify: `docs/operations/tcp-mtls.md:171-196` (`## Cert rotation via SIGHUP` step 2, and the "One reload path" paragraph's last sentence)

**Interfaces:**
- Consumes: the admin socket design (spec §5.9, §11), HTTP routes and pools (§5.8, §10), bounded shutdown (§7), as built by PRs 4-6.
- Produces: docs_drift `const THREAT_MODEL: &str`, test `the_threat_model_covers_the_health_listener_and_the_admin_socket`; the pinned titles `| Network peer reading the metrics, health and status listener |`, `| Network peer starving the health probes |`, `| Local process reaching the admin socket |`, `- **Network peer reaching the metrics/health port**`, `- **Producer or sidecar sharing the socket volume**`.

- [ ] **Step 1: Write the failing test.** Add the const and append the test:

```rust
const THREAT_MODEL: &str = include_str!("../../../docs/security/threat-model.md");

/// The threat model covers the two surfaces Foundations added, and no longer
/// describes the shutdown it replaced.
///
/// Spec §17 lists these edits because each surface changes who can learn or
/// do what: an unauthenticated listener that may bind 0.0.0.0 in Kubernetes,
/// and a control socket that pauses delivery. A threat model that predates
/// them is the document a reviewer trusts most and should trust least.
#[test]
fn the_threat_model_covers_the_health_listener_and_the_admin_socket() {
    let section = |heading: &str| -> &'static str {
        let marker = format!("\n## {heading}\n");
        let start = THREAT_MODEL
            .find(&marker)
            .unwrap_or_else(|| panic!("threat-model.md has no `## {heading}`"))
            + marker.len();
        let rest = &THREAT_MODEL[start..];
        &rest[..rest.find("\n## ").unwrap_or(rest.len())]
    };
    let in_scope = section("In scope");
    let actors = section("Threat actors considered");
    let mut problems = Vec::new();

    for row in [
        "| Network peer reading the metrics, health and status listener |", // PR 5 Task 5.11
        "| Network peer starving the health probes |",                    // PR 5 Task 5.11
        "| Local process reaching the admin socket |",                     // PR 6 Task 6.17
    ] {
        if !in_scope.contains(row) {
            problems.push(format!("In scope: no row {row:?}"));
        }
    }
    for actor in [
        "- **Network peer reaching the metrics/health port**",
        "- **Producer or sidecar sharing the socket volume**",
    ] {
        if !actors.contains(actor) {
            problems.push(format!("Threat actors: no {actor:?}"));
        }
    }
    if !actors.contains("producer containers that share the daemon's uid") {
        problems.push(
            "Threat actors: the daemon-uid actor must be qualified for producer \
             containers that share the daemon's uid"
                .to_string(),
        );
    }
    if !THREAT_MODEL.contains("The admin socket deliberately also admits root") {
        problems.push(
            "the Unix socket section must reconcile \"Root is not implicitly trusted\" \
             with the admin socket's root allowance"
                .to_string(),
        );
    }
    for stale in [
        "the runtime is dropped before joining threads",
        "A daemon that is shutting down still looks live",
    ] {
        if THREAT_MODEL.contains(stale) {
            problems.push(format!("still describes the old shutdown: {stale:?}"));
        }
    }
    assert!(
        problems.is_empty(),
        "docs/security/threat-model.md does not cover what Foundations added:\n  {}",
        problems.join("\n  ")
    );
}
```

- [ ] **Step 2: Run it to verify it fails.**
  `cargo test -p weir-server --test docs_drift -- --test-threads=1 the_threat_model_covers_the_health_listener_and_the_admin_socket` → FAIL with `still describes the old shutdown: "the runtime is dropped before joining threads"` and `still describes the old shutdown: "A daemon that is shutting down still looks live"` (PR 5 and PR 6 already wrote the rows, actors, root bullet and daemon-uid qualification under these exact titles; if any is reported missing, the owning PR's text was not merged as planned: add that PR's text, not new text).

- [ ] **Step 3: Implement** in `docs/security/threat-model.md`: replace the "Stuck pipeline on shutdown" row and the two residual-window passages (below), then the socket-bind.md, container.md and tcp-mtls.md edits that follow. The rest of this step's text is the reference wording the guard was written against; PR 5 Task 5.11 and PR 6 Task 6.17 already added their equivalents (the three rows, the two actor bullets, the root bullet, the daemon-uid qualification, and the admin-socket sentence in "Stale socket files"), so do **not** add them a second time. Use the blocks below only to fill a gap the guard reports.

Reference (already added by PR 6 Task 6.17) — after the bullet ending `discouraged; see the assumptions below.)` (line 31):

```markdown
- The admin socket deliberately also admits root (euid 0) besides the
  daemon's own uid: `sudo weir-ctl drain pause` is an expected way to call
  it. This diverges from the producer rule above on purpose. Root can already
  read and rewrite the WAB, so admitting it to pause delivery or reload TLS
  grants nothing new.
```

Replace the `| Stuck pipeline on shutdown | ... |` row with:

```markdown
| Stuck pipeline on shutdown | Shutdown is bounded. Listeners close at once (the socket file is unlinked, inode-checked); a connection in the middle of a frame finishes it and gets its ack, for at most `shutdown_timeout_secs`; an idle connection is closed at once. Each flusher waits at most 1 s to hand its last sealed segment to the drain, and the drain gets `drain_shutdown_budget_secs` before it abandons in-flight work, unconfirmed, for replay. The runtime outlives the worker and flusher joins, so `/metrics`, the probes and the admin `status` verb answer throughout, and the joins are never skipped. A drain stuck in a sink call that never yields is left behind: the process exits 0 and the kernel releases the `wab_dir` lock only once it is gone. A second SIGTERM shortens the connection drain and the drain wait, never the joins. |
```

In `## Single ownership of \`wab_dir\` and \`socket_path\``'s residual windows, replace the bullet starting `- **A daemon that is shutting down still looks live.**` (through `the old process to exit.`) with:

```markdown
- **A daemon that is shutting down still holds the `wab_dir` lock.** At the
  first SIGTERM it unlinks its socket and closes its listeners, so a new
  instance's probe finds nothing live; it refuses with "locked by" instead,
  until the old process exits, up to
  `shutdown_timeout_secs + drain_shutdown_budget_secs + 5` seconds later.
  Under systemd the old process has exited before the new one starts;
  elsewhere, wait for it to exit.
```

In the `- **Deleting \`.weir.lock\` under a running daemon.**` bullet, replace `a long startup replay (when the socket and metrics port are not bound yet and a duplicate with an identical config would pass preflight)` with `a long recovery (when the producer socket is not bound yet, so a duplicate with its own metrics port would pass preflight)`. (Recovery no longer replays into the channel, and the metrics listener now binds before recovery.)

`docs/security/socket-bind.md`, under **Side effects**, replace `A daemon that is shutting down keeps its listener open while in-flight connections drain (up to \`shutdown_timeout_secs\`), so a probe in that window sees it as live and the new instance refuses.` with:

```markdown
A daemon that is shutting down unlinks its socket at the first SIGTERM, so a
probe finds no socket. A new instance on the same `wab_dir` never gets that
far: it refuses on the `wab_dir` lock ("locked by"), which is taken before
the probe, until the old process exits.
```

(Checked against 3adc5f5: `main.rs` takes the lock at line 973, before `preflight::preflight` at line 1019.) Keep the following `Under systemd the old process has exited before the new one starts.` sentence.

`docs/security/container.md`, in the Kubernetes bullet, replace `which can take up to \`shutdown_timeout_secs\` after SIGTERM` with `which can take up to \`shutdown_timeout_secs + drain_shutdown_budget_secs + 5\` seconds after SIGTERM`.

`docs/operations/tcp-mtls.md`, in the **One reload path** paragraph, replace `The same loop will` / `serve the admin socket's \`reload\` verb later (\`trigger="admin"\` is reserved).` with:

```markdown
The admin socket's `reload` verb (`weir-ctl reload`) goes through the same
loop, is counted as `trigger="admin"`, and returns its outcome (`applied`,
`noop` or `failed`) with a per-item report instead of only a log line. A
failed reload keeps the previous certificates either way.
```

and in `## Cert rotation via SIGHUP` step 2, append to the code block after the `docker kill --signal=HUP <container>` line:

```bash
   # or through the admin socket, which prints whether the new certificates
   # were applied:
   sudo weir-ctl reload
```

Reference only (PR 6 Task 6.17 already appended its equivalent): the `| Stale socket files | ... |` row ends with ` The admin socket goes through the same probe and the same own-inode-only removal.`

Reference only (added by PR 5 Task 5.11 and PR 6 Task 6.17) — the rows after the table's last row (`| Plaintext TCP fallback | ... |`):

```markdown
| Network peer reading the metrics, health and status listener | The listener (`metrics_bind:metrics_port`, default `127.0.0.1:9185`) is unauthenticated and read-only: no route reads a body or changes state. `/status` serves `PublicStatusDoc`, a separate type with no free-text field (no error strings, panic messages, paths, uids, pids, sink URLs, hosts or buckets), so the redaction is enforced by the compiler. A sentinel test plants a marker in every admin-only field and planted credentials, and asserts it never reaches `/status`, `/livez?verbose` or `/readyz?verbose`. Keep the listener on loopback, or restrict the port (a Kubernetes NetworkPolicy to kubelet and Prometheus). |
| Network peer starving the health probes | A client must send its whole request head within 1 s while holding one of 64 head slots; the probe and status routes then draw from their own pool of 4 permits, separate from `metrics_max_connections`, so a scrape storm cannot starve kubelet and a probe flood cannot starve Prometheus. Drops are counted in `weir_http_requests_dropped_total{reason}`. This reduces starvation by a determined attacker on the network path; it does not eliminate it. Loopback binding or a NetworkPolicy remains the boundary, and the recommended liveness `failureThreshold` of 6 absorbs a minute of pressure. |
| Local process reaching the admin socket | The admin socket (default `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`) is bound by `bind_hardened` at mode 0600, never in the producer socket's directory, and refused outright when its parent is group- or world-writable. Every accepted connection must come from the daemon's euid or root, checked on each accept independently of `peer_uid_check` and fail-closed without peer credentials; a refusal increments `weir_admin_requests_total{verb="_connect",outcome="refused"}`. Framing is bounded (64 KiB lines, 5 s per request, 64 requests and 4 connections). It changes only drain intent (pause, resume, retry stranded) and reloads TLS; it carries no secrets. `weir-ctl` authenticates the server in turn: socket type and mode, parent permissions, the peer uid, and the `wab_dir` (dev, ino) the daemon reports. |
```

Reference only (added by PR 5 Task 5.11 and PR 6 Task 6.17) — the three actor bullets that replace the `- **Compromised process running as the daemon uid**` bullet (lines 298-299):

```markdown
- **Network peer reaching the metrics/health port**: can read `/metrics` and
  the public `/status` (no free text, no secrets) and can try to exhaust the
  probe pools; cannot change anything. Bind to loopback, or restrict the
  port with a firewall or NetworkPolicy.
- **Producer or sidecar sharing the socket volume**: has producer authority
  only. The admin socket is not on that volume: by default it lives beside
  `wab_dir`, whose parent is never shared with producers, so a producer
  running as the daemon's uid (as container.md recommends for
  `peer_uid_check`) still cannot pause delivery or reload TLS.
- **Compromised process running as the daemon uid**: out of scope.
  Equivalent to the daemon itself. Defense is OS-level isolation. This does
  not extend to producer containers that share the daemon's uid only so that
  `peer_uid_check` admits them: they reach the producer socket, never the
  admin socket or the WAB, as long as the `wab_dir` parent is not mounted
  into them.
```

- [ ] **Step 4: Run to verify it passes.** `cargo test -p weir-server --test docs_drift -- --test-threads=1` → all PASS.

- [ ] **Step 5: Verify by mutation.** Revert each.
  1. Delete the `| Local process reaching the admin socket |` row: FAIL.
  2. Restore the old "Stuck pipeline" row text: FAIL `still describes the old shutdown`.
  3. Restore the old residual-window bullet title `- **A daemon that is shutting down still looks live.**`: FAIL `still describes the old shutdown`.

- [ ] **Step 6: Format.** `cargo fmt --all`, then re-run the Step 4 commands → all PASS.

- [ ] **Step 7: Commit.**
  `git add crates/weir-server/tests/docs_drift.rs docs/security/threat-model.md docs/security/socket-bind.md docs/security/container.md docs/operations/tcp-mtls.md`
  `git commit -m "docs(security): the threat model covers the health listener, the admin socket and the lame duck"`

---

### Task 8.13: CHANGELOG — the Foundations release notes

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

Spec §17 lists ten behaviour changes and what must be release-noted. This task writes them, plus the deploy changes of this PR, under `## [Unreleased]`.

**Placement.** The owner's ship-order decision releases the safety floor first, so at PR 8 time `## [Unreleased]` is expected to be empty or Foundations-only. Check with `sed -n '16,20p' CHANGELOG.md`:
- If the line after `## [Unreleased]` starts `The safety floor:`, the safety floor is still unreleased: append the Foundations intro paragraph below as a second paragraph after the safety-floor one, and append each bullet group to the end of the existing `### Added`, `### Changed` and `### Fixed` lists.
- Otherwise insert the block below directly under `## [Unreleased]`.
Earlier PRs may have added bullets for their own pieces: before adding each bullet, `grep -nF '<its first backticked term>' CHANGELOG.md` within the Unreleased section, and merge rather than duplicate.

**Files:**
- Modify: `CHANGELOG.md` (the `## [Unreleased]` section, line 16)

**Interfaces:**
- Consumes: every name produced by PRs 1-8 as listed in the spec.
- Produces: release notes; no code.

- [ ] **Step 1: Write the failing check.** Save as `$SCRATCH/changelog-check.sh` (scratch space, not the repository):

```bash
#!/usr/bin/env bash
# Every Foundations item an operator meets on upgrade is in [Unreleased].
set -euo pipefail
unreleased="$(awk '/^## \[Unreleased\]/{on=1; next} /^## \[/{on=0} on' CHANGELOG.md)"
missing=0
for needle in '/readyz' '/livez' '/startupz' 'weir.status/v1' 'drain_shutdown_budget_secs' \
  'admin_socket_enabled' 'admin_socket_path' 'livez_checks' 'weir-ctl status' \
  'weir-ctl drain pause' 'weir-ctl reload' 'stop_grace_period: 45s' 'TimeoutStopSec' \
  'weir-readiness.sh' '--fail-on-critical' 'WeirNotReady' 'WeirConditionCritical' \
  'WeirSegmentQuarantined' 'weir-status' 'shard_count = 1' 'HEALTHCHECK'; do
  grep -qF -- "$needle" <<<"$unreleased" || { echo "MISSING: $needle"; missing=1; }
done
exit "$missing"
```

- [ ] **Step 2: Run it to verify it fails.** `bash "$SCRATCH/changelog-check.sh"` from the repository root → prints `MISSING:` lines (at least `stop_grace_period: 45s`, `--fail-on-critical`, `WeirNotReady`) and exits 1.

- [ ] **Step 3: Implement.** Insert (per Placement above):

```markdown
Foundations: weir can be observed and steered while it runs, and it starts
and stops within bounds. Health routes and a typed status document say *why*
a daemon is not ready, an admin socket pauses and resumes delivery, startup
no longer waits on the sink, and shutdown is bounded. The wire protocol, the
WAB format, `HealthCheck`, every existing metric and every existing `--json`
shape (including `weir-ctl attest`) are unchanged. Downgrading is safe:
older releases warn about and ignore the new config keys, and never read
`wab.admin.sock`.

### Added

- **Health and status routes** on the metrics listener: `/livez`, `/readyz`,
  `/startupz` and `/status` (the public `weir.status/v1` JSON document). They
  answer within milliseconds of start, through recovery and through
  shutdown. Probes get their own connection pool, so a scrape storm cannot
  starve them. Every other path still returns the metrics, byte for byte.
- **Conditions.** One evaluator, in the new `weir-status` crate, turns the
  daemon's state into typed condition codes (30 of them, from
  `wab_cap_rejecting` to `drain_backpressure`), each with a severity, what it
  affects, and whether it gates readiness. `/status`, `/readyz`, `/livez`,
  `weir_condition_active{code,severity}`, `weir-readiness.sh` and
  `weir-ctl status` all read it, so they agree. Runbooks for every code are in
  `docs/monitoring.md#conditions`.
- **An admin socket**, `<parent(wab_dir)>/<basename(wab_dir)>.admin.sock`
  (`/var/lib/weir/wab.admin.sock` in the image and the systemd unit), mode
  0600, daemon uid or root only. Verbs: `status`, `reload` (TLS, the same
  path as SIGHUP), `drain.pause`, `drain.resume`, `drain.retry_stranded`.
  A pause stops delivery before the next commit batch, keeps ingest
  buffering, and is in memory only: a restart resumes.
- **`weir-ctl status`** (admin socket, then `/status`, then metrics and disk;
  exit 0 ready, 1 not ready, 2 unreachable, 3 ready with a critical
  condition), **`weir-ctl reload`**, and **`weir-ctl drain pause`**,
  `resume` and `retry-stranded`.
- **Config keys**: `drain_shutdown_budget_secs` (default 10),
  `admin_socket_enabled` (default true), `admin_socket_path`, `livez_checks`
  (default none).
- **Metrics**, all new families: build and lifecycle (`weir_build_info`,
  `weir_startup_phase`, `weir_ready`, `weir_live`, `weir_condition_active`),
  shards and workers, WAB backlog age, drain channel depth and pause, sink
  success and failure times, admin requests, and the effective config. See
  the metric reference in `docs/monitoring.md`.
- **Alert rules**, each with `promtool` tests in both directions and a
  runbook: `WeirNotReady`, `WeirConditionCritical`, `WeirDrainPausedLong`,
  `WeirOldestUndeliveredAge`, `WeirDrainBackpressure`,
  `WeirAdminUnavailable`.

### Changed

- **Shutdown is bounded.** After the flushers join, the drain gets
  `drain_shutdown_budget_secs` (default 10 s) to deliver; the rest stays
  sealed and replays at the next start. A flusher blocked on a full drain
  channel gives up its hand-off after 1 s, and that segment replays too. At
  most one segment per shutdown is abandoned mid-delivery, and its committed
  prefix is delivered again on replay (records it dead-lettered are
  dead-lettered again). To decommission a host, wait for an empty backlog in
  `weir-ctl status` first, or raise the budget. A second SIGTERM forces the
  shutdown.
- **Startup no longer waits on the sink.** The replay backlog is handed to
  the drain in memory and interleaves with new traffic. A daemon restarted
  over `wab_max_bytes` is not ready from its first probe.
- **Lame duck.** On SIGTERM the producer socket file is unlinked and
  `/readyz` answers 503 at once; a connection in the middle of a frame still
  finishes it and gets its ack, and an idle connection is closed at once.
- **New connections go only to live shards.**
- **Four reserved HTTP paths.** A metrics client must now send its request
  head within 1 s; Prometheus and curl do.
- **`weir-readiness.sh`** decides on `/readyz` (or `weir-ctl status` when
  installed). It no longer gates on sink health, drain state or lifetime
  counters, which are now `WARN` lines. Exit 1 means exactly "`/readyz`
  answers 503"; exit 3 needs `--fail-on-critical`. `--socket` is ignored.
- **Deploy defaults.** The image's `HEALTHCHECK` sends `GET /livez` instead
  of opening a TCP connection, with a 10 s start period. Compose sets
  `stop_grace_period: 45s` and the systemd unit `TimeoutStopSec` 35 → 45:
  the grace is `shutdown_timeout_secs + drain_shutdown_budget_secs + 5`.
  Raise it with either key. `ExecReload=` is still absent on purpose.
- **`WeirSegmentQuarantined`** now fires on `weir_quarantine_bytes_on_disk >
  0`, so it keeps firing across a restart until the quarantine is dealt with.
  `WeirAckTimeout` and `WeirQueueSaturation` now point at the drain hand-off
  before storage.
- **Build.** `arc-swap` is always compiled. The flusher's "drain channel
  closed" WARN is INFO while shutting down.

### Fixed

- **With `shard_count = 1`, a dead flusher left a zombie daemon**: the drain
  exited as if shutting down, so nothing was ingested or delivered while the
  process still ran. The drain's lifetime is now explicit; it keeps
  delivering the on-disk backlog until told to stop.
- **A large backlog behind a slow sink kept the producer socket and
  `/metrics` down during startup**, so `WeirInstanceDown` fired through every
  recovery.
- **`weir-readiness.sh` stayed NOT READY until a restart** after one
  recovered flusher panic or one fsync failure, because it gated on lifetime
  counters.
- **The Docker `HEALTHCHECK` passed for anything listening on 9185.**
- **ACK_TIMEOUT nacks of durable records were read as a storage problem.**
  They are usually the drain channel backing up into the flushers; the
  `drain_backpressure` condition and `WeirDrainBackpressure` now say so.
- **The `wab_max_bytes` cap flag only cleared on the next push**, and a daemon
  restarted over the cap admitted pushes until its first background scan.
  The cap is now evaluated at startup, before any listener, and by the poll
  task.
- **A second SIGTERM was swallowed**, and `/metrics` died at the start of
  shutdown instead of the end.
```

- [ ] **Step 4: Run to verify it passes.** `bash "$SCRATCH/changelog-check.sh"` → no output, exit 0.

- [ ] **Step 5: Commit.**
  `git add CHANGELOG.md`
  `git commit -m "docs(changelog): Foundations, with the deploy defaults and alert changes"`

---

### Task 8.14: Merge gate — the real workflows, locally, before the push

*PR-level merge gate: PR 8's merge gate (header block of the PR 8 section), executed in Task 8.14.*

No new code. This task runs the gate the spec (§16) and the repository's own rules require, in the order that catches the expensive failures first.

**Files:** none modified, unless a step fails.

**Interfaces:**
- Consumes: everything above; `.github/workflows/ci.yml` jobs `lint`, `test`, `monitoring`; `.github/workflows/docker.yml` job `smoke`; `.github/workflows/docs.yml` job `build`.
- Produces: a PR with a body listing every guard and its mutation outcome.

- [ ] **Step 1: Rust gates, as CI runs them.**
  `cargo fmt --check`
  `cargo clippy --all-targets -- -D warnings`
  `cargo clippy --all-targets --all-features -- -D warnings`
  `cargo clippy --all-targets --no-default-features -- -D warnings`
  `cargo clippy --all-targets --features s3-sink -- -D warnings`
  (the four clippy lines of ci.yml's `lint` job, whole workspace, no `-p`)
  `cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test deploy --test load --test load_tls --test tls_client --test tls_listener -- --test-threads=1`
  `cargo test -p weir-server --lib --test system --test docs_drift --test demo_drift --test deploy --test load --test load_tls --test tls_client --test tls_listener --all-features -- --test-threads=1`
  Expected: all PASS. If a `docs_drift` vintage guard fails (memory: the first PR after a release always trips them), refresh the figures and the prose reasoning built on them in the same PR, not just the numbers.

- [ ] **Step 2: Alert rules.**
  `docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/r:ro" -w /r prom/prometheus:v3.14.0 check rules weir-alerts.yml` → `SUCCESS: 28 rules found`.
  `docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/r:ro" -w /r prom/prometheus:v3.14.0 test rules weir-alerts_test.yml` → `SUCCESS`.

- [ ] **Step 3: The shell.** `bash -n deploy/systemd/weir-readiness.sh`; and, if installed, `shellcheck deploy/systemd/weir-readiness.sh` → clean. (Not added to CI: no new tool dependency for one file the deploy tests already execute.)

- [ ] **Step 4: The real workflows with act, including the container jobs** (memory: the cargo gate is Rust-only and misses them; refresh the crates.io index first if `cargo-deny` runs):
  `act -W .github/workflows/ci.yml -j lint` (promtool, the systemd flock check)
  `act -W .github/workflows/ci.yml -j test`
  `act -W .github/workflows/ci.yml -j monitoring` (the compose stack with the new healthcheck: `weir` and `weir-chaos` must reach `healthy`)
  `act -W .github/workflows/docker.yml -j smoke` (the new wait for `healthy` in the built image)
  `act -W .github/workflows/docs.yml -j build` (the mdBook build; docs.yml runs on every PR that touches `docs/**`, which this one does)
  Expected: all green. Make sure act runs against this checkout's HEAD (memory: act can pass against a stale checkout).

- [ ] **Step 5: The §17 docs list, owned by earlier PRs.** These are not PR 8's edits, but the release notes promise them. Each command must print nothing; a hit blocks the merge and goes back to the owning PR:
  `grep -n 'CancellationToken' docs/architecture.md` (PR 4: the shutdown order rewrite)
  `grep -L 'admin socket' docs/security/socket-bind.md` (PR 6: the admin-socket section)
  `grep -L 'weir-ctl reload' docs/operations/tcp-mtls.md` (PR 8 Task 8.12)
  `grep -n 'stays ignored through WAB recovery' deploy/systemd/README.md` (PR 4: the signal task now runs before recovery, so the SIGHUP section's startup bullet must say so)

- [ ] **Step 6: Push and open the PR.** Before committing anything further, check `git log -5 --format=%B` carries no trailers. Push the branch, then:
  `gh pr create --title "Foundations PR 8: deploy defaults, alerts, runbooks and docs guards" --body-file "$SCRATCH/pr8-body.md"`
  where the body lists: the behaviour changes (from the header block), every new docs_drift guard with its Step-5 mutation and observed failure, the promtool and act results, and ends with:
  ```
  🤖 Generated with [Claude Code](https://claude.com/claude-code)
  ```
  Wait for every remote check, including Windows, before merging.
