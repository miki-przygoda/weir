//! Guards against published claims drifting away from the code and the data
//! they came from.
//!
//! Every test here exists because the drift it checks for actually happened.
//! The pattern is the same each time: a figure or a caveat is written by hand
//! into prose, the thing it describes moves, and nothing notices — the README
//! carried a two-release-old benchmark vintage for weeks, and
//! `docs/benchmarks.md` published a single-thread `Durable` rate as if it were
//! the daemon's ceiling.
//!
//! These are string assertions on documentation, which is unusual for a test
//! suite and deliberately so. Docs that make measurable claims are part of the
//! product; a claim nothing can falsify is one nobody maintains.

const README: &str = include_str!("../../../README.md");
const BENCH_INDEX: &str = include_str!("../../../docs/benchmarks.md");
const LATEST: &str = include_str!("../../../docs/benchmarks/latest.md");
const ENVIRONMENTS: &str = include_str!("../../../docs/benchmarks/environments.md");
const DRAIN: &str = include_str!("../../../docs/benchmarks/drain-throughput.md");
const DASHBOARD: &str = include_str!("../../../deploy/grafana/weir-dashboard.json");
const PLATFORMS: &str = include_str!("../../../docs/platform-support.md");
const RELEASE_YML: &str = include_str!("../../../.github/workflows/release.yml");
const PEER: &str = include_str!("../src/socket/peer.rs");
const HISTORY: &str = include_str!("../../../docs/benchmarks/history.md");
const MONITORING: &str = include_str!("../../../docs/monitoring.md");
const ALERTS: &str = include_str!("../../../deploy/prometheus/weir-alerts.yml");
const ALERTS_TEST: &str = include_str!("../../../deploy/prometheus/weir-alerts_test.yml");
const METRICS_SRC: &str = include_str!("../src/metrics/mod.rs");
const CONFIGURATION: &str = include_str!("../../../docs/operations/configuration.md");
const CONFIG_EXAMPLE: &str = include_str!("../../../deploy/docker/weir.toml.example");
const CONFIG_ENV_SRC: &str = include_str!("../src/config/env.rs");
const CONFIG_CLI_SRC: &str = include_str!("../src/config/cli.rs");
const CI_YML: &str = include_str!("../../../.github/workflows/ci.yml");
const CONFIG_MOD_SRC: &str = include_str!("../src/config/mod.rs");
const SYSTEMD_TOML: &str = include_str!("../../../deploy/systemd/weir.toml");
const DOCKER_IMAGE: &str = include_str!("../../../deploy/docker/Dockerfile");
const CONTAINER_MD: &str = include_str!("../../../docs/security/container.md");
const CONFIG_FILE_SRC: &str = include_str!("../src/config/file.rs");
const THREAT_MODEL: &str = include_str!("../../../docs/security/threat-model.md");

/// What a process manager must wait, after SIGTERM, before it SIGKILLs weir.
/// Spec §7: connection drain + joins (~1 s) + drain budget + 2 s exit grace,
/// rounded up.
const GRACE_FORMULA: &str = "shutdown_timeout_secs + drain_shutdown_budget_secs + 5";

const MONITORING_COMPOSE: &str = include_str!("../../../deploy/monitoring/docker-compose.yml");
// CI_YML is PR 4 Task 4.2's const; reused, not redeclared.

/// The container liveness check, spec §12: one HTTP/1.0 request to /livez
/// over bash's /dev/tcp (the image ships no curl or wget), healthy only on a
/// 200 status line.
const LIVEZ_HEALTHCHECK: &str = "exec 3<>/dev/tcp/127.0.0.1/9185 && printf 'GET /livez HTTP/1.0\\r\\n\\r\\n' >&3 && read -r l <&3 && [[ $l == *' 200 '* ]]";

const STATUS_GOLDEN_PUBLIC: &str =
    include_str!("../../weir-status/tests/golden/status_v1_public.json");
const STATUS_GOLDEN_ADMIN: &str =
    include_str!("../../weir-status/tests/golden/status_v1_admin.json");

/// `Version: 2.1.0  ` → `2.1.0`.
fn latest_md_version() -> &'static str {
    LATEST
        .lines()
        .find_map(|l| l.strip_prefix("Version:"))
        .map(str::trim)
        .expect(
            "docs/benchmarks/latest.md must carry a `Version:` line.\n\
             `deploy/avg_benchmarks.py` emits it from $WEIR_VERSION. Until 2.1.0 it \n\
             read that variable inside the history-writing branch only, so latest.md \n\
             was published with no version at all — which is how every hand-written \n\
             `as of` line in the tree drifted with nothing to catch it.",
        )
}

/// The generator must stamp the version it rendered into its own output.
///
/// This is the root-cause guard for the vintage drift: the two tests below can
/// only compare against `latest.md` because `latest.md` says what it is.
#[test]
fn latest_md_carries_a_version_stamp() {
    let v = latest_md_version();
    assert!(
        v.split('.').count() == 3 && v.split('.').all(|p| p.chars().all(|c| c.is_ascii_digit())),
        "latest.md's `Version:` line reads {v:?}, which is not an x.y.z version. \n\
         In CI the load job exports WEIR_VERSION from the workspace Cargo.toml; a \n\
         local run without it renders the literal `dev` and must not be committed."
    );
}

/// `docs/benchmarks.md` is hand-maintained and says so. That is a deliberate
/// choice — it is an index with commentary, not a generated table — but it
/// means the only thing keeping its vintage honest is this test.
#[test]
fn benchmarks_index_vintage_matches_latest_md() {
    let want = latest_md_version();
    let line = BENCH_INDEX
        .lines()
        .find(|l| l.contains("averaged CI run (v"))
        .expect("docs/benchmarks.md must state which run its headline numbers came from");
    assert!(
        line.contains(&format!("(v{want},")),
        "docs/benchmarks.md's headline block cites a run that is no longer the \n\
         latest one.\n\
         \n  it says:   {}\n  latest.md: v{want}\n\n\
         The load job regenerates latest.md on every push to main and commits it \n\
         with [skip ci], so this fires on the first PR after a release. Fix it by \n\
         copying the current figures out of docs/benchmarks/latest.md into the \n\
         headline tables and updating the vintage — not by editing the vintage \n\
         alone, which would leave the numbers lying about their provenance.",
        line.trim()
    );
}

/// The README's "as of" line had drifted two releases behind before anything
/// looked at it.
#[test]
fn readme_benchmark_vintage_matches_latest_md() {
    let want = latest_md_version();
    let line = README
        .lines()
        .find(|l| l.contains(" as of "))
        .expect("README must carry the `<version> as of <date>` benchmark vintage line");
    assert!(
        line.contains(&format!("{want} as of ")),
        "the README's benchmark vintage is stale.\n\
         \n  it says:   {}\n  latest.md: {want}\n",
        line.trim()
    );
}

/// The substance of the `~3,670 rec/s` defect.
///
/// A single-thread `Durable` rate is a latency reciprocal — the daemon acks
/// frame N before reading frame N+1 on a connection, so one synchronous
/// producer pays one fsync per record. Published in a column next to a
/// concurrent figure with no concurrency stated, it reads as weir's ceiling
/// while understating it by more than an order of magnitude.
///
/// The fix is structural, so the guard is too: the headline table must state
/// concurrency, and must show the `Durable` tier at more than one connection.
/// That survives every benchmark refresh — only a genuine regression in how
/// the numbers are presented can break it.
#[test]
fn headline_throughput_table_states_concurrency_for_every_tier() {
    let table: Vec<&str> = BENCH_INDEX
        .lines()
        .skip_while(|l| !l.starts_with("### Throughput at"))
        .skip(1)
        .take_while(|l| !l.starts_with("###"))
        .filter(|l| l.starts_with('|'))
        .collect();
    assert!(
        !table.is_empty(),
        "no throughput table under `### Throughput at`"
    );

    let header = table[0].to_ascii_lowercase();
    assert!(
        header.contains("concurrency"),
        "the headline throughput table has no concurrency column, so a \n\
         single-thread row and a 48-thread row sit in the same column with \n\
         nothing to tell them apart. Header was: {}",
        table[0].trim()
    );

    let durable_rows: Vec<&&str> = table
        .iter()
        .filter(|l| l.contains("Durable") && !l.contains("---"))
        .collect();
    assert!(
        durable_rows.len() >= 2,
        "the headline throughput table publishes {} `Durable` row(s). It must \n\
         carry at least two — one single-connection and one concurrent — or a \n\
         reader takes the fsync-bound single-thread figure for the daemon's \n\
         ceiling. On the run this test was written against those two rows were \n\
         ~2,400 and ~43,200 rec/s: an 18x difference with no code change \n\
         between them.",
        durable_rows.len()
    );
    assert!(
        durable_rows
            .iter()
            .any(|l| l.split('|').nth(2).is_some_and(|c| c.trim() != "1")),
        "every `Durable` row in the headline table is at concurrency 1"
    );
}

/// The citation rule has to name a file that exists and holds numbers.
///
/// It previously required external claims to cite `bare-metal.md`, which has
/// never been captured — so the rule forbade, in its own terms, every
/// performance statement the project makes, including the correct ones.
#[test]
fn the_external_claim_rule_names_captures_that_exist() {
    let section = ENVIRONMENTS
        .split("### What an external claim may cite")
        .nth(1)
        .expect("environments.md must define what an external claim may cite");
    let section = section.split("\n## ").next().unwrap();

    for cited in [
        "snapshot-2026-06-13-beast.md",
        "phase3-results.md",
        "bare-metal.md",
    ] {
        assert!(
            section.contains(cited),
            "the citation rule does not name {cited}, one of the operator-run \n\
             captures on named hardware that make external claims possible"
        );
        assert!(
            std::path::Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../docs/benchmarks"
            ))
            .join(cited)
            .exists(),
            "the citation rule names {cited}, which does not exist"
        );
    }
    assert!(
        section.contains("Never") && section.contains("latest.md"),
        "the citation rule must still forbid citing latest.md — CI rows for one \n\
         released version span 1.45x with no code change between them"
    );

    // Existing is not the same as holding numbers. `bare-metal.md` existed for
    // the whole life of the project while saying "awaiting first capture", which
    // is exactly the state that made the old citation rule unsatisfiable. Now
    // that it is named as citable, pin the capture itself: the env header the
    // file's own rules call mandatory ("a bare-metal run without this header is
    // not a bare-metal result") plus a result table to go with it.
    let bare_metal = std::fs::read_to_string(
        std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/benchmarks"
        ))
        .join("bare-metal.md"),
    )
    .expect("bare-metal.md must be readable");
    // Every marker below must be one the *scaffold* cannot satisfy. The first
    // version of this check also required "Captured:", "CPU:" and "Kernel:",
    // and all three passed on the empty file -- they appear in this document's
    // own "Environment annotations" template, as `Captured: <UTC timestamp>`.
    // Three of four assertions proved nothing, which is the failure this
    // codebase keeps finding: a claim about a set, checked on the part of it
    // that happens to be easy.
    for required in ["single_thread_sync", "SATURATED", "Host: beast"] {
        assert!(
            bare_metal.contains(required),
            "environments.md names bare-metal.md as an operator-run capture a \n\
             claim may cite, but that file has no `{required}` — it is back to \n\
             being a scaffold, and the citation rule is unsatisfiable again"
        );
    }
    assert!(
        !bare_metal.contains("awaiting first capture"),
        "bare-metal.md still carries its `awaiting first capture` status while \n\
         environments.md lists it as citable"
    );
}

/// `environments.md` forbids comparing absolute rates across machines. The
/// drain document broke that rule against its own project: it set beast's
/// drain rate beside an M3 Max *client's* ingest rate and called the ratio
/// "several times".
#[test]
fn the_drain_document_compares_ingest_on_the_same_box() {
    let conclusions = DRAIN
        .split("## What changed in the conclusions")
        .nth(1)
        .expect("drain-throughput.md must have a conclusions section");

    assert!(
        conclusions.contains("49,725"),
        "the drain-vs-ingest comparison does not cite linux-ssd's own \n\
         single-thread Buffered ingest (49,725 rec/s, \n\
         snapshot-2026-06-13-beast.md). Comparing the drain against a figure \n\
         from a different machine is what environments.md forbids."
    );
    assert!(
        conclusions.contains("155,577"),
        "the comparison omits the same box's *concurrent* Buffered ingest \n\
         (155,577 rec/s at 48 threads), which exceeds the drain rate. Leaving \n\
         it out is what let `delivery is the wider half on every configuration \n\
         measured` be published — it is not, and the case where it is not is \n\
         the case weir exists for."
    );
}

/// The one operator-facing artifact weir ships. Its fsync-latency panel read
/// as though macOS were a slower disk; it is a different primitive, and the
/// gap is ~17x at the WAB record path, not a rounding difference.
#[test]
fn the_dashboard_fsync_panel_names_the_macos_primitive() {
    let dash: serde_json::Value =
        serde_json::from_str(DASHBOARD).expect("weir-dashboard.json must be valid JSON");

    let descriptions: Vec<&str> = dash["panels"]
        .as_array()
        .expect("dashboard has a panels array")
        .iter()
        .filter_map(|p| p["description"].as_str())
        .collect();

    let fsync_panel = descriptions
        .iter()
        .find(|d| d.contains("durable-write latency"))
        .expect("dashboard must carry the durable-write latency panel");

    for token in ["F_BARRIERFSYNC", "F_FULLFSYNC"] {
        assert!(
            fsync_panel.contains(token),
            "the durable-write latency panel quotes Linux fdatasync baselines \n\
             without naming {token}. An operator reading it on macOS compares \n\
             their number against a primitive they are not running.\n\n  {fsync_panel}"
        );
    }
}

/// `docs/platform-support.md` exists because the released architectures were
/// named in exactly one place and what is *untested* was named nowhere. A
/// consolidated page is only worth having if it cannot silently fall behind
/// the workflow that does the releasing.
#[test]
fn the_platform_page_names_every_released_target() {
    let released: Vec<&str> = RELEASE_YML
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- target: "))
        .map(str::trim)
        .collect();
    assert!(
        released.len() >= 4,
        "parsed {} targets out of release.yml; the `- target:` shape must have \
         changed and this guard is no longer reading anything",
        released.len()
    );

    for target in &released {
        assert!(
            PLATFORMS.contains(target),
            "release.yml ships {target}, which docs/platform-support.md does not \
             mention. A new release target needs a row in the daemon table and, \
             if it is untested, a line in the `Untested and unsupported` section."
        );
    }

    // The absence is as load-bearing as the presence: 2.0.5 dropped a Windows
    // binary that could not accept a record by any route, and the page says so.
    assert!(
        !released.iter().any(|t| t.contains("windows")),
        "release.yml has regained a Windows target. weir-server has no ingest \
         path there -- `mod socket` is #[cfg(unix)], the mTLS listener is \
         #[cfg(all(unix, feature = \"tls\"))], and main.rs's #[cfg(not(unix))] \
         arm awaits shutdown -- so shipping one publishes a binary that starts \
         and does nothing, which is what 2.0.5 removed."
    );
}

/// Every Unix that is not Linux or macOS takes `peer_uid`'s `Unsupported` arm,
/// and the accept loop fails closed on a failed credential lookup with
/// `peer_uid_check` defaulting to true — so the daemon compiles there, starts,
/// and refuses every connection.
///
/// That is the right behaviour and a bewildering one to meet undocumented.
/// This asserts the platform set the docs describe is still the platform set
/// the code implements: adding FreeBSD support should fail here, as the prompt
/// to move it out of the "untested" section.
#[test]
fn peer_uid_is_implemented_for_exactly_the_platforms_the_docs_name() {
    let implemented: Vec<&str> = ["linux", "macos", "freebsd", "netbsd", "openbsd", "illumos"]
        .into_iter()
        .filter(|os| PEER.contains(&format!("#[cfg(target_os = \"{os}\")]")))
        .collect();

    assert_eq!(
        implemented,
        vec!["linux", "macos"],
        "peer_uid's platform set changed to {implemented:?}. \
         docs/platform-support.md says Linux and macOS are the only targets with \
         a peer-credential implementation, and that everything else refuses every \
         connection because the accept loop fails closed. Update the page -- \
         moving a platform out of `Untested and unsupported` if it now works."
    );
    assert!(
        PEER.contains("peer credential check not implemented on this platform"),
        "the unsupported-platform arm is gone; the page's claim that other Unix \
         targets fail closed no longer follows from the code"
    );

    for needed in ["Android", "peer_uid_check", "refuse"] {
        assert!(
            PLATFORMS.to_lowercase().contains(&needed.to_lowercase()),
            "docs/platform-support.md no longer mentions {needed:?}, which is \
             half of why a daemon on a non-Linux, non-macOS Unix looks hung"
        );
    }
}

/// Three rows in the published benchmark trend were poisoned by a descheduled
/// CI runner — 28.2 ms, 35.5 ms and 98.9 ms single-thread `Sync` p99, against a
/// healthy range topping out near 3.6 ms — and sat there unannotated, reading
/// as catastrophic regressions that never happened.
///
/// `deploy/avg_benchmarks.py` now marks them at write time. This checks the
/// marking is complete, so a row that slips through unmarked (a threshold
/// change, a generator regression, a hand-edited row) fails here rather than
/// being quoted as trend.
#[test]
fn every_hostile_runner_row_in_the_trend_is_marked() {
    // Matches the generator's HOSTILE_RUNNER_P99_US.
    const THRESHOLD_MS: f64 = 10.0;

    let mut checked = 0usize;
    for line in HISTORY.lines().filter(|l| l.starts_with("| ")) {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        // | version | date | runs | sync rps | sync p99 | buf p50 | ramp |
        let Some(p99) = cells.get(5) else { continue };
        let Some(ms) = p99
            .strip_suffix(" ms")
            .or_else(|| p99.strip_suffix(" ms (!)"))
        else {
            continue; // µs rows are far below the threshold, and the header.
        };
        let Ok(value) = ms.parse::<f64>() else {
            continue;
        };
        checked += 1;
        if value > THRESHOLD_MS {
            assert!(
                p99.ends_with("(!)"),
                "history.md row `{}` has a single-thread Sync p99 of {value} ms, \
                 past the {THRESHOLD_MS} ms hostile-runner threshold, and is not \
                 marked `(!)`. At one connection and one fsync per record weir \
                 cannot produce that; it is the shared runner being descheduled \
                 mid-measurement, and an unmarked row gets read as a regression \
                 that never happened.",
                line.trim()
            );
        }
    }
    assert!(
        checked >= 3,
        "parsed {checked} millisecond p99 rows out of history.md; the table shape \
         must have changed and this guard is no longer reading anything"
    );
    assert!(
        HISTORY.contains("`(!)` marks a run the runner poisoned"),
        "history.md no longer explains its `(!)` marker, so the rows carrying it \
         are more confusing than the unmarked ones were"
    );
}

/// The anchor of every `#### ` heading in `doc`, as GitHub and mdBook build
/// it: lowercased, punctuation dropped except `-` and `_`, spaces to `-`.
///
/// mdBook's `normalize_id` and GitHub both keep `_`. No alert anchor has one,
/// so dropping it never mattered there; every `cond-<code>` anchor has one.
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

/// Every alert's `runbook` annotation must land on a heading that exists.
///
/// The annotation is the operator's entry point: an alert fires at 3am, they
/// follow the link. A rule added without its runbook section, or a section
/// renamed without its rules, sends them to a page that scrolls to nowhere —
/// and nothing else in the tree checks it. `promtool` validates that the rules
/// parse, not that their documentation exists.
#[test]
fn every_alert_runbook_anchor_resolves_to_a_heading() {
    let headings = heading_slugs(MONITORING);

    let mut unresolved = Vec::new();
    let mut count = 0usize;
    for (i, _) in ALERTS.match_indices("runbook: \"docs/monitoring.md#") {
        let rest = &ALERTS[i..];
        let start = rest.find('#').expect("anchor marker") + 1;
        let end = rest[start..]
            .find('"')
            .expect("unterminated runbook annotation")
            + start;
        let anchor = &rest[start..end];
        count += 1;
        if !headings.iter().any(|h| h == anchor) {
            unresolved.push(anchor.to_string());
        }
    }

    // Every rule, not "at least fifteen". The old floor was three below the
    // rule count, so three annotations could vanish without failing this.
    let rule_count = ALERTS.matches("- alert:").count();
    assert_eq!(
        count, rule_count,
        "weir-alerts.yml has {rule_count} rules but {count} runbook annotations; \n\
         every rule must carry a link to its remediation"
    );
    assert!(
        unresolved.is_empty(),
        "these alert runbook anchors do not resolve to a `#### ` heading in \n\
         docs/monitoring.md: {unresolved:?}\n\n\
         An operator following the link from a firing alert lands nowhere."
    );
}

/// Every workspace member must appear in the Docker builder's manifest list.
///
/// `deploy/docker/Dockerfile` copies each member's `Cargo.toml` individually and
/// creates a stub source for it, so Cargo can resolve the workspace before the
/// real sources are copied. A member missing from that list does not degrade —
/// the image build fails outright with "failed to load manifest for workspace
/// member", and it fails in the `docker` workflow, which the Rust gate does not
/// run.
///
/// The Dockerfile's own comment records this happening when `weir-sink-s3`
/// landed. It happened again when `weir-attest` landed, because a comment is a
/// note to a human and this is a set-membership property. Deriving the set from
/// the workspace manifest is what actually holds it closed.
#[test]
fn every_workspace_member_is_in_the_docker_builder() {
    const ROOT_MANIFEST: &str = include_str!("../../../Cargo.toml");
    const DOCKERFILE: &str = include_str!("../../../deploy/docker/Dockerfile");

    // The `members = [...]` array, as literal `crates/<name>` entries.
    let members: Vec<&str> = ROOT_MANIFEST
        .split_once("members = [")
        .and_then(|(_, rest)| rest.split_once(']'))
        .map(|(list, _)| list)
        .expect("root Cargo.toml has a members array")
        .lines()
        .filter_map(|l| {
            l.trim()
                .trim_end_matches(',')
                .trim_matches('"')
                .strip_prefix("crates/")
        })
        .filter(|n| !n.is_empty())
        .collect();

    assert!(
        members.len() >= 9,
        "found only {} workspace members; this guard is no longer reading the \n\
         manifest correctly",
        members.len()
    );

    // Three obligations, each checked against the region of the Dockerfile that
    // actually carries it. A bare `DOCKERFILE.contains("crates/<name>/src")` is
    // satisfied by the `touch` list on its own, so deleting a crate's `mkdir -p`
    // entry — the exact break this guard exists to catch — slipped straight
    // through the first version of this check.
    let stub_run = {
        let (_, rest) = DOCKERFILE
            .split_once("RUN mkdir -p")
            .expect("Dockerfile stubs sources with a `RUN mkdir -p` block");
        // The block runs until the first line that does not continue with `\`.
        let mut end = rest.len();
        let mut at = 0;
        for line in rest.lines() {
            at += line.len() + 1;
            if !line.trim_end().ends_with('\\') {
                end = at.min(rest.len());
                break;
            }
        }
        &rest[..end]
    };

    // `mkdir -p` takes every member; the source stub after it is an `echo`ed
    // `main.rs` for the two binaries and a `touch`ed `lib.rs` for the libraries.
    let (mkdir_list, source_stubs) = stub_run
        .split_once("echo ")
        .expect("stub block writes a binary stub with `echo`");

    let mkdir_entries: Vec<&str> = mkdir_list
        .lines()
        .map(|l| {
            l.trim()
                .trim_end_matches('\\')
                .trim()
                .trim_end_matches("&&")
                .trim()
        })
        .filter(|l| l.starts_with("crates/"))
        .collect();

    let mut missing: Vec<String> = Vec::new();
    for name in &members {
        if !DOCKERFILE.contains(&format!("COPY crates/{name}/Cargo.toml")) {
            missing.push(format!("{name}: no `COPY crates/{name}/Cargo.toml` line"));
        }
        let dir = format!("crates/{name}/src");
        if !mkdir_entries.iter().any(|e| *e == dir) {
            missing.push(format!("{name}: not in the `RUN mkdir -p` list"));
        }
        let lib = format!("crates/{name}/src/lib.rs");
        let main = format!("crates/{name}/src/main.rs");
        if !source_stubs.contains(&lib) && !source_stubs.contains(&main) {
            missing.push(format!("{name}: no stub `lib.rs` or `main.rs`"));
        }
    }

    assert!(
        missing.is_empty(),
        "deploy/docker/Dockerfile is missing entries for workspace members:\n  \
         {}\n\n\
         The image build fails with \"failed to load manifest for workspace \n\
         member\" — and it fails in the `docker` workflow, which `cargo test` \n\
         does not exercise.",
        missing.join("\n  ")
    );
}

/// Every alert rule must have a `promtool` unit test.
///
/// Three rules shipped with no test in either direction —
/// `WeirFsyncLatencyHigh`, `WeirFsyncLatencyCritical` and `WeirHighNackRate` —
/// while `ci.yml`, `weir-alerts_test.yml`'s own header and `docs/monitoring.md`
/// all stated that the suite "pins every rule in both directions". It did not,
/// and nothing could tell: `promtool test rules` checks the assertions that
/// exist, so a rule with no assertions passes, and deleting those three rules
/// outright left the CI gate green.
///
/// This is the same shape as the response-cap guards in the five polyglot
/// clients and the `NackReason` label guard: a claim about a whole set, checked
/// by enumerating part of it. The fix is the same — derive the set and compare.
#[test]
fn every_alert_rule_has_a_unit_test() {
    let defined: Vec<&str> = ALERTS
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- alert: "))
        .map(str::trim)
        .collect();
    assert!(
        defined.len() >= 18,
        "found only {} rules in weir-alerts.yml; the file has shrunk and this \n\
         guard is no longer reading it correctly",
        defined.len()
    );

    // Both directions, because that is what ci.yml, this suite's own header and
    // docs/monitoring.md all claim. A firing-only test cannot catch the more
    // corrosive failure — a rule that fires constantly — and two rules had
    // exactly that gap while the claim stood.
    //
    // An entry is "non-firing" when its `alertname:` is followed by
    // `exp_alerts: []`, and "firing" when it is followed by a populated block.
    let mut firing = Vec::new();
    let mut quiet = Vec::new();
    let mut lines = ALERTS_TEST.lines().peekable();
    while let Some(line) = lines.next() {
        let Some(name) = line.trim().strip_prefix("alertname: ") else {
            continue;
        };
        let name = name.trim();
        // The next `exp_alerts:` line decides which list this entry joins.
        for following in lines.by_ref() {
            let t = following.trim();
            if let Some(rest) = t.strip_prefix("exp_alerts:") {
                if rest.trim() == "[]" {
                    quiet.push(name);
                } else {
                    firing.push(name);
                }
                break;
            }
        }
    }

    let missing: Vec<String> = defined
        .iter()
        .filter_map(|name| match (firing.contains(name), quiet.contains(name)) {
            (true, true) => None,
            (true, false) => Some(format!("{name} (no non-firing test)")),
            (false, true) => Some(format!("{name} (no firing test)")),
            (false, false) => Some(format!("{name} (no test at all)")),
        })
        .collect();

    assert!(
        missing.is_empty(),
        "these alert rules are not pinned in both directions: {missing:?}\n\n\
         A rule with no test can be inverted, neutered or deleted and \n\
         `promtool test rules` still reports SUCCESS — which is how three \n\
         shipped untested, and two more firing-only, while ci.yml, this suite's \n\
         header and docs/monitoring.md all said every rule was pinned in both \n\
         directions."
    );
}

/// Every metric family the daemon registers, as `(exposition name, is_counter)`,
/// parsed out of the `reg!(<metric>, "weir_…", "<help>")` calls in
/// `src/metrics/mod.rs`. A counter is exposed with a `_total` suffix the source
/// does not spell, so the returned name carries it.
fn registered_metric_families() -> Vec<String> {
    let mut out = Vec::new();
    for (i, _) in METRICS_SRC.match_indices("reg!(") {
        let call = &METRICS_SRC[i + "reg!(".len()..];
        // The first string literal in the call is the name; everything before it
        // is the metric expression, which names its type.
        let open = call.find('"').expect("a reg! call with no name literal");
        let (expr, rest) = call.split_at(open);
        let name = &rest[1..rest[1..].find('"').expect("unterminated name literal") + 1];
        assert!(
            name.starts_with("weir_"),
            "the first literal in a reg! call is {name:?}, not a weir_ metric name; \n\
             the call shape changed and this parser is no longer reading names"
        );
        if expr.contains("Counter") {
            out.push(format!("{name}_total"));
        } else {
            out.push(name.to_string());
        }
    }
    out
}

/// The metric names a `docs/monitoring.md` reference table row documents: every
/// backticked `weir_…` name in the row's first cell, label set stripped.
fn documented_metric_names(section: &str) -> Vec<String> {
    section
        .lines()
        .filter(|l| l.starts_with("| `weir_"))
        .flat_map(|l| {
            let first_cell = l.split('|').nth(1).unwrap_or("");
            first_cell
                .split('`')
                .skip(1)
                .step_by(2)
                .filter(|t| t.starts_with("weir_"))
                .map(|t| t.split('{').next().unwrap_or(t).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Every metric the daemon registers has a row in `docs/monitoring.md`'s
/// metric reference, under the name a scrape actually shows, and every row
/// there names a metric the daemon still registers.
///
/// The safety floor added ten metric families at once. The reference table is
/// the only place an operator learns what `weir_drain_segments_parked` means
/// or what to do about it, and a family that ships without a row is one the
/// first operator to see it non-zero has to read the source for. The second
/// direction catches the rename: a row describing a name nothing exports is a
/// dashboard query that silently returns no data. This derives the set from
/// the registrations themselves, so it is closed against both — a property
/// checked by enumerating part of a set is how this file's other guards were
/// born.
///
/// Counters must be documented with their `_total` suffix, because that is the
/// name a query uses. The weir-attest rows are excluded from the second
/// direction: those gauges come from `weir-ctl attest verify`, not the daemon.
#[test]
fn every_registered_metric_is_in_the_monitoring_reference() {
    let registered = registered_metric_families();
    assert!(
        registered.len() >= 50,
        "parsed only {} reg! calls out of src/metrics/mod.rs; the registration \n\
         shape must have changed and this guard is no longer reading it",
        registered.len()
    );

    let reference = MONITORING
        .split("\n## Metric reference")
        .nth(1)
        .expect("docs/monitoring.md must keep its `## Metric reference` section")
        .split("\n## ")
        .next()
        .unwrap();
    // The daemon's own tables, without the weir-attest section (whose rows are
    // written by weir-ctl onto the node_exporter textfile collector).
    let daemon_tables: String = reference
        .split("\n### ")
        .filter(|s| !s.starts_with("Integrity / tamper evidence (weir-attest)"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        reference.contains("Integrity / tamper evidence (weir-attest)"),
        "the weir-attest section was renamed; update the exclusion above or \n\
         its rows will be checked against the daemon's registrations"
    );

    let documented = documented_metric_names(&daemon_tables);
    let missing: Vec<&String> = registered
        .iter()
        .filter(|name| !documented.contains(name))
        .collect();
    assert!(
        missing.is_empty(),
        "these metric families are registered in src/metrics/mod.rs but have no \n\
         row in docs/monitoring.md's metric reference (a counter is matched with \n\
         its `_total` suffix): {missing:?}\n\n\
         Add a row with its type, labels and label values, what it means and what \n\
         an operator should do about it."
    );

    let stale: Vec<&String> = documented
        .iter()
        .filter(|name| !registered.contains(name))
        .collect();
    assert!(
        stale.is_empty(),
        "docs/monitoring.md's metric reference documents names the daemon does \n\
         not export: {stale:?}\n\n\
         A renamed or removed family, or a counter documented without its \n\
         `_total` suffix. A query built from that row returns no data."
    );
}

/// `wab_dir_lock` is the one knob that turns off the single-owner guard on a
/// `wab_dir`, so an operator who meets the refusal must find it, under the
/// spellings the daemon actually parses, in the configuration reference and
/// in the example file that claims to list every option.
///
/// The env and CLI spellings are checked against the parser first, so a
/// rename in the code fails here rather than leaving the docs naming a flag
/// the daemon rejects.
#[test]
fn the_wab_dir_lock_key_is_in_the_configuration_reference() {
    assert!(
        CONFIG_ENV_SRC.contains("\"WEIR_WAB_DIR_LOCK\""),
        "src/config/env.rs no longer reads WEIR_WAB_DIR_LOCK; update this guard \n\
         and the docs to the new spelling"
    );
    assert!(
        CONFIG_CLI_SRC.contains("\"--wab-dir-lock\""),
        "src/config/cli.rs no longer parses --wab-dir-lock; update this guard \n\
         and the docs to the new spelling"
    );
    for needle in [
        "#### `wab_dir_lock`",
        "`WEIR_WAB_DIR_LOCK`",
        "`--wab-dir-lock",
    ] {
        assert!(
            CONFIGURATION.contains(needle),
            "docs/operations/configuration.md must document `wab_dir_lock` \n\
             (missing {needle:?}): the key, its env var and its CLI flag"
        );
    }
    for needle in ["wab_dir_lock", "WEIR_WAB_DIR_LOCK"] {
        assert!(
            CONFIG_EXAMPLE.contains(needle),
            "deploy/docker/weir.toml.example claims to list every option; it is \n\
             missing {needle:?}"
        );
    }
}

/// The admin socket is on by default and refuses an insecure parent, so an
/// operator who meets `admin_unavailable` must find both keys, under the
/// spellings the daemon parses, in the reference and the example file.
#[test]
fn the_admin_socket_keys_are_in_the_configuration_reference() {
    assert!(
        CONFIG_ENV_SRC.contains("\"WEIR_ADMIN_SOCKET_ENABLED\""),
        "src/config/env.rs no longer reads WEIR_ADMIN_SOCKET_ENABLED; update this \n\
         guard and the docs to the new spelling"
    );
    assert!(
        CONFIG_ENV_SRC.contains("\"WEIR_ADMIN_SOCKET_PATH\""),
        "src/config/env.rs no longer reads WEIR_ADMIN_SOCKET_PATH; update this \n\
         guard and the docs to the new spelling"
    );
    assert!(
        CONFIG_CLI_SRC.contains("\"--admin-socket-path\""),
        "src/config/cli.rs no longer parses --admin-socket-path; update this guard \n\
         and the docs to the new spelling"
    );
    for needle in [
        "#### `admin_socket_enabled`",
        "#### `admin_socket_path`",
        "`WEIR_ADMIN_SOCKET_ENABLED`",
        "`WEIR_ADMIN_SOCKET_PATH`",
        "`--admin-socket-path",
        ".admin.sock",
    ] {
        assert!(
            CONFIGURATION.contains(needle),
            "docs/operations/configuration.md must document the admin socket keys \n\
             (missing {needle:?})"
        );
    }
    for needle in [
        "admin_socket_enabled",
        "admin_socket_path",
        "WEIR_ADMIN_SOCKET_PATH",
    ] {
        assert!(
            CONFIG_EXAMPLE.contains(needle),
            "deploy/docker/weir.toml.example claims to list every option; it is \n\
             missing {needle:?}"
        );
    }
}

/// The bounded-shutdown budget (Foundations §7) is an operator-facing knob
/// whose default decides how much a SIGTERM delivers; it must be findable in
/// the reference and in the example that claims to list every option.
#[test]
fn the_drain_shutdown_budget_key_is_in_the_configuration_reference() {
    assert!(
        CONFIG_ENV_SRC.contains("\"WEIR_DRAIN_SHUTDOWN_BUDGET_SECS\""),
        "src/config/env.rs no longer reads WEIR_DRAIN_SHUTDOWN_BUDGET_SECS; update \n\
         this guard and the docs to the new spelling"
    );
    assert!(
        CONFIG_CLI_SRC.contains("\"--drain-shutdown-budget-secs\""),
        "src/config/cli.rs no longer parses --drain-shutdown-budget-secs"
    );
    // Scoped to the entry's own section: "**Default**: `10`" also appears
    // under two other keys, so a whole-file search would stay green with
    // this entry's default deleted.
    let entry = CONFIGURATION
        .split("#### `drain_shutdown_budget_secs`")
        .nth(1)
        .expect(
            "docs/operations/configuration.md must have a #### `drain_shutdown_budget_secs` entry",
        )
        .split("\n#### ")
        .next()
        .unwrap();
    for needle in [
        "**Default**: `10`",
        "**Range**: 0-3600",
        "`WEIR_DRAIN_SHUTDOWN_BUDGET_SECS`",
        "`--drain-shutdown-budget-secs",
    ] {
        assert!(
            entry.contains(needle),
            "the drain_shutdown_budget_secs entry is missing {needle:?}"
        );
    }
    for needle in [
        "drain_shutdown_budget_secs",
        "WEIR_DRAIN_SHUTDOWN_BUDGET_SECS",
    ] {
        assert!(
            CONFIG_EXAMPLE.contains(needle),
            "deploy/docker/weir.toml.example claims to list every option; it is \n\
             missing {needle:?}"
        );
    }
}

/// `test-hooks` system tests compile only under that feature. The CI line that
/// runs the system tests with `--all-features` is what keeps them from being
/// silently skipped forever (spec §16 "a drift guard asserts the
/// --all-features line runs them").
#[test]
fn the_test_hooks_system_tests_run_in_ci() {
    let cargo = include_str!("../Cargo.toml");
    assert!(
        cargo.contains("\ntest-hooks = []"),
        "the test-hooks feature is gone from Cargo.toml"
    );
    let ci = CI_YML;
    assert!(
        ci.lines().any(|l| l.contains("cargo test -p weir-server")
            && l.contains("--test system")
            && l.contains("--all-features")),
        "no CI line runs tests/system.rs with --all-features, so every \
         #[cfg(feature = \"test-hooks\")] system test is skipped"
    );
    // And no release build may carry the hooks (Foundations §16).
    assert!(
        !RELEASE_YML.contains("test-hooks") && !RELEASE_YML.contains("--all-features"),
        "the release workflow must never build weir-server with the test hooks"
    );
}

const ARCHITECTURE: &str = include_str!("../../../docs/architecture.md");

/// The startup order in architecture.md names the six lifecycle phases in the
/// order the daemon publishes them (Foundations §6), and no longer claims the
/// metrics endpoint binds after recovery.
#[test]
fn the_startup_order_doc_lists_the_phases_in_order() {
    let section = ARCHITECTURE
        .split("### Startup order")
        .nth(1)
        .and_then(|s| s.split("\n## ").next())
        .expect("docs/architecture.md must keep a `### Startup order` section");
    let mut at = 0;
    for phase in [
        "initializing",
        "recovering",
        "handoff",
        "binding",
        "serving",
        "shutting_down",
    ] {
        // Anchored on the diagram's arrow: the bare word also occurs in prose
        // ("something binding in between"), which let a swapped diagram pass.
        let step = format!("→ {phase}");
        let pos = section[at..].find(&step).unwrap_or_else(|| {
            panic!("`### Startup order` must name phase `{phase}` after the previous one")
        });
        at += pos + step.len();
    }
    assert!(
        !section.contains("replay of sealed-but-unconfirmed segments into the drain channel"),
        "the blocking replay is gone; the replay list is handed to the drain in memory"
    );
    assert!(
        !ARCHITECTURE.contains("CancellationToken"),
        "the socket layer takes a watch<ShutdownLevel>, not a CancellationToken"
    );
}

const SYSTEMD_UNIT: &str = include_str!("../../../deploy/systemd/weir.service");
const SYSTEMD_README: &str = include_str!("../../../deploy/systemd/README.md");
const COMPOSE: &str = include_str!("../../../deploy/docker/docker-compose.yml");

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
        (
            "deploy/systemd/weir.toml",
            SYSTEMD_TOML,
            "TimeoutStopSec (35s)",
        ),
        (
            "deploy/systemd/README.md",
            SYSTEMD_README,
            "(in `weir.service`, **35**)",
        ),
        (
            "deploy/systemd/weir.service",
            SYSTEMD_UNIT,
            "TimeoutStopSec=35",
        ),
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
        assert!(
            src.contains("start_period: 10s"),
            "{file}: needs `start_period: 10s`"
        );
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
            let Some(rest) = m.strip_prefix("code") else {
                continue;
            };
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
            (
                "weir-alerts.yml description",
                alert_annotation(rule, "description"),
            ),
        ] {
            let storage = text
                .find("fsync")
                .unwrap_or_else(|| panic!("{rule} ({place}) no longer mentions fsync at all"));
            for signal in [
                "weir_wab_handoff_blocked_seconds",
                "weir_drain_channel_depth",
            ] {
                match text.find(signal) {
                    Some(i) if i < storage => {}
                    Some(_) => panic!("{rule} ({place}): {signal} comes after the fsync triage"),
                    None => panic!("{rule} ({place}): does not mention {signal}"),
                }
            }
        }
    }
}

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
        if gates.is_empty() {
            "none".to_string()
        } else {
            gates.join(", ")
        },
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
    // The offline-only list is weir_status's, not a copy that can drift.
    let offline: Vec<String> = weir_status::OFFLINE_ONLY
        .iter()
        .map(|e| wire_name(serde_json::to_value(e.code).expect("serialize a ConditionCode")))
        .collect();
    assert_eq!(
        offline, OFFLINE_ONLY_CODES,
        "weir_status::OFFLINE_ONLY changed; update OFFLINE_ONLY_CODES and the catalogue"
    );

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
    for e in weir_status::OFFLINE_ONLY {
        let mut want = catalogue_row(&e.code);
        want[3] = format!("{} (weir-ctl only)", want[3]);
        let code = &want[0];
        match rows.iter().find(|r| r.first() == Some(code)) {
            None => problems.push(format!("{code} (weir-ctl only): no row")),
            Some(r) if r.len() < 4 || r[..4] != want[..] => problems.push(format!(
                "{code} (weir-ctl only): the table says {:?}, weir_status says {want:?}",
                &r[..r.len().min(4)]
            )),
            Some(_) => {}
        }
        if !slugs.contains(&format!("cond-{code}")) {
            problems.push(format!(
                "{code} (weir-ctl only): no `#### cond-{code}` heading"
            ));
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

    for (golden, view) in [
        (STATUS_GOLDEN_PUBLIC, "public"),
        (STATUS_GOLDEN_ADMIN, "admin"),
    ] {
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
            problems.push(format!(
                "{}: not a known TOML key in src/config/file.rs",
                k.key
            ));
        }
        if !CONFIG_ENV_SRC.contains(&format!("\"{}\"", k.env)) {
            problems.push(format!(
                "{}: src/config/env.rs does not read {}",
                k.key, k.env
            ));
        }
        if let Some(flag) = k.cli
            && !CONFIG_CLI_SRC.contains(&format!("\"{flag}\""))
        {
            problems.push(format!(
                "{}: src/config/cli.rs does not parse {flag}",
                k.key
            ));
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
                    Some(flag) if !section.contains(&format!("`{flag}")) => {
                        problems.push(format!("{}: its section does not name `{flag}`", k.key))
                    }
                    None if !section.contains("**CLI**: none") => {
                        problems.push(format!("{}: its section must say `**CLI**: none`", k.key))
                    }
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
        "| Network peer starving the health probes |",                      // PR 5 Task 5.11
        "| Local process reaching the admin socket |",                      // PR 6 Task 6.17
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
