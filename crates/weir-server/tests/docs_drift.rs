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

    for cited in ["snapshot-2026-06-13-beast.md", "phase3-results.md"] {
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

/// Every alert's `runbook` annotation must land on a heading that exists.
///
/// The annotation is the operator's entry point: an alert fires at 3am, they
/// follow the link. A rule added without its runbook section, or a section
/// renamed without its rules, sends them to a page that scrolls to nowhere —
/// and nothing else in the tree checks it. `promtool` validates that the rules
/// parse, not that their documentation exists.
#[test]
fn every_alert_runbook_anchor_resolves_to_a_heading() {
    let headings: Vec<String> = MONITORING
        .lines()
        .filter_map(|l| l.strip_prefix("#### "))
        .map(|h| {
            h.trim()
                .to_ascii_lowercase()
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == ' ')
                .collect::<String>()
                .replace(' ', "-")
        })
        .collect();

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

    assert!(
        count >= 15,
        "found only {count} runbook annotations; weir-alerts.yml has eighteen \n\
         rules and every one should carry a link to its remediation"
    );
    assert!(
        unresolved.is_empty(),
        "these alert runbook anchors do not resolve to a `#### ` heading in \n\
         docs/monitoring.md: {unresolved:?}\n\n\
         An operator following the link from a firing alert lands nowhere."
    );
}
