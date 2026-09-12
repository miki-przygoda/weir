//! Guards the browser demo bundle in `demo/` against the code it describes.
//!
//! Sibling of `docs_drift.rs`, split into its own target so a failure names the
//! artifact rather than hiding in a shared summary. Every test here exists
//! because the drift it checks for actually happened, and it went unnoticed for
//! two major releases: `demo/index.html` shipped `Sync` and `Batched` as live
//! durability controls — with `Sync` the *default* — after 2.0 collapsed both
//! into `Durable`. `demo/clients/ts.html` published a snippet quoting
//! `Durability.Sync`, a key its own source file does not define, so the
//! published code would have thrown. `demo/clients/c.html` told the reader to
//! run `./telemetry … sync`, which the C client rejects with exit 2.
//!
//! The bundle already had a guard: `scripts/sync-demo-version.sh` regenerates
//! `demo/version.js` and CI fails on a diff. That pins the version *string* and
//! nothing about the content — which is worse than no guard, because it stamped
//! a 3.0.0 banner on a 1.x tier model.
//!
//! The tier assertions deliberately derive the live set from the decoder rather
//! than hardcoding `["durable", "buffered"]`. That way the retired `0x02` byte
//! collapses automatically, and a future third tier fails here and forces a UI
//! decision instead of silently shipping a control set that omits it.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use weir_core::Durability;

const INDEX: &str = include_str!("../../../demo/index.html");
const CRATES: &str = include_str!("../../../demo/crates.html");
const DEMO_README: &str = include_str!("../../../demo/README.md");
const C_PAGE: &str = include_str!("../../../demo/clients/c.html");
const TELEMETRY_C: &str = include_str!("../../../demos/c-wire-client/telemetry.c");
const SERVER_MANIFEST: &str = include_str!("../Cargo.toml");

/// Repo root, derived the way `docs_drift.rs` does it.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

/// Every `demo/**/*.html` in the bundle.
fn bundle_pages() -> Vec<(String, String)> {
    let demo = repo_root().join("demo");
    let mut out = Vec::new();
    let mut dirs = vec![demo.clone()];
    while let Some(d) = dirs.pop() {
        for entry in fs::read_dir(&d).expect("read demo dir").flatten() {
            let p = entry.path();
            if p.is_dir() {
                dirs.push(p);
            } else if p.extension().is_some_and(|e| e == "html") {
                let rel = p.strip_prefix(&demo).unwrap().display().to_string();
                out.push((rel, fs::read_to_string(&p).expect("read page")));
            }
        }
    }
    assert!(!out.is_empty(), "no HTML pages found under demo/");
    out.sort();
    out
}

/// The tiers a conformant decoder actually yields, over the whole byte space.
///
/// Not a hardcoded list: `0x02` (retired `Batched`) decodes permissively to
/// `Durable`, so it collapses here on its own, and a new variant shows up here
/// the moment it exists.
fn live_tiers() -> BTreeSet<String> {
    (0u8..=u8::MAX)
        .filter_map(|b| Durability::try_from(b).ok())
        .map(|d| d.to_string())
        .collect()
}

/// Values of `data-tier="…"` on the demo's durability control.
fn offered_tiers() -> BTreeSet<String> {
    INDEX
        .match_indices("data-tier=\"")
        .map(|(i, pat)| {
            let rest = &INDEX[i + pat.len()..];
            let end = rest.find('"').expect("unterminated data-tier attribute");
            rest[..end].to_ascii_lowercase()
        })
        .collect()
}

#[test]
fn the_demo_offers_exactly_the_tiers_the_enum_decodes() {
    let want = live_tiers();
    let got = offered_tiers();
    assert_eq!(
        got, want,
        "the demo's durability control offers {got:?} but a conformant decoder \n\
         yields {want:?}.\n\n\
         `Sync` and `Batched` were collapsed into `Durable` in 2.0 — wire byte \n\
         0x02 still decodes, so a 1.x producer keeps working, but the encoder \n\
         never emits it and no UI should offer it as a choice. If a tier was \n\
         ADDED, this test is telling you the demo needs a control for it."
    );
}

#[test]
fn the_demo_default_tier_is_live_and_matches_the_script() {
    // The button carrying class="sel" is the one rendered pre-selected.
    let sel = INDEX
        .split("<button ")
        .find(|b| b.contains("class=\"sel\"") && b.contains("data-tier="))
        .expect("no pre-selected data-tier button in the durability control");
    let after = sel.split("data-tier=\"").nth(1).expect("data-tier value");
    let default_tier = after[..after.find('"').unwrap()].to_ascii_lowercase();

    assert!(
        live_tiers().contains(&default_tier),
        "the demo pre-selects the `{default_tier}` tier, which no longer exists. \n\
         It shipped defaulting to `Sync` for two major releases."
    );

    // The JS initialiser must agree with the markup, or the page renders one
    // tier as selected while simulating another.
    let init = INDEX
        .split("let tier = \"")
        .nth(1)
        .expect("no `let tier = \"…\"` initialiser");
    let js_tier = init[..init.find('"').unwrap()].to_ascii_lowercase();
    assert_eq!(
        js_tier, default_tier,
        "the markup pre-selects `{default_tier}` but the script initialises \n\
         `{js_tier}`, so the highlighted control and the simulated tier disagree."
    );
}

#[test]
fn no_retired_tier_name_is_presented_as_a_live_one() {
    // Capitalised, whole-word. That excludes `fsync`, `group fsync`,
    // `std::sync`, `Synchronous`, and English "batched drain" / "batched N→1",
    // all of which appear legitimately throughout the bundle.
    fn offending(line: &str) -> bool {
        for name in ["Sync", "Batched"] {
            let mut from = 0;
            while let Some(i) = line[from..].find(name) {
                let at = from + i;
                let before = line[..at].chars().next_back();
                let after = line[at + name.len()..].chars().next();
                let boundary =
                    |c: Option<char>| !c.is_some_and(|c| c.is_alphanumeric() || c == '_');
                if boundary(before) && boundary(after) {
                    return true;
                }
                from = at + name.len();
            }
        }
        false
    }
    // A line that explains the retirement is exactly what we want to keep.
    fn explains_retirement(line: &str) -> bool {
        let l = line.to_ascii_lowercase();
        l.contains("retired") || l.contains("0x02") || l.contains("1.x")
    }

    // Prose wraps, so a sentence explaining the retirement routinely puts the
    // marker on a neighbouring line. Look at a small window rather than
    // demanding both land on the same one.
    fn flagged(body: &str, label: &str, bad: &mut Vec<String>) {
        let lines: Vec<&str> = body.lines().collect();
        for (n, line) in lines.iter().enumerate() {
            if !offending(line) {
                continue;
            }
            let lo = n.saturating_sub(2);
            let hi = (n + 3).min(lines.len());
            if lines[lo..hi].iter().any(|l| explains_retirement(l)) {
                continue;
            }
            bad.push(format!("  {label}:{}  {}", n + 1, line.trim()));
        }
    }

    let mut bad = Vec::new();
    for (name, body) in bundle_pages() {
        flagged(&body, &format!("demo/{name}"), &mut bad);
    }
    flagged(DEMO_README, "demo/README.md", &mut bad);
    assert!(
        bad.is_empty(),
        "the demo bundle presents a retired durability tier as a live one, on \n\
         {} line(s):\n{}\n\n\
         `Sync` and `Batched` were collapsed into `Durable` in 2.0. Either use \n\
         the live name, or keep the mention and say on the same line that it is \n\
         retired (mention `retired`, `0x02`, or `1.x`).",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
fn the_c_page_shows_a_durability_arg_the_c_client_accepts() {
    // What telemetry.c actually parses: strcmp(argv[3], "…")
    let accepted: BTreeSet<String> = TELEMETRY_C
        .match_indices("strcmp(argv[3], \"")
        .map(|(i, pat)| {
            let rest = &TELEMETRY_C[i + pat.len()..];
            rest[..rest.find('"').expect("unterminated strcmp literal")].to_string()
        })
        .collect();
    assert!(
        !accepted.is_empty(),
        "could not parse any durability argument out of telemetry.c — if its \n\
         argument handling was rewritten, this test needs to learn the new shape"
    );

    let mut bad = Vec::new();
    for line in C_PAGE.lines() {
        let Some(cmd) = line.split("./telemetry").nth(1) else {
            continue;
        };
        // ./telemetry <socket> <count> <durability>
        if let Some(arg) = cmd.split_whitespace().nth(2) {
            let arg = arg.trim_end_matches(|c: char| !c.is_alphanumeric());
            if !accepted.contains(arg) {
                bad.push(format!("  shows `{arg}`, accepted: {accepted:?}"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "demo/clients/c.html publishes a `./telemetry` command the C client \n\
         rejects:\n{}\n\n\
         It shipped `… 8 sync`, which prints \"unknown durability 'sync'\" and \n\
         exits 2 — a copy-pasteable command that cannot work.",
        bad.join("\n")
    );
}

#[test]
fn the_demo_lists_every_published_crate() {
    let root = repo_root();
    let ws = fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest");
    let members: Vec<String> = ws
        .lines()
        .skip_while(|l| !l.starts_with("[workspace]"))
        .take_while(|l| !l.starts_with("[workspace.package]"))
        .filter_map(|l| l.trim().strip_prefix('"')?.strip_suffix("\",")) // "crates/weir-x",
        .filter_map(|p| p.strip_prefix("crates/"))
        .map(str::to_string)
        .collect();
    assert!(!members.is_empty(), "parsed no workspace members");

    let mut missing = Vec::new();
    for m in &members {
        let manifest =
            fs::read_to_string(root.join("crates").join(m).join("Cargo.toml")).expect("manifest");
        if manifest.contains("publish = false") {
            continue; // weir-testkit is deliberately unpublished
        }
        if !CRATES.contains(m.as_str()) {
            missing.push(m.clone());
        }
    }
    assert!(
        missing.is_empty(),
        "demo/crates.html does not mention {missing:?}, which are published \n\
         crates. It carried six cards for eight crates — weir-sink-s3 and \n\
         weir-rs were both absent, and weir-rs is arguably the one a newcomer \n\
         should meet first."
    );
}

#[test]
fn the_demo_sink_list_matches_the_server_features() {
    // `*-sink` features on weir-server, minus internal `_`-prefixed ones.
    let stems: BTreeSet<String> = SERVER_MANIFEST
        .lines()
        .skip_while(|l| !l.starts_with("[features]"))
        .take_while(|l| !l.starts_with('[') || l.starts_with("[features]"))
        .filter_map(|l| l.split('=').next())
        .map(str::trim)
        .filter(|k| k.ends_with("-sink") && !k.starts_with('_'))
        .filter_map(|k| k.strip_suffix("-sink"))
        .map(str::to_string)
        .collect();
    assert!(
        stems.len() >= 4,
        "parsed only {stems:?} as sink features; the manifest shape changed"
    );

    let missing: Vec<&String> = stems
        .iter()
        .filter(|s| !CRATES.contains(s.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "demo/crates.html omits sink(s) {missing:?}. The s3 sink shipped in \n\
         2.1.0 and the page still listed five."
    );
}

#[test]
fn the_demo_does_not_cite_ci_benchmarks_for_a_figure() {
    // environments.md: "Never `latest.md` or `history.md`" for an external
    // claim — and this bundle is the most external artifact in the tree.
    let mut bad = Vec::new();
    for (name, body) in bundle_pages() {
        for (n, line) in body.lines().enumerate() {
            let l = line.to_ascii_lowercase();
            let cites =
                l.contains("ci benchmark") || l.contains("latest.md") || l.contains("history.md");
            // Naming the forbidden source in order to disclaim it is the
            // opposite of citing it.
            let disclaims =
                l.contains("not a ci figure") || l.contains("forbids") || l.contains("never ");
            if cites && !disclaims {
                bad.push(format!("  demo/{name}:{}  {}", n + 1, line.trim()));
            }
        }
    }
    for (n, line) in DEMO_README.lines().enumerate() {
        let l = line.to_ascii_lowercase();
        if l.contains("ci benchmark") {
            bad.push(format!("  demo/README.md:{}  {}", n + 1, line.trim()));
        }
    }
    assert!(
        bad.is_empty(),
        "the demo cites CI benchmarks for a figure:\n{}\n\n\
         docs/benchmarks/environments.md forbids citing latest.md / history.md \n\
         for an external claim — consecutive runs of one released version span \n\
         ~1.45x. Quote an operator-run capture and name its hardware and fsync \n\
         primitive instead.",
        bad.join("\n")
    );
}

#[test]
fn every_demo_page_is_wired_to_the_generated_version_banner() {
    // sync-demo-version.sh regenerates version.js and CI fails on a diff, but
    // nothing checked that a page actually loads it. A page added without the
    // script tag silently shows no version at all.
    let mut bad = Vec::new();
    for (name, body) in bundle_pages() {
        if !body.contains("version.js") || !body.contains("data-weir-version") {
            bad.push(name);
        }
    }
    assert!(
        bad.is_empty(),
        "these demo pages are not wired to the generated version banner: {bad:?}\n\
         Each page needs both a `version.js` script tag and a \n\
         `data-weir-version` element for the generated value to land anywhere."
    );
}
