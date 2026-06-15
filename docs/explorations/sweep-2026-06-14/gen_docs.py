#!/usr/bin/env python3
"""Regenerate FINDINGS.md + ESCALATIONS.md from findings.json + the git log.

A finding is 'fixed' iff a commit since the sweep tags it `[Fxx]`. Escalations
(redesign/decision fix_class, plus the explicit EXTRA_ESCALATE set) get their
own ESCALATIONS.md; everything else not fixed is 'queued-safe'. Run from repo
root: python3 docs/explorations/sweep-2026-06-14/gen_docs.py
"""
import json, subprocess, re, pathlib

HERE = pathlib.Path("docs/explorations/sweep-2026-06-14")
ff = json.load(open(HERE / "findings.json"))
real, uncertain, refuted = ff["real"], ff["uncertain"], ff["refuted"]

log = subprocess.run(
    ["git", "log", "--oneline", "--no-decorate", "fb02a62~1..HEAD"],
    capture_output=True, text=True,
).stdout
fixed = {}
for line in log.splitlines():
    m = re.match(r"(\w+)\s+(.*)", line)
    if not m:
        continue
    h, subj = m.group(1), m.group(2)
    for tag in re.findall(r"\[([F0-9,]+)\]", subj):
        for fid in tag.split(","):
            fid = fid.strip()
            if re.match(r"F\d+", fid):
                fixed[fid] = (h, subj)

ESCALATE = {"redesign", "decision"}
EXTRA_ESCALATE = {"F25", "F43", "F54"}
recos = {
 "F41": "**Mitigated tonight by F02** — the drain now refuses to confirm a CommitResult whose committed+dead_lettered don't cover the batch. The deeper fix (encode the partition invariant in the SDK type: a validating constructor instead of public fields) is an irreversible 1.0 SDK-API choice. **Recommend:** fold into the freeze decisions; low urgency now that F02 guards the runtime.",
 "F42": "Related to F41. Whole-batch permanent/transient paths dead-letter raw payloads, bypassing `SinkRecord::into_payload`. Harmless for the built-in `Payload` record (identity transform); only matters for a third-party custom `Record`. **Recommend:** decide with the `Sink::commit` signature freeze — route all dead-letter paths through `into_payload`, or narrow its doc to 'per-record-result only'.",
 "F05": "Multi-batch segment retry re-dead-letters earlier sub-batches → duplicate dead-letter files. Duplicates are noise in a terminal inspection store, not data loss. A real fix needs per-sub-batch progress tracking within a segment retry (drain redesign). **Recommend:** defer post-1.0 unless observed.",
 "F24": "`QueueSender::len()` has no `is_empty()` (clippy::len_without_is_empty), latent only because there's no clippy lint config. Trivial + safe. **Recommend:** add `is_empty()` (could also just be a queued-safe fix).",
 "F48": "Public error enums (`DecodeError`,`WeirError`,`ClientError`) + `SinkHealth` + `CommitResult` aren't `#[non_exhaustive]`, so any post-1.0 variant/field is breaking; the error model explicitly expects variant growth. **Recommend (freeze):** mark the error enums + `SinkHealth` `#[non_exhaustive]` before 1.0; pair `CommitResult` with F41.",
 "F50": "`Header::new` takes a `payload_len` that `Envelope::new` always overwrites, and a bare `Header::encode` can still desync. **Recommend (freeze):** drop `payload_len` from `Header::new` so it can only be set via `Envelope` (which derives it). Pairs with R2/F49.",
 "F52": "Decode preserves arbitrary `flags` without checking zero, so a v1 daemon silently ignores future flag bits. **Recommend (freeze):** decide flag-evolution policy — reject nonzero now (clean error when a flag is added later) vs keep preserve-and-ignore. Tied to the wire-freeze cluster + reserved-Nack-byte decision.",
 "F25": "UnknownMessageType/UnknownDurability are nacked as `InternalError` (documented transient/keep-open) but the connection is then CLOSED — contradicting the wire contract. The clean fix needs either a dedicated Nack reason (a wire change) or deciding these stay open (the framing IS intact — valid header, unknown enum). **Recommend (freeze):** add a reserved `UnknownMessage` Nack reason in the wire-freeze cluster, or document+keep-open.",
 "F43": "The blocking client sets no read/write/connect timeouts, so a wedged daemon blocks a producer forever. A fix needs a DEFAULT timeout value (a judgment call — too short breaks slow-but-legit Sync acks under load) and/or a configurable setter. **Recommend:** add `set_read_timeout`/`set_write_timeout` setters (opt-in, no default behaviour change) + document; optionally a generous default (~30s).",
 "F54": "Config-load `warn!`s (unknown TOML keys, the dead_letter <1MiB advisory) are discarded because the tracing subscriber is initialised AFTER `Config::load()` — so a TOML typo silently takes defaults. Fix needs either collecting the warnings and emitting them post-init, or a reloadable filter (bootstrap level → reload to config.log_level). **Recommend:** collect-and-emit.",
}

# Escalations grouped by what they touch / how closely they're related, so we
# can work through them a group at a time. Groups 1-2 are irreversible 1.0-freeze
# gates (must be decided BEFORE 1.0); Group 3 is reversible (can land anytime,
# even post-1.0). Any escalated finding not listed here falls into an "Ungrouped"
# bucket so a future sweep can't silently drop a new escalation.
ESC_GROUPS = [
    {
        "title": "Group 1 — Wire-protocol v1 freeze",
        "tag": "irreversible · decide before 1.0",
        "blurb": "These lock the on-the-wire byte contract. Best decided together as "
                 "one wire-freeze session, alongside the deferred wire-freeze hooks "
                 "(reserved Nack-reason byte, language-neutral conformance vectors). "
                 "Once 1.0 ships at WIRE_VERSION 1, changing any of these needs a "
                 "version bump.",
        "ids": ["F25", "F50", "F52"],
    },
    {
        "title": "Group 2 — Public Rust API freeze",
        "tag": "irreversible · decide before 1.0",
        "blurb": "These shape the public Rust types and the Sink/SDK contract before "
                 "they're locked. CommitResult threads through F41 (its invariant) and "
                 "F48 (its exhaustiveness); F42 is the SinkRecord::into_payload half of "
                 "the same `Sink::commit` contract. `#[non_exhaustive]` + a validating "
                 "constructor are free now and impossible after 1.0.",
        "ids": ["F41", "F42", "F48"],
    },
    {
        "title": "Group 3 — Reversible fixes (not freeze-gated)",
        "tag": "reversible · land anytime",
        "blurb": "Independent fixes, each touching a different subsystem (client, config, "
                 "drain, queue). None locks an API or the wire, so any of these can land "
                 "before OR after 1.0 — pick them off when convenient. F05 is the only "
                 "one needing real work (a drain segment-retry redesign); F24 is trivial.",
        "ids": ["F43", "F54", "F05", "F24"],
    },
]

def sev_rank(s): return {"critical":0,"high":1,"medium":2,"low":3,"info":4}.get(s,5)
def is_esc(f): return f["fix_class"] in ESCALATE or f["id"] in EXTRA_ESCALATE
def esc_anchor(f): return f"{f['id'].lower()}--{re.sub(r'[^a-z0-9]+','-',f['title'].lower()).strip('-')}"
def gh_anchor(s):
    # Mirror GitHub's heading-anchor algorithm closely enough for our titles:
    # lowercase, drop punctuation (keep word chars, hyphens, spaces), spaces →
    # hyphens WITHOUT collapsing (so " — " → "--", matching GitHub).
    s = re.sub(r"[^\w\- ]", "", s.lower())
    return s.replace(" ", "-")

fixed_f = [f for f in real if f["id"] in fixed]
escalated = [f for f in real if f["id"] not in fixed and is_esc(f)]
queued = [f for f in real if f["id"] not in fixed and not is_esc(f)]
for lst in (fixed_f, escalated, queued):
    lst.sort(key=lambda f: (sev_rank(f["severity"]), f["id"]))

# ── ESCALATIONS.md ───────────────────────────────────────────────────────────
# Place each escalated finding into its group; collect any leftover into an
# "Ungrouped" bucket so nothing is silently dropped after a future sweep.
esc_by_id = {f["id"]: f for f in escalated}
group_order = []  # [(title, tag, blurb, [findings sorted])]
placed = set()
for g in ESC_GROUPS:
    members = [esc_by_id[i] for i in g["ids"] if i in esc_by_id]
    members.sort(key=lambda f: (sev_rank(f["severity"]), f["id"]))
    placed.update(f["id"] for f in members)
    if members:
        group_order.append((g["title"], g["tag"], g["blurb"], members))
leftover = [f for f in escalated if f["id"] not in placed]
leftover.sort(key=lambda f: (sev_rank(f["severity"]), f["id"]))
if leftover:
    group_order.append((
        "Ungrouped — newly surfaced",
        "needs triage",
        "Escalated findings not yet sorted into a group above — assign them when reviewed.",
        leftover,
    ))

e = []; ew = e.append
ew("# Escalations — your decisions needed (codebase sweep 2026-06-14)\n")
ew("> The findings from the sweep that I did **not** change autonomously: each needs a product/API call or a redesign. Nothing here is load-bearing-broken. Fixed items + queued-safe items live in [`FINDINGS.md`](FINDINGS.md).\n")
ew(f"**{len(escalated)} open decisions, grouped by what they touch.** Groups 1–2 are irreversible 1.0-freeze gates (decide before 1.0); Group 3 is reversible (land anytime). Jump to a group:\n")
for title, tag, _blurb, members in group_order:
    ids = ", ".join(f["id"] for f in members)
    ew(f"- **[{title}](#{gh_anchor(title)})** — _{tag}_ ({ids})")
ew("")
ew("---\n")
for title, tag, blurb, members in group_order:
    ew(f"## {title}")
    ew(f"_{tag}_\n")
    ew(f"{blurb}\n")
    for f in members:
        ew(f"### {f['id']} — {f['title']}  \n*({f['severity']} · {f['fix_class']} · {f['subsystem']} · `{f['file']}`)*\n")
        ew(f"{f['claim']}\n")
        ew(f"➡️ {recos.get(f['id'],'(see claim; needs your call)')}\n")
(HERE / "ESCALATIONS.md").write_text("\n".join(e))

# ── FINDINGS.md ──────────────────────────────────────────────────────────────
o = []; w = o.append
w("# Codebase sweep — findings & fixes (2026-06-14 → 15)\n")
w("> Output of the Max-tier multi-agent sweep (orthogonal lens + independent subsystem sweeps, completeness critic, per-finding adversarial verification). Plan: [`PLAN.md`](PLAN.md). Per-subsystem detail: [`subsystems/`](subsystems/). Raw data: `findings.json`. Every fix is its own commit (grep the log for `[Fxx]`), gated (fmt + clippy -D warnings + tests; DST 300-seed where durability-adjacent).\n")
w("## TL;DR\n")
w(f"- **{ff['totalRaw']} raw** → **{len(real)} confirmed-real** after adversarial verification (+ {len(uncertain)} uncertain, {len(refuted)} refuted).")
w(f"- **Fixed: {len(fixed_f)}** — incl. the one CRITICAL data-loss bug (F12), every high-severity bug, and the mediums.")
w(f"- **Needs your decision: {len(escalated)}** → all in **[`ESCALATIONS.md`](ESCALATIONS.md)** (separate file so they're easy to find).")
w(f"- **Queued safe fixes: {len(queued)}** — being worked through; this list shrinks as they land.\n")
w("## ⚠️ Your decisions live in [`ESCALATIONS.md`](ESCALATIONS.md)\n")
if escalated:
    w("Quick index (full detail + recommendations in that file):\n")
    for f in escalated:
        w(f"- **{f['id']}** *({f['severity']}, {f['subsystem']})* — {f['title']}")
else:
    w("_(all escalations resolved)_")
w("")
w("## ✅ Fixed\n")
w("| ID | Sev | Subsystem | Finding | Commit |")
w("|----|-----|-----------|---------|--------|")
for f in fixed_f:
    h, _ = fixed[f["id"]]; t = f["title"].replace("|", "\\|")
    w(f"| {f['id']} | {f['severity']} | {f['subsystem']} | {t} | `{h}` |")
w("")
w("## 🟡 Queued safe fixes (in progress)\n")
if queued:
    w("| ID | Sev | Subsystem | Finding |")
    w("|----|-----|-----------|---------|")
    for f in queued:
        t = f["title"].replace("|", "\\|")
        w(f"| {f['id']} | {f['severity']} | {f['subsystem']} | {t} |")
else:
    w("_(all queued-safe fixes landed)_")
w("")
w("## ⚪ Considered & dismissed\n")
for f in refuted:
    w(f"- **Refuted — {f['title']}** ({f['subsystem']}). {f.get('verdict_reason','')}")
for f in uncertain:
    w(f"- **Uncertain — {f['title']}** ({f['subsystem']}). Safe to fix (doc+test); in the queued list.")
w("\n## Per-subsystem detail\n")
for s in sorted(set(f["subsystem"] for f in real)):
    w(f"- [`subsystems/{s}.md`](subsystems/{s}.md)")
w("")
(HERE / "FINDINGS.md").write_text("\n".join(o))

print(f"fixed={len(fixed_f)} escalated={len(escalated)} queued={len(queued)}")
print("queued:", " ".join(f["id"] for f in queued))
