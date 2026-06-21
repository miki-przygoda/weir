const $ = (sel) => document.querySelector(sel);
document.querySelectorAll("[data-weir-version]").forEach((e) => (e.textContent = "1.2.0"));

async function getJSON(url, opts) {
  const r = await fetch(url, opts);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}
function chip(t, c) { return `<span class="wc-chip ${c || ""}">${t}</span>`; }

// ── pure helpers (unit-tested) ──
function statusLine(s) {
  if (!s || s.daemon !== "up") return `daemon ${chip("down", "wc-bad")}`;
  const m = s.summary || {};
  const health = m.sink_health === "healthy" ? chip("healthy", "wc-ok") : chip(m.sink_health || "?", "wc-bad");
  const alarms = (m.flusher_panics || 0) + (m.fsync_failures || 0);
  const alarmChip = alarms > 0 ? chip(`${alarms} alarm(s)`, "wc-bad") : "";
  return `daemon ${chip("up", "wc-ok")} · sink ${m.sink_type || "?"} ${health} · ` +
    `acc ${m.accepted || 0} / ack ${m.ack || 0} / nack ${m.nack || 0} · ` +
    `queue ${m.queue_depth || 0} · fsync ${m.fsync_avg_ms ?? "?"}ms · ` +
    `wab ${m.wab_bytes_on_disk || 0}B · dl ${m.dead_letter_bytes_on_disk || 0}B ${alarmChip}`;
}
function dlSummary(dl) { return `${dl.count || 0} dead-letter segment(s) · ${dl.total_bytes || 0} bytes`; }
function dropConfirmMatches(typed, count) { return String(typed).trim() === String(count); }
function requeuePreviewText(p) {
  // weir-ctl dl requeue --json: dry-run -> {segments, readable_segments, unreadable_segments, requeuable_records};
  // empty store -> {segments:0, requeued_records:0}; commit -> {segments, requeued_records, skipped_segments}.
  const recs = p.requeuable_records ?? p.requeued_records ?? 0;
  const segs = p.readable_segments ?? p.segments ?? 0;
  const skipped = p.unreadable_segments ?? p.skipped_segments ?? 0;
  return `would requeue ${recs} record(s) from ${segs} segment(s)` +
    (skipped ? ` · ${skipped} corrupt segment(s) skipped` : "");
}

// ── live status ──
let daemonLive = false;
async function refreshStatus() {
  try {
    const s = await getJSON("/api/ops/status");
    daemonLive = s.daemon === "up";
    $("#ops-status").innerHTML = statusLine(s);
  } catch (e) {
    $("#ops-status").innerHTML = `<span class="wc-bad">status error: ${e.message}</span>`;
  }
}

// ── dead-letter panel ──
async function refreshDeadLetter() {
  try {
    const dl = await getJSON("/api/ops/dead-letter");
    const rows = (dl.segments || []).map((s) => `<div class="wc-rec">${s.segment} · ${s.bytes}B</div>`).join("");
    $("#dl-body").innerHTML = `<p class="sb-dim">${dlSummary(dl)}</p>${rows || "<p class='sb-dim'>empty</p>"}`;
  } catch (e) {
    $("#dl-body").innerHTML = `<span class="wc-bad">error: ${e.message}</span>`;
  }
}

// ── modal + live-confirm ──
function openModal(html) { const m = $("#modal"); m.innerHTML = `<div class="wc-modal-box">${html}</div>`; m.style.display = "flex"; }
function closeModal() { const m = $("#modal"); if (!m) return; m.innerHTML = ""; m.style.display = "none"; }
function liveWarn() {
  return daemonLive
    ? `<p class="wc-bad">⚠ the daemon appears to be running. <label><input type="checkbox" id="live-ok"> I understand the daemon is running</label></p>`
    : "";
}
function liveConfirmed() { if (!daemonLive) return true; const c = $("#live-ok"); return !!(c && c.checked); }

// ── requeue flow ──
async function requeueFlow() {
  let preview;
  try { preview = await getJSON("/api/ops/requeue/preview?durability=batched", { method: "POST" }); }
  catch (e) { openModal(`<p class="wc-bad">preview failed: ${e.message}</p><button id="x-close">close</button>`); $("#x-close").onclick = closeModal; return; }
  openModal(
    `<div class="panel-title">Requeue all</div>
     <p>${requeuePreviewText(preview)}</p>
     <p class="sb-dim">Re-delivery is at-least-once; identical payloads are deduped by the sink's idempotency key.</p>
     <label>Durability: <select id="rq-dur">
       <option value="batched" selected>batched</option><option value="sync">sync</option><option value="buffered">buffered</option>
     </select></label>
     ${liveWarn()}
     <div class="wc-actions"><button id="rq-go">Requeue</button><button id="rq-cancel">Cancel</button></div>
     <div id="rq-result"></div>`
  );
  $("#rq-cancel").onclick = closeModal;
  $("#rq-go").onclick = async () => {
    if (!liveConfirmed()) { $("#rq-result").innerHTML = `<span class="wc-bad">tick the box to proceed</span>`; return; }
    const dur = $("#rq-dur").value;
    try {
      const r = await getJSON(`/api/ops/requeue?durability=${encodeURIComponent(dur)}`, { method: "POST" });
      $("#rq-result").innerHTML = `<span class="wc-ok">requeued ${r.requeued_records ?? 0} record(s)</span>`;
      refreshStatus(); refreshDeadLetter();
    } catch (e) { $("#rq-result").innerHTML = `<span class="wc-bad">${e.message}</span>`; }
  };
}

// ── drop flow ──
async function dropFlow() {
  let preview;
  try { preview = await getJSON("/api/ops/drop/preview", { method: "POST" }); }
  catch (e) { openModal(`<p class="wc-bad">preview failed: ${e.message}</p><button id="x-close">close</button>`); $("#x-close").onclick = closeModal; return; }
  // weir-ctl dl drop --json dry-run -> {candidates, candidate_bytes} (whole-segment; no record count).
  const count = preview.candidates ?? 0;
  openModal(
    `<div class="panel-title">Drop all dead-letter segments</div>
     <p class="wc-bad">IRREVERSIBLE — would delete ${count} segment(s) / ${preview.candidate_bytes ?? 0} bytes.</p>
     <label>Type the segment count (<b>${count}</b>) to confirm: <input id="dr-count" /></label>
     ${liveWarn()}
     <div class="wc-actions"><button id="dr-go" disabled>Drop</button><button id="dr-cancel">Cancel</button></div>
     <div id="dr-result"></div>`
  );
  $("#dr-cancel").onclick = closeModal;
  const inp = $("#dr-count"), go = $("#dr-go");
  inp.oninput = () => { go.disabled = !dropConfirmMatches(inp.value, count); };
  go.onclick = async () => {
    if (!liveConfirmed()) { $("#dr-result").innerHTML = `<span class="wc-bad">tick the box to proceed</span>`; return; }
    try {
      const r = await getJSON("/api/ops/drop", { method: "POST" });
      $("#dr-result").innerHTML = `<span class="wc-ok">dropped ${r.dropped ?? 0} segment(s)</span>`;
      refreshStatus(); refreshDeadLetter();
    } catch (e) { $("#dr-result").innerHTML = `<span class="wc-bad">${e.message}</span>`; }
  };
}

function wireActions() {
  const rq = $("#act-requeue"), dr = $("#act-drop");
  if (rq) rq.onclick = requeueFlow;
  if (dr) dr.onclick = dropFlow;
}

async function main() {
  wireActions();
  await refreshStatus();
  await refreshDeadLetter();
  if (typeof setInterval === "function") setInterval(refreshStatus, 5000);
}
main();
