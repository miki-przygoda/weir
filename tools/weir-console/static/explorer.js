const $ = (sel) => document.querySelector(sel);
document.querySelectorAll("[data-weir-version]").forEach(e => e.textContent = "1.2.0");

async function getJSON(url) {
  const r = await fetch(url);
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

function chip(text, cls) { return `<span class="wc-chip ${cls || ""}">${text}</span>`; }

function renderTopbar(inv) {
  const t = inv.totals;
  $("#topbar").innerHTML = `
    <div class="sec-head"><span class="label">WAB</span><div class="rule"></div>
      <span class="sb-dim">${inv.wab_dir}</span></div>
    <p class="sb-dim">${t.segments} segments · ${t.sealed} sealed · ${t.active} active ·
      ${t.confirmed} confirmed · ${t.dead_letter} dead-letter · ${t.total_bytes} bytes</p>`;
}

function integrityChip(seg) {
  if (!seg.integrity) return "";
  return seg.integrity.ok ? chip("✓", "wc-ok") : chip("✗ " + seg.integrity.kind, "wc-bad");
}

function renderTree(inv) {
  const byShard = {};
  for (const s of inv.segments) (byShard[s.shard ?? "?"] ??= []).push(s);
  let html = "";
  for (const shard of Object.keys(byShard).sort()) {
    html += `<div class="label">shard ${shard}</div>`;
    for (const s of byShard[shard]) {
      const name = s.file.split("/").pop();
      html += `<div class="wc-seg" data-path="${s.file}">${name}${chip(s.state)}${integrityChip(s)}</div>`;
    }
  }
  html += `<div class="label">dead-letter</div><div class="wc-seg" data-deadletter="1">dead_letter/</div>`;
  $("#tree").innerHTML = html;
  $("#tree").querySelectorAll(".wc-seg[data-path]").forEach(el =>
    el.onclick = () => showSegment(el.dataset.path, inv));
  const dl = $("#tree").querySelector(".wc-seg[data-deadletter]");
  if (dl) dl.onclick = () => showDeadLetter();
}

let hexMode = false;
function recordRow(r) {
  if (r.error) return `<div class="wc-rec wc-bad">#${r.index} ERROR: ${r.error}</div>`;
  const preview = hexMode ? r.hex_preview : r.utf8_preview;
  return `<div class="wc-rec">#${r.index} · ${r.len}B · ${r.crc_ok ? '<span class="wc-ok">crc✓</span>' : '<span class="wc-bad">crc✗</span>'} · ${preview}</div>`;
}

function metaBlock(seg) {
  let h = "";
  if (seg.header) h += `<p class="sb-dim">header: shard ${seg.header.shard_id} · created_at ${seg.header.created_at} · v${seg.header.format_version}</p>`;
  if (seg.footer) h += `<p class="sb-dim">footer: ${seg.footer.record_count} records · ${seg.footer.data_bytes}B · crc ${seg.footer.file_crc32}</p>`;
  if (seg.confirmed) h += `<p class="sb-dim">confirmed: drained ${seg.confirmed.record_count} @ ${seg.confirmed.drained_at}</p>`;
  if (seg.integrity && !seg.integrity.ok) {
    const i = seg.integrity;
    h += `<p class="wc-bad">integrity: ${i.kind}${i.expected ? ` (expected ${i.expected}, computed ${i.computed})` : ""}${i.detail ? " — " + i.detail : ""}</p>`;
  }
  return h;
}

async function showSegment(path, inv) {
  const seg = inv.segments.find(s => s.file === path) || {};
  try {
    const data = await getJSON(`/api/wab/segment?path=${encodeURIComponent(path)}&offset=0&limit=200`);
    const term = data.terminated_cleanly === true ? "clean end (sentinel)" :
                 data.terminated_cleanly === false ? "torn tail (no sentinel)" : "—";
    $("#detail").innerHTML = `<div class="panel-title">${path.split("/").pop()}
        <button id="toggle" class="pt-right">${hexMode ? "utf8" : "hex"}</button></div>
      <div class="panel-body">${metaBlock(seg)}
        ${data.records.map(recordRow).join("")}
        <p class="sb-dim">— ${term}</p></div>`;
    $("#toggle").onclick = () => { hexMode = !hexMode; showSegment(path, inv); };
  } catch (e) {
    $("#detail").innerHTML = `<div class="panel-body wc-bad">error: ${e.message}</div>`;
  }
}

async function showDeadLetter() {
  try {
    const data = await getJSON("/api/wab/dead-letter");
    const segs = data.segments.map(s =>
      `<p class="label">${s.file}</p>${s.records.map(recordRow).join("")}`).join("");
    $("#detail").innerHTML = `<div class="panel-title">dead-letter</div>
      <div class="panel-body">${segs || "<p class='sb-dim'>empty</p>"}</div>`;
  } catch (e) { $("#detail").innerHTML = `<div class="panel-body wc-bad">error: ${e.message}</div>`; }
}

async function main() {
  try {
    const inv = await getJSON("/api/wab/segments");
    renderTopbar(inv); renderTree(inv);
  } catch (e) {
    $("#tree").innerHTML = `<span class="wc-bad">error: ${e.message}</span>`;
  }
}
main();
