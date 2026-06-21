const $ = (sel) => document.querySelector(sel);
document.querySelectorAll("[data-weir-version]").forEach((e) => (e.textContent = "1.2.0"));

const POLL_MS = 1500;
const SPARK_CAP = 60;

async function getStatus() {
  const r = await fetch("/api/ops/status");
  const body = await r.json();
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}

// ── pure helpers (unit-tested) ──
// Per-second rates from two counter samples. First sample (prev null) -> zeros.
// Counter reset (cur < prev, daemon restarted) -> 0 for that series, never negative.
function computeRates(prev, cur, dtSeconds) {
  const rate = (a, b) => {
    if (prev == null || dtSeconds <= 0) return 0;
    const d = (b ?? 0) - (a ?? 0);
    return d < 0 ? 0 : d / dtSeconds;
  };
  return {
    accepted: rate(prev && prev.accepted, cur.accepted),
    ack: rate(prev && prev.ack, cur.ack),
    nack: rate(prev && prev.nack, cur.nack),
  };
}

// Fixed-capacity ring buffer (oldest dropped past cap).
function makeRing(cap) {
  const buf = [];
  return {
    push(v) { buf.push(v); if (buf.length > cap) buf.shift(); return buf; },
    values() { return buf.slice(); },
  };
}

function fmtRate(r) { return (r >= 1000 ? (r / 1000).toFixed(1) + "k" : r.toFixed(0)) + "/s"; }
function fmtBytes(b) {
  b = b || 0;
  const K = 1024;
  if (b >= K * K) return (b / (K * K)).toFixed(1) + " MiB";
  if (b >= K) return (b / K).toFixed(1) + " KiB";
  return b + " B";
}
function chip(t, c) { return `<span class="wc-chip ${c || ""}">${t}</span>`; }

function countersHtml(summary, rates) {
  const m = summary || {};
  const health = m.sink_health === "healthy" ? chip("healthy", "wc-ok") : chip(m.sink_health || "?", "wc-bad");
  const alarms = (m.flusher_panics || 0) + (m.fsync_failures || 0);
  const alarmChip = alarms > 0 ? chip(`${alarms} alarm(s)`, "wc-bad") : chip("0 alarms", "wc-ok");
  return `acc ${fmtRate(rates.accepted)} · ack ${fmtRate(rates.ack)} · nack ${fmtRate(rates.nack)} · ` +
    `queue ${m.queue_depth || 0} · fsync ${m.fsync_avg_ms ?? "?"}ms · ` +
    `sink ${m.sink_type || "?"} ${health} · dl ${fmtBytes(m.dead_letter_bytes_on_disk)} · ${alarmChip}`;
}

function sparkline(vals) {
  if (!vals.length) return "";
  const blocks = "▁▂▃▄▅▆▇█";
  const max = Math.max(1, ...vals);
  return vals.map((v) => blocks[Math.min(blocks.length - 1, Math.floor((v / max) * (blocks.length - 1)))]).join("");
}

// ── live state ──
let prevSample = null;
let rates = { accepted: 0, ack: 0, nack: 0 };
let live = false;
let queueNow = 0;
const accRing = makeRing(SPARK_CAP);
const queueRing = makeRing(SPARK_CAP);

function renderCounters(status) {
  if (!status || status.daemon !== "up") {
    $("#live-counters").innerHTML = `<span class="wc-bad">waiting for daemon at ${(status && status.metrics_addr) || "?"}…</span>`;
    return;
  }
  $("#live-counters").innerHTML = countersHtml(status.summary, rates);
}

function renderSparks() {
  $("#spark-acc").textContent = sparkline(accRing.values());
  $("#spark-queue").textContent = sparkline(queueRing.values());
}

async function poll() {
  try {
    const status = await getStatus();
    if (status.daemon === "up") {
      const cur = status.summary || {};
      rates = computeRates(prevSample, cur, POLL_MS / 1000);
      prevSample = cur;
      live = true;
      queueNow = cur.queue_depth || 0;
      accRing.push(rates.accepted);
      queueRing.push(queueNow);
    } else {
      live = false;
      prevSample = null; // re-baseline when the daemon returns
    }
    renderCounters(status);
    renderSparks();
  } catch (e) {
    live = false;
    prevSample = null;
    $("#live-counters").innerHTML = `<span class="wc-bad">${e.message}</span>`;
  }
}

// ── canvas pipeline animation ──
const STAGES = ["Producer", "Socket", "WAB", "fsync", "Ack", "Drain", "Sink"];
let dots = [];
let spawnAccum = 0;
let lastTs = 0;

function drawPipeline(ctx, W, H) {
  ctx.clearRect(0, 0, W, H);
  const lane = H / 2;
  ctx.fillStyle = "#9aa4b2";
  ctx.font = "11px monospace";
  STAGES.forEach((s, i) => {
    const x = (i / (STAGES.length - 1)) * (W - 60) + 20;
    ctx.fillText(s, x, lane - 12);
    if (s === "Drain") ctx.fillText(`queue:${queueNow}`, x, lane + 22);
  });
  ctx.strokeStyle = "#2a2a2a";
  ctx.beginPath();
  ctx.moveTo(10, lane);
  ctx.lineTo(W - 10, lane);
  ctx.stroke();
  ctx.fillStyle = live ? "#38bdf8" : "#555";
  for (const d of dots) {
    ctx.beginPath();
    ctx.arc(d.x, lane, 3, 0, Math.PI * 2);
    ctx.fill();
  }
  if (!live) {
    ctx.fillStyle = "#f87171";
    ctx.fillText("waiting for daemon…", W / 2 - 50, lane - 30);
  }
}

function frame(ts) {
  const cv = $("#cv");
  const ctx = cv && cv.getContext ? cv.getContext("2d") : null;
  const dt = lastTs ? (ts - lastTs) / 1000 : 0;
  lastTs = ts;
  if (ctx) {
    if (live) {
      spawnAccum += Math.min(rates.accepted, 200) * dt; // cap spawn for sanity
      while (spawnAccum >= 1) { dots.push({ x: 0 }); spawnAccum -= 1; }
    }
    const W = cv.width, H = cv.height, speed = W / 2.5; // cross in ~2.5s
    for (const d of dots) d.x += speed * dt;
    dots = dots.filter((d) => d.x <= W);
    drawPipeline(ctx, W, H);
  }
  requestAnimationFrame(frame);
}

async function main() {
  await poll();
  if (typeof setInterval === "function") setInterval(poll, POLL_MS);
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(frame);
}
main();
