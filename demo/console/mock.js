// Static-demo backend shim for weir-console. Installs globalThis.fetch so the REAL view
// scripts (explorer.js / ops.js / live.js — verbatim copies) run with NO backend, against
// canned JSON in the verified real /api/* shapes. Loaded BEFORE each view script.
(function () {
  // ── Explorer: WAB inventory (incl. one CrcMismatch segment) ──
  const SEGMENTS = {
    wab_dir: "/var/lib/weir/wab  (demo · sample data)",
    totals: { segments: 6, sealed: 5, active: 1, confirmed: 2, dead_letter: 6, total_bytes: 26240 },
    segments: [
      {
        shard: "00", file: "shard_00/seg_00000001.wab.sealed", state: "sealed", size_bytes: 4096,
        header: { format_version: 1, shard_id: 0, created_at: 1782000000000000000 },
        footer: { record_count: 3, data_bytes: 60, file_crc32: "0x1a2b3c4d", sealed_at: 1782000001000000000 },
        integrity: { ok: true },
        confirmed: { sealed_at: 1782000001000000000, record_count: 3, drained_at: 1782000002000000000 },
      },
      {
        shard: "00", file: "shard_00/seg_00000002.wab.sealed", state: "sealed", size_bytes: 4096,
        header: { format_version: 1, shard_id: 0, created_at: 1782000003000000000 },
        footer: { record_count: 2, data_bytes: 40, file_crc32: "0xdeadbeef", sealed_at: 1782000004000000000 },
        integrity: { ok: false, kind: "CrcMismatch", expected: "0xdeadbeef", computed: "0x0badf00d" },
      },
      {
        shard: "00", file: "shard_00/seg_00000004.wab.sealed", state: "sealed", size_bytes: 5376,
        header: { format_version: 1, shard_id: 0, created_at: 1782000006000000000 },
        // a torn tail: the daemon was killed mid-seal, so the footer never landed.
        integrity: { ok: false, kind: "MissingFooter" },
      },
      {
        shard: "00", file: "shard_00/seg_00000003.wab", state: "active", size_bytes: 512,
        header: { format_version: 1, shard_id: 0, created_at: 1782000005000000000 },
      },
      {
        shard: "01", file: "shard_01/seg_00000005.wab.sealed", state: "sealed", size_bytes: 6144,
        header: { format_version: 1, shard_id: 1, created_at: 1782000006500000000 },
        footer: { record_count: 3, data_bytes: 60, file_crc32: "0x77c1e0a2", sealed_at: 1782000007000000000 },
        integrity: { ok: true },
      },
      {
        shard: "01", file: "shard_01/seg_00000006.wab.sealed", state: "sealed", size_bytes: 6144,
        header: { format_version: 1, shard_id: 1, created_at: 1782000008000000000 },
        footer: { record_count: 3, data_bytes: 60, file_crc32: "0x3fa9b1c4", sealed_at: 1782000009000000000 },
        integrity: { ok: true },
        confirmed: { sealed_at: 1782000009000000000, record_count: 3, drained_at: 1782000010000000000 },
      },
    ],
  };

  function recordsFor(path) {
    if (path.indexOf("seg_00000002") !== -1) {
      // corrupt: one good record, then an error row (the reader stops there)
      return {
        file: path, header: SEGMENTS.segments[1].header,
        records: [
          { index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 31", utf8_preview: "order-1" },
          { index: 1, crc_ok: false, error: "CrcMismatch" },
        ],
        terminated_cleanly: false,
      };
    }
    if (path.indexOf("seg_00000004") !== -1) {
      // truncated/torn segment: a few good records, then the tail is gone (no sentinel).
      return {
        file: path, header: SEGMENTS.segments[2].header,
        records: [
          { index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 34", utf8_preview: "order-4" },
          { index: 1, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 35", utf8_preview: "order-5" },
        ],
        terminated_cleanly: false, // torn tail — no clean-end sentinel
      };
    }
    if (path.indexOf("seg_00000003") !== -1) {
      return {
        file: path, header: SEGMENTS.segments[3].header,
        records: [{ index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 39", utf8_preview: "order-9" }],
        terminated_cleanly: null,
      };
    }
    return {
      file: path, header: SEGMENTS.segments[0].header,
      records: [
        { index: 0, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 31", utf8_preview: "order-1" },
        { index: 1, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 32", utf8_preview: "order-2" },
        { index: 2, len: 7, crc_ok: true, hex_preview: "6f 72 64 65 72 2d 33", utf8_preview: "order-3" },
      ],
      terminated_cleanly: true,
    };
  }

  function verifyFor(path) {
    if (path.indexOf("seg_00000002") !== -1) {
      return { ok: false, kind: "CrcMismatch", expected: "0xdeadbeef", computed: "0x0badf00d" };
    }
    if (path.indexOf("seg_00000004") !== -1) {
      return { ok: false, kind: "MissingFooter" };
    }
    return { ok: true };
  }

  const WAB_DEAD_LETTER = {
    segments: [
      {
        file: "dead_letter/dl_00000001.wab.sealed",
        records: [
          { index: 0, len: 8, crc_ok: true, hex_preview: "72 65 6a 65 63 74 2d 31", utf8_preview: "reject-1" },
          { index: 1, len: 8, crc_ok: true, hex_preview: "72 65 6a 65 63 74 2d 32", utf8_preview: "reject-2" },
        ],
      },
    ],
  };

  // ── Ops: mutable dead-letter store (a backlog that piled up during a sink outage) ──
  let dlStore = [
    { segment: "dl_00000001.wab.sealed", bytes: 6144, records: 80 },
    { segment: "dl_00000002.wab.sealed", bytes: 4992, records: 64 },
    { segment: "dl_00000003.wab.sealed", bytes: 5520, records: 72 },
    { segment: "dl_00000004.wab.sealed", bytes: 3120, records: 40 },
    { segment: "dl_00000005.wab.sealed", bytes: 4368, records: 56 },
    { segment: "dl_00000006.wab.sealed", bytes: 2184, records: 28 },
  ];
  const dlCount = () => dlStore.length;
  const dlBytes = () => dlStore.reduce((a, s) => a + s.bytes, 0);
  const dlRecords = () => dlStore.reduce((a, s) => a + s.records, 0);

  // ── Live: counters climb like a busy producer so the pipeline + sparklines move ──
  let accepted = 1_284_000, ack = 1_283_950;
  const nack = 12;
  function opsStatus() {
    accepted += 1800 + Math.floor(Math.random() * 2400); // ~1.2k–2.8k/s over a ~1.5s poll
    ack = accepted - Math.floor(Math.random() * 40);
    return {
      daemon: "up", metrics_addr: "127.0.0.1:9185  (demo)",
      summary: {
        accepted, ack, nack,
        fsync_avg_ms: 0.38, queue_depth: 3 + Math.floor(Math.random() * 11),
        wab_bytes_on_disk: 41_943_040, dead_letter_bytes_on_disk: dlBytes(),
        sink_type: "http", sink_health: "healthy", flusher_panics: 0, fsync_failures: 0,
      },
    };
  }

  function opsDlList() {
    return { dead_letter_dir: "/var/lib/weir/wab/dead_letter  (demo)", count: dlCount(), total_bytes: dlBytes(), segments: dlStore.slice() };
  }
  function dropPreview() { return { dry_run: true, candidates: dlCount(), candidate_bytes: dlBytes(), dropped: 0, dropped_bytes: 0 }; }
  function dropCommit() { const c = dlCount(), b = dlBytes(); dlStore = []; return { dry_run: false, candidates: c, dropped: c, dropped_bytes: b, failures: 0 }; }
  function requeuePreview() { return { dry_run: true, segments: dlCount(), readable_segments: dlCount(), unreadable_segments: 0, requeuable_records: dlRecords() }; }
  function requeueCommit(durability) { const s = dlCount(), r = dlRecords(); dlStore = []; return { dry_run: false, segments: s, requeued_records: r, segments_cleared: s, skipped_segments: 0, delete_failures: 0, durability: durability || "batched" }; }

  function respond(body, ok) {
    return Promise.resolve({ ok: ok !== false, status: ok === false ? 500 : 200, statusText: "OK", json: () => Promise.resolve(body) });
  }

  function mockFetch(url, opts) {
    const u = new URL(url, "http://demo.local");
    const p = u.pathname;
    const method = (opts && opts.method) || "GET";
    if (p === "/api/wab/segments") return respond(SEGMENTS);
    if (p === "/api/wab/segment") return respond(recordsFor(u.searchParams.get("path") || ""));
    if (p === "/api/wab/verify") return respond(verifyFor(u.searchParams.get("path") || ""));
    if (p === "/api/wab/dead-letter") return respond(WAB_DEAD_LETTER);
    if (p === "/api/ops/status") return respond(opsStatus());
    if (p === "/api/ops/dead-letter") return respond(opsDlList());
    if (p === "/api/ops/requeue/preview") return respond(requeuePreview());
    if (p === "/api/ops/requeue") return respond(requeueCommit(u.searchParams.get("durability")));
    if (p === "/api/ops/drop/preview") return respond(dropPreview());
    if (p === "/api/ops/drop") return respond(dropCommit());
    return respond({ error: "mock: unhandled " + method + " " + p }, false);
  }

  globalThis.fetch = mockFetch;
})();
