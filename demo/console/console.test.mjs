import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// Install the mock fetch shim (exactly as the browser would, loading mock.js first).
function mockFetch() {
  const g = {};
  new Function("globalThis", readFileSync(new URL("./mock.js", import.meta.url), "utf8"))(g);
  return g.fetch;
}
// Load a verbatim view-JS copy with its auto-main() stripped, exposing the functions we drive.
function loadView(file, documentStub, fetchStub) {
  let src = readFileSync(new URL("./" + file, import.meta.url), "utf8");
  src = src.replace(/main\(\);\s*$/, "");
  const names = ["main", "renderTree", "renderTopbar", "refreshStatus", "refreshDeadLetter", "poll"];
  const ret =
    "\ntry { return { " +
    names.map((n) => n + ": typeof " + n + "!=='undefined'?" + n + ":undefined").join(", ") +
    " }; } catch (e) { return {}; }";
  return new Function("document", "fetch", "setInterval", "requestAnimationFrame", src + ret)(
    documentStub,
    fetchStub,
    () => 0,
    () => 0,
  );
}
function makeDom() {
  const els = {};
  const makeEl = () => ({
    innerHTML: "", textContent: "", value: "", disabled: false, checked: false,
    onclick: null, oninput: null, style: {}, querySelector: () => null, querySelectorAll: () => [],
  });
  return { els, document: { querySelector: (s) => (els[s] ??= makeEl()), querySelectorAll: () => [] } };
}

test("Explorer renders the tree (incl. the corrupt segment) from the mock", async () => {
  const dom = makeDom();
  const app = loadView("explorer.js", dom.document, mockFetch());
  await app.main();
  const tree = dom.els["#tree"].innerHTML;
  assert.match(tree, /seg_00000001\.wab\.sealed/);
  assert.match(tree, /CrcMismatch/); // the corrupt segment's integrity badge
});

test("Ops renders the status header + dead-letter panel from the mock", async () => {
  const dom = makeDom();
  const app = loadView("ops.js", dom.document, mockFetch());
  await app.refreshStatus();
  await app.refreshDeadLetter();
  assert.match(dom.els["#ops-status"].innerHTML, /healthy/);
  assert.match(dom.els["#dl-body"].innerHTML, /dl_00000001\.wab\.sealed/);
});

test("Live renders counters from the mock after two polls", async () => {
  const dom = makeDom();
  const app = loadView("live.js", dom.document, mockFetch());
  await app.poll();
  await app.poll();
  assert.match(dom.els["#live-counters"].innerHTML, /healthy/);
  assert.match(dom.els["#live-counters"].innerHTML, /acc /);
});
