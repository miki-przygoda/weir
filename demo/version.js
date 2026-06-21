// Single source of truth for the version shown in the demo bundle's banners.
// GENERATED from [workspace.package] version in Cargo.toml by
// scripts/sync-demo-version.sh — CI fails if it drifts. Do not edit by hand.
window.WEIR_VERSION = "1.2.0";
document.addEventListener("DOMContentLoaded", function () {
  for (const el of document.querySelectorAll("[data-weir-version]")) {
    el.textContent = window.WEIR_VERSION;
  }
});
