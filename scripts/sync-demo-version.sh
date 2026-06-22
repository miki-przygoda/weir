#!/usr/bin/env bash
# Regenerate demo/version.js from the canonical workspace version in Cargo.toml.
#
# The demo bundle's banners (demo/**.html) show the version via a single
# `<span data-weir-version>` filled at load time from demo/version.js — so a
# version bump means editing Cargo.toml and running this script, never hand-editing
# the HTML. CI (the `lint` job) runs this and fails if demo/version.js drifts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The [workspace.package] version — the FIRST `version = ` line after that header
# (the [workspace.dependencies] pins are inline `... version = "..."` and don't
# start a line, so an anchored match can't pick them up).
VERSION="$(awk '
  /^\[workspace\.package\]/ { in_pkg = 1; next }
  /^\[/                     { in_pkg = 0 }
  in_pkg && /^version = / {
    gsub(/^version = "|"$/, ""); print; exit
  }
' "$ROOT/Cargo.toml")"

if [ -z "$VERSION" ]; then
  echo "sync-demo-version: could not read [workspace.package] version from Cargo.toml" >&2
  exit 1
fi

cat > "$ROOT/demo/version.js" <<EOF
// Single source of truth for the version shown in the demo bundle's banners.
// GENERATED from [workspace.package] version in Cargo.toml by
// scripts/sync-demo-version.sh — CI fails if it drifts. Do not edit by hand.
window.WEIR_VERSION = "$VERSION";
document.addEventListener("DOMContentLoaded", function () {
  for (const el of document.querySelectorAll("[data-weir-version]")) {
    el.textContent = window.WEIR_VERSION;
  }
});
EOF

echo "sync-demo-version: demo/version.js -> weir $VERSION"
