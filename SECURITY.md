# Security policy

weir is a local write-ahead daemon. On the default transport its trust
boundary is the Unix domain socket file's permissions, backed by a
default-on peer-uid check that refuses any connection whose peer euid does
not match the daemon's. The optional TCP + mutual-TLS listener (the `tls`
feature) extends that boundary to clients holding a certificate your CA
issued. Everything else builds on those. The detailed threat model,
in-scope and out-of-scope threats, and operator assumptions live at:

- [`docs/security/threat-model.md`](docs/security/threat-model.md) — overall
  trust model, threats considered, non-goals, and deployment expectations.
- [`docs/security/socket-bind.md`](docs/security/socket-bind.md) — TOCTOU
  analysis of the socket bind sequence and the hardening applied.

## Reporting a vulnerability

If you find a security issue, please **do not file a public GitHub issue**.

Instead, either:

- Open a [private security advisory](https://github.com/miki-przygoda/weir/security/advisories/new)
  on this repository (preferred — GitHub keeps the report and the fix
  thread together), or
- Contact the maintainer directly via the email on the GitHub profile.

Please include enough detail to reproduce, and indicate whether you would
like to be credited in the eventual advisory. This project is maintained
on a best-effort basis: expect acknowledgement within a few days, with fix
timelines depending on severity and complexity.

## Supported versions

weir follows [Semantic Versioning](https://semver.org/). Security fixes
land on `main` and ship in the latest `1.x` release; there are no LTS
branches, so running the most recent `1.x` is the supported configuration.

## What counts as a vulnerability

- Any way for a local user without socket access to read, modify, or
  inject WAB records.
- Any way to crash, deadlock, or otherwise wedge the daemon from inside
  the wire protocol (i.e. as a client who already has socket access).
- Any TOCTOU, symlink, or filesystem-race issue around the socket path,
  WAB directory, or dead-letter directory.
- Any case where the daemon writes data outside the configured
  `wab_dir` / dead-letter directory.
- On the TCP + mutual-TLS path: any way to complete a handshake without a
  certificate the configured CA issued, or to bypass client-certificate
  verification.

## What is explicitly NOT a vulnerability

These are documented in
[`docs/security/threat-model.md`](docs/security/threat-model.md):

- A client with legitimate socket access pushing malicious payloads — by
  design, weir trusts every connected client. The payloads are treated
  as opaque bytes.
- The daemon running as root (operator's responsibility — launch under a
  dedicated user).
- The socket placed in a world-writable directory (operator's
  responsibility — the bind hardening relies on a daemon-owned parent
  directory).
- Anything requiring uid-equivalent access to the daemon process (out of
  scope; OS-level isolation is the defense).
