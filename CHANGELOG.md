# Changelog

All notable changes to `weir` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until v1.0, breaking changes may land in minor releases. The wire protocol
version (the `VERSION` byte in the envelope header) is tracked separately
under **Wire protocol** below and may evolve independently of crate versions.

---

## [Unreleased]

### Notes
- `weir` evolves from an earlier private project, HTDIP (Hardware-Tuned Data
  Ingestion Pipeline), which was built to a real production spec for a
  Rails + MySQL stack. `weir` removes the domain coupling entirely
  and reshapes the daemon as a sink-agnostic write buffer with explicit
  durability tiers, a user-implementable sink trait, and a public crate
  surface. The HTDIP write-ahead buffer design, crash-recovery proof, and
  benchmark methodology carry over; the command schema, Rails integration,
  and MySQL-specific drain logic do not.

### Added
- Conversion plan and architecture decisions (workspace-local, not committed).
- Initial repository scaffolding.

---

## Wire protocol

The envelope format carries a `VERSION` byte in its fixed header. Changes to
the wire format are logged here so producers and consumers can negotiate.

### v1 (unreleased)
- Initial design. 16-byte header (magic, version, type, durability, flags,
  payload length, header CRC32) followed by payload and payload CRC32.

---

[Unreleased]: https://github.com/miki-przygoda/weir