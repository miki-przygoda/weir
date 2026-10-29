# Escalations — your decisions needed (codebase sweep 2026-06-14)

> **Historical.** Written in June 2026 for weir v0.8–v1.0 and kept as a record of
> the reasoning. Much of it has since changed: the `Sync` tier is now `Durable`, and
> deterministic simulation testing, the S3 sink, `SinkBatch`, WAB compression and the
> health and `/status` routes have shipped. For how weir works today, read
> [architecture](../../architecture.md), [monitoring](../../monitoring.md) and [configuration](../../operations/configuration.md).

> The findings from the sweep that I did **not** change autonomously: each needs a product/API call or a redesign. Nothing here is load-bearing-broken. Fixed items + queued-safe items live in [`FINDINGS.md`](FINDINGS.md).

**0 open decisions, grouped by what they touch.** Each group's tag marks whether it's an irreversible 1.0-freeze gate (decide before 1.0) or a reversible fix (land anytime). Jump to a group:


---
