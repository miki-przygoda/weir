# weir-ctl

Admin and inspection CLI for the [weir](https://github.com/miki-przygoda/weir)
daemon.

A thin operator tool over the daemon's existing surfaces (the Unix socket and the
Prometheus `/metrics` endpoint): `health`, `push`, `metrics`, `segments`
(per-shard WAB inspect), `dl` (dead-letter `list` / `drop` / `requeue`), and
`quarantine` (`list` / `inspect` / `requeue`) for segments that crash recovery
set aside after a corrupt read, and `attest` (`seal` / `verify` / `head`) over a
sealed segment's `weir-attest` hash chain.

`--json` switches command output to machine-readable form — not only the
read/inspect subcommands: `push`, `dl drop`, `dl requeue`, `quarantine requeue`
and all three `attest` subcommands honour it too.

See the [workspace README](https://github.com/miki-przygoda/weir).
