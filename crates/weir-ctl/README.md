# weir-ctl

Admin and inspection CLI for the [weir](https://github.com/miki-przygoda/weir)
daemon.

A thin operator tool over the daemon's existing surfaces (the Unix socket and the
Prometheus `/metrics` endpoint): `health`, `push`, `metrics`, `segments`
(per-shard WAB inspect), `dl` (dead-letter `list` / `drop` / `requeue`), and
`quarantine` (`list` / `inspect` / `requeue`) for segments that crash recovery
set aside after a corrupt read.

`--json` switches the read/inspect subcommands to machine-readable output.

See the [workspace README](https://github.com/miki-przygoda/weir).
