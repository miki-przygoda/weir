# weir-ctl

Admin and inspection CLI for the [weir](https://github.com/miki-przygoda/weir)
daemon.

A thin operator tool over the daemon's existing surfaces (the Unix socket and the
Prometheus `/metrics` endpoint): `health`, `push`, `metrics`, `segments`
(per-shard WAB inspect), and `dl` (dead-letter `list` / `drop` / `requeue`).

See the [workspace README](https://github.com/miki-przygoda/weir).
