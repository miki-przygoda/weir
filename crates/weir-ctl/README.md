# weir-ctl

Admin and inspection CLI for the [weir](https://github.com/miki-przygoda/weir)
daemon.

An operator tool over the daemon's surfaces: the producer Unix socket, the
metrics listener (`/metrics`, `/status`), the admin Unix socket, and the
`wab_dir` on disk (read-only). Subcommands: `status` (can the daemon take
ingest, and why not — admin socket, then `/status`, then `/metrics` plus disk,
then disk alone; exit 0 ready, 1 not ready (including a dead daemon whose
`wab_dir` was read from disk), 2 unreachable with no offline facts, or identity
mismatch, 3 ready with a critical condition), `reload` (TLS certificates, the
same path as SIGHUP; exit 0 applied or no-op, 1 failed, 2 no admin socket, no
reply or identity mismatch), `drain` (`pause` / `resume` / `retry-stranded`;
pause is in-memory only, and retry-stranded skips segments still inside their
cool-down; `--wait[=SECS]`, at most 30, waits for the drain to act; exit 0
done, 4 requested but not yet in effect, 1 error, 2 as for `reload`),
`health`, `push`, `metrics`, `segments` (per-shard WAB inspect, `--files` for
every segment file), `dl` (dead-letter `list` / `drop` / `requeue`),
`quarantine` (`list` / `inspect` / `requeue`) for segments that crash recovery
set aside after a corrupt read, and `attest` (`seal` / `verify` / `head`) over
a sealed segment's `weir-attest` hash chain.

The admin socket defaults to `WEIR_ADMIN_SOCKET_PATH`, then to
`<parent(wab_dir)>/<basename(wab_dir)>.admin.sock` derived from `--wab-dir` /
`WEIR_WAB_DIR`, then to `/var/lib/weir/wab.admin.sock`. Before trusting an
answer, weir-ctl checks that the path is a 0600 socket in a directory nobody
else can write to, that the daemon runs as weir-ctl's uid, the wab_dir's owner
or root, and that the daemon serves the same `wab_dir` inode.

`--json` switches command output to machine-readable form — not only the
read/inspect subcommands: `status`, `reload`, all three `drain` subcommands,
`push`, `dl drop`, `dl requeue`, `quarantine requeue` and all three `attest`
subcommands honour it too.

See the [workspace README](https://github.com/miki-przygoda/weir).
