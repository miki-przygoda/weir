# weir-wab

On-disk WAB (write-ahead buffer) segment format and `SegmentReader` for
[weir](https://github.com/miki-przygoda/weir).

The single source of truth for weir's on-disk segment layout and a streaming reader
that CRC-verifies each record. Shared by the daemon (`weir-server`) and the admin CLI
(`weir-ctl`) so the two can never drift.

**Format v1 is frozen at 1.0. Format v2 is not** — it was added in 2.0.0 for zstd
payload compression and is a one-way door: a 1.x daemon refuses to read a v2
segment. Segments are still written as v1 unless compression is enabled; readers
accept both.

Depends on `weir-core`, `crc32fast` and `zstd` (vendored libzstd, so a C toolchain
is needed at build time); no async runtime.

See the [workspace README](https://github.com/miki-przygoda/weir).
