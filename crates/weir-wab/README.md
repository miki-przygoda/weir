# weir-wab

On-disk WAB (write-ahead buffer) segment format and `SegmentReader` for
[weir](https://github.com/miki-przygoda/weir).

The single source of truth for weir's on-disk segment layout (`FORMAT_VERSION = 1`,
frozen at 1.0) and a streaming reader that CRC-verifies each record. Shared by the
daemon (`weir-server`) and the admin CLI (`weir-ctl`) so the two can never drift.
Depends only on `weir-core` + `crc32fast`; no async runtime.

See the [workspace README](https://github.com/miki-przygoda/weir).
