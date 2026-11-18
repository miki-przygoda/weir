# release-4-2-0-sweep: log

A temper stabilise sweep (part 4a only: sweep and verification, no fixes) over everything weir 4.2.0
changes since 4.1.0, before the v4.2.0 tag. The owner picks what to fix from the list below.

- Base: `v4.1.0` (09762e7)
- Tip: `main` af7db97 (release PR #79, 1f93f8f, plus a bench-baseline commit)
- Scope: `git diff v4.1.0...sweep/4.2.0` (188 files): PR #77's cleanup and the d10-and-batch-error epic
- Date: 2026-11-18, cc4 box
- Already decided, not defects: D10 as a hard error; the coordinate dedup key and RecordId format
  change; the reverted attest gap fixes (attest-disposal-log); `PayloadTooLarge` count-as-bytes;
  a 2,048-record batch passing the client cap but Nacked by a default daemon.
