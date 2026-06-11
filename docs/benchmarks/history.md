# Benchmark History

One row appended per CI run on `main`. All numbers at `batch_deadline_ms=1`.
Sync p99 and Buffered p50 are single-thread latency measurements.
Ramp peak = highest throughput level before connection-cap saturation kicks in.

| Version | Date | Runs | Sync RPS | Sync p99 | Buf p50 | Ramp peak RPS |
|---------|------|------|----------|----------|---------|---------------|
| 0.2.0 | 2026-05-24 | 2 | 623 | 3.2 ms | — | 17,229 |
| 0.3.0 | 2026-05-25 | 5 | 621 | 3.6 ms | 1.2 ms | 17,116 |
| 0.3.0 | 2026-05-25 | 5 | 692 | 1.7 ms | 1.2 ms | 17,316 |
| 0.3.0 | 2026-05-25 | 5 | 650 | 2.0 ms | 1.2 ms | 17,298 |
| 0.5.0 | 2026-06-10 | 5 | 1,933 | 1.5 ms | 74 µs | 47,492 |
| 0.5.0 | 2026-06-10 | 5 | 3,216 | 687 µs | 40 µs | 83,857 |
| 0.6.0 | 2026-06-11 | 5 | 1,027 | 28.2 ms | 51 µs | 64,505 |
