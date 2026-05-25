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
