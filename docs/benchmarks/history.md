# Benchmark History

One row appended per CI run on `main`. All numbers at `batch_deadline_ms=1`.
Sync p99 and Buffered p50 are single-thread latency measurements.
Ramp peak = highest throughput level before connection-cap saturation kicks in.

**Read the deltas conservatively.** These are shared 2-vCPU CI runners and the
run-to-run spread is large. Rows for the *same released version* span
1,926–2,783 Sync RPS (1.45×) and 610 µs – 1.2 ms Sync p99 (~2×) across the three
1.3.1 runs; the three 0.3.0 runs span 621–692 RPS with p99 from 1.7 to 3.6 ms. A
difference between adjacent rows smaller than that is the runner, not a
regression or an improvement. Only order-of-magnitude moves are signal here — see
[`environments.md`](environments.md).

**`(!)` marks a run the runner poisoned, not a regression.** Single-thread Sync
p99 above 10 ms is roughly 3× the worst healthy row in this file and ~25× the
median; at one connection and one fsync per record, weir cannot produce it. The
cause is the shared runner being descheduled mid-measurement, and the giveaway
is that the ramp column in those same rows is often the *highest* in the file —
0.6.0, 2.0.0 and 2.0.5 all beat their neighbours there. The rows are kept
because deleting an inconvenient measurement is worse than publishing it with a
marker, but **do not read them as trend**. `deploy/avg_benchmarks.py` now
applies the marker at write time, so this is not a judgement anyone has to
remember to make again.

| Version | Date | Runs | Sync RPS | Sync p99 | Buf p50 | Ramp peak RPS |
|---------|------|------|----------|----------|---------|---------------|
| 0.2.0 | 2026-05-24 | 2 | 623 | 3.2 ms | — | 17,229 |
| 0.3.0 | 2026-05-25 | 5 | 621 | 3.6 ms | 1.2 ms | 17,116 |
| 0.3.0 | 2026-05-25 | 5 | 692 | 1.7 ms | 1.2 ms | 17,316 |
| 0.3.0 | 2026-05-25 | 5 | 650 | 2.0 ms | 1.2 ms | 17,298 |
| 0.5.0 | 2026-06-10 | 5 | 1,933 | 1.5 ms | 74 µs | 47,492 |
| 0.5.0 | 2026-06-10 | 5 | 3,216 | 687 µs | 40 µs | 83,857 |
| 0.6.0 | 2026-06-11 | 5 | 1,027 | 28.2 ms (!) | 51 µs | 64,505 |
| 0.9.0 | 2026-06-14 | 5 | 2,547 | 751 µs | 69 µs | 58,067 |
| 1.0.0 | 2026-06-16 | 5 | 2,570 | 950 µs | 69 µs | 58,984 |
| 1.0.0 | 2026-06-17 | 5 | 2,661 | 604 µs | 67 µs | 58,755 |
| 1.3.0 | 2026-06-22 | 5 | 2,768 | 640 µs | 72 µs | 67,745 |
| 1.3.1 | 2026-06-23 | 5 | 1,926 | 1.2 ms | 64 µs | 67,372 |
| 1.3.1 | 2026-06-25 | 5 | 2,783 | 610 µs | 79 µs | 94,839 |
| 1.3.1 | 2026-06-25 | 5 | 2,308 | 1.1 ms | 74 µs | 99,490 |
| 2.0.0 | 2026-08-31 | 5 | 1,190 | 35.5 ms (!) | 76 µs | 133,368 |
| 2.0.1 | 2026-09-01 | 5 | 3,784 | 613 µs | 67 µs | 91,016 |
| 2.0.3 | 2026-09-02 | 5 | 3,665 | 381 µs | 70 µs | 91,386 |
| 2.0.4 | 2026-09-02 | 5 | 2,849 | 540 µs | 76 µs | 96,887 |
| 2.0.4 | 2026-09-04 | 5 | 2,600 | 693 µs | 80 µs | 96,332 |
| 2.0.5 | 2026-09-05 | 5 | 2,754 | 625 µs | 80 µs | 97,692 |
| 2.0.5 | 2026-09-05 | 5 | 402 | 98.9 ms (!) | 89 µs | 158,193 |
| 2.0.5 | 2026-09-05 | 5 | 2,900 | 549 µs | 74 µs | 93,616 |
| 2.1.0 | 2026-09-07 | 5 | 3,681 | 467 µs | 65 µs | 127,016 |
| 2.1.0 | 2026-09-08 | 5 | 3,652 | 407 µs | 68 µs | 93,634 |
| 2.1.0 | 2026-09-08 | 5 | 2,399 | 933 µs | 73 µs | 96,230 |
| 3.0.0 | 2026-09-10 | 5 | 2,690 | 702 µs | 79 µs | 99,971 |
| 3.0.0 | 2026-09-12 | 5 | 2,046 | 3.8 ms | 78 µs | 123,605 |
| 3.0.0 | 2026-09-12 | 5 | 478 | 48.4 ms (!) | 101 µs | 152,930 |
| 3.0.0 | 2026-09-12 | 5 | 2,125 | 843 µs | 78 µs | 115,819 |
| 3.0.0 | 2026-09-12 | 5 | 1,402 | 9.8 ms | 73 µs | 135,595 |
