
## Per-stage latency breakdown (bench-trace)

> Mean µs per stage, averaged over all runs. Captured with `--features bench-trace`.
> queue = enqueue→worker-flush; bridge_wait = worker-flush→flusher-dequeue;
> write = flusher-dequeue→write_record (pre-fsync); total = enqueue→ack-fired.

### deadline = d1ms

| Stage | stage_sync_d1ms | stage_batched_d1ms | stage_buffered_d1ms |
|-------|-------- | -------- | --------|
| Queue (µs) | 2 | 2 | 3 |
| Bridge wait (µs) | 4 | 4 | 7 |
| Write (µs) | 12 | 11 | 8 |
| Total (µs) | 166 | 152 | 18 |

### deadline = d2ms

| Stage | stage_sync_d2ms | stage_batched_d2ms | stage_buffered_d2ms |
|-------|-------- | -------- | --------|
| Queue (µs) | 2 | 2 | 3 |
| Bridge wait (µs) | 4 | 4 | 8 |
| Write (µs) | 9 | 9 | 6 |
| Total (µs) | 158 | 145 | 18 |
