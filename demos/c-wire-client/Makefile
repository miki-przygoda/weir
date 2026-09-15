# weir C wire client — stdlib + POSIX only, no external deps.
CC      ?= cc
CFLAGS  ?= -std=c11 -O2 -Wall -Wextra -Wpedantic -Wconversion -Wshadow
LIB_SRC  = weir_wire.c weir_conn.c

all: conformance conformance_tracked conformance_batch telemetry negative

conformance: conformance.c weir_wire.c weir_wire.h
	$(CC) $(CFLAGS) -o $@ conformance.c weir_wire.c

# A second binary, not a branch in conformance.c: that scanner is sized and
# shaped for the frozen file (its array locator, its 1024-byte object buffer and
# its 256-byte hex buffer all assume it). Keeping them separate means the frozen
# set's green light cannot be disturbed by tracked-file churn.
conformance_tracked: conformance_tracked.c weir_wire.c weir_wire.h
	$(CC) $(CFLAGS) -o $@ conformance_tracked.c weir_wire.c

# A third binary, for the reason there is a second: each scanner is sized and
# shaped for one file. This one's arrays are "frame_vectors"/"body_vectors"/
# "ack_vectors" and its largest object is 1626 bytes.
conformance_batch: conformance_batch.c weir_wire.c weir_wire.h
	$(CC) $(CFLAGS) -o $@ conformance_batch.c weir_wire.c

telemetry: telemetry.c $(LIB_SRC) weir_wire.h weir_conn.h
	$(CC) $(CFLAGS) -o $@ telemetry.c $(LIB_SRC)

negative: negative.c $(LIB_SRC) weir_wire.h weir_conn.h
	$(CC) $(CFLAGS) -o $@ negative.c $(LIB_SRC)

# Run the conformance suite against the canonical vectors (relative to the repo;
# override with `make check VECTORS=/path/to/wire_v1_vectors.json`).
VECTORS ?= ../../docs/conformance/wire_v1_vectors.json
check: conformance
	./conformance "$(VECTORS)"

TRACKED_VECTORS ?= ../../docs/conformance/wire_v1_tracked_vectors.json
check-tracked: conformance_tracked
	./conformance_tracked "$(TRACKED_VECTORS)"

BATCH_VECTORS ?= ../../docs/conformance/wire_v1_batch_vectors.json
check-batch: conformance_batch
	./conformance_batch "$(BATCH_VECTORS)"

clean:
	rm -f conformance conformance_tracked conformance_batch telemetry negative

.PHONY: all check check-tracked check-batch clean
