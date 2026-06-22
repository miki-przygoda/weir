# weir C wire client — stdlib + POSIX only, no external deps.
CC      ?= cc
CFLAGS  ?= -std=c11 -O2 -Wall -Wextra -Wpedantic -Wconversion -Wshadow
LIB_SRC  = weir_wire.c weir_conn.c

all: conformance telemetry negative

conformance: conformance.c weir_wire.c weir_wire.h
	$(CC) $(CFLAGS) -o $@ conformance.c weir_wire.c

telemetry: telemetry.c $(LIB_SRC) weir_wire.h weir_conn.h
	$(CC) $(CFLAGS) -o $@ telemetry.c $(LIB_SRC)

negative: negative.c $(LIB_SRC) weir_wire.h weir_conn.h
	$(CC) $(CFLAGS) -o $@ negative.c $(LIB_SRC)

# Run the conformance suite against the canonical vectors (relative to the repo;
# override with `make check VECTORS=/path/to/wire_v1_vectors.json`).
VECTORS ?= ../../docs/conformance/wire_v1_vectors.json
check: conformance
	./conformance "$(VECTORS)"

clean:
	rm -f conformance telemetry negative

.PHONY: all check clean
