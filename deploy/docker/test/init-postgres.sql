-- Schema seeded into the test Postgres container by docker-compose.
-- Matches the reference schema in docs/operations/configuration.md
-- (the Postgres sink section). Pair with
-- `sink_postgres_insert_mode = "on_conflict_do_nothing"` (the default)
-- for idempotent crash-recovery retries — the UNIQUE constraint on
-- payload_sha256 is what `ON CONFLICT DO NOTHING` keys against.

CREATE TABLE weir_records (
    id BIGSERIAL PRIMARY KEY,
    payload BYTEA NOT NULL,
    -- Generated column so the constraint catches duplicates without
    -- requiring the producer to compute the hash itself. SHA-256 is
    -- collision-safe at the scale of any plausible weir deployment.
    payload_sha256 BYTEA GENERATED ALWAYS AS (sha256(payload)) STORED,
    UNIQUE (payload_sha256)
);
