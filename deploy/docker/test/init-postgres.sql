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

-- Second table, keyed on the record's WAB coordinate rather than its bytes.
-- Pair with `sink_postgres_id_column = "record_id"`.
--
-- The table above is the pre-2.2 reference schema and is kept deliberately: it
-- keys idempotency on CONTENT, so `ON CONFLICT DO NOTHING` silently discards a
-- genuinely distinct record that happens to share bytes with one already
-- stored. `sql_sink_content_keyed_schema_loses_a_distinct_duplicate_record`
-- pushes byte-identical records at both tables and asserts the difference, so
-- the defect is demonstrated rather than described.
CREATE TABLE weir_records_keyed (
    id BIGSERIAL PRIMARY KEY,
    record_id CHAR(64) NOT NULL,
    payload BYTEA NOT NULL,
    UNIQUE (record_id)
);
