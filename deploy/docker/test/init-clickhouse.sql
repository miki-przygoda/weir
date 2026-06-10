-- Reference schema for the ClickHouse sink integration test.
--
-- A single `String` column holds the opaque weir payload bytes (the sink
-- inserts via RowBinary into this one column). `MergeTree` with a non-zero
-- `non_replicated_deduplication_window` makes the table deduplicate inserts
-- by the `insert_deduplication_token` weir sends, so a crash-replayed
-- byte-identical batch does not create duplicate rows. Production deployments
-- typically use a Replicated*MergeTree (where insert deduplication is on by
-- default) — see docs/operations/configuration.md.
CREATE TABLE IF NOT EXISTS default.weir_records
(
    payload String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;
