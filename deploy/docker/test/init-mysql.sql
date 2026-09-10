-- Schema seeded into the test MySQL container by docker-compose.
-- Matches the reference schema in docs/operations/configuration.md
-- (the MySQL sink section). Pair with `sink_mysql_insert_mode = "ignore"`
-- (the default) for idempotent crash-recovery retries.

CREATE TABLE weir_records (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    payload VARBINARY(4096) NOT NULL,
    -- Prefix index over `payload` so `INSERT IGNORE` can drop dupes
    -- without scanning every row. 255 = MySQL's default unique-index
    -- key-prefix limit on VARBINARY in InnoDB.
    UNIQUE KEY uniq_payload (payload(255))
);

-- Second table, keyed on the record's WAB coordinate rather than its bytes.
-- Pair with `sink_mysql_id_column = "record_id"`.
--
-- The table above is the pre-2.2 reference schema and is kept deliberately: it
-- keys idempotency on CONTENT, so `INSERT IGNORE` silently discards a genuinely
-- distinct record that happens to share bytes with one already stored.
-- `sql_sink_content_keyed_schema_loses_a_distinct_duplicate_record` pushes
-- byte-identical records at both tables and asserts the difference, so the
-- defect is demonstrated rather than described.
CREATE TABLE weir_records_keyed (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  record_id CHAR(64) NOT NULL,
  payload VARBINARY(4096) NOT NULL,
  UNIQUE KEY uq_record_id (record_id)
);
