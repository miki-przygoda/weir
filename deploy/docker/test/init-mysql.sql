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
