CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE connector_adapter_rows (
  k INT NOT NULL,
  v VARCHAR(32) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- This is planned as a schema-scan fragment.  The FE rows are fetched through
-- novarocks-compat, while core only receives SchemaRow domain values.
SELECT COUNT(*) AS matching_tables
FROM information_schema.tables
WHERE table_schema = '${case_db}'
  AND table_name = 'connector_adapter_rows';
