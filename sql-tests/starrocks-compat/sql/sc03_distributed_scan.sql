CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE scan_rows (
  k INT NOT NULL,
  v INT NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO scan_rows VALUES
  (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60);

-- @be_log_count_at_least=compat_scan node_type=LAKE_SCAN_NODE,2
-- @be_log_be_count_at_least=compat_scan node_type=LAKE_SCAN_NODE,2
SELECT COUNT(*) AS row_count, SUM(v) AS value_sum
FROM scan_rows;
