CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE protocol_owner_rollup_rows (
  k INT NOT NULL,
  v INT NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO protocol_owner_rollup_rows VALUES (1, 10), (2, 20), (3, 30);

-- @skip_result_check=true
-- @wait_alter_rollup=protocol_owner_rollup_rows
ALTER TABLE protocol_owner_rollup_rows ADD ROLLUP protocol_owner_k_v (k, v);

SELECT k, v
FROM protocol_owner_rollup_rows
ORDER BY k;
