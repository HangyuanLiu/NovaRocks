CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

CREATE TABLE fact (
  k INT NOT NULL,
  payload VARCHAR(64) NOT NULL,
  padding VARCHAR(128) NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO fact VALUES
  (1, 'a', 'padding-a'), (2, 'b', 'padding-b'), (3, 'c', 'padding-c');

SET enable_global_late_materialization = true;
SET enable_global_late_materialization_cost_based = false;

-- @explain_contains=FETCH
-- @be_log_contains=compat_rpc method=lookup
-- @be_log_be_count_at_least=compat_rpc method=lookup_close direction=receive status=ok,3
SELECT payload FROM fact WHERE k >= 1 ORDER BY k LIMIT 2;
