CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};

SELECT 1 AS scalar_value;

CREATE TABLE left_rows (
  k INT NOT NULL,
  v INT NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

CREATE TABLE right_rows (
  k INT NOT NULL,
  label VARCHAR(16) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- @be_log_contains=compat_ingress method=exec_plan_fragment
INSERT INTO left_rows VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO right_rows VALUES (1, 'one'), (3, 'three');

SELECT l.k, l.v, r.label
FROM left_rows l JOIN [BROADCAST] right_rows r ON l.k = r.k
ORDER BY l.k;
