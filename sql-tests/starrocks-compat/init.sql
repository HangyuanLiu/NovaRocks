CREATE DATABASE IF NOT EXISTS starrocks_compat_suite_setup;
USE starrocks_compat_suite_setup;

CREATE TABLE IF NOT EXISTS load_ingress_rows (
  k INT NOT NULL,
  source VARCHAR(32) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");
