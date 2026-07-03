-- @order_sensitive=true
-- @tags=iceberg,runtime_filter
-- Validate that completed join runtime filters prune Iceberg data files before
-- HDFS scan morsel execution, while preserving join results.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.build_${uuid0} (
  k1 INT
);

-- query 3
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0} (
  k1 INT,
  payload STRING
);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.build_${uuid0} VALUES
  (100),
  (101);

-- query 5
-- Keep this cold range in its own Iceberg data file so runtime file pruning has
-- a whole-file miss to remove.
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0} VALUES
  (1, 'cold-1'),
  (2, 'cold-2');

-- query 6
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0} VALUES
  (100, 'hot-100'),
  (101, 'hot-101');

-- query 7
-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.build_${uuid0};

-- query 8
-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0};

-- query 9
-- @skip_result_check=true
SET global_runtime_filter_build_max_size = 10737418240;

-- query 10
-- @skip_result_check=true
SET global_runtime_filter_probe_min_selectivity = 0.0;

-- query 11
-- @skip_result_check=true
SET runtime_filter_scan_wait_time = 10000;

-- query 12
-- @skip_result_check=true
SET global_runtime_filter_wait_timeout = 10000;

-- query 13
SELECT p.k1, p.payload
FROM iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0} p
JOIN iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.build_${uuid0} b
  ON p.k1 = b.k1
ORDER BY p.k1;

-- query 14
-- @skip_result_check=true
-- @result_contains=Profile: fragments=
-- @result_contains=ProfileCounters:
-- @result_contains=HASH JOIN
-- @result_contains=IcebergRuntimeFilePruning/FilesTotal=2
-- @result_contains=IcebergRuntimeFilePruning/FilesSelected=1
-- @result_contains=IcebergRuntimeFilePruning/FilesPruned=1
-- @result_contains=IcebergRuntimeFilePruning/Predicates
-- @result_contains=IcebergRuntimeFilePruning/Unavailable=0
EXPLAIN ANALYZE
SELECT p.k1, p.payload
FROM iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0} p
JOIN iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.build_${uuid0} b
  ON p.k1 = b.k1
ORDER BY p.k1;

-- query 15
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.probe_${uuid0} FORCE;

-- query 16
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0}.build_${uuid0} FORCE;

-- query 17
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.runtime_prune_db_${uuid0};
