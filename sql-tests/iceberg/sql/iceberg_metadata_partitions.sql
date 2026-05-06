-- @order_sensitive=true
-- Validate SELECT * FROM <tbl>$partitions surfaces per-partition row counts.
-- Tests Phase A (metadata table SQL routing) — partitions flavour.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0} (id INT, region STRING)
PARTITION BY (region)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0} VALUES
  (1, 'us'), (2, 'us'), (3, 'eu');

-- query 4
-- 2 partitions: 'us' and 'eu'.
SELECT count(*) AS n_partitions
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0}$partitions;

-- query 5
-- Total rows across all partitions = 3.
SELECT sum(record_count) AS total_records
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0}$partitions;

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0};

-- query 7
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
