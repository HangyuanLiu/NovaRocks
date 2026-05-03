-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,strategy
-- Test Objective:
-- 1. Validate aggregate MV full refresh over a v3 row-lineage Iceberg base table.
-- 2. Validate append snapshots refresh incrementally.
-- 3. Validate INSERT OVERWRITE refresh falls back to full refresh and advances metadata.
-- 4. Validate S3-backed Iceberg DELETE mutates visible base rows and MV refresh state.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_strategy_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_strategy_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_strategy_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_strategy_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
AS SELECT
  customer,
  COUNT(*) AS c,
  SUM(amount) AS s
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 3
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_strategy_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (4, 'A', 100);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 6
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 7
-- @skip_result_check=true
INSERT OVERWRITE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
SELECT id, customer, amount + 100
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
WHERE id >= 2;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 9
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 10
-- @skip_result_check=true
DELETE FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 2;

-- query 11
SELECT id, customer, amount
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 12
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 13
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 14
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_strategy_mv;
DROP TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_strategy_ice_${uuid0};
