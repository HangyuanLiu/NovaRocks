-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,strategy
-- Test Objective:
-- 1. Validate aggregate MV full refresh over a v3 row-lineage Iceberg base table.
-- 2. Validate append snapshots refresh incrementally.
-- 3. Validate INSERT OVERWRITE refresh falls back to full refresh and advances metadata.

-- query 1
-- @skip_result_check=true
-- Use a local-FS Iceberg warehouse here because S3-backed Iceberg
-- INSERT OVERWRITE abort cleanup is not wired through the standalone SQL path yet.
CREATE EXTERNAL CATALOG mv_strategy_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "file:///tmp/novarocks-mv-strategy-${uuid0}"
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
DROP MATERIALIZED VIEW ${case_db}.orders_strategy_mv;
DROP TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_strategy_ice_${uuid0};
