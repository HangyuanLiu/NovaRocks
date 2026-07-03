-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,lake_native,c1
-- Test Objective:
-- Validate that the active materialized-view suite runs on its suite-level
-- REST Iceberg catalog and covers the lake-native MV create/refresh/read/drop
-- path required by NIDL-C1. The StarRocks-compatible MV cases from the legacy
-- FE suite are parked under materialized-view/legacy/.

-- query 1
-- @skip_result_check=true
CREATE TABLE orders_base_${uuid0} (
  k1 INT NOT NULL,
  v2 BIGINT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO orders_base_${uuid0} VALUES
  (1, 10),
  (2, 20);
CREATE MATERIALIZED VIEW orders_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders_base_${uuid0};

-- query 2
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 4
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 5
-- @skip_result_check=true
INSERT INTO orders_base_${uuid0} VALUES (3, 30);

-- query 6
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 8
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv_${uuid0};
DROP TABLE orders_base_${uuid0} FORCE;
