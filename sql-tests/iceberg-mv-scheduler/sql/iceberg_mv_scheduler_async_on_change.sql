-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,scheduler,async_on_change
-- Test Objective:
-- Validate that REFRESH ASYNC ON CHANGE refreshes an Iceberg target MV after a
-- base table snapshot changes.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_sched_change_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_mv_sched_change_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_mv_sched_change_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_sched_change_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_mv_sched_change_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20);
SET CATALOG ice_mv_sched_change_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_change_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH ASYNC ON CHANGE
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;

-- query 2
-- Let the scheduler observe the baseline snapshot before creating a new one.
-- @skip_result_check=true
shell: sleep 2

-- query 3
-- @skip_result_check=true
INSERT INTO ice_mv_sched_change_${uuid0}.ns_${uuid0}.orders VALUES
  (3, 30);

-- query 4
-- @retry_count=30
-- @retry_interval_ms=500
SELECT k1, v2 FROM orders_change_mv_${uuid0} ORDER BY k1;

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_change_mv_${uuid0};
DROP TABLE ice_mv_sched_change_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_mv_sched_change_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_sched_change_${uuid0};
