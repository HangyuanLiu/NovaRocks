-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,scheduler,pause_resume
-- Test Objective:
-- Validate that PAUSE REFRESH blocks scheduler work and RESUME REFRESH allows
-- the scheduler to refresh an Iceberg target MV again.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_sched_pause_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_mv_sched_pause_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_mv_sched_pause_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_sched_pause_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_mv_sched_pause_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_pause_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH ASYNC EVERY INTERVAL 1 SECOND
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;
INSERT INTO ice_mv_sched_pause_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10);

-- query 2
-- @retry_count=30
-- @retry_interval_ms=500
SELECT k1, v2 FROM orders_pause_mv_${uuid0} ORDER BY k1;

-- query 3
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_pause_mv_${uuid0} PAUSE REFRESH;
INSERT INTO ice_mv_sched_pause_${uuid0}.ns_${uuid0}.orders VALUES
  (2, 20);

-- query 4
-- @skip_result_check=true
shell: sleep 2

-- query 5
SELECT k1, v2 FROM orders_pause_mv_${uuid0} ORDER BY k1;

-- query 6
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_pause_mv_${uuid0} RESUME REFRESH;

-- query 7
-- @retry_count=30
-- @retry_interval_ms=500
SELECT k1, v2 FROM orders_pause_mv_${uuid0} ORDER BY k1;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_pause_mv_${uuid0};
DROP TABLE ice_mv_sched_pause_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_mv_sched_pause_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_sched_pause_${uuid0};
