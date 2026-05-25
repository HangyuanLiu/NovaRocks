-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,scheduler,manual
-- Test Objective:
-- Validate that REFRESH DEFERRED MANUAL is not auto-scheduled even when the
-- standalone MV refresh scheduler is enabled.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_mv_sched_manual_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_mv_sched_manual_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_mv_sched_manual_${uuid0}.ns_${uuid0};
CREATE TABLE ice_mv_sched_manual_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_mv_sched_manual_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_manual_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;
INSERT INTO ice_mv_sched_manual_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20);

-- query 2
-- @skip_result_check=true
shell: sleep 2

-- query 3
SELECT k1, v2 FROM orders_manual_mv_${uuid0} ORDER BY k1;

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_manual_mv_${uuid0};

-- query 5
SELECT k1, v2 FROM orders_manual_mv_${uuid0} ORDER BY k1;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_manual_mv_${uuid0};
DROP TABLE ice_mv_sched_manual_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_mv_sched_manual_${uuid0}.ns_${uuid0};
DROP CATALOG ice_mv_sched_manual_${uuid0};
