-- @sequential=true
-- @tags=mv,iceberg,ivm,storage_engine_iceberg,refresh_policy,scheduler
-- Test Objective:
-- 1. Validate Iceberg target MV refresh policy metadata is visible through SHOW MATERIALIZED VIEWS.
-- 2. Validate PAUSE/RESUME and ALTER SET REFRESH update user-facing scheduler state.

-- query 1
CREATE EXTERNAL CATALOG ice_ivm_policy_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_policy_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_policy_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_policy_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_policy_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_policy_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH ASYNC EVERY INTERVAL 5 MINUTE
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;

-- query 2
-- @result_contains=orders_policy_mv_
-- @result_contains=iceberg
-- @result_contains=ASYNC_INTERVAL
-- @result_contains=RefreshState
-- @result_contains=RetryAfterTime
-- @result_contains=PENDING
SHOW MATERIALIZED VIEWS;

-- query 3
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} PAUSE REFRESH;

-- query 4
-- @result_contains=orders_policy_mv_
-- @result_contains=ASYNC_INTERVAL
-- @result_contains=true
-- @result_contains=PAUSED
SHOW MATERIALIZED VIEWS;

-- query 5
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} SET REFRESH ASYNC ON CHANGE;
ALTER MATERIALIZED VIEW orders_policy_mv_${uuid0} RESUME REFRESH;

-- query 6
-- @result_contains=orders_policy_mv_
-- @result_contains=ASYNC_ON_CHANGE
-- @result_contains=false
-- @result_contains=PENDING
SHOW MATERIALIZED VIEWS;

-- query 7
DROP MATERIALIZED VIEW orders_policy_mv_${uuid0};
DROP TABLE ice_ivm_policy_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_policy_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_policy_${uuid0};
