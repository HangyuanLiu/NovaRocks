-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,statelessness
-- Test Objective:
-- 1. Validate standalone CREATE / INSERT / REFRESH over an Iceberg-target MV
--    (mirrors iceberg_backed_mv_basic_lifecycle.sql's setup).
-- 2. Dedicated coverage for the W4 lake-native statelessness `full` level:
--    `@imv_stateless_rebuild=...,level=full` drives the server to clear the
--    MV's SQLite definition and rebuild it purely from the lake (Iceberg MV
--    table descriptor properties), then
--    re-runs the same SELECT and asserts it is unchanged.
-- 3. The directive itself emits no output; the golden below is the SELECT's
--    own result, which must be identical before and after the clear+rebuild
--    round-trip the directive performs server-side.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_ice_stateless_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_ice_stateless_${uuid0}.ns_${uuid0};
CREATE TABLE mv_ice_stateless_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_ice_stateless_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (2, 20),
  (3, 50);
SET CATALOG mv_ice_stateless_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv;

-- query 3
-- @imv_stateless_rebuild=orders_mv,catalog=mv_ice_stateless_${uuid0},level=full
SELECT k1, v2 FROM orders_mv ORDER BY k1;

-- query 4
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE mv_ice_stateless_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_ice_stateless_${uuid0}.ns_${uuid0};
DROP CATALOG mv_ice_stateless_${uuid0};
