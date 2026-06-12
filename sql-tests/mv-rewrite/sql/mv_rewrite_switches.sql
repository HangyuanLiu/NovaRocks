-- @sequential=true
-- @order_sensitive=true
-- @tags=optimizer,mv,rewrite,iceberg
-- Test Objective: the rewrite is gated by two independent session switches.
-- 1. enable_materialized_view_rewrite = off  -> no rewrite; on -> hit.
-- 2. disable_optimizer_rules = 'MvRewrite'    -> no rewrite; cleared -> hit.
--
-- Data is scaled like the hit case so the rewrite is cost-favourable when the
-- switches allow it (see mv_rewrite_hit_basic.sql for the rationale).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrw_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mvrw_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvrw_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region STRING,
  day STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrw_${uuid0}.ns_${uuid0}.orders
SELECT
  number AS id,
  CASE WHEN number % 3 = 0 THEN 'east' WHEN number % 3 = 1 THEN 'west' ELSE 'north' END AS region,
  CASE WHEN number % 2 = 0 THEN 'd1' ELSE 'd2' END AS day,
  CAST(number % 10 AS BIGINT) AS amount
FROM TABLE(generate_series(1, 1200)) t(number);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrw_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW agg_mv
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, day, COUNT(*) AS c, SUM(amount) AS s
FROM orders GROUP BY region, day;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv WITH SYNC MODE;

-- query 9
-- @skip_result_check=true
SET enable_materialized_view_rewrite = off;

-- query 10
-- switch off -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT region, SUM(amount) FROM orders GROUP BY region;

-- query 11
-- @skip_result_check=true
SET enable_materialized_view_rewrite = on;

-- query 12
-- switch on -> hit
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: agg_mv
SELECT region, SUM(amount) FROM orders GROUP BY region;

-- query 13
-- @skip_result_check=true
SET disable_optimizer_rules = 'MvRewrite';

-- query 14
-- rule disabled -> no rewrite
-- @skip_result_check=true
-- @explain_not_contains=rewritten with mv
SELECT region, SUM(amount) FROM orders GROUP BY region;

-- query 15
-- @skip_result_check=true
SET disable_optimizer_rules = '';

-- query 16
-- rule re-enabled -> hit
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: agg_mv
SELECT region, SUM(amount) FROM orders GROUP BY region;

-- query 17
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv;

-- query 18
-- @skip_result_check=true
DROP TABLE mvrw_${uuid0}.ns_${uuid0}.orders FORCE;

-- query 19
-- @skip_result_check=true
DROP DATABASE mvrw_${uuid0}.ns_${uuid0};

-- query 20
-- @skip_result_check=true
DROP CATALOG mvrw_${uuid0};
