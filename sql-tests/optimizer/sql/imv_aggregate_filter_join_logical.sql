-- @sequential=true
-- @order_sensitive=true
-- @tags=optimizer,iceberg,imv,aggregate,join,filter,logical_cutover
-- Test Objective:
-- Validate optimizer-visible plan evidence for aggregate IMV incremental
-- refresh over a filtered join after the refresh path has cut over to IMV
-- rewrite execution. The case builds real previous snapshots before appending
-- filtered fact rows so refresh planning must keep the filter and join-delta
-- version scan shape.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG imv_afj_cut_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_afj_cut_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE imv_afj_cut_${uuid0}.ns_${uuid0};
CREATE TABLE imv_afj_cut_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE imv_afj_cut_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO imv_afj_cut_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west');
INSERT INTO imv_afj_cut_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, -7),
  (3, 20, 50);
SET CATALOG imv_afj_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_filter_join_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
WHERE f.amount > 0
GROUP BY d.region;

-- query 2
-- Build the real previous snapshots required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_filter_join_mv_${uuid0};

-- query 3
-- Append both positive and negative fact rows so the refresh plan must keep
-- the filter path while using IMV join-delta version scans.
-- @skip_result_check=true
INSERT INTO imv_afj_cut_${uuid0}.ns_${uuid0}.fact VALUES
  (4, 20, 80),
  (5, 10, -9);

-- query 4
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=FILTER
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergVersionTable
-- @explain_contains=IcebergMvTargetState
EXPLAIN REFRESH MATERIALIZED VIEW agg_filter_join_mv_${uuid0};

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_filter_join_mv_${uuid0};
DROP TABLE imv_afj_cut_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE imv_afj_cut_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE imv_afj_cut_${uuid0}.ns_${uuid0};
DROP CATALOG imv_afj_cut_${uuid0};
