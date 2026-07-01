-- @sequential=true
-- @order_sensitive=true
-- @tags=optimizer,iceberg,imv,projection_filter,logical_cutover
-- Test Objective:
-- Plan-shape golden for projection/filter IMV incremental refresh. This locks
-- the default EXPLAIN REFRESH output and the VERBOSE rendering for the same
-- refresh rewrite pipeline.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG imv_pf_cut_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/imv_pf_cut_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE imv_pf_cut_${uuid0}.ns_${uuid0};
CREATE TABLE imv_pf_cut_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO imv_pf_cut_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10), (1, 20), (2, 40), (3, 5);
SET CATALOG imv_pf_cut_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pf_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT k1, v2 FROM orders WHERE v2 > 0;

-- query 2
-- Build the real previous snapshot required by incremental refresh planning.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW pf_mv_${uuid0};

-- query 3
-- Delta with both a delete and an insert before inspecting the refresh-time plan.
-- @skip_result_check=true
DELETE FROM imv_pf_cut_${uuid0}.ns_${uuid0}.orders WHERE k1 = 2;
INSERT INTO imv_pf_cut_${uuid0}.ns_${uuid0}.orders VALUES (4, 7);

-- query 4
-- Default EXPLAIN REFRESH should print the actual refresh logical plan.
-- @skip_result_check=true
-- @result_contains=LEFT OUTER JOIN
-- @result_contains=predicate: v2 > 0
-- @result_contains=__nova_base_row_id
EXPLAIN REFRESH MATERIALIZED VIEW pf_mv_${uuid0};

-- query 5
-- VERBOSE should include the same refresh plan shape. EXPLAIN VERBOSE REFRESH
-- currently renders the refresh plan without per-node stats.
-- @skip_result_check=true
-- @result_contains=LEFT OUTER JOIN
-- @result_contains=predicate: v2 > 0
-- @result_contains=__nova_base_row_id
-- @result_not_contains=stats={rows=
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW pf_mv_${uuid0};

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW pf_mv_${uuid0};
DROP TABLE imv_pf_cut_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE imv_pf_cut_${uuid0}.ns_${uuid0};
DROP CATALOG imv_pf_cut_${uuid0};
