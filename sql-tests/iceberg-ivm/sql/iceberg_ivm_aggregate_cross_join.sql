-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,cross_join,target_state
-- Test Point: aggregate over a direct CROSS JOIN refreshes incrementally.
-- Method: regions CROSS JOIN amounts, GROUP BY region. Initial REFRESH,
-- then INSERT into the right base and DELETE from the left base; cross-check MV == full recompute.
-- Scope: join-delta decomposition for zero-key cross joins under aggregate state merge.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_xjoin_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_xjoin_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_xjoin_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts (
  id BIGINT NOT NULL,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_xjoin_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW xjoin_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT r.region, COUNT(*) AS c, SUM(a.amount) AS s
FROM ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions AS r
CROSS JOIN ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts AS a
GROUP BY r.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions VALUES
  (1, 'east'),
  (2, 'west');
INSERT INTO ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts VALUES
  (10, 10),
  (20, 20);
REFRESH MATERIALIZED VIEW xjoin_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM xjoin_mv_${uuid0}
ORDER BY region;

-- query 4
SELECT r.region, COUNT(*) AS c, SUM(a.amount) AS s
FROM ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions AS r
CROSS JOIN ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts AS a
GROUP BY r.region
ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts VALUES
  (30, 5);

-- query 6
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW xjoin_mv_${uuid0};

-- query 7
SELECT region, c, s
FROM xjoin_mv_${uuid0}
ORDER BY region;

-- query 8
SELECT r.region, COUNT(*) AS c, SUM(a.amount) AS s
FROM ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions AS r
CROSS JOIN ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts AS a
GROUP BY r.region
ORDER BY region;

-- query 9
-- @skip_result_check=true
DELETE FROM ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions WHERE id = 2;

-- query 10
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW xjoin_mv_${uuid0};

-- query 11
SELECT region, c, s
FROM xjoin_mv_${uuid0}
ORDER BY region;

-- query 12
SELECT r.region, COUNT(*) AS c, SUM(a.amount) AS s
FROM ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions AS r
CROSS JOIN ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts AS a
GROUP BY r.region
ORDER BY region;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW xjoin_mv_${uuid0};
DROP TABLE ice_ivm_xjoin_${uuid0}.ns_${uuid0}.regions FORCE;
DROP TABLE ice_ivm_xjoin_${uuid0}.ns_${uuid0}.amounts FORCE;
DROP DATABASE ice_ivm_xjoin_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_xjoin_${uuid0};
