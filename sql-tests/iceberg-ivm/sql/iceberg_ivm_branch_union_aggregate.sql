-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,union,aggregate,branch_union,target_state
-- Test Point: Iceberg UNION ALL of aggregate branches keeps same group keys
-- independent across branches.
-- Method: Build a UNION ALL MV whose two aggregate branches both output
-- region='k1'. Delete/insert in one branch and verify the other branch's
-- aggregate row is not merged or retracted.
-- Scope: RefreshStrategy::BranchUnionAggregate, BranchUtf8 apply key.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_bunion_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_bunion_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_bunion_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_bunion_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW branch_union_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 VALUES
  (1, 'k1', 10),
  (2, 'k2', 5);
INSERT INTO ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 VALUES
  (3, 'k1', 100),
  (4, 'k3', 7);
REFRESH MATERIALIZED VIEW branch_union_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM branch_union_mv_${uuid0}
ORDER BY region, s;

-- query 4
-- @skip_result_check=true
DELETE FROM ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 WHERE region = 'k1';

-- query 5
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW branch_union_mv_${uuid0};

-- query 6
SELECT region, c, s
FROM branch_union_mv_${uuid0}
ORDER BY region, s;

-- query 7
-- @skip_result_check=true
INSERT INTO ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 VALUES
  (5, 'k1', 50);

-- query 8
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW branch_union_mv_${uuid0};

-- query 9
SELECT region, c, s
FROM branch_union_mv_${uuid0}
ORDER BY region, s;

-- query 10
-- @expect_error=Column '__agg_state_c' cannot be resolved
SELECT __agg_state_c FROM branch_union_mv_${uuid0};

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW branch_union_mv_${uuid0};
DROP TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t1 FORCE;
DROP TABLE ice_ivm_bunion_${uuid0}.ns_${uuid0}.t2 FORCE;
DROP DATABASE ice_ivm_bunion_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_bunion_${uuid0};
