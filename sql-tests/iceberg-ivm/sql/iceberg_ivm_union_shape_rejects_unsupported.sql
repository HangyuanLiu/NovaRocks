-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,union,negative,shape_validation
-- Test Point: Iceberg IMV UNION shape validation rejects unsupported
-- neighboring shapes at CREATE time.
-- Scope: UNION DISTINCT, mixed projection/aggregate branches, incompatible
-- aggregate branches, duplicate base refs, and reserved branch-id output names.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_union_reject_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_union_reject_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_union_reject_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2 (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_union_reject_${uuid0};
USE ns_${uuid0};

-- query 2
-- @expect_error=Iceberg IMV refresh contract only supports UNION ALL set operations
CREATE MATERIALIZED VIEW union_distinct_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT id, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
UNION
SELECT id, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2;

-- query 3
-- @expect_error=Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches
CREATE MATERIALIZED VIEW union_mixed_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, amount
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
UNION ALL
SELECT region, SUM(amount) AS amount
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2
GROUP BY region;

-- query 4
-- @expect_error=Iceberg IMV refresh contract requires compatible aggregate branch contracts
CREATE MATERIALIZED VIEW union_incompatible_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
GROUP BY region
UNION ALL
SELECT region, amount, COUNT(*) AS c
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2
GROUP BY region, amount;

-- query 5
-- @expect_error=requires 2 distinct Iceberg base table refs
CREATE MATERIALIZED VIEW union_duplicate_base_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
GROUP BY region;

-- query 6
-- @expect_error=reserved
CREATE MATERIALIZED VIEW union_reserved_branch_id_mv_${uuid0}
DISTRIBUTED BY HASH(__branch_id__) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT id AS __branch_id__, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1
UNION ALL
SELECT id AS __branch_id__, region
FROM ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2;

-- query 7
-- @skip_result_check=true
DROP TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t1 FORCE;
DROP TABLE ice_ivm_union_reject_${uuid0}.ns_${uuid0}.t2 FORCE;
DROP DATABASE ice_ivm_union_reject_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_union_reject_${uuid0};
