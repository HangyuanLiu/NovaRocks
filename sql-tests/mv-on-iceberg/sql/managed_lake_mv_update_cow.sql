-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,projection_filter,row_lineage,update,cow
-- Test Point:
--   Validate projection/filter MV incremental refresh after a copy-on-write
--   UPDATE on the v3 row-lineage Iceberg base table.
-- Method:
--   Create a primary-key projection MV over a v3 row-lineage Iceberg base
--   table (default copy-on-write update mode), perform an initial refresh,
--   UPDATE one base row's non-key column, refresh the MV, and verify the
--   updated value is reflected without row duplication.
-- Scope:
--   Managed-lake projection/filter MV on an unpartitioned Iceberg v3
--   row-lineage base table updated through copy-on-write semantics.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_update_cow_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_update_cow_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_update_cow_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_update_cow_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  status STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_update_cow_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'open', 10),
  (2, 'open', 20);
CREATE MATERIALIZED VIEW ${case_db}.orders_update_cow_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, amount
FROM mv_update_cow_ice_${uuid0}.ns_${uuid0}.orders
WHERE status = 'open';

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_update_cow_mv;

-- query 3
SELECT id, amount
FROM ${case_db}.orders_update_cow_mv
ORDER BY id;

-- query 4
-- @skip_result_check=true
UPDATE mv_update_cow_ice_${uuid0}.ns_${uuid0}.orders AS t
SET amount = 25
WHERE t.id = 2;

-- query 5
SELECT id, amount
FROM mv_update_cow_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_update_cow_mv;

-- query 7
SELECT id, amount
FROM ${case_db}.orders_update_cow_mv
ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_update_cow_mv;
DROP TABLE mv_update_cow_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_update_cow_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_update_cow_ice_${uuid0};
