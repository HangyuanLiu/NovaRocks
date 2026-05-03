-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,projection_filter,row_lineage,delete
-- Test Point:
--   Validate projection/filter MV incremental refresh over an Iceberg v3
--   row-lineage deletion-vector snapshot.
-- Method:
--   Create a primary-key projection MV over a v3 row-lineage Iceberg base table,
--   delete one base row, refresh the MV, and verify the deleted row is removed.
-- Scope:
--   Managed-lake projection/filter MV on an unpartitioned Iceberg v3 row-lineage base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_proj_v3_delete_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_proj_v3_delete_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_projection_v3_delete_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, customer, amount
FROM mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_projection_v3_delete_mv;

-- query 3
SELECT id, customer, amount
FROM ${case_db}.orders_projection_v3_delete_mv
ORDER BY id;

-- query 4
-- @skip_result_check=true
DELETE FROM mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 1;

-- query 5
SELECT id, customer, amount
FROM mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_projection_v3_delete_mv;

-- query 7
SELECT id, customer, amount
FROM ${case_db}.orders_projection_v3_delete_mv
ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_projection_v3_delete_mv;
DROP TABLE mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_proj_v3_delete_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_proj_v3_delete_ice_${uuid0};
