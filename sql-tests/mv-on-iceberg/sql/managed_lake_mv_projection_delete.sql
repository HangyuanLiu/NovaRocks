-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,projection_filter,delete
-- Test Point:
--   Validate projection/filter MV incremental refresh over an Iceberg delete snapshot.
-- Method:
--   Create a primary-key projection MV over an Iceberg base table, apply both
--   position-delete and equality-delete snapshots, refresh the MV, and verify
--   deleted rows are removed from the MV.
-- Scope:
--   Managed-lake projection/filter MV on an unpartitioned Iceberg v2 base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_proj_delete_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_proj_delete_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_proj_delete_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_projection_delete_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, customer, amount
FROM mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_projection_delete_mv;

-- query 3
SELECT id, customer, amount
FROM ${case_db}.orders_projection_delete_mv
ORDER BY id;

-- query 4
-- @skip_result_check=true
DELETE FROM mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 1;

-- query 5
SELECT id, customer, amount
FROM mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_projection_delete_mv;

-- query 7
SELECT id, customer, amount
FROM ${case_db}.orders_projection_delete_mv
ORDER BY id;

-- query 8
-- @skip_result_check=true
ALTER TABLE mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (id) VALUES (2);

-- query 9
SELECT id, customer, amount
FROM mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 10
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_projection_delete_mv;

-- query 11
SELECT id, customer, amount
FROM ${case_db}.orders_projection_delete_mv
ORDER BY id;

-- query 12
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_projection_delete_mv;
DROP TABLE mv_proj_delete_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_proj_delete_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_proj_delete_ice_${uuid0};
