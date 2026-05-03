-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,equality_delete
-- Test Point:
--   Validate aggregate MV incremental refresh over an Iceberg equality-delete snapshot.
-- Method:
--   Write an equality-delete file through ALTER TABLE ADD EQUALITY DELETE, verify base-table
--   visibility, then refresh the aggregate MV and verify COUNT/SUM retraction.
-- Scope:
--   Managed-lake aggregate MV on an unpartitioned Iceberg v2 base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_eq_delete_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_eq_delete_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_eq_delete_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_eq_delete_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO mv_eq_delete_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_equality_delete_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
AS SELECT
  customer,
  COUNT(*) AS c,
  SUM(amount) AS s
FROM mv_eq_delete_ice_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_equality_delete_mv;

-- query 3
SELECT customer, c, s
FROM ${case_db}.orders_equality_delete_mv
ORDER BY customer;

-- query 4
-- @skip_result_check=true
ALTER TABLE mv_eq_delete_ice_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (id) VALUES (1);

-- query 5
SELECT id, customer, amount
FROM mv_eq_delete_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_equality_delete_mv;

-- query 7
SELECT customer, c, s
FROM ${case_db}.orders_equality_delete_mv
ORDER BY customer;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_equality_delete_mv;
DROP TABLE mv_eq_delete_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_eq_delete_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_eq_delete_ice_${uuid0};
