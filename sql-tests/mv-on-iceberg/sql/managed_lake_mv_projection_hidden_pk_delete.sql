-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,projection_filter,delete,hidden_pk
-- Test Point:
--   Validate projection/filter MV delete apply when the MV PRIMARY KEY is not
--   part of the user-visible SELECT output.
-- Method:
--   Create a primary-key projection MV over an Iceberg base table while hiding
--   the PK column from the MV output, refresh through position-delete and
--   equality-delete snapshots, and verify the visible MV rows are removed.
-- Scope:
--   Managed-lake projection/filter MV on an unpartitioned Iceberg v2 base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_hidden_pk_delete_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_hidden_pk_delete_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_hidden_pk_delete_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (id)
AS SELECT customer, amount
FROM mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_hidden_pk_delete_mv;

-- query 3
SELECT customer, amount
FROM ${case_db}.orders_hidden_pk_delete_mv
ORDER BY customer, amount;

-- query 4
-- @skip_result_check=true
DELETE FROM mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 1;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_hidden_pk_delete_mv;

-- query 6
SELECT customer, amount
FROM ${case_db}.orders_hidden_pk_delete_mv
ORDER BY customer, amount;

-- query 7
-- @skip_result_check=true
ALTER TABLE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (id) VALUES (2);

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_hidden_pk_delete_mv;

-- query 9
SELECT customer, amount
FROM ${case_db}.orders_hidden_pk_delete_mv
ORDER BY customer, amount;

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_hidden_pk_delete_mv;
DROP TABLE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_hidden_pk_delete_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_hidden_pk_delete_ice_${uuid0};
