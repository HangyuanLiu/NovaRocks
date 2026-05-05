-- @sequential=true
-- @order_sensitive=true
-- @tags=managed_lake,mv,iceberg,ivm,equality_delete,schema_evolution,read_semantics
-- Test Point:
--   Validate projection MV incremental refresh when an Iceberg equality delete targets a widened base column.
-- Method:
--   Create a primary-key projection MV, widen the equality column, add an equality delete, refresh MV, and verify row retraction.
-- Scope:
--   Managed-lake projection/filter MV over an Iceberg base table.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_read_sem_eq_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_read_sem_eq_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_read_sem_eq_${uuid0}.ns_${uuid0};
CREATE TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  amount FLOAT,
  customer STRING
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10.5, 'A'),
  (2, 20.5, 'B'),
  (3, 30.5, 'C');
CREATE MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, amount, customer
FROM mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10.0;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders;

-- query 3
SELECT id, amount, customer
FROM ${case_db}.mv_read_sem_eq_orders
ORDER BY id;

-- query 4
-- @skip_result_check=true
ALTER TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders MODIFY COLUMN amount DOUBLE;
ALTER TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders
ADD EQUALITY DELETE (amount) VALUES (20.5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders;

-- query 6
SELECT id, amount, customer
FROM ${case_db}.mv_read_sem_eq_orders
ORDER BY id;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.mv_read_sem_eq_orders;
DROP TABLE mv_read_sem_eq_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_read_sem_eq_${uuid0}.ns_${uuid0};
DROP CATALOG mv_read_sem_eq_${uuid0};
