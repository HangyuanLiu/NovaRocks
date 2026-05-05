-- @sequential=true
-- @order_sensitive=true
-- @tags=managed_lake,mv,iceberg,ivm,projection_filter,row_lineage,delete,partition_evolution,read_semantics
-- Test Point:
--   Validate projection MV incremental refresh over Iceberg v3 row-lineage deletion-vector deletes after partition evolution.
-- Method:
--   Create a partitioned row-lineage table, evolve the partition spec, delete one old-spec row and one new-spec row, refresh MV, and verify both rows retract.
-- Scope:
--   Managed-lake projection/filter MV on an Iceberg v3 row-lineage base table with evolved partition specs.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_read_sem_part_v3_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_read_sem_part_v3_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_read_sem_part_v3_${uuid0}.ns_${uuid0};
CREATE TABLE mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
)
PARTITION BY (customer)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20);

-- query 2
-- @skip_result_check=true
ALTER TABLE mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders DROP PARTITION COLUMN customer;

-- query 3
-- @skip_result_check=true
INSERT INTO mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders VALUES
  (3, 'B', 30),
  (4, 'B', 40);

-- query 4
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.mv_read_sem_part_v3_orders
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, customer, amount
FROM mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.mv_read_sem_part_v3_orders;

-- query 6
SELECT id, customer, amount
FROM ${case_db}.mv_read_sem_part_v3_orders
ORDER BY id;

-- query 7
-- @skip_result_check=true
DELETE FROM mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders WHERE id IN (1, 3);

-- query 8
SELECT id, customer, amount
FROM mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 9
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.mv_read_sem_part_v3_orders;

-- query 10
SELECT id, customer, amount
FROM ${case_db}.mv_read_sem_part_v3_orders
ORDER BY id;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.mv_read_sem_part_v3_orders;
DROP TABLE mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_read_sem_part_v3_${uuid0}.ns_${uuid0};
DROP CATALOG mv_read_sem_part_v3_${uuid0};
