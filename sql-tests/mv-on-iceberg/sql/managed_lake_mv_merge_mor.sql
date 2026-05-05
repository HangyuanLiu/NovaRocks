-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,projection_filter,row_lineage,merge,mor
-- Test Point:
--   Validate projection/filter MV incremental refresh after a MERGE INTO
--   on a v3 row-lineage Iceberg base table whose update mode is
--   merge-on-read. The MERGE produces a MOR UPDATE snapshot
--   (Operation::Delete with the NovaRocks update marker) followed by a
--   FastAppend INSERT snapshot; the MV planner must walk both.
-- Method:
--   Create a primary-key projection MV, refresh once, MERGE updates +
--   inserts, refresh, and verify the MV reflects the matched update plus
--   the new row exactly once.
-- Scope:
--   Managed-lake projection/filter MV on an unpartitioned Iceberg v3
--   row-lineage base table updated via MERGE INTO with merge-on-read
--   update mode.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_merge_mor_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/mv_merge_mor_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_merge_mor_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_merge_mor_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  status STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO mv_merge_mor_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'open', 10),
  (2, 'open', 20);
CREATE TABLE mv_merge_mor_ice_${uuid0}.ns_${uuid0}.staging (
  id BIGINT NOT NULL,
  status STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_merge_mor_ice_${uuid0}.ns_${uuid0}.staging VALUES
  (2, 'open', 25),
  (3, 'open', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_merge_mor_mv
DISTRIBUTED BY HASH(id) BUCKETS 2
PRIMARY KEY (id)
AS SELECT id, amount
FROM mv_merge_mor_ice_${uuid0}.ns_${uuid0}.orders
WHERE status = 'open';

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_merge_mor_mv;

-- query 3
SELECT id, amount
FROM ${case_db}.orders_merge_mor_mv
ORDER BY id;

-- query 4
-- @skip_result_check=true
MERGE INTO mv_merge_mor_ice_${uuid0}.ns_${uuid0}.orders AS t
USING mv_merge_mor_ice_${uuid0}.ns_${uuid0}.staging AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT (id, status, amount) VALUES (s.id, s.status, s.amount);

-- query 5
SELECT id, amount
FROM mv_merge_mor_ice_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_merge_mor_mv;

-- query 7
SELECT id, amount
FROM ${case_db}.orders_merge_mor_mv
ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_merge_mor_mv;
DROP TABLE mv_merge_mor_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP TABLE mv_merge_mor_ice_${uuid0}.ns_${uuid0}.staging FORCE;
DROP DATABASE mv_merge_mor_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_merge_mor_ice_${uuid0};
