-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,managed_lake,iceberg,optimize,row_lineage
-- Test Point:
--   Iceberg v3 ALTER TABLE OPTIMIZE compacts visible rows from data + DV files
--   into a fresh data file set and reports FINISHED.
-- Method:
--   Create a v3 row-lineage Iceberg table in a Hadoop catalog, INSERT rows in
--   two snapshots, DELETE rows to create deletion-vector state, run
--   ALTER TABLE OPTIMIZE, wait for it via wait_alter_optimize, and verify
--   visible rows are preserved before and after the rewrite.
-- Scope:
--   Standalone Iceberg v3 row-lineage OPTIMIZE end-to-end via SQL surface.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG opt_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/opt_ice_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE opt_ice_${uuid0}.ns_${uuid0};
CREATE TABLE opt_ice_${uuid0}.ns_${uuid0}.orders (
  id INT,
  user_id INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO opt_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10, 100), (2, 20, 200);
INSERT INTO opt_ice_${uuid0}.ns_${uuid0}.orders VALUES (3, 30, 300), (4, 40, 400);
DELETE FROM opt_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 2;
DELETE FROM opt_ice_${uuid0}.ns_${uuid0}.orders WHERE id = 4;

-- query 2
-- @db=opt_ice_${uuid0}.ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 3
-- @skip_result_check=true
-- @wait_alter_optimize=orders
-- @db=ns_${uuid0}
ALTER TABLE orders OPTIMIZE;

-- query 4
-- @db=ns_${uuid0}
SELECT id, user_id, amount FROM orders ORDER BY id;

-- query 5
-- @skip_result_check=true
DROP TABLE opt_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE opt_ice_${uuid0}.ns_${uuid0};
DROP CATALOG opt_ice_${uuid0};
