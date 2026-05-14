-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,base_rename,projection,filter
-- Test Objective:
-- 1. Validate A11 contract guard recognizes a base column rename (same field id)
--    and refreshes via CompatibleSafeWithRebind.
-- 2. Validate the MV's persisted contract is name-independent: after rename, the
--    incremental refresh still finds the underlying column by field id.
--
-- Setup: REST catalog (required for Spark schema evolution).
-- Spark renames base column `region` -> `area` (same field id).
-- NovaRocks must still refresh correctly using field-id-based rebind.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_a11_rename_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0};
CREATE TABLE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} VALUES
  (1, 'US', 100),
  (2, 'EU', 50),
  (3, 'US', 200);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_a11_rename_${uuid0};
USE ns_a11_rename_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, region, amount FROM base_${uuid0} WHERE region = 'US';

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- Initial MV state: US rows only.
SELECT id, region, amount FROM mv_${uuid0} ORDER BY id;

-- query 5
-- Spark: rename base column `region` -> `area` (same field id) and insert new row.
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-rename-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_a11_rename_${uuid0}.base_${uuid0} RENAME COLUMN region TO area;
INSERT INTO ice_rest.ns_a11_rename_${uuid0}.base_${uuid0} VALUES (4, 'US', 300);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- A11 contract guard fires CompatibleSafeWithRebind; refresh should succeed
-- and the new US row (id=4) should appear in the MV.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 7
-- After rename + refresh: MV output column name is `region` (frozen at CREATE time).
-- New row id=4 should appear.
SELECT id, region, amount FROM mv_${uuid0} ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0}.base_${uuid0} FORCE;
DROP DATABASE ice_ivm_a11_rename_${uuid0}.ns_a11_rename_${uuid0};
DROP CATALOG ice_ivm_a11_rename_${uuid0};
