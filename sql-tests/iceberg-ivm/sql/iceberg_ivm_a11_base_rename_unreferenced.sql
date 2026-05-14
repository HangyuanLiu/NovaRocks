-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,base_rename,unreferenced
-- Test Objective:
-- Validate that renaming a base column NOT referenced by the MV is transparent:
-- the A11 contract guard sees no referenced-field change and refresh succeeds
-- with an incremental update.
--
-- The MV selects only `id` and `amount` (not `region`).
-- Spark renames `region` -> `geo` (unreferenced by the MV).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_a11_rename_unref_${uuid0}
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
CREATE DATABASE ice_ivm_a11_rename_unref_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_a11_rename_unref_${uuid0}.ns_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_a11_rename_unref_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES
  (1, 'US', 100),
  (2, 'EU', 50),
  (3, 'US', 200);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_a11_rename_unref_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount FROM base_${uuid0};

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- Initial MV state: all rows, only id and amount columns.
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 5
-- Spark: rename `region` -> `geo` (NOT referenced by the MV).
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-unref-rename-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.base_${uuid0} RENAME COLUMN region TO geo;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
INSERT INTO ice_ivm_a11_rename_unref_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES (4, 'AP', 400);

-- query 7
-- Unreferenced rename: contract guard sees CompatibleSafe (or CompatibleSafeWithRebind
-- for unreferenced fields, which is also safe). Refresh must succeed.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 8
-- MV should now have all 4 rows including the new one.
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_rename_unref_${uuid0}.ns_${uuid0}.base_${uuid0} FORCE;
DROP DATABASE ice_ivm_a11_rename_unref_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_a11_rename_unref_${uuid0};
