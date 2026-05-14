-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,base_type_change,referenced,error
-- Test Objective:
-- Validate that changing the type of a referenced base column triggers
-- BaseFieldTypeChanged error, blocking incremental refresh.
-- Even a widening type change (INT -> BIGINT) is rejected because the
-- MV's expression output type was frozen at CREATE time.
--
-- The MV references `amount` (BIGINT). Spark changes `amount` to STRING.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_a11_type_chg_${uuid0}
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
CREATE DATABASE ice_ivm_a11_type_chg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_a11_type_chg_${uuid0}.ns_${uuid0}.base_${uuid0} (
  id INT NOT NULL,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_a11_type_chg_${uuid0}.ns_${uuid0}.base_${uuid0} VALUES
  (1, 100),
  (2, 50);

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_a11_type_chg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount FROM base_${uuid0};

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 4
-- Initial MV state.
SELECT id, amount FROM mv_${uuid0} ORDER BY id;

-- query 5
-- Spark: widen `amount` from INT to BIGINT (still a type change, blocked by A11).
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-type-chg-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.base_${uuid0} ALTER COLUMN amount TYPE bigint;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- Refresh must fail: BaseFieldTypeChanged for `amount` (int -> long).
-- @expect_error=changed type from
REFRESH MATERIALIZED VIEW mv_${uuid0};

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_${uuid0};
DROP TABLE ice_ivm_a11_type_chg_${uuid0}.ns_${uuid0}.base_${uuid0} FORCE;
DROP DATABASE ice_ivm_a11_type_chg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_a11_type_chg_${uuid0};
