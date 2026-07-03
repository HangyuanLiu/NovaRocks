-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,aggregate,nullability,error
-- Test Point: Iceberg aggregate MV refresh rejects referenced base nullability drift.
-- Method: Create an aggregate MV over a required group key, relax the field to optional through Spark, and verify refresh fails fast.
-- Scope: Iceberg target MV, single-base aggregate, schema contract.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_a11_null_${uuid0}
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
CREATE DATABASE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders (
  region STRING NOT NULL,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders VALUES ('east', 10), ('west', 5);
SET CATALOG ice_ivm_agg_a11_null_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-agg-null-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders ALTER COLUMN region DROP NOT NULL;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 4
-- @expect_error=changed nullability
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_a11_null_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_a11_null_${uuid0};
