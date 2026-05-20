-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,partition_evolution
-- Test Point: Iceberg aggregate MV refresh treats base partition evolution as transparent when schema contract remains compatible.
-- Method: Create an unpartitioned base, refresh aggregate MV, evolve base to PARTITION BY region through Spark, write new rows, refresh, and compare with base aggregate.
-- Scope: Iceberg target MV, single-base aggregate, base partition evolution.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_part_${uuid0}
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
CREATE DATABASE ice_ivm_agg_part_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_part_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_agg_part_${uuid0};
USE ns_${uuid0};
INSERT INTO orders VALUES ('east', 10), ('west', 5);
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-agg-part-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders ADD PARTITION FIELD region;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO orders VALUES ('east', 20), ('north', 7);
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM orders
GROUP BY region
ORDER BY region;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_part_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_part_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_part_${uuid0};
