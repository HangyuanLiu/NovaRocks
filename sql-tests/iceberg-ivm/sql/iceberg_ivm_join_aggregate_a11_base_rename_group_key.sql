-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,a11,join,aggregate,base_rename
-- Test Point: Iceberg join aggregate MV refresh rebinds a renamed dim-side GROUP BY key.
-- Method: Rename dim.region -> area through Spark, insert new fact rows through NovaRocks, refresh, and compare the MV with the rewritten base query.
-- Scope: Iceberg target MV, join aggregate, schema evolution, field-id rebind.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_a11_group_${uuid0}
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
CREATE DATABASE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_agg_a11_group_${uuid0};
USE ns_${uuid0};
INSERT INTO dim VALUES (10, 'east'), (20, 'west');
INSERT INTO fact VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50);
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 4
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-a11-join-group-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.dim RENAME COLUMN region TO area;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 5
-- @skip_result_check=true
INSERT INTO dim VALUES (30, 'north');
INSERT INTO fact VALUES (4, 30, 7);
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 6
SELECT region, c, s FROM join_agg_mv_${uuid0} ORDER BY region;

-- query 7
SELECT d.area AS region, COUNT(*) AS c, SUM(f.amount) AS s
FROM fact AS f
JOIN dim AS d ON f.dim_id = d.id
GROUP BY d.area
ORDER BY d.area;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_a11_group_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_a11_group_${uuid0};
