-- @order_sensitive=true
-- @sequential=true
-- Validate Spark-written Iceberg v3 partitioned data through REST Catalog + MinIO.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-partition-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_partition_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_partition_${uuid0} (
  id BIGINT,
  region STRING,
  bucket_id INT,
  metric INT
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_partition_${uuid0} VALUES
  (1, 'us', 10, 10),
  (2, 'eu', 20, 20),
  (3, 'us', 10, 30),
  (4, 'apac', 30, 40),
  (5, 'apac', 30, 60);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT region, COUNT(*) AS cnt, SUM(metric) AS total_metric
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_partition_${uuid0}
WHERE region IN ('apac', 'us')
GROUP BY region
ORDER BY region;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_partition_${uuid0} FORCE;
