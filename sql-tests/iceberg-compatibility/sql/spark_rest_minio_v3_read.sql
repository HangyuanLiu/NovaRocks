-- @order_sensitive=true
-- @sequential=true
-- Validate cross-engine Iceberg v3 compatibility:
-- Spark writes a format-v3 table through REST Catalog + MinIO, then NovaRocks reads it.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_read_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_read_${uuid0} (
  id BIGINT,
  data STRING,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_read_${uuid0} VALUES
  (1, 'spark-rest-minio-a', 10),
  (2, 'spark-rest-minio-b', 20),
  (3, 'spark-rest-minio-c', 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/.codex/environments/iceberg-rest-spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, data, metric
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_read_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_read_${uuid0} FORCE;
DROP DATABASE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0};
