-- @order_sensitive=true
-- @sequential=true
-- Validate NovaRocks applies Spark-written Iceberg v3 row-level DELETE files.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-delete-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_delete_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_delete_${uuid0} (
  id BIGINT,
  data STRING,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet',
  'write.delete.mode' = 'merge-on-read'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_delete_${uuid0} VALUES
  (1, 'keep-a', 10),
  (2, 'drop-b', 20),
  (3, 'keep-c', 30),
  (4, 'drop-d', 40),
  (5, 'keep-e', 50);

DELETE FROM ice_rest.nr_compat_${suite_uuid0}.spark_v3_delete_${uuid0}
WHERE id IN (2, 4);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, data, metric
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_delete_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_delete_${uuid0} FORCE;
