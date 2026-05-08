-- @order_sensitive=true
-- @sequential=true
-- Validate NovaRocks reads Spark-created Iceberg refs through FOR VERSION AS OF.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-refs-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0} (
  id BIGINT,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0} VALUES
  (1, 10),
  (2, 20);

ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0}
  CREATE BRANCH backup;

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0} VALUES
  (3, 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT ref_name, row_count, metric_sum
FROM (
  SELECT 'backup' AS ref_name, COUNT(*) AS row_count, SUM(metric) AS metric_sum
  FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0}
  FOR VERSION AS OF 'backup'
  UNION ALL
  SELECT 'main' AS ref_name, COUNT(*) AS row_count, SUM(metric) AS metric_sum
  FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0}
) refs
ORDER BY ref_name;

-- query 3
ALTER TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0} DROP BRANCH backup;
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_refs_${uuid0} FORCE;
