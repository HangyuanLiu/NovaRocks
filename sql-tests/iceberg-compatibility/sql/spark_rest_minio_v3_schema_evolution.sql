-- @order_sensitive=true
-- @sequential=true
-- Validate NovaRocks reads a Spark-evolved Iceberg v3 schema from REST Catalog + MinIO.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-schema-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0} (
  id BIGINT,
  payload STRING,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0} VALUES
  (1, 'before-a', 10),
  (2, 'before-b', 20);

ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0}
  ADD COLUMN category STRING;

ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0}
  RENAME COLUMN payload TO label;

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0} VALUES
  (3, 'after-c', 30, 'hot'),
  (4, 'after-d', 40, 'cold');
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, label, category, metric
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_schema_${uuid0} FORCE;
