-- @order_sensitive=true
-- @sequential=true
-- Validate NovaRocks reads Spark-written data across Iceberg partition spec evolution.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-part-evo-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0} (
  id BIGINT,
  region STRING,
  metric INT
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0} VALUES
  (1, 'us', 10),
  (2, 'eu', 20),
  (3, 'us', 30);

ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0}
  DROP PARTITION FIELD region;

ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0}
  ADD PARTITION FIELD bucket(4, id);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0} VALUES
  (4, 'eu', 40),
  (5, 'apac', 50);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT region, COUNT(*) AS cnt, SUM(metric) AS total_metric
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0}
GROUP BY region
ORDER BY region;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_part_evo_${uuid0} FORCE;
