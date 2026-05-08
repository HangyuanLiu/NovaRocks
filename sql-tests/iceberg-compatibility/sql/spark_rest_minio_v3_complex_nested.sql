-- @order_sensitive=true
-- @sequential=true
-- Validate Spark-written Iceberg v3 ARRAY, MAP, STRUCT, and nested ARRAY<STRUCT> reads.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-complex-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_complex_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_complex_${uuid0} (
  id BIGINT,
  profile STRUCT<name: STRING, city: STRING>,
  tags ARRAY<STRING>,
  attrs MAP<STRING, INT>,
  events ARRAY<STRUCT<kind: STRING, weight: INT>>
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_complex_${uuid0} VALUES
  (
    1,
    named_struct('name', 'ann', 'city', 'sf'),
    array('blue', 'green'),
    map('score', 10, 'level', 2),
    array(named_struct('kind', 'click', 'weight', 3), named_struct('kind', 'view', 'weight', 4))
  ),
  (
    2,
    named_struct('name', 'bob', 'city', 'ny'),
    array('red'),
    map('score', 20, 'level', 1),
    array(named_struct('kind', 'click', 'weight', 7))
  );
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id,
       profile.city AS city,
       array_length(tags) AS tag_count,
       tags[1] AS first_tag,
       attrs['score'] AS score,
       events[1].weight AS first_event_weight
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_complex_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_complex_${uuid0} FORCE;
