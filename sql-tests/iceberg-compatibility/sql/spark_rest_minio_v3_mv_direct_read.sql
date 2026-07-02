-- @order_sensitive=true
-- @sequential=true
-- Validate Spark can read a NovaRocks-created Iceberg MV table directly after W6 single-table conversion.

-- query 1
-- @skip_result_check=true
SET CATALOG iceberg_compat_${suite_uuid0};
CREATE DATABASE IF NOT EXISTS nr_compat_${suite_uuid0};
USE nr_compat_${suite_uuid0};
DROP MATERIALIZED VIEW IF EXISTS mv_direct_read_${uuid0};
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mv_direct_base_${uuid0} FORCE;
CREATE TABLE mv_direct_base_${uuid0} (
  id BIGINT,
  metric INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_direct_base_${uuid0} VALUES
  (1, 10),
  (2, 20),
  (3, 30);
CREATE MATERIALIZED VIEW mv_direct_read_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, metric
FROM mv_direct_base_${uuid0}
WHERE metric >= 20;
REFRESH MATERIALIZED VIEW mv_direct_read_${uuid0};

-- query 2
-- @result_contains=SPARK_SQL_OK
shell: set -eu
describe_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-mv-direct-read-describe-XXXXXX.sql")"
select_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-mv-direct-read-select-XXXXXX.sql")"
trap 'rm -f "$describe_sql" "$select_sql"' EXIT

cat > "$describe_sql" <<'SPARK_SQL'
DESCRIBE TABLE ice_rest.nr_compat_${suite_uuid0}.mv_direct_read_${uuid0};
SPARK_SQL

cat > "$select_sql" <<'SPARK_SQL'
SELECT id, metric
FROM ice_rest.nr_compat_${suite_uuid0}.mv_direct_read_${uuid0}
ORDER BY id;
SPARK_SQL

if ! describe_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$describe_sql" 2>&1)"; then
  printf '%s\n' "$describe_out"
  exit 1
fi
printf '%s\n' "$describe_out"
if ! printf '%s\n' "$describe_out" | grep -Eq '(^|[[:space:]])__nova_base_row_id([[:space:]]|$)'; then
  printf 'missing __nova_base_row_id in Spark DESCRIBE output\n' >&2
  exit 1
fi

if ! select_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$select_sql" 2>&1)"; then
  printf '%s\n' "$select_out"
  exit 1
fi
printf '%s\n' "$select_out"
actual_rows="$(printf '%s\n' "$select_out" | awk 'NF == 2 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ { print $1 " " $2 }')"
expected_rows="$(printf '2 20\n3 30')"
if [ "$actual_rows" != "$expected_rows" ]; then
  printf 'unexpected Spark MV rows:\n%s\n' "$actual_rows" >&2
  exit 1
fi
printf 'SPARK_SQL_OK\n'

-- query 3
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS mv_direct_read_${uuid0};
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.mv_direct_base_${uuid0} FORCE;
