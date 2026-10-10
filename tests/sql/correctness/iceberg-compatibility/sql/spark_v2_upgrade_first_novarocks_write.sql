-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @sequential=true
-- Historical V2 manifests have no row-ID range. The first NovaRocks V3
-- publication must assign every carried manifest before allocating new rows.

-- query 1
-- @result_contains=SPARK_UPGRADE_READY
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-v2-upgrade-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};
DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0};
DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.up_partitioned_${uuid0};
CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} (id BIGINT, category STRING) USING iceberg TBLPROPERTIES ('format-version'='2');
CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.up_partitioned_${uuid0} (id BIGINT, category STRING) USING iceberg PARTITIONED BY (category) TBLPROPERTIES ('format-version'='2');
INSERT INTO ice_rest.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} VALUES (1,'a'),(2,'b');
INSERT INTO ice_rest.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} VALUES (3,'a'),(4,'b');
INSERT INTO ice_rest.nr_compat_${suite_uuid0}.up_partitioned_${uuid0} VALUES (1,'a'),(2,'b');
INSERT INTO ice_rest.nr_compat_${suite_uuid0}.up_partitioned_${uuid0} VALUES (3,'a'),(4,'b');
ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} SET TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true');
ALTER TABLE ice_rest.nr_compat_${suite_uuid0}.up_partitioned_${uuid0} SET TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true');
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_UPGRADE_READY\n'

-- query 2
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} SELECT 99,'empty' WHERE FALSE;
INSERT INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_partitioned_${uuid0} VALUES (5,'c');

-- query 3
SELECT 'empty-append' AS first_write, COUNT(*) AS total_rows, COUNT(_row_id) AS assigned_rows, COUNT(DISTINCT _row_id) AS unique_rows
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0}
UNION ALL
SELECT 'insert' AS first_write, COUNT(*) AS total_rows, COUNT(_row_id) AS assigned_rows, COUNT(DISTINCT _row_id) AS unique_rows
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_partitioned_${uuid0}
ORDER BY first_write;

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} VALUES (5,'c');

-- query 5
-- @result_contains=SPARK_HISTORICAL_ROW_IDS_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-v3-row-ids-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT CONCAT('UNPARTITIONED_ROW_IDS=', CAST(COUNT(*) AS STRING), ':', CAST(COUNT(_row_id) AS STRING), ':', CAST(COUNT(DISTINCT _row_id) AS STRING)) FROM ice_rest.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0};
SELECT CONCAT('PARTITIONED_ROW_IDS=', CAST(COUNT(*) AS STRING), ':', CAST(COUNT(_row_id) AS STRING), ':', CAST(COUNT(DISTINCT _row_id) AS STRING)) FROM ice_rest.nr_compat_${suite_uuid0}.up_partitioned_${uuid0};
SPARK_SQL
view_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql")"
printf '%s\n' "$view_out" | grep -F 'UNPARTITIONED_ROW_IDS=5:5:5' >/dev/null
printf '%s\n' "$view_out" | grep -F 'PARTITIONED_ROW_IDS=5:5:5' >/dev/null
printf 'SPARK_HISTORICAL_ROW_IDS_OK\n'

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_unpartitioned_${uuid0} FORCE;
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.up_partitioned_${uuid0} FORCE;
