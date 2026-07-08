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
-- Validate Spark can read a NovaRocks-written v2 position-delete file.

-- query 1
CREATE DATABASE IF NOT EXISTS iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0};

-- query 2
CREATE TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.nr_v2_pos_delete_${uuid0} (
  id BIGINT,
  region STRING,
  score INT
)
PARTITION BY (region)
TBLPROPERTIES ("format-version" = "2");

-- query 3
INSERT INTO iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.nr_v2_pos_delete_${uuid0}
VALUES
  (1, 'east', 10),
  (2, 'east', 20),
  (3, 'west', 30),
  (4, 'west', 40);

-- query 4
DELETE FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.nr_v2_pos_delete_${uuid0}
WHERE id IN (2, 4);

-- query 5
SELECT id, region, score
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.nr_v2_pos_delete_${uuid0}
ORDER BY id;

-- query 6
-- @result_contains=SPARK_POSITION_DELETE_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-read-nr-position-delete-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
SELECT id, region, score
FROM ice_rest.nr_compat_${suite_uuid0}.nr_v2_pos_delete_${uuid0}
ORDER BY id;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" \
  | tr -s '[:space:]' ' ' \
  | grep -F "1 east 10" >/dev/null
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" \
  | tr -s '[:space:]' ' ' \
  | grep -F "3 west 30" >/dev/null
printf 'SPARK_POSITION_DELETE_OK\n'

-- query 7
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.nr_v2_pos_delete_${uuid0} FORCE;
