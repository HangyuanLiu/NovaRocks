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

-- @sequential=true
-- This native 1FE+3BE case reads REST-catalog metadata through Spark so it
-- proves the StatisticsFile attachment, not merely a frontend presentation.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0};

-- query 2
-- Start with maintenance disabled so this nonempty parent deliberately has no
-- Puffin. Enabling it for the second append must not publish an NDV that only
-- describes the second row.
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} (
    id BIGINT,
    k BIGINT
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} VALUES (1, 10);

-- query 4
-- @skip_result_check=true
ALTER TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
SET TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'true');

-- query 5
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} VALUES (2, 20);

-- query 6
-- @result_contains=STAT_2E_MISSING_PARENT_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-stat-2e-missing-parent-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
require(table.statisticsFiles().asScala.forall(_.snapshotId() != current), "nonempty parent without statistics gained a partial current-snapshot StatisticsFile")
println("STAT_2E_MISSING_PARENT_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "STAT_2E_MISSING_PARENT_OK"

-- query 7
-- Seed authoritative parent statistics through the native ANALYZE path. The
-- write-side sketch channel is BE-local today, while ANALYZE has its own
-- distributed collection contract; this keeps this test focused on STAT-2E's
-- no-reseat semantics in the required 1FE+3BE topology.
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0};

-- query 8
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 9
-- DELETE must not reseat this ancestor StatisticsFile onto its new snapshot.
-- @skip_result_check=true
DELETE FROM statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} WHERE id = 1;

-- query 10
-- @result_contains=STAT_2E_DELETE_BASIS_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-stat-2e-delete-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
require(table.statisticsFiles().asScala.forall(_.snapshotId() != current), "DELETE reseated a StatisticsFile onto the new snapshot")
require(table.statisticsFiles().asScala.exists(_.snapshotId() != current), "DELETE discarded the ancestor StatisticsFile")
println("STAT_2E_DELETE_BASIS_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "STAT_2E_DELETE_BASIS_OK"

-- query 11
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} FORCE;
