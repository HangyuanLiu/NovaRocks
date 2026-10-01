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
-- @order_sensitive=true
-- @tags=mv,iceberg,visible_bag,null,decimal,timestamp,nan,string
-- UEA-7B3: native 1FE+3BE acceptance input; no golden is recorded before verification.

-- Real NaN is written by the frozen Spark fixture: NovaRocks string-to-double
-- CAST intentionally maps non-finite results to NULL. Spark independently
-- checks source and target IEEE NaN values and records their raw bit patterns.

-- Spark source mutations request copy-on-write mode and record their actual
-- snapshot operation and removed/added files; whole-file deletion is valid.
-- Existing Native source DML cannot serialize non-finite values as SQL literals.
-- MV maintenance remains Native, with bag oracles and target bit comparisons.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG bag_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE bag_${uuid0}.ns_${uuid0};
CREATE TABLE bag_${uuid0}.ns_${uuid0}.typed_t (id BIGINT NOT NULL, label STRING, amount DECIMAL(20,4), event_ts DATETIME, reading DOUBLE)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG bag_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=REAL_NAN_CONTENT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/visible-bag-real-nan/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial.scala"
uea_log="$uea_receipts/initial.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "typed_nan")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.typed_t"
  session.sql("ALTER TABLE " + name + " SET TBLPROPERTIES ('write.delete.mode'='copy-on-write','write.update.mode'='copy-on-write')")
  session.sql("INSERT INTO " + name + " VALUES (1,'dictionary-repeat',12.3400,CAST('2024-02-01 00:00:00' AS TIMESTAMP),CAST('NaN' AS DOUBLE)),(2,'dictionary-repeat',12.3400,CAST('2024-02-01 00:00:00' AS TIMESTAMP),CAST('NaN' AS DOUBLE))")
  session.sql("INSERT INTO " + name + " VALUES (3,NULL,NULL,NULL,NULL),(4,NULL,NULL,NULL,NULL)")

  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
  val rows = session.sql("SELECT reading FROM " + name + " WHERE reading IS NOT NULL").collect().toVector
  require(rows.size == 2 && rows.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "Source must contain actual IEEE NaN values")
  emit(obj("record" -> "typed_source_nan_bits", "stage" -> "initial", "snapshot" -> t.currentSnapshot().snapshotId(), "bits" -> rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  println("REAL_NAN_CONTENT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^REAL_NAN_CONTENT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial.jsonl"
printf 'REAL_NAN_CONTENT_OK\n'

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW typed_mv
DISTRIBUTED BY HASH(label) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT label, amount, event_ts, reading FROM typed_t;
REFRESH MATERIALIZED VIEW typed_mv;

-- query 4
-- @skip_result_check=true
-- @imv_equivalence_check=typed_mv
SELECT * FROM typed_mv;

-- query 5
-- @skip_result_check=true
-- @result_contains=REAL_NAN_CONTENT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/visible-bag-real-nan/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/cow-delete.scala"
uea_log="$uea_receipts/cow-delete.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "typed_nan")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.typed_t"
  val mutationTable = Spark3Util.loadIcebergTable(session, name)
  mutationTable.refresh()
  val before = mutationTable.currentSnapshot().snapshotId()
  def filePaths(t: Table): Set[String] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.map(_.file().location()).toSet finally planned.close()
  }
  val beforePaths = filePaths(mutationTable)
  session.sql("DELETE FROM " + name + " WHERE id IN (1,3)")
  mutationTable.refresh()
  val afterPaths = filePaths(mutationTable)
  emit(obj("record" -> "typed_source_mutation", "intended_operation" -> "delete", "actual_snapshot_operation" -> mutationTable.currentSnapshot().operation(), "before_snapshot" -> before, "after_snapshot" -> mutationTable.currentSnapshot().snapshotId(), "removed_files" -> beforePaths.diff(afterPaths).toVector.sorted, "added_files" -> afterPaths.diff(beforePaths).toVector.sorted))
  require(mutationTable.currentSnapshot().snapshotId() != before, "Source mutation did not change the snapshot")
  require(beforePaths.diff(afterPaths).nonEmpty, "Source mutation did not remove affected data files")

  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
  val rows = session.sql("SELECT reading FROM " + name + " WHERE reading IS NOT NULL").collect().toVector
  require(rows.size == 1 && rows.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "Source must contain actual IEEE NaN values")
  emit(obj("record" -> "typed_source_nan_bits", "stage" -> "cow-delete", "snapshot" -> t.currentSnapshot().snapshotId(), "bits" -> rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  println("REAL_NAN_CONTENT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^REAL_NAN_CONTENT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/cow-delete.jsonl"
printf 'REAL_NAN_CONTENT_OK\n'

-- query 6
-- @skip_result_check=true
-- @imv_equivalence_check=typed_mv
REFRESH MATERIALIZED VIEW typed_mv WITH SYNC MODE;
SELECT * FROM typed_mv;

-- query 7
-- @skip_result_check=true
-- @result_contains=TYPED_RETRACT_DUPLICATE_OK
-- @result_not_contains=TYPED_RETRACT_DUPLICATE_FAIL
SELECT IF((SELECT COUNT(*) FROM typed_mv) = 2 AND (SELECT COUNT(*) FROM typed_mv WHERE label IS NULL AND amount IS NULL AND event_ts IS NULL AND reading IS NULL) = 1, 'TYPED_RETRACT_DUPLICATE_OK', 'TYPED_RETRACT_DUPLICATE_FAIL') AS status;

-- query 8
-- @skip_result_check=true
-- @result_contains=REAL_NAN_CONTENT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/visible-bag-real-nan/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/retracted.scala"
uea_log="$uea_receipts/retracted.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "typed_nan")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.typed_t"

  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
  val rows = session.sql("SELECT reading FROM " + name + " WHERE reading IS NOT NULL").collect().toVector
  require(rows.size == 1 && rows.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "Source must contain actual IEEE NaN values")
  emit(obj("record" -> "typed_source_nan_bits", "stage" -> "retracted", "snapshot" -> t.currentSnapshot().snapshotId(), "bits" -> rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  val target = session.sql("SELECT reading FROM ice_rest.ns_${uuid0}.typed_mv WHERE reading IS NOT NULL").collect().toVector
  require(target.size == 1 && target.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "MV did not preserve real NaN contents")
  require(rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0))).sorted == target.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0))).sorted, "Source and MV NaN payload bags differ")
  emit(obj("record" -> "typed_mv_nan_bits", "stage" -> "retracted", "bits" -> target.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  println("REAL_NAN_CONTENT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^REAL_NAN_CONTENT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/retracted.jsonl"
printf 'REAL_NAN_CONTENT_OK\n'

-- query 9
-- @skip_result_check=true
-- @result_contains=REAL_NAN_CONTENT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/visible-bag-real-nan/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/cow-update.scala"
uea_log="$uea_receipts/cow-update.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "typed_nan")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.typed_t"
  val mutationTable = Spark3Util.loadIcebergTable(session, name)
  mutationTable.refresh()
  val before = mutationTable.currentSnapshot().snapshotId()
  def filePaths(t: Table): Set[String] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.map(_.file().location()).toSet finally planned.close()
  }
  val beforePaths = filePaths(mutationTable)
  session.sql("UPDATE " + name + " SET amount = 99.9999, event_ts = CAST('2024-03-02 12:34:56' AS TIMESTAMP) WHERE id = 2")
  mutationTable.refresh()
  val afterPaths = filePaths(mutationTable)
  emit(obj("record" -> "typed_source_mutation", "intended_operation" -> "update", "actual_snapshot_operation" -> mutationTable.currentSnapshot().operation(), "before_snapshot" -> before, "after_snapshot" -> mutationTable.currentSnapshot().snapshotId(), "removed_files" -> beforePaths.diff(afterPaths).toVector.sorted, "added_files" -> afterPaths.diff(beforePaths).toVector.sorted))
  require(mutationTable.currentSnapshot().snapshotId() != before, "Source mutation did not change the snapshot")
  require(beforePaths.diff(afterPaths).nonEmpty, "Source mutation did not remove affected data files")

  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
  val rows = session.sql("SELECT reading FROM " + name + " WHERE reading IS NOT NULL").collect().toVector
  require(rows.size == 1 && rows.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "Source must contain actual IEEE NaN values")
  emit(obj("record" -> "typed_source_nan_bits", "stage" -> "cow-update", "snapshot" -> t.currentSnapshot().snapshotId(), "bits" -> rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  println("REAL_NAN_CONTENT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^REAL_NAN_CONTENT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/cow-update.jsonl"
printf 'REAL_NAN_CONTENT_OK\n'

-- query 10
-- @skip_result_check=true
-- @result_contains=REAL_NAN_CONTENT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/visible-bag-real-nan/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/updated.scala"
uea_log="$uea_receipts/updated.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "typed_nan")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.typed_t"
  session.sql("INSERT INTO " + name + " VALUES (5,'dictionary-repeat',99.9999,CAST('2024-03-02 12:34:56' AS TIMESTAMP),CAST('NaN' AS DOUBLE))")

  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
  val rows = session.sql("SELECT reading FROM " + name + " WHERE reading IS NOT NULL").collect().toVector
  require(rows.size == 2 && rows.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "Source must contain actual IEEE NaN values")
  emit(obj("record" -> "typed_source_nan_bits", "stage" -> "updated", "snapshot" -> t.currentSnapshot().snapshotId(), "bits" -> rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  println("REAL_NAN_CONTENT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^REAL_NAN_CONTENT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/updated.jsonl"
printf 'REAL_NAN_CONTENT_OK\n'

-- query 11
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW typed_mv WITH SYNC MODE;

-- query 12
-- @skip_result_check=true
-- @imv_equivalence_check=typed_mv
SELECT * FROM typed_mv;

-- query 13
-- @skip_result_check=true
-- @result_contains=TYPED_UPDATE_CONTENT_OK
-- @result_not_contains=TYPED_UPDATE_CONTENT_FAIL
SELECT IF((SELECT COUNT(*) FROM typed_mv WHERE amount = 99.9999 AND event_ts = '2024-03-02 12:34:56') = 2, 'TYPED_UPDATE_CONTENT_OK', 'TYPED_UPDATE_CONTENT_FAIL') AS status;

-- query 14
-- @skip_result_check=true
-- @result_contains=REAL_NAN_CONTENT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/visible-bag-real-nan/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/published.scala"
uea_log="$uea_receipts/published.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "typed_nan")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.typed_t"

  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
  val rows = session.sql("SELECT reading FROM " + name + " WHERE reading IS NOT NULL").collect().toVector
  require(rows.size == 2 && rows.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "Source must contain actual IEEE NaN values")
  emit(obj("record" -> "typed_source_nan_bits", "stage" -> "published", "snapshot" -> t.currentSnapshot().snapshotId(), "bits" -> rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  val target = session.sql("SELECT reading FROM ice_rest.ns_${uuid0}.typed_mv WHERE reading IS NOT NULL").collect().toVector
  require(target.size == 2 && target.forall(r => java.lang.Double.isNaN(r.getDouble(0))), "MV did not preserve real NaN contents")
  require(rows.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0))).sorted == target.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0))).sorted, "Source and MV NaN payload bags differ")
  emit(obj("record" -> "typed_mv_nan_bits", "stage" -> "published", "bits" -> target.map(r => java.lang.Double.doubleToRawLongBits(r.getDouble(0)))))
  println("REAL_NAN_CONTENT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^REAL_NAN_CONTENT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/published.jsonl"
printf 'REAL_NAN_CONTENT_OK\n'

-- query 15
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS typed_mv;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.typed_t FORCE;
DROP DATABASE bag_${uuid0}.ns_${uuid0};
DROP CATALOG bag_${uuid0};
