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
-- @tags=mv,iceberg,visible_bag,external_snapshot_guard,equality_delete
-- An external Equality delete changes S while retaining all table properties,
-- including NovaRocks D/L/P/E. Ordinary REFRESH must preserve the external
-- snapshot guard. This case does not prove strict target-format refusal.
-- No golden or native acceptance receipt is recorded before verification.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG bag_guard_${uuid0}
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
CREATE DATABASE bag_guard_${uuid0}.ns_${uuid0};
CREATE TABLE bag_guard_${uuid0}.ns_${uuid0}.fact (id BIGINT NOT NULL, amount BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO bag_guard_${uuid0}.ns_${uuid0}.fact VALUES (1,10),(2,20);
SET CATALOG bag_guard_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;
CREATE MATERIALIZED VIEW target_mv
DISTRIBUTED BY HASH(id) BUCKETS 3
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id, amount FROM fact;
REFRESH MATERIALIZED VIEW target_mv WITH SYNC MODE;

-- query 2
-- @skip_result_check=true
-- @imv_equivalence_check=target_mv
SELECT id, amount FROM target_mv;

-- query 3
-- @skip_result_check=true
-- @result_contains=TARGET_EXTERNAL_EQUALITY_SNAPSHOT_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/external-target-guard/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/equality.scala"
uea_log="$uea_receipts/equality.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "target_guard")
  import DeleteApplicabilityFixture._
  val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest.ns_${uuid0}.target_mv")
  t.refresh()
  val before = t.currentSnapshot().snapshotId()
  val properties = t.properties().asScala.toMap
  val id = t.schema().findField("id")
  require(id != null, "Published target lacks the visible id field")
  val equalitySchema = t.schema().select("id")
  val w = new GenericAppenderFactory(t.schema(), t.spec(), Array(id.fieldId()), equalitySchema, null)
    .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(output(t, ".parquet")), FileFormat.PARQUET, partition(t, 1))
  try w.write(record(equalitySchema, Seq(1L))) finally w.close()
  val eq = w.toDeleteFile()
  require(eq.content() == FileContent.EQUALITY_DELETES && eq.format() == FileFormat.PARQUET)
  t.newRowDelta().addDeletes(eq).commit()
  t.refresh()
  require(t.currentSnapshot().snapshotId() != before, "External delete did not change the target snapshot")
  require(t.properties().asScala.toMap == properties, "External writer changed target properties")
  val planned = t.newScan().planFiles()
  val tasks = try planned.asScala.toVector finally planned.close()
  require(tasks.exists(_.deletes().asScala.exists(_.location() == eq.location())), "Equality delete is not attached to a target data file")
  emit(obj("record" -> "external_target_guard", "before_snapshot" -> before,
    "after_snapshot" -> t.currentSnapshot().snapshotId(), "all_properties_unchanged" -> true,
    "delete" -> describe(eq)))
  println("TARGET_EXTERNAL_EQUALITY_SNAPSHOT_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_EXTERNAL_EQUALITY_SNAPSHOT_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/equality.jsonl"
printf 'TARGET_EXTERNAL_EQUALITY_SNAPSHOT_OK\n'

-- query 4
-- @skip_result_check=true
DELETE FROM fact WHERE id = 1;

-- query 5
-- Source progress and target publication cannot advance past an external S.
-- @expect_error=Eligible does not bind exact Current publication
REFRESH MATERIALIZED VIEW target_mv WITH SYNC MODE;

-- query 6
-- @cleanup=true
-- @skip_result_check=true
-- @result_contains=TARGET_EXTERNAL_SNAPSHOT_RESTORED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/external-target-guard/${uuid0}"
uea_before_snapshot="$(python3 -c 'import json,sys; rows=[json.loads(line.removeprefix("UEA4G_RECEIPT ")) for line in open(sys.argv[1]) if line.startswith("UEA4G_RECEIPT ")]; rows=[r for r in rows if r.get("record")=="external_target_guard"]; assert len(rows)==1; before=rows[0]["before_snapshot"]; assert isinstance(before,int) and 0<before<=9223372036854775807; print(before)' "$uea_receipts/equality.jsonl")"
uea_scala="$uea_receipts/restore.scala"
uea_log="$uea_receipts/restore.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
printf '\nval restoredTargetSnapshot = %sL\n' "$uea_before_snapshot" >> "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "target_guard_restore")
  import DeleteApplicabilityFixture._
  val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest.ns_${uuid0}.target_mv")
  t.refresh()
  val before = restoredTargetSnapshot
  require(t.snapshot(before) != null, "Original private target snapshot is absent")
  val external = t.currentSnapshot().snapshotId()
  val properties = t.properties().asScala.toMap
  t.manageSnapshots().setCurrentSnapshot(before).commit()
  t.refresh()
  require(t.currentSnapshot().snapshotId() == before, "Official snapshot restore did not recover the original target baseline")
  require(t.properties().asScala.toMap == properties, "Snapshot restore changed target document properties")
  emit(obj("record" -> "external_target_restore", "external_snapshot" -> external,
    "restored_snapshot" -> t.currentSnapshot().snapshotId(), "all_properties_unchanged" -> true))
  println("TARGET_EXTERNAL_SNAPSHOT_RESTORED")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_EXTERNAL_SNAPSHOT_RESTORED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/restore.jsonl"
printf 'TARGET_EXTERNAL_SNAPSHOT_RESTORED\n'

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS target_mv;
DROP TABLE IF EXISTS bag_guard_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE bag_guard_${uuid0}.ns_${uuid0};
DROP CATALOG bag_guard_${uuid0};
