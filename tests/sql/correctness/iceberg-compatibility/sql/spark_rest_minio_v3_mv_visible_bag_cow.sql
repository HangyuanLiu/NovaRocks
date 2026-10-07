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
-- @tags=mv,iceberg,visible_bag,spark,cow,update,merge,deletion_vector,partitioned
-- Spark writes v3 row-lineage sources with explicit COW UPDATE/MERGE modes.
-- Source receipts assert overwrite and real removed/added file sets; fixed
-- integer/string bags are independent of NovaRocks full-content matching.
-- A MERGE and official Puffin DV share one retained source-history window.
-- A later COW UPDATE reads the already masked source without retracting the
-- deleted occurrence twice. Both unpartitioned and identity-partitioned MVs
-- compare full visible bags; candidate file/scan-byte counters are not asserted.
-- This is native acceptance input, with no golden or execution receipt recorded.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG spark_bag_${uuid0}
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
CREATE DATABASE spark_bag_${uuid0}.ns_${uuid0};
SET CATALOG spark_bag_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=SPARK_VISIBLE_BAG_STAGE_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/spark-visible-bag-cow/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial.scala"
uea_log="$uea_receipts/initial.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "spark_cow")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.spark_cow_source"
  def scan(t: Table): Vector[FileScanTask] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.toVector finally planned.close()
  }
  def assertCow(t: Table, before: Long, paths: Set[String], operation: String): Unit = {
    t.refresh()
    require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
    require(t.currentSnapshot().snapshotId() != before && t.currentSnapshot().operation() == "overwrite", operation + " did not produce a COW overwrite")
    val current = scan(t).map(_.file().location()).toSet
    val removed = paths.diff(current)
    val added = current.diff(paths)
    require(removed.nonEmpty && added.nonEmpty, operation + " did not replace physical source files")
    emit(obj("record" -> "spark_cow_source", "operation" -> operation, "from_snapshot" -> before,
      "to_snapshot" -> t.currentSnapshot().snapshotId(), "removed_data_files" -> removed.toVector.sorted,
      "added_data_files" -> added.toVector.sorted))
  }
  session.sql("CREATE TABLE " + name + " (id BIGINT NOT NULL, p INT NOT NULL, label STRING NOT NULL, amount BIGINT NOT NULL) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version'='3','write.row-lineage'='true','write.update.mode'='copy-on-write','write.merge.mode'='copy-on-write')")
  session.sql("INSERT INTO " + name + " VALUES (1,1,'shared',7),(3,2,'shared',7),(5,3,'kept',11)")
  session.sql("INSERT INTO " + name + " VALUES (2,1,'shared',7),(4,2,'shared',7),(6,3,'filtered',0)")
  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true")

  val actual = session.sql("SELECT label, amount FROM " + name + " WHERE amount >= 5").collect().toVector
    .map(r => Seq[Any](r.getString(0), r.getLong(1)))
  val expected = Vector(Seq[Any]("shared",7L),Seq[Any]("shared",7L),Seq[Any]("shared",7L),Seq[Any]("shared",7L),Seq[Any]("kept",11L))
  require(actual.map(r => json(r).toString).sorted == expected.map(r => json(r).toString).sorted, "Independent Spark endpoint bag mismatch")
  emit(obj("record" -> "spark_cow_endpoint", "stage" -> "initial", "source_snapshot" -> Spark3Util.loadIcebergTable(session, name).currentSnapshot().snapshotId(), "independent_expected_bag" -> expected, "actual_spark_bag" -> actual))
  println("SPARK_VISIBLE_BAG_STAGE_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^SPARK_VISIBLE_BAG_STAGE_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial.jsonl"
printf 'SPARK_VISIBLE_BAG_STAGE_OK\n'

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW bag_mv
DISTRIBUTED BY HASH(label) BUCKETS 3
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT label, amount FROM spark_cow_source WHERE amount >= 5;
CREATE MATERIALIZED VIEW partition_mv
PARTITION BY p
DISTRIBUTED BY HASH(label) BUCKETS 3
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT p, label, amount FROM spark_cow_source WHERE amount >= 5;

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW bag_mv WITH SYNC MODE;
REFRESH MATERIALIZED VIEW partition_mv WITH SYNC MODE;

-- query 5
-- @skip_result_check=true
-- @imv_equivalence_check=bag_mv
SELECT label, amount FROM bag_mv;

-- query 6
-- @skip_result_check=true
-- @imv_equivalence_check=partition_mv
SELECT p, label, amount FROM partition_mv;

-- query 7
-- @skip_result_check=true
-- @result_contains=SPARK_VISIBLE_BAG_STAGE_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/spark-visible-bag-cow/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/update.scala"
uea_log="$uea_receipts/update.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "spark_cow")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.spark_cow_source"
  def scan(t: Table): Vector[FileScanTask] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.toVector finally planned.close()
  }
  def assertCow(t: Table, before: Long, paths: Set[String], operation: String): Unit = {
    t.refresh()
    require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
    require(t.currentSnapshot().snapshotId() != before && t.currentSnapshot().operation() == "overwrite", operation + " did not produce a COW overwrite")
    val current = scan(t).map(_.file().location()).toSet
    val removed = paths.diff(current)
    val added = current.diff(paths)
    require(removed.nonEmpty && added.nonEmpty, operation + " did not replace physical source files")
    emit(obj("record" -> "spark_cow_source", "operation" -> operation, "from_snapshot" -> before,
      "to_snapshot" -> t.currentSnapshot().snapshotId(), "removed_data_files" -> removed.toVector.sorted,
      "added_data_files" -> added.toVector.sorted))
  }
  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  val before = t.currentSnapshot().snapshotId()
  val paths = scan(t).map(_.file().location()).toSet
  session.sql("UPDATE " + name + " SET amount = CASE WHEN id = 1 THEN 9 ELSE 13 END WHERE id IN (1,6)")
  assertCow(t, before, paths, "UPDATE")

  val actual = session.sql("SELECT label, amount FROM " + name + " WHERE amount >= 5").collect().toVector
    .map(r => Seq[Any](r.getString(0), r.getLong(1)))
  val expected = Vector(Seq[Any]("shared",9L),Seq[Any]("shared",7L),Seq[Any]("shared",7L),Seq[Any]("shared",7L),Seq[Any]("kept",11L),Seq[Any]("filtered",13L))
  require(actual.map(r => json(r).toString).sorted == expected.map(r => json(r).toString).sorted, "Independent Spark endpoint bag mismatch")
  emit(obj("record" -> "spark_cow_endpoint", "stage" -> "update", "source_snapshot" -> Spark3Util.loadIcebergTable(session, name).currentSnapshot().snapshotId(), "independent_expected_bag" -> expected, "actual_spark_bag" -> actual))
  println("SPARK_VISIBLE_BAG_STAGE_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^SPARK_VISIBLE_BAG_STAGE_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/update.jsonl"
printf 'SPARK_VISIBLE_BAG_STAGE_OK\n'

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW bag_mv WITH SYNC MODE;
REFRESH MATERIALIZED VIEW partition_mv WITH SYNC MODE;

-- query 9
-- @skip_result_check=true
-- @imv_equivalence_check=bag_mv
SELECT label, amount FROM bag_mv;

-- query 10
-- @skip_result_check=true
-- @imv_equivalence_check=partition_mv
SELECT p, label, amount FROM partition_mv;

-- query 11
-- @skip_result_check=true
-- @result_contains=COW_UPDATE_BAG_OK
-- @result_not_contains=COW_UPDATE_BAG_FAIL
SELECT IF((SELECT COUNT(*) FROM bag_mv) = 6 AND (SELECT COUNT(*) FROM bag_mv WHERE label = 'shared' AND amount = 7) = 3 AND (SELECT COUNT(*) FROM bag_mv WHERE label = 'filtered' AND amount = 13) = 1, 'COW_UPDATE_BAG_OK', 'COW_UPDATE_BAG_FAIL') AS status;

-- query 12
-- @skip_result_check=true
-- @result_contains=SPARK_VISIBLE_BAG_STAGE_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/spark-visible-bag-cow/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/merge_dv.scala"
uea_log="$uea_receipts/merge_dv.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "spark_cow")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.spark_cow_source"
  def scan(t: Table): Vector[FileScanTask] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.toVector finally planned.close()
  }
  def assertCow(t: Table, before: Long, paths: Set[String], operation: String): Unit = {
    t.refresh()
    require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
    require(t.currentSnapshot().snapshotId() != before && t.currentSnapshot().operation() == "overwrite", operation + " did not produce a COW overwrite")
    val current = scan(t).map(_.file().location()).toSet
    val removed = paths.diff(current)
    val added = current.diff(paths)
    require(removed.nonEmpty && added.nonEmpty, operation + " did not replace physical source files")
    emit(obj("record" -> "spark_cow_source", "operation" -> operation, "from_snapshot" -> before,
      "to_snapshot" -> t.currentSnapshot().snapshotId(), "removed_data_files" -> removed.toVector.sorted,
      "added_data_files" -> added.toVector.sorted))
  }
  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  val before = t.currentSnapshot().snapshotId()
  val paths = scan(t).map(_.file().location()).toSet
  session.sql("MERGE INTO " + name + " AS target USING (SELECT CAST(2 AS BIGINT) AS id, 1 AS p, 'changed' AS label, CAST(15 AS BIGINT) AS amount UNION ALL SELECT CAST(3 AS BIGINT), 2, 'shared', CAST(1 AS BIGINT) UNION ALL SELECT CAST(7 AS BIGINT), 2, 'shared', CAST(7 AS BIGINT)) AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET label = source.label, amount = source.amount WHEN NOT MATCHED THEN INSERT (id,p,label,amount) VALUES (source.id,source.p,source.label,source.amount)")
  assertCow(t, before, paths, "MERGE")
  val positions = session.sql("SELECT _file, _pos FROM " + name + " WHERE id = 4").collect()
  require(positions.length == 1, "DV fixture must name one visible occurrence")
  val vector = dv(t, Seq((positions(0).getString(0), positions(0).getLong(1))), 2).head
  require(vector.format() == FileFormat.PUFFIN && vector.content() == FileContent.POSITION_DELETES)
  val merged = t.currentSnapshot().snapshotId()
  t.newRowDelta().addDeletes(vector).commit()
  t.refresh()
  val task = scan(t).find(_.file().location() == positions(0).getString(0)).get
  require(task.deletes().asScala.exists(_.location() == vector.location()), "Source DV is not attached to its exact file")
  emit(obj("record" -> "source_dv", "from_snapshot" -> merged, "to_snapshot" -> t.currentSnapshot().snapshotId(), "delete" -> describe(vector)))

  val actual = session.sql("SELECT label, amount FROM " + name + " WHERE amount >= 5").collect().toVector
    .map(r => Seq[Any](r.getString(0), r.getLong(1)))
  val expected = Vector(Seq[Any]("shared",9L),Seq[Any]("changed",15L),Seq[Any]("shared",7L),Seq[Any]("kept",11L),Seq[Any]("filtered",13L))
  require(actual.map(r => json(r).toString).sorted == expected.map(r => json(r).toString).sorted, "Independent Spark endpoint bag mismatch")
  emit(obj("record" -> "spark_cow_endpoint", "stage" -> "merge_dv", "source_snapshot" -> Spark3Util.loadIcebergTable(session, name).currentSnapshot().snapshotId(), "independent_expected_bag" -> expected, "actual_spark_bag" -> actual))
  println("SPARK_VISIBLE_BAG_STAGE_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^SPARK_VISIBLE_BAG_STAGE_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/merge_dv.jsonl"
printf 'SPARK_VISIBLE_BAG_STAGE_OK\n'

-- query 13
-- @skip_result_check=true
-- @result_contains=source: IcebergDeltaTable
-- @result_contains=QUOTA PRECLAIM
-- @result_contains=QUOTA TRIM
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW bag_mv;

-- query 14
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW bag_mv WITH SYNC MODE;
REFRESH MATERIALIZED VIEW partition_mv WITH SYNC MODE;

-- query 15
-- @skip_result_check=true
-- @imv_equivalence_check=bag_mv
SELECT label, amount FROM bag_mv;

-- query 16
-- @skip_result_check=true
-- @imv_equivalence_check=partition_mv
SELECT p, label, amount FROM partition_mv;

-- query 17
-- @skip_result_check=true
-- @result_contains=COW_MERGE_DV_BAG_OK
-- @result_not_contains=COW_MERGE_DV_BAG_FAIL
SELECT IF((SELECT COUNT(*) FROM bag_mv) = 5 AND (SELECT COUNT(*) FROM bag_mv WHERE label = 'shared' AND amount = 7) = 1 AND (SELECT COUNT(*) FROM bag_mv WHERE label = 'changed' AND amount = 15) = 1, 'COW_MERGE_DV_BAG_OK', 'COW_MERGE_DV_BAG_FAIL') AS status;

-- query 18
-- @skip_result_check=true
-- @result_contains=SPARK_VISIBLE_BAG_STAGE_OK
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/spark-visible-bag-cow/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/after_dv.scala"
uea_log="$uea_receipts/after_dv.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "spark_cow")
  import DeleteApplicabilityFixture._
  val session = org.apache.spark.sql.SparkSession.active
  val name = "ice_rest.ns_${uuid0}.spark_cow_source"
  def scan(t: Table): Vector[FileScanTask] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.toVector finally planned.close()
  }
  def assertCow(t: Table, before: Long, paths: Set[String], operation: String): Unit = {
    t.refresh()
    require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source row-lineage contract changed")
    require(t.currentSnapshot().snapshotId() != before && t.currentSnapshot().operation() == "overwrite", operation + " did not produce a COW overwrite")
    val current = scan(t).map(_.file().location()).toSet
    val removed = paths.diff(current)
    val added = current.diff(paths)
    require(removed.nonEmpty && added.nonEmpty, operation + " did not replace physical source files")
    emit(obj("record" -> "spark_cow_source", "operation" -> operation, "from_snapshot" -> before,
      "to_snapshot" -> t.currentSnapshot().snapshotId(), "removed_data_files" -> removed.toVector.sorted,
      "added_data_files" -> added.toVector.sorted))
  }
  val t = Spark3Util.loadIcebergTable(session, name)
  t.refresh()
  require(scan(t).exists(_.deletes().asScala.exists(_.format() == FileFormat.PUFFIN)), "Expected the prior source DV")
  val before = t.currentSnapshot().snapshotId()
  val paths = scan(t).map(_.file().location()).toSet
  session.sql("UPDATE " + name + " SET amount = 9 WHERE id = 7")
  assertCow(t, before, paths, "UPDATE_AFTER_DV")

  val actual = session.sql("SELECT label, amount FROM " + name + " WHERE amount >= 5").collect().toVector
    .map(r => Seq[Any](r.getString(0), r.getLong(1)))
  val expected = Vector(Seq[Any]("shared",9L),Seq[Any]("shared",9L),Seq[Any]("changed",15L),Seq[Any]("kept",11L),Seq[Any]("filtered",13L))
  require(actual.map(r => json(r).toString).sorted == expected.map(r => json(r).toString).sorted, "Independent Spark endpoint bag mismatch")
  emit(obj("record" -> "spark_cow_endpoint", "stage" -> "after_dv", "source_snapshot" -> Spark3Util.loadIcebergTable(session, name).currentSnapshot().snapshotId(), "independent_expected_bag" -> expected, "actual_spark_bag" -> actual))
  println("SPARK_VISIBLE_BAG_STAGE_OK")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^SPARK_VISIBLE_BAG_STAGE_OK$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/after_dv.jsonl"
printf 'SPARK_VISIBLE_BAG_STAGE_OK\n'

-- query 19
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW bag_mv WITH SYNC MODE;
REFRESH MATERIALIZED VIEW partition_mv WITH SYNC MODE;

-- query 20
-- @skip_result_check=true
-- @imv_equivalence_check=bag_mv
SELECT label, amount FROM bag_mv;

-- query 21
-- @skip_result_check=true
-- @imv_equivalence_check=partition_mv
SELECT p, label, amount FROM partition_mv;

-- query 22
-- @skip_result_check=true
-- @result_contains=COW_MASKED_SOURCE_BAG_OK
-- @result_not_contains=COW_MASKED_SOURCE_BAG_FAIL
SELECT IF((SELECT COUNT(*) FROM bag_mv) = 5 AND (SELECT COUNT(*) FROM bag_mv WHERE label = 'shared' AND amount = 9) = 2 AND (SELECT COUNT(*) FROM bag_mv WHERE label = 'shared' AND amount = 7) = 0, 'COW_MASKED_SOURCE_BAG_OK', 'COW_MASKED_SOURCE_BAG_FAIL') AS status;

-- query 23
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS bag_mv;
DROP MATERIALIZED VIEW IF EXISTS partition_mv;
DROP TABLE IF EXISTS spark_bag_${uuid0}.ns_${uuid0}.spark_cow_source FORCE;
DROP DATABASE spark_bag_${uuid0}.ns_${uuid0};
DROP CATALOG spark_bag_${uuid0};
