// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Loaded after iceberg-delete-applicability/generate.scala by the runner.
object TargetCandidateFixture {
  import DeleteApplicabilityFixture._
  val NoOpId = -7L
  def target(namespace: String): Table = {
    require(namespace.matches("ns_[a-zA-Z0-9_]+"), "Private fixture namespace is invalid")
    val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest." + namespace + ".candidate_mv")
    t.refresh()
    require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Target contract differs from the fixture")
    t
  }
  def seed(namespace: String): Unit = {
    val t = target(namespace)
    require(t.currentSnapshot() == null, "Seed may only precede the first target publication")
    val properties = t.properties().asScala.toMap
    val id = t.schema().findField("id")
    val p = t.schema().findField("p")
    require(id != null && p != null && id.`type`().typeId() == org.apache.iceberg.types.Type.TypeID.LONG && p.`type`().typeId() == org.apache.iceberg.types.Type.TypeID.LONG)
    require(t.spec().fields().size() == 1 && t.spec().fields().get(0).sourceId() == p.fieldId() && t.spec().fields().get(0).transform().toString == "identity")
    val token = java.util.UUID.randomUUID().toString
    val path = t.location().stripSuffix("/") + "/data/uea7b3-seed-" + token + "/equality.parquet"
    val part = new PartitionData(t.spec().partitionType()); part.set(0, Long.box(2L))
    val eqSchema = t.schema().select("id")
    val w = new GenericAppenderFactory(t.schema(), t.spec(), Array(id.fieldId()), eqSchema, null)
      .newEqDeleteWriter(EncryptedFiles.plainAsEncryptedOutput(t.io().newOutputFile(path)), FileFormat.PARQUET, part)
    try w.write(record(eqSchema, Seq(NoOpId))) finally w.close()
    val eq = w.toDeleteFile()
    require(eq.content() == FileContent.EQUALITY_DELETES && eq.recordCount() == 1 && eq.location() == path)
    t.refresh()
    require(t.currentSnapshot() == null && t.properties().asScala.toMap == properties, "Writing the artifact changed target metadata")
    val source = org.apache.spark.sql.SparkSession.active.sql("SELECT id FROM ice_rest." + namespace + ".fact WHERE id = -7").collect()
    require(source.isEmpty, "Seed Equality value is not absent from the source")
    val arm = obj("token" -> token, "table_uuid" -> metadata(t).uuid(), "namespace" -> namespace,
      "table" -> "candidate_mv", "table_location" -> t.location(), "schema_id" -> metadata(t).currentSchemaId(),
      "spec_id" -> t.spec().specId(), "partition_source_id" -> p.fieldId(), "partition_value" -> 2L,
      "equality_field_id" -> id.fieldId(), "no_op_value" -> NoOpId, "artifact_path" -> eq.location(),
      "artifact_size" -> eq.fileSizeInBytes(), "artifact_record_count" -> eq.recordCount())
    emit(obj("record" -> "candidate_seed_arm", "arm" -> arm, "artifact" -> describe(eq), "catalog_commit" -> false))
    println("TARGET_CANDIDATE_SEED_READY")
  }
  def retractP1(namespace: String): Unit = {
    require(namespace.matches("ns_[a-zA-Z0-9_]+"), "Private fixture namespace is invalid")
    val session = org.apache.spark.sql.SparkSession.active
    val name = "ice_rest." + namespace + ".fact"
    val source = Spark3Util.loadIcebergTable(session, name)
    source.refresh()
    require(metadata(source).formatVersion() == 3 && source.properties().get("write.row-lineage") == "true")
    def scan(): Vector[FileScanTask] = {
      val planned = source.newScan().planFiles()
      try planned.asScala.toVector finally planned.close()
    }
    def deletes(task: FileScanTask) = task.deletes().asScala.toVector.map(d => describe(d).toString).sorted
    val beforeSnapshot = source.currentSnapshot().snapshotId()
    val beforeUuid = metadata(source).uuid()
    val before = scan()
    require(before.nonEmpty && before.size <= 64)
    val beforeByPath = before.map(t => t.file().location() -> t).toMap
    require(beforeByPath.size == before.size, "Source contains duplicate physical files")
    session.sql("ALTER TABLE " + name + " SET TBLPROPERTIES ('write.merge.mode'='copy-on-write')")
    // Rewriting id2 with its original value guarantees an actual COW replacement
    // while id1 is removed. Both matched rows are in p1; visible id2 is unchanged.
    session.sql("MERGE INTO " + name + " t USING (SELECT CAST(1 AS BIGINT) AS id UNION ALL SELECT CAST(2 AS BIGINT) AS id) s " +
      "ON t.id = s.id AND t.p = 1 WHEN MATCHED AND s.id = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET amount = t.amount")
    source.refresh()
    require(metadata(source).uuid() == beforeUuid && metadata(source).formatVersion() == 3 && source.properties().get("write.row-lineage") == "true")
    val after = scan()
    require(after.nonEmpty && after.size <= 64)
    val afterByPath = after.map(t => t.file().location() -> t).toMap
    require(afterByPath.size == after.size, "Source contains duplicate physical files")
    val removed = beforeByPath.keySet.diff(afterByPath.keySet).toVector.sorted
    val added = afterByPath.keySet.diff(beforeByPath.keySet).toVector.sorted
    val retained = beforeByPath.keySet.intersect(afterByPath.keySet).toVector.sorted
    val removedP = removed.map(p => beforeByPath(p).file().partition().get(0,classOf[java.lang.Long]).longValue())
    val addedP = added.map(p => afterByPath(p).file().partition().get(0,classOf[java.lang.Long]).longValue())
    require(source.currentSnapshot().snapshotId() != beforeSnapshot && source.currentSnapshot().operation() == "overwrite", "p1 source mutation did not produce an actual COW overwrite")
    require(removed.nonEmpty && added.nonEmpty && removedP.forall(_ == 1L) && addedP.forall(_ == 1L), "COW file changes are not exactly within p1")
    require(retained.nonEmpty && retained.forall(p => deletes(beforeByPath(p)) == deletes(afterByPath(p))), "Retained source delete applications changed")
    require(added.forall(p => afterByPath(p).deletes().isEmpty), "New COW source files have row deletes")
    val actual = session.sql("SELECT id,p,label,amount FROM " + name).collect().toVector.map(r => (r.getLong(0),r.getLong(1),r.getString(2),r.getLong(3))).groupBy(identity).map { case (key, values) => key -> values.size }
    val expected = Vector((2L,1L,"same",10L),(3L,2L,"same",10L),(4L,2L,"same",10L)).groupBy(identity).map { case (key, values) => key -> values.size }
    require(actual == expected, "COW source bag differs from the exact retraction endpoint")
    emit(obj("record" -> "candidate_source_cow", "table_uuid" -> beforeUuid, "from_snapshot" -> beforeSnapshot,
      "to_snapshot" -> source.currentSnapshot().snapshotId(), "operation" -> source.currentSnapshot().operation(),
      "removed_data_files" -> removed, "removed_partitions" -> removedP, "added_data_files" -> added, "added_partitions" -> addedP,
      "retained_delete_applications" -> retained.map(p => obj("data_path" -> p, "before" -> deletes(beforeByPath(p)), "after" -> deletes(afterByPath(p)))),
      "before_files" -> before.map(t => obj("data" -> describe(t.file()), "deletes" -> t.deletes().asScala.toVector.map(describe))),
      "after_files" -> after.map(t => obj("data" -> describe(t.file()), "deletes" -> t.deletes().asScala.toVector.map(describe)))))
    println("TARGET_CANDIDATE_SOURCE_COW_READY")
  }
  def observe(namespace: String, stage: String): Unit = {
    val t = target(namespace)
    val session = org.apache.spark.sql.SparkSession.active
    val snapshot = t.currentSnapshot()
    require(snapshot != null, "Published target has no snapshot")
    if (stage == "initial") {
      val markerKey = "novarocks.write.session.v1"
      val marker = snapshot.summary().get(markerKey)
      require(marker != null && !marker.isEmpty, "Current B2 lacks its final write session marker")
      val matches = metadata(t).snapshots().asScala.toVector.filter(s => marker == s.summary().get(markerKey)).map(_.snapshotId())
      require(matches == Vector(snapshot.snapshotId()), "Final write session marker is duplicated or belongs to a noncurrent snapshot")
      emit(obj("record" -> "candidate_session_marker", "table_uuid" -> metadata(t).uuid(),
        "current_snapshot" -> snapshot.snapshotId(), "marker" -> marker, "matching_snapshot_ids" -> matches))
    }
    val planned = t.newScan().planFiles()
    val tasks = try planned.asScala.toVector finally planned.close()
    val p2 = tasks.filter(_.file().partition().get(0, classOf[java.lang.Long]).longValue() == 2L)
    val eqs = p2.flatMap(_.deletes().asScala).filter(_.content() == FileContent.EQUALITY_DELETES)
    if (stage == "full") {
      def summaryCount(key: String): Long = {
        val value = snapshot.summary().get(key)
        require(value != null, "Full rebuild is missing actual snapshot summary " + key)
        value.toLong
      }
      Vector("total-delete-files", "total-equality-deletes", "total-position-deletes").foreach { key =>
        require(summaryCount(key) == 0L, "Full rebuild did not clear actual snapshot summary " + key)
      }
      require(summaryCount("total-data-files") == tasks.size.toLong, "Full rebuild data-file summary differs from actual SDK scan")
      require(tasks.forall(_.deletes().isEmpty), "Full rebuild retained attached delete artifacts")
      require(tasks.forall(task => !task.file().location().contains("/data/uea7b3-seed-") &&
        task.deletes().asScala.forall(d => !d.location().contains("/data/uea7b3-seed-"))), "Full rebuild retained a live seed artifact")
    } else require(eqs.nonEmpty && eqs.forall(_.location().contains("/data/uea7b3-seed-")), "Seed Equality is not attached to noncandidate p2 data")
    val snapshotManifest = mapper.readTree(snapshot.summary().get("novarocks.documents.v1"))
    val publication = snapshotManifest.get("documents").elements().asScala.toVector.filter(_.get("name").asText() == "publication")
    require(publication.size == 1 && publication.head.get("attachment").get("kind").asText() == "exact-output" && publication.head.get("attachment").get("snapshot_id").asLong() == snapshot.snapshotId(), "P is not bound to actual Current output")
    val metadataManifest = mapper.readTree(t.properties().get("novarocks.documents.v1"))
    val eligibility = metadataManifest.get("documents").elements().asScala.toVector.filter(_.get("name").asText() == "eligibility")
    require(eligibility.size == 1, "Normal publication lacks E")
    def rows(table: String) = session.sql("SELECT id,p,label,amount FROM ice_rest." + namespace + "." + table).collect().toVector.map(r => (r.getLong(0),r.getLong(1),r.getString(2),r.getLong(3))).groupBy(identity).map { case (key, values) => key -> values.size }
    val expectedIds = stage match {
      case "initial" => Vector(1L,2L,3L,4L)
      case "p1" | "stopped" | "progress" => Vector(2L,3L,4L)
      case "full" => Vector(2L,4L,5L)
      case _ => throw new IllegalArgumentException("Unknown target fixture stage")
    }
    val expected = expectedIds.map(id => (id, if (id <= 2) 1L else 2L, "same", 10L)).groupBy(identity).map { case (key, values) => key -> values.size }
    require(rows("candidate_mv") == expected, "Target visible bag differs from the independent fixture endpoint")
    if (stage == "initial" || stage == "p1" || stage == "full") require(rows("candidate_mv") == rows("fact"), "Independent source and target bags differ")
    if (stage == "p1") require(tasks.filter(_.file().partition().get(0,classOf[java.lang.Long]).longValue() == 1L).exists(_.deletes().asScala.exists(_.format() == FileFormat.PUFFIN)), "p1 retraction did not create a real DV")
    emit(obj("record" -> "candidate_observation", "stage" -> stage, "table_uuid" -> metadata(t).uuid(),
      "snapshot" -> snapshot.snapshotId(), "snapshot_summary" -> snapshot.summary().asScala.toMap, "table_location" -> t.location(),
      "p1_paths" -> tasks.filter(_.file().partition().get(0,classOf[java.lang.Long]).longValue() == 1L).map(_.file().location()).sorted,
      "p2_paths" -> p2.map(_.file().location()).sorted, "publication" -> publication.head, "eligibility" -> eligibility.head,
      "files" -> tasks.map(task => obj("data" -> describe(task.file()), "deletes" -> task.deletes().asScala.toVector.map(describe)))))
    println("TARGET_CANDIDATE_OBSERVED")
  }
}
