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

// Compose after iceberg-delete-applicability/generate.scala and
// mv-visible-content-encodings/fixture.scala. No inherited fixture is modified.
object MetadataHistoryFixture {
  import DeleteApplicabilityFixture._
  import org.apache.iceberg.types.{Type, Types, TypeUtil}
  import org.apache.iceberg.catalog.TableIdentifier
  val DomainProperty = "novarocks.field_domains.v1"
  val DepthNames = Set("metadata_depth", "metadata_depth_copy")
  val HistoryName = "domain_history"
  val DeepPath = "deep." + (1 to 63).map(i => "n" + i).mkString(".")
  val MaxRows = 16
  val MaxSchemas = 16

  // Keep the same SDK-action deadline/daemon form as FieldDomainNullKeyFixture.
  // The existing Spark wrapper owns its EXIT cleanup; timeout never reports PASS.
  def withDeadline(action: => Unit): Unit = {
    val executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "metadata-history-sdk-deadline"); t.setDaemon(true); t
      }
    })
    val deadline = executor.schedule(new Runnable {
      override def run(): Unit = {
        System.err.println("Metadata/history SDK fixture exceeded 120-second deadline")
        System.exit(124)
      }
    }, 120, java.util.concurrent.TimeUnit.SECONDS)
    try action finally { deadline.cancel(false); executor.shutdownNow() }
  }

  def session = {
    require(IcebergBuild.version() == "1.11.0", "Metadata history oracle requires Iceberg 1.11.0")
    org.apache.spark.sql.SparkSession.active
  }
  def checkNamespace(ns: String): Unit =
    require(ns.length <= 64 && ns.matches("ns_[a-zA-Z0-9_]+"), "Invalid exact fixture namespace")
  def catalog(name: String) = {
    require(Set("ice_rest", "nr_metadata_hadoop").contains(name), "Unexpected fixture catalog")
    Spark3Util.loadIcebergCatalog(session, name)
  }
  def load(ns: String, name: String, catalogName: String = "ice_rest"): Table = {
    checkNamespace(ns)
    require(DepthNames.contains(name) || name == HistoryName, "Unexpected fixture table")
    val t = catalog(catalogName).loadTable(TableIdentifier.of(ns, name)); t.refresh(); t
  }
  def fresh(schema: Schema): Schema = {
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    TypeUtil.assignFreshIds(schema, new TypeUtil.NextID {
      override def get(): Int = counter.incrementAndGet()
    })
  }
  def domainText(fields: Seq[(Int, String)]): String = {
    require(fields.size <= 8 && fields.map(_._1).distinct.size == fields.size &&
      fields.forall { case(id, domain) => id > 0 && Set("tinyint", "smallint", "json").contains(domain) })
    val n = mapper.createObjectNode()
    fields.sortBy(_._1).foreach { case(id, domain) => n.put(id.toString, domain) }
    mapper.writeValueAsString(obj("version" -> 1, "fields" -> n))
  }
  def create(ns: String, name: String, requested: Schema, domains: Schema => String,
             catalogName: String = "ice_rest"): Table = {
    checkNamespace(ns)
    val allocated = fresh(requested)
    val properties = Map("format-version" -> "3", "write.row-lineage" -> "true",
      DomainProperty -> domains(allocated))
    val t = catalog(catalogName).createTable(TableIdentifier.of(ns, name), allocated,
      PartitionSpec.unpartitioned(), properties.asJava)
    require(RecursiveTypeFixture.facts(t.schema()) == RecursiveTypeFixture.facts(allocated),
      "CREATE changed the SDK-allocated IDs, requiredness or complete type tree")
    require(t.currentSnapshot() == null && t.properties().get(DomainProperty) == domains(t.schema()),
      "CREATE response lost its exact declaration or clean initial state")
    t
  }
  def fieldDepth(schema: Schema): Int = {
    def depth(t: Type): Int = t.typeId() match {
      case Type.TypeID.STRUCT => 1 + t.asStructType().fields().asScala.map(f => depth(f.`type`())).max
      case Type.TypeID.LIST => 1 + depth(t.asListType().elementType())
      case Type.TypeID.MAP => 1 + math.max(depth(t.asMapType().keyType()), depth(t.asMapType().valueType()))
      case _ => 1
    }
    schema.columns().asScala.map(f => depth(f.`type`())).max
  }
  def jsonDepth(n: JsonNode): Int = if (n.isContainerNode && n.size() > 0)
    1 + n.elements().asScala.map(jsonDepth).max else 1
  def snapshotSchema(t: Table, snapshot: Long): Schema = {
    val s = t.snapshot(snapshot)
    require(s != null && s.schemaId() != null && t.schemas().containsKey(s.schemaId()),
      "Actual snapshot has no retained exact SDK schema")
    t.schemas().get(s.schemaId())
  }
  def tag(t: Table, name: String): Long = {
    val ref = t.refs().get(name)
    require(ref != null && ref.isTag() && ref.snapshotId() > 0, "Exact fixture tag is absent")
    ref.snapshotId()
  }
  def createTag(t: Table, name: String): Unit = {
    require(t.currentSnapshot() != null && !t.refs().containsKey(name))
    t.manageSnapshots().createTag(name, t.currentSnapshot().snapshotId()).commit(); t.refresh()
    require(tag(t, name) == t.currentSnapshot().snapshotId(), "Tag did not bind the actual snapshot")
  }
  def append(t: Table, rows: Vector[Record], name: String): Unit = {
    val before = Option(t.currentSnapshot()).map(_.snapshotId())
    val uuid = metadata(t).uuid(); val schema = SchemaParser.toJson(t.schema())
    val domains = t.properties().get(DomainProperty)
    val file = RecursiveTypeFixture.write(t, rows, name)
    t.newAppend().appendFile(file).commit(); t.refresh()
    require(metadata(t).uuid() == uuid && SchemaParser.toJson(t.schema()) == schema &&
      t.properties().get(DomainProperty) == domains, "Append changed exact table/schema/domain facts")
    require(t.currentSnapshot().snapshotId() > 0 && !before.contains(t.currentSnapshot().snapshotId()) &&
      Option(t.currentSnapshot().parentId()).map(_.longValue()) == before,
      "Append did not produce the exact parent/child snapshot frontier")
    require(snapshotSchema(t, t.currentSnapshot().snapshotId()).schemaId() == t.schema().schemaId(),
      "Append snapshot did not freeze its actual write schema")
  }
  def content(schema: Schema, row: Record): String = {
    val n = mapper.createObjectNode()
    schema.columns().asScala.foreach { f =>
      n.set[JsonNode](f.name(), RecursiveTypeFixture.value(f.`type`(), row.getField(f.name())))
    }
    val text = mapper.writeValueAsString(n)
    require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 16 * 1024,
      "Independent complete row-content receipt exceeds its existing bound")
    text
  }
  def expected(schema: Schema, rows: Vector[Record]): Map[String, Int] =
    rows.map(content(schema, _)).groupBy(identity).map { case(k, v) => k -> v.size }
  def bag(t: Table, snapshot: Long, schema: Schema): Map[String, Int] = {
    require(snapshot > 0 && t.schemas().containsKey(schema.schemaId()))
    val reader = IcebergGenerics.read(t).useSnapshot(snapshot).project(schema).build()
    val rows = scala.collection.mutable.ArrayBuffer.empty[String]
    try reader.asScala.foreach { row =>
      require(rows.size < MaxRows, "SDK whole-bag row bound exceeded")
      rows += content(schema, row)
    } finally reader.close()
    rows.groupBy(identity).map { case(k, v) => k -> v.size }.toMap
  }
  def bagFact(values: Map[String, Int]): Vector[JsonNode] =
    values.toVector.sortBy(_._1).map { case(k, v) => obj("content" -> k, "count" -> v) }
  def physicalFiles(t: Table): Vector[JsonNode] = {
    val tasks = RecursiveTypeFixture.boundedScan(t)
    require(tasks.size <= 8 && tasks.forall(_.deletes().isEmpty), "Unexpected whole-file/delete scope")
    val schemas = t.schemas().values().asScala.toVector
    require(schemas.nonEmpty && schemas.size <= MaxSchemas)
    tasks.foreach { task =>
      val local = java.nio.file.Files.createTempFile("uea7b3-metadata-history-", ".parquet")
      try {
        java.nio.file.Files.write(local, RecursiveTypeFixture.boundedBytes(t, task.file().location()))
        val input = org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
          new org.apache.hadoop.fs.Path(local.toUri()), new org.apache.hadoop.conf.Configuration())
        val reader = org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(input)
        try {
          val raw = reader.getFooter().getFileMetaData().getSchema()
          val converted = org.apache.iceberg.parquet.ParquetSchemaUtil.convertAndPrune(raw)
          val all = RecursiveTypeFixture.facts(converted)
          val fields = all.filterNot(f => Set("_row_id", "_last_updated_sequence_number").contains(f.path))
          require(fields.nonEmpty && fields.map(_.id).distinct.size == fields.size,
            "Parquet semantic field IDs are absent or duplicated")
          require(schemas.exists { schema =>
            val actual = RecursiveTypeFixture.facts(schema)
            actual == fields
          }, "Raw Parquet IDs/required/names/types do not match any exact retained write schema")
          val primitive = scala.collection.mutable.Map.empty[Int, String]
          val visitor = new org.apache.iceberg.parquet.ParquetTypeVisitor[java.lang.Integer]() {
            import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType, PrimitiveType, MessageType}
            override def message(t: MessageType, children: java.util.List[java.lang.Integer]): java.lang.Integer = java.lang.Integer.valueOf(0)
            override def struct(t: GroupType, children: java.util.List[java.lang.Integer]): java.lang.Integer = java.lang.Integer.valueOf(0)
            override def list(t: GroupType, child: java.lang.Integer): java.lang.Integer = java.lang.Integer.valueOf(0)
            override def map(t: GroupType, key: java.lang.Integer, value: java.lang.Integer): java.lang.Integer = java.lang.Integer.valueOf(0)
            override def primitive(t: PrimitiveType): java.lang.Integer = {
              require(t.getId() != null && t.getId().intValue() > 0)
              primitive.put(t.getId().intValue(), t.getPrimitiveTypeName().toString()); java.lang.Integer.valueOf(0)
            }
          }
          org.apache.iceberg.parquet.ParquetTypeVisitor.visit(raw, visitor)
          fields.filter(_.kind == "INTEGER").foreach(f => require(primitive.get(f.id).contains("INT32"),
            "A narrow/ordinary INTEGER leaf is not physically standard INT32"))
          fields.filter(_.kind == "STRING").foreach(f => require(primitive.get(f.id).contains("BINARY"),
            "A Json/ordinary STRING leaf is not physically standard Parquet BINARY"))
        } finally reader.close()
      } finally java.nio.file.Files.deleteIfExists(local)
    }
    tasks.map(task => RecursiveTypeFixture.fileFact(task.file()))
  }
  def facts(t: Table): JsonNode = {
    val m = metadata(t)
    val schemas = t.schemas().asScala.toVector.sortBy(_._1.intValue())
    require(schemas.nonEmpty && schemas.size <= MaxSchemas)
    val fullJson = TableMetadataParser.toJson(m)
    require(fullJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 256 * 1024,
      "Bounded metadata history receipt input exceeded")
    val full = mapper.readTree(fullJson)
    val source = m.metadataFileLocation()
    require(source != null && source.startsWith(t.location().stripSuffix("/") + "/metadata/"),
      "Metadata pointer leaves the exact fixture table")
    val bytes = RecursiveTypeFixture.boundedBytes(t, source)
    val sha = java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
    require(t.properties().keySet().asScala.filter(_.startsWith("novarocks.field_domains.")).toSet == Set(DomainProperty),
      "Unexpected declaration namespace in exact owned table")
    obj("table_uuid" -> m.uuid().toString, "table_location" -> t.location(),
      "metadata_file" -> source, "metadata_file_bytes" -> bytes.length, "metadata_file_sha256" -> sha,
      "current_schema_id" -> t.schema().schemaId(), "current_schema_depth" -> fieldDepth(t.schema()),
      "schema_json_container_depth" -> jsonDepth(full.get("schemas")),
      "field_domains_json" -> t.properties().get(DomainProperty),
      "retained_schemas" -> schemas.map { case(id, schema) => obj("schema_id" -> id.intValue(),
        "semantic_depth" -> fieldDepth(schema), "schema_json" -> SchemaParser.toJson(schema),
        "fields" -> RecursiveTypeFixture.facts(schema).map(f =>
          obj("path" -> f.path, "id" -> f.id, "required" -> f.required, "kind" -> f.kind))) },
      "snapshot" -> t.currentSnapshot().snapshotId(), "snapshot_schema_id" -> t.currentSnapshot().schemaId(),
      "references" -> t.refs().asScala.toVector.sortBy(_._1).map { case(name, ref) =>
        obj("name" -> name, "snapshot" -> ref.snapshotId(), "kind" -> (if(ref.isTag()) "tag" else "branch")) },
      "data_files" -> physicalFiles(t))
  }

  // 63 Struct containers plus the primitive leaf: semantic depth exactly 64.
  def depthSchema: Schema = {
    var t: Type = Types.IntegerType.get()
    for(i <- (1 to 63).reverse) t = Types.StructType.of(Types.NestedField.optional(i + 2, "n" + i, t))
    new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "deep", t))
  }
  def depthRow(schema: Schema, id: Long, leaf: Option[Int]): Record = {
    val row = GenericRecord.create(schema); row.setField("id", Long.box(id))
    leaf.foreach { value =>
      var typ = schema.findType("deep")
      val types = scala.collection.mutable.ArrayBuffer.empty[Type]
      while(typ.typeId() == Type.TypeID.STRUCT) { types += typ; typ = typ.asStructType().fields().get(0).`type`() }
      require(types.size == 63 && typ == Types.IntegerType.get())
      var child: AnyRef = Int.box(value)
      types.reverse.foreach { struct =>
        val record = GenericRecord.create(struct.asStructType())
        record.setField(struct.asStructType().fields().get(0).name(), child); child = record
      }
      row.setField("deep", child)
    }; row
  }
  def depthInitialize(ns: String, catalogName: String = "ice_rest"): Unit = {
    val t = create(ns, "metadata_depth", depthSchema,
      schema => domainText(Vector(schema.findField(DeepPath).fieldId() -> "tinyint")), catalogName)
    require(fieldDepth(t.schema()) == 64)
    append(t, Vector(depthRow(t.schema(), 1, Some(-128)), depthRow(t.schema(), 1, Some(-128)),
      depthRow(t.schema(), 2, None)), "depth-initial")
    createTag(t, "depth_initial")
    val expectedBag = expected(t.schema(), Vector(depthRow(t.schema(), 1, Some(-128)),
      depthRow(t.schema(), 1, Some(-128)), depthRow(t.schema(), 2, None)))
    require(bag(t, t.currentSnapshot().snapshotId(), t.schema()) == expectedBag)
    val f = facts(t)
    require(f.get("schema_json_container_depth").intValue() > 128,
      "Fixture did not cross serde's structural recursion boundary")
    RecursiveTypeFixture.boundedEmit(obj("record" -> "metadata_depth_initialized", "facts" -> f,
      "bag" -> bagFact(expectedBag)))
    println("METADATA_DEPTH_READY")
  }
  def depthObserveAppend(ns: String, catalogName: String = "ice_rest"): Unit = {
    val t = load(ns, "metadata_depth", catalogName)
    val initial = tag(t, "depth_initial")
    require(fieldDepth(t.schema()) == 64 && t.currentSnapshot().snapshotId() != initial &&
      t.currentSnapshot().parentId() != null && t.currentSnapshot().parentId().longValue() == initial,
      "Native append did not retain the exact depth64 schema and snapshot parent")
    val currentBag = expected(t.schema(), Vector(depthRow(t.schema(), 1, Some(-128)),
      depthRow(t.schema(), 1, Some(-128)), depthRow(t.schema(), 2, None), depthRow(t.schema(), 3, None)))
    require(bag(t, t.currentSnapshot().snapshotId(), t.schema()) == currentBag)
    val copy = load(ns, "metadata_depth_copy", catalogName)
    require(metadata(copy).uuid() != metadata(t).uuid() && fieldDepth(copy.schema()) == 64,
      "CTAS did not create its own exact depth64 table")
    require(copy.properties().get(DomainProperty) == domainText(Vector(copy.schema().findField(DeepPath).fieldId() -> "tinyint")),
      "CTAS lost the actual leaf's declared domain")
    require(bag(copy, copy.currentSnapshot().snapshotId(), copy.schema()) == currentBag,
      "SDK whole content bags of source and depth64 CTAS differ")
    RecursiveTypeFixture.boundedEmit(obj("record" -> "metadata_depth_native_committed",
      "source" -> facts(t), "ctas" -> facts(copy), "bag" -> bagFact(currentBag)))
    println("METADATA_DEPTH_NATIVE_COMMIT_OBSERVED")
  }
  def depthRetainHistory(ns: String, catalogName: String = "ice_rest"): Unit = {
    val t = load(ns, "metadata_depth", catalogName)
    require(fieldDepth(t.schema()) == 64)
    val before = facts(t); val id = t.schema().schemaId(); val uuid = metadata(t).uuid()
    val oldDomains = t.properties().get(DomainProperty)
    createTag(t, "depth_before_drop")
    t.updateSchema().deleteColumn("deep").commit(); t.refresh()
    require(fieldDepth(t.schema()) == 1 && metadata(t).uuid() == uuid &&
      t.properties().get(DomainProperty) == oldDomains && t.schemas().containsKey(Int.box(id)) &&
      fieldDepth(t.schemas().get(Int.box(id))) == 64,
      "Metadata-only drop lost the accurate depth64 historical schema/domain")
    append(t, Vector({ val r = GenericRecord.create(t.schema()); r.setField("id", Long.box(4)); r }), "depth-shallow")
    val current = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(current == Map("{\"id\":1}" -> 2, "{\"id\":2}" -> 1, "{\"id\":3}" -> 1, "{\"id\":4}" -> 1))
    val historical = snapshotSchema(t, tag(t, "depth_initial"))
    require(fieldDepth(historical) == 64)
    val oldBag = expected(historical, Vector(depthRow(historical, 1, Some(-128)),
      depthRow(historical, 1, Some(-128)), depthRow(historical, 2, None)))
    require(bag(t, tag(t, "depth_initial"), historical) == oldBag)
    val after = facts(t)
    require(after.get("schema_json_container_depth").intValue() > 128 &&
      before.get("metadata_file").asText() != after.get("metadata_file").asText(),
      "Retained depth64 metadata or actual commit pointer did not change as expected")
    RecursiveTypeFixture.boundedEmit(obj("record" -> "metadata_depth_retained_history", "before" -> before,
      "after" -> after, "current_bag" -> bagFact(current), "historical_bag" -> bagFact(oldBag)))
    println("METADATA_DEPTH_HISTORY_READY")
  }

  def historySchema: Schema = new Schema(
    Types.NestedField.required(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "payload", Types.StructType.of(
      Types.NestedField.optional(3, "tiny", Types.IntegerType.get()),
      Types.NestedField.optional(4, "note", Types.StringType.get()),
      Types.NestedField.optional(5, "reused", Types.IntegerType.get()))),
    Types.NestedField.optional(6, "plain", Types.StringType.get()),
    Types.NestedField.optional(7, "small", Types.IntegerType.get()))
  def historyDomains(schema: Schema): String = domainText(Vector(
    schema.findField("payload.tiny").fieldId() -> "tinyint",
    schema.findField("payload.note").fieldId() -> "json",
    schema.findField("payload.reused").fieldId() -> "smallint",
    schema.findField("small").fieldId() -> "smallint"))
  def historyRow(schema: Schema, id: Int, stage: String): Record = {
    val renamed = schema.findField("payload.tiny_renamed") != null
    val wide = schema.findType(if(renamed) "payload.tiny_renamed" else "payload.tiny") == Types.LongType.get()
    val r = GenericRecord.create(schema); r.setField("id", Long.box(id))
    val text = id match {
      case 1 => "{\"b\":2,\"a\":1}"
      case 2 => "{ \"a\" : 1 }"
      case 3 | 4 => "null"
      case 5 => "[2,1]"
      case 6 => "\"new\""
      case 7 => "{\"wide\":true}"
    }
    r.setField("plain", text)
    val small: Option[Int] = id match {
      case 1 | 6 => Some(32767); case 2 => Some(-32768); case 3 => None
      case 4 => Some(0); case 5 => Some(7); case 7 => Some(32768)
    }
    r.setField(if(renamed) "small_renamed" else "small", small.map[java.lang.Number](v => if(wide) Long.box(v.toLong) else Int.box(v)).orNull)
    if(id != 3) {
      val p = GenericRecord.create(schema.findType("payload").asStructType())
      val tiny: Option[Int] = id match {
        case 1 | 6 => Some(127); case 2 => Some(-128); case 4 => None
        case 5 => Some(0); case 7 => Some(128)
      }
      p.setField(if(renamed) "tiny_renamed" else "tiny", tiny.map[java.lang.Number](v => if(wide) Long.box(v.toLong) else Int.box(v)).orNull)
      p.setField("note", text)
      val reused: Option[Int] = if(Set("readded", "promoted").contains(stage) && id <= 5) None else id match {
        case 1 => Some(32767); case 2 => Some(-32768); case 4 => None
        case 5 => Some(7); case 6 => Some(128); case 7 => Some(32768)
      }
      p.setField("reused", reused.map(Int.box).orNull); r.setField("payload", p)
    }; r
  }
  def rows(schema: Schema, stage: String): Vector[Record] = {
    val last = stage match { case "initial" => 4; case "renamed" => 5; case "readded" => 6; case "promoted" => 7 }
    (Vector(1, 1) ++ (2 to last)).map(id => historyRow(schema, id, stage))
  }
  def assertHistoryDomains(t: Table): String = {
    val initial = snapshotSchema(t, tag(t, "domain_initial"))
    val exact = historyDomains(initial)
    require(t.properties().get(DomainProperty) == exact &&
      t.properties().keySet().asScala.filter(_.startsWith("novarocks.field_domains.")).toSet == Set(DomainProperty),
      "Evolution silently changed/duplicated the frozen declarations")
    exact
  }
  def historyInitialize(ns: String): Unit = {
    val t = create(ns, HistoryName, historySchema, historyDomains)
    append(t, rows(t.schema(), "initial"), "domain-initial"); createTag(t, "domain_initial")
    assertHistoryDomains(t)
    val actual = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(actual == expected(t.schema(), rows(t.schema(), "initial")))
    RecursiveTypeFixture.boundedEmit(obj("record" -> "field_domain_history_initialized",
      "facts" -> facts(t), "bag" -> bagFact(actual)))
    println("FIELD_DOMAIN_HISTORY_READY")
  }
  def historyRenameReorder(ns: String): Unit = {
    val t = load(ns, HistoryName); val before = facts(t); val uuid = metadata(t).uuid()
    val original = RecursiveTypeFixture.facts(t.schema())
    t.updateSchema().renameColumn("payload.tiny", "tiny_renamed").renameColumn("small", "small_renamed")
      .moveFirst("plain").moveFirst("payload.note").commit(); t.refresh()
    val renamed = RecursiveTypeFixture.facts(t.schema())
    require(renamed.map(f => (f.id, f.required, f.kind)).toSet == original.map(f => (f.id, f.required, f.kind)).toSet &&
      t.schema().columns().get(0).name() == "plain" &&
      t.schema().findType("payload").asStructType().fields().get(0).name() == "note" && metadata(t).uuid() == uuid,
      "Rename/reorder changed provider identity/required/family instead of just names/order")
    assertHistoryDomains(t)
    append(t, Vector(historyRow(t.schema(), 5, "renamed")), "domain-renamed")
    createTag(t, "domain_renamed")
    val actual = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(actual == expected(t.schema(), rows(t.schema(), "renamed")))
    RecursiveTypeFixture.boundedEmit(obj("record" -> "field_domain_history_renamed",
      "before" -> before, "after" -> facts(t), "bag" -> bagFact(actual)))
    println("FIELD_DOMAIN_RENAME_READY")
  }
  def historyDropReadd(ns: String): Unit = {
    val t = load(ns, HistoryName); val before = facts(t); val uuid = metadata(t).uuid()
    val oldId = t.schema().findField("payload.reused").fieldId()
    t.updateSchema().deleteColumn("payload.reused").commit(); t.refresh()
    t.updateSchema().addColumn("payload", "reused", Types.IntegerType.get()).commit(); t.refresh()
    val newId = t.schema().findField("payload.reused").fieldId()
    require(newId > oldId && metadata(t).uuid() == uuid, "Same-name re-add reused the old field identity")
    val exact = assertHistoryDomains(t); val fields = mapper.readTree(exact).get("fields")
    require(fields.has(oldId.toString) && !fields.has(newId.toString),
      "A foreign new INTEGER field inherited the dropped narrow domain")
    append(t, Vector(historyRow(t.schema(), 6, "readded")), "domain-readded")
    createTag(t, "domain_before_promotion")
    val actual = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(actual == expected(t.schema(), rows(t.schema(), "readded")))
    val renamed = snapshotSchema(t, tag(t, "domain_renamed"))
    require(renamed.findField("payload.reused").fieldId() == oldId &&
      bag(t, tag(t, "domain_renamed"), renamed) == expected(renamed, rows(renamed, "renamed")),
      "Drop/re-add corrupted the tagged old-ID complete bag")
    RecursiveTypeFixture.boundedEmit(obj("record" -> "field_domain_history_readded", "old_id" -> oldId,
      "new_id" -> newId, "before" -> before, "after" -> facts(t), "bag" -> bagFact(actual),
      "historical_bag" -> bagFact(bag(t, tag(t, "domain_renamed"), renamed))))
    println("FIELD_DOMAIN_READD_READY")
  }
  def historyPromote(ns: String): Unit = {
    val t = load(ns, HistoryName); val before = facts(t); val uuid = metadata(t).uuid()
    val old = t.schema(); val tinyId = old.findField("payload.tiny_renamed").fieldId()
    val smallId = old.findField("small_renamed").fieldId()
    t.updateSchema().updateColumn("payload.tiny_renamed", Types.LongType.get())
      .updateColumn("small_renamed", Types.LongType.get()).commit(); t.refresh()
    require(t.schema().findField("payload.tiny_renamed").fieldId() == tinyId &&
      t.schema().findField("small_renamed").fieldId() == smallId && metadata(t).uuid() == uuid &&
      t.schema().findType("payload.tiny_renamed") == Types.LongType.get() &&
      t.schema().findType("small_renamed") == Types.LongType.get(),
      "Legal INT to LONG promotion changed exact field identity")
    assertHistoryDomains(t)
    append(t, Vector(historyRow(t.schema(), 7, "promoted")), "domain-promoted")
    val actual = bag(t, t.currentSnapshot().snapshotId(), t.schema())
    require(actual == expected(t.schema(), rows(t.schema(), "promoted")))
    val historical = snapshotSchema(t, tag(t, "domain_before_promotion"))
    require(historical.findField("payload.tiny_renamed").fieldId() == tinyId &&
      historical.findType("payload.tiny_renamed") == Types.IntegerType.get() &&
      historical.findField("small_renamed").fieldId() == smallId &&
      historical.findType("small_renamed") == Types.IntegerType.get() &&
      bag(t, tag(t, "domain_before_promotion"), historical) == expected(historical, rows(historical, "readded")),
      "Current LONG promotion lost the accurately declared old INT snapshot")
    RecursiveTypeFixture.boundedEmit(obj("record" -> "field_domain_history_promoted", "before" -> before,
      "after" -> facts(t), "bag" -> bagFact(actual),
      "historical_bag" -> bagFact(bag(t, tag(t, "domain_before_promotion"), historical))))
    println("FIELD_DOMAIN_PROMOTION_READY")
  }
}
