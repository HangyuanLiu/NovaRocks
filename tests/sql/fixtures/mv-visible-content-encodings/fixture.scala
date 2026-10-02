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

// Loaded after iceberg-delete-applicability/generate.scala.
// Parquet page encodings are observed here. Arrow DictionaryArray carriers are
// a separate execution-layer contract and are not claimed by this fixture.
object VisibleContentEncodingsFixture {
  import DeleteApplicabilityFixture._
  val Visible = "label,items,attrs,rec"
  def source(namespace: String): Table = {
    require(namespace.matches("ns_[a-zA-Z0-9_]+"))
    val t = Spark3Util.loadIcebergTable(org.apache.spark.sql.SparkSession.active, "ice_rest." + namespace + ".encoded_source")
    t.refresh()
    require(metadata(t).formatVersion() == 3 && t.properties().get("write.row-lineage") == "true", "Source contract changed")
    t
  }
  def scan(t: Table): Vector[FileScanTask] = {
    val planned = t.newScan().planFiles()
    try planned.asScala.toVector finally planned.close()
  }
  def template(t: Table, id: Long, kind: String): Record = {
    val r = GenericRecord.create(t.schema()); r.setField("id", Long.box(id))
    val nested = GenericRecord.create(t.schema().findType("rec").asStructType())
    val attrs = new java.util.LinkedHashMap[String, java.lang.Long]()
    kind match {
      case "null" => ()
      case "empty" =>
        r.setField("label", ""); r.setField("items", new java.util.ArrayList[java.lang.Long]())
        r.setField("attrs", attrs); r.setField("rec", nested)
      case "same" | "reordered" | "changed" =>
        r.setField("label", "shared")
        val items = if (kind == "reordered") java.util.Arrays.asList[java.lang.Long](null, Long.box(1L))
          else java.util.Arrays.asList[java.lang.Long](Long.box(1L), null)
        attrs.put("a", Long.box(1L)); attrs.put("b", null)
        nested.setField("code", Long.box(if (kind == "changed") 8L else 7L)); nested.setField("note", "nested")
        r.setField("items", items); r.setField("attrs", attrs); r.setField("rec", nested)
      case _ => throw new IllegalArgumentException("Unknown visible content template")
    }
    r
  }
  def write(t: Table, rows: Seq[Record], dictionary: Boolean, label: String): DataFile = {
    val out = t.io().newOutputFile(t.location().stripSuffix("/") + "/data/sdk-" + label + "-" + java.util.UUID.randomUUID() + ".parquet")
    val writer = new GenericAppenderFactory(t.schema(), t.spec())
      .set("write.metadata.metrics.default", "full")
      .set("parquet.enable.dictionary", dictionary.toString)
      .set("write.parquet.dict-size-bytes", "1048576")
      .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out), FileFormat.PARQUET, partition(t, 1))
    try rows.foreach(writer.write) finally writer.close()
    writer.toDataFile()
  }
  def labelEncoding(t: Table, path: String, dictionary: Boolean): JsonNode = {
    val local = java.nio.file.Files.createTempFile("uea7b3-encoding-", ".parquet")
    try {
      java.nio.file.Files.write(local, bytes(t.io(), path))
      val input = org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
        new org.apache.hadoop.fs.Path(local.toUri()), new org.apache.hadoop.conf.Configuration())
      val reader = org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(input)
      try {
        val column = reader.getFooter().getFileMetaData().getSchema().getColumns().asScala
          .filter(_.getPath().toVector == Vector("label"))
        require(column.size == 1, "Parquet label column is absent or ambiguous")
        val footer = reader.getRowGroups().asScala.toVector.map { group =>
          val labels = group.getColumns().asScala.filter(_.getPath().toDotString() == "label").toVector
          require(labels.size == 1, "Parquet footer label column is absent or ambiguous")
          labels.head.getEncodings().asScala.toVector.map(_.toString).sorted
        }
        val pages = scala.collection.mutable.ArrayBuffer.empty[String]
        var dictionaries = 0
        var group = reader.readNextRowGroup()
        while (group != null) {
          val pageReader = group.getPageReader(column.head)
          if (pageReader.readDictionaryPage() != null) dictionaries += 1
          var page = pageReader.readPage()
          while (page != null) {
            val encoding = page match {
              case p: org.apache.iceberg.shaded.org.apache.parquet.column.page.DataPageV1 => p.getValueEncoding().toString
              case p: org.apache.iceberg.shaded.org.apache.parquet.column.page.DataPageV2 => p.getDataEncoding().toString
              case _ => throw new IllegalStateException("Unknown Parquet data page version")
            }
            pages += encoding; page = pageReader.readPage()
          }
          group = reader.readNextRowGroup()
        }
        require(pages.nonEmpty, "Parquet label has no actual data pages")
        if (dictionary) require(dictionaries > 0 && pages.forall(_.contains("DICTIONARY")) && footer.exists(_.exists(_.contains("DICTIONARY"))), "Requested dictionary file did not contain actual dictionary-encoded label pages")
        else require(dictionaries == 0 && pages.forall(_ == "PLAIN") && footer.forall(encodings => encodings.forall(encoding => !encoding.contains("DICTIONARY"))), "Requested plain file did not contain actual plain label pages")
        obj("path" -> path, "column" -> "label", "footer_encodings" -> footer, "dictionary_pages" -> dictionaries,
          "data_page_encodings" -> pages.toVector, "observed_encoding" -> (if (dictionary) "ParquetDictionary" else "ParquetPlain"),
          "arrow_dictionary_array" -> "NotObserved")
      } finally reader.close()
    } finally java.nio.file.Files.deleteIfExists(local)
  }
  def initialize(namespace: String): Unit = {
    val session = org.apache.spark.sql.SparkSession.active
    session.sql("CREATE TABLE ice_rest." + namespace + ".encoded_source (id BIGINT NOT NULL, label STRING, items ARRAY<BIGINT>, attrs MAP<STRING,BIGINT>, rec STRUCT<code:BIGINT,note:STRING>) USING iceberg TBLPROPERTIES ('format-version'='3','write.row-lineage'='true')")
    val t = source(namespace)
    def rows(base: Long) = (0 until 256).map(i => template(t, base + i, "same")) ++
      Seq(template(t,base+256,"reordered"),template(t,base+257,"null"),template(t,base+258,"empty"))
    val dictionary = write(t,rows(1L),true,"dictionary")
    val plain = write(t,rows(1001L),false,"plain")
    val encodings = Vector(labelEncoding(t,dictionary.location(),true),labelEncoding(t,plain.location(),false))
    t.newAppend().appendFile(dictionary).appendFile(plain).commit(); t.refresh()
    require(scan(t).size == 2 && scan(t).forall(_.deletes().isEmpty), "Initial source files differ from the frozen fixture")
    emit(obj("record" -> "visible_encoding_source", "stage" -> "initial", "snapshot" -> t.currentSnapshot().snapshotId(),
      "schema" -> t.schema().toString, "files" -> Vector(describe(dictionary),describe(plain)), "encodings" -> encodings))
    observeSource(namespace,"initial")
    println("VISIBLE_CONTENT_ENCODINGS_READY")
  }
  def mutate(namespace: String): Unit = {
    val t = source(namespace); val before = t.currentSnapshot().snapshotId()
    val dictionary = scan(t).filter(_.file().location().contains("/sdk-dictionary-")).map(_.file())
    require(dictionary.size == 1 && dictionary.head.recordCount() == 259, "Dictionary source file is not exact")
    val fresh = write(t,Seq(template(t,2001L,"changed"),template(t,2002L,"changed"),template(t,2003L,"reordered")),false,"plain-delta")
    val encoding = labelEncoding(t,fresh.location(),false)
    val vectors = dv(t,Seq((dictionary.head.location(),0L)))
    require(vectors.size == 1 && vectors.head.format() == FileFormat.PUFFIN && vectors.head.recordCount() == 1, "Retraction did not write one real source DV")
    t.newRowDelta().addRows(fresh).addDeletes(vectors.head).commit(); t.refresh()
    require(t.currentSnapshot().snapshotId() != before, "Source delta did not publish a new endpoint")
    require(scan(t).exists(task => task.file().location() == dictionary.head.location() && task.deletes().asScala.exists(_.location() == vectors.head.location())), "Source DV is not actually attached")
    emit(obj("record" -> "visible_encoding_source", "stage" -> "delta", "from_snapshot" -> before,
      "snapshot" -> t.currentSnapshot().snapshotId(), "added" -> describe(fresh), "deletes" -> vectors.map(describe), "encodings" -> Vector(encoding),
      "negative_occurrences" -> 1, "positive_occurrences" -> 3))
    observeSource(namespace,"delta")
    println("VISIBLE_CONTENT_ENCODINGS_CHANGED")
  }
  def expected(stage: String): Map[String,Int] = {
    val session = org.apache.spark.sql.SparkSession.active
    val common = "'attrs',map('a',CAST(1 AS BIGINT),'b',CAST(NULL AS BIGINT)),"
    val expressions = Vector(
      "named_struct('label','shared','items',array(CAST(1 AS BIGINT),CAST(NULL AS BIGINT))," + common + "'rec',named_struct('code',CAST(7 AS BIGINT),'note','nested'))",
      "named_struct('label','shared','items',array(CAST(NULL AS BIGINT),CAST(1 AS BIGINT))," + common + "'rec',named_struct('code',CAST(7 AS BIGINT),'note','nested'))",
      "named_struct('label',CAST(NULL AS STRING),'items',CAST(NULL AS ARRAY<BIGINT>),'attrs',CAST(NULL AS MAP<STRING,BIGINT>),'rec',CAST(NULL AS STRUCT<code:BIGINT,note:STRING>))",
      "named_struct('label','','items',CAST(array() AS ARRAY<BIGINT>),'attrs',CAST(map() AS MAP<STRING,BIGINT>),'rec',named_struct('code',CAST(NULL AS BIGINT),'note',CAST(NULL AS STRING)))",
      "named_struct('label','shared','items',array(CAST(1 AS BIGINT),CAST(NULL AS BIGINT))," + common + "'rec',named_struct('code',CAST(8 AS BIGINT),'note','nested'))")
    val counts = if (stage == "initial") Vector(512,2,2,2,0) else Vector(511,3,2,2,2)
    expressions.zip(counts).filter(_._2 > 0).map { case (expression,count) =>
      val text = session.sql("SELECT to_json(" + expression + ",map('ignoreNullFields','false')) AS content").head().getString(0)
      text -> count
    }.toMap
  }
  def bag(namespace: String, table: String): Map[String,Int] = {
    val rows = org.apache.spark.sql.SparkSession.active.sql("SELECT to_json(named_struct('label',label,'items',items,'attrs',attrs,'rec',rec),map('ignoreNullFields','false')) AS content FROM ice_rest." + namespace + "." + table).collect().toVector.map(_.getString(0))
    rows.groupBy(identity).map { case (content,occurrences) => content -> occurrences.size }
  }
  def observeSource(namespace: String, stage: String): Unit = {
    val actual = bag(namespace,"encoded_source"); val wanted = expected(stage)
    require(actual == wanted, "Independent source full-content bag differs from the expected endpoint")
    emit(obj("record" -> "visible_content_source_bag", "stage" -> stage, "bag" -> actual.toVector.sortBy(_._1).map { case (content,count) => obj("content" -> content,"count" -> count) }))
  }
  def observe(namespace: String, stage: String): Unit = {
    val session = org.apache.spark.sql.SparkSession.active
    val t = source(namespace)
    val target = Spark3Util.loadIcebergTable(session,"ice_rest." + namespace + ".encoded_mv"); target.refresh()
    val wantedStage = if (stage == "initial") "initial" else "delta"
    val wanted = expected(wantedStage); val actual = bag(namespace,"encoded_mv"); val sourceBag = bag(namespace,"encoded_source")
    require(actual == wanted && sourceBag == wanted, "Independent source/MV full-content bags differ")
    val names = target.schema().columns().asScala.map(_.name()).toVector
    require(names == Vector("label","items","attrs","rec"), "Target persisted hidden or unexpected visible columns")
    require(session.table("ice_rest." + namespace + ".encoded_mv").schema.fields.map(_.dataType).toVector ==
      session.table("ice_rest." + namespace + ".encoded_source").select("label","items","attrs","rec").schema.fields.map(_.dataType).toVector, "Target visible recursive types changed")
    val tasks = scan(target)
    if (stage == "incremental") require(tasks.exists(_.deletes().asScala.exists(_.format() == FileFormat.PUFFIN)), "Incremental visible retraction did not write a target DV")
    if (stage == "full") require(tasks.forall(_.deletes().isEmpty), "Full rebuild retained target deletes")
    emit(obj("record" -> "visible_content_mv_bag", "stage" -> stage, "source_snapshot" -> t.currentSnapshot().snapshotId(),
      "target_snapshot" -> target.currentSnapshot().snapshotId(), "visible_columns" -> names, "schema" -> target.schema().toString,
      "bag" -> actual.toVector.sortBy(_._1).map { case (content,count) => obj("content" -> content,"count" -> count) },
      "files" -> tasks.map(task => obj("data" -> describe(task.file()),"deletes" -> task.deletes().asScala.toVector.map(describe))),
      "parquet_encodings" -> tasks.map(task => {
        val path = task.file().location()
        // The target writer owns its encoding choice. Source dictionary/plain
        // assertions above are the controlled input proof.
        obj("path" -> path,"encoding_claim" -> "NotRequiredForTarget")
      })))
    println("VISIBLE_CONTENT_ENCODINGS_OBSERVED")
  }
  def scalarExpected(stage: String): Map[String,Int] = {
    val session = org.apache.spark.sql.SparkSession.active
    val values = if (stage == "initial") Vector(("'shared'",514),("CAST(NULL AS STRING)",2),("''",2))
      else Vector(("'shared'",513),("'added'",3),("CAST(NULL AS STRING)",2),("''",2))
    values.map { case (expression,count) =>
      session.sql("SELECT to_json(named_struct('label'," + expression + "),map('ignoreNullFields','false')) AS content").head().getString(0) -> count
    }.toMap
  }
  def scalarBag(namespace: String, table: String): Map[String,Int] = {
    val rows = org.apache.spark.sql.SparkSession.active.sql("SELECT to_json(named_struct('label',label),map('ignoreNullFields','false')) AS content FROM ice_rest." + namespace + "." + table).collect().toVector.map(_.getString(0))
    rows.groupBy(identity).map { case (content,occurrences) => content -> occurrences.size }
  }
  def scalarMutate(namespace: String): Unit = {
    val t = source(namespace); val before = t.currentSnapshot().snapshotId()
    val dictionary = scan(t).filter(_.file().location().contains("/sdk-dictionary-")).map(_.file())
    require(dictionary.size == 1 && dictionary.head.recordCount() == 259, "Dictionary source file is not exact")
    // Unlike the complex-output delta, scalar content must retain a negative
    // shared key and a separate positive added key after weight consolidation.
    val added = Seq(template(t,3001L,"changed"),template(t,3002L,"changed"),template(t,3003L,"reordered"))
    added.foreach(_.setField("label","added"))
    val fresh = write(t,added,false,"plain-scalar-delta")
    val encoding = labelEncoding(t,fresh.location(),false)
    val vectors = dv(t,Seq((dictionary.head.location(),0L)))
    require(vectors.size == 1 && vectors.head.format() == FileFormat.PUFFIN && vectors.head.recordCount() == 1, "Scalar retraction did not write one real source DV")
    t.newRowDelta().addRows(fresh).addDeletes(vectors.head).commit(); t.refresh()
    require(t.currentSnapshot().snapshotId() != before, "Scalar source delta did not publish a new endpoint")
    require(scan(t).exists(task => task.file().location() == dictionary.head.location() && task.deletes().asScala.exists(_.location() == vectors.head.location())), "Scalar source DV is not actually attached")
    val actual = scalarBag(namespace,"encoded_source"); val wanted = scalarExpected("delta")
    require(actual == wanted, "Independent scalar source bag differs from its exact endpoint")
    emit(obj("record" -> "scalar_dictionary_source_delta", "from_snapshot" -> before,
      "snapshot" -> t.currentSnapshot().snapshotId(), "added" -> describe(fresh), "deletes" -> vectors.map(describe),
      "encodings" -> Vector(encoding), "net_removed_shared" -> 1, "net_added_added" -> 3,
      "bag" -> actual.toVector.sortBy(_._1).map { case (content,count) => obj("content" -> content,"count" -> count) }))
    println("SCALAR_DICTIONARY_CHANGED")
  }
  def scalarObserve(namespace: String, stage: String): Unit = {
    val session = org.apache.spark.sql.SparkSession.active
    val t = source(namespace)
    val target = Spark3Util.loadIcebergTable(session,"ice_rest." + namespace + ".dictionary_mv"); target.refresh()
    val wanted = scalarExpected(if (stage == "initial") "initial" else "delta")
    val actual = scalarBag(namespace,"dictionary_mv"); val sourceBag = scalarBag(namespace,"encoded_source")
    require(actual == wanted && sourceBag == wanted, "Independent scalar source/MV complete bags differ")
    require(target.schema().columns().asScala.map(_.name()).toVector == Vector("label"), "Scalar target persisted hidden or unexpected columns")
    require(session.table("ice_rest." + namespace + ".dictionary_mv").schema.fields.map(_.dataType).toVector ==
      session.table("ice_rest." + namespace + ".encoded_source").select("label").schema.fields.map(_.dataType).toVector, "Scalar target visible type changed")
    val tasks = scan(target)
    if (stage == "incremental") require(tasks.exists(_.deletes().asScala.exists(_.format() == FileFormat.PUFFIN)), "Scalar negative key did not write a real target DV")
    if (stage == "full") require(tasks.forall(_.deletes().isEmpty), "Scalar full rebuild retained target deletes")
    emit(obj("record" -> "scalar_dictionary_mv_bag", "stage" -> stage,
      "source_snapshot" -> t.currentSnapshot().snapshotId(), "target_snapshot" -> target.currentSnapshot().snapshotId(),
      "schema" -> target.schema().toString, "arrow_dictionary_array" -> "NotObserved",
      "bag" -> actual.toVector.sortBy(_._1).map { case (content,count) => obj("content" -> content,"count" -> count) },
      "files" -> tasks.map(task => obj("data" -> describe(task.file()),"deletes" -> task.deletes().asScala.toVector.map(describe)))))
    println("SCALAR_DICTIONARY_OBSERVED")
  }

}

// Draft addition to fixture.scala. Loaded after generate.scala and the existing fixture.
// Separate tables preserve every existing encoding-fixture expectation.
object RecursiveTypeFixture {
  import DeleteApplicabilityFixture._
  import org.apache.iceberg.types.Types
  import org.apache.iceberg.catalog.TableIdentifier
  import java.util.{ArrayList, LinkedHashMap}
  val MaxRows=1000
  val MaxFiles=64
  val MaxFields=256
  val MaxSchemaBytes=256*1024
  val MaxFileBytes=16L*1024*1024
  def boundedEmit(n: JsonNode): Unit = {
    val text=mapper.writeValueAsString(n)
    require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 256*1024,"Recursive receipt byte budget exceeded")
    println("UEA4G_RECEIPT "+text)
  }
  def schemaJson(t: Table): String = {
    val text=SchemaParser.toJson(t.schema())
    require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= MaxSchemaBytes,"Recursive schema byte budget exceeded"); text
  }
  def boundedScan(t: Table): Vector[FileScanTask] = {
    val planned=t.newScan().useSnapshot(t.currentSnapshot().snapshotId()).planFiles()
    val out=scala.collection.mutable.ArrayBuffer.empty[FileScanTask]
    try planned.asScala.foreach { task =>
      require(out.size < MaxFiles && task.deletes().size() <= MaxFiles,"Recursive scan file budget exceeded")
      require(task.file().fileSizeInBytes()>0 && task.file().fileSizeInBytes()<=MaxFileBytes && task.file().recordCount()>=0 && task.file().recordCount()<=MaxRows,"Recursive data-file budget exceeded")
      require(task.file().location().startsWith(t.location().stripSuffix("/")+"/"),"Recursive data file leaves its exact private table")
      task.deletes().asScala.foreach(d => require(d.fileSizeInBytes()>0 && d.fileSizeInBytes()<=MaxFileBytes && d.recordCount()>=0 && d.recordCount()<=MaxRows && d.location().startsWith(t.location().stripSuffix("/")+"/"),"Recursive delete-file budget/scope exceeded"))
      out+=task
    } finally planned.close()
    require(out.map(_.file().location()).distinct.size==out.size,"Recursive whole-file scan repeats a data file")
    out.toVector
  }
  def boundedBytes(t: Table,path: String): Array[Byte] = {
    require(path.startsWith(t.location().stripSuffix("/")+"/"),"Recursive object leaves its private table")
    val input=t.io().newInputFile(path); val length=input.getLength
    require(length>0 && length<=MaxFileBytes,"Recursive object exceeds byte budget")
    val in=input.newStream(); val out=new java.io.ByteArrayOutputStream()
    try { val b=new Array[Byte](8192); var n=in.read(b)
      while(n>=0) { if(n>0) { require(out.size().toLong+n<=MaxFileBytes,"Recursive stream exceeds byte budget"); out.write(b,0,n) }; n=in.read(b) }
      val content=out.toByteArray; require(content.length.toLong==length,"Recursive object length changed during read"); content
    } finally in.close()
  }
  def fileFact(f: ContentFile[_]): JsonNode = {
    val n=obj("path"->f.location(),"content"->f.content().toString,"format"->f.format().toString,
      "spec_id"->f.specId(),"record_count"->f.recordCount(),"file_size"->f.fileSizeInBytes())
    def optionalLong(v: java.lang.Long): Any = if(v==null) null else v.longValue()
    n.set[JsonNode]("data_sequence",json(optionalLong(f.dataSequenceNumber())))
    n.set[JsonNode]("file_sequence",json(optionalLong(f.fileSequenceNumber())))
    f match { case d: DeleteFile =>
      n.set[JsonNode]("referenced_data_file",json(d.referencedDataFile()))
      n.set[JsonNode]("content_offset",json(optionalLong(d.contentOffset())))
      n.set[JsonNode]("content_size",json(optionalLong(d.contentSizeInBytes())))
      n.set[JsonNode]("equality_ids",json(d.equalityFieldIds()))
      case _ => ()
    }; n
  }
  def deletes(tasks: Vector[FileScanTask]): Vector[DeleteFile] = {
    val out=scala.collection.mutable.LinkedHashMap.empty[(String,String,String,String),DeleteFile]
    tasks.foreach(_.deletes().asScala.foreach { d =>
      val key=(d.location(),String.valueOf(d.contentOffset()),String.valueOf(d.contentSizeInBytes()),String.valueOf(d.referencedDataFile()))
      out.get(key).foreach(old => require(old.recordCount()==d.recordCount() && old.fileSizeInBytes()==d.fileSizeInBytes(),"Repeated delete reference has conflicting facts"))
      require(out.contains(key) || out.size<MaxFiles,"Recursive distinct delete-file budget exceeded"); out.put(key,d)
    }); out.values.toVector
  }
  def summary(t: Table): JsonNode = {
    val pairs=t.currentSnapshot().summary().asScala.toVector.sortBy(_._1)
    require(pairs.size<=64 && pairs.forall { case(k,v)=>k.length<=256 && v.length<=256*1024 },"Recursive snapshot summary budget exceeded")
    obj(pairs.map { case(k,v)=>(k,v:Any) }: _*)
  }
  def countSummary(t: Table,key: String): Long = {
    val text=Option(t.currentSnapshot().summary().get(key)).getOrElse(throw new IllegalStateException("Required exact summary total absent: "+key))
    require(text.matches("0|[1-9][0-9]{0,18}"),"Invalid exact snapshot count: "+key)
    val count=java.lang.Long.parseLong(text); require(count>=0,"Negative exact snapshot count"); count
  }
  val Visible = "label,payload,ordered,js,smalls"
  val VisibleNames = Vector("label","payload","ordered","js","smalls")
  val Source = "recursive_source"
  val Target = "recursive_mv"
  val Ddl = "recursive_ddl"
  val Ctas = "recursive_ctas"
  def session = {
    require(IcebergBuild.version()=="1.11.0","The recursive oracle requires Iceberg 1.11.0")
    org.apache.spark.sql.SparkSession.active
  }
  def table(ns: String, name: String): Table = {
    require(ns.length<=64 && ns.matches("[a-zA-Z0-9_]+") && Set(Source,Target,Ddl,Ctas).contains(name))
    val t = Spark3Util.loadIcebergTable(session,s"ice_rest.$ns.$name"); t.refresh(); t
  }
  // SDK create may assign fresh IDs. Freeze the actual returned schema, never
  // treat requested IDs as provider authority. Source has a preceding id column.
  def sourceSchema = new Schema(
    Types.NestedField.required(100,"id",Types.LongType.get()),
    Types.NestedField.optional(101,"label",Types.StringType.get()),
    Types.NestedField.optional(200,"payload",Types.StructType.of(
      Types.NestedField.required(201,"items",Types.ListType.ofOptional(202,Types.IntegerType.get())),
      Types.NestedField.required(203,"attrs",Types.MapType.ofRequired(204,205,Types.StringType.get(),Types.IntegerType.get())),
      Types.NestedField.optional(206,"detail",Types.StructType.of(
        Types.NestedField.required(207,"code",Types.IntegerType.get()),
        Types.NestedField.optional(208,"note",Types.StringType.get()))),
      Types.NestedField.optional(209,"jsonitems",Types.ListType.ofOptional(210,Types.StringType.get())))),
    Types.NestedField.optional(300,"ordered",Types.MapType.ofOptional(301,302,Types.StringType.get(),Types.LongType.get())),
    Types.NestedField.optional(400,"js",Types.StringType.get()),
    Types.NestedField.optional(500,"smalls",Types.ListType.ofOptional(501,Types.IntegerType.get())))
  val FieldDomainProperty = "novarocks.field_domains.v1"
  val DomainPaths = Map("payload.items.element"->"tinyint","payload.attrs.value"->"smallint",
    "payload.detail.code"->"tinyint","payload.detail.note"->"json",
    "payload.jsonitems.element"->"json","js"->"json","smalls.element"->"smallint")
  def domainProperty(schema: Schema): String = {
    val fields=mapper.createObjectNode()
    DomainPaths.toVector.map { case(path,domain) =>
      val field=schema.findField(path)
      require(field!=null && field.fieldId()>0,"Domain path is absent from the actual SDK schema: "+path)
      val expected=if(domain=="json") Types.StringType.get() else Types.IntegerType.get()
      require(field.`type`()==expected,"Declared domain has a wrong physical carrier: "+path)
      (field.fieldId(),domain)
    }.sortBy(_._1).foreach { case(id,domain) => fields.put(id.toString,domain) }
    mapper.writeValueAsString(obj("version"->1,"fields"->fields))
  }
  def assertDomains(t: Table): String = {
    val expected=domainProperty(t.schema())
    require(t.properties().get(FieldDomainProperty)==expected,"Actual field-domain property differs from exact SDK IDs/paths")
    require(t.properties().keySet().asScala.filter(_.startsWith("novarocks.field_domains.")).toSet==Set(FieldDomainProperty),"Unexpected field-domain namespace member")
    expected
  }
  case class FieldFact(path: String,id: Int,required: Boolean,kind: String)
  def facts(schema: Schema): Vector[FieldFact] = {
    var visited=0
    def visit(path: String,f: Types.NestedField,depth: Int): Vector[FieldFact] = {
      visited+=1; require(visited<=MaxFields && depth<=64 && path.length<=4096 && f.fieldId()>0,"Recursive provider schema budget or ID invalid")
      val here = Vector(FieldFact(path,f.fieldId(),f.isRequired,f.`type`().typeId().toString))
      val children = f.`type`().typeId() match {
        case org.apache.iceberg.types.Type.TypeID.STRUCT => f.`type`().asStructType().fields().asScala.toVector.map(c => (path+"."+c.name(),c))
        case org.apache.iceberg.types.Type.TypeID.LIST => Vector((path+".element",f.`type`().asListType().fields().get(0)))
        case org.apache.iceberg.types.Type.TypeID.MAP => f.`type`().asMapType().fields().asScala.toVector.map(c => (path+"."+c.name(),c))
        case _ => Vector.empty
      }
      here ++ children.flatMap { case(p,c) => visit(p,c,depth+1) }
    }
    schema.columns().asScala.toVector.flatMap(f => visit(f.name(),f,1))
  }
  def assertSchema(t: Table,isTarget: Boolean): Vector[FieldFact] = {
    require(metadata(t).formatVersion()==3,"Recursive table no longer has the exact format-v3 contract")
    schemaJson(t); assertDomains(t)
    val actual = facts(t.schema()); val expected = facts(sourceSchema).filter(_.path != "id")
    val selected = if (isTarget) actual else actual.filter(_.path != "id")
    require(selected.map(f => (f.path,f.required,f.kind)) == expected.map(f => (f.path,f.required,f.kind)),"Exact recursive required/type/path contract changed")
    require(actual.map(_.id).distinct.size == actual.size,"Provider field IDs are not unique within this schema")
    if (!isTarget) require(actual.map(f => (f.path,f.required,f.kind)) == facts(sourceSchema).map(f => (f.path,f.required,f.kind)),"Source schema constraints changed")
    actual
  }
  def row(t: Table,id: Long,kind: String): Record = row(t.schema(),id,kind)
  def row(schema: Schema,id: Long,kind: String): Record = {
    val r=GenericRecord.create(schema); r.setField("id",Long.box(id))
    if(kind == "null") return r
    r.setField("label","same")
    val p=GenericRecord.create(schema.findType("payload").asStructType())
    val items=new ArrayList[java.lang.Integer](); val attrs=new LinkedHashMap[String,java.lang.Integer]()
    if(kind != "empty") { items.add(Int.box(-128)); items.add(null); items.add(Int.box(127)); attrs.put("a",Int.box(-32768)) }
    p.setField("items",items); p.setField("attrs",attrs)
    if(kind != "empty") {
      val d=GenericRecord.create(schema.findType("payload.detail").asStructType())
      d.setField("code",Int.box(if(kind == "changed") 127 else -128))
      d.setField("note",if(kind == "note-null") null else "{\"b\":2,\"a\":1}"); p.setField("detail",d)
    }
    val jsonitems=new ArrayList[String](); val smalls=new ArrayList[java.lang.Integer]()
    if(kind!="empty") {
      jsonitems.add("{\"b\":2,\"a\":1}"); jsonitems.add(null); jsonitems.add("{}")
      smalls.add(Int.box(-32768)); smalls.add(null); smalls.add(Int.box(32767))
    }
    p.setField("jsonitems",jsonitems)
    r.setField("js",if(kind=="empty") "{}" else if(kind=="changed") "{\"b\":9,\"a\":1}" else "{\"b\":2,\"a\":1}")
    r.setField("smalls",smalls)
    val m=new LinkedHashMap[String,java.lang.Long]()
    if(kind != "empty") {
      if(kind == "reverse") { m.put("b",null); m.put("a",Long.box(1)) }
      else { m.put("a",Long.box(1)); m.put("b",null) }
    }
    r.setField("payload",p); r.setField("ordered",m); r
  }
  def write(t: Table,rows: Seq[Record],tag: String): DataFile = {
    require(rows.nonEmpty && rows.size<=MaxRows && tag.length<=64 && tag.matches("[a-zA-Z0-9_-]+"),"Recursive writer input budget invalid")
    val out=t.io().newOutputFile(t.location().stripSuffix("/")+"/data/recursive-"+tag+"-"+java.util.UUID.randomUUID()+".parquet")
    val writer=new GenericAppenderFactory(t.schema(),t.spec()).newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out),FileFormat.PARQUET,partition(t,1))
    try rows.foreach(writer.write) finally writer.close()
    val file=writer.toDataFile()
    require(file.recordCount()==rows.size && file.fileSizeInBytes()>0 && file.fileSizeInBytes()<=MaxFileBytes,"Recursive written file differs from exact bounded input")
    file
  }
  // Independent SDK walk. Object nodes are filled in actual map iteration order;
  // no Map sorting or matcher codec is used. Arrays and Struct field order remain exact.
  def value(t: org.apache.iceberg.types.Type,v: Any): JsonNode = {
    if(v == null) return mapper.getNodeFactory.nullNode()
    t.typeId() match {
      case org.apache.iceberg.types.Type.TypeID.STRUCT =>
        val n=mapper.createObjectNode(); val r=v.asInstanceOf[Record]
        t.asStructType().fields().asScala.foreach(f => n.set[JsonNode](f.name(),value(f.`type`(),r.getField(f.name())))); n
      case org.apache.iceberg.types.Type.TypeID.LIST =>
        val n=mapper.createArrayNode(); v.asInstanceOf[java.util.List[Any]].asScala.foreach(x => n.add(value(t.asListType().elementType(),x))); n
      case org.apache.iceberg.types.Type.TypeID.MAP =>
        require(t.asMapType().keyType()==Types.StringType.get(),"This bounded oracle supports only string map keys")
        val n=mapper.createObjectNode(); v.asInstanceOf[java.util.Map[String,Any]].entrySet().asScala.foreach(e => n.set[JsonNode](e.getKey,value(t.asMapType().valueType(),e.getValue))); n
      case _ => json(v)
    }
  }
  def sdkBag(t: Table): Map[String,Int] = {
    val reader=IcebergGenerics.read(t).useSnapshot(t.currentSnapshot().snapshotId()).project(t.schema()).build()
    val rows=scala.collection.mutable.ArrayBuffer.empty[String]
    try reader.asScala.foreach { r =>
      require(rows.size < 1000,"SDK fixture row budget exceeded")
      val n=mapper.createObjectNode()
      VisibleNames.foreach(name => n.set[JsonNode](name,value(t.schema().findType(name),r.getField(name))))
      val content=mapper.writeValueAsString(n)
      require(content.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=16*1024,"SDK recursive row-content byte budget exceeded")
      rows += content
    } finally reader.close()
    rows.groupBy(identity).map { case(k,v) => k->v.size }.toMap
  }
  def sparkBag(ns: String,name: String): Map[String,Int] = {
    val rows=session.sql(s"SELECT to_json(named_struct('label',label,'payload',payload,'ordered',ordered,'js',js,'smalls',smalls),map('ignoreNullFields','false')) FROM ice_rest.$ns.$name").take(1001)
    require(rows.length <= 1000,"Spark fixture row budget exceeded")
    rows.toVector.map { r =>
      val content=r.getString(0); require(content!=null && content.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=16*1024,"Spark recursive row-content byte budget exceeded"); content
    }.groupBy(identity).map { case(k,v) => k->v.size }
  }
  def content(schema: Schema,r: Record): String = {
    val n=mapper.createObjectNode()
    VisibleNames.foreach(name => n.set[JsonNode](name,value(schema.findType(name),r.getField(name))))
    mapper.writeValueAsString(n)
  }
  def expected(stage: String): Map[String,Int] = {
    require(Set("initial","delta").contains(stage),"Unknown frozen endpoint")
    val kinds=if(stage=="initial") Vector("same","same","reverse","null","empty","note-null") else Vector("same","reverse","null","empty","note-null","changed","changed")
    kinds.zipWithIndex.map { case(kind,i) => content(sourceSchema,row(sourceSchema,i+1,kind)) }.groupBy(identity).map { case(k,v)=>k->v.size }
  }
  def assertParquetIds(t: Table): Unit = assertParquetIds(t,facts(t.schema()))
  def assertParquetIds(t: Table,expected: Vector[FieldFact]): Unit = {
    require(expected==facts(t.schema()),"Parquet oracle must use the exact actual provider schema")
    boundedScan(t).foreach { task =>
      val in=t.io().newInputFile(task.file().location())
      require(in.getLength>0 && in.getLength<=MaxFileBytes && in.getLength==task.file().fileSizeInBytes(),"Parquet actual/manifest length differs or exceeds budget")
      val local=java.nio.file.Files.createTempFile("uea7b3-recursive-",".parquet")
      try {
        java.nio.file.Files.write(local,boundedBytes(t,task.file().location()))
        val input=org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(local.toUri()),new org.apache.hadoop.conf.Configuration())
        val reader=org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(input)
        try {
          val raw=reader.getFooter().getFileMetaData().getSchema()
          // Visitor callbacks visit semantic fields; repeated LIST/MAP wrappers
          // are traversal hooks, not schema fields. Check all IDs before convert.
          val rawIds=scala.collection.mutable.ArrayBuffer.empty[JsonNode]
          val seen=scala.collection.mutable.Set.empty[Int]
          val visitor=new org.apache.iceberg.parquet.ParquetTypeVisitor[java.lang.Integer]() {
            import org.apache.iceberg.shaded.org.apache.parquet.schema.{Type=>PType,GroupType,PrimitiveType,MessageType}
            def check(t: PType): java.lang.Integer = {
              require(rawIds.size<MaxFields && currentPath().length<=64,"Raw Parquet semantic-node budget exceeded")
              require(t.getId()!=null,"Raw Parquet semantic field lacks a real ID")
              val id=t.getId().intValue(); require(id>0 && seen.add(id),"Raw Parquet semantic field ID invalid or duplicate")
              rawIds+=obj("path"->currentPath().mkString("."),"id"->id,"repetition"->t.getRepetition().toString,
                "primitive_type"->(if(t.isPrimitive) t.asPrimitiveType().getPrimitiveTypeName().toString else null))
              java.lang.Integer.valueOf(0)
            }
            override def message(t: MessageType,fields: java.util.List[java.lang.Integer]): java.lang.Integer = java.lang.Integer.valueOf(0)
            override def struct(t: GroupType,fields: java.util.List[java.lang.Integer]): java.lang.Integer = check(t)
            override def list(t: GroupType,element: java.lang.Integer): java.lang.Integer = check(t)
            override def map(t: GroupType,key: java.lang.Integer,value: java.lang.Integer): java.lang.Integer = check(t)
            override def primitive(t: PrimitiveType): java.lang.Integer = check(t)
            override def variant(t: GroupType): java.lang.Integer = throw new IllegalStateException("Variant is outside this exact recursive fixture")
          }
          org.apache.iceberg.parquet.ParquetTypeVisitor.visit(raw,visitor)
          require(rawIds.nonEmpty,"No raw Parquet semantic field IDs observed")
          val rawById=rawIds.map(n=>n.get("id").asInt()->n).toMap
          DomainPaths.foreach { case(path,domain) =>
            val id=t.schema().findField(path).fieldId()
            val primitive=if(domain=="json") "BINARY" else "INT32"
            require(rawById.get(id).exists(n=>n.get("primitive_type").asText()==primitive),
              "Declared domain has a wrong raw Parquet physical primitive: "+path)
          }
          val requirednessMismatches=expected.flatMap { f =>
            val actual=rawById.get(f.id).map(n=>n.get("repetition").asText()).getOrElse("MISSING")
            if(actual==(if(f.required) "REQUIRED" else "OPTIONAL")) Vector.empty[String]
            else Vector("path="+f.path+",id="+f.id+",expected.required="+f.required+",actual.repetition="+actual)
          }
          require(requirednessMismatches.isEmpty,"Raw Parquet field required/optional fact differs from provider schema: "+requirednessMismatches.mkString("; "))
          val physical=org.apache.iceberg.parquet.ParquetSchemaUtil.convertAndPrune(raw)
          val observed=facts(physical).map(f=>f.id->f).toMap
          require(expected.forall(f=>observed.get(f.id).contains(f)),"Actual Parquet field IDs, required or recursive shape differ from provider schema")
          require(observed.size==facts(physical).size && expected.forall(f=>seen.contains(f.id)),"Converted schema duplicated/pruned a required field ID")
          boundedEmit(obj("record"->"recursive_parquet_binding","table_uuid"->metadata(t).uuid().toString,"schema_id"->t.schema().schemaId(),"snapshot"->t.currentSnapshot().snapshotId(),"path"->task.file().location(),"raw_semantic_ids"->rawIds.toVector,"schema_json"->SchemaParser.toJson(physical)))
        } finally reader.close()
      } finally java.nio.file.Files.deleteIfExists(local)
    }
  }
  def initialize(ns: String): Unit = {
    require(ns.length<=64 && ns.matches("[a-zA-Z0-9_]+"))
    session.sql(s"CREATE NAMESPACE IF NOT EXISTS ice_rest.$ns")
    val catalog=Spark3Util.loadIcebergCatalog(session,"ice_rest")
    // Use Iceberg's own fresh-ID allocator before the single CREATE, then
    // verify the returned SDK tree/property before any data is written.
    val ids=new java.util.concurrent.atomic.AtomicInteger(0)
    val allocated=org.apache.iceberg.types.TypeUtil.assignFreshIds(sourceSchema,
      new org.apache.iceberg.types.TypeUtil.NextID { override def get(): Int = ids.incrementAndGet() })
    val t=catalog.createTable(TableIdentifier.of(ns,Source),allocated,PartitionSpec.unpartitioned(),
      Map("format-version"->"3","write.row-lineage"->"true",FieldDomainProperty->domainProperty(allocated)).asJava)
    require(facts(t.schema())==facts(allocated),"CREATE response changed SDK-allocated field identities")
    assertDomains(t)
    val before=Option(t.currentSnapshot()).map(_.snapshotId())
    require(before.isEmpty,"New recursive source unexpectedly has a prior snapshot")
    val file=write(t,Vector("same","same","reverse","null","empty","note-null").zipWithIndex.map { case(k,i) => row(t,i+1,k) },"initial")
    t.newAppend().appendFile(file).commit(); t.refresh(); assertSchema(t,false); assertParquetIds(t)
    // The actual SDK Parquet reader must preserve a,b and b,a as distinct sequences.
    val sdk=sdkBag(t); require(sdk==expected("initial") && sdk==sparkBag(ns,Source),"SDK and Spark actual ordered content disagree")
    require(sdk.keys.exists(_.contains("\"ordered\":{\"a\":1,\"b\":null}")) && sdk.keys.exists(_.contains("\"ordered\":{\"b\":null,\"a\":1}")),"Actual SDK/Spark read path did not preserve both Map orders")
    val tasks=boundedScan(t); require(tasks.size==1 && tasks.head.file().location()==file.location() && deletes(tasks).isEmpty,"Initial committed source files differ from the exact append")
    require(t.currentSnapshot().snapshotId()>0 && countSummary(t,"total-data-files")==1 && countSummary(t,"total-records")==6,"Initial source snapshot totals differ")
    boundedEmit(obj("record"->"recursive_source_initial","source_uuid"->metadata(t).uuid().toString,"schema_id"->t.schema().schemaId(),"schema_json"->schemaJson(t),"field_domains_json"->assertDomains(t),"from_snapshot"->null,"to_snapshot"->t.currentSnapshot().snapshotId(),"snapshot"->t.currentSnapshot().snapshotId(),"fields"->assertSchema(t,false).map(f=>obj("path"->f.path,"id"->f.id,"required"->f.required,"kind"->f.kind)),"added_files"->tasks.map(task=>fileFact(task.file())),"data_files"->tasks.map(task=>fileFact(task.file())),"delete_files"->Vector.empty[JsonNode],"summary"->summary(t),"bag"->sdk.toVector.sortBy(_._1).map { case(k,v) => obj("content"->k,"count"->v) }))
    println("RECURSIVE_SOURCE_READY")
  }
  def mutate(ns: String): Unit = {
    val t=table(ns,Source); assertSchema(t,false)
    val sourceUuid=metadata(t).uuid().toString; val schemaBefore=schemaJson(t); val schemaId=t.schema().schemaId(); val from=t.currentSnapshot().snapshotId()
    require(from>0,"Recursive mutation has no exact source baseline")
    val files=boundedScan(t).filter(_.file().location().contains("/recursive-initial-")).map(_.file())
    require(files.size==1 && files.head.recordCount()==6,"Frozen initial source file changed")
    val added=write(t,Vector(row(t,7,"changed"),row(t,8,"changed")),"delta")
    val dvs=dv(t,Seq((files.head.location(),0L)))
    require(dvs.size==1 && dvs.head.recordCount()==1 && dvs.head.format()==FileFormat.PUFFIN && dvs.head.referencedDataFile()==files.head.location(),"Source retraction lacks an exact real single-row DV reference")
    t.newRowDelta().addRows(added).addDeletes(dvs.head).commit(); t.refresh()
    require(t.currentSnapshot().snapshotId()>0 && t.currentSnapshot().snapshotId()!=from && t.currentSnapshot().parentId()!=null && t.currentSnapshot().parentId().longValue()==from,"Source mutation lost the exact from/to frontier")
    require(metadata(t).uuid().toString==sourceUuid && schemaJson(t)==schemaBefore && t.schema().schemaId()==schemaId,"Mutation changed frozen source identity/schema")
    require(sdkBag(t)==expected("delta") && sdkBag(t)==sparkBag(ns,Source),"Independent SDK and Spark delta bags differ")
    assertParquetIds(t)
    val tasks=boundedScan(t); val liveDeletes=deletes(tasks); val actualAdded=tasks.filter(_.file().location()==added.location())
    require(actualAdded.size==1 && actualAdded.head.file().recordCount()==2 && liveDeletes.size==1 && liveDeletes.head.location()==dvs.head.location() && liveDeletes.head.referencedDataFile()==files.head.location(),"Actual source delta files do not match exact committed additions/DV")
    boundedEmit(obj("record"->"recursive_source_changed","source_uuid"->sourceUuid,"schema_id"->schemaId,"from_snapshot"->from,"to_snapshot"->t.currentSnapshot().snapshotId(),"snapshot"->t.currentSnapshot().snapshotId(),"schema_json"->schemaJson(t),"field_domains_json"->assertDomains(t),"fields"->assertSchema(t,false).map(f=>obj("path"->f.path,"id"->f.id,"required"->f.required,"kind"->f.kind)),"added_files"->actualAdded.map(task=>fileFact(task.file())),"data_files"->tasks.map(task=>fileFact(task.file())),"delete_files"->liveDeletes.map(fileFact),"summary"->summary(t),"bag"->sdkBag(t).toVector.sortBy(_._1).map { case(k,v)=>obj("content"->k,"count"->v) }))
    println("RECURSIVE_SOURCE_CHANGED")
  }
  def observe(ns: String,stage: String): Unit = {
    require(Set("initial","restored","incremental","full").contains(stage),"Unknown recursive observation stage")
    val s=table(ns,Source); val t=table(ns,Target); val sourceFields=assertSchema(s,false); val bound=assertSchema(t,true); assertParquetIds(s); assertParquetIds(t)
    require(metadata(s).uuid()!=metadata(t).uuid(),"Source/target provider identities were aliased")
    val byPath=sourceFields.map(f=>f.path->f.id).toMap
    require(bound.forall(f=>byPath(f.path)!=f.id),"This fixture target copied source field IDs")
    val sdkSource=sdkBag(s); val sparkSource=sparkBag(ns,Source); val sdkTarget=sdkBag(t); val sparkTarget=sparkBag(ns,Target)
    val endpoint=if(Set("initial","restored").contains(stage)) "initial" else "delta"
    require(sdkSource==expected(endpoint) && sdkSource==sparkSource && sdkSource==sdkTarget && sdkTarget==sparkTarget,"Independent complete source/target bags differ")
    val tasks=boundedScan(t)
    if(stage=="incremental") require(tasks.exists(_.deletes().asScala.exists(_.format()==FileFormat.PUFFIN)),"No actual target retraction DV")
    val liveDeletes=deletes(tasks)
    if(stage=="full") {
      require(liveDeletes.isEmpty,"FULL retained target deletes")
      val totals=Vector("total-data-files"->tasks.size.toLong,"total-delete-files"->0L,
        "total-records"->tasks.map(_.file().recordCount()).sum,"total-files-size"->tasks.map(_.file().fileSizeInBytes()).sum,
        "total-position-deletes"->0L,"total-equality-deletes"->0L)
      totals.foreach { case(k,v)=>require(countSummary(t,k)==v,"FULL exact summary total differs from actual planned files: "+k) }
      require(countSummary(t,"total-records")==expected("delta").values.map(_.toLong).sum,"FULL physical records differ from complete independent bag")
    }
    require(t.currentSnapshot().snapshotId()>0 && s.currentSnapshot().snapshotId()>0,"Observation lacks exact positive snapshot")
    boundedEmit(obj("record"->"recursive_mv_observed","stage"->stage,"table_uuid"->metadata(t).uuid().toString,"source_uuid"->metadata(s).uuid().toString,"source_schema_json"->schemaJson(s),"source_field_domains_json"->assertDomains(s),"field_domains_json"->assertDomains(t),"source_schema_id"->s.schema().schemaId(),"source_snapshot"->s.currentSnapshot().snapshotId(),"source_fields"->sourceFields.map(f=>obj("path"->f.path,"id"->f.id,"required"->f.required,"kind"->f.kind)),"schema_json"->schemaJson(t),"schema_id"->t.schema().schemaId(),"snapshot"->t.currentSnapshot().snapshotId(),"data_files"->tasks.map(task=>fileFact(task.file())),"delete_files"->liveDeletes.map(fileFact),"summary"->summary(t),"fields"->bound.map(f=>obj("path"->f.path,"id"->f.id,"required"->f.required,"kind"->f.kind)),"bag"->sdkTarget.toVector.sortBy(_._1).map { case(k,v)=>obj("content"->k,"count"->v) }))
    println("RECURSIVE_MV_OBSERVED")
  }
  // Ordinary DDL defaults each child to optional except the required Map key.
  // CTAS instead freezes the already-proved source children without widening.
  def assertDdlCtasSchema(t: Table,isCtas: Boolean): Vector[FieldFact] = {
    require(metadata(t).formatVersion()==3,"DDL/CTAS fixture requires format-v3")
    schemaJson(t); assertDomains(t)
    val actual=facts(t.schema())
    val expected=facts(sourceSchema).filter(_.path!="id").map(f =>
      if(isCtas) f else f.copy(required=f.path.endsWith(".key")))
    require(actual.map(f=>(f.path,f.required,f.kind))==expected.map(f=>(f.path,f.required,f.kind)),"DDL default or CTAS source recursive contract differs")
    require(actual.map(_.id).distinct.size==actual.size,"DDL/CTAS actual provider IDs are not unique")
    actual
  }
  def ddlCtasTableFacts(t: Table): JsonNode = {
    val uuidText=metadata(t).uuid().toString
    val uuid=java.util.UUID.fromString(uuidText)
    require(uuid.toString==uuidText && uuid!=new java.util.UUID(0L,0L) && t.schema().schemaId()>=0,"DDL/CTAS provider identity/schema ID invalid")
    val current=Option(t.currentSnapshot())
    current.foreach(s=>require(s.snapshotId()>0,"DDL/CTAS snapshot is not positive"))
    val fs=facts(t.schema())
    val tasks=if(current.isEmpty) Vector.empty[FileScanTask] else boundedScan(t)
    obj("table_uuid"->metadata(t).uuid().toString,"schema_id"->t.schema().schemaId(),
      "schema_json"->schemaJson(t),"field_domains_json"->assertDomains(t),"snapshot"->current.map(s=>s.snapshotId():Any).getOrElse(null),
      "fields"->fs.map(f=>obj("path"->f.path,"id"->f.id,"required"->f.required,"kind"->f.kind)),
      "data_files"->tasks.map(task=>fileFact(task.file())),"delete_files"->deletes(tasks).map(fileFact),
      "summary"->current.map(_=>summary(t)).getOrElse(obj()),
      "bag"->sdkBagIfSnapshot(t).toVector.sortBy(_._1).map { case(k,v)=>obj("content"->k,"count"->v) })
  }
  def sdkBagIfSnapshot(t: Table): Map[String,Int] =
    if(t.currentSnapshot()==null) Map.empty[String,Int] else sdkBag(t)
  def prepareDdlCtas(ns: String): Unit = {
    val s=table(ns,Source); val d=table(ns,Ddl)
    assertSchema(s,false); assertDdlCtasSchema(d,false); assertParquetIds(s)
    require(metadata(s).uuid()!=metadata(d).uuid(),"DDL table aliased source UUID")
    require(s.currentSnapshot()!=null && sdkBag(s)==expected("initial") && sdkBag(s)==sparkBag(ns,Source),"DDL/CTAS input differs from frozen six-row recipe")
    require(d.currentSnapshot()==null,"DDL/CTAS prepared table already has a snapshot")
    boundedEmit(obj("record"->"recursive_ddl_ctas_prepared","namespace"->ns,
      "source"->ddlCtasTableFacts(s),"ddl"->ddlCtasTableFacts(d)))
    println("RECURSIVE_DDL_CTAS_PREPARED")
  }
  def observeDdlCtas(ns: String,frozenInputJson: String): Unit = {
    require(frozenInputJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=MaxSchemaBytes,"Frozen DDL/CTAS input exceeds budget")
    val reader=mapper.copy().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    val frozen=reader.readTree(frozenInputJson)
    // Compare two parsed JSON values: SDK Long nodes otherwise differ from
    // the Int nodes chosen when the same bounded integer is read from JSON.
    def parsedTableFacts(t: Table): JsonNode = {
      val bytes=mapper.writeValueAsBytes(ddlCtasTableFacts(t))
      require(bytes.length<=MaxSchemaBytes,"Observed DDL/CTAS table facts exceed budget")
      reader.readTree(bytes)
    }
    def member(n: JsonNode,key: String): JsonNode = {
      require(n!=null && n.isObject && n.has(key),"Missing frozen DDL/CTAS member: "+key); n.get(key)
    }
    def text(n: JsonNode,key: String): String = {
      val v=member(n,key); require(v.isTextual && v.asText().nonEmpty,"Invalid frozen DDL/CTAS string: "+key); v.asText()
    }
    def integer(n: JsonNode,key: String,positive: Boolean): Long = {
      val v=member(n,key); require(v.isIntegralNumber && v.canConvertToLong,"Invalid frozen DDL/CTAS integer: "+key)
      val out=v.asLong(); require(if(positive) out>0 else out>=0,"Out-of-range frozen DDL/CTAS integer: "+key); out
    }
    require(frozen!=null && frozen.isObject && frozen.fieldNames().asScala.toSet==Set("record","namespace","source","ddl"),"Unexpected frozen DDL/CTAS receipt shape")
    require(text(frozen,"record")=="recursive_ddl_ctas_prepared" && text(frozen,"namespace")==ns,"Frozen DDL/CTAS receipt belongs to another stage/namespace")
    Vector("source","ddl").foreach { k=>
      val prior=member(frozen,k)
      require(prior.isObject && prior.fieldNames().asScala.toSet==Set("table_uuid","schema_id","schema_json","field_domains_json","snapshot","fields","data_files","delete_files","summary","bag"),"Unexpected frozen DDL/CTAS table-fact shape")
    }
    val s=table(ns,Source); val d=table(ns,Ddl); val c=table(ns,Ctas)
    assertSchema(s,false); val df=assertDdlCtasSchema(d,false); val cf=assertDdlCtasSchema(c,true)
    val sf=facts(s.schema())
    require(Vector(metadata(s).uuid(),metadata(d).uuid(),metadata(c).uuid()).distinct.size==3,"DDL/CTAS provider UUIDs were aliased")
    Vector((s,member(frozen,"source")),(d,member(frozen,"ddl"))).foreach { case(t,prior)=>
      val u=java.util.UUID.fromString(text(prior,"table_uuid")); require(u.toString==text(prior,"table_uuid") && u!=new java.util.UUID(0L,0L) && u.toString==metadata(t).uuid().toString,"DDL/CTAS frozen UUID changed")
      require(integer(prior,"schema_id",false)==t.schema().schemaId() && text(prior,"schema_json")==schemaJson(t),"DDL/CTAS frozen schema changed")
      require(text(prior,"field_domains_json")==assertDomains(t),"DDL/CTAS frozen domain declaration changed")
      val observed=parsedTableFacts(t)
      require(member(prior,"fields").isArray && member(prior,"fields")==member(observed,"fields"),"DDL/CTAS frozen field bindings changed")
    }
    val priorSource=member(frozen,"source"); val priorDdl=member(frozen,"ddl")
    require(s.currentSnapshot()!=null && integer(priorSource,"snapshot",true)==s.currentSnapshot().snapshotId(),"DDL/CTAS source frontier changed")
    require(member(priorDdl,"snapshot").isNull && member(priorDdl,"data_files").isArray && member(priorDdl,"data_files").size()==0 && member(priorDdl,"delete_files").isArray && member(priorDdl,"delete_files").size()==0 && member(priorDdl,"bag").isArray && member(priorDdl,"bag").size()==0,"Prepared DDL was not empty")
    val sourceFact=parsedTableFacts(s)
    Vector("data_files","delete_files","summary","bag").foreach(k=>require(member(priorSource,k)==member(sourceFact,k),"DDL/CTAS source fact changed: "+k))
    val sourceByPath=sf.map(f=>f.path->f.id).toMap
    require(cf.forall(f=>sourceByPath(f.path)!=f.id),"This CTAS fixture copied source IDs instead of allocating its own bindings")
    // Pass role-specific actual provider facts to the bounded raw-ID oracle.
    assertParquetIds(s,sf); assertParquetIds(d,df); assertParquetIds(c,cf)
    Vector((Source,s),(Ddl,d),(Ctas,c)).foreach { case(name,t)=>
      require(t.currentSnapshot()!=null && sdkBag(t)==expected("initial") && sdkBag(t)==sparkBag(ns,name),"DDL/CTAS complete SDK/Spark ordered bag differs: "+name)
      val tasks=boundedScan(t)
      require(tasks.nonEmpty && deletes(tasks).isEmpty,"DDL/CTAS write has no data or unexpected deletes: "+name)
      Vector("total-data-files"->tasks.size.toLong,"total-delete-files"->0L,
        "total-records"->tasks.map(_.file().recordCount()).sum,"total-files-size"->tasks.map(_.file().fileSizeInBytes()).sum,
        "total-position-deletes"->0L,"total-equality-deletes"->0L).foreach { case(k,v)=>
          require(countSummary(t,k)==v,"DDL/CTAS exact summary total differs: "+name+"/"+k) }
      require(countSummary(t,"total-records")==6,"DDL/CTAS physical row count differs from six-row recipe")
    }
    boundedEmit(obj("record"->"recursive_ddl_ctas_observed","namespace"->ns,
      "frozen_input"->frozen,"source"->sourceFact,"ddl"->ddlCtasTableFacts(d),"ctas"->ddlCtasTableFacts(c)))
    println("RECURSIVE_DDL_CTAS_OBSERVED")
  }
}
