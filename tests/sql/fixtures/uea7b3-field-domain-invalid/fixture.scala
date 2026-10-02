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

// Loaded after iceberg-delete-applicability/generate.scala. Every table,
// metadata property and data file below belongs to this case's namespace.
object FieldDomainInvalidFixture {
  import DeleteApplicabilityFixture.{obj, mapper, metadata, partition}
  import scala.collection.JavaConverters._
  import org.apache.iceberg._
  import org.apache.iceberg.catalog.TableIdentifier
  import org.apache.iceberg.data.{GenericAppenderFactory, GenericRecord, IcebergGenerics, Record}
  import org.apache.iceberg.encryption.EncryptedFiles
  import org.apache.iceberg.spark.Spark3Util
  import org.apache.iceberg.types.Types
  import com.fasterxml.jackson.databind.JsonNode
  import java.nio.charset.StandardCharsets.UTF_8

  val DomainKey = "novarocks.field_domains.v1"
  // This is the existing provider payload byte cap, not a new product budget.
  val DomainMaxBytes = 1024 * 1024
  val MaxReceiptBytes = 256 * 1024
  val MaxFileBytes = 1024 * 1024
  val MaxMetadataBytes = 2 * 1024 * 1024
  val InvalidCases = Vector("namespace", "version", "domain", "duplicate",
    "carrier", "history", "dual", "member", "budget")
  val OverflowCases = Vector("tiny_root_high", "tiny_root_low", "small_root_high",
    "small_root_low", "tiny_list_high", "tiny_list_low", "small_list_high", "small_list_low")
  val Cases = Vector("foreign", "formal_empty", "valid") ++ InvalidCases ++ OverflowCases
  require(Cases.size == 20 && Cases.distinct.size == Cases.size)

  def run(action: => Unit): Unit = {
    val executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
      new java.util.concurrent.ThreadFactory {
        def newThread(r: Runnable): Thread = { val t = new Thread(r,"field-domain-invalid-deadline"); t.setDaemon(true); t }
      })
    val deadline = executor.schedule(new Runnable {
      def run(): Unit = { System.err.println("Field-domain SDK fixture exceeded 120-second deadline"); System.exit(124) }
    },120,java.util.concurrent.TimeUnit.SECONDS)
    try action finally { deadline.cancel(false); executor.shutdownNow() }
  }
  def catalog = Spark3Util.loadIcebergCatalog(org.apache.spark.sql.SparkSession.active,"ice_rest")
  def identifier(ns: String, name: String): TableIdentifier = {
    require(ns.length <= 96 && ns.matches("ns_[a-zA-Z0-9_]+") && Cases.contains(name),"Fixture identity is outside its exact owned namespace/tables")
    TableIdentifier.of(ns,"fd_" + name)
  }
  def load(ns: String, name: String): Table = { val t=catalog.loadTable(identifier(ns,name)); t.refresh(); t }
  def emit(n: JsonNode): Unit = {
    val raw=mapper.writeValueAsBytes(n); require(raw.length <= MaxReceiptBytes,"Field-domain receipt exceeds its byte budget")
    println("UEA4G_RECEIPT " + new String(raw,UTF_8))
  }
  def digest(bytes: Array[Byte]): String = java.security.MessageDigest.getInstance("SHA-256")
    .digest(bytes).map(b => f"${b & 0xff}%02x").mkString
  def read(io: org.apache.iceberg.io.FileIO,path: String,cap: Int): Array[Byte] = {
    val file=io.newInputFile(path); require(file.getLength() > 0 && file.getLength() <= cap,"SDK object length exceeds the fixture's I/O bound")
    val in=file.newStream(); val out=new java.io.ByteArrayOutputStream()
    try {
      val buffer=new Array[Byte](8192); var n=in.read(buffer)
      while(n >= 0) { if(n > 0) { require(out.size().toLong+n <= cap,"SDK object grew beyond its frozen length bound"); out.write(buffer,0,n) }; n=in.read(buffer) }
      require(out.size().toLong == file.getLength(),"SDK object length changed during the exact read")
      out.toByteArray
    } finally in.close()
  }
  def schema: Schema = new Schema(
    Types.NestedField.required(1,"id",Types.LongType.get()),
    Types.NestedField.optional(2,"j",Types.StringType.get()),
    Types.NestedField.optional(3,"n",Types.IntegerType.get()),
    Types.NestedField.optional(4,"s",Types.IntegerType.get()),
    Types.NestedField.optional(5,"xs",Types.ListType.ofOptional(6,Types.IntegerType.get())),
    Types.NestedField.optional(7,"js",Types.ListType.ofOptional(8,Types.StringType.get())))
  def integerList(values: Seq[java.lang.Integer]): java.util.List[java.lang.Integer] = {
    val xs=new java.util.ArrayList[java.lang.Integer](); values.foreach(xs.add); xs
  }
  def textList(values: Seq[String]): java.util.List[String] = {
    val xs=new java.util.ArrayList[String](); values.foreach(xs.add); xs
  }
  // These exact Java INT32 values are the independent inputs. No Spark/SQL
  // CAST, safe cast or narrow writer ever produces the overflow oracle.
  def values(name: String): Vector[Vector[AnyRef]] = {
    if(OverflowCases.contains(name)) {
      val number=if(name.startsWith("tiny")) { if(name.endsWith("high")) 128 else -129 }
        else { if(name.endsWith("high")) 32768 else -32769 }
      val nested=name.contains("_list_")
      Vector(Vector(Long.box(1L),"{\"b\":2,\"a\":1}",Int.box(if(nested) 0 else number),Int.box(0),
        integerList(Seq(Int.box(if(nested) number else 0))),textList(Seq("{}"))))
    } else Vector(
      Vector(Long.box(1L),"{\"b\":2,\"a\":1}",Int.box(-128),Int.box(-32768),integerList(Seq(Int.box(-128),null,Int.box(127))),textList(Seq("{\"b\":2,\"a\":1}",null,"{}"))),
      Vector(Long.box(2L),"{\"b\":2,\"a\":1}",Int.box(-128),Int.box(-32768),integerList(Seq(Int.box(-128),null,Int.box(127))),textList(Seq("{\"b\":2,\"a\":1}",null,"{}"))),
      Vector(Long.box(3L),"{}",Int.box(127),Int.box(32767),integerList(Seq.empty),textList(Seq.empty)),
      Vector(Long.box(4L),null,null,null,null,null),
      // Logical JSON is its existing payload contract; no new parse validity
      // restriction is introduced by the persisted field declaration.
      Vector(Long.box(5L),"not-json",Int.box(0),Int.box(0),integerList(Seq(Int.box(0))),textList(Seq("not-json"))))
  }
  def expectedRow(v: Vector[AnyRef]): JsonNode = {
    val a=mapper.createArrayNode()
    def value(v: Any): JsonNode = v match {
      case null => mapper.getNodeFactory.nullNode()
      case i: java.lang.Integer => mapper.getNodeFactory.numberNode(i.intValue())
      case l: java.lang.Long => mapper.getNodeFactory.numberNode(l.longValue())
      case s: String => mapper.getNodeFactory.textNode(s)
      case xs: java.util.List[_] => val n=mapper.createArrayNode(); xs.asScala.foreach(x => n.add(value(x))); n
      case other => throw new IllegalStateException("Unexpected exact fixture SDK value class: " + other.getClass.getName)
    }
    v.foreach(x => a.add(value(x))); a
  }
  def rows(t: Table): Vector[String] = {
    val reader=IcebergGenerics.read(t).useSnapshot(t.currentSnapshot().snapshotId()).build()
    val out=scala.collection.mutable.ArrayBuffer.empty[String]
    try reader.asScala.foreach { r =>
      require(out.size < 5,"SDK rows exceed the fixture's exact row bound")
      out+=mapper.writeValueAsString(expectedRow(Vector("id","j","n","s","xs","js").map(n => r.getField(n).asInstanceOf[AnyRef])))
    } finally reader.close()
    out.toVector.sorted
  }
  def payload(entries: Seq[(Int,String)]): String = {
    require(entries.size <= 8 && entries.map(_._1).distinct.size == entries.size && entries.forall(_._1 > 0))
    val fields=mapper.createObjectNode(); entries.sortBy(_._1).foreach { case(id,domain) => fields.put(id.toString,domain) }
    mapper.writeValueAsString(obj("version" -> 1,"fields" -> fields))
  }
  def properties(name: String,schema: Schema): Map[String,String] = {
    def id(path: String): Int = schema.findField(path).fieldId()
    val n=id("n"); val j=id("j"); val element=id("xs.element")
    name match {
      case "foreign" => Map.empty
      case "formal_empty" => Map(DomainKey -> payload(Seq.empty))
      case "valid" => Map(DomainKey -> payload(Seq(j -> "json",n -> "tinyint",id("s") -> "smallint",element -> "tinyint",id("js.element") -> "json")))
      case "namespace" => Map("novarocks.field_domains.v2" -> "{\"version\":2,\"fields\":{}}")
      case "version" => Map(DomainKey -> "{\"version\":2,\"fields\":{}}")
      case "domain" => Map(DomainKey -> payload(Seq(n -> "unknown-domain")))
      case "duplicate" => Map(DomainKey -> ("{\"version\":1,\"fields\":{\""+n+"\":\"tinyint\",\""+n+"\":\"smallint\"}}"))
      case "carrier" => Map(DomainKey -> payload(Seq(j -> "tinyint")))
      case "history" =>
        require(schema.findField(999999) == null,"The absent-history fixture ID unexpectedly exists")
        Map(DomainKey -> payload(Seq(999999 -> "tinyint")))
      case "dual" => Map(DomainKey -> payload(Seq(n -> "tinyint")),"novarocks.scalar_integer_domains.v1" -> ("{\""+n+"\":\"tinyint\"}"))
      case "member" => Map(DomainKey -> "{\"version\":1,\"fields\":{},\"unexpected\":true}")
      case "budget" =>
        val raw=payload(Seq.empty)+(" " * (DomainMaxBytes+1-payload(Seq.empty).getBytes(UTF_8).length))
        require(raw.getBytes(UTF_8).length == DomainMaxBytes+1,"Over-budget declaration is not exactly MAX_BYTES+1")
        Map(DomainKey -> raw)
      case overflow if OverflowCases.contains(overflow) =>
        Map(DomainKey -> payload(Seq((if(overflow.contains("_list_")) element else n) -> (if(overflow.startsWith("tiny")) "tinyint" else "smallint"))))
      case _ => throw new IllegalStateException("Unknown exact fixture case")
    }
  }
  def files(t: Table): Vector[FileScanTask] = {
    val planned=t.newScan().useSnapshot(t.currentSnapshot().snapshotId()).planFiles(); val out=scala.collection.mutable.ArrayBuffer.empty[FileScanTask]
    try planned.asScala.foreach { task => require(out.size < 1 && task.deletes().isEmpty,"Fixture is not exactly one data file without deletes"); out+=task }
    finally planned.close()
    require(out.size == 1,"Fixture lacks its one exact data file"); out.toVector
  }
  def parquet(t: Table,f: DataFile): JsonNode = {
    val bytes=read(t.io(),f.location(),MaxFileBytes)
    require(bytes.length.toLong == f.fileSizeInBytes(),"Manifest and actual Parquet byte length differ")
    val local=java.nio.file.Files.createTempFile("field-domain-int32-",".parquet")
    try {
      java.nio.file.Files.write(local,bytes)
      val reader=org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader.open(
        org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(local.toUri),new org.apache.hadoop.conf.Configuration()))
      try {
        val raw=reader.getFooter().getFileMetaData().getSchema()
        val observed=scala.collection.mutable.ArrayBuffer.empty[JsonNode]; val seen=scala.collection.mutable.Set.empty[Int]
        val visitor=new org.apache.iceberg.parquet.ParquetTypeVisitor[java.lang.Integer]() {
          import org.apache.iceberg.shaded.org.apache.parquet.schema.{Type => PType,GroupType,PrimitiveType,MessageType}
          def check(node: PType): java.lang.Integer = {
            require(observed.size < 8 && currentPath().length <= 3 && node.getId()!=null,"Raw Parquet semantic ID budget/presence failed")
            val id=node.getId().intValue(); require(id>0 && seen.add(id),"Raw Parquet semantic ID invalid or duplicated")
            val field=t.schema().findField(id); require(field!=null,"Raw Parquet field is absent from the exact SDK schema")
            require(node.getRepetition().toString == (if(field.isRequired) "REQUIRED" else "OPTIONAL"),"Raw Parquet requiredness differs for exact field ID " + id)
            observed+=obj("id" -> id,"path" -> currentPath().mkString("."),"repetition" -> node.getRepetition().toString,
              "primitive" -> (if(node.isPrimitive) node.asPrimitiveType().getPrimitiveTypeName().toString() else null))
            java.lang.Integer.valueOf(0)
          }
          override def message(t: MessageType,fields: java.util.List[java.lang.Integer]): java.lang.Integer = java.lang.Integer.valueOf(0)
          override def struct(t: GroupType,fields: java.util.List[java.lang.Integer]): java.lang.Integer = check(t)
          override def list(t: GroupType,element: java.lang.Integer): java.lang.Integer = check(t)
          override def map(t: GroupType,key: java.lang.Integer,value: java.lang.Integer): java.lang.Integer = check(t)
          override def primitive(t: PrimitiveType): java.lang.Integer = check(t)
          override def variant(t: GroupType): java.lang.Integer = throw new IllegalStateException("Variant is outside the exact fixture")
        }
        org.apache.iceberg.parquet.ParquetTypeVisitor.visit(raw,visitor)
        val expected=Vector("id","j","n","s","xs","xs.element","js","js.element").map(t.schema().findField(_).fieldId()).toSet
        require(seen.toSet == expected,"Raw Parquet semantic ID set is not the exact provider schema")
        for(path <- Vector("n","s","xs.element")) require(observed.exists(n => n.get("id").asInt()==t.schema().findField(path).fieldId() && n.get("primitive").asText()=="INT32"),"Actual narrow/foreign file is not INT32 at " + path)
        for(path <- Vector("j","js.element")) require(observed.exists(n => n.get("id").asInt()==t.schema().findField(path).fieldId() && n.get("primitive").asText()=="BINARY"),"Actual JSON/foreign file is not standard BINARY string at " + path)
        obj("path" -> f.location(),"bytes" -> bytes.length,"sha256" -> digest(bytes),"raw_fields" -> observed.toVector)
      } finally reader.close()
    } finally java.nio.file.Files.deleteIfExists(local)
  }
  def fact(name: String,t: Table): JsonNode = {
    val m=metadata(t); val snapshot=t.currentSnapshot()
    require(m.formatVersion()==3 && snapshot!=null && snapshot.snapshotId()>0,"Fixture lacks exact v3 publication")
    val schemaText=SchemaParser.toJson(t.schema()); require(schemaText.getBytes(UTF_8).length <= 4096,"Fixture schema byte bound exceeded")
    val history=m.schemas().asScala.toVector.sortBy(_.schemaId()); require(history.size == 1 && history.head.schemaId()==t.schema().schemaId(),"Fixture retained schema history changed")
    val expectedProperties=properties(name,t.schema())
    val actualProperties=t.properties().asScala.filter(_._1.startsWith("novarocks.")).toMap
    require(actualProperties==expectedProperties,"SDK persisted properties differ from independent raw malformed/domain inputs")
    val actualRows=rows(t); val expectedRows=values(name).map(v => mapper.writeValueAsString(expectedRow(v))).sorted
    require(actualRows==expectedRows,"SDK complete bag differs from independently fixed INT32/string inputs")
    val tasks=files(t); val file=tasks.head.file()
    val objectBytes=read(t.io(),m.metadataFileLocation(),MaxMetadataBytes)
    val kind=if(name=="budget") "ResourceExhausted" else if(InvalidCases.contains(name)||OverflowCases.contains(name)) "CorruptData" else "None"
    val physical=parquet(t,file)
    emit(obj("record" -> "field_domain_physical_input","case" -> name,"file" -> physical))
    obj("case" -> name,"table_uuid" -> m.uuid().toString,"metadata_path" -> m.metadataFileLocation(),
      "metadata_bytes" -> objectBytes.length,"metadata_sha256" -> digest(objectBytes),"snapshot" -> snapshot.snapshotId(),
      "snapshot_sequence" -> snapshot.sequenceNumber(),"snapshot_schema_id" -> snapshot.schemaId(),"schema_id" -> t.schema().schemaId(),
      "schema_json" -> schemaText,"retained_schemas" -> history.map(s => obj("schema_id" -> s.schemaId(),"schema_json" -> SchemaParser.toJson(s))),
      "properties" -> actualProperties.toVector.sortBy(_._1).map { case(k,v) => obj("key" -> k,"bytes" -> v.getBytes(UTF_8).length,"sha256" -> digest(v.getBytes(UTF_8))) },
      "files" -> tasks.map(task => obj("path" -> task.file().location(),"records" -> task.file().recordCount(),"bytes" -> task.file().fileSizeInBytes(),
        "spec_id" -> task.file().specId(),"data_sequence" -> task.file().dataSequenceNumber(),"file_sequence" -> task.file().fileSequenceNumber(),"deletes" -> task.deletes().asScala.toVector.map(_.location()))),
      "physical_file" -> physical,"sdk_rows" -> actualRows,"expected_provider_failure_kind" -> kind)
  }
  def initialize(ns: String): Unit = {
    require(IcebergBuild.version()=="1.11.0","This independent SDK oracle requires Iceberg 1.11.0")
    Cases.foreach { name =>
      require(!catalog.tableExists(identifier(ns,name)),"Fixture refuses to reuse an existing table")
      val t=catalog.createTable(identifier(ns,name),schema,PartitionSpec.unpartitioned(),
        Map("format-version" -> "3","write.row-lineage" -> "true").asJava)
      require(t.currentSnapshot()==null,"Fixture CREATE unexpectedly has a data snapshot")
      val expectedPaths=Vector("id","j","n","s","xs","xs.element","js","js.element")
      val fieldIds=expectedPaths.map(path => { val f=t.schema().findField(path); require(f!=null,"CREATE lost an exact fixture field path"); f.fieldId() })
      require(fieldIds.forall(_>0) && fieldIds.distinct.size==8,"CREATE response has invalid or duplicate provider field identities")
      require(t.schema().columns().asScala.map(_.name()).toVector==Vector("id","j","n","s","xs","js"),"CREATE response reordered/changed fixture roots")
      require(t.schema().findField("id").isRequired && expectedPaths.filterNot(_=="id").forall(path => t.schema().findField(path).isOptional),"CREATE response changed exact requiredness")
      for(path <- Vector("n","s","xs.element")) require(t.schema().findType(path)==Types.IntegerType.get(),"CREATE response lost standard INT carrier at " + path)
      for(path <- Vector("j","js.element")) require(t.schema().findType(path)==Types.StringType.get(),"CREATE response lost standard STRING carrier at " + path)
      val records=values(name).map { v => val r=GenericRecord.create(t.schema()); Vector("id","j","n","s","xs","js").zip(v).foreach { case(k,value) => r.setField(k,value) }; r }
      val out=t.io().newOutputFile(t.location().stripSuffix("/")+"/data/domain-input-"+java.util.UUID.randomUUID()+".parquet")
      val writer=new GenericAppenderFactory(t.schema(),t.spec()).set("write.metadata.metrics.default",if(OverflowCases.contains(name)) "none" else "full")
        .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out),FileFormat.PARQUET,partition(t,1))
      try records.foreach(writer.write) finally writer.close()
      val file=writer.toDataFile(); require(file.recordCount()==records.size && file.fileSizeInBytes()>0 && file.fileSizeInBytes()<=MaxFileBytes,"Actual file does not match fixed input rows/byte bounds")
      if(OverflowCases.contains(name)) {
        for(bounds <- Vector(file.lowerBounds(),file.upperBounds())) require(bounds==null||bounds.isEmpty,"Overflow file unexpectedly permits a manifest-bound-only rejection")
      }
      t.newAppend().appendFile(file).commit(); t.refresh()
      val props=properties(name,t.schema()); if(props.nonEmpty) { val update=t.updateProperties(); props.toVector.sortBy(_._1).foreach { case(k,v) => update.set(k,v) }; update.commit(); t.refresh() }
    }
    val frozen=Cases.map(name => fact(name,load(ns,name)))
    emit(obj("record" -> "field_domain_invalid_initial","namespace" -> ns,"tables" -> frozen))
    println("FIELD_DOMAIN_INVALID_READY")
  }
  def before(encoded: String,ns: String): JsonNode = {
    require(encoded.nonEmpty && encoded.length <= ((MaxReceiptBytes+2)/3)*4,"Frozen fixture Base64 exceeds byte bounds")
    val bytes=java.util.Base64.getDecoder.decode(encoded)
    require(bytes.nonEmpty && bytes.length<=MaxReceiptBytes && java.util.Base64.getEncoder.encodeToString(bytes)==encoded,"Frozen fixture receipt has invalid/noncanonical Base64")
    val parser=mapper.getFactory.createParser(bytes)
    parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    val n=try {
      val value=mapper.readTree[JsonNode](parser)
      require(parser.nextToken()==null,"Frozen fixture receipt has trailing JSON")
      value
    } finally parser.close()
    require(n!=null && java.util.Arrays.equals(mapper.writeValueAsBytes(n),bytes),"Frozen fixture receipt is not canonical compact JSON")
    require(n.isObject && n.get("record")!=null && n.get("record").asText()=="field_domain_invalid_initial" && n.get("namespace")!=null && n.get("namespace").asText()==ns && n.get("tables")!=null && n.get("tables").isArray && n.get("tables").size()==Cases.size,"Invalid exact initial fixture receipt")
    n.get("tables").elements().asScala.zip(Cases.iterator).foreach { case(t,name) =>
      require(t.isObject && t.get("case")!=null && t.get("case").isTextual && t.get("case").asText()==name,"Frozen fixture table set/order differs")
      require(t.get("table_uuid")!=null && t.get("table_uuid").isTextual && java.util.UUID.fromString(t.get("table_uuid").asText()).toString==t.get("table_uuid").asText(),"Invalid exact frozen table UUID")
      require(t.get("snapshot")!=null && t.get("snapshot").isIntegralNumber && t.get("snapshot").canConvertToLong && t.get("snapshot").asLong()>0,"Invalid exact frozen positive snapshot")
      require(t.get("schema_id")!=null && t.get("schema_id").isIntegralNumber && t.get("schema_id").canConvertToInt && t.get("schema_id").asInt()>=0,"Invalid frozen schema ID")
      require(t.get("metadata_path")!=null && t.get("metadata_path").isTextual && t.get("metadata_sha256")!=null && t.get("metadata_sha256").asText().matches("[a-f0-9]{64}"),"Invalid frozen metadata fact")
      require(t.get("physical_file")!=null && t.get("physical_file").isObject && t.get("physical_file").get("sha256")!=null && t.get("physical_file").get("sha256").asText().matches("[a-f0-9]{64}"),"Missing exact physical-file hash")
    }
    n
  }
  def observe(ns: String,encoded: String): Unit = {
    val frozen=before(encoded,ns); val current=Cases.map(name => fact(name,load(ns,name)))
    val actual=obj("record" -> "field_domain_invalid_initial","namespace" -> ns,"tables" -> current)
    // Serialize both sides: Jackson integral node widths depend on whether a
    // value came from SDK Long or readTree; JSON numeric meaning remains exact.
    require(mapper.writeValueAsString(actual)==mapper.writeValueAsString(frozen),"Native controls/failures changed exact UUID/metadata/schema/property/snapshot/file/bag facts")
    emit(obj("record" -> "field_domain_invalid_unchanged","namespace" -> ns,"tables" -> current))
    println("FIELD_DOMAIN_INVALID_UNCHANGED")
  }
  def cleanup(ns: String,encoded: String): Unit = {
    val frozen=before(encoded,ns)
    Cases.zipWithIndex.foreach { case(name,index) =>
      val t=load(ns,name); val recorded=frozen.get("tables").get(index)
      require(recorded.get("case").asText()==name && recorded.get("table_uuid").asText()==metadata(t).uuid().toString,"Cleanup refuses a replacement/wrong owned table")
      require(catalog.dropTable(identifier(ns,name),true),"Exact SDK cleanup failed")
    }
    println("FIELD_DOMAIN_INVALID_CLEANED")
  }
}
