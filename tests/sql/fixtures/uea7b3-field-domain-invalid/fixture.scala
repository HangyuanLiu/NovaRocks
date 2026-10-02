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
  val OwnerKey = "uea7b3.fixture.owner.v1"
  val MaxJournalBytes = 4096
  val Phases = Vector("create_confirmed","file_intent","file_closed","append_confirmed","properties_confirmed")
  val Reasons = Set("unknown_create","ownership_conflict","incomplete_journal","storage_error","over_budget")
  def exactKeys(n: JsonNode, keys: Set[String]): Unit =
    require(n != null && n.isObject && n.fieldNames().asScala.toSet == keys,"Fixture lifecycle record has missing/unknown keys")
  def text(n: JsonNode,key: String): String = {
    val v=n.get(key); require(v!=null && v.isTextual && v.asText().nonEmpty && v.asText().getBytes(UTF_8).length<=4096,"Invalid bounded fixture lifecycle text at " + key); v.asText()
  }
  def uuid(n: JsonNode,key: String): String = { val v=text(n,key); require(java.util.UUID.fromString(v).toString==v,"Noncanonical fixture UUID"); v }
  def sha(n: JsonNode,key: String): String = { val v=text(n,key); require(v.matches("[a-f0-9]{64}"),"Invalid fixture SHA256"); v }
  def number(n: JsonNode,key: String,positive: Boolean=false): Long = {
    val v=n.get(key); require(v!=null && v.isIntegralNumber && v.canConvertToLong && (if(positive) v.asLong()>0 else v.asLong()>=0),"Invalid fixture lifecycle integer at " + key); v.asLong()
  }
  def bool(n: JsonNode,key: String): Boolean = { val v=n.get(key); require(v!=null && v.isBoolean,"Invalid fixture lifecycle boolean"); v.asBoolean() }
  def canonical(n: JsonNode): Array[Byte] = mapper.writeValueAsBytes(n)
  def strictBytes(raw: Array[Byte],cap: Int): JsonNode = {
    require(raw.nonEmpty && raw.length<=cap,"Fixture lifecycle JSON exceeds its byte bound")
    val scan=mapper.getFactory.createParser(raw); scan.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    try {
      var depth=0; var nodes=0; var token=scan.nextToken()
      while(token!=null) {
        nodes+=1; require(nodes<=8192,"Fixture lifecycle JSON exceeds its node bound")
        if(token.isStructStart) { depth+=1; require(depth<=16,"Fixture lifecycle JSON exceeds its depth bound") }
        if(token.isStructEnd) depth-=1
        if(token==com.fasterxml.jackson.core.JsonToken.VALUE_STRING || token==com.fasterxml.jackson.core.JsonToken.FIELD_NAME)
          require(scan.getText.getBytes(UTF_8).length<=4096,"Fixture lifecycle JSON string exceeds its text bound")
        token=scan.nextToken()
      }
    } finally scan.close()
    val parser=mapper.getFactory.createParser(raw); parser.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    val n=try { val v=mapper.readTree[JsonNode](parser); require(parser.nextToken()==null,"Fixture lifecycle JSON has trailing input"); v } finally parser.close()
    require(n!=null && java.util.Arrays.equals(raw,canonical(n)),"Fixture lifecycle JSON is not compact canonical JSON"); n
  }
  def decodeLifecycle(encoded: String): JsonNode = {
    require(encoded.nonEmpty && encoded.length<=((MaxReceiptBytes+2)/3)*4,"Fixture lifecycle Base64 exceeds its byte bound")
    val raw=java.util.Base64.getDecoder.decode(encoded)
    require(java.util.Base64.getEncoder.encodeToString(raw)==encoded,"Fixture lifecycle Base64 is not canonical")
    strictBytes(raw,MaxReceiptBytes)
  }
  def owner(encoded: String,ns: String): JsonNode = {
    val n=decodeLifecycle(encoded)
    exactKeys(n,Set("record","version","run_token","namespace","cases","publication_identity_sha256"))
    require(text(n,"record")=="field_domain_fixture_owner" && number(n,"version")==1 && text(n,"namespace")==ns,"Fixture owner identity/version differs")
    uuid(n,"run_token"); sha(n,"publication_identity_sha256")
    val cases=n.get("cases"); require(cases.isArray && cases.elements().asScala.toVector.map { v => require(v.isTextual,"Invalid fixture owner case"); v.asText() }==Cases,"Fixture owner table set/order differs")
    identifier(ns,Cases.head); n
  }
  def token(o: JsonNode,name: String): String = text(o,"run_token") + "/" + name
  def owned(t: Table,o: JsonNode,name: String): Unit =
    require(Option(t.properties().get(OwnerKey)).contains(token(o,name)),"Fixture ownership token differs")
  def fresh(ns: String,name: String): Option[Table] = {
    val c=catalog; val id=identifier(ns,name); c.invalidateTable(id)
    try { val t=c.loadTable(id); t.refresh(); Some(t) }
    catch { case _: org.apache.iceberg.exceptions.NoSuchTableException => None }
  }
  def location(t: Table): String = {
    val p=t.location().stripSuffix("/"); require(p.nonEmpty && p.getBytes(UTF_8).length<=4096 && !p.contains("\n") && !p.contains("\r"),"Invalid fixture table location")
    val uri=new java.net.URI(p); require(Set("s3","s3a","s3n").contains(uri.getScheme) && uri.getHost!=null && uri.getQuery==null && uri.getFragment==null && uri.getUserInfo==null && !uri.getPath.split("/").contains(".."),"Fixture location is not an exact S3 object prefix"); p
  }
  def dataPath(loc: String,o: JsonNode,name: String): String = loc + "/data/domain-input-" + text(o,"run_token") + "-" + name + ".parquet"
  def journalPath(loc: String,o: JsonNode,name: String,phase: String): String = {
    require(Phases.contains(phase),"Unknown fixture journal phase")
    loc + "/_uea7b3_fixture/" + text(o,"run_token") + "/" + name + "/" + (Phases.indexOf(phase)+1) + "-" + phase + ".json"
  }
  def recoveryIO(): org.apache.iceberg.aws.s3.S3FileIO = {
    val prefix="spark.sql.catalog.ice_rest."
    val options=org.apache.spark.sql.SparkSession.active.conf.getAll.iterator.filter { case(k,_) => k.startsWith(prefix) }.map { case(k,v) => k.substring(prefix.length)->v }.toMap
    require(options.get("io-impl").contains("org.apache.iceberg.aws.s3.S3FileIO"),"Fixture recovery requires the explicitly configured S3FileIO")
    Vector("s3.endpoint","s3.region","s3.path-style-access","s3.access-key-id","s3.secret-access-key").foreach(k => require(options.get(k).exists(_.nonEmpty),"Fixture recovery lacks explicit static S3 configuration"))
    val io=new org.apache.iceberg.aws.s3.S3FileIO(); io.initialize(options.asJava); io
  }
  def checkpoint(failAfter: String,name: String,phase: String): Unit =
    if(failAfter==name+":"+phase) throw new IllegalStateException("Explicit field-domain fixture failure after " + name + ":" + phase)
  def journal(t: Table,io: org.apache.iceberg.io.FileIO,o: JsonNode,name: String,phase: String,previous: Option[String],payload: JsonNode): String = {
    owned(t,o,name)
    val n=obj("record" -> "field_domain_fixture_journal","version" -> 1,"run_token" -> text(o,"run_token"),"namespace" -> text(o,"namespace"),"case" -> name,
      "table_uuid" -> metadata(t).uuid().toString,"table_location" -> location(t),"owner_sha256" -> digest(canonical(o)),"phase" -> phase,"previous_sha256" -> previous.orNull,"payload" -> payload)
    val raw=canonical(n); require(raw.length<=MaxJournalBytes,"Fixture journal exceeds its fixed byte bound")
    val path=journalPath(location(t),o,name,phase)
    if(t.io().newInputFile(path).exists()) require(java.util.Arrays.equals(read(t.io(),path,MaxJournalBytes),raw),"Fixture journal replay conflicts")
    else { val out=t.io().newOutputFile(path).create(); try out.write(raw) finally out.close() }
    require(java.util.Arrays.equals(read(t.io(),path,MaxJournalBytes),raw) && java.util.Arrays.equals(read(io,path,MaxJournalBytes),raw),"Fixture journal was not durably verified through both exact IO owners")
    digest(raw)
  }
  def metadataPayload(t: Table): JsonNode = { val m=metadata(t); obj("metadata_path" -> m.metadataFileLocation(),"metadata_sha256" -> digest(read(t.io(),m.metadataFileLocation(),MaxMetadataBytes))) }
  def terminal(encoded: String,o: JsonNode): JsonNode = {
    val n=decodeLifecycle(encoded)
    exactKeys(n,Set("record","version","run_token","phase","job_token","container_id","image_id","image_reference","script_sha256","defaults_sha256","publication_identity_sha256","execution_confirmed","exit_code","confirmed_gone","forced"))
    require(text(n,"record")=="field_domain_owned_spark_terminal" && number(n,"version")==1 && uuid(n,"run_token")==text(o,"run_token") && text(n,"phase")=="initialize" && sha(n,"publication_identity_sha256")==text(o,"publication_identity_sha256"),"Initializer terminal identity differs")
    uuid(n,"job_token"); require(text(n,"container_id").matches("[a-f0-9]{64}") && text(n,"image_id").matches("sha256:[a-f0-9]{64}"),"Initializer terminal lacks its exact container/image identity")
    text(n,"image_reference"); sha(n,"script_sha256"); sha(n,"defaults_sha256"); bool(n,"execution_confirmed"); bool(n,"forced")
    val code=n.get("exit_code"); require(code!=null && (code.isNull || (code.isIntegralNumber && code.canConvertToInt)),"Initializer terminal has invalid exit code")
    require(bool(n,"confirmed_gone"),"Initializer has no confirmed remote termination"); n
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
  def initialize(ns: String,ownerBase64: String,failAfter: String=""): Unit = {
    val o=owner(ownerBase64,ns)
    require(failAfter.isEmpty || Cases.exists(name => Phases.exists(phase => failAfter==name+":"+phase)),"Invalid closed fixture failure checkpoint")
    val recovery=recoveryIO()
    try {
    require(IcebergBuild.version()=="1.11.0","This independent SDK oracle requires Iceberg 1.11.0")
    Cases.foreach { name =>
      require(!catalog.tableExists(identifier(ns,name)),"Fixture refuses to reuse an existing table")
      val t=catalog.createTable(identifier(ns,name),schema,PartitionSpec.unpartitioned(),
        Map("format-version" -> "3","write.row-lineage" -> "true",OwnerKey -> token(o,name)).asJava)
      require(t.currentSnapshot()==null,"Fixture CREATE unexpectedly has a data snapshot")
      val expectedPaths=Vector("id","j","n","s","xs","xs.element","js","js.element")
      val fieldIds=expectedPaths.map(path => { val f=t.schema().findField(path); require(f!=null,"CREATE lost an exact fixture field path"); f.fieldId() })
      require(fieldIds.forall(_>0) && fieldIds.distinct.size==8,"CREATE response has invalid or duplicate provider field identities")
      require(t.schema().columns().asScala.map(_.name()).toVector==Vector("id","j","n","s","xs","js"),"CREATE response reordered/changed fixture roots")
      require(t.schema().findField("id").isRequired && expectedPaths.filterNot(_=="id").forall(path => t.schema().findField(path).isOptional),"CREATE response changed exact requiredness")
      for(path <- Vector("n","s","xs.element")) require(t.schema().findType(path)==Types.IntegerType.get(),"CREATE response lost standard INT carrier at " + path)
      for(path <- Vector("j","js.element")) require(t.schema().findType(path)==Types.StringType.get(),"CREATE response lost standard STRING carrier at " + path)
      val created=metadataPayload(t)
      var previous=journal(t,recovery,o,name,"create_confirmed",None,obj("schema_id" -> t.schema().schemaId(),"metadata_path" -> created.get("metadata_path"),"metadata_sha256" -> created.get("metadata_sha256")))
      checkpoint(failAfter,name,"create_confirmed")
      val path=dataPath(location(t),o,name)
      previous=journal(t,recovery,o,name,"file_intent",Some(previous),obj("data_path" -> path))
      checkpoint(failAfter,name,"file_intent")
      require(!t.io().newInputFile(path).exists(),"Fixture refuses to reopen an existing registered data object")
      val records=values(name).map { v => val r=GenericRecord.create(t.schema()); Vector("id","j","n","s","xs","js").zip(v).foreach { case(k,value) => r.setField(k,value) }; r }
      val out=t.io().newOutputFile(path)
      val writer=new GenericAppenderFactory(t.schema(),t.spec()).set("write.metadata.metrics.default",if(OverflowCases.contains(name)) "none" else "full")
        .newDataWriter(EncryptedFiles.plainAsEncryptedOutput(out),FileFormat.PARQUET,partition(t,1))
      try records.foreach(writer.write) finally writer.close()
      val file=writer.toDataFile(); require(file.recordCount()==records.size && file.fileSizeInBytes()>0 && file.fileSizeInBytes()<=MaxFileBytes,"Actual file does not match fixed input rows/byte bounds")
      if(OverflowCases.contains(name)) {
        for(bounds <- Vector(file.lowerBounds(),file.upperBounds())) require(bounds==null||bounds.isEmpty,"Overflow file unexpectedly permits a manifest-bound-only rejection")
      }
      previous=journal(t,recovery,o,name,"file_closed",Some(previous),obj("data_path" -> path,"file_size" -> file.fileSizeInBytes(),"record_count" -> file.recordCount(),"sha256" -> digest(read(t.io(),path,MaxFileBytes))))
      checkpoint(failAfter,name,"file_closed")
      t.newAppend().appendFile(file).commit(); t.refresh()
      val appended=metadataPayload(t)
      previous=journal(t,recovery,o,name,"append_confirmed",Some(previous),obj("snapshot_id" -> t.currentSnapshot().snapshotId(),"sequence_number" -> t.currentSnapshot().sequenceNumber(),"metadata_path" -> appended.get("metadata_path"),"metadata_sha256" -> appended.get("metadata_sha256")))
      checkpoint(failAfter,name,"append_confirmed")
      val props=properties(name,t.schema()); if(props.nonEmpty) { val update=t.updateProperties(); props.toVector.sortBy(_._1).foreach { case(k,v) => update.set(k,v) }; update.commit(); t.refresh() }
      val updated=metadataPayload(t)
      val propertiesBytes=canonical(obj("properties" -> t.properties().asScala.toVector.sortBy(_._1).map { case(k,v) => obj("key" -> k,"value" -> v) }))
      previous=journal(t,recovery,o,name,"properties_confirmed",Some(previous),obj("snapshot_id" -> t.currentSnapshot().snapshotId(),"sequence_number" -> t.currentSnapshot().sequenceNumber(),"metadata_path" -> updated.get("metadata_path"),"metadata_sha256" -> updated.get("metadata_sha256"),"properties_sha256" -> digest(propertiesBytes)))
      checkpoint(failAfter,name,"properties_confirmed")
    }
    val frozen=Cases.map(name => fact(name,load(ns,name)))
    emit(obj("record" -> "field_domain_invalid_initial","namespace" -> ns,"tables" -> frozen))
    println("FIELD_DOMAIN_INVALID_READY")
    } finally recovery.close()
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
  def objectPath(loc: String,path: String): Unit =
    require(path.getBytes(UTF_8).length<=4096 && path.startsWith(loc+"/metadata/") && !path.substring(loc.length).split("/").contains("..") && !path.contains("\n") && !path.contains("\r"),"SDK object is outside the exact owned metadata prefix")
  def validateJournal(n: JsonNode,o: JsonNode,name: String,id: String,loc: String,phase: String,previous: Option[String]): Unit = {
    exactKeys(n,Set("record","version","run_token","namespace","case","table_uuid","table_location","owner_sha256","phase","previous_sha256","payload"))
    require(text(n,"record")=="field_domain_fixture_journal" && number(n,"version")==1 && uuid(n,"run_token")==text(o,"run_token") && text(n,"namespace")==text(o,"namespace") && text(n,"case")==name && uuid(n,"table_uuid")==id && text(n,"table_location")==loc && sha(n,"owner_sha256")==digest(canonical(o)) && text(n,"phase")==phase,"Journal exact identity differs")
    val prior=n.get("previous_sha256")
    require(prior!=null && (previous match { case None => prior.isNull; case Some(h) => prior.isTextual && prior.asText()==h }),"Journal predecessor differs")
    val p=n.get("payload")
    phase match {
      case "create_confirmed" => exactKeys(p,Set("schema_id","metadata_path","metadata_sha256")); require(number(p,"schema_id")<=Int.MaxValue,"Invalid journal schema ID")
      case "file_intent" => exactKeys(p,Set("data_path"))
      case "file_closed" =>
        exactKeys(p,Set("data_path","file_size","record_count","sha256")); require(number(p,"file_size",true)<=MaxFileBytes && number(p,"record_count",true)==values(name).size,"Closed file facts differ from fixed inputs"); sha(p,"sha256")
      case "append_confirmed" => exactKeys(p,Set("snapshot_id","sequence_number","metadata_path","metadata_sha256")); number(p,"snapshot_id",true); number(p,"sequence_number")
      case "properties_confirmed" => exactKeys(p,Set("snapshot_id","sequence_number","metadata_path","metadata_sha256","properties_sha256")); number(p,"snapshot_id",true); number(p,"sequence_number"); sha(p,"properties_sha256")
      case _ => throw new IllegalArgumentException("Unknown journal phase")
    }
    if(p.has("data_path")) require(text(p,"data_path")==dataPath(loc,o,name),"Journal data path differs from its exact intent")
    if(p.has("metadata_path")) { objectPath(loc,text(p,"metadata_path")); sha(p,"metadata_sha256") }
  }
  def readJournals(io: org.apache.iceberg.io.FileIO,o: JsonNode,name: String,id: String,loc: String): Vector[(String,String,JsonNode)] = {
    val found=scala.collection.mutable.ArrayBuffer.empty[(String,String,JsonNode)]
    var gap=false; var previous: Option[String]=None
    Phases.foreach { phase =>
      val path=journalPath(loc,o,name,phase)
      if(io.newInputFile(path).exists()) {
        require(!gap,"Journal chain contains a missing predecessor")
        val raw=read(io,path,MaxJournalBytes); val n=strictBytes(raw,MaxJournalBytes)
        validateJournal(n,o,name,id,loc,phase,previous)
        val h=digest(raw); found += ((phase,h,n)); previous=Some(h)
      } else gap=true
    }
    found.toVector
  }
  def knownObjects(t: Table): Vector[JsonNode] = {
    val m=metadata(t); val loc=location(t)
    val previous=m.previousFiles().asScala.iterator.take(9).toVector
    require(previous.size<=8,"SDK metadata history exceeds fixed fixture inventory")
    val snapshots=m.snapshots().asScala.iterator.take(2).toVector
    require(snapshots.size<=1,"SDK snapshots exceed the fixed fixture inventory")
    val paths=scala.collection.mutable.ArrayBuffer.empty[(String,String)]
    paths += (("metadata",m.metadataFileLocation()))
    previous.foreach(f => paths += (("metadata",f.file())))
    snapshots.foreach { s =>
      val list=s.manifestListLocation(); require(list!=null,"Actual snapshot has no manifest list")
      read(t.io(),list,MaxMetadataBytes); paths += (("manifest_list",list))
      val manifests=s.allManifests(t.io()).asScala.iterator.take(2).toVector
      require(manifests.size<=1,"SDK manifests exceed the fixed fixture inventory")
      manifests.foreach(f => { require(f.length()>0 && f.length()<=MaxMetadataBytes,"SDK manifest exceeds the fixture byte bound"); paths += (("manifest",f.path())) })
    }
    val unique=paths.distinct.sortBy(x => (x._1,x._2)).toVector
    require(unique.size<=8 && unique.map(_._2).distinct.size==unique.size,"SDK object inventory has duplicate/incompatible roles")
    unique.map { case(kind,path) => objectPath(loc,path); obj("kind" -> kind,"path" -> path) }
  }
  def unresolved(name: String,reason: String): JsonNode = {
    require(Reasons.contains(reason),"Unknown fixture unresolved reason")
    obj("case" -> name,"state" -> "unresolved","reason" -> reason)
  }
  def prepareCleanup(ns: String,ownerBase64: String,terminalBase64: String,optionalInitialBase64: Option[String]): Unit = {
    val o=owner(ownerBase64,ns); val ended=terminal(terminalBase64,o)
    val initial=optionalInitialBase64.map(encoded => before(encoded,ns))
    val io=recoveryIO()
    try {
      val tables=Cases.zipWithIndex.map { case(name,index) =>
        try {
          fresh(ns,name) match {
            case None => unresolved(name,"unknown_create")
            case Some(t) if !Option(t.properties().get(OwnerKey)).contains(token(o,name)) => unresolved(name,"ownership_conflict")
            case Some(t) =>
              val id=metadata(t).uuid().toString; val loc=location(t)
              initial.foreach(n => require(n.get("tables").get(index).get("table_uuid").asText()==id,"Initial receipt differs from actual table UUID"))
              val observed=readJournals(io,o,name,id,loc)
              // Recover an unknown CREATE response from its atomic owner token.
              // Persist the observed UUID before ACK so post-DROP replay retains it.
              if(observed.isEmpty) {
                require(t.currentSnapshot()==null,"Recovered CREATE has no durable data-file intent")
                val created=metadataPayload(t)
                journal(t,io,o,name,"create_confirmed",None,obj("schema_id" -> t.schema().schemaId(),"metadata_path" -> created.get("metadata_path"),"metadata_sha256" -> created.get("metadata_sha256")))
              }
              val chain=readJournals(io,o,name,id,loc)
              val intent=chain.find(_._1=="file_intent")
              val closed=chain.find(_._1=="file_closed")
              if(t.currentSnapshot()!=null) {
                require(intent.isDefined,"Actual snapshot has no durable file intent")
                val current=files(t)
                require(current.size==1 && current.head.file().location()==dataPath(loc,o,name),"Actual file inventory differs from the registered path")
              }
              val registered=intent.toVector.map { _ =>
                val closedFact=closed.map { x => val p=x._3.get("payload"); obj("file_size" -> p.get("file_size"),"record_count" -> p.get("record_count"),"sha256" -> p.get("sha256")) }.orNull
                obj("path" -> dataPath(loc,o,name),"closed_fact" -> closedFact)
              }
              obj("case" -> name,"state" -> "owned_present","identity" -> obj("table_uuid" -> id,"table_location" -> loc),
                "create_evidence" -> (if(observed.nonEmpty) "journal" else "recovered_token"),
                "journal_objects" -> chain.map { x => obj("phase" -> x._1,"path" -> journalPath(loc,o,name,x._1),"sha256" -> x._2) },
                "registered_data_files" -> registered,"known_sdk_objects" -> knownObjects(t),"unproven_sdk_orphans" -> true)
          }
        } catch {
          case _: IllegalArgumentException => unresolved(name,"incomplete_journal")
          case scala.util.control.NonFatal(_) => unresolved(name,"storage_error")
        }
      }
      emit(obj("record" -> "field_domain_cleanup_candidate","version" -> 1,"run_token" -> text(o,"run_token"),"namespace" -> ns,"owner_sha256" -> digest(canonical(o)),
        "terminal_sha256" -> digest(canonical(ended)),"publication_identity_sha256" -> text(o,"publication_identity_sha256"),"tables" -> tables))
    } finally io.close()
  }
  def validateCandidateTable(n: JsonNode,o: JsonNode,name: String): Unit = {
    require(text(n,"case")==name,"Cleanup candidate case set/order differs")
    if(text(n,"state")=="unresolved") {
      exactKeys(n,Set("case","state","reason")); require(Reasons.contains(text(n,"reason")),"Unknown candidate unresolved reason")
    } else {
      exactKeys(n,Set("case","state","identity","create_evidence","journal_objects","registered_data_files","known_sdk_objects","unproven_sdk_orphans"))
      require(Set("owned_present","owned_absent").contains(text(n,"state")) && bool(n,"unproven_sdk_orphans"),"Invalid owned candidate variant")
      require(Set("journal","recovered_token","durable_candidate").contains(text(n,"create_evidence")),"Invalid CREATE ownership evidence")
      val identity=n.get("identity"); exactKeys(identity,Set("table_uuid","table_location")); uuid(identity,"table_uuid")
      val loc=text(identity,"table_location"); val uri=new java.net.URI(loc)
      require(Set("s3","s3a","s3n").contains(uri.getScheme) && uri.getHost!=null && uri.getQuery==null && uri.getFragment==null && uri.getUserInfo==null && !uri.getPath.split("/").contains("..") && !loc.endsWith("/"),"Invalid frozen exact table location")
      val journals=n.get("journal_objects"); require(journals.isArray && journals.size()<=Phases.size,"Invalid candidate journal count")
      journals.elements().asScala.zipWithIndex.foreach { case(j,index) => exactKeys(j,Set("phase","path","sha256")); require(text(j,"phase")==Phases(index) && text(j,"path")==journalPath(loc,o,name,Phases(index)),"Candidate journal path/order differs"); sha(j,"sha256") }
      val data=n.get("registered_data_files"); require(data.isArray && data.size()<=1,"Invalid candidate registered file count")
      require((journals.size()>=2)==(data.size()==1),"Candidate file intent/registration differs")
      data.elements().asScala.foreach { f =>
        exactKeys(f,Set("path","closed_fact")); require(text(f,"path")==dataPath(loc,o,name),"Candidate data file is outside exact intent")
        val closed=f.get("closed_fact"); require(closed!=null,"Missing candidate closed fact")
        if(!closed.isNull) { exactKeys(closed,Set("file_size","record_count","sha256")); require(number(closed,"file_size",true)<=MaxFileBytes && number(closed,"record_count",true)==values(name).size,"Candidate closed file differs"); sha(closed,"sha256") }
        require((journals.size()>=3)== !closed.isNull,"Candidate closed-file phase differs")
      }
      val objects=n.get("known_sdk_objects"); require(objects.isArray && objects.size()<=8,"Candidate SDK inventory exceeds its bound")
      val seen=scala.collection.mutable.Set.empty[String]
      objects.elements().asScala.foreach { f => exactKeys(f,Set("kind","path")); require(Set("metadata","manifest_list","manifest").contains(text(f,"kind")),"Invalid SDK object kind"); val p=text(f,"path"); objectPath(loc,p); require(seen.add(p),"Duplicate SDK object path") }
    }
  }
  def verifyCandidateTable(n: JsonNode,o: JsonNode,io: org.apache.iceberg.io.FileIO): Option[Table] = {
    val name=text(n,"case"); val identity=n.get("identity"); val id=uuid(identity,"table_uuid"); val loc=text(identity,"table_location")
    val current=fresh(text(o,"namespace"),name)
    current.foreach { t => owned(t,o,name); require(metadata(t).uuid().toString==id && location(t)==loc,"Cleanup refuses a replacement table UUID/location") }
    val chain=readJournals(io,o,name,id,loc)
    val refs=n.get("journal_objects").elements().asScala.toVector
    require(chain.size==refs.size && chain.zip(refs).forall { case(x,r) => x._1==text(r,"phase") && x._2==sha(r,"sha256") },"Cleanup journal chain changed after durable ACK")
    require(chain.nonEmpty || current.isDefined,"Absent table lacks an external exact UUID journal")
    current.foreach { t =>
      val actual=knownObjects(t).map(n => new String(canonical(n),UTF_8))
      val expected=n.get("known_sdk_objects").elements().asScala.map(n => new String(canonical(n),UTF_8)).toVector
      require(actual==expected,"SDK metadata inventory changed after durable ACK")
      if(t.currentSnapshot()!=null) require(n.get("registered_data_files").size()==1 && files(t).head.file().location()==dataPath(loc,o,name),"Actual file changed after durable ACK")
    }
    n.get("registered_data_files").elements().asScala.foreach { f =>
      val p=chain.find(_._1=="file_closed").map(_._3.get("payload"))
      val cf=f.get("closed_fact")
      require(p.isDefined== !cf.isNull,"Closed file fact lacks its authoritative journal")
      p.foreach(x => require(number(x,"file_size")==number(cf,"file_size") && number(x,"record_count")==number(cf,"record_count") && sha(x,"sha256")==sha(cf,"sha256"),"Closed file fact changed after ACK"))
    }
    current
  }
  def commitCleanup(ns: String,ownerBase64: String,candidateBase64: String,ackBase64: String): Unit = {
    val o=owner(ownerBase64,ns); val candidate=decodeLifecycle(candidateBase64); val ack=decodeLifecycle(ackBase64)
    exactKeys(candidate,Set("record","version","run_token","namespace","owner_sha256","terminal_sha256","publication_identity_sha256","tables"))
    require(text(candidate,"record")=="field_domain_cleanup_candidate" && number(candidate,"version")==1 && uuid(candidate,"run_token")==text(o,"run_token") && text(candidate,"namespace")==ns && sha(candidate,"owner_sha256")==digest(canonical(o)) && sha(candidate,"publication_identity_sha256")==text(o,"publication_identity_sha256"),"Cleanup candidate identity differs")
    sha(candidate,"terminal_sha256")
    exactKeys(ack,Set("record","version","run_token","namespace","owner_sha256","candidate_sha256"))
    require(text(ack,"record")=="field_domain_cleanup_ack" && number(ack,"version")==1 && uuid(ack,"run_token")==text(o,"run_token") && text(ack,"namespace")==ns && sha(ack,"owner_sha256")==digest(canonical(o)) && sha(ack,"candidate_sha256")==digest(canonical(candidate)),"Cleanup durable ACK identity differs")
    val tables=candidate.get("tables"); require(tables.isArray && tables.size()==Cases.size,"Cleanup candidate has incomplete table set")
    val records=tables.elements().asScala.toVector
    records.zip(Cases).foreach { case(n,name) => validateCandidateTable(n,o,name) }
    val io=recoveryIO()
    try {
      // Validate every known identity and journal before the first deletion.
      // A conflict never grants deletion for that case; other closed cases may converge.
      val checked=records.map { n =>
        if(text(n,"state")=="unresolved") Left(text(n,"reason"))
        else try { Right(verifyCandidateTable(n,o,io)) } catch {
          case _: IllegalArgumentException => Left("ownership_conflict")
          case scala.util.control.NonFatal(_) => Left("storage_error")
        }
      }
      val results=records.zip(checked).map { case(n,check) =>
        val name=text(n,"case")
        check match {
          case Left(reason) => unresolved(name,reason)
          case Right(_) =>
            try {
              // Repeat immediately before DROP: no cached candidate authorizes a replacement.
              val current=verifyCandidateTable(n,o,io)
              if(current.isDefined) catalog.dropTable(identifier(ns,name),true)
              require(fresh(ns,name).isEmpty,"Catalog still has a table after exact cleanup")
              val data=n.get("registered_data_files").elements().asScala.toVector.map { f =>
                val path=text(f,"path"); require(fresh(ns,name).isEmpty,"Cleanup refuses a replacement table before object deletion"); if(io.newInputFile(path).exists()) io.deleteFile(path)
                obj("path" -> path,"absent" -> !io.newInputFile(path).exists())
              }
              val objects=n.get("known_sdk_objects").elements().asScala.toVector.map { f =>
                val path=text(f,"path"); require(fresh(ns,name).isEmpty,"Cleanup refuses a replacement table before object deletion"); if(io.newInputFile(path).exists()) io.deleteFile(path)
                obj("kind" -> text(f,"kind"),"path" -> path,"absent" -> !io.newInputFile(path).exists())
              }
              val journals=n.get("journal_objects").elements().asScala.toVector.map { j =>
                require(digest(read(io,text(j,"path"),MaxJournalBytes))==sha(j,"sha256"),"Cleanup lost its retained journal")
                obj("phase" -> text(j,"phase"),"path" -> text(j,"path"),"retained" -> true)
              }
              obj("case" -> name,"state" -> "owned","table_uuid" -> uuid(n.get("identity"),"table_uuid"),"catalog_absent" -> true,
                "registered_data_files" -> data,"known_sdk_objects" -> objects,"journal_objects" -> journals,"unproven_sdk_orphans" -> true,
                "unresolved" -> (if(data.forall(_.get("absent").asBoolean()) && objects.forall(_.get("absent").asBoolean())) Vector.empty[String] else Vector("storage_error")))
            } catch {
              case _: IllegalArgumentException => unresolved(name,"ownership_conflict")
              case scala.util.control.NonFatal(_) => unresolved(name,"storage_error")
            }
        }
      }
      val complete=results.forall(n => text(n,"state")=="owned" && n.get("unresolved").isArray && n.get("unresolved").size()==0)
      emit(obj("record" -> "field_domain_cleanup_result","version" -> 1,"run_token" -> text(o,"run_token"),"namespace" -> ns,
        "owner_sha256" -> digest(canonical(o)),"candidate_sha256" -> digest(canonical(candidate)),"complete" -> complete,"tables" -> results))
      if(complete) println("FIELD_DOMAIN_INVALID_CLEANED")
    } finally io.close()
  }
}
