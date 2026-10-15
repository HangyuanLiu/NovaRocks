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

import scala.collection.JavaConverters._
import org.apache.iceberg._
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.data.{GenericRecord, IcebergGenerics, Record}
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.{Parquet, ParquetValueWriter, VariantShreddingFunction}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.types.Types
import org.apache.iceberg.variants.{Variant, VariantValue, Variants}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{Type => ParquetType, Types => ParquetTypes, PrimitiveType => ParquetPrimitiveType}

object VariantFixture {
  var spark: org.apache.spark.sql.SparkSession = _
  var namespace = ""
  var tableName = ""
  def initialize(session: org.apache.spark.sql.SparkSession, ns: String, name: String): Unit = {
    require(ns.matches("[a-zA-Z0-9_]+") && name.matches("[a-zA-Z0-9_]+"))
    require(IcebergBuild.version() == "1.11.0", "IRU-9 fixture requires pinned Iceberg 1.11.0")
    spark = session; namespace = ns; tableName = name
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS ice_rest.$ns")
  }
  def load(): Table = Spark3Util.loadIcebergCatalog(spark,"ice_rest").loadTable(TableIdentifier.of(namespace,tableName))
  def create(schema: Schema, spec: PartitionSpec = PartitionSpec.unpartitioned(), properties: Map[String,String] = Map.empty): Table =
    Spark3Util.loadIcebergCatalog(spark,"ice_rest").createTable(
      TableIdentifier.of(namespace,tableName),schema,spec,
      (Map("format-version"->"3","write.metadata.metrics.default"->"full") ++ properties).asJava)
  def row(schema: Schema, values: Seq[Any]): Record = {
    val record = GenericRecord.create(schema)
    values.zipWithIndex.foreach{case (value,index) => record.set(index,value.asInstanceOf[AnyRef])};record
  }
  def standardSchema(): Schema = new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.optional(2,"v",Types.VariantType.get()))
  def variant(value: VariantValue): Variant = Variant.of(Variants.emptyMetadata(),value)
  def objectVariant(): Variant = {
    val metadata = Variants.metadata("a","nested","text" )
    val nested = Variants.`object`(metadata); nested.put("a",Variants.of(7))
    val value = Variants.`object`(metadata);value.put("a",Variants.of(42));value.put("nested",nested);value.put("text",Variants.of("hello"))
    Variant.of(metadata,value)
  }
  def values(): Seq[Any] = {
    val array = Variants.array();array.add(Variants.of(1));array.add(Variants.ofNull());array.add(Variants.of("x"))
    Seq(null,variant(Variants.ofNull()),objectVariant(),variant(array),
      variant(Variants.of(true)),variant(Variants.of(false)),
      variant(Variants.of(12.toByte)),variant(Variants.of(1234.toShort)),variant(Variants.of(123456)),variant(Variants.of(9876543210L)),
      variant(Variants.of(1.25f)),variant(Variants.of(2.5d)),
      variant(Variants.of(new java.math.BigDecimal("12.34"))),
      variant(Variants.of(new java.math.BigDecimal("123456789.12"))),
      variant(Variants.of(new java.math.BigDecimal("12345678901234567890.12"))),
      variant(Variants.ofIsoDate("2024-01-02")),variant(Variants.ofIsoTimestamptz("2024-01-02T03:04:05.123456+00:00")),
      variant(Variants.ofIsoTimestampntz("2024-01-02T03:04:05.123456")),
      variant(Variants.of(java.nio.ByteBuffer.wrap(Array[Byte](0,-1)))),
      variant(Variants.of("a" * 70)),variant(Variants.ofIsoTime("03:04:05.123456")),
      variant(Variants.ofIsoTimestamptzNanos("2024-01-02T03:04:05.123456789+00:00")),
      variant(Variants.ofIsoTimestampntzNanos("2024-01-02T03:04:05.123456789")),
      variant(Variants.ofUUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56")))
  }
  def write(t: Table, records: Seq[Seq[Any]], shredded: Boolean = false, bounds: Boolean = false): DataFile = {
    val output = t.io().newOutputFile(t.location()+"/data/iru9-"+java.util.UUID.randomUUID()+".parquet")
    val builder = Parquet.write(output).schema(t.schema())
      .createWriterFunc(new java.util.function.Function[org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType,ParquetValueWriter[_]] {
        override def apply(fileSchema: org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType): ParquetValueWriter[_] = GenericParquetWriter.create(t.schema(),fileSchema)
      })
    if (shredded) builder.variantShreddingFunc(new VariantShreddingFunction {
      override def apply(id: Integer,name: String): ParquetType = ParquetTypes.optional(ParquetPrimitiveType.PrimitiveTypeName.INT32).named("typed_value")
    })
    val writer = builder.build[Record]()
    try records.foreach(values => writer.add(row(t.schema(),values))) finally writer.close()
    val original = writer.metrics()
    require(original.recordCount().longValue() == records.size.toLong,"writer row count differs from fixture inputs")
    // Ordinary read fixtures omit Variant bounds so they exercise data decoding;
    // the bounds fixture deliberately retains the writer's real Variant bounds.
    def filtered(map: java.util.Map[Integer,java.nio.ByteBuffer]) = {
      if (map == null || bounds) map else map.asScala.filterNot { case (id,_) =>
        Option(t.schema().findType(id.intValue())).exists(_.typeId() == org.apache.iceberg.types.Type.TypeID.VARIANT)
      }.toMap.asJava
    }
    val metrics = new Metrics(original.recordCount(),original.columnSizes(),original.valueCounts(),original.nullValueCounts(),original.nanValueCounts(),filtered(original.lowerBounds()),filtered(original.upperBounds()))
    val file = DataFiles.builder(t.spec()).withPath(output.location()).withFormat(FileFormat.PARQUET)
      .withFileSizeInBytes(t.io().newInputFile(output.location()).getLength()).withMetrics(metrics).build()
    if (bounds) require(Option(file.lowerBounds()).exists(_.containsKey(Int.box(2))) && Option(file.upperBounds()).exists(_.containsKey(Int.box(2))),"fixture must carry actual Variant bounds")
    t.newAppend().appendFile(file).commit();t.refresh();file
  }
  def physicalData(t: Table): String = t.io().asInstanceOf[org.apache.iceberg.io.SupportsPrefixOperations]
    .listPrefix(t.location()+"/data/").asScala.map(_.location()).toVector.sorted.mkString("|")
  def snapshot(t: Table): String = Option(t.currentSnapshot()).map(_.snapshotId().toString).getOrElse("absent")
  def remember(t: Table): Unit = {
    t.updateProperties().set("iru9.fixture.snapshot",snapshot(t))
      .set("iru9.fixture.data-files",physicalData(t)).commit()
  }
  // These targets replace NovaRocks-authored empty tables. Appending even an
  // empty file would change their missing-statistics and no-snapshot behavior.
  def empty(schema: Schema, spec: PartitionSpec = PartitionSpec.unpartitioned(), properties: Map[String,String] = Map.empty): Unit = {
    val t=create(schema,spec,properties)
    t.refresh()
    require(t.schema().asStruct() == schema.asStruct(),"empty fixture changed field IDs, types or nullability")
    def fields(s: PartitionSpec) = s.fields().asScala.map(f => (f.sourceId(),f.name(),f.transform().toString)).toVector
    require(fields(t.spec()) == fields(spec),"empty fixture changed partition fields")
    properties.foreach { case (key,value) => require(t.properties().get(key) == value,"empty fixture lost property "+key) }
    require(t.currentSnapshot() == null && !t.snapshots().iterator().hasNext(),"empty fixture must have no snapshot")
    require(physicalData(t).isEmpty,"empty fixture must have no physical data files")
    remember(t)
    val persisted=load()
    require(persisted.asInstanceOf[HasTableOperations].operations().current().formatVersion() == 3,"empty fixture must retain format-version 3")
    require(persisted.currentSnapshot() == null && !persisted.snapshots().iterator().hasNext(),"remembering an empty fixture created a snapshot")
  }
  def unchanged(): Unit = {
    val t=load();require(snapshot(t) == t.properties().get("iru9.fixture.snapshot"),"rejected write changed snapshot")
    require(physicalData(t)==t.properties().get("iru9.fixture.data-files"),"rejected write created physical data files")
    println("IRU9_UNCHANGED_OK")
  }
  def observe(): Unit = {
    val reader=IcebergGenerics.read(load()).build()
    try {
      val ids=reader.asScala.map(_.getField("id").asInstanceOf[Integer].intValue()).toVector.sorted
      println("IRU9_JAVA_IDS "+(if(ids.isEmpty) "EMPTY" else ids.mkString(",")))
    } finally reader.close()
  }
}
