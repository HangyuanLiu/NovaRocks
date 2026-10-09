#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Caller-owned stock HMS external bulk preparation, oracle and cleanup.

This library has no service-running CLI or NovaRocks write assertions. A caller
must supply reviewed source freezes and absolute monotonic clocks. INPUT_READY
certifies external input only. Native settlement is a separate caller gate.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from contextlib import contextmanager
import importlib.util
import math
import os
from pathlib import Path
import re
import time
import threading

BASE_PATH = "docs/testing/mem-1-m07/scripts/prepare_real_hms_capability.py"
CL_PATH = "docs/testing/mem-1-m07/inputs/cl-listing-freeze-v3.json"
CL_SHA = "e279724dc4ab2ce34dfdef5f3a939ad3f0f05ed076c36c5c60a7b7d7c6c1a3d3"
PREFIX = b"NR_HMS_READONLY_CL|"
FILE_CAP = OUTPUT_CAP = METADATA_CAP = 1_048_576
MARKER_CAP = 65_536
MARKER_COUNT = 1024
NORMAL = {"namespaces":32, "tables_per_namespace":512, "views_per_namespace":512,
    "page_size":256, "namespace_pattern":"cl_ns_%04d", "table_pattern":"cl_table_%06d",
    "view_pattern":"cl_view_%06d", "concurrency":[1,8,16]}
SHARDS = tuple((namespace, shard) for namespace in range(32) for shard in range(4))
CATALOG = "m07_hms_readonly_cl"


def need(ok, message):
    if not ok:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class CallerClocks:
    preparation_until: float
    verification_until: float
    cleanup_until: float

    def validate(self, now=None):
        now = time.monotonic() if now is None else now
        values = (self.preparation_until, self.verification_until, self.cleanup_until)
        need(all(type(value) in (int, float) and math.isfinite(value) for value in values),
            "caller absolute clocks must be finite")
        need(now < values[0] <= values[1] <= values[2], "caller absolute clocks are expired or unordered")


def names(namespace):
    need(type(namespace) is int and 0 <= namespace < 32, "namespace index differs")
    return f"cl_ns_{namespace:04}", [f"cl_table_{i:06}" for i in range(512)], [f"cl_view_{i:06}" for i in range(512)]


def stage_id(kind, namespace=None, shard=None):
    need(kind in ("baseline", "create", "before", "after", "drop", "restored"), "unknown bulk stage")
    if kind in ("baseline", "restored"):
        need(namespace is None and shard is None, "root stage acquired object scope")
        return kind
    need(type(namespace) is int and type(shard) is int and (namespace, shard) in SHARDS, "bulk shard differs")
    return f"{kind}-n{namespace:04}-s{shard}"


def mutations(kind, namespace, shard):
    sid = stage_id(kind, namespace, shard)
    need(kind in ("create", "drop"), "non-mutating stage requested mutations")
    ns, tables, views = names(namespace)
    result = [("create_namespace", ns)] if kind == "create" and shard == 0 else []
    for i in range(shard * 128, (shard + 1) * 128):
        result.extend((("create_table", tables[i]), ("create_view", views[i])) if kind == "create"
            else (("drop_view", views[i]), ("drop_table", tables[i])))
    if kind == "drop" and shard == 3:
        result.append(("drop_namespace", ns))
    return [{"operation":op, "namespace":ns, "object":obj} for op, obj in result]


def load_base(repo, base_freeze):
    repo = Path(repo).resolve(strict=True)
    path = repo / BASE_PATH
    need(path.stat().st_size <= FILE_CAP and path.is_file(), "base helper exceeds local file bound")
    with path.open("rb") as stream:
        raw = stream.read(FILE_CAP + 1)
    need(len(raw) <= FILE_CAP and digest(raw) == base_freeze["helper_sha256"], "pinned base helper differs")
    spec = importlib.util.spec_from_file_location("m07_bulk_pinned_stock_owner", path)
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(path), "exec"), module.__dict__)
    need(module.REPO.resolve() == repo, "pinned base helper repository differs")
    module.validate_freeze(base_freeze)
    return module


def validate_freeze(base, frozen, base_freeze, repo):
    base.exact_keys(frozen, ("schema_version", "task", "spec_revision", "purpose", "review_status",
        "frozen_before_execution", "source_revision", "base_freeze_sha256", "helper_sha256",
        "scala_template_sha256", "original_input_sha256", "normal", "bounds"))
    need(type(frozen["schema_version"]) is int and frozen["schema_version"] == 1 and type(frozen["spec_revision"]) is int and frozen["task"] == "MEM-1-M07" and frozen["spec_revision"] == 7
        and frozen["purpose"] == "private-stock-hms-external-readonly-cl-input-only", "bulk freeze scope differs")
    need(frozen["review_status"] == "reviewed" and frozen["frozen_before_execution"] is True,
        "bulk freeze is not reviewed before execution")
    need(isinstance(frozen["source_revision"], str) and re.fullmatch(r"[0-9a-f]{40}", frozen["source_revision"])
        and frozen["source_revision"] == base_freeze["source_revision"], "bulk and owner source revisions differ")
    need(frozen["base_freeze_sha256"] == base.sha(base.canonical(base_freeze)), "bulk owner freeze differs")
    need(frozen["helper_sha256"] == base.sha(base.bounded_read(Path(__file__))), "bulk helper source differs")
    need(frozen["scala_template_sha256"] == digest(SCALA_TEMPLATE.encode()), "bulk Scala template differs")
    original = base.bounded_read(Path(repo) / CL_PATH)
    need(digest(original) == CL_SHA == frozen["original_input_sha256"], "original CL source differs")
    need(base.decode_json(original)["normal"] == NORMAL and base.canonical(frozen["normal"]) == base.canonical(NORMAL), "bulk changed original scale")
    need(isinstance(frozen["bounds"], dict) and all(type(v) is int for v in frozen["bounds"].values()), "bulk bounds must be integers")
    need(frozen["bounds"] == {"pairs_per_shard":128, "shards_per_namespace":4,
        "serial_writer_concurrency":1, "max_markers_per_stage":MARKER_COUNT,
        "max_output_bytes":OUTPUT_CAP, "max_marker_bytes":MARKER_CAP,
        "max_metadata_bytes":METADATA_CAP, "max_root_namespaces":128,
        "max_table_names":512, "max_view_names":512, "max_all_object_names":1024},
        "bulk finite bounds differ")


# Replaced mechanically with a complete independent stock public-API template.
SCALA_TEMPLATE = r'''

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.JavaConverters._
import org.apache.iceberg.{BaseTable, PartitionSpec, Schema, SchemaParser}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.types.Types
import org.apache.iceberg.util.JsonUtil
import org.apache.iceberg.view.{BaseView, SQLViewRepresentation}

object M07HmsReadonlyCl {
  val phase = @@PHASE@@
  val stageId = @@STAGE@@
  val ordinalStart = @@START@@
  val ordinalEnd = @@END@@
  val shard = @@SHARD@@
  val namespaceName = @@NAMESPACE@@
  val warehouse = @@WAREHOUSE@@
  val catalogName = @@CATALOG@@
  var tableName = @@TABLE@@
  var viewName = @@VIEW@@
  var query = @@QUERY@@
  val maxMetadataBytes = @@METADATA_CAP@@L
  val maxNamespaces = @@NAMESPACE_CAP@@
  val mapper = JsonUtil.mapper()
  val cat = new HiveCatalog()
  val ns = Namespace.of(namespaceName)
  var tid = TableIdentifier.of(ns, tableName)
  var vid = TableIdentifier.of(ns, viewName)
  val schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()))
  var tableLocation = warehouse.stripSuffix("/") + "/" + namespaceName + "/" + tableName
  var viewLocation = warehouse.stripSuffix("/") + "/" + namespaceName + "/" + viewName
  def digest(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString
  def emit(kind: String, value: org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode): Unit = {
    val envelope = mapper.createObjectNode()
    envelope.put("phase", stageId); envelope.put("kind", kind)
    if(kind == "table" || kind == "view") {
      val wrapped = mapper.createObjectNode(); wrapped.put("namespace", namespaceName)
      wrapped.put("object", if(kind == "table") tableName else viewName)
      wrapped.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("facts", value)
      envelope.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("value", wrapped)
    } else envelope.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("value", value)
    println("NR_HMS_READONLY_CL|" + mapper.writeValueAsString(envelope)); Console.out.flush()
  }
  def json(value: String) = mapper.readTree(value)
  def names(values: Seq[String], cap: Int = maxNamespaces) = {
    require(values.size <= cap && values.distinct.size == values.size, "namespace/name bound or duplicate")
    val result = mapper.createArrayNode(); values.sorted.foreach(value => result.add(value)); result
  }
  def rootNames() = names(cat.listNamespaces(Namespace.empty()).asScala.map { n =>
    require(n.length() == 1, "unexpected hierarchical HMS namespace"); n.level(0)
  }.toSeq, 128)
  def mutation(operation: String, state: String): Unit = {
    val value = mapper.createObjectNode(); value.put("operation", operation); value.put("namespace", namespaceName)
    value.put("object", if(operation.contains("table")) tableName else if(operation.contains("view")) viewName else namespaceName)
    value.put("state", state); emit("mutation", value)
  }
  def select(ordinal: Int): Unit = {
    require(ordinal >= 0 && ordinal < 512, "ordinal bound")
    tableName = f"cl_table_${ordinal}%06d"; viewName = f"cl_view_${ordinal}%06d"
    query = "SELECT id FROM " + tableName
    tid = TableIdentifier.of(ns, tableName); vid = TableIdentifier.of(ns, viewName)
    tableLocation = warehouse.stripSuffix("/") + "/" + namespaceName + "/" + tableName
    viewLocation = warehouse.stripSuffix("/") + "/" + namespaceName + "/" + viewName
  }
  def body(file: org.apache.iceberg.io.InputFile) = {
    val declared = file.getLength(); require(declared > 0 && declared <= maxMetadataBytes, "metadata length bound")
    val stream = file.newStream(); val hash = MessageDigest.getInstance("SHA-256"); var total = 0L
    try {
      val buffer = new Array[Byte](4096); var count = stream.read(buffer)
      while(count >= 0) {
        require(total + count <= maxMetadataBytes, "metadata stream bound")
        if(count > 0) { hash.update(buffer, 0, count); total += count }
        count = stream.read(buffer)
      }
    } finally stream.close()
    require(total == declared, "metadata length changed")
    val result = mapper.createObjectNode(); result.put("bytes", total)
    result.put("sha256", hash.digest().map(b => f"${b & 255}%02x").mkString); result
  }
  def facts(): Unit = {
    val table = cat.loadTable(tid); val tm = table.asInstanceOf[BaseTable].operations().current()
    require(table.currentSnapshot() == null && tm.snapshots().isEmpty, "empty table acquired a snapshot")
    require(table.location() == tableLocation, "table location differs")
    val tf = mapper.createObjectNode(); tf.put("uuid", tm.uuid()); tf.put("format_version", tm.formatVersion())
    tf.put("location", tm.location()); tf.put("metadata_location", tm.metadataFileLocation())
    tf.put("schema_id", tm.currentSchemaId()); tf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("schema", json(SchemaParser.toJson(table.schema())))
    tf.put("schema_count", tm.schemas().size()); tf.putNull("snapshot_id"); tf.put("snapshot_count", tm.snapshots().size())
    val spec = tm.spec()
    require(tm.defaultSpecId() == 0 && spec.specId() == 0 && table.spec().specId() == 0 &&
      spec.fields().isEmpty && table.spec().fields().isEmpty && spec.isUnpartitioned() &&
      tm.specs().size() == 1 && tm.specs().get(0).specId() == 0 && tm.specs().get(0).fields().isEmpty,
      "loaded table partition specification differs")
    val sf = mapper.createObjectNode(); sf.put("spec-id", spec.specId())
    sf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("fields", mapper.createArrayNode())
    tf.put("default_spec_id", tm.defaultSpecId()); tf.put("spec_count", tm.specs().size())
    tf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("partition_spec", sf)
    require(tm.metadataFileLocation().startsWith(tableLocation + "/metadata/"), "table metadata authority differs")
    tf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("raw_metadata", body(table.io().newInputFile(tm.metadataFileLocation()))); emit("table", tf)
    val view = cat.loadView(vid); val vm = view.asInstanceOf[BaseView].operations().current(); val version = view.currentVersion()
    require(view.location() == viewLocation, "view location differs")
    val vf = mapper.createObjectNode(); vf.put("uuid", vm.uuid()); vf.put("format_version", vm.formatVersion())
    vf.put("location", vm.location()); vf.put("metadata_location", vm.metadataFileLocation())
    vf.put("schema_id", version.schemaId()); vf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("schema", json(SchemaParser.toJson(view.schema())))
    vf.put("schema_count", vm.schemas().size()); vf.put("version_id", version.versionId()); vf.put("version_count", vm.versions().size())
    vf.put("default_catalog", version.defaultCatalog()); vf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("default_namespace", names(version.defaultNamespace().levels().toSeq))
    val reps = mapper.createArrayNode()
    version.representations().asScala.foreach { representation =>
      require(representation.isInstanceOf[SQLViewRepresentation], "non SQL view representation")
      val sql = representation.asInstanceOf[SQLViewRepresentation]; val entry = mapper.createObjectNode()
      entry.put("dialect", sql.dialect()); entry.put("sql", sql.sql()); reps.add(entry)
    }
    vf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("representations", reps)
    require(vm.metadataFileLocation().startsWith(viewLocation + "/metadata/"), "view metadata authority differs")
    vf.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("raw_metadata", body(table.io().newInputFile(vm.metadataFileLocation()))); emit("view", vf)
  }
  def run(): Unit = {
    val conf = new org.apache.hadoop.conf.Configuration(spark.sparkContext.hadoopConfiguration)
    conf.set("hive.metastore.failure.retries", "0")
    // Hive 2.3.9 counts total connection rounds: zero disables the initial
    // connection. Exactly one round permits the first attempt and no retry.
    conf.set("hive.metastore.connect.retries", "1")
    val hiveConf = new org.apache.hadoop.hive.conf.HiveConf(conf, classOf[HiveCatalog])
    require(hiveConf.getIntVar(org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES) == 0, "HMS failure retry setting ignored")
    require(hiveConf.getIntVar(org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES) == 1, "HMS initial connection attempt setting ignored")
    cat.setConf(conf)
    val properties = new java.util.HashMap[String,String]()
    Seq("uri", "warehouse", "io-impl", "s3.endpoint", "s3.path-style-access", "s3.access-key-id", "s3.secret-access-key", "s3.region").foreach { key =>
      properties.put(key, spark.conf.get("spark.sql.catalog.hms_catalog." + key))
    }
    // The canonical owner publishes its actual fixture region as s3.region.
    // Direct S3FileIO consumes the public client.region key; unlike the
    // Compose Spark service, this one-shot writer has no ambient AWS_REGION.
    properties.put(org.apache.iceberg.aws.AwsClientProperties.CLIENT_REGION, properties.get("s3.region"))
    require(new org.apache.iceberg.aws.AwsClientProperties(properties).clientRegion() ==
      spark.conf.get("spark.sql.catalog.hms_catalog.s3.region"), "S3 client region binding differs")
    require(properties.get("warehouse") == warehouse, "HMS warehouse binding differs")
    properties.put(HiveCatalog.LIST_ALL_TABLES, "false"); properties.put("clients", "1")
    cat.initialize(catalogName, properties)
    try {
      if(phase == "baseline" || phase == "restored") {
        val root = rootNames(); emit("namespaces", root)
        val allCatalog = new HiveCatalog()
        try {
          val allProperties = new java.util.HashMap[String,String](properties)
          allProperties.put(HiveCatalog.LIST_ALL_TABLES, "true")
          allCatalog.setConf(conf); allCatalog.initialize(catalogName + "_baseline_objects", allProperties)
          root.elements().asScala.foreach { value =>
            val baselineNs = Namespace.of(value.asText())
            val identifiers = allCatalog.listTables(baselineNs).asScala.toSeq
            require(identifiers.forall(_.namespace() == baselineNs), "baseline object namespace differs")
            val objects = names(identifiers.map(_.name()), 1024)
            require(objects.size() == 0, "private baseline has unexpected objects")
            val fact = mapper.createObjectNode(); fact.put("namespace", value.asText())
            fact.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("objects", objects)
            emit("baseline_objects", fact)
          }
        } finally allCatalog.close()
      } else if(phase == "create") {
        require(ordinalEnd - ordinalStart == 128, "shard pair count")
        if(shard == 0) {
          mutation("create_namespace", "attempt")
          cat.createNamespace(ns, java.util.Collections.emptyMap[String,String]())
          mutation("create_namespace", "applied")
        }
        for(ordinal <- ordinalStart until ordinalEnd) {
          select(ordinal)
          mutation("create_table", "attempt")
          cat.buildTable(tid, schema).withPartitionSpec(PartitionSpec.unpartitioned()).withLocation(tableLocation).withProperty("format-version", "2").create()
          mutation("create_table", "applied")
          mutation("create_view", "attempt")
          cat.buildView(vid).withSchema(schema).withDefaultCatalog(catalogName).withDefaultNamespace(ns).withQuery("spark", query).withLocation(viewLocation).create()
          mutation("create_view", "applied")
        }
      } else if(phase == "oracle") {
        require(ordinalEnd - ordinalStart == 128, "oracle shard pair count")
        if(namespaceName == "cl_ns_0000" && shard == 0) emit("namespaces", rootNames())
        if(shard == 0) {
          emit("tables", names(cat.listTables(ns).asScala.map(_.name()).toSeq, 512))
          emit("views", names(cat.listViews(ns).asScala.map(_.name()).toSeq, 512))
          val allCatalog = new HiveCatalog()
          try {
            val allProperties = new java.util.HashMap[String,String](properties)
            allProperties.put(HiveCatalog.LIST_ALL_TABLES, "true")
            allCatalog.setConf(conf); allCatalog.initialize(catalogName + "_all_objects", allProperties)
            val identifiers = allCatalog.listTables(ns).asScala.toSeq
            require(identifiers.forall(_.namespace() == ns), "all-object namespace differs")
            emit("all_objects", names(identifiers.map(_.name()), 1024))
          } finally allCatalog.close()
        }
        for(ordinal <- ordinalStart until ordinalEnd) { select(ordinal); facts() }
      } else if(phase == "drop") {
        require(ordinalEnd - ordinalStart == 128, "drop shard pair count")
        for(ordinal <- ordinalStart until ordinalEnd) {
          select(ordinal)
          mutation("drop_view", "attempt"); require(cat.dropView(vid), "view drop did not apply"); mutation("drop_view", "applied")
          mutation("drop_table", "attempt"); require(cat.dropTable(tid, true), "table drop did not apply"); mutation("drop_table", "applied")
        }
        if(shard == 3) {
          mutation("drop_namespace", "attempt"); require(cat.dropNamespace(ns), "namespace drop did not apply"); mutation("drop_namespace", "applied")
        }
      } else throw new IllegalArgumentException("unknown bulk phase")
      val complete = mapper.createObjectNode(); complete.put("status", "complete"); emit("complete", complete)
    } finally cat.close()
  }
}
try { M07HmsReadonlyCl.run(); System.exit(0) } catch {
  case failure: Throwable =>
    val error = org.apache.iceberg.util.JsonUtil.mapper().createObjectNode()
    error.put("exception_class", failure.getClass.getName)
    error.put("message_sha256", M07HmsReadonlyCl.digest(Option(failure.getMessage).getOrElse("").getBytes(java.nio.charset.StandardCharsets.UTF_8)))
    M07HmsReadonlyCl.emit("failure", error); System.exit(1)
}
'''


def program(base, kind, namespace, shard, sid, warehouse):
    actual = "oracle" if kind in ("before", "after") else kind
    ns = names(namespace)[0] if namespace is not None else "cl_ns_0000"
    values = {"PHASE":actual, "STAGE":sid, "NAMESPACE":ns, "WAREHOUSE":warehouse,
        "CATALOG":CATALOG, "TABLE":"cl_table_000000", "VIEW":"cl_view_000000",
        "QUERY":"SELECT id FROM cl_table_000000", "START":0 if shard is None else shard * 128,
        "END":0 if shard is None else (shard + 1) * 128,
        "SHARD":-1 if shard is None else shard, "METADATA_CAP":METADATA_CAP, "NAMESPACE_CAP":1024}
    result = SCALA_TEMPLATE
    import json
    for key, value in values.items():
        result = result.replace("@@" + key + "@@", json.dumps(value) if isinstance(value, str) else str(value))
    need("@@" not in result and len(result.encode()) <= 32768, "bulk Scala source exceeds bound or has missing fields")
    return result


def iter_markers(base, output, sid):
    need(len(output) <= OUTPUT_CAP, "bulk output exceeds frozen bound")
    count = 0
    for line in output.splitlines(keepends=True):
        if PREFIX not in line:
            continue
        need(line.startswith(PREFIX) and line.endswith(b"\n"), "partial or embedded bulk marker")
        body = line[len(PREFIX):].rstrip(b"\r\n")
        need(len(body) <= MARKER_CAP, "bulk marker exceeds frozen bound")
        row = base.decode_json(body)
        base.exact_keys(row, ("phase", "kind", "value"))
        need(row["phase"] == sid and row["kind"] in ("namespaces", "tables", "views", "all_objects",
            "table", "view", "mutation", "baseline_objects", "complete", "failure"), "bulk marker identity differs")
        need(count < MARKER_COUNT, "too many bulk stage markers")
        count += 1
        yield row


def parse_markers(base, output, sid):
    return list(iter_markers(base, output, sid))


def expected_records(kind, namespace=None, shard=None):
    sid = stage_id(kind, namespace, shard)
    if kind in ("baseline", "restored"):
        return [("namespaces", None), ("complete", None)]
    if kind in ("create", "drop"):
        return [("mutation", {**item, "state":state}) for item in mutations(kind, namespace, shard)
            for state in ("attempt", "applied")] + [("complete", None)]
    result = [("namespaces", None)] if namespace == 0 and shard == 0 else []
    result += [(key, None) for key in ("tables", "views", "all_objects")] if shard == 0 else []
    ns, tables, views = names(namespace)
    for i in range(shard * 128, (shard + 1) * 128):
        result.extend((("table", tables[i]), ("view", views[i])))
    return result + [("complete", None)]


def validate_records(base, rows, kind, namespace, shard, warehouse, base_freeze, baseline, secrets, *, complete, receipt=None):
    expected = expected_records(kind, namespace, shard)
    pending = mutations(kind, namespace, shard) if kind in ("create", "drop") else []
    ns = names(namespace)[0] if namespace is not None else None
    accepted = []
    failed = False
    if receipt is not None:
        receipt["records"] = accepted
        receipt["unconfirmed_mutations"] = pending.copy()
    for index, row in enumerate(rows):
        need(not failed, "bulk marker follows a failure")
        need(all(not secret or secret.encode() not in base.canonical(row) for secret in secrets),
            "bulk safe marker contains credential material")
        value, actual = row["value"], row["kind"]
        if actual == "failure":
            base.exact_keys(value, ("exception_class", "message_sha256"))
            need(isinstance(value["exception_class"], str)
                and re.fullmatch(r"[A-Za-z0-9_.$]{1,256}", value["exception_class"])
                and isinstance(value["message_sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", value["message_sha256"]),
                "bulk failure projection differs")
            need(not complete, "stock Java reported failure")
            accepted.append(row)
            failed = True
            continue
        need(index < len(expected) and actual == expected[index][0], "bulk marker sequence differs")
        wanted = expected[index][1]
        if actual == "mutation":
            need(value == wanted, "bulk mutation identity/state differs")
            if value["state"] == "applied":
                pending.remove({key:value[key] for key in ("operation", "namespace", "object")})
        elif actual in ("table", "view"):
            base.exact_keys(value, ("namespace", "object", "facts"))
            need(value["namespace"] == ns and value["object"] == wanted, "bulk metadata identity differs")
            copied = {**base_freeze, "input":{**base_freeze["input"], "catalog_name":CATALOG,
                actual + "_name":wanted, "view_sql":"SELECT id FROM cl_table_" + wanted.rsplit("_", 1)[1]}}
            base.validate_facts(value["facts"], actual, ns, warehouse, copied)
            need(base.canonical(value["facts"]["schema"]) == base.canonical(base_freeze["input"]["schema"]),
                "metadata schema typed fields differ")
            numeric = ("format_version", "schema_id", "schema_count") + (("snapshot_count", "default_spec_id", "spec_count")
                if actual == "table" else ("version_id", "version_count"))
            need(all(type(value["facts"][field]) is int for field in numeric), "metadata numeric facts are not typed integers")
        elif actual == "namespaces":
            observed = base.name_set(value, 128)
            created = {names(i)[0] for i in range(32)}
            need(not observed & created if kind == "baseline" else
                observed == set(baseline) | created if kind in ("before", "after") else observed == set(baseline),
                "bulk namespace baseline/set differs")
            if kind in ("baseline", "restored"):
                expected = [("namespaces", None)] + [("baseline_objects", name) for name in value] + [("complete", None)]
        elif actual == "baseline_objects":
            base.exact_keys(value, ("namespace", "objects"))
            need(value["namespace"] == wanted and base.name_set(value["objects"], 1024) == set(),
                "private baseline namespace has unexpected actual objects")
        elif actual in ("tables", "views", "all_objects"):
            _, tables, views = names(namespace)
            target = tables if actual == "tables" else views if actual == "views" else sorted(tables + views)
            need(value == target and base.name_set(value, len(target)) == set(target), "bulk object-name set differs")
        elif actual == "complete":
            need(value == {"status":"complete"}, "bulk completion differs")
        accepted.append(row)
        if receipt is not None:
            receipt["unconfirmed_mutations"] = pending.copy()
    if complete:
        need(len(accepted) == len(expected) and not pending and not failed, "bulk stage is incomplete")
    return accepted


def make_owner_class(base):
    class BulkOwner(base.Preflight):
        def __init__(self, base_freeze, frozen, output, clocks):
            clocks.validate()
            output = Path(output).resolve()
            need(output.is_relative_to(base.REPO / "logs/mem-1-m07") and not output.exists(),
                "bulk output is not a fresh task-private root")
            validate_freeze(base, frozen, base_freeze, base.REPO)
            need(re.fullmatch(r"[a-z0-9_-]{1,96}", output.name), "bulk output name is not bounded")
            output.mkdir(parents=True, exist_ok=False); output.chmod(0o700)
            base_freeze = base.decode_json(base.canonical(base_freeze))
            frozen = base.decode_json(base.canonical(frozen))
            super().__init__(base_freeze, output)
            # The parent's capability clocks have not been used. The caller's
            # immutable absolute clocks are the sole bulk lifecycle authority.
            self.clocks, self.bulk_freeze = clocks, frozen
            self.work_deadline = clocks.preparation_until
            self.wall_deadline = clocks.cleanup_until
            self.cleanup_deadline = clocks.cleanup_until
            self.status.pop("namespace", None)
            self.status.update(scope="private-stock-hms-external-readonly-cl-input-only",
                clocks={"preparation_until":clocks.preparation_until,
                    "verification_until":clocks.verification_until, "cleanup_until":clocks.cleanup_until},
                bulk_freeze_sha256=base.sha(base.canonical(frozen)), bulk_ready=False,
                native_role_settlement="not-started", stage_index=[], phases=[])
            self.baseline = None
            self.ready = None
            self.native_pending = False
            self.before = []
            self.uuid_seen = set()
            self.active_stage = None
            self.issued_stages = set()
            self.owner_thread_id = threading.get_ident()

        def check_thread(self):
            need(threading.get_ident() == self.owner_thread_id, "bulk owner is confined to one serial caller thread")

        def run_stages(self):
            raise ValueError("bulk owner requires prepare/verify_after/drop_and_restore APIs")

        def command_ownership(self, args, label):
            if label.startswith("stock-spark-bulk-"):
                need(args[0] == self.docker and self.active_stage == label.removeprefix("stock-spark-bulk-"),
                    "bulk direct Docker operation identity differs")
                return "direct-docker"
            return super().command_ownership(args, label)

        def save_json(self, path, value):
            raw = base.canonical(value)
            need(len(raw) + 1 <= FILE_CAP, "bulk receipt exceeds bounded local file")
            need(all(not secret or secret.encode() not in raw for secret in self.secrets), "receipt contains credential material")
            base.atomic_json(path, value)
            return digest(raw + b"\n")

        def stage(self, kind, namespace=None, shard=None):
            self.check_thread()
            sid = stage_id(kind, namespace, shard)
            need(self.active_stage is None and not self.children and not self.native_pending,
                "bulk stage overlaps a child or Native owner")
            need(not self.status["resource_retained_whole_failure"], "whole failure retained")
            base.remaining(self.work_deadline)
            need(sid not in self.issued_stages, "bulk stage cannot be replayed")
            self.issued_stages.add(sid)
            self.active_stage = sid
            deadline = min(self.work_deadline, time.monotonic() + self.bounds["spark_stage_seconds"])
            script = self.root / (sid + ".scala")
            code = program(base, kind, namespace, shard, sid, self.hms["hms"]["warehouse"])
            script.write_text(code); script.chmod(0o644)
            config = Path(self.hms["hms"]["spark_defaults"]).resolve(strict=True)
            need(config.is_relative_to(self.owner_root.resolve()), "Spark config escaped private owner")
            name = "nr-m07-hms-cl-" + self.run_id + "-" + sid
            self.writer_names.append(sid)
            receipt = {"phase":sid, "container_name":name, "program_sha256":digest(code.encode()),
                "records":[], "writer_exited":False, "unconfirmed_mutations":
                mutations(kind, namespace, shard) if kind in ("create", "drop") else []}
            self.status["phases"] = [{"phase":sid, "receipt_path":str(self.root / (sid + "-receipt.json"))}]
            primary = secondary = None
            def retain_records(output, complete):
                return validate_records(base, iter_markers(base, output, sid), kind, namespace, shard,
                    self.hms["hms"]["warehouse"], self.freeze, self.baseline, self.secrets,
                    complete=complete, receipt=receipt)
            try:
                _, output = self.command([self.docker, "create", "--pull", "never", "--interactive",
                    "--name", name, "--platform", self.freeze["source"]["platform"],
                    "--network", self.hms["rest_network"], "--label", "novarocks.m07.run=" + self.run_id,
                    "--label", "novarocks.m07.phase=" + sid,
                    "--mount", "type=bind,src=" + str(config) + ",dst=/run/m07/hms.conf,readonly",
                    "--mount", "type=bind,src=" + str(script) + ",dst=/run/m07/program.scala,readonly",
                    "--entrypoint", "/opt/spark/bin/spark-shell", self.freeze["source"]["writer_image_id"],
                    "--master", "local[1]", "--conf", "spark.ui.enabled=false",
                    "--properties-file", "/run/m07/hms.conf", "-i", "/run/m07/program.scala"],
                    self.bounds["inspection_seconds"], "writer-create", deadline=deadline)
                identity = output.decode().strip()
                need(re.fullmatch(r"[0-9a-f]{64}", identity) and self.writer_ids(sid, deadline) == [identity],
                    "bulk actual writer identity differs")
                receipt["container_id"] = identity
                exit_code, output = self.command([self.docker, "start", "--attach", "--interactive", identity],
                    self.bounds["spark_stage_seconds"], "stock-spark-bulk-" + sid,
                    deadline=deadline, check=False, input_bytes=b":quit\n")
                receipt.update(exit_code=exit_code, output_bytes=len(output), output_sha256=digest(output))
                parsed = retain_records(output, False)
                need(exit_code == 0, "stock bulk JVM failed")
                retain_records(output, True)
                base.remaining(deadline)
            except base.CaptureFailure as error:
                primary = error
                receipt.update(**error.safe_facts(), output_bytes=len(error.output), output_sha256=digest(error.output))
                try:
                    retain_records(error.output, False)
                except BaseException as partial_error:
                    self.retain_cancellation("partial-bulk-marker", partial_error)
                    receipt["partial_marker_error_class"] = type(partial_error).__name__
            except BaseException as error:
                primary = error
                self.retain_cancellation("bulk-stage-" + sid, error)
                receipt["primary_exception_class"] = type(error).__name__
            finally:
                try:
                    if self.status["resource_retained_whole_failure"] or self.children:
                        receipt["writer_exit_deferred"] = True
                    else:
                        self.remove_writer(sid, min(self.wall_deadline, time.monotonic() + self.bounds["writer_exit_seconds"]))
                        receipt["writer_exited"] = True
                        # This exact writer was removed and independently absent;
                        # keep its receipt, retain only unsettled dependency owners.
                        self.writer_names.remove(sid)
                except BaseException as error:
                    secondary = error
                    self.retain_cancellation("bulk-writer-exit", error)
                    receipt["writer_exit_error_class"] = type(error).__name__
                    self.status["resource_retained_whole_failure"] = True
                try:
                    receipt["commands"] = self.status["commands"]
                    path = self.root / (sid + "-receipt.json")
                    saved_hash = self.save_json(path, receipt)
                    self.status["stage_index"].append({"phase":sid, "path":str(path), "sha256":saved_hash,
                        "writer_exited":receipt["writer_exited"], "success":primary is None and secondary is None})
                    need(len(self.status["stage_index"]) <= 514, "bulk stage count exceeds frozen lifecycle")
                    self.status["commands"] = []; self.status["phases"] = []
                except BaseException as error:
                    self.retain_cancellation("bulk-receipt", error)
                    self.status["resource_retained_whole_failure"] = True
                    self.status["cleanup_errors"].append({"operation":"bulk-receipt", "class":type(error).__name__})
                    if secondary is None:
                        secondary = error
                self.active_stage = None
            if primary is not None:
                raise primary
            if secondary is not None:
                raise secondary
            base.remaining(self.work_deadline)
            return receipt

        def metadata_values(self, receipt):
            return [row["value"] for row in receipt["records"] if row["kind"] in ("table", "view")]

        def oracle(self, epoch):
            self.check_thread()
            need(epoch in ("before", "after"), "bulk oracle epoch differs")
            index = []
            for offset, (namespace, shard) in enumerate(SHARDS):
                receipt = self.stage(epoch, namespace, shard)
                facts = self.metadata_values(receipt)
                need(len(facts) == 256, "independent shard metadata load count differs")
                if epoch == "before":
                    for item in facts:
                        identifier = item["facts"]["uuid"]
                        need(identifier not in self.uuid_seen and len(self.uuid_seen) < 32768, "metadata UUID is duplicated")
                        self.uuid_seen.add(identifier)
                else:
                    prior = self.before[offset]
                    raw = base.bounded_read(prior["path"])
                    need(digest(raw) == prior["sha256"], "independent prior oracle receipt changed")
                    original = base.decode_json(raw)
                    need(facts == self.metadata_values(original), "post-Native immutable metadata facts differ")
                entry = self.status["stage_index"][-1]
                index.append({"path":entry["path"], "sha256":entry["sha256"], "namespace":namespace, "shard":shard})
            need(len(index) == 128, "bulk oracle shard count differs")
            if epoch == "before":
                need(len(self.uuid_seen) == 32768, "bulk independent object UUID count differs")
                self.before = index
            self.save_json(self.root / (epoch + "-oracle-manifest.json"), {"epoch":epoch, "shards":index,
                "tables":16384, "views":16384, "actual_metadata_loads":32768})

        def prepare(self):
            self.check_thread()
            need(self.manifest is None and self.ready is None, "private bulk owner already bound/prepared")
            self.source_precheck(); self.bind_owner()
            self.save_json(self.root / "bulk-bound-input.json", {"bulk_freeze":self.bulk_freeze,
                "binding":self.status["binding"], "normal":NORMAL})
            baseline = self.stage("baseline")
            self.baseline = baseline["records"][0]["value"]
            for namespace, shard in SHARDS:
                self.stage("create", namespace, shard)
            self.oracle("before")
            base.remaining(self.clocks.preparation_until)
            ready = {"schema_version":1, "scope":"external-stock-input-only", "normal":NORMAL,
                "bulk_freeze_sha256":self.status["bulk_freeze_sha256"], "binding":self.status["binding"],
                "oracle_manifest_sha256":digest(base.bounded_read(self.root / "before-oracle-manifest.json")),
                "tables":16384, "views":16384, "native_acceptance":False}
            ready_path = self.root / "INPUT_READY.json"
            try:
                self.save_json(ready_path, ready)
                base.remaining(self.clocks.preparation_until)
            except BaseException as error:
                self.retain_cancellation("input-ready-publication", error)
                try:
                    # Remove only this new artifact's marker, never provider data.
                    ready_path.unlink(missing_ok=True)
                except BaseException as secondary:
                    self.retain_cancellation("input-ready-unpublish", secondary)
                    self.status["resource_retained_whole_failure"] = True
                    self.status["cleanup_errors"].append({"operation":"input-ready-unpublish", "class":type(secondary).__name__})
                raise
            self.ready = ready; self.status["bulk_ready"] = True
            return ready

        def enter_native_window(self):
            self.check_thread()
            need(self.ready is not None and not self.native_pending and not self.status["native_started"] and self.active_stage is None and not self.children
                and not self.status["resource_retained_whole_failure"], "Native dependency transition is invalid")
            self.native_pending = True
            self.status["native_started"] = True
            self.status["native_role_settlement"] = "pending caller-validated exact four-role evidence"
            return dict(self.status["binding"])

        def settle_native_window(self, receipt, validator):
            self.check_thread()
            need(self.native_pending and callable(validator), "Native settlement requires caller validator")
            try:
                raw = base.canonical(receipt)
                need(len(raw) <= FILE_CAP and all(not secret or secret.encode() not in raw for secret in self.secrets),
                    "Native settlement receipt is unbounded or contains credentials")
                # Validator must check source/binary/config/receipt and actual
                # exact PIDs. Neither host wrapper PGID nor this helper is its authority.
                need(validator(receipt) is True, "caller did not confirm exact Native role settlement")
            except BaseException as error:
                self.status["resource_retained_whole_failure"] = True
                self.retain_cancellation("native-settlement", error)
                raise
            self.status["native_settlement_receipt_sha256"] = digest(raw)
            self.status["native_role_settlement"] = "caller-validated four exact roles independently exited"
            self.native_pending = False

        def verify_after(self):
            self.check_thread()
            need(self.ready is not None and not self.native_pending and not self.status.get("post_native_input_unchanged"),
                "external oracle has unresolved Native dependency or was already run")
            self.work_deadline = self.clocks.verification_until
            base.remaining(self.work_deadline)
            self.oracle("after")
            base.remaining(self.work_deadline)
            self.status["post_native_input_unchanged"] = True

        def drop_and_restore(self):
            self.check_thread()
            need(self.status.get("post_native_input_unchanged") is True and not self.native_pending and not self.status.get("baseline_restored"),
                "external drop requires independent oracle and actual dependency exit")
            self.work_deadline = self.clocks.cleanup_until
            base.remaining(self.work_deadline)
            for namespace, shard in SHARDS:
                self.stage("drop", namespace, shard)
            self.stage("restored")
            self.status["baseline_restored"] = True

        def cleanup(self):
            self.check_thread()
            if self.native_pending:
                self.status["resource_retained_whole_failure"] = True
                self.status["cleanup_errors"].append({"operation":"native-owner-retained",
                    "class":"UnconfirmedExactRoleSettlement", "dependent_owner_retained":True})
                return
            # Preserve the original pinned v5 child/descendant/owner cleanup.
            # Its original 240s clock cannot replace the caller's absolute clock.
            self.cleanup_deadline = self.clocks.cleanup_until
            super().cleanup()

        def check_cleanup_clock(self):
            self.check_thread()
            base.remaining(self.clocks.cleanup_until)

        def revoke_final_status(self):
            self.check_thread()
            # Revoke this new private artifact only, never provider data or a clock.
            (self.root / "bulk-status.json").unlink(missing_ok=True)
            directory = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)

        def save_status(self):
            self.check_thread()
            return self.save_json(self.root / "bulk-status.json", self.status)

    return BulkOwner


def new_owner(repo, base_freeze, bulk_freeze, output, clocks):
    """Create one owner. The caller must always finally cleanup and save_status.

    No function here starts NovaRocks. If a caller starts roles, it must first
    enter_native_window and must not skip the independent settlement validator.
    """
    base = load_base(repo, base_freeze)
    return make_owner_class(base)(base_freeze, bulk_freeze, output, clocks)


def _failure_facts(operation, error):
    facts = {"class": type(error).__name__, "operation": operation}
    try:
        rendered = str(error).encode("utf-8", errors="replace")
    except BaseException as render_error:
        # A broken formatter cannot replace the actual original failure object.
        rendered = b""
        facts["reason_unavailable_class"] = type(render_error).__name__
    facts.update(reason_bytes=len(rendered), reason_sha256=digest(rendered))
    return facts


@contextmanager
def managed_owner(repo, base_freeze, bulk_freeze, output, clocks):
    """Preserve first error and require the original clock through final fsync."""
    owner = new_owner(repo, base_freeze, bulk_freeze, output, clocks)
    primary = secondary = None

    def record_secondary(operation, error):
        owner.retain_cancellation(operation, error)
        owner.status["cleanup_errors"].append(_failure_facts(operation, error))

    try:
        yield owner
    except BaseException as error:
        primary = error
        owner.retain_cancellation("bulk-lifecycle", error)
        owner.status["errors"].append(_failure_facts("bulk-lifecycle", error))
    finally:
        try:
            owner.check_cleanup_clock()
            owner.cleanup()
            owner.check_cleanup_clock()
        except BaseException as error:
            secondary = error
            record_secondary("bulk-cleanup", error)
            # A late receipt after confirmed cleanup does not invent live resources.
            if not owner.status.get("cleanup_complete") or owner.children:
                owner.status["resource_retained_whole_failure"] = True
        owner.status["status"] = ("EXTERNAL_BULK_LIFECYCLE_SETTLED" if primary is None and secondary is None
            and owner.status.get("baseline_restored") is True and owner.status.get("cleanup_complete") is True
            and not owner.status["resource_retained_whole_failure"] and not owner.status["cleanup_errors"]
            else "EXTERNAL_BULK_LIFECYCLE_FAILED")
        try:
            owner.check_cleanup_clock()
            owner.save_status()
            owner.check_cleanup_clock()
        except BaseException as error:
            record_secondary("bulk-final-receipt", error)
            if secondary is None:
                secondary = error
            owner.status["status"] = "EXTERNAL_BULK_LIFECYCLE_FAILED"
            # Failure recording never renews the original deadline or runs provider IO.
            # A success file may already have synced before the post-sync clock check.
            try:
                owner.save_status()
            except BaseException as persist_error:
                record_secondary("bulk-failed-receipt", persist_error)
                try:
                    owner.revoke_final_status()
                except BaseException as revoke_error:
                    record_secondary("bulk-final-receipt-revoke", revoke_error)
                    owner.status["success_marker_unconfirmed"] = True
    if primary is not None:
        raise primary
    if secondary is not None:
        raise secondary
    need(owner.status.get("baseline_restored") is True and owner.status["cleanup_complete"] and not owner.status["cleanup_errors"]
        and not owner.status["resource_retained_whole_failure"] and not owner.children,
        "external bulk lifecycle did not physically settle")
