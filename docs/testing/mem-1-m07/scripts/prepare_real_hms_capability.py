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

"""Reviewed-freeze-only private stock HMS capability preflight.

No native workload or bulk READY. No provisioning, pulls, builds, or create
retries. Only the exact task-private RuntimeOwner/HiveOwner owns services.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time
import uuid
from urllib.parse import urlparse


REPO = Path(__file__).resolve().parents[4]
PREFIX = b"NR_HMS_CAPABILITY|"
PHASES = ("create", "oracle", "drop", "restored")
LOCAL_FILE_CAP = 1048576
JAR_BLOCK_BYTES = 65536
GROUP_CENSUS_INTERVAL_SECONDS = 0.05
GROUP_CENSUS_MAX_CHECKS = 101
OWNER_WRAPPERS = {
    "private-rest-up": "docker/iceberg-rest/up.sh",
    "private-hms-up": "docker/iceberg-hive/up.sh",
    "private-hms-purge": "docker/iceberg-hive/down.sh",
    "private-rest-unbind-purge": "docker/iceberg-rest/down.sh",
    "private-catalog-delete": "docker/iceberg-rest/fixture-runtime.sh",
    "private-object-store-delete": "docker/iceberg-rest/fixture-runtime.sh",
}
DIRECT_DOCKER_OPERATIONS = frozenset(("inspect-image", "actual-hms-container", "writer-census",
    "writer-inspect", "writer-kill", "writer-remove", "writer-create",
    *("stock-spark-" + phase for phase in PHASES)))


class Refusal(Exception):
    """A safe, bounded validation failure with no provider payload."""


class CaptureFailure(Refusal):
    def __init__(self, reason, output, *, exit_facts, primary_exception_class=None):
        super().__init__(reason)
        self.output = bytes(output)
        self.primary_reason = reason
        self.primary_exception_class = primary_exception_class
        self.exit_facts = exit_facts

    def safe_facts(self):
        return {"primary_failure": self.primary_reason,
            "primary_exception_class": self.primary_exception_class,
            "primary_reason_bytes": len(self.primary_reason.encode()),
            "primary_reason_sha256": sha(self.primary_reason.encode()), **self.exit_facts}


def is_cancellation(error):
    return isinstance(error, BaseException) and not isinstance(error, Exception)


def need(ok, message):
    if not ok:
        raise Refusal(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def json_stream(output):
    decoder = json.JSONDecoder()
    fields, rest = [], output.decode().strip()
    while rest:
        value, end = decoder.raw_decode(rest)
        fields.append(value)
        need(len(fields) <= 4, "Docker identity projection has extra fields")
        rest = rest[end:].lstrip()
    return fields


def bounded_read(path, cap=LOCAL_FILE_CAP):
    """Reject oversized/non-regular input before allocating its bounded contents."""
    path = Path(path)
    before = path.stat()
    need(stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= cap,
         "local input is not regular or exceeds frozen bound")
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        need((opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns) ==
             (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns),
             "local input changed before read")
        data = stream.read(cap + 1)
        after = os.fstat(stream.fileno())
    need(len(data) <= cap and len(data) == before.st_size and
         (after.st_size, after.st_mtime_ns, after.st_ctime_ns) ==
         (before.st_size, before.st_mtime_ns, before.st_ctime_ns), "local input changed or exceeded bound")
    return data


def read_json(path, cap=LOCAL_FILE_CAP):
    return decode_json(bounded_read(path, cap))


def jar_sha1(path, expected_bytes, deadline):
    """Hash the exact frozen JAR length with bounded reads and an explicit EOF check."""
    before = path.stat()
    need(stat.S_ISREG(before.st_mode) and before.st_size == expected_bytes,
         "actual stock JAR length differs before read")
    digest, total = hashlib.sha1(), 0
    with path.open("rb") as stream:
        opened = os.fstat(stream.fileno())
        need((opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns) ==
             (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns),
             "actual stock JAR changed before read")
        while total < expected_bytes:
            remaining(deadline)
            block = stream.read(min(JAR_BLOCK_BYTES, expected_bytes - total))
            need(bool(block), "actual stock JAR ended before frozen length")
            total += len(block)
            digest.update(block)
        remaining(deadline)
        need(stream.read(1) == b"", "actual stock JAR has bytes beyond frozen length")
        after = os.fstat(stream.fileno())
    need((after.st_size, after.st_mtime_ns, after.st_ctime_ns) ==
         (before.st_size, before.st_mtime_ns, before.st_ctime_ns), "actual stock JAR changed during read")
    remaining(deadline)
    return digest.hexdigest()


def decode_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate JSON field")
            result[key] = value
        return result
    def constant(_):
        raise Refusal("non-finite JSON number")
    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def atomic_json(path, value):
    path = Path(path)
    staging = path.with_suffix(path.suffix + ".tmp")
    with staging.open("xb") as stream:
        stream.write(canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    staging.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def exact_keys(value, keys):
    need(isinstance(value, dict) and set(value) == set(keys), "JSON schema fields differ")


def validate_freeze(freeze):
    exact_keys(freeze, ("schema_version", "task", "spec_revision", "purpose", "review_status",
        "frozen_before_execution", "source_revision", "helper_sha256", "scala_template_sha256",
        "source", "bounds", "input", "pins"))
    need(freeze["schema_version"] == 1 and freeze["task"] == "MEM-1-M07"
         and freeze["spec_revision"] == 7, "freeze identity differs")
    need(freeze["purpose"] == "private-stock-hms-capability-only", "freeze scope differs")
    need(freeze["review_status"] == "reviewed" and freeze["frozen_before_execution"] is True,
         "draft freeze has not been reviewed and frozen")
    need(sha(bounded_read(Path(__file__))) == freeze["helper_sha256"], "helper source differs")
    need(sha(SCALA_TEMPLATE.encode()) == freeze["scala_template_sha256"], "Scala template differs")
    exact_keys(freeze["source"], ("docker_binary", "fixture_store", "bom_sha256", "lock_sha256",
        "hms_image_id", "writer_image_id", "writer_definition_sha256", "jar_name", "jar_sha1",
        "jar_bytes", "platform"))
    expected = {
        "hms_image_id": "sha256:b748d30841d84294c2c2f0e0f9115adb65080695c09861beb58dbd10c18ea2fc",
        "jar_name": "iceberg-spark-runtime-3.5_2.12-1.11.0.jar",
        "jar_sha1": "86eb12917658be2c8dd8982ee0c57cfece862591", "jar_bytes": 47964645,
        "platform": "linux/arm64",
    }
    for key, value in expected.items():
        need(freeze["source"][key] == value, "stock source identity differs")
    for key in ("docker_binary", "fixture_store"):
        need(Path(freeze["source"][key]).is_absolute(), "source path is not absolute")
    exact_keys(freeze["bounds"], ("work_deadline_seconds", "cleanup_deadline_seconds",
        "owner_command_seconds", "spark_stage_seconds", "inspection_seconds", "writer_exit_seconds",
        "max_output_bytes", "max_marker_bytes", "max_namespaces", "max_metadata_bytes",
        "port_start", "port_end", "host_reap_seconds", "max_local_file_bytes", "jar_block_bytes"))
    need(freeze["bounds"] == {
        "work_deadline_seconds": 1200, "cleanup_deadline_seconds": 240,
        "owner_command_seconds": 180, "spark_stage_seconds": 240, "inspection_seconds": 15,
        "writer_exit_seconds": 45, "max_output_bytes": 1048576, "max_marker_bytes": 65536,
        "max_namespaces": 128, "max_metadata_bytes": 1048576,
        "port_start": 28250, "port_end": 28499,
        "host_reap_seconds": 5, "max_local_file_bytes": LOCAL_FILE_CAP, "jar_block_bytes": JAR_BLOCK_BYTES,
    }, "numeric bounds differ from reviewed capability draft")
    exact_keys(freeze["input"], ("namespace_prefix", "catalog_name", "table_name", "view_name",
        "view_sql", "view_dialect", "table_format_version", "view_format_version",
        "view_version_id", "schema", "hms_failure_retries", "hms_connect_retries",
        "table_partition_spec", "table_spec_count", "oracle_list_all_tables"))
    need(freeze["input"] == {
        "namespace_prefix": "m07_cap_", "catalog_name": "m07_hms_preflight",
        "table_name": "cap_table", "view_name": "cap_view", "view_sql": "SELECT id FROM cap_table",
        "view_dialect": "spark", "table_format_version": 2, "view_format_version": 1,
        "view_version_id": 1, "schema": {"type": "struct", "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": True, "type": "long"}]},
        "hms_failure_retries": 0, "hms_connect_retries": 0,
        "table_partition_spec": {"spec-id": 0, "fields": []}, "table_spec_count": 1,
        "oracle_list_all_tables": True,
    }, "capability input differs")
    need(isinstance(freeze["pins"], list) and 1 <= len(freeze["pins"]) <= 32, "source pins missing")
    seen = set()
    for pin in freeze["pins"]:
        exact_keys(pin, ("path", "sha256"))
        path = (REPO / pin["path"]).resolve(strict=True)
        need(path.is_relative_to(REPO) and pin["path"] not in seen, "unsafe or duplicate source pin")
        seen.add(pin["path"])
        need(sha(bounded_read(path)) == pin["sha256"], "fixture source pin differs")


def remaining(deadline):
    value = deadline - time.monotonic()
    need(value > 0, "absolute command deadline expired")
    return value


def group_exit_census(group_id, deadline):
    """Signal zero only; presence after leader reap never grants kill authority."""
    state, checks, errors = "unknown", 0, []
    for _ in range(GROUP_CENSUS_MAX_CHECKS):
        checks += 1
        try:
            os.killpg(group_id, 0)
            # It may be the old group or a reused ID. Neither permits teardown.
            state = "present"
        except ProcessLookupError:
            return "gone", checks, errors
        except BaseException as error:
            errors.append({"operation": "host-group-census", "class": type(error).__name__,
                "cancel_observed": is_cancellation(error)})
            return "unknown", checks, errors
        wait = deadline - time.monotonic()
        if wait <= 0:
            break
        try:
            time.sleep(min(GROUP_CENSUS_INTERVAL_SECONDS, wait))
        except BaseException as error:
            errors.append({"operation": "host-group-census-wait", "class": type(error).__name__,
                "cancel_observed": is_cancellation(error)})
            return "unknown", checks, errors
    return state, checks, errors


def reap_child(process, wall_deadline, reap_seconds):
    """Require leader reap AND group absence inside one clipped exit window."""
    errors = []
    reap_deadline = min(wall_deadline, time.monotonic() + reap_seconds)
    kill_outcome = "not-authorized-after-leader-reap"
    try:
        # This helper is the sole wait owner. Do not poll/reap before killing:
        # an unreaped leader reserves its PID, hence this original owned PGID.
        if process.returncode is None:
            kill_outcome = "attempt"
            try:
                os.killpg(process.pid, signal.SIGKILL)
                kill_outcome = "sent"
            except ProcessLookupError:
                kill_outcome = "group-absent-before-leader-reap"
    except BaseException as error:
        kill_outcome = "failed"
        errors.append({"operation": "host-child-kill", "class": type(error).__name__,
            "cancel_observed": is_cancellation(error)})
    try:
        if process.returncode is None:
            process.wait(timeout=max(0, reap_deadline - time.monotonic()))
    except BaseException as error:
        errors.append({"operation": "host-child-reap", "class": type(error).__name__,
            "cancel_observed": is_cancellation(error)})
    # A zero-duration wait can confirm an already exited leader at wall expiry.
    leader_reaped = process.returncode is not None
    group_state, group_checks, group_errors = group_exit_census(process.pid, reap_deadline)
    errors.extend(group_errors)
    for stream in (process.stdin, process.stdout):
        if stream is not None:
            try:
                stream.close()
            except BaseException as error:
                errors.append({"operation": "host-child-close", "class": type(error).__name__,
                    "cancel_observed": is_cancellation(error)})
    releasable = leader_reaped and group_state == "gone"
    facts = {"child_pid": process.pid, "owned_group_id": process.pid,
        "leader_reaped": leader_reaped, "group_exit_confirmed": group_state == "gone",
        "group_exit_state": group_state, "group_census_checks": group_checks,
        "kill_outcome": kill_outcome, "exit_code": process.returncode if leader_reaped else "unknown",
        "host_reap_errors": errors, "dependent_owner_retained": not releasable}
    process.m07_exit_facts = facts
    return releasable, facts


def capture(args, env, deadline, cap, input_bytes=None, owned_children=None, *, wall_deadline,
            reap_seconds, ownership):
    """Convert cancellation at every capture boundary into retained failure facts."""
    state = {"output": bytearray(), "process": None, "primary_error": None, "reap_started": False}
    try:
        return _capture_impl(args, env, deadline, cap, input_bytes, owned_children,
            wall_deadline=wall_deadline, reap_seconds=reap_seconds, ownership=ownership,
            capture_state=state)
    except CaptureFailure:
        raise
    except BaseException as error:
        primary = state["primary_error"] or error
        process = state["process"]
        facts = {"child_pid": None, "owned_group_id": None, "leader_reaped": False,
            "group_exit_confirmed": False, "group_exit_state": "unknown", "group_census_checks": 0,
            "kill_outcome": "not-confirmed", "exit_code": "unknown", "host_reap_errors": [],
            "dependent_owner_retained": True}
        if process is not None:
            facts = dict(getattr(process, "m07_exit_facts", facts))
            facts.update(child_pid=process.pid, owned_group_id=process.pid,
                leader_reaped=process.returncode is not None,
                exit_code=process.returncode if process.returncode is not None else "unknown")
            if not state["reap_started"]:
                state["reap_started"] = True
                try:
                    _, facts = reap_child(process, wall_deadline, reap_seconds)
                except BaseException as secondary:
                    facts["host_reap_errors"] = [*facts["host_reap_errors"],
                        {"operation": "outer-capture-reap", "class": type(secondary).__name__,
                            "cancel_observed": is_cancellation(secondary)}]
            if owned_children is not None and process not in owned_children and not (
                facts["leader_reaped"] and facts["group_exit_confirmed"]):
                owned_children.append(process)
        cancelled = (is_cancellation(primary) or is_cancellation(error) or
            any(row.get("cancel_observed", False) for row in facts["host_reap_errors"]))
        facts.update(command_ownership=ownership, capture_completed=False,
            detached_child_exit_confirmed=False if ownership == "owner-wrapper" else None,
            descendant_exit_basis="unconfirmed-capture-boundary",
            resource_retained_whole_failure=True, dependent_owner_retained=True,
            cancel_observed=cancelled, boundary_exception_class=type(error).__name__)
        if process is not None:
            process.m07_exit_facts = facts
        reason = ("host command cancelled by " + type(primary).__name__ if is_cancellation(primary)
            else str(primary) if isinstance(primary, Refusal) else "capture boundary failed")
        raise CaptureFailure(reason, state["output"], exit_facts=facts,
            primary_exception_class=type(primary).__name__) from primary


def _capture_impl(args, env, deadline, cap, input_bytes, owned_children, *, wall_deadline,
                  reap_seconds, ownership, capture_state):
    """Preserve the first failure and bounded output even when host reaping fails."""
    need(ownership in ("owner-wrapper", "direct-docker", "verifier-wrapper", "git-readonly"),
         "capture ownership is not declared")
    remaining(deadline)
    process = subprocess.Popen(args, cwd=REPO, env=env, start_new_session=True,
        stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    capture_state["process"] = process
    process.m07_exit_facts = {"child_pid": process.pid, "owned_group_id": process.pid,
        "leader_reaped": False, "group_exit_confirmed": False, "group_exit_state": "unknown",
        "group_census_checks": 0, "kill_outcome": "not-attempted", "exit_code": "unknown",
        "host_reap_errors": [], "dependent_owner_retained": True}
    if owned_children is not None:
        owned_children.append(process)
    selector = None
    output = capture_state["output"]
    primary_error = None
    code = None
    try:
        selector = selectors.DefaultSelector()
        if input_bytes is not None:
            need(len(input_bytes) <= 4096, "stdin exceeds frozen bound")
            process.stdin.write(input_bytes)
            process.stdin.close()
        os.set_blocking(process.stdout.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ)
        while selector.get_map():
            for key, _ in selector.select(min(0.1, remaining(deadline))):
                data = os.read(key.fd, 8192)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                available = cap - len(output)
                output.extend(data[:available])
                need(len(data) <= available, "child output exceeds frozen bound")
        code = process.wait(timeout=remaining(deadline))
        remaining(deadline)
    except BaseException as error:
        primary_error = error
        capture_state["primary_error"] = error
    finally:
        selector_error = None
        try:
            if selector is not None:
                selector.close()
        except BaseException as error:
            selector_error = {"operation": "host-selector-close", "class": type(error).__name__,
                "cancel_observed": is_cancellation(error)}
        try:
            capture_state["reap_started"] = True
            releasable, exit_facts = reap_child(process, wall_deadline, reap_seconds)
        except BaseException as error:
            releasable = False
            exit_facts = dict(process.m07_exit_facts)
            exit_facts.update(leader_reaped=process.returncode is not None,
                exit_code=process.returncode if process.returncode is not None else "unknown",
                group_exit_confirmed=False, group_exit_state="unknown", dependent_owner_retained=True)
            exit_facts["host_reap_errors"] = [*exit_facts["host_reap_errors"],
                {"operation": "host-exit-confirmation", "class": type(error).__name__,
                    "cancel_observed": is_cancellation(error)}]
            process.m07_exit_facts = exit_facts
        if selector_error is not None:
            exit_facts["host_reap_errors"].append(selector_error)
        if releasable and owned_children is not None:
            owned_children.remove(process)
    if primary_error is None:
        try:
            remaining(deadline)
            need(code is not None and code >= 0, "child process terminated by signal")
        except BaseException as error:
            primary_error = error
    # Host PGID absence covers only the directly owned group. RuntimeOwner's
    # Docker.command starts detached sessions; abnormal wrapper termination
    # cannot prove that those children settled, even when this PGID is gone.
    capture_completed = primary_error is None and not exit_facts["host_reap_errors"] and releasable
    wrapper_settled = capture_completed and code == 0
    cancel_observed = is_cancellation(primary_error) or any(
        row.get("cancel_observed", False) for row in exit_facts["host_reap_errors"])
    retain_whole_failure = (cancel_observed or (ownership == "owner-wrapper" and not wrapper_settled) or
        (ownership in ("direct-docker", "verifier-wrapper") and not capture_completed))
    exit_facts.update(command_ownership=ownership, capture_completed=capture_completed,
        detached_child_exit_confirmed=wrapper_settled if ownership == "owner-wrapper" else None,
        descendant_exit_basis=("normal-exit0-pinned-wrapper-synchronous-child-settlement" if wrapper_settled
            and ownership == "owner-wrapper" else "owned-host-group-only"),
        resource_retained_whole_failure=retain_whole_failure, cancel_observed=cancel_observed)
    process.m07_exit_facts = exit_facts
    if primary_error is not None or exit_facts["host_reap_errors"] or not releasable:
        primary_class = (type(primary_error).__name__ if primary_error is not None else
            selector_error["class"] if selector_error is not None else
            exit_facts["host_reap_errors"][0]["class"] if exit_facts["host_reap_errors"] else None)
        reason = ("host command cancelled by " + type(primary_error).__name__ if is_cancellation(primary_error) else
            str(primary_error) if isinstance(primary_error, Refusal) else
            "child capture failed before confirmed exit" if primary_error is not None else
            "host exit confirmation cancelled by " + str(primary_class) if cancel_observed else
            "host leader reap and whole owned-group exit were not confirmed")
        failure = CaptureFailure(reason, output, exit_facts=exit_facts,
            primary_exception_class=primary_class)
        raise failure from primary_error
    return code, bytes(output), exit_facts


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

object M07HmsCapability {
  val phase = @@PHASE@@
  val namespaceName = @@NAMESPACE@@
  val warehouse = @@WAREHOUSE@@
  val catalogName = @@CATALOG@@
  val tableName = @@TABLE@@
  val viewName = @@VIEW@@
  val query = @@QUERY@@
  val maxMetadataBytes = @@METADATA_CAP@@L
  val maxNamespaces = @@NAMESPACE_CAP@@
  val mapper = JsonUtil.mapper()
  val cat = new HiveCatalog()
  val ns = Namespace.of(namespaceName)
  val tid = TableIdentifier.of(ns, tableName)
  val vid = TableIdentifier.of(ns, viewName)
  val schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()))
  val tableLocation = warehouse.stripSuffix("/") + "/" + namespaceName + "/" + tableName
  val viewLocation = warehouse.stripSuffix("/") + "/" + namespaceName + "/" + viewName
  def digest(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString
  def emit(kind: String, value: org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode): Unit = {
    val envelope = mapper.createObjectNode()
    envelope.put("phase", phase); envelope.put("kind", kind); envelope.set[org.apache.iceberg.shaded.com.fasterxml.jackson.databind.JsonNode]("value", value)
    println("NR_HMS_CAPABILITY|" + mapper.writeValueAsString(envelope)); Console.out.flush()
  }
  def json(value: String) = mapper.readTree(value)
  def names(values: Seq[String]) = {
    require(values.size <= maxNamespaces && values.distinct.size == values.size, "namespace/name bound or duplicate")
    val result = mapper.createArrayNode(); values.sorted.foreach(value => result.add(value)); result
  }
  def rootNames() = names(cat.listNamespaces(Namespace.empty()).asScala.map { n =>
    require(n.length() == 1, "unexpected hierarchical HMS namespace"); n.level(0)
  }.toSeq)
  def mutation(operation: String, state: String): Unit = {
    val value = mapper.createObjectNode(); value.put("operation", operation); value.put("namespace", namespaceName)
    value.put("object", if(operation.contains("table")) tableName else if(operation.contains("view")) viewName else namespaceName)
    value.put("state", state); emit("mutation", value)
  }
  def body(file: org.apache.iceberg.io.InputFile) = {
    val declared = file.getLength()
    require(declared > 0 && declared <= maxMetadataBytes, "metadata length bound")
    val stream = file.newStream(); val sink = new java.io.ByteArrayOutputStream()
    try {
      val buffer = new Array[Byte](4096); var count = stream.read(buffer)
      while(count >= 0) {
        require(sink.size().toLong + count <= maxMetadataBytes, "metadata stream bound")
        if(count > 0) sink.write(buffer, 0, count)
        count = stream.read(buffer)
      }
    } finally stream.close()
    val bytes = sink.toByteArray(); require(bytes.length.toLong == declared, "metadata length changed")
    val result = mapper.createObjectNode(); result.put("bytes", bytes.length); result.put("sha256", digest(bytes)); result
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
    conf.set("hive.metastore.failure.retries", "0"); conf.set("hive.metastore.connect.retries", "0")
    val hiveConf = new org.apache.hadoop.hive.conf.HiveConf(conf, classOf[HiveCatalog])
    require(hiveConf.getIntVar(org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES) == 0, "HMS failure retry setting ignored")
    require(hiveConf.getIntVar(org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES) == 0, "HMS connection retry setting ignored")
    cat.setConf(conf)
    val properties = new java.util.HashMap[String,String]()
    Seq("uri", "warehouse", "io-impl", "s3.endpoint", "s3.path-style-access", "s3.access-key-id", "s3.secret-access-key", "s3.region").foreach { key =>
      properties.put(key, spark.conf.get("spark.sql.catalog.hms_catalog." + key))
    }
    require(properties.get("warehouse") == warehouse, "HMS warehouse binding differs")
    properties.put(HiveCatalog.LIST_ALL_TABLES, "false"); properties.put("clients", "1")
    cat.initialize(catalogName, properties)
    try {
      if(phase == "create") {
        val baseline = rootNames(); require(!baseline.asScala.exists(_.asText() == namespaceName), "private namespace exists")
        emit("baseline", baseline)
        mutation("create_namespace", "attempt"); cat.createNamespace(ns, java.util.Collections.emptyMap[String,String]()); mutation("create_namespace", "applied")
        mutation("create_table", "attempt")
        cat.buildTable(tid, schema).withPartitionSpec(PartitionSpec.unpartitioned()).withLocation(tableLocation).withProperty("format-version", "2").create()
        mutation("create_table", "applied")
        mutation("create_view", "attempt")
        cat.buildView(vid).withSchema(schema).withDefaultCatalog(catalogName).withDefaultNamespace(ns).withQuery("spark", query).withLocation(viewLocation).create()
        mutation("create_view", "applied"); facts()
      } else if(phase == "oracle") {
        emit("namespaces", rootNames())
        emit("tables", names(cat.listTables(ns).asScala.map(_.name()).toSeq))
        emit("views", names(cat.listViews(ns).asScala.map(_.name()).toSeq))
        // Public read-only second catalog checks the raw HMS object-name projection.
        // This is provider capability evidence, not native mixed-object classification.
        val allCatalog = new HiveCatalog()
        try {
          val allProperties = new java.util.HashMap[String,String](properties)
          allProperties.put(HiveCatalog.LIST_ALL_TABLES, "true")
          allCatalog.setConf(conf); allCatalog.initialize(catalogName + "_all_objects", allProperties)
          val identifiers = allCatalog.listTables(ns).asScala.toSeq
          require(identifiers.forall(_.namespace() == ns), "all-object namespace differs")
          emit("all_objects", names(identifiers.map(_.name())))
        } finally allCatalog.close()
        facts()
      } else if(phase == "drop") {
        mutation("drop_view", "attempt"); require(cat.dropView(vid), "view drop did not apply"); mutation("drop_view", "applied")
        mutation("drop_table", "attempt"); require(cat.dropTable(tid, true), "table drop did not apply"); mutation("drop_table", "applied")
        mutation("drop_namespace", "attempt"); require(cat.dropNamespace(ns), "namespace drop did not apply"); mutation("drop_namespace", "applied")
      } else if(phase == "restored") {
        emit("namespaces", rootNames())
      } else throw new IllegalArgumentException("unknown phase")
      val complete = mapper.createObjectNode(); complete.put("status", "complete"); emit("complete", complete)
    } finally cat.close()
  }
}
try { M07HmsCapability.run(); System.exit(0) } catch {
  case failure: Throwable =>
    val error = org.apache.iceberg.util.JsonUtil.mapper().createObjectNode()
    error.put("exception_class", failure.getClass.getName)
    error.put("message_sha256", M07HmsCapability.digest(Option(failure.getMessage).getOrElse("").getBytes(java.nio.charset.StandardCharsets.UTF_8)))
    M07HmsCapability.emit("failure", error); System.exit(1)
}
'''


def scala_program(phase, namespace, warehouse, freeze):
    values = {
        "PHASE": phase, "NAMESPACE": namespace, "WAREHOUSE": warehouse,
        "CATALOG": freeze["input"]["catalog_name"], "TABLE": freeze["input"]["table_name"],
        "VIEW": freeze["input"]["view_name"], "QUERY": freeze["input"]["view_sql"],
    }
    code = SCALA_TEMPLATE
    for key, value in values.items():
        code = code.replace("@@" + key + "@@", json.dumps(value))
    code = code.replace("@@METADATA_CAP@@", str(freeze["bounds"]["max_metadata_bytes"]))
    code = code.replace("@@NAMESPACE_CAP@@", str(freeze["bounds"]["max_namespaces"]))
    need("@@" not in code and len(code.encode()) <= 32768, "Scala input exceeds frozen bounds")
    return code


def markers(output, phase, freeze):
    result = []
    for line in output.splitlines():
        if PREFIX not in line:
            continue
        need(line.startswith(PREFIX), "marker is not a complete stdout record")
        body = line[len(PREFIX):]
        need(len(body) <= freeze["bounds"]["max_marker_bytes"], "marker exceeds frozen bound")
        value = decode_json(body)
        exact_keys(value, ("phase", "kind", "value"))
        need(value["phase"] == phase and value["kind"] in
             ("baseline", "namespaces", "tables", "views", "all_objects", "table", "view", "mutation", "complete", "failure"),
             "marker identity differs")
        result.append(value)
        need(len(result) <= 16, "too many stage markers")
    return result


def one(records, kind):
    values = [row["value"] for row in records if row["kind"] == kind]
    need(len(values) == 1, "required unique marker is missing or duplicated")
    return values[0]


def name_set(value, cap):
    need(isinstance(value, list) and len(value) <= cap and
         all(isinstance(name, str) and re.fullmatch(r"[a-z0-9_]{1,128}", name) for name in value)
         and value == sorted(set(value)), "namespace/name set is malformed")
    return set(value)


def validate_facts(value, kind, namespace, warehouse, freeze):
    common = ("uuid", "format_version", "location", "metadata_location", "schema_id", "schema",
              "schema_count", "raw_metadata")
    extra = ("snapshot_id", "snapshot_count", "default_spec_id", "spec_count", "partition_spec") if kind == "table" else (
        "version_id", "version_count", "default_catalog", "default_namespace", "representations")
    exact_keys(value, (*common, *extra))
    try:
        identifier = uuid.UUID(value["uuid"])
    except (ValueError, TypeError, AttributeError) as error:
        raise Refusal("metadata UUID is malformed") from error
    need(identifier.int != 0 and str(identifier) == value["uuid"], "metadata UUID is not canonical")
    location = warehouse.rstrip("/") + "/" + namespace + "/" + freeze["input"][kind + "_name"]
    need(value["location"] == location and isinstance(value["metadata_location"], str)
         and value["metadata_location"].startswith(location + "/metadata/")
         and len(value["metadata_location"].encode()) <= 4096, "metadata location escaped private authority")
    parsed = urlparse(value["metadata_location"])
    need(parsed.scheme == "s3" and parsed.netloc == urlparse(warehouse).netloc and
         not parsed.query and not parsed.fragment and ".." not in parsed.path.split("/"),
         "metadata URI contains an unsafe authority or credential-bearing suffix")
    need(value["schema_id"] == 0 and value["schema"] == freeze["input"]["schema"]
         and value["schema_count"] == 1, "metadata schema differs")
    need(value["format_version"] == freeze["input"][kind + "_format_version"], "metadata format differs")
    exact_keys(value["raw_metadata"], ("bytes", "sha256"))
    need(type(value["raw_metadata"]["bytes"]) is int and
         0 < value["raw_metadata"]["bytes"] <= freeze["bounds"]["max_metadata_bytes"]
         and re.fullmatch(r"[0-9a-f]{64}", value["raw_metadata"]["sha256"]), "metadata body proof differs")
    if kind == "table":
        need(value["snapshot_id"] is None and value["snapshot_count"] == 0, "empty table snapshot differs")
        exact_keys(value["partition_spec"], ("spec-id", "fields"))
        need(type(value["default_spec_id"]) is int and type(value["spec_count"]) is int and
             type(value["partition_spec"]["spec-id"]) is int and
             value["default_spec_id"] == freeze["input"]["table_partition_spec"]["spec-id"] and
             value["partition_spec"] == freeze["input"]["table_partition_spec"] and
             value["spec_count"] == freeze["input"]["table_spec_count"], "loaded partition specification differs")
    else:
        need(value["version_id"] == freeze["input"]["view_version_id"] and value["version_count"] == 1
             and value["default_catalog"] == freeze["input"]["catalog_name"]
             and value["default_namespace"] == [namespace]
             and value["representations"] == [{"dialect": freeze["input"]["view_dialect"],
                 "sql": freeze["input"]["view_sql"]}], "view version or SQL facts differ")


class Preflight:
    def __init__(self, freeze, output):
        self.freeze, self.root = freeze, output
        self.bounds = freeze["bounds"]
        self.docker = freeze["source"]["docker_binary"]
        self.run_id = uuid.uuid4().hex
        self.namespace = freeze["input"]["namespace_prefix"] + self.run_id
        self.workspace = output / ("m07-hms-cap-" + self.run_id)
        self.owner_root = output / "fixture-owner"
        self.config = output / "declaration.env"
        self.env = {key: value for key, value in os.environ.items()
            if not key.startswith(("NOVA_", "NOVAROCKS_", "AWS_")) and
               key not in ("HMS_IMAGE", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")}
        self.env.update(PATH=str(Path(self.docker).parent) + os.pathsep + self.env.get("PATH", ""),
            NO_PROXY="127.0.0.1,localhost", NOVAROCKS_WORKSPACE_ROOT=str(self.workspace),
            NOVA_ENV_CONFIG_FILE=str(self.config), NOVA_ENV_UPDATE_CURRENT="false",
            NOVA_FIXTURE_RUNTIME_DIR=str(self.owner_root),
            NOVA_FIXTURE_STORE=freeze["source"]["fixture_store"],
            NOVA_ENV_RUNTIME_PORT_START=str(self.bounds["port_start"]),
            NOVA_ENV_RUNTIME_PORT_END=str(self.bounds["port_end"]))
        self.started = time.monotonic()
        self.work_deadline = self.started + self.bounds["work_deadline_seconds"]
        self.wall_deadline = self.work_deadline + self.bounds["cleanup_deadline_seconds"]
        self.cleanup_deadline = None
        self.publication = self.manifest = self.hms = None
        self.hms_attempted = False
        self.rest_attempted = False
        self.writer_names = []
        self.children = []
        self.secrets = []
        self.status = {"schema_version": 1, "scope": "private-stock-hms-capability-only",
            "run_id": self.run_id, "namespace": self.namespace, "phases": [], "commands": [],
            "errors": [], "cleanup_errors": [], "cleanup_complete": False,
            "private_owner_root": str(self.owner_root), "native_started": False, "bulk_ready": False,
            "resource_retained_whole_failure": False, "retention_barriers": [],
            "freeze_canonical_sha256": sha(canonical(freeze)), "helper_sha256": sha(bounded_read(Path(__file__)))}

    def command_ownership(self, args, label):
        if label in OWNER_WRAPPERS:
            need(args[0] == str(REPO / OWNER_WRAPPERS[label]), "owner wrapper source differs")
            return "owner-wrapper"
        if label == "existing-bom-verify":
            need(args[0] == str(REPO / "docker/fixture-inputs/verify.sh"), "verifier source differs")
            return "verifier-wrapper"
        if label in DIRECT_DOCKER_OPERATIONS:
            need(args[0] == self.docker, "direct Docker command identity differs")
            return "direct-docker"
        if label in ("source-clean", "source-revision"):
            expected = ["git", "status", "--porcelain"] if label == "source-clean" else ["git", "rev-parse", "HEAD"]
            need(args == expected, "read-only source command differs")
            return "git-readonly"
        raise Refusal("command ownership is not declared")

    def retain_command_owner(self, label, facts):
        if facts["resource_retained_whole_failure"]:
            self.status["resource_retained_whole_failure"] = True
            self.status["retention_barriers"].append({"operation": label,
                "command_index": len(self.status["commands"]) - 1,
                "reason": "command descendant/operation settlement unconfirmed; whole failure retained",
                **facts})

    def retain_cancellation(self, label, error):
        if is_cancellation(error):
            self.status["resource_retained_whole_failure"] = True
            self.status["retention_barriers"].append({"operation": label,
                "reason": "host cancellation; whole failure retained",
                "primary_exception_class": type(error).__name__, "cancel_observed": True,
                "dependent_owner_retained": True})

    def command(self, args, seconds, label, *, deadline=None, check=True, input_bytes=None):
        try:
            return self._command_impl(args, seconds, label, deadline=deadline,
                check=check, input_bytes=input_bytes)
        except CaptureFailure:
            raise
        except BaseException as error:
            if is_cancellation(error):
                self.retain_cancellation(label, error)
                original = error.__context__ if isinstance(error.__context__, CaptureFailure) else None
                facts = {"command_ownership": "unconfirmed-command-boundary",
                    "capture_completed": False, "child_pid": None, "owned_group_id": None,
                    "leader_reaped": False, "group_exit_confirmed": False,
                    "group_exit_state": "unknown", "group_census_checks": 0,
                    "kill_outcome": "not-confirmed", "exit_code": "unknown", "host_reap_errors": [],
                    "detached_child_exit_confirmed": False, "dependent_owner_retained": True,
                    "cancel_observed": True, "resource_retained_whole_failure": True}
                if original is not None:
                    facts.update(original.exit_facts)
                    facts.update(cancel_observed=True, resource_retained_whole_failure=True,
                        dependent_owner_retained=True, boundary_exception_class=type(error).__name__)
                    failure = CaptureFailure(original.primary_reason, original.output,
                        exit_facts=facts, primary_exception_class=original.primary_exception_class)
                else:
                    facts["bounded_partial_available"] = False
                    failure = CaptureFailure("command boundary cancelled by " + type(error).__name__, b"",
                        exit_facts=facts, primary_exception_class=type(error).__name__)
                self.status["commands"].append({"operation": label, **failure.safe_facts(),
                    "output_bytes": len(failure.output) if original is not None else None,
                    "output_sha256": sha(failure.output) if original is not None else None})
                raise failure from error
            raise

    def _command_impl(self, args, seconds, label, *, deadline=None, check=True, input_bytes=None):
        need(not self.status["resource_retained_whole_failure"], "whole failure retained; no further commands")
        ownership = self.command_ownership(args, label)
        deadline = min(deadline or self.work_deadline, time.monotonic() + seconds)
        try:
            code, output, exit_facts = capture(args, self.env, deadline, self.bounds["max_output_bytes"], input_bytes,
                owned_children=self.children, wall_deadline=self.wall_deadline,
                reap_seconds=self.bounds["host_reap_seconds"], ownership=ownership)
        except CaptureFailure as error:
            self.status["commands"].append({"operation": label, **error.safe_facts(),
                "output_bytes": len(error.output), "output_sha256": sha(error.output)})
            self.retain_command_owner(label, error.safe_facts())
            raise
        self.status["commands"].append({"operation": label, **exit_facts,
            "output_bytes": len(output), "output_sha256": sha(output)})
        self.retain_command_owner(label, exit_facts)
        if check:
            need(code == 0, "command failed: " + label)
        return code, output

    def inspect_image(self, identity):
        _, output = self.command([self.docker, "image", "inspect", identity, "--format", "{{json .Id}}"],
            self.bounds["inspection_seconds"], "inspect-image")
        need(json.loads(output) == identity, "existing local image ID differs")

    def source_precheck(self):
        _, dirty = self.command(["git", "status", "--porcelain"], 15, "source-clean")
        _, revision = self.command(["git", "rev-parse", "HEAD"], 15, "source-revision")
        need(not dirty and revision.decode().strip() == self.freeze["source_revision"], "source checkout differs")
        store = Path(self.freeze["source"]["fixture_store"])
        bom_path = store / "bom.json"
        bom_bytes = bounded_read(bom_path)
        need(sha(bom_bytes) == self.freeze["source"]["bom_sha256"], "BOM file differs")
        bom = decode_json(bom_bytes)
        need(bom["lock_sha256"] == self.freeze["source"]["lock_sha256"], "BOM canonical lock differs")
        writer = bom["derived_images"]["iceberg-spark"]
        need(writer["image_id"] == self.freeze["source"]["writer_image_id"] and
             writer["definition_sha256"] == self.freeze["source"]["writer_definition_sha256"] and
             writer["platform"] == self.freeze["source"]["platform"], "writer BOM receipt differs")
        jar = (store / bom["artifact_dir"] / self.freeze["source"]["jar_name"]).resolve(strict=True)
        need(jar.is_relative_to(store.resolve()), "JAR artifact escaped fixture store")
        need(jar_sha1(jar, self.freeze["source"]["jar_bytes"], self.work_deadline) ==
             self.freeze["source"]["jar_sha1"], "actual stock JAR differs")
        # This verifier reads existing images and artifacts; it never provisions.
        self.command([str(REPO / "docker/fixture-inputs/verify.sh"), "--store", str(store),
            "--repo-root", str(REPO), "--consumer", "iceberg-rest"], 60, "existing-bom-verify")
        self.inspect_image(self.freeze["source"]["hms_image_id"])
        self.inspect_image(self.freeze["source"]["writer_image_id"])
        self.status["source_revision"] = self.freeze["source_revision"]

    def bind_owner(self):
        self.workspace.mkdir()
        self.config.write_bytes(bounded_read(REPO / "docker/iceberg-rest/shared.env"))
        self.config.chmod(0o600)
        self.rest_attempted = True
        _, output = self.command([str(REPO / "docker/iceberg-rest/up.sh")],
            self.bounds["owner_command_seconds"], "private-rest-up")
        documents = [json.loads(line) for line in output.splitlines() if line.startswith(b"{")]
        need(len(documents) == 1, "REST owner did not return one publication")
        publication = Path(documents[0]["published_dir"]).resolve(strict=True)
        need(publication.is_relative_to(self.workspace), "publication escaped new consumer")
        manifest_bytes = bounded_read(publication / "manifest.json")
        manifest = decode_json(manifest_bytes)
        locator = manifest["runtime"]["owner_locator"]
        need(manifest["ready"] is True and manifest["shared_docker"] is True and
             manifest["runtime"]["profile"] == "stock" and
             manifest["workspace_root"] == str(self.workspace) and
             Path(locator["control_root"]).resolve() == self.owner_root.resolve(), "private REST owner differs")
        need(Path(manifest["runtime"]["publication_dir"]).resolve() == publication,
             "publication commit identity differs")
        need(manifest["fixture_inputs"]["verified"] is True and
             manifest["fixture_inputs"]["lock_sha256"] == self.freeze["source"]["lock_sha256"] and
             manifest["runtime"]["catalog"]["owner_locator"] == locator and
             manifest["runtime"]["object_store"]["owner_locator"] == locator,
             "published fixture inputs or resource owner differs")
        need(manifest["runtime"]["catalog"]["images"]["spark"]["image_id"] ==
             self.freeze["source"]["writer_image_id"], "owner stock Spark image differs")
        self.publication, self.manifest = publication, manifest
        self.secrets = [manifest["minio"]["access_key_id"], manifest["minio"]["secret_access_key"]]
        self.env.update(NOVA_ENV_REST_ENV_FILE=str(publication / "env.sh"),
            HMS_IMAGE=self.freeze["source"]["hms_image_id"])
        self.status["binding"] = {"publication": str(publication),
            "manifest_sha256": sha(manifest_bytes),
            "owner_locator": locator, "env_id": manifest["env_id"],
            "catalog_id": manifest["runtime"]["catalog"]["id"],
            "object_store_id": manifest["runtime"]["object_store"]["id"]}
        self.hms_attempted = True
        self.command([str(REPO / "docker/iceberg-hive/up.sh"), "--env-file", str(publication / "env.sh")],
            self.bounds["owner_command_seconds"], "private-hms-up")
        hms_path = self.owner_root / locator["daemon_id"] / "hms" / self.status["binding"]["catalog_id"] / "manifest.json"
        hms_bytes = bounded_read(hms_path)
        hms = decode_json(hms_bytes)
        need(hms["state"] == "ready" and hms["owner_locator"] == locator and
             hms["catalog_id"] == self.status["binding"]["catalog_id"] and
             hms["rest_network"] == manifest["runtime"]["catalog"]["network"] and
             hms["images"]["hms"]["image_id"] == self.freeze["source"]["hms_image_id"], "HMS binding differs")
        warehouse = hms["hms"]["warehouse"]
        need(warehouse == manifest["runtime"]["catalog"]["server_warehouse"].rstrip("/") + "/hms",
             "HMS server warehouse differs")
        parsed = urlparse(warehouse)
        need(parsed.scheme == "s3" and parsed.netloc == "warehouse" and
             parsed.path == "/" + self.status["binding"]["catalog_id"] + "/rest/hms" and
             not parsed.query and not parsed.fragment and ".." not in parsed.path.split("/"),
             "HMS warehouse is not the exact private owner prefix")
        _, output = self.command([self.docker, "inspect", hms["container_id"], "--format",
            '{{json .Id}} {{json .Image}} {{json .Config.Labels}} {{json .State.Running}}'],
            self.bounds["inspection_seconds"], "actual-hms-container")
        fields = json_stream(output)
        need(len(fields) == 4 and fields[0] == hms["container_id"] and
             fields[1] == self.freeze["source"]["hms_image_id"] and fields[3] is True and
             fields[2].get("novarocks.fixture.owner") == hms["namespace"] and
             fields[2].get("novarocks.fixture.key") == hms["catalog_id"],
             "actual HMS image or container ownership differs")
        self.hms = hms
        self.status["binding"].update(hms_uri=hms["hms"]["uri"], hms_project=hms["project"],
            hms_container_id=hms["container_id"], hms_image_id=hms["images"]["hms"]["image_id"],
            hms_manifest_sha256=sha(hms_bytes), warehouse=warehouse)
        # Publish actual non-secret binding before the first metadata mutation.
        atomic_json(self.root / "bound-input.json", {"freeze": self.freeze,
            "binding": self.status["binding"], "namespace": self.namespace})

    def writer_ids(self, phase, deadline):
        _, output = self.command([self.docker, "ps", "-a", "-q", "--no-trunc",
            "--filter", "label=novarocks.m07.run=" + self.run_id,
            "--filter", "label=novarocks.m07.phase=" + phase], self.bounds["inspection_seconds"],
            "writer-census", deadline=deadline)
        values = output.decode().splitlines()
        need(len(values) <= 1 and all(re.fullmatch(r"[0-9a-f]{64}", item) for item in values),
             "writer identity census is ambiguous")
        return values

    def remove_writer(self, phase, deadline):
        for identity in self.writer_ids(phase, deadline):
            _, output = self.command([self.docker, "inspect", identity, "--format",
                '{{json .Id}} {{json .Image}} {{json .Config.Labels}} {{json .State.Running}}'],
                self.bounds["inspection_seconds"], "writer-inspect", deadline=deadline)
            fields = json_stream(output)
            need(len(fields) == 4 and fields[0] == identity and
                 fields[1] == self.freeze["source"]["writer_image_id"] and
                 fields[2].get("novarocks.m07.run") == self.run_id and
                 fields[2].get("novarocks.m07.phase") == phase, "writer removal owner differs")
            if fields[3] is True:
                self.command([self.docker, "kill", "--signal", "KILL", identity],
                    self.bounds["inspection_seconds"], "writer-kill", deadline=deadline)
            self.command([self.docker, "rm", identity], self.bounds["inspection_seconds"],
                "writer-remove", deadline=deadline)
        need(not self.writer_ids(phase, deadline), "writer did not physically exit")

    def safe_records(self, output, phase, receipt):
        records = markers(output, phase, self.freeze)
        receipt["records"], receipt["record_sha256"] = [], []
        for row in records:
            value, kind = row["value"], row["kind"]
            safe = canonical(row)
            need(all(not secret or secret.encode() not in safe for secret in self.secrets),
                 "safe marker contains credential material")
            if kind in ("table", "view"):
                validate_facts(value, kind, self.namespace, self.hms["hms"]["warehouse"], self.freeze)
            elif kind in ("baseline", "namespaces", "tables", "views", "all_objects"):
                name_set(value, self.bounds["max_namespaces"])
            elif kind == "mutation":
                exact_keys(value, ("operation", "namespace", "object", "state"))
                operations = {action + "_" + object_kind for action in ("create", "drop")
                    for object_kind in ("namespace", "table", "view")}
                need(value["operation"] in operations and value["state"] in ("attempt", "applied"),
                     "mutation protocol fields differ")
                object_kind = value["operation"].split("_", 1)[1]
                expected = self.namespace if object_kind == "namespace" else self.freeze["input"][object_kind + "_name"]
                need(value["namespace"] == self.namespace and value["object"] == expected,
                     "safe mutation identity differs")
            elif kind == "complete":
                need(value == {"status": "complete"}, "completion protocol differs")
            elif kind == "failure":
                exact_keys(value, ("exception_class", "message_sha256"))
                need(isinstance(value["exception_class"], str) and
                     re.fullmatch(r"[A-Za-z0-9_.$]{1,256}", value["exception_class"]) and
                     isinstance(value["message_sha256"], str) and
                     re.fullmatch(r"[0-9a-f]{64}", value["message_sha256"]), "unsafe failure projection")
            receipt["records"].append(row)
            receipt["record_sha256"].append(sha(safe))
            if kind == "mutation" and value["state"] == "applied":
                receipt["unconfirmed_mutations"] = [operation for operation in receipt["unconfirmed_mutations"]
                    if operation != value["operation"]]
        return records

    def stage(self, phase):
        phase_deadline = min(self.work_deadline, time.monotonic() + self.bounds["spark_stage_seconds"])
        script = self.root / (phase + ".scala")
        script.write_text(scala_program(phase, self.namespace, self.hms["hms"]["warehouse"], self.freeze))
        script.chmod(0o644)
        config = Path(self.hms["hms"]["spark_defaults"]).resolve(strict=True)
        need(config.is_relative_to(self.owner_root.resolve()), "Spark defaults escaped private owner")
        name = "nr-m07-hms-cap-" + self.run_id + "-" + phase
        self.writer_names.append(phase)
        # Register the intended exact label before create, including unknown outcome.
        self.status["phases"].append({"phase": phase, "container_name": name,
            "program_sha256": sha(bounded_read(script, 32768)), "records": [], "writer_exited": False,
            "unconfirmed_mutations": (["create_namespace", "create_table", "create_view"] if phase == "create"
                else ["drop_view", "drop_table", "drop_namespace"] if phase == "drop" else [])})
        receipt = self.status["phases"][-1]
        stage_failure = None
        try:
            _, output = self.command([self.docker, "create", "--pull", "never", "--interactive",
                "--name", name, "--platform", self.freeze["source"]["platform"],
                "--network", self.hms["rest_network"], "--label", "novarocks.m07.run=" + self.run_id,
                "--label", "novarocks.m07.phase=" + phase,
                "--mount", "type=bind,src=" + str(config) + ",dst=/run/m07/hms.conf,readonly",
                "--mount", "type=bind,src=" + str(script) + ",dst=/run/m07/program.scala,readonly",
                "--entrypoint", "/opt/spark/bin/spark-shell", self.freeze["source"]["writer_image_id"],
                "--master", "local[1]", "--conf", "spark.ui.enabled=false",
                "--properties-file", "/run/m07/hms.conf", "-i", "/run/m07/program.scala"],
                self.bounds["inspection_seconds"], "writer-create", deadline=phase_deadline)
            identity = output.decode().strip()
            need(re.fullmatch(r"[0-9a-f]{64}", identity), "writer create identity missing")
            need(self.writer_ids(phase, phase_deadline) == [identity], "writer create census differs")
            receipt["container_id"] = identity
            code, output = self.command([self.docker, "start", "--attach", "--interactive", identity],
                self.bounds["spark_stage_seconds"], "stock-spark-" + phase, deadline=phase_deadline,
                check=False, input_bytes=b":quit\n")
            receipt.update(exit_code=code, output_bytes=len(output), output_sha256=sha(output))
            # Never persist full Spark output, secrets, or metadata JSON bodies.
            parsed = self.safe_records(output, phase, receipt)
            need(code == 0 and not any(row["kind"] == "failure" for row in parsed)
                 and one(parsed, "complete") == {"status": "complete"}, "stock Spark stage did not complete")
            expected_kinds = {
                "create": ["baseline", *(["mutation"] * 6), "table", "view", "complete"],
                "oracle": ["namespaces", "tables", "views", "all_objects", "table", "view", "complete"],
                "drop": [*(["mutation"] * 6), "complete"], "restored": ["namespaces", "complete"],
            }
            need([row["kind"] for row in parsed] == expected_kinds[phase], "stage protocol record sequence differs")
            remaining(phase_deadline)
            return parsed
        except CaptureFailure as error:
            stage_failure = error
            receipt.update(**error.safe_facts(), output_bytes=len(error.output), output_sha256=sha(error.output))
            try:
                self.safe_records(error.output, phase, receipt)
            except BaseException as secondary:
                self.retain_cancellation("partial-stage-records-" + phase, secondary)
                receipt["partial_markers_rejected"] = True
                receipt["partial_marker_exception_class"] = type(secondary).__name__
            raise
        except BaseException as error:
            stage_failure = error
            self.retain_cancellation("stage-" + phase, error)
            raise
        finally:
            # An independent exit budget never authorizes another metadata operation.
            exit_failure = None
            try:
                exit_deadline = min(self.wall_deadline, time.monotonic() + self.bounds["writer_exit_seconds"])
                if self.status["resource_retained_whole_failure"]:
                    receipt["writer_exit_deferred_whole_failure_retained"] = True
                elif self.children:
                    receipt["writer_exit_deferred_until_host_reap"] = True
                else:
                    self.remove_writer(phase, exit_deadline)
                    receipt["writer_exited"] = True
            except BaseException as error:
                self.retain_cancellation("stage-writer-exit-" + phase, error)
                exit_failure = error
                receipt["writer_exit_error_class"] = type(error).__name__
                self.status["cleanup_errors"].append({"operation": "stage-writer-exit-" + phase,
                    "class": type(error).__name__, "dependent_owner_retained": True})
            try:
                atomic_json(self.root / (phase + "-receipt.json"), receipt)
            except BaseException as error:
                self.retain_cancellation("stage-receipt-" + phase, error)
                self.status["cleanup_errors"].append({"operation": "stage-receipt-" + phase,
                    "class": type(error).__name__})
                if exit_failure is None:
                    exit_failure = error
            # Secondary exit/receipt failures must not replace the primary capture.
            if stage_failure is None and exit_failure is not None:
                raise exit_failure

    def run_stages(self):
        created = self.stage("create")
        baseline = name_set(one(created, "baseline"), self.bounds["max_namespaces"])
        need(self.namespace not in baseline, "fresh namespace was already present")
        for kind in ("table", "view"):
            validate_facts(one(created, kind), kind, self.namespace, self.hms["hms"]["warehouse"], self.freeze)
        oracle = self.stage("oracle")
        need(name_set(one(oracle, "namespaces"), self.bounds["max_namespaces"]) == baseline | {self.namespace},
             "independent namespace oracle has missing or extra names")
        need(name_set(one(oracle, "tables"), 1) == {self.freeze["input"]["table_name"]} and
             name_set(one(oracle, "views"), 1) == {self.freeze["input"]["view_name"]}, "independent child oracle differs")
        need(name_set(one(oracle, "all_objects"), 2) ==
             {self.freeze["input"]["table_name"], self.freeze["input"]["view_name"]},
             "public list-all-tables object oracle has missing or extra names")
        for kind in ("table", "view"):
            validate_facts(one(oracle, kind), kind, self.namespace, self.hms["hms"]["warehouse"], self.freeze)
            need(one(oracle, kind) == one(created, kind), "independent loaded metadata changed")
        dropped = self.stage("drop")
        for records, prefix in ((created, "create"), (dropped, "drop")):
            expected = ["namespace", "table", "view"] if prefix == "create" else ["view", "table", "namespace"]
            mutations = [row["value"] for row in records if row["kind"] == "mutation"]
            need([(row.get("operation"), row.get("state")) for row in mutations] ==
                 [(prefix + "_" + kind, state) for kind in expected for state in ("attempt", "applied")],
                 "mutation ledger has duplicate, missing, or reordered operation")
            for row in mutations:
                exact_keys(row, ("operation", "namespace", "object", "state"))
                object_kind = row["operation"].split("_", 1)[1]
                expected_name = self.namespace if object_kind == "namespace" else self.freeze["input"][object_kind + "_name"]
                need(row["namespace"] == self.namespace and row["object"] == expected_name,
                     "mutation identity escaped ownership")
        restored = self.stage("restored")
        need(name_set(one(restored, "namespaces"), self.bounds["max_namespaces"]) == baseline,
             "final independent namespace baseline differs")
        self.status["capability"] = {"stock_table_create_load_drop": True, "stock_view_create_load_drop": True,
            "baseline_namespaces": sorted(baseline), "restored_namespaces": sorted(baseline),
            "table": one(oracle, "table"), "view": one(oracle, "view"),
            "stock_public_list_all_tables": one(oracle, "all_objects"),
            "native_mixed_object_classification": "OPEN; no NovaRocks process was started",
            "rust_hms_views": "Unsupported remains unchanged; no NovaRocks process was started"}

    def cleanup(self):
        if self.cleanup_deadline is None:
            self.cleanup_deadline = min(self.wall_deadline, time.monotonic() + self.bounds["cleanup_deadline_seconds"])
        deadline = self.cleanup_deadline
        # A reaped leader with a present/unknown group still retains dependencies.
        for process in list(self.children):
            try:
                releasable, facts = reap_child(process, min(deadline, self.wall_deadline),
                    self.bounds["host_reap_seconds"])
                if releasable:
                    self.children.remove(process)
                if facts["host_reap_errors"] or not releasable:
                    self.status["cleanup_errors"].append({"operation": "host-child-reap",
                        **facts})
                    if any(row.get("cancel_observed", False) for row in facts["host_reap_errors"]):
                        self.status["resource_retained_whole_failure"] = True
                        return
            except BaseException as error:
                self.retain_cancellation("cleanup-host-reap", error)
                self.status["cleanup_errors"].append({"operation": "host-child-reap",
                    "class": type(error).__name__, "dependent_owner_retained": True})
                if is_cancellation(error):
                    return
        if self.children:
            return
        if self.status["resource_retained_whole_failure"]:
            # Even reaped wrapper + absent original PGID does not cover a
            # detached Docker.command child after abnormal wrapper capture.
            self.status["cleanup_errors"].append({"operation": "whole-failure-owner-retained",
                "class": "UnconfirmedCommandDescendantSettlement", "dependent_owner_retained": True})
            return
        writers_ok = True
        for phase in self.writer_names:
            try:
                self.remove_writer(phase, deadline)
            except BaseException as error:
                self.retain_cancellation("cleanup-writer-exit-" + phase, error)
                writers_ok = False
                self.status["cleanup_errors"].append({"operation": "writer-exit-" + phase, "class": type(error).__name__})
                if self.children or self.status["resource_retained_whole_failure"]:
                    return
        if not writers_ok:
            return
        if self.manifest is None:
            if not self.rest_attempted:
                self.status["cleanup_complete"] = True
                return
            # An unknown startup outcome retains the new owner root; never guess IDs.
            self.status["cleanup_errors"].append({"operation": "private-rest-binding-unavailable",
                "class": "UnknownOwnerOutcome", "owner_root_retained": True})
            return
        locator = self.manifest["runtime"]["owner_locator"]
        catalog = self.manifest["runtime"]["catalog"]["id"]
        store = self.manifest["runtime"]["object_store"]["id"]
        operations = []
        if self.hms_attempted:
            operations.append(("private-hms-purge", [str(REPO / "docker/iceberg-hive/down.sh"),
                "--root", locator["control_root"], "--daemon", locator["daemon_id"], "--catalog-id", catalog, "--purge"]))
        operations.append(("private-rest-unbind-purge", [str(REPO / "docker/iceberg-rest/down.sh"), "--runtime-only", "--purge"]))
        for label, identity in (("private-catalog-delete", catalog), ("private-object-store-delete", store)):
            operations.append((label, [str(REPO / "docker/iceberg-rest/fixture-runtime.sh"),
                "--root", locator["control_root"], "--daemon", locator["daemon_id"], "delete", identity]))
        for label, args in operations:
            try:
                self.command(args, self.bounds["owner_command_seconds"], label, deadline=deadline)
            except BaseException as error:
                self.retain_cancellation(label, error)
                self.status["cleanup_errors"].append({"operation": label, "class": type(error).__name__,
                    "dependent_owner_retained": True})
                return
        self.status["cleanup_complete"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    freeze_path = Path(args.freeze).resolve(strict=True)
    freeze_bytes = bounded_read(freeze_path)
    freeze = decode_json(freeze_bytes)
    validate_freeze(freeze)
    output = Path(args.output).resolve()
    need(output.is_relative_to(REPO / "logs/mem-1-m07"), "output escaped task-private ignored artifact root")
    need(re.fullmatch(r"[a-z0-9_-]+", output.name), "output directory name is not a finite safe identifier")
    output.mkdir(parents=True, exist_ok=False)
    run = Preflight(freeze, output)
    run.status.update(freeze_file_path=str(freeze_path), freeze_file_sha256=sha(freeze_bytes))
    passed = False
    try:
        run.source_precheck()
        run.bind_owner()
        run.run_stages()
        remaining(run.work_deadline)
        passed = True
    except BaseException as error:
        run.retain_cancellation("preflight", error)
        run.status["errors"].append({"class": type(error).__name__,
            "message": str(error) if isinstance(error, Refusal) else "unclassified preflight failure; no raw provider payload saved"})
    finally:
        try:
            run.cleanup()
        except BaseException as error:
            run.retain_cancellation("cleanup-dispatch", error)
            run.status["cleanup_errors"].append({"operation": "cleanup-dispatch", "class": type(error).__name__})
        passed = (passed and run.status["cleanup_complete"] and not run.status["cleanup_errors"]
            and not run.children and not run.status["resource_retained_whole_failure"])
        run.status["unconfirmed_host_children"] = [child.m07_exit_facts for child in run.children]
        run.status["status"] = "CAPABILITY_PREFLIGHT_PASS" if passed else "CAPABILITY_PREFLIGHT_FAILED"
        try:
            atomic_json(output / "status.json", run.status)
            if passed:
                atomic_json(output / "CAPABILITY_PREFLIGHT_PASS.json", {"status": run.status["status"],
                    "status_sha256": sha(bounded_read(output / "status.json")), "scope": run.status["scope"]})
            print(json.dumps({"status": run.status["status"], "output": str(output)}, sort_keys=True))
        except BaseException as error:
            passed = False
            run.retain_cancellation("final-receipt", error)
            run.status["status"] = "CAPABILITY_PREFLIGHT_FAILED"
            run.status["errors"].append({"operation": "final-receipt", "class": type(error).__name__})
            try:
                # Remove only this new artifact's success marker, never provider resources.
                (output / "CAPABILITY_PREFLIGHT_PASS.json").unlink(missing_ok=True)
                atomic_json(output / "status.json", run.status)
            except BaseException as secondary:
                run.retain_cancellation("failed-receipt", secondary)
                run.status["errors"].append({"operation": "failed-receipt", "class": type(secondary).__name__})
            try:
                print(json.dumps({"status": "CAPABILITY_PREFLIGHT_FAILED", "output": str(output)}, sort_keys=True))
            except BaseException as secondary:
                run.retain_cancellation("failure-output", secondary)
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        exit_code = main()
    except BaseException as error:
        print(json.dumps({"status": "PRECHECK_OR_RECEIPT_FAILURE", "class": type(error).__name__,
            "message": str(error) if isinstance(error, Refusal) else "input validation failed"}, sort_keys=True))
        exit_code = 1
    sys.exit(exit_code)
