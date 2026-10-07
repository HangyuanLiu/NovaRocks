#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""Host persistence and orchestration for this exact SDK fixture only."""

import argparse
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import uuid
from urllib.parse import urlsplit

import job

CAP = 256 * 1024
CASES = ("foreign", "formal_empty", "valid", "namespace", "version", "domain",
         "duplicate", "carrier", "history", "dual", "member", "budget",
         "tiny_root_high", "tiny_root_low", "small_root_high", "small_root_low",
         "tiny_list_high", "tiny_list_low", "small_list_high", "small_list_low")
PHASES = ("create_confirmed", "file_intent", "file_closed", "append_confirmed",
          "properties_confirmed")
REASONS = {"unknown_create", "ownership_conflict", "incomplete_journal",
           "storage_error", "over_budget"}
HEX = re.compile(r"[0-9a-f]{64}\Z")
TERMINAL_KEYS = {"record", "version", "run_token", "phase", "job_token", "container_id",
                 "image_id", "image_reference", "script_sha256", "defaults_sha256",
                 "publication_identity_sha256", "execution_confirmed", "exit_code",
                 "confirmed_gone", "forced"}


class LifecycleError(RuntimeError):
    """Status-only errors must not echo provider configuration or SDK logs."""


def require(value, message):
    if not value:
        raise LifecycleError(message)


def canonical(value):
    try:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False,
                          allow_nan=False).encode("utf-8")
    except (ValueError, UnicodeError, RecursionError) as error:
        raise LifecycleError("Invalid fixture JSON representation") from error


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def closed(value, keys):
    require(type(value) is dict and set(value) == set(keys), "Invalid closed fixture record")


def text(value):
    require(type(value) is str and 0 < len(value.encode("utf-8")) <= CAP,
            "Invalid fixture text")


def integer(value, minimum=0):
    require(type(value) is int and minimum <= value <= 2**63 - 1, "Invalid fixture integer")


def boolean(value):
    require(type(value) is bool, "Invalid fixture boolean")


def hash_value(value):
    require(type(value) is str and HEX.fullmatch(value), "Invalid fixture hash")


def uuid_value(value):
    try:
        require(type(value) is str and str(uuid.UUID(value)) == value, "Invalid fixture UUID")
    except (ValueError, AttributeError) as error:
        raise LifecycleError("Invalid fixture UUID") from error


def parse(raw):
    require(type(raw) is bytes and 0 < len(raw) <= CAP, "Fixture receipt exceeds byte bound")

    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, "Duplicate fixture JSON member")
            out[key] = value
        return out

    def nonfinite(_):
        raise LifecycleError("Nonfinite fixture JSON number")

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=nonfinite)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise LifecycleError("Invalid fixture JSON") from error
    require(canonical(value) == raw, "Noncanonical fixture receipt")
    return value


def read(path, cap=CAP):
    try:
        return job.read_regular(Path(path), cap)
    except (OSError, job.JobFailure) as error:
        raise LifecycleError("Cannot read exact bounded fixture file") from error


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def persist(path, raw, new=False):
    """Atomic exclusive publication; an existing identity can only be replayed."""
    require(0 < len(raw) <= CAP, "Fixture output exceeds byte bound")
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as target:
            target.write(raw)
            target.flush()
            os.fsync(target.fileno())
        try:
            # Same-directory link publishes complete bytes without replacing a
            # conflicting identity, including a concurrent creator or symlink.
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            require(not new and read(path) == raw, "Conflicting immutable fixture file")
        sync_directory(path.parent)
    finally:
        os.unlink(temporary)
        sync_directory(path.parent)


def header(value, record, owner=None):
    require(value["record"] == record and type(value["version"]) is int
            and value["version"] == 1, "Unsupported fixture record version")
    uuid_value(value["run_token"])
    text(value["namespace"])
    if owner is not None:
        require(value["run_token"] == owner["run_token"]
                and value["namespace"] == owner["namespace"], "Fixture owner mismatch")


def validate_owner(value, namespace, publication_identity):
    closed(value, {"record", "version", "run_token", "namespace", "cases",
                   "publication_identity_sha256"})
    header(value, "field_domain_fixture_owner")
    require(value["namespace"] == namespace and re.fullmatch(r"ns_[a-zA-Z0-9_]{1,93}", namespace),
            "Fixture namespace mismatch")
    require(value["cases"] == list(CASES), "Fixture case set mismatch")
    hash_value(value["publication_identity_sha256"])
    require(value["publication_identity_sha256"] == publication_identity,
            "Fixture publication mismatch")


def validate_terminal(value, owner, phase, launch=None):
    closed(value, TERMINAL_KEYS)
    require(value["record"] == "field_domain_owned_spark_terminal"
            and type(value["version"]) is int and value["version"] == 1,
            "Unsupported owned Spark terminal")
    require(value["run_token"] == owner["run_token"] and value["phase"] == phase,
            "Owned Spark action mismatch")
    uuid_value(value["job_token"])
    for key in ("script_sha256", "defaults_sha256", "publication_identity_sha256"):
        hash_value(value[key])
    require(value["publication_identity_sha256"] == owner["publication_identity_sha256"],
            "Owned Spark publication mismatch")
    require(type(value["image_id"]) is str and value["image_id"].startswith("sha256:")
            and HEX.fullmatch(value["image_id"][7:]), "Invalid exact Spark image")
    text(value["image_reference"])
    # The immutable image ID and the exact BOM reference are distinct facts;
    # the job owner validates them against one publication before launch.
    if value["container_id"] is not None:
        hash_value(value["container_id"])
    for key in ("execution_confirmed", "confirmed_gone", "forced"):
        boolean(value[key])
    if value["exit_code"] is not None:
        integer(value["exit_code"])
        require(value["exit_code"] <= 255, "Invalid Spark exit status")
    require(value["execution_confirmed"] == (value["exit_code"] is not None),
            "Inconsistent Spark execution outcome")
    if launch is not None:
        closed(launch, TERMINAL_KEYS)
        for key in ("record", "version", "run_token", "phase", "job_token", "image_id",
                    "image_reference", "script_sha256", "defaults_sha256",
                    "publication_identity_sha256"):
            require(value[key] == launch[key], "Owned Spark launch identity changed")


def quiescent(value):
    require(value["confirmed_gone"] is True and value["container_id"] is not None,
            "Initializer container termination is unconfirmed")


def successful(value):
    quiescent(value)
    require(value["execution_confirmed"] is True and value["exit_code"] == 0,
            "Owned Spark action did not succeed")


def paths(items, keys):
    require(type(items) is list, "Invalid fixture object collection")
    seen = set()
    for item in items:
        closed(item, keys)
        text(item["path"])
        require(item["path"] not in seen, "Duplicate fixture object path")
        seen.add(item["path"])
    return seen


def validate_candidate(value, owner, owner_raw, terminal_raw):
    closed(value, {"record", "version", "run_token", "namespace", "owner_sha256",
                   "terminal_sha256", "publication_identity_sha256", "tables"})
    header(value, "field_domain_cleanup_candidate", owner)
    require(value["owner_sha256"] == digest(owner_raw)
            and value["terminal_sha256"] == digest(terminal_raw)
            and value["publication_identity_sha256"] == owner["publication_identity_sha256"],
            "Cleanup candidate authority mismatch")
    require(type(value["tables"]) is list and len(value["tables"]) == len(CASES),
            "Cleanup candidate case set mismatch")
    uuids = set()
    all_paths = set()
    for name, table in zip(CASES, value["tables"]):
        require(type(table) is dict and table.get("case") == name, "Cleanup case order mismatch")
        if table.get("state") == "unresolved":
            closed(table, {"case", "state", "reason"})
            require(type(table["reason"]) is str and table["reason"] in REASONS, "Unknown cleanup refusal")
            continue
        closed(table, {"case", "state", "identity", "create_evidence", "journal_objects",
                       "registered_data_files", "known_sdk_objects", "unproven_sdk_orphans"})
        require(table["state"] in ("owned_present", "owned_absent"), "Invalid cleanup state")
        closed(table["identity"], {"table_uuid", "table_location"})
        uuid_value(table["identity"]["table_uuid"])
        require(table["identity"]["table_uuid"] not in uuids, "Duplicate owned table UUID")
        uuids.add(table["identity"]["table_uuid"])
        location = table["identity"]["table_location"]
        text(location)
        uri = urlsplit(location)
        require(uri.scheme in ("s3", "s3a", "s3n") and uri.hostname and not uri.query
                and not uri.fragment and uri.username is None and not location.endswith("/")
                and ".." not in uri.path.split("/"), "Invalid exact table location")
        require(table["create_evidence"] in ("journal", "recovered_token", "durable_candidate"),
                "Unknown CREATE evidence")
        require(table["unproven_sdk_orphans"] is True, "Invalid orphan proof claim")
        journals = table["journal_objects"]
        require(type(journals) is list and len(journals) <= len(PHASES), "Excessive journal set")
        paths(journals, {"phase", "path", "sha256"})
        require([entry["phase"] for entry in journals] == list(PHASES[:len(journals)]),
                "Journal phase order mismatch")
        for index, entry in enumerate(journals):
            hash_value(entry["sha256"])
            expected_path = (location + "/_uea7b3_fixture/" + owner["run_token"] + "/"
                             + name + "/" + str(index + 1) + "-" + PHASES[index] + ".json")
            require(entry["path"] == expected_path, "Journal path differs from exact phase")
        files = table["registered_data_files"]
        require(type(files) is list and len(files) <= 1, "Excessive registered data files")
        paths(files, {"path", "closed_fact"})
        require((len(journals) >= 2) == (len(files) == 1), "Registered file has no exact intent")
        for entry in files:
            require(entry["path"] == location + "/data/domain-input-" + owner["run_token"]
                    + "-" + name + ".parquet", "Registered data path differs from intent")
            fact = entry["closed_fact"]
            if fact is not None:
                closed(fact, {"file_size", "record_count", "sha256"})
                integer(fact["file_size"], 1)
                require(fact["file_size"] <= 1024 * 1024, "Fixture file exceeds byte bound")
                integer(fact["record_count"], 1)
                require(fact["record_count"] == (1 if name in CASES[12:] else 5),
                        "Registered record count differs from fixed input")
                hash_value(fact["sha256"])
        require(type(table["known_sdk_objects"]) is list and len(table["known_sdk_objects"]) <= 8,
                "Excessive known SDK object inventory")
        paths(table["known_sdk_objects"], {"kind", "path"})
        for entry in table["known_sdk_objects"]:
            require(entry["kind"] in ("metadata", "manifest_list", "manifest"),
                    "Unknown SDK object kind")
        location = table["identity"]["table_location"].rstrip("/") + "/"
        for entry in journals + files + table["known_sdk_objects"]:
            require(entry["path"].startswith(location) and entry["path"] not in all_paths,
                    "Cleanup object scope conflict")
            all_paths.add(entry["path"])


def make_ack(owner, owner_raw, candidate_raw):
    # Callers must successfully persist the candidate before this function.
    return {"record": "field_domain_cleanup_ack", "version": 1,
            "run_token": owner["run_token"], "namespace": owner["namespace"],
            "owner_sha256": digest(owner_raw), "candidate_sha256": digest(candidate_raw)}


def validate_ack(value, owner, owner_raw, candidate_raw):
    closed(value, {"record", "version", "run_token", "namespace", "owner_sha256",
                   "candidate_sha256"})
    header(value, "field_domain_cleanup_ack", owner)
    require(value == make_ack(owner, owner_raw, candidate_raw), "Cleanup ACK mismatch")


def validate_result(value, owner, owner_raw, candidate_raw, candidate):
    closed(value, {"record", "version", "run_token", "namespace", "owner_sha256",
                   "candidate_sha256", "complete", "tables"})
    header(value, "field_domain_cleanup_result", owner)
    boolean(value["complete"])
    require(value["owner_sha256"] == digest(owner_raw)
            and value["candidate_sha256"] == digest(candidate_raw), "Cleanup result authority mismatch")
    require(type(value["tables"]) is list and len(value["tables"]) == len(CASES),
            "Cleanup result case set mismatch")
    complete = True
    for planned, actual in zip(candidate["tables"], value["tables"]):
        require(type(actual) is dict and actual.get("case") == planned["case"],
                "Cleanup result case mismatch")
        if planned["state"] == "unresolved":
            require(actual == planned, "Unresolved case acquired deletion authority")
            complete = False
            continue
        if actual.get("state") == "unresolved":
            closed(actual, {"case", "state", "reason"})
            require(type(actual["reason"]) is str and actual["reason"] in REASONS,
                    "Unknown cleanup result refusal")
            complete = False
            continue
        closed(actual, {"case", "state", "table_uuid", "catalog_absent", "registered_data_files",
                        "known_sdk_objects", "journal_objects", "unproven_sdk_orphans", "unresolved"})
        require(actual["state"] == "owned" and actual["table_uuid"] == planned["identity"]["table_uuid"],
                "Cleanup result table identity mismatch")
        boolean(actual["catalog_absent"])
        require(actual["unproven_sdk_orphans"] is True and type(actual["unresolved"]) is list
                and all(type(reason) is str and reason in REASONS for reason in actual["unresolved"]), "Invalid cleanup conclusion")
        complete &= actual["catalog_absent"] and not actual["unresolved"]
        for collection, keys in (("registered_data_files", {"path", "absent"}),
                                 ("known_sdk_objects", {"kind", "path", "absent"}),
                                 ("journal_objects", {"phase", "path", "retained"})):
            paths(actual[collection], keys)
            require(len(actual[collection]) == len(planned[collection]), "Cleanup object inventory changed")
            for before, after in zip(planned[collection], actual[collection]):
                require(after["path"] == before["path"], "Cleanup object path changed")
                if collection == "journal_objects":
                    require(after["phase"] == before["phase"] and after["retained"] is True,
                            "Cleanup journal retention changed")
                else:
                    if collection == "known_sdk_objects":
                        require(after["kind"] == before["kind"], "Cleanup object kind changed")
                    boolean(after["absent"])
                    complete &= after["absent"]
    require(value["complete"] == bool(complete), "Unproven cleanup completeness")


def validate_full(value, namespace, record):
    closed(value, {"record", "namespace", "tables"})
    require(value["record"] == record and value["namespace"] == namespace
            and type(value["tables"]) is list and len(value["tables"]) == len(CASES),
            "Invalid complete SDK receipt")
    for name, table in zip(CASES, value["tables"]):
        closed(table, {"case", "table_uuid", "metadata_path", "metadata_bytes", "metadata_sha256",
                       "snapshot", "snapshot_sequence", "snapshot_schema_id", "schema_id", "schema_json",
                       "retained_schemas", "properties", "files", "physical_file", "sdk_rows",
                       "expected_provider_failure_kind"})
        require(table["case"] == name, "Complete SDK case order mismatch")
        uuid_value(table["table_uuid"])
        hash_value(table["metadata_sha256"])
        for key in ("metadata_path", "schema_json"):
            text(table[key])
        for key in ("metadata_bytes", "snapshot", "snapshot_sequence"):
            integer(table[key], 1)
        for key in ("snapshot_schema_id", "schema_id"):
            integer(table[key])
        require(type(table["retained_schemas"]) is list and len(table["retained_schemas"]) == 1,
                "Invalid retained schema receipt")
        retained = table["retained_schemas"][0]
        closed(retained, {"schema_id", "schema_json"})
        require(retained == {"schema_id": table["schema_id"], "schema_json": table["schema_json"]},
                "Retained schema fact mismatch")
        require(type(table["properties"]) is list, "Invalid properties receipt")
        for prop in table["properties"]:
            closed(prop, {"key", "bytes", "sha256"})
            text(prop["key"])
            integer(prop["bytes"], 1)
            hash_value(prop["sha256"])
        require(type(table["files"]) is list and len(table["files"]) == 1,
                "Invalid exact data file set")
        file = table["files"][0]
        closed(file, {"path", "records", "bytes", "spec_id", "data_sequence", "file_sequence", "deletes"})
        text(file["path"])
        for key in ("records", "bytes", "data_sequence", "file_sequence"):
            integer(file[key], 1)
        integer(file["spec_id"])
        require(file["deletes"] == [], "Unexpected fixture delete files")
        physical = table["physical_file"]
        closed(physical, {"path", "bytes", "sha256", "raw_fields"})
        require(physical["path"] == file["path"] and physical["bytes"] == file["bytes"],
                "Physical data file fact mismatch")
        hash_value(physical["sha256"])
        require(type(physical["raw_fields"]) is list and len(physical["raw_fields"]) == 8,
                "Invalid physical field inventory")
        ids = set()
        for field in physical["raw_fields"]:
            closed(field, {"id", "path", "repetition", "primitive"})
            integer(field["id"], 1)
            require(field["id"] not in ids, "Duplicate physical field identity")
            ids.add(field["id"])
            text(field["path"])
            require(field["repetition"] in ("REQUIRED", "OPTIONAL"), "Invalid physical requiredness")
            require(field["primitive"] in (None, "INT64", "INT32", "BINARY"), "Invalid physical primitive")
        require(type(table["sdk_rows"]) is list and 0 < len(table["sdk_rows"]) <= 5
                and all(type(row) is str for row in table["sdk_rows"]), "Invalid complete SDK bag")
        expected = "ResourceExhausted" if name == "budget" else (
            "None" if name in CASES[:3] else "CorruptData")
        require(table["expected_provider_failure_kind"] == expected, "Invalid expected failure annotation")


def receipt(log, record):
    raw = read(log, job.LOG_BYTES)
    matches = []
    for index, line in enumerate(raw.splitlines()):
        require(index < 10000, "Fixture log line count exceeds bound")
        require(len(line) < CAP + 64, "Fixture log line exceeds bound")
        if line.startswith(b"UEA4G_RECEIPT "):
            payload = line[len(b"UEA4G_RECEIPT "):]
            value = parse(payload)
            if type(value) is dict and value.get("record") == record:
                matches.append((payload, value))
    require(len(matches) == 1, "Missing or duplicate exact SDK receipt")
    return matches[0]


def scala_literal(raw):
    encoded = base64.b64encode(raw).decode("ascii")
    return "Vector(" + ",".join(json.dumps(encoded[i:i+16384])
                                for i in range(0, len(encoded), 16384)) + ").mkString"


class Lifecycle:
    def __init__(self, workspace, publication, directory, namespace, jobs=job):
        self.workspace = Path(workspace).resolve(strict=True)
        self.publication = Path(publication).resolve(strict=True)
        if self.publication.is_file():
            require(self.publication.name == "env.sh", "Invalid fixture publication file")
            self.publication = self.publication.parent
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(not self.directory.is_symlink() and stat.S_ISDIR(self.directory.lstat().st_mode),
                "Invalid fixture receipt directory")
        self.directory = self.directory.resolve(strict=True)
        self.state = self.directory / "lifecycle"
        self.state.mkdir(mode=0o700, exist_ok=True)
        require(not self.state.is_symlink(), "Invalid lifecycle directory")
        self.namespace = namespace
        self.jobs = jobs
        self.publication_hash = jobs.publication_identity(self.publication)
        self.owner = self.owner_raw = None

    def load_owner(self):
        self.owner_raw = read(self.state / "owner.json")
        self.owner = parse(self.owner_raw)
        validate_owner(self.owner, self.namespace, self.publication_hash)

    def action(self, phase, call):
        scripts = self.directory / "scripts"
        runs = self.directory / "jobs"
        for path in (scripts, runs):
            path.mkdir(mode=0o700, exist_ok=True)
            require(not path.is_symlink(), "Invalid job input directory")
        invocation = str(uuid.uuid4())
        script = scripts / (phase + "-" + invocation + ".scala")
        fixture = self.workspace / "tests/sql/fixtures"
        source = b"\n".join(read(fixture / name) for name in (
            "iceberg-delete-applicability/generate.scala", "uea7b3-field-domain-invalid/fixture.scala"))
        entry = ('\ntry { FieldDomainInvalidFixture.run {\n'
                 'DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, '
                 + json.dumps(self.namespace) + ', "field_domain_invalid")\n' + call
                 + '\n} } catch { case failure: Throwable => '
                 'System.err.println("Field-domain SDK action failed"); System.exit(1) }\n')
        persist(script, source + entry.encode("utf-8"), new=True)
        run_directory = runs / (phase + "-" + invocation)
        if phase == "initialize":
            invocation_record = {"record": "field_domain_initializer_invocation", "version": 1,
                                 "run_token": self.owner["run_token"],
                                 "job_directory": str(run_directory), "script": str(script),
                                 "script_sha256": digest(read(script))}
            persist(self.state / "initializer-invocation.json", canonical(invocation_record), new=True)
        succeeded = False
        try:
            self.jobs.run(self.workspace, self.publication, script, run_directory,
                          self.owner["run_token"], phase)
            succeeded = True
        except job.JobFailure:
            pass
        # Exact invocation path, including failures; never select a latest job.
        terminal_raw = read(run_directory / "terminal.json")
        terminal = parse(terminal_raw)
        launch = parse(read(run_directory / "launch.json"))
        validate_terminal(terminal, self.owner, phase, launch)
        require(terminal["script_sha256"] == digest(read(script)), "Spark script changed")
        if phase == "initialize":
            persist(self.state / "initializer-terminal.json", terminal_raw, new=True)
        require(succeeded, "Owned Spark action failed; exact terminal receipt retained")
        successful(terminal)
        return run_directory / "output/stdout.log"

    def initialize(self, fail_after=""):
        require(not fail_after or fail_after in {case + ":" + phase for case in CASES for phase in PHASES},
                "Invalid fixture failure phase")
        self.owner = {"record": "field_domain_fixture_owner", "version": 1,
                      "run_token": str(uuid.uuid4()), "namespace": self.namespace,
                      "cases": list(CASES), "publication_identity_sha256": self.publication_hash}
        validate_owner(self.owner, self.namespace, self.publication_hash)
        self.owner_raw = canonical(self.owner)
        persist(self.state / "owner.json", self.owner_raw, new=True)
        log = self.action("initialize", "FieldDomainInvalidFixture.initialize("
                          + json.dumps(self.namespace) + "," + scala_literal(self.owner_raw)
                          + "," + json.dumps(fail_after) + ")")
        raw, value = receipt(log, "field_domain_invalid_initial")
        validate_full(value, self.namespace, "field_domain_invalid_initial")
        persist(self.directory / "initialize.json", raw)
        return "FIELD_DOMAIN_INVALID_READY"

    def observe(self):
        self.load_owner()
        initial = read(self.directory / "initialize.json")
        validate_full(parse(initial), self.namespace, "field_domain_invalid_initial")
        log = self.action("observe", "FieldDomainInvalidFixture.observe("
                          + json.dumps(self.namespace) + "," + scala_literal(initial) + ")")
        raw, value = receipt(log, "field_domain_invalid_unchanged")
        validate_full(value, self.namespace, "field_domain_invalid_unchanged")
        expected = parse(initial)
        require(value["tables"] == expected["tables"], "Complete SDK facts changed")
        persist(self.directory / "observe.json", raw)
        return "FIELD_DOMAIN_INVALID_UNCHANGED"

    def initializer_terminal(self):
        invocation = parse(read(self.state / "initializer-invocation.json"))
        closed(invocation, {"record", "version", "run_token", "job_directory", "script", "script_sha256"})
        require(invocation["record"] == "field_domain_initializer_invocation"
                and type(invocation["version"]) is int and invocation["version"] == 1
                and invocation["run_token"] == self.owner["run_token"], "Initializer invocation mismatch")
        script, directory = Path(invocation["script"]), Path(invocation["job_directory"])
        require(script.parent == self.directory / "scripts" and directory.parent == self.directory / "jobs"
                and script.name == directory.name + ".scala" and directory.name.startswith("initialize-"),
                "Initializer invocation escaped exact receipt scope")
        for exact_directory in (script.parent, directory.parent, directory):
            require(not exact_directory.is_symlink()
                    and stat.S_ISDIR(exact_directory.lstat().st_mode)
                    and exact_directory.resolve(strict=True) == exact_directory,
                    "Initializer directory identity changed")
        uuid_value(directory.name[len("initialize-"):])
        hash_value(invocation["script_sha256"])
        require(digest(read(script)) == invocation["script_sha256"], "Initializer script changed")
        raw = read(directory / "terminal.json")
        terminal = parse(raw)
        validate_terminal(terminal, self.owner, "initialize", parse(read(directory / "launch.json")))
        require(terminal["script_sha256"] == invocation["script_sha256"], "Initializer script identity mismatch")
        persist(self.state / "initializer-terminal.json", raw)
        return raw, terminal

    def cleanup(self):
        self.load_owner()
        terminal_raw, terminal = self.initializer_terminal()
        quiescent(terminal)
        candidate_path = self.state / "cleanup-candidate.json"
        ack_path = self.state / "cleanup-ack.json"
        if candidate_path.exists() or candidate_path.is_symlink():
            candidate_raw = read(candidate_path)
            candidate = parse(candidate_raw)
            validate_candidate(candidate, self.owner, self.owner_raw, terminal_raw)
        else:
            require(not ack_path.exists() and not ack_path.is_symlink(), "ACK has no durable candidate")
            initial_path = self.directory / "initialize.json"
            initial_arg = "None"
            if initial_path.exists() or initial_path.is_symlink():
                initial = read(initial_path)
                validate_full(parse(initial), self.namespace, "field_domain_invalid_initial")
                initial_arg = "Some(" + scala_literal(initial) + ")"
            log = self.action("prepare-cleanup", "FieldDomainInvalidFixture.prepareCleanup("
                              + json.dumps(self.namespace) + "," + scala_literal(self.owner_raw)
                              + "," + scala_literal(terminal_raw) + "," + initial_arg + ")")
            candidate_raw, candidate = receipt(log, "field_domain_cleanup_candidate")
            validate_candidate(candidate, self.owner, self.owner_raw, terminal_raw)
            persist(candidate_path, candidate_raw)
        # Reconfirm durability on replay too, including a previous process that
        # published bytes but failed its directory fsync before issuing an ACK.
        persist(candidate_path, candidate_raw)
        # ACK construction reads the published candidate, never stdout bytes.
        require(read(candidate_path) == candidate_raw, "Candidate persistence is unconfirmed")
        ack_raw = canonical(make_ack(self.owner, self.owner_raw, candidate_raw))
        persist(ack_path, ack_raw)
        validate_ack(parse(read(ack_path)), self.owner, self.owner_raw, candidate_raw)
        log = self.action("commit-cleanup", "FieldDomainInvalidFixture.commitCleanup("
                          + json.dumps(self.namespace) + "," + scala_literal(self.owner_raw)
                          + "," + scala_literal(candidate_raw) + "," + scala_literal(ack_raw) + ")")
        result_raw, result = receipt(log, "field_domain_cleanup_result")
        validate_result(result, self.owner, self.owner_raw, candidate_raw, candidate)
        # Every actual commit invocation retains its own result. A retry may
        # advance a previous partial outcome without overwriting its evidence.
        persist(log.parent.parent / "cleanup-result.json", result_raw)
        require(result["complete"], "Partial cleanup remains unresolved; exact subset result retained")
        persist(self.state / "cleanup-result.json", result_raw)
        return "FIELD_DOMAIN_INVALID_CLEANED"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("initialize", "observe", "cleanup"))
    for name in ("workspace", "publication", "directory", "namespace"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--fail-after", default="")
    args = parser.parse_args(argv)
    require(args.action == "initialize" or not args.fail_after, "Failure injection requires initialize")
    controller = Lifecycle(args.workspace, args.publication, args.directory, args.namespace)
    lock = os.open(controller.state / "lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        require(stat.S_ISREG(os.fstat(lock).st_mode), "Invalid lifecycle lock")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.action == "initialize":
            output = controller.initialize(args.fail_after)
        else:
            output = getattr(controller, args.action)()
        print(output)
    finally:
        os.close(lock)


if __name__ == "__main__":
    try:
        main()
    except (LifecycleError, job.JobFailure, OSError, ValueError, UnicodeError):
        print("Field-domain fixture lifecycle failed; exact bounded receipts retained", file=sys.stderr)
        sys.exit(1)
