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
"""Pure host tests; no SDK, Docker, SQL runner or external process is invoked."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import lifecycle as l

HASH = "a" * 64


def owner():
    return {"record": "field_domain_fixture_owner", "version": 1,
            "run_token": str(uuid.uuid4()), "namespace": "ns_runner_not_uuid",
            "cases": list(l.CASES), "publication_identity_sha256": HASH}


def terminal(own, phase="initialize"):
    return {"record": "field_domain_owned_spark_terminal", "version": 1,
            "run_token": own["run_token"], "phase": phase, "job_token": str(uuid.uuid4()),
            "container_id": "b" * 64, "image_id": "sha256:" + HASH,
            "image_reference": "apache/iceberg:1.11.0-fixture-bom",
            "script_sha256": HASH, "defaults_sha256": HASH,
            "publication_identity_sha256": HASH, "execution_confirmed": True,
            "exit_code": 0, "confirmed_gone": True, "forced": False}


def candidate(own, own_raw, term_raw, partial=False):
    tables = []
    for index, case in enumerate(l.CASES):
        if partial and index:
            tables.append({"case": case, "state": "unresolved", "reason": "unknown_create"})
            continue
        location = "s3://owned/ns/fd_" + case
        tables.append({"case": case, "state": "owned_present",
                       "identity": {"table_uuid": str(uuid.UUID(int=index + 1)), "table_location": location},
                       "create_evidence": "journal",
                       "journal_objects": [{"phase": phase, "path": location + "/_uea7b3_fixture/"
                                            + own["run_token"] + "/" + case + "/" + str(i + 1)
                                            + "-" + phase + ".json", "sha256": HASH}
                                           for i, phase in enumerate(l.PHASES[:2])],
                       "registered_data_files": [{"path": location + "/data/domain-input-" + own["run_token"]
                                                  + "-" + case + ".parquet", "closed_fact": None}],
                       "known_sdk_objects": [{"kind": "metadata", "path": location + "/metadata/exact.json"}],
                       "unproven_sdk_orphans": True})
    return {"record": "field_domain_cleanup_candidate", "version": 1,
            "run_token": own["run_token"], "namespace": own["namespace"],
            "owner_sha256": l.digest(own_raw), "terminal_sha256": l.digest(term_raw),
            "publication_identity_sha256": HASH, "tables": tables}


def result(own, own_raw, candidate_raw, planned):
    tables = []
    for table in planned["tables"]:
        if table["state"] == "unresolved":
            tables.append(copy.deepcopy(table))
            continue
        tables.append({"case": table["case"], "state": "owned",
                       "table_uuid": table["identity"]["table_uuid"], "catalog_absent": True,
                       "registered_data_files": [{"path": file["path"], "absent": True}
                                                 for file in table["registered_data_files"]],
                       "known_sdk_objects": [{"kind": file["kind"], "path": file["path"], "absent": True}
                                             for file in table["known_sdk_objects"]],
                       "journal_objects": [{"phase": file["phase"], "path": file["path"], "retained": True}
                                           for file in table["journal_objects"]],
                       "unproven_sdk_orphans": True, "unresolved": []})
    return {"record": "field_domain_cleanup_result", "version": 1,
            "run_token": own["run_token"], "namespace": own["namespace"],
            "owner_sha256": l.digest(own_raw), "candidate_sha256": l.digest(candidate_raw),
            "complete": all(table["state"] == "owned" for table in tables), "tables": tables}


def full_receipt(namespace, record="field_domain_invalid_initial"):
    tables = []
    for index, case in enumerate(l.CASES):
        file = "s3://owned/fd_" + case + "/data/exact.parquet"
        tables.append({"case": case, "table_uuid": str(uuid.UUID(int=index + 1)),
                       "metadata_path": "s3://owned/fd_" + case + "/metadata/exact.json",
                       "metadata_bytes": 100, "metadata_sha256": HASH,
                       "snapshot": 42, "snapshot_sequence": 1, "snapshot_schema_id": 0,
                       "schema_id": 0, "schema_json": "{}",
                       "retained_schemas": [{"schema_id": 0, "schema_json": "{}"}],
                       "properties": [], "files": [{"path": file, "records": 1, "bytes": 100,
                       "spec_id": 0, "data_sequence": 1, "file_sequence": 1, "deletes": []}],
                       "physical_file": {"path": file, "bytes": 100, "sha256": HASH,
                       "raw_fields": [{"id": i, "path": "f" + str(i), "repetition": "OPTIONAL", "primitive": "INT32"}
                                      for i in range(1, 9)]}, "sdk_rows": ["[1]"],
                       "expected_provider_failure_kind": "ResourceExhausted" if case == "budget" else (
                           "None" if case in l.CASES[:3] else "CorruptData")})
    return {"record": record, "namespace": namespace, "tables": tables}


class FakeJobs:
    """Copies the job owner's real receipt shape, without executing any job."""
    def __init__(self):
        self.calls = []
        self.partial = False
        self.force_initializer = False
        self.unknown_cleanup = False
        self.lose_commit_response = False
        self.initial_tokens = []
        self.partial_commit_once = False

    def publication_identity(self, _):
        return HASH

    def run(self, workspace, publication, script, directory, run_token, phase):
        self.calls.append(phase)
        directory.mkdir()
        (directory / "output").mkdir()
        state = directory.parent.parent / "lifecycle"
        own_raw = (state / "owner.json").read_bytes()
        own = l.parse(own_raw)
        self.initial_tokens.append(run_token)
        self_term = terminal(own, phase)
        self_term["script_sha256"] = l.digest(script.read_bytes())
        launch = copy.deepcopy(self_term)
        launch.update(container_id=None, execution_confirmed=False, exit_code=None, confirmed_gone=False)
        (directory / "launch.json").write_bytes(l.canonical(launch))
        failure = False
        if phase == "initialize" and (self.force_initializer or self.unknown_cleanup):
            self_term.update(execution_confirmed=False, exit_code=None, forced=True,
                             confirmed_gone=not self.unknown_cleanup)
            failure = True
            value = None
        elif phase == "initialize":
            value = full_receipt(own["namespace"])
        elif phase == "observe":
            value = full_receipt(own["namespace"], "field_domain_invalid_unchanged")
        elif phase == "prepare-cleanup":
            value = candidate(own, own_raw, (state / "initializer-terminal.json").read_bytes(), self.partial)
        else:
            self.assert_durable_ack(state, own, own_raw)
            planned_raw = (state / "cleanup-candidate.json").read_bytes()
            value = result(own, own_raw, planned_raw, l.parse(planned_raw))
            if self.partial_commit_once:
                value["tables"][0] = {"case": l.CASES[0], "state": "unresolved", "reason": "storage_error"}
                value["complete"] = False
                self.partial_commit_once = False
            if self.lose_commit_response:
                self.lose_commit_response = False
                failure = True
        (directory / "terminal.json").write_bytes(l.canonical(self_term))
        (directory / "output/stdout.log").write_bytes(
            b"" if value is None else b"UEA4G_RECEIPT " + l.canonical(value) + b"\n")
        if failure:
            raise l.job.JobFailure("Simulated exact job failure")
        return self_term

    @staticmethod
    def assert_durable_ack(state, own, own_raw):
        candidate_raw = (state / "cleanup-candidate.json").read_bytes()
        l.validate_ack(l.parse((state / "cleanup-ack.json").read_bytes()), own, own_raw, candidate_raw)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        for name in ("iceberg-delete-applicability/generate.scala", "uea7b3-field-domain-invalid/fixture.scala"):
            path = self.workspace / "tests/sql/fixtures" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("// Pure test input, not executable SDK code\n")
        self.publication = self.root / "publication"
        self.publication.mkdir()
        self.jobs = FakeJobs()
        self.controller = l.Lifecycle(self.workspace, self.publication, self.root / "reports", "ns_runner_not_uuid", self.jobs)

    def initialized(self):
        self.assertEqual(self.controller.initialize(), "FIELD_DOMAIN_INVALID_READY")

    def test_strict_json_rejects_duplicate_trailing_nonfinite_oversize_and_noncanonical(self):
        for raw in (b'{"a":1,"a":2}', b'{}{}', b'{"a":NaN}', b'{ "a":1}', b'{"a":"\\u0061"}', b'x' * (l.CAP + 1)):
            with self.subTest(raw=raw[:40]), self.assertRaises(l.LifecycleError):
                l.parse(raw)

    def test_owner_mints_uuid_independently_of_runner_namespace_and_refuses_replacement(self):
        self.initialized()
        own = l.parse(l.read(self.controller.state / "owner.json"))
        l.uuid_value(own["run_token"])
        self.assertNotEqual(own["run_token"], "runner_not_uuid")
        with self.assertRaises(l.LifecycleError):
            self.controller.initialize()
        changed = dict(own, version=True)
        with self.assertRaises(l.LifecycleError):
            l.validate_owner(changed, own["namespace"], HASH)
        with self.assertRaises(l.LifecycleError):
            l.validate_owner(own, own["namespace"], "b" * 64)

    def test_distinct_bom_reference_and_image_id_are_preserved(self):
        own = owner()
        value = terminal(own)
        l.validate_terminal(value, own, "initialize", value)
        self.assertNotEqual(value["image_id"], value["image_reference"])
        altered = dict(value, image_reference="different:tag")
        with self.assertRaises(l.LifecycleError):
            l.validate_terminal(altered, own, "initialize", value)

    def test_terminal_false_null_wrong_phase_and_bool_status_cannot_be_success(self):
        own = owner()
        for changes in ({"confirmed_gone": False}, {"container_id": None},
                        {"execution_confirmed": False, "exit_code": None}, {"exit_code": 1}):
            value = dict(terminal(own), **changes)
            l.validate_terminal(value, own, "initialize")
            with self.assertRaises(l.LifecycleError):
                l.successful(value)
        for changes in ({"phase": "observe"}, {"exit_code": True}, {"extra": 1}):
            with self.assertRaises(l.LifecycleError):
                l.validate_terminal(dict(terminal(own), **changes), own, "initialize")

    def test_forced_gone_initializer_can_prepare_partial_but_never_ready_or_cleaned(self):
        self.jobs.force_initializer = True
        self.jobs.partial = True
        with self.assertRaises(l.LifecycleError):
            self.controller.initialize()
        self.assertFalse((self.controller.directory / "initialize.json").exists())
        with self.assertRaises(l.LifecycleError):
            self.controller.cleanup()
        self.assertEqual(self.jobs.calls, ["initialize", "prepare-cleanup", "commit-cleanup"])
        results = list((self.controller.directory / "jobs").glob("commit-cleanup-*/cleanup-result.json"))
        self.assertEqual(len(results), 1)
        value = l.parse(results[0].read_bytes())
        self.assertFalse(value["complete"])
        self.assertFalse((self.controller.state / "cleanup-result.json").exists())
        self.assertEqual(value["tables"][0]["state"], "owned")
        self.assertEqual(value["tables"][1], {"case": l.CASES[1], "state": "unresolved", "reason": "unknown_create"})

    def test_unknown_container_cleanup_refuses_prepare_and_ack(self):
        self.jobs.unknown_cleanup = True
        with self.assertRaises(l.LifecycleError):
            self.controller.initialize()
        with self.assertRaises(l.LifecycleError):
            self.controller.cleanup()
        self.assertEqual(self.jobs.calls, ["initialize"])
        self.assertFalse((self.controller.state / "cleanup-ack.json").exists())

    def test_lost_terminal_mirror_recovers_exact_invocation_without_glob(self):
        self.initialized()
        (self.controller.state / "initializer-terminal.json").unlink()
        raw, value = self.controller.initializer_terminal()
        self.assertTrue(value["confirmed_gone"])
        self.assertEqual(raw, (self.controller.state / "initializer-terminal.json").read_bytes())

    def test_candidate_persistence_failure_cannot_create_ack_or_commit(self):
        self.initialized()
        original = l.persist
        def failed(path, raw, new=False):
            if Path(path).name == "cleanup-candidate.json":
                raise OSError("Injected durable write failure")
            return original(path, raw, new)
        with patch.object(l, "persist", failed), self.assertRaises(OSError):
            self.controller.cleanup()
        self.assertFalse((self.controller.state / "cleanup-ack.json").exists())
        self.assertNotIn("commit-cleanup", self.jobs.calls)

    def test_lost_commit_response_replays_same_candidate_and_ack_without_prepare(self):
        self.initialized()
        self.jobs.lose_commit_response = True
        with self.assertRaises(l.LifecycleError):
            self.controller.cleanup()
        frozen = l.read(self.controller.state / "cleanup-candidate.json")
        ack = l.read(self.controller.state / "cleanup-ack.json")
        self.assertEqual(self.controller.cleanup(), "FIELD_DOMAIN_INVALID_CLEANED")
        self.assertEqual(self.jobs.calls.count("prepare-cleanup"), 1)
        self.assertEqual(self.jobs.calls.count("commit-cleanup"), 2)
        self.assertEqual(l.read(self.controller.state / "cleanup-candidate.json"), frozen)
        self.assertEqual(l.read(self.controller.state / "cleanup-ack.json"), ack)

    def test_partial_commit_then_success_retains_each_exact_result_and_replays(self):
        self.initialized()
        self.jobs.partial_commit_once = True
        with self.assertRaises(l.LifecycleError):
            self.controller.cleanup()
        self.assertFalse((self.controller.state / "cleanup-result.json").exists())
        self.assertEqual(self.controller.cleanup(), "FIELD_DOMAIN_INVALID_CLEANED")
        self.assertEqual(self.jobs.calls.count("prepare-cleanup"), 1)
        results = list((self.controller.directory / "jobs").glob("commit-cleanup-*/cleanup-result.json"))
        self.assertEqual(len(results), 2)
        self.assertEqual(sorted(l.parse(path.read_bytes())["complete"] for path in results), [False, True])
        self.assertTrue(l.parse(l.read(self.controller.state / "cleanup-result.json"))["complete"])

    def test_initializer_directory_symlink_is_rejected_during_recovery(self):
        self.initialized()
        invocation = l.parse(l.read(self.controller.state / "initializer-invocation.json"))
        original = Path(invocation["job_directory"])
        external = self.root / "outside"
        original.rename(external)
        original.symlink_to(external, target_is_directory=True)
        with self.assertRaises(l.LifecycleError):
            self.controller.initializer_terminal()

    def test_ack_boolean_version_is_not_integer_version(self):
        own = owner()
        own_raw, raw = l.canonical(own), b'{}'
        ack = l.make_ack(own, own_raw, raw)
        ack["version"] = True
        with self.assertRaises(l.LifecycleError):
            l.validate_ack(ack, own, own_raw, raw)

    def test_candidate_closed_shape_and_exact_authority_reject_forgery(self):
        self.initialized()
        self.controller.load_owner()
        raw, _ = self.controller.initializer_terminal()
        value = candidate(self.controller.owner, self.controller.owner_raw, raw)
        for modify in (lambda v: v.update(owner_sha256="b" * 64),
                       lambda v: v["tables"][0].update(extra=True),
                       lambda v: v["tables"][0]["registered_data_files"][0].update(path="s3://foreign/file"),
                       lambda v: v["tables"][0].update(unproven_sdk_orphans=False)):
            forged = copy.deepcopy(value)
            modify(forged)
            with self.assertRaises(l.LifecycleError):
                l.validate_candidate(forged, self.controller.owner, self.controller.owner_raw, raw)

    def test_result_cannot_hide_registered_objects_or_promote_unresolved(self):
        own = owner()
        own_raw = l.canonical(own)
        planned = candidate(own, own_raw, l.canonical(terminal(own)), partial=True)
        raw = l.canonical(planned)
        for modify in (lambda v: v.update(complete=True),
                       lambda v: v["tables"][0].update(registered_data_files=[]),
                       lambda v: v["tables"][1].update(state="owned")):
            value = result(own, own_raw, raw, planned)
            modify(value)
            with self.assertRaises(l.LifecycleError):
                l.validate_result(value, own, own_raw, raw, planned)

    def test_atomic_publication_rejects_symlink_conflict_and_fsync_failure(self):
        path = self.root / "exact.json"
        l.persist(path, b'{}', new=True)
        l.persist(path, b'{}')
        with self.assertRaises(l.LifecycleError):
            l.persist(path, b'{"changed":1}')
        alias = self.root / "alias.json"
        alias.symlink_to(path)
        with self.assertRaises(l.LifecycleError):
            l.persist(alias, b'{}')
        with patch.object(l.os, "fsync", side_effect=OSError("Injected fsync refusal")), self.assertRaises(OSError):
            l.persist(self.root / "never-published.json", b'{}')
        self.assertFalse((self.root / "never-published.json").exists())

    def test_observe_requires_original_complete_receipt_and_preserves_bag(self):
        self.initialized()
        self.assertEqual(self.controller.observe(), "FIELD_DOMAIN_INVALID_UNCHANGED")
        initial = l.parse((self.controller.directory / "initialize.json").read_bytes())
        observed = l.parse((self.controller.directory / "observe.json").read_bytes())
        self.assertEqual(initial["tables"], observed["tables"])
        (self.controller.directory / "initialize.json").unlink()
        with self.assertRaises(l.LifecycleError):
            self.controller.observe()


if __name__ == "__main__":
    unittest.main()
