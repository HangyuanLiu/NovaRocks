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
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Independent admission fixtures exercise membership, batches and failed inventory."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.admission import Enablement, compose_attempt, load_enablement
from verification.compose import compose
from verification.materials import read_invocation
from verification.model import Attempt, Observation, Scope, UnitKey
from verification import run

SOURCE = {"commit": "a" * 40, "dirty": False}
IDENTITY = {"platform": {"system": "Linux", "machine": "x86_64"}, "build": "actual"}
AGGREGATE = "stage:workspace nextest and doctests"


def document():
    return {"schema": 1, "enabled": {"stage": ["workspace nextest and doctests"], "rust-test": "all", "rust-doctest": "all", "sql-suite": [], "system-scenario": []},
            "pending": [], "enumeration_gaps": []}


def scope(key, **kwargs):
    return Scope(key, (UnitKey.parse(key),), IDENTITY, "immutable", **kwargs)


def observation(key, result="pass", **kwargs):
    values = dict(unit=key, scope=key, attempt="current", lifecycle="L", identity=IDENTITY,
                  result=result, identity_confirmed=True, record_complete=True, terminated=True, cleanup="ok", ref=key, detail={"batch_root": "/actual/rust"})
    values.update(kwargs)
    return Observation(**values)


def batch():
    return {"schema": 1, "complete": True, "cancelled": [], "exit_code": 1,
            "stages": {name: {"returncode": 100 if name == "nextest" else 0, "errors": []}
                       for name in ("prepare", "error_manifest", "nextest", "doctest", "final_prepare")}}


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n")
    return path


class Admission(unittest.TestCase):
    def test_enablement_rejects_invalid_structures(self):
        values = []
        for key, value in (("schema", 2), ("enabled", {"unknown": []}), ("pending", [{"selector": {"kind": "stage"}}])):
            item = document(); item[key] = value; values.append(item)
        for kind in ("stage", "sql-suite", "system-scenario"):
            item = document(); item["enabled"][kind] = "all"; values.append(item)
        item = document(); item["pending"] = [{"selector": {"kind": "rust-test"}, "owner": "team", "enable_when": "fixed"}]; values.append(item)
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError): Enablement(value)

    def test_enablement_names_and_closed_gap_require_native_declarations(self):
        declarations = [scope(AGGREGATE), scope("rust-test:p::a"), scope("rust-doctest:workspace")]
        Enablement(document()).validate(declarations)
        unknown = document(); unknown["enabled"]["stage"].append("missing")
        with self.assertRaisesRegex(ValueError, "unknown"): Enablement(unknown).validate(declarations)
        closed = document(); closed["enumeration_gaps"] = [{"kind": "rust-test", "name": "p::a", "reason": "binding", "closes_with": "TST-G5"}]
        with self.assertRaisesRegex(ValueError, "now natively"): Enablement(closed).validate(declarations)
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(load_enablement(Path(directory) / "missing.json"))

    def test_not_admitted_observations_remain_visible_and_cleanup_blocks(self):
        key = "system-scenario:pending"
        pending = document(); pending["pending"] = [{"selector": {"kind": "system-scenario"}, "owner": "system", "enable_when": "baseline"}]
        for cleanup in ("ok", "unknown"):
            attempt = Attempt("current", [observation(key, "fail", cleanup=cleanup)], current=True, formal=True, source=SOURCE)
            value = compose({}, "admission", [attempt], [scope(key)], enablement=Enablement(pending))
            self.assertEqual(value["scopes"][0]["verdict"], "not_admitted")
            self.assertEqual(value["admission_gaps"]["not_admitted"], [{"key": key, "reason": "pending"}])
            self.assertEqual(value["scopes"][0]["observations"][0]["result"], "fail")
            self.assertEqual(value["layer_ok"], cleanup == "ok")
        no_plan = compose({}, "admission", [attempt], [scope(key)], enablement=Enablement(document()))
        self.assertEqual(no_plan["scopes"][0]["reason"], "no_plan")
        default = compose({}, "admission", [], [scope(key)])
        self.assertFalse(default["layer_ok"])

    def fixture(self, failed=True):
        declarations = [scope(AGGREGATE), scope("rust-test:p::a"), scope("rust-doctest:workspace")]
        observations = [observation(AGGREGATE, "fail", detail={"batch_root": "/actual/rust", "batch_index": batch(), "invocation": {"exit": {"code": 1}}}),
                        observation("rust-test:p::a", "fail" if failed else "pass"), observation("rust-doctest:workspace")]
        attempt = Attempt("current", observations, current=True, formal=True, source=SOURCE)
        quarantine = {"schema": 1, "known_failures": [], "quarantine": [{"unit": "rust-test:p::a", "reason": "flaky", "owner": "rust", "exit_condition": "fixed", "review_by": "2999-01-01"}]}
        return declarations, attempt, quarantine

    def test_member_quarantine_derives_aggregate_and_preserves_native_failure(self):
        declarations, attempt, disposition = self.fixture()
        before = copy.deepcopy(attempt)
        value = compose({}, "admission", [attempt], declarations, disposition, enablement=Enablement(document()))
        self.assertTrue(value["layer_ok"])
        aggregate = next(item for item in value["scopes"] if item["key"] == AGGREGATE)
        self.assertEqual(aggregate["verdict"], "satisfied")
        self.assertEqual(aggregate["native_exit"][0]["code"], 1)
        self.assertEqual(aggregate["observations"][0]["result"], "fail")
        self.assertEqual(attempt, before)
        self.assertFalse(compose({}, "admission", [attempt], declarations)["layer_ok"])

    def test_aggregate_requires_own_confirmed_batch_and_current_member_provenance(self):
        declarations, attempt, disposition = self.fixture(False)
        attempt.observations[0].identity = {"other": "identity"}
        value = compose({}, "full", [attempt], declarations)
        aggregate = next(item for item in value["scopes"] if item["key"] == AGGREGATE)
        self.assertFalse(value["layer_ok"])
        self.assertEqual(aggregate["verdict"], "not_run")
        declarations, attempt, _ = self.fixture(False)
        member = attempt.observations.pop(1)
        member.attempt = "historical"
        historical = Attempt("historical", [member], formal=True, source=SOURCE)
        value = compose({}, "admission", [attempt, historical], declarations)
        self.assertFalse(value["layer_ok"])
        member.attempt = "current"
        member.detail = {"batch_root": "/other-targeted/rust"}
        attempt.observations.append(member)
        value = compose({}, "admission", [attempt], declarations)
        self.assertFalse(value["layer_ok"])

    def test_aggregate_quarantine_keeps_excluded_verdict(self):
        declarations, attempt, disposition = self.fixture(False)
        disposition["quarantine"][0]["unit"] = AGGREGATE
        attempt.observations.pop(0)
        value = compose({}, "admission", [attempt], declarations, disposition)
        self.assertEqual(next(item for item in value["scopes"] if item["key"] == AGGREGATE)["verdict"], "excluded")
        self.assertTrue(value["layer_ok"])

    def test_environment_cancellation_and_missing_batch_never_satisfy_aggregate(self):
        declarations, attempt, disposition = self.fixture(False)
        changes = [("nextest", 101), ("final_prepare", 1), ("doctest", 101)]
        for native, code in changes:
            current = copy.deepcopy(attempt); current.observations[0].detail["batch_index"]["stages"][native]["returncode"] = code
            value = compose({}, "admission", [current], declarations, disposition)
            self.assertEqual(next(item for item in value["scopes"] if item["key"] == AGGREGATE)["verdict"], "not_run")
        for field, value in (("cancelled", [15]), ("error", "report error"), ("complete", False)):
            current = copy.deepcopy(attempt); current.observations[0].detail["batch_index"][field] = value
            self.assertFalse(compose({}, "admission", [current], declarations, disposition)["layer_ok"])
        attempt.observations[0].detail = {}
        self.assertFalse(compose({}, "admission", [attempt], declarations, disposition)["layer_ok"])

    def test_full_includes_heavy_members_and_ignores_admission_quarantine(self):
        declarations, attempt, disposition = self.fixture(False)
        declarations.append(scope("rust-test:p::heavy", tier="heavy"))
        attempt.observations.append(observation("rust-test:p::heavy", "fail"))
        value = compose({}, "full", [attempt], declarations, disposition)
        self.assertEqual(next(item for item in value["scopes"] if item["key"] == AGGREGATE)["verdict"], "failed")
        attempt.observations[-1].result = "pass"
        attempt.observations[-1].detail = {"batch_root": "/actual/rust-heavy-1"}
        self.assertTrue(compose({}, "full", [attempt], declarations, disposition)["layer_ok"])
        declarations, attempt, disposition = self.fixture()
        self.assertFalse(compose({}, "full", [attempt], declarations, disposition)["layer_ok"])

    def test_development_aggregate_remains_deferred(self):
        declarations, attempt, _ = self.fixture(False)
        declarations[0].deferred = {"reason": "native full batch", "backstop": "admission"}
        attempt.observations = attempt.observations[1:]
        value = compose({}, "dev", [attempt], declarations)
        self.assertEqual(next(item for item in value["scopes"] if item["key"] == AGGREGATE)["verdict"], "deferred")

    def test_internal_stage_exports_keep_aggregate_identity_and_absence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            put(root / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [], "quarantine": []})
            index = batch(); index["stages"].pop("final_prepare")
            path = put(root / "invocations/0001.json", {"schema": 1, "kind": "stage", "seq": 1, "units": [AGGREGATE], "exit": {"code": 1}, "ended_at": "now",
                "inputs_start": IDENTITY, "inputs_end": IDENTITY, "inputs_stable": True, "cleanup": {"summary": "ok"},
                "materials": {"execution_index": str(put(root / "rust/execution-index.json", index))}})
            observations, errors = read_invocation(path, "current")
            self.assertEqual(errors, [])
            by_key = {obs.scope: obs for obs in observations}
            self.assertEqual(by_key["stage:joint tests and binaries preparation"].result, "pass")
            self.assertEqual(by_key["stage:SQL error manifest freshness"].identity, IDENTITY)
            self.assertEqual(by_key["stage:main binary build information"].result, "not_run")
            self.assertEqual(by_key[AGGREGATE].detail["batch_index"], index)

    def test_current_only_formal_and_rehearsal_target_confirmed_identity_after_relink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            put(root / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [], "quarantine": []})
            key = "stage:guard"
            attempt = Attempt("current", [observation(key)], current=True, source=SOURCE)
            put(root / "attempt/identity/source-end.json", {"schema": 1, "source": SOURCE})
            original = put(root / "attempt/native.json", {"unchanged": "raw"})
            digest = hashlib.sha256(original.read_bytes()).hexdigest()
            declaration = scope(key); declaration.identity = {"build": "relinked"}
            for evidence_class in ("formal-admission", "local-rehearsal", "diagnostic"):
                with mock.patch.object(run, "owned_attempt", return_value=copy.deepcopy(attempt)):
                    value = compose_attempt(root, root / "attempt", root / "missing.json", {"head_sha": "a" * 40}, root / evidence_class,
                                            evidence_class, declarations=[declaration])
                self.assertTrue(value["layer_ok"])
                self.assertEqual(value["evidence_class"], evidence_class)
                self.assertEqual(value["attempts"][0]["formal"], evidence_class == "formal-admission")
                self.assertEqual("formal_run" in value, evidence_class == "formal-admission")
            self.assertEqual(hashlib.sha256(original.read_bytes()).hexdigest(), digest)
            attempt.observations[0].identity_confirmed = False
            with mock.patch.object(run, "owned_attempt", return_value=attempt):
                value = compose_attempt(root, root / "attempt", root / "missing.json", {}, root / "unconfirmed", declarations=[declaration])
            self.assertFalse(value["layer_ok"])

    def test_formal_rejects_dirty_short_and_mismatched_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            put(root / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [], "quarantine": []})
            for source in ({"dirty": True, "commit": "a" * 40}, {"dirty": False, "commit": "a123"}, {"dirty": False, "commit": "b" * 40}):
                with mock.patch.object(run, "owned_attempt", return_value=Attempt("current", source=source)):
                    with self.assertRaisesRegex(ValueError, "exact full head"):
                        compose_attempt(root, root / "attempt", root / "missing", {"head_sha": "a" * 40}, root / "out", "formal-admission", declarations=[])

    def test_formal_requires_final_source_and_stable_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = Attempt("current", current=True, source=SOURCE)
            with mock.patch.object(run, "owned_attempt", return_value=attempt):
                with self.assertRaisesRegex(ValueError, "final source"):
                    compose_attempt(root, root / "attempt", root / "missing", {"head_sha": "a" * 40}, root / "out", "formal-admission", declarations=[])
                put(root / "attempt/identity/source-end.json", {"schema": 1, "source": dict(SOURCE, dirty=True)})
                with self.assertRaisesRegex(ValueError, "source changed"):
                    compose_attempt(root, root / "attempt", root / "missing", {"head_sha": "a" * 40}, root / "out", "formal-admission", declarations=[])

    def test_failed_native_before_batch_reports_without_building_and_retains_raw_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            put(root / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [], "quarantine": []})
            key = "stage:Cargo dependency policy"
            path = put(root / "attempt/invocations/0001.json", {"schema": 1, "kind": "stage", "seq": 1,
                "units": [key], "exit": {"code": 1}, "ended_at": "now", "inputs_start": IDENTITY,
                "inputs_end": IDENTITY, "inputs_stable": True, "cleanup": {"summary": "ok"}})
            raw = path.read_bytes()
            with mock.patch.object(run, "prepare", side_effect=AssertionError("failure reporting must not build")) as preparation, mock.patch.object(run, "json_command", return_value={"stages": [{"name": "Cargo dependency policy", "tier": "admission"}]}):
                value = compose_attempt(root, root / "attempt", root / "missing", {}, root / "output", native_exit=1)
            preparation.assert_not_called()
            self.assertFalse(value["layer_ok"])
            self.assertEqual(value["enumeration_errors"], ["native Rust batch was not reached"])
            self.assertEqual({item["kind"] for item in value["admission_gaps"]["unenumerated"]},
                             {"rust-test", "rust-doctest", "sql-suite", "system-scenario"})
            self.assertEqual(value["scopes"][0]["verdict"], "failed")
            self.assertEqual(value["scopes"][0]["observations"][0]["result"], "fail")
            self.assertEqual(path.read_bytes(), raw)
            self.assertTrue((root / "output/composition.json").is_file())

    def test_failed_preparation_and_runner_listing_still_save_blocking_native_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            put(root / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [], "quarantine": []})
            key = "stage:guard"
            attempt = Attempt("current", [observation(key)], current=True, source=SOURCE)
            for step in ("prepare", "inventory", "scopes"):
                with mock.patch.object(run, "prepare", return_value=root / "fresh") as preparation, mock.patch.object(run, "inventory", return_value={}) as inventory, mock.patch.object(run, "scopes", return_value=[]) as scopes, mock.patch.object(run, "json_command", return_value={"stages": [{"name": "guard", "tier": "admission"}]}), mock.patch.object(run, "owned_attempt", return_value=copy.deepcopy(attempt)):
                    {"prepare": preparation, "inventory": inventory, "scopes": scopes}[step].side_effect = subprocess.CalledProcessError(1, [step])
                    value = compose_attempt(root, root / "attempt", root / "missing", {}, root / step)
                self.assertFalse(value["layer_ok"])
                self.assertEqual(len(value["admission_gaps"]["unenumerated"]), 4)
                self.assertEqual(value["scopes"][0]["observations"][0]["result"], "pass")
                self.assertTrue((root / step / "composition.json").is_file())


if __name__ == "__main__":
    unittest.main()
