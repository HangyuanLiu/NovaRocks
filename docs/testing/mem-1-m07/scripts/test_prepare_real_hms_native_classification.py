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

"""Host-only negative and hook tests for the ignored optional companion draft.

These tests must not start Git, the Native runner, providers, Docker or HTTP. One
separate capture test starts only a tiny direct Python child. They were
written, not executed, by the audit agent. Synthetic facts prove only these
controller boundaries, never native classification or physical role exit.
"""
import importlib.util
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

SOURCE = Path(__file__).with_name("prepare_real_hms_native_classification.py")
spec = importlib.util.spec_from_file_location("hms_native_companion", SOURCE)
companion = importlib.util.module_from_spec(spec)
spec.loader.exec_module(companion)

class Refusal(Exception):
    pass

class CaptureFailure(Refusal):
    output = b"partial-native-diagnostic"
    def safe_facts(self):
        return {"capture_completed":False, "group_exit_confirmed":True, "leader_reaped":True}

class Base:
    Refusal = Refusal
    CaptureFailure = CaptureFailure
    @staticmethod
    def need(value, message):
        if not value:
            raise Refusal(message)
    @staticmethod
    def exact_keys(value, keys):
        Base.need(isinstance(value, dict) and set(value) == set(keys), "keys differ")
    @staticmethod
    def sha(value):
        import hashlib
        return hashlib.sha256(value).hexdigest()
    @staticmethod
    def remaining(deadline):
        return 1
    @staticmethod
    def one(records, kind):
        return records[kind]
    @staticmethod
    def name_set(value, bound):
        return set(value)
    atomic_json = mock.Mock()
    decode_json = staticmethod(json.loads)


def frozen():
    return {"source_revision":"a"*40, "source_tree_sha256":"b"*64, "runner_native_build_identity":"actual-build",
        "runner_path":"/private/runner", "server_path":"/private/server", "cargo_lock_sha256":"c"*64}

def evidence(code=0):
    return {"schema_version":5, "scenario":companion.SCENARIO, "outcome":"passed" if code == 0 else "failed",
        "exit_code":code, "source_dirty":False, **frozen(), "runner_executable":"/private/runner",
        "primary_binary":"/private/server", "base_config_sha256":"d"*64, "cluster_size":3,
        "launch_profile":"fault-scenario", "process_launch_identities":[
            {"role":role,"pid":100+index,"process_start_token":"actual-token-"+str(index)}
            for index,role in enumerate(("fe","be-0","be-1","be-2"))]}

class Owner:
    def __init__(self, root):
        self.root = root
        self.work_deadline = 200
        self.wall_deadline = 440
        self.bounds = {"max_output_bytes":1048576,"host_reap_seconds":5,"max_local_file_bytes":1048576}
        self.children = []
        self.env = {}
        self.status = {"commands":[],"resource_retained_whole_failure":False,"retention_barriers":[]}

class SettlementTests(unittest.TestCase):
    def test_only_fixed_schema5_normal_returns_can_settle(self):
        with mock.patch.object(companion,"independently_absent",return_value=True):
            for code in (0,1):
                self.assertEqual(len(companion.validate_settled_evidence(Base,evidence(code),code,frozen(),"d"*64)),4)
            wrong = evidence();wrong["schema_version"] = 1
            with self.assertRaises(Refusal):
                companion.validate_settled_evidence(Base,wrong,0,frozen(),"d"*64)

    def test_partial_duplicate_foreign_and_missing_role_ledgers_fail(self):
        variants = []
        partial = evidence();partial["process_launch_identities"].pop();variants.append(partial)
        duplicate = evidence();duplicate["process_launch_identities"][3] = duplicate["process_launch_identities"][2].copy();variants.append(duplicate)
        foreign = evidence();foreign["process_launch_identities"][3]["role"] = "other";variants.append(foreign)
        missing = evidence();del missing["process_launch_identities"];variants.append(missing)
        with mock.patch.object(companion,"independently_absent",return_value=True):
            for value in variants:
                with self.assertRaises(Refusal):
                    companion.validate_settled_evidence(Base,value,0,frozen(),"d"*64)

    def test_present_or_reused_pid_never_grants_settlement(self):
        with mock.patch.object(companion,"independently_absent",return_value=False):
            with self.assertRaises(Refusal):
                companion.validate_settled_evidence(Base,evidence(),0,frozen(),"d"*64)

    def test_only_esrch_is_pid_absence_no_signals_other_than_zero(self):
        import errno
        for error,wanted in ((OSError(errno.ESRCH,"gone"),True),(OSError(errno.EPERM,"unknown"),False)):
            with mock.patch.object(companion.os,"kill",side_effect=error) as kill:
                self.assertEqual(companion.independently_absent(123),wanted)
                kill.assert_called_once_with(123,0)
        with mock.patch.object(companion.os,"kill",return_value=None):
            self.assertFalse(companion.independently_absent(123))

    def test_source_build_and_failed_outcome_mismatch_are_not_passes(self):
        for field,value in (("source_dirty",True),("source_revision","f"*40),
                ("runner_native_build_identity","other"),("outcome","failed")):
            wrong = evidence();wrong[field] = value
            with mock.patch.object(companion,"independently_absent",return_value=True):
                with self.assertRaises(Refusal):
                    companion.validate_settled_evidence(Base,wrong,0,frozen(),"d"*64)

    def test_abnormal_capture_sticky_even_when_runner_group_gone(self):
        with tempfile.TemporaryDirectory() as root:
            owner = Owner(Path(root))
            base = types.SimpleNamespace(**{name:getattr(Base,name) for name in
                ("need","remaining","atomic_json","sha","CaptureFailure")},
                capture=mock.Mock(side_effect=CaptureFailure("timeout")))
            with mock.patch.object(companion.time,"monotonic",return_value=100):
                with self.assertRaises(CaptureFailure):
                    companion.native_runner_step(base,owner,frozen(),{},Path(root)/"config",Path(root)/"artifacts")
            self.assertTrue(owner.status["resource_retained_whole_failure"])
            self.assertTrue(owner.status["native_started"])
            self.assertEqual(owner.status["commands"][-1]["output_bytes"],len(CaptureFailure.output))
            owner.children.clear()
            self.assertTrue(owner.status["resource_retained_whole_failure"])

    def test_native_grant_consumes_original_absolute_deadline_and_reserves_exit(self):
        with tempfile.TemporaryDirectory() as root:
            owner = Owner(Path(root))
            capture = mock.Mock(return_value=(0,b"diagnostic",{"capture_completed":True,
                "leader_reaped":True,"group_exit_confirmed":True,"resource_retained_whole_failure":False}))
            writes = []
            base = types.SimpleNamespace(**{name:getattr(Base,name) for name in
                ("need","remaining","sha","CaptureFailure","exact_keys","decode_json")}, capture=capture,
                atomic_json=lambda path,value:writes.append((path,value)),
                bounded_read=lambda path,cap:json.dumps(evidence()).encode())
            with mock.patch.object(companion.time,"monotonic",return_value=100), \
                    mock.patch.object(companion,"independently_absent",return_value=True):
                result = companion.native_runner_step(base,owner,frozen(),{"base_config_sha256":"d"*64},
                    Path(root)/"config",Path(root)/"artifacts")
            self.assertTrue(result["classification_passed"])
            self.assertEqual(capture.call_args.args[2],200)
            self.assertEqual(writes[0][1]["assertion_budget_millis"],76000)
            self.assertEqual(owner.work_deadline,200)

class ParentPreflight:
    def __init__(self, freeze, output):
        self.freeze = freeze;self.root = output;self.bounds = {"max_namespaces":128};self.events = []
        self.status = {"scope":"old","capability":{},"resource_retained_whole_failure":False}
    def stage(self, phase):
        self.events.append(phase)
        return {"baseline":[],"table":{},"view":{}}
    def run_stages(self):
        for phase in ("create","oracle","drop","restored"):
            self.stage(phase)
        self.status["capability"] = {"native_mixed_object_classification":"OPEN"}

class HookTests(unittest.TestCase):
    def owner(self, passed=True, unknown=False):
        base = types.SimpleNamespace(Preflight=ParentPreflight,need=Base.need,
            one=Base.one,name_set=Base.name_set)
        native = companion.make_native_preflight_class(base,{})
        class Owner(native):
            def run_native(self):
                self.events.append("native")
                if unknown:
                    self.status["resource_retained_whole_failure"] = True
                    raise Refusal("unknown native owner")
                return {"classification_passed":passed}
        return Owner({},Path("/unused"))

    def test_hook_runs_after_original_oracle_before_original_drop(self):
        owner = self.owner()
        with mock.patch.object(companion,"fresh_java_oracle",side_effect=lambda *args:owner.events.append("fresh-oracle")):
            owner.run_stages()
        self.assertEqual(owner.events,["create","oracle","native","fresh-oracle","drop","restored"])

    def test_normal_failed_native_still_restores_but_never_passes(self):
        owner = self.owner(passed=False)
        with mock.patch.object(companion,"fresh_java_oracle",side_effect=lambda *args:owner.events.append("fresh-oracle")):
            with self.assertRaises(Refusal):
                owner.run_stages()
        self.assertEqual(owner.events,["create","oracle","native","fresh-oracle","drop","restored"])
        self.assertFalse(owner.status["capability"]["native_mixed_object_classification"]["classification_passed"])

    def test_unknown_native_owner_stops_java_drop_and_restored(self):
        owner = self.owner(unknown=True)
        with mock.patch.object(companion,"fresh_java_oracle") as fresh:
            with self.assertRaises(Refusal):
                owner.run_stages()
            fresh.assert_not_called()
        self.assertEqual(owner.events,["create","oracle","native"])
        self.assertTrue(owner.status["resource_retained_whole_failure"])

class ActualHostCaptureTests(unittest.TestCase):
    def test_existing_verifier_wrapper_accepts_normal_code1_without_detached_role_proof(self):
        import sys, time
        repo = next(parent for parent in SOURCE.parents if (parent / "Cargo.lock").is_file())
        path = repo / companion.BASE_RELATIVE
        source = companion.bounded_file(path)
        specification = importlib.util.spec_from_file_location("actual_stock_capture", path)
        base = importlib.util.module_from_spec(specification)
        exec(compile(source, str(path), "exec"), base.__dict__)
        children = []
        code, output, facts = base.capture([sys.executable, "-c", "raise SystemExit(1)"],
            {}, time.monotonic()+3, 1024, owned_children=children,
            wall_deadline=time.monotonic()+8, reap_seconds=5, ownership="verifier-wrapper")
        self.assertEqual(code,1)
        self.assertEqual(output,b"")
        self.assertTrue(facts["capture_completed"])
        self.assertTrue(facts["leader_reaped"])
        self.assertTrue(facts["group_exit_confirmed"])
        self.assertEqual(facts["descendant_exit_basis"],"owned-host-group-only")
        self.assertIsNone(facts["detached_child_exit_confirmed"])
        self.assertFalse(facts["resource_retained_whole_failure"])
        self.assertEqual(children,[])
        # This is actual direct-host-group evidence only, never a Native role ledger.


class InputTests(unittest.TestCase):
    def test_optional_cli_binding_cannot_be_repeated(self):
        import contextlib, io
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                companion.parse_arguments(["--freeze", "/base.json", "--output", "/out", "--native-freeze", "/one.json", "--native-freeze", "/two.json"])

    def test_duplicate_json_keys_are_rejected(self):
        with self.assertRaises(ValueError):
            companion.decode_closed(b'{"source":1,"source":2}')

    def test_raw_secret_cannot_replace_exact_declared_role_reference(self):
        good = {"purpose":"object-store-metadata","name":"iceberg-test-data","generation":"v1","kind":"s3",
            "access_key_id":"${ENV:AWS_S3_ACCESS_KEY_ID}","access_key_secret":"${ENV:AWS_S3_SECRET_ACCESS_KEY}"}
        self.assertEqual(companion.credential(Base,{"connector":{"credentials":[good]}},"object-store-metadata"),good)
        bad = {**good,"access_key_secret":"raw-secret"}
        with self.assertRaises(Refusal):
            companion.credential(Base,{"connector":{"credentials":[bad]}},"object-store-metadata")

if __name__ == "__main__":
    unittest.main()
