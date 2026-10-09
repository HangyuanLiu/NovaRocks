# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""Pure tests. All PID probes are mocked; no process is inspected."""
import dataclasses
import errno
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

MODULE_PATH = Path(__file__).with_name("verify_exact_native_final.py")
spec = importlib.util.spec_from_file_location("exact_native_final_draft", MODULE_PATH)
v = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = v
spec.loader.exec_module(v)


def encoded(value):
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def fixture():
    # Pure model inputs. No source, binary, or Native acceptance is claimed.
    source = v.SourceSnapshot("a" * 40, "", b"100644 pinned\tfile.rs\n", b"", b"")
    base = b"[server]\nhost='127.0.0.1'\n"
    effective = {"schema_version": 1, "semantics": {"cluster_size": 3, "coefficient": 0.5}}
    raw_effective = json.dumps(effective, indent=2).encode("utf-8")
    provenance = {
        "clean_revision": source.revision, "source_tree_sha256": source.clean_tree_sha256(),
        "server_binary_sha256": "b" * 64, "server_build_identity": "native-model-source",
        "runner_binary_sha256": "c" * 64, "base_config_sha256": digest(base),
        "frozen_execution_binding_sha256": "d" * 64,
        "large_input_sha256": "e" * 64, "tiny_input_sha256": "f" * 64,
    }
    roles = tuple(v.LaunchIdentity(role, 30001 + index, f"macos:100:{index}")
                  for index, role in enumerate(v.ROLES))
    expected = v.AdmittedExpected(
        case="exact-native-resident-cut-1", provenance=provenance,
        server_build_commit=source.revision, runner_build_commit=source.revision,
        runner_build_identity="native-model-runner", cargo_lock_sha256="1" * 64,
        effective_launch_config_sha256=digest(raw_effective), roles=roles,
        server_path="/model/novarocks", runner_path="/model/novarocks-system-tests",
        base_config_path="/model/base.toml", runtime_dir="/model/runtime",
        evidence_path="/model/artifacts/exact-native-resident-cut-1/scenario-evidence.json",
        sql_sha256="2" * 64, row_sha256="3" * 64, prefix_sha256="4" * 64,
        cut_row_wire_bytes=1,
    )
    schema5 = {
        "schema_version": 5, "scenario": expected.case, "outcome": "passed", "exit_code": 0,
        "started_unix_millis": 1, "ended_unix_millis": 2,
        "command": [expected.runner_path, "--only", expected.case],
        "source_revision": source.revision, "source_dirty": False,
        "source_tree_sha256": provenance["source_tree_sha256"],
        "runner_native_build_identity": expected.runner_build_identity,
        "runner_executable": expected.runner_path, "cargo_lock_sha256": expected.cargo_lock_sha256,
        "platform": {"os": "macos", "architecture": "aarch64", "logical_cpu_count": 8},
        "actions": ["scenario assertions passed", "cluster and fixture cleanup passed"],
        "phase_observations": [], "runtime_dir": expected.runtime_dir,
        "primary_binary": expected.server_path, "base_config_path": expected.base_config_path,
        "base_config_sha256": digest(base), "cluster_size": 3, "launch_profile": "fault-scenario",
        "process_launch_identities": [role.json() for role in roles],
        "effective_launch_config_sha256": digest(raw_effective),
        "effective_launch_config_semantics_sha256": digest(raw_effective),
        "effective_launch_config": effective, "diagnostics": None,
    }
    observation = {
        "schema_version": 1, "case": expected.case, "native_acceptance": False,
        "operation_before_receipt_pass": True, "receipt_settlement": v.RECEIPT_SETTLEMENT,
        "all_four_role_exit": v.ROLE_SETTLEMENT, "admitted_provenance": provenance.copy(),
        "original_launch_identities": {"frontend": roles[0].json(),
                                       "backends": [role.json() for role in roles[1:]]},
        "exact_sql_sha256": expected.sql_sha256, "cut_row_wire_bytes": expected.cut_row_wire_bytes,
        "expected_row_sha256": expected.row_sha256, "expected_prefix_sha256": expected.prefix_sha256,
        "controls": ["ActualArm"] + [None] * 15, "control_prefixes": ["Prefix"] + [None] * 15,
        "final_control_prefixes": "FinalPrefix", "held_roots": [{}, {}, {}],
        "independent_root": "FullRootFiniteFacts", "raw_original_binding": "FullRawTokenFiniteFacts",
        "reader": "ExactReadSnapshotFiniteFacts", "kill_started_us": 1, "kill_returned_us": 2,
        "resumed_us": 3, "timing_origin": v.TIMING_ORIGIN,
        "recovery_before_tasks_created": [0, 0, 0], "recovery_after_tasks_created": [1, 1, 0],
        "errors": [None] * 5, "scope": v.SCOPE,
        "effective_launch_config_sha256": digest(raw_effective),
        "effective_launch_config_semantics_sha256": digest(raw_effective),
        "effective_launch_config_bytes": len(raw_effective),
        "original_prelaunch_config_artifact": "exact-native-effective-launch-config.json",
    }
    actual = v.ActualMaterials(
        source=source, server_binary_sha256=provenance["server_binary_sha256"],
        runner_binary_sha256=provenance["runner_binary_sha256"], server_build_commit=source.revision,
        server_build_identity=provenance["server_build_identity"], server_exact_mysql_write=True,
        runner_build_commit=source.revision, runner_build_identity=expected.runner_build_identity,
        cargo_lock_sha256=expected.cargo_lock_sha256, base_config_bytes=base,
        effective_launch_config_bytes=raw_effective, runner_settled=True, runner_returncode=0,
        runner_log=f"scenario={expected.case} PASS evidence={expected.evidence_path}\n".encode(),
        schema5_bytes=encoded(schema5), observation_bytes=encoded(observation),
    )
    return expected, actual, schema5, observation


class Probe:
    def __init__(self, errno_by_pid=None):
        self.calls = []
        self.errno_by_pid = errno_by_pid or {}

    def __call__(self, pid, signal):
        self.calls.append((pid, signal))
        code = self.errno_by_pid.get(pid, errno.ESRCH)
        if code is None:
            return None
        raise OSError(code, "SOURCE_CANARY_MUST_NOT_APPEAR")


class FinalVerifierTests(unittest.TestCase):
    def refuse(self, expected, actual, code=None, probe=None):
        probe = probe or Probe()
        with self.assertRaises(v.VerificationFailure) as caught:
            v.verify_final(expected, actual, kill=probe)
        if code is not None:
            self.assertEqual(caught.exception.code, code)
        self.assertNotIn("SOURCE_CANARY", str(caught.exception))
        self.assertNotIn("SOURCE_CANARY", repr(caught.exception))
        return probe

    def test_model_pass_requires_original_wait_and_four_mocked_esrch(self):
        expected, actual, _, observation = fixture()
        self.assertIs(observation["native_acceptance"], False)
        probe = Probe()
        result = v.verify_final(expected, actual, kill=probe)
        self.assertIs(result["native_acceptance"], True)
        self.assertEqual(probe.calls, [(role.pid, 0) for role in expected.roles])
        self.assertEqual(len(result["original_role_absence_after_runner_settled"]), 4)
        self.assertNotIn("effective_launch_config", result)
        self.assertNotIn("base_config", result)

    def test_unsettled_unknown_nonzero_and_bool_exit_never_probe_pids(self):
        expected, actual, _, _ = fixture()
        for changes, code in (({"runner_settled": False}, "runner_unsettled"),
                              ({"runner_returncode": None}, "runner_exit_unknown"),
                              ({"runner_returncode": False}, "runner_exit_unknown"),
                              ({"runner_returncode": 1}, "runner_exit_failed"),
                              ({"runner_returncode": -9}, "runner_exit_failed")):
            with self.subTest(changes=changes):
                self.assertEqual(self.refuse(expected, dataclasses.replace(actual, **changes), code).calls, [])

    def test_schema5_failed_zero_unknown_and_precleanup_do_not_pass(self):
        expected, actual, evidence, _ = fixture()
        for changes in ({"outcome": "failed", "exit_code": 0}, {"outcome": "Passed"},
                        {"exit_code": None}, {"exit_code": False},
                        {"actions": ["scenario assertions passed"]}, {"diagnostics": "SOURCE_CANARY"}):
            with self.subTest(fields=tuple(changes)):
                raw = encoded({**evidence, **changes})
                self.assertEqual(self.refuse(expected, dataclasses.replace(actual, schema5_bytes=raw)).calls, [])

    def test_operation_false_unknown_errors_or_premature_native_true_refuse(self):
        expected, actual, _, observation = fixture()
        for changes in ({"operation_before_receipt_pass": False},
                        {"operation_before_receipt_pass": None}, {"native_acceptance": True},
                        {"errors": [None, {"class": "SOURCE_CANARY"}, None, None, None]},
                        {"errors": [None] * 4}):
            with self.subTest(fields=tuple(changes)):
                self.assertEqual(self.refuse(expected, dataclasses.replace(
                    actual, observation_bytes=encoded({**observation, **changes}))).calls, [])

    def test_source_dirty_staged_or_wrong_full_revision_refuse(self):
        expected, actual, _, _ = fixture()
        for source in (dataclasses.replace(actual.source, status=" M source.rs"),
                       dataclasses.replace(actual.source, diff=b"diff raw"),
                       dataclasses.replace(actual.source, staged=b"staged raw"),
                       dataclasses.replace(actual.source, revision="a" * 9),
                       dataclasses.replace(actual.source, tracked=b"different actual index\n")):
            with self.subTest(source_type=type(source).__name__):
                self.assertEqual(self.refuse(expected, dataclasses.replace(actual, source=source)).calls, [])

    def test_actual_build_binary_config_and_feature_mismatch_refuse(self):
        expected, actual, _, _ = fixture()
        for changes in ({"server_binary_sha256": "0" * 64}, {"runner_binary_sha256": "0" * 64},
                        {"server_build_commit": "0" * 40}, {"runner_build_commit": "0" * 40},
                        {"server_build_identity": "different"}, {"runner_build_identity": "different"},
                        {"server_exact_mysql_write": False}, {"cargo_lock_sha256": "0" * 64},
                        {"base_config_bytes": b"SOURCE_CANARY"}):
            with self.subTest(fields=tuple(changes)):
                self.assertEqual(self.refuse(expected, dataclasses.replace(actual, **changes)).calls, [])

    def test_raw_effective_bytes_cannot_be_canonical_reordered_or_embedded_only(self):
        expected, actual, evidence, _ = fixture()
        same_value = encoded(json.loads(actual.effective_launch_config_bytes))
        self.assertNotEqual(same_value, actual.effective_launch_config_bytes)
        self.refuse(expected, dataclasses.replace(actual, effective_launch_config_bytes=same_value),
                    "actual_effective_config_mismatch")
        changed = {**evidence, "effective_launch_config": {"schema_version": 1, "semantics": {}}}
        self.refuse(expected, dataclasses.replace(actual, schema5_bytes=encoded(changed)),
                    "scenario_effective_value_mismatch")

    def test_embedded_bool_cannot_equal_actual_integer_semantics(self):
        expected, actual, evidence, _ = fixture()
        changed = json.loads(encoded(evidence))
        changed["effective_launch_config"]["schema_version"] = True
        self.refuse(expected, dataclasses.replace(actual, schema5_bytes=encoded(changed)),
                    "scenario_effective_value_mismatch")

    def test_runner_log_wrong_case_path_duplicate_or_failure_line_refuse(self):
        expected, actual, _, _ = fixture()
        for raw in (actual.runner_log.replace(b"cut-1", b"cut-2"),
                    actual.runner_log.replace(b"/model/artifacts", b"/different/artifacts"),
                    actual.runner_log * 2,
                    actual.runner_log + b"system scenario runner failed: SOURCE_CANARY\n", b"scenario_pass\n"):
            with self.subTest(length=len(raw)):
                self.assertEqual(self.refuse(expected, dataclasses.replace(actual, runner_log=raw)).calls, [])

    def test_original_identity_token_pid_role_or_duplicate_mismatch_refuse(self):
        expected, actual, evidence, observation = fixture()
        for field, replacement in (("pid", 39000), ("process_start_token", "macos:other:token"),
                                   ("role", "be-1")):
            changed = json.loads(encoded(evidence))
            changed["process_launch_identities"][0][field] = replacement
            self.refuse(expected, dataclasses.replace(actual, schema5_bytes=encoded(changed)))
            changed = json.loads(encoded(observation))
            changed["original_launch_identities"]["frontend"][field] = replacement
            self.refuse(expected, dataclasses.replace(actual, observation_bytes=encoded(changed)))
        changed = json.loads(encoded(evidence))
        changed["process_launch_identities"][1]["pid"] = changed["process_launch_identities"][0]["pid"]
        self.refuse(expected, dataclasses.replace(actual, schema5_bytes=encoded(changed)), "original_pid_duplicate")

    def test_pid_present_eperm_unknown_and_reuse_are_not_absence(self):
        expected, actual, _, _ = fixture()
        for code, refusal in ((None, "original_pid_present"), (errno.EPERM, "original_pid_absence_unknown"),
                              (errno.EIO, "original_pid_absence_unknown")):
            probe = Probe({expected.roles[1].pid: code})
            self.refuse(expected, actual, refusal, probe)
            self.assertEqual(probe.calls, [(expected.roles[0].pid, 0), (expected.roles[1].pid, 0)])
        # A reused occupied PID still returns success from signal 0; token equality
        # across receipts never waives that occupied PID.
        self.refuse(expected, actual, "original_pid_present", Probe({expected.roles[0].pid: None}))

    def test_unknown_or_cancelled_probe_is_finite_whole_refusal(self):
        expected, actual, _, _ = fixture()
        for error in (RuntimeError("SOURCE_CANARY"), KeyboardInterrupt("SOURCE_CANARY")):
            def failing_probe(pid, signal):
                raise error
            self.refuse(expected, actual, "original_pid_absence_unknown", failing_probe)

    def test_json_duplicate_unknown_missing_truncated_nonfinite_depth_and_caps(self):
        expected, actual, evidence, _ = fixture()
        for raw in (b'{"schema_version":5,"schema_version":5}',
                    encoded({**evidence, "unknown": "SOURCE_CANARY"}),
                    encoded({key: value for key, value in evidence.items() if key != "source_dirty"}),
                    actual.schema5_bytes[:-1], b'{"value":NaN}', b'{"value":Infinity}',
                    b'{"value":' + b'[' * 25 + b'0' + b']' * 25 + b'}',
                    b' ' * (v.JSON_BYTES + 1), b'{"value":"\\ud800"}'):
            with self.subTest(length=len(raw)):
                self.assertEqual(self.refuse(expected, dataclasses.replace(actual, schema5_bytes=raw)).calls, [])

    def test_missing_partial_effective_group_and_case_input_mismatch_refuse(self):
        expected, actual, _, observation = fixture()
        for changes in ({"effective_launch_config_bytes": len(actual.effective_launch_config_bytes) + 1},
                        {"effective_launch_config_sha256": "0" * 64},
                        {"original_prelaunch_config_artifact": "other.json"},
                        {"exact_sql_sha256": "0" * 64}, {"cut_row_wire_bytes": True},
                        {"expected_row_sha256": "0" * 64}, {"expected_prefix_sha256": "0" * 64},
                        {"reader": False}, {"controls": ["ActualArm", None, "hole"] + [None] * 13},
                        {"held_roots": None}, {"recovery_after_tasks_created": [0, True, 0]}):
            self.refuse(expected, dataclasses.replace(actual, observation_bytes=encoded({**observation, **changes})))
        changed = {key: value for key, value in observation.items()
                   if key != "effective_launch_config_bytes"}
        self.refuse(expected, dataclasses.replace(actual, observation_bytes=encoded(changed)), "observation_fields")

    def test_admitted_expected_cannot_be_filled_from_observation_under_test(self):
        expected, actual, _, observation = fixture()
        changed = json.loads(encoded(observation))
        changed["admitted_provenance"]["server_binary_sha256"] = "0" * 64
        self.refuse(expected, dataclasses.replace(actual, observation_bytes=encoded(changed)),
                    "observation_provenance_mismatch")
        bad = dataclasses.replace(expected, provenance={**expected.provenance, "clean_revision": "a" * 9})
        self.refuse(bad, actual, "admitted_full_revision")

    def test_existing_server_build_marker_strict_full_commit_feature_and_one_line(self):
        raw = ("NOVAROCKS_MEM_1_M07_BUILD commit=" + "a" * 40
               + " build_identity=exact-test exact_mysql_write=true\n").encode()
        self.assertEqual(v.parse_server_build_identity(raw), ("a" * 40, "exact-test"))
        for malformed in (raw * 2, raw[:-1], raw.replace(b"true", b"false"),
                          raw.replace(b"a" * 40, b"a" * 9), raw.replace(b"exact-test", b"unknown"),
                          raw.replace(b" exact_mysql_write=", b" unknown=SOURCE_CANARY exact_mysql_write=")):
            with self.assertRaises(v.VerificationFailure) as caught:
                v.parse_server_build_identity(malformed)
            self.assertNotIn("SOURCE_CANARY", str(caught.exception))

    def test_raw_secret_canary_never_in_error_or_dataclass_presentation(self):
        expected, actual, _, _ = fixture()
        actual = dataclasses.replace(actual, base_config_bytes=b"password='SOURCE_CANARY'\n")
        self.refuse(expected, actual, "actual_base_config_mismatch")
        for item in (actual, expected, actual.source, expected.roles[0]):
            self.assertNotIn("SOURCE_CANARY", repr(item))
            self.assertNotIn("password", repr(item))


if __name__ == "__main__":
    unittest.main()
