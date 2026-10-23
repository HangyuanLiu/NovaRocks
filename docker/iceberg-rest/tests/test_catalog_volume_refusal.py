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

"""A complete native refusal is a failed request, never an absence barrier."""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
import machine_runtime as machine


class CatalogVolumeRefusalTests(unittest.TestCase):
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            backend = machine.MachineDocker(environment={})
            owner = machine.MachineOwner(Path(directory), daemon="fixture-daemon", backend=backend)
            base = owner.base
            old = dict(kind="machine", namespace=base.name, control_root=str(base.parent),
                       generation="old-generation", version="old-version", daemon_id="fixture-daemon")
            volume = dict(Name=machine.PROJECT + "_rest-catalog", Driver="local", Scope="local",
                          Options=None, Labels=backend.labels(old), CreatedAt="2026-10-20T00:00:00Z")
            state = dict(schema=1, phase="maintenance", generation="new-generation", version="new-version",
                         daemon_id="fixture-daemon", previous=copy.deepcopy(old), fenced_store=old,
                         steps=dict(writers_stopped=True, root_rotated=True, root_fenced=True))
            fact = dict(schema=1, kind="control", phase="command-unknown", mutating=True,
                        control_root=str(base.parent), machine=base.name, command_dispatched=True,
                        cli_pid=12345, command_exit=1, command_succeeded=False, timed_out=False,
                        interrupted=False, leader_waited=True, group_exit_confirmed=True,
                        group_exit_observed=True, maintenance_generation=state["generation"],
                        maintenance_version=state["version"], catalog_removal=dict(
                            source_generation=old["generation"], source_version=old["version"], volume=volume))
            path = base / "operations/control-refused.json"
            request = dict(operation="control", args=["volume", "rm", volume["Name"]],
                           environment=backend.environment, deadline_monotonic=1.0)
            runtime.atomic_json(base / "state.json", state)
            runtime.atomic_json(path, fact)
            runtime.atomic_json(path.with_suffix(".request"), request)
            stdout = path.with_suffix(".stdout")
            stderr = Path(str(stdout) + ".stderr")
            stdout.write_bytes(b"")
            stderr.write_text("Error response from daemon: remove " + volume["Name"]
                              + ": volume is in use - [" + "a" * 64 + "]\n")
            with owner.maintenance_lock(), mock.patch.object(backend, "daemon_id", return_value="fixture-daemon"), \
                    mock.patch.object(backend, "validate_resources") as validate, \
                    mock.patch.object(backend, "inspect", return_value=volume) as inspect:
                yield backend, base, path, fact, state, request, stdout, stderr, validate, inspect

    def test_complete_exact_refusal_settles_failure_without_changing_maintenance(self):
        with self.fixture() as (backend, base, path, fact, state, _, _, stderr, validate, inspect):
            original_state = (base / "state.json").read_bytes()
            original_error = stderr.read_bytes()
            backend.settle_operations(base)
            final = runtime.read_json(path)
            self.assertEqual(final["phase"], "settled")
            self.assertFalse(final["command_succeeded"])
            self.assertEqual(final["command_exit"], 1)
            self.assertEqual(final["terminal_failure"], "daemon-catalog-volume-in-use")
            self.assertTrue(final["reconciled_same_maintenance_intent"])
            self.assertEqual(set(final["terminal_evidence"]), {"fact", "request", "state", "stdout", "stderr"})
            self.assertEqual((base / "state.json").read_bytes(), original_state)
            self.assertEqual(stderr.read_bytes(), original_error)
            validate.assert_called_once_with(state["fenced_store"])
            inspect.assert_called_once_with("volume", fact["catalog_removal"]["volume"]["Name"], absent_ok=True)

    def test_removal_persists_exact_volume_identity_before_control_dispatch(self):
        with self.fixture() as values:
            backend, base, _, fact, state = values[:5]
            volume = copy.deepcopy(fact["catalog_removal"]["volume"])
            def capture(path, request, timeout):
                saved = runtime.read_json(path)
                self.assertEqual(saved["phase"], "intent")
                self.assertEqual(saved["catalog_removal"], fact["catalog_removal"])
                self.assertEqual(saved["maintenance_generation"], state["generation"])
                self.assertEqual(request["args"], ["volume", "rm", volume["Name"]])
                raise runtime.RuntimeFailure("DispatchCaptured", "no Docker call dispatched")
            with mock.patch.object(backend, "container", return_value=None), \
                    mock.patch.object(backend, "_supervise", side_effect=capture) as supervisor:
                with self.assertRaisesRegex(runtime.RuntimeFailure, "DispatchCaptured"):
                    backend.remove_rest_state(state["fenced_store"])
                supervisor.assert_called_once()

    def test_real_supervisor_waits_fake_cli_refusal_before_narrow_settlement(self):
        # The real control owner supervises a local fake CLI. This exercises
        # actual wait/group facts, not an assertion about a real daemon reply.
        with self.fixture() as values:
            backend, base, original_path, fact = values[:4]
            original_path.unlink()
            binary = base / "fake-bin/docker"
            binary.parent.mkdir()
            binary.write_text("#!" + sys.executable + "\nimport sys\n"
                "print('Error response from daemon: remove ' + sys.argv[-1] + "
                "': volume is in use - [' + 'a' * 64 + ']', file=sys.stderr)\nsys.exit(1)\n")
            binary.chmod(0o700)
            backend.environment = runtime.controlled_environment({"PATH": str(binary.parent) + os.pathsep + os.environ["PATH"]})
            with self.assertRaisesRegex(runtime.RuntimeFailure, "MachineOperationUnconfirmed"):
                backend.command(["volume", "rm", fact["catalog_removal"]["volume"]["Name"]],
                                catalog_removal=fact["catalog_removal"])
            paths = list((base / "operations").glob("*.json"))
            self.assertEqual(len(paths), 1)
            native = runtime.read_json(paths[0])
            self.assertEqual(native["phase"], "command-unknown")
            self.assertEqual(native["command_exit"], 1)
            self.assertIs(native["timed_out"], False)
            for key in ("leader_waited", "group_exit_confirmed", "group_exit_observed"):
                self.assertIs(native[key], True)
            backend.settle_operations(base)
            settled = runtime.read_json(paths[0])
            self.assertEqual(settled["phase"], "settled")
            self.assertFalse(settled["command_succeeded"])
            self.assertEqual(settled["command_exit"], 1)

    def test_incomplete_native_completion_never_settles(self):
        changes = {"schema": True, "kind": "mc", "mutating": False, "command_exit": True,
                   "timed_out": True, "interrupted": True, "command_succeeded": True,
                   "cli_pid": 0, "phase": "cleanup-failed", "control_root": "foreign",
                   "machine": "foreign", "maintenance_generation": "foreign",
                   "maintenance_version": "foreign", "error": "failure",
                   "cleanup_error": "failure", "cleanup_failed": True,
                   "group_observation_error": "failure", "leader_observation_error": "failure"}
        changes.update({key: False for key in ("command_dispatched", "leader_waited",
                                              "group_exit_confirmed", "group_exit_observed")})
        for key, value in changes.items():
            optional_errors = {"error", "cleanup_error", "cleanup_failed", "group_observation_error", "leader_observation_error"}
            for omit in ((False,) if key in optional_errors else (False, True)):
                with self.subTest(key=key, omit=omit), self.fixture() as values:
                    backend, base, path, fact = values[:4]
                    if omit:
                        fact.pop(key, None)
                    else:
                        fact[key] = value
                    runtime.atomic_json(path, fact)
                    before = path.read_bytes()
                    self.assertFalse(backend.reconcile_terminal_catalog_refusal(path, fact, base))
                    self.assertEqual(path.read_bytes(), before)

    def test_wrong_command_response_or_replaced_volume_remains_unknown(self):
        for condition in ("force", "multiple", "different", "environment", "stdout", "extra-line",
                          "empty-refs", "short-ref", "invalid-utf8", "oversize", "symlink",
                          "group-writable", "replacement", "absent", "no-purpose", "changed-output"):
            with self.subTest(condition=condition), self.fixture() as values:
                backend, base, path, fact, _, request, stdout, stderr, validate, inspect = values
                if condition == "force": request["args"].insert(2, "--force")
                if condition == "multiple": request["args"].append("another-volume")
                if condition == "different": request["args"][-1] = "foreign"
                if condition == "environment": request["environment"] = {"DOCKER_HOST": "unix:///other.sock"}
                runtime.atomic_json(path.with_suffix(".request"), request)
                if condition == "stdout": stdout.write_text("partial output")
                if condition == "extra-line": stderr.write_bytes(stderr.read_bytes() + b"another error\n")
                if condition == "empty-refs": stderr.write_text(stderr.read_text().replace("a" * 64, ""))
                if condition == "short-ref": stderr.write_text(stderr.read_text().replace("a" * 64, "a" * 12))
                if condition == "invalid-utf8": stderr.write_bytes(b"\xff")
                if condition == "oversize": stderr.write_bytes(b"x" * (1024 * 1024 + 1))
                if condition == "symlink":
                    original = stderr.with_name("original-stderr")
                    stderr.rename(original)
                    stderr.symlink_to(original)
                if condition == "group-writable": stderr.chmod(0o660)
                if condition == "replacement":
                    inspect.return_value = {**inspect.return_value, "CreatedAt": "new-volume"}
                if condition == "absent": inspect.return_value = None
                if condition == "no-purpose":
                    fact.pop("catalog_removal")
                    runtime.atomic_json(path, fact)
                if condition == "changed-output": validate.side_effect = lambda _: stderr.write_bytes(b"changed")
                with self.assertRaisesRegex(runtime.RuntimeFailure, "MachineOperationUnconfirmed"):
                    backend.settle_operations(base)
                self.assertEqual(runtime.read_json(path)["phase"], "command-unknown")

    def test_wrong_or_completed_maintenance_stage_cannot_reconcile(self):
        for key in ("writers_stopped", "root_rotated", "root_fenced", "rest_removed", "purged", "activation_started"):
            with self.subTest(key=key), self.fixture() as values:
                backend, base, path, fact, state = values[:5]
                state["steps"][key] = key in {"rest_removed", "purged", "activation_started"}
                runtime.atomic_json(base / "state.json", state)
                self.assertFalse(backend.reconcile_terminal_catalog_refusal(path, fact, base))
                self.assertEqual(runtime.read_json(path)["phase"], "command-unknown")

    def test_foreign_native_resources_fail_before_settlement(self):
        with self.fixture() as values:
            backend, base, path, fact = values[:4]
            values[8].side_effect = runtime.RuntimeFailure("RuntimeIdentityMismatch", "foreign volume")
            with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
                backend.settle_operations(base)
            self.assertEqual(runtime.read_json(path)["phase"], "command-unknown")


if __name__ == "__main__":
    unittest.main()
