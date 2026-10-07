# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Behavioral ownership tests; no Docker or Spark process is started."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
import uuid

SPEC = importlib.util.spec_from_file_location("field_domain_owned_job", Path(__file__).with_name("job.py"))
job = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(job)

JOB_ID = "a" * 64
SHARED_ID = "b" * 64
JOB_TOKEN = "11111111-1111-4111-8111-111111111111"
RUN_TOKEN = "22222222-2222-4222-8222-222222222222"
NAME = "nr-fd-job-" + JOB_TOKEN
SAFE = {
    "publication": "/immutable-publication", "project": "exact-project",
    "owner": "exact-owner", "key": "exact-key", "kind": "rest",
    "image_id": "sha256:" + "c" * 64,
    "image_reference": "novarocks-spark:versioned-bom",
}


def identity(container_id=JOB_ID, name=NAME, token=JOB_TOKEN, status="exited"):
    return {
        "id": container_id, "name": "/" + name, "project": SAFE["project"],
        "service": "spark", "owner": SAFE["owner"], "key": SAFE["key"],
        "kind": SAFE["kind"], "token": token, "image_id": SAFE["image_id"],
        "image_reference": SAFE["image_reference"], "status": status, "exit_code": 0,
    }


class FakeDocker:
    """Explicit command transitions, including lost replies and double absence."""

    def __init__(self, directory, launch="ok", inspected="exited", wrong_owner=False,
                 shared_discovery=False, remaining_id=False, oversized_log=False,
                 deadline=False):
        self.directory = directory
        self.launch_mode = launch
        self.inspected = inspected
        self.wrong_owner = wrong_owner
        self.shared_discovery = shared_discovery
        self.remaining_id = remaining_id
        self.oversized_log = oversized_log
        self.deadline = deadline
        self.now = 0.0
        self.live = False
        self.calls = []
        self.job_inspections = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += 5.0

    def command(self, args, until):
        self.calls.append(tuple(args))
        if self.now >= until:
            raise job.JobFailure("Fake control deadline elapsed")
        if args[:3] == ["docker", "ps", "-q"]:
            return SHARED_ID
        if args[:3] == ["docker", "ps", "-aq"]:
            selector = args[-1]
            if selector == "name=^/" + NAME + "$":
                if self.shared_discovery:
                    return SHARED_ID
                return JOB_ID if self.live else ""
            if selector == "id=" + JOB_ID:
                return JOB_ID if self.remaining_id else ""
            raise AssertionError("Unexpected discovery selector: " + selector)
        if args[:2] == ["docker", "inspect"]:
            container_id = args[-1]
            if container_id == SHARED_ID:
                return json.dumps(identity(SHARED_ID, "shared-spark", "", "running"))
            if container_id != JOB_ID or not self.live:
                raise AssertionError("Inspection is not of a live exact job")
            self.job_inspections += 1
            if self.launch_mode == "after-observation-loss" and self.job_inspections == 2:
                raise job.JobFailure("Fake status reply was lost")
            if self.deadline and self.job_inspections == 2:
                self.now += 121.0
            if self.oversized_log and self.job_inspections == 1:
                with (self.directory / "output/stdout.log").open("wb") as stream:
                    stream.truncate(job.LOG_BYTES)
            value = identity(status=self.inspected)
            if self.wrong_owner:
                value["owner"] = "foreign-owner"
            return json.dumps(value)
        if args[:2] == ["docker", "wait"]:
            if args[-1] != JOB_ID:
                raise AssertionError("Wait target drifted")
            return "0"
        if args[:3] == ["docker", "rm", "--force"]:
            if args[-1] != JOB_ID or not self.live:
                raise AssertionError("Removal is not of the exact live job")
            self.live = False
            return JOB_ID
        if args[:2] == ["python3", str(self.directory.parent / "docker/iceberg-rest/runtime_entry.py")]:
            self.live = self.launch_mode != "unknown-empty"
            if self.launch_mode in ("lost-launch-ack", "unknown-empty"):
                raise job.JobFailure("Fake launch outcome is unknown")
            return JOB_ID
        raise AssertionError("Unexpected control operation: " + repr(args))


class IdentityTests(unittest.TestCase):
    def test_every_frozen_identity_component_rejects_drift(self):
        for key, wrong in {
            "name": "/other", "project": "other", "service": "other",
            "owner": "other", "kind": "other", "key": "other", "token": "other",
            "image_id": "sha256:" + "d" * 64,
            "image_reference": "mutable:tag", "id": "short-id",
        }.items():
            with self.subTest(component=key):
                value = identity()
                value[key] = wrong
                with self.assertRaises(job.JobFailure):
                    job.validate_identity(value, SAFE, NAME, JOB_TOKEN)
        with self.assertRaises(job.JobFailure):
            job.validate_identity(identity(container_id="d" * 64), SAFE, NAME, JOB_TOKEN, JOB_ID)
        self.assertEqual(job.validate_identity(identity(), SAFE, NAME, JOB_TOKEN, JOB_ID)["id"], JOB_ID)

    def test_empty_discovery_without_an_observed_id_cannot_complete_unknown_launch(self):
        self.assertFalse(job.absence_can_complete(True, False, None))
        self.assertFalse(job.absence_can_complete(True, True, None))
        self.assertTrue(job.absence_can_complete(True, True, JOB_ID))
        self.assertTrue(job.absence_can_complete(False, False, None))


class RunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.workspace = Path(self.tmp.name).resolve(strict=True)
        self.directory = self.workspace / "action"
        self.script = self.workspace / "query.scala"
        self.defaults = self.workspace / "spark-defaults.conf"
        self.script.write_bytes(b"println(1)\n")
        self.defaults.write_bytes(b"spark.test true\n")
        self.manifest = {"compose_env": "/exact/env", "compose_file": "/exact/compose"}

    def run_fake(self, docker, terminal_failure=False):
        original_write = job.write_new

        def write(path, raw):
            if terminal_failure and path.name == "terminal.json":
                raise OSError("Fake terminal receipt failure")
            return original_write(path, raw)

        with mock.patch.object(job, "_publication", return_value=(self.manifest, SAFE, self.defaults)), \
             mock.patch.object(job, "command", side_effect=docker.command), \
             mock.patch.object(job.time, "monotonic", side_effect=docker.monotonic), \
             mock.patch.object(job.time, "sleep", side_effect=docker.sleep), \
             mock.patch.object(job.uuid, "uuid4", return_value=uuid.UUID(JOB_TOKEN)), \
             mock.patch.object(job, "write_new", side_effect=write):
            return job.run(self.workspace, "/publication", self.script, self.directory, RUN_TOKEN, "initialize")

    def terminal(self):
        return json.loads((self.directory / "terminal.json").read_bytes())

    def removals(self, docker):
        return [args for args in docker.calls if args[:3] == ("docker", "rm", "--force")]

    def assert_exact_cleanup(self, docker):
        self.assertEqual(self.removals(docker), [("docker", "rm", "--force", JOB_ID)])
        rm_index = docker.calls.index(("docker", "rm", "--force", JOB_ID))
        later = docker.calls[rm_index + 1:]
        self.assertTrue(any(args[-1] == "name=^/" + NAME + "$" for args in later))
        self.assertTrue(any(args[-1] == "id=" + JOB_ID for args in later))

    def test_success_removes_exact_job_and_proves_name_and_id_absence(self):
        docker = FakeDocker(self.directory)
        receipt = self.run_fake(docker)
        self.assertTrue(receipt["execution_confirmed"])
        self.assertTrue(receipt["confirmed_gone"])
        self.assertFalse(receipt["forced"])
        self.assertEqual(receipt["container_id"], JOB_ID)
        self.assertEqual(receipt["image_id"], SAFE["image_id"])
        self.assertEqual(receipt["image_reference"], SAFE["image_reference"])
        self.assertNotEqual(receipt["image_id"], receipt["image_reference"])
        self.assert_exact_cleanup(docker)
        launch = next(args for args in docker.calls if args[0] == "python3")
        self.assertIn("--detach", launch)
        self.assertIn("--no-deps", launch)
        self.assertEqual(launch[launch.index("--pull") + 1], "never")
        self.assertNotIn("--service-ports", launch)
        self.assertNotIn("--publish", launch)
        self.assertFalse(any(args[:2] in (("docker", "stop"), ("docker", "kill")) for args in docker.calls))

    def test_lost_launch_ack_discovers_ownership_then_removes_exact_id(self):
        docker = FakeDocker(self.directory, launch="lost-launch-ack", inspected="running")
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        receipt = self.terminal()
        self.assertEqual(receipt["container_id"], JOB_ID)
        self.assertTrue(receipt["forced"])
        self.assertTrue(receipt["confirmed_gone"])
        self.assertFalse(receipt["execution_confirmed"])
        self.assert_exact_cleanup(docker)

    def test_reply_loss_after_exact_observation_keeps_exact_cleanup_identity(self):
        docker = FakeDocker(self.directory, launch="after-observation-loss", inspected="running")
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assertTrue(self.terminal()["confirmed_gone"])
        self.assertFalse(self.terminal()["execution_confirmed"])
        self.assert_exact_cleanup(docker)

    def test_unknown_launch_with_empty_discovery_never_claims_gone(self):
        docker = FakeDocker(self.directory, launch="unknown-empty")
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        receipt = self.terminal()
        self.assertIsNone(receipt["container_id"])
        self.assertFalse(receipt["confirmed_gone"])
        self.assertFalse(receipt["execution_confirmed"])
        self.assertEqual(self.removals(docker), [])

    def test_wrong_owner_is_never_removed(self):
        docker = FakeDocker(self.directory, wrong_owner=True)
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assertEqual(self.removals(docker), [])
        self.assertFalse(self.terminal()["confirmed_gone"])

    def test_shared_spark_is_never_removed_even_if_discovery_returns_it(self):
        docker = FakeDocker(self.directory, shared_discovery=True)
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assertEqual(self.removals(docker), [])
        self.assertFalse(self.terminal()["confirmed_gone"])

    def test_successful_rm_without_exact_id_absence_is_not_complete(self):
        docker = FakeDocker(self.directory, remaining_id=True)
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assert_exact_cleanup(docker)
        self.assertTrue(self.terminal()["execution_confirmed"])
        self.assertFalse(self.terminal()["confirmed_gone"])

    def test_execution_deadline_still_forces_exact_cleanup(self):
        docker = FakeDocker(self.directory, inspected="running", deadline=True)
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assert_exact_cleanup(docker)
        self.assertTrue(self.terminal()["forced"])
        self.assertTrue(self.terminal()["confirmed_gone"])

    def test_log_budget_failure_still_forces_exact_cleanup(self):
        docker = FakeDocker(self.directory, inspected="running", oversized_log=True)
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assert_exact_cleanup(docker)
        self.assertFalse(self.terminal()["execution_confirmed"])
        self.assertTrue(self.terminal()["confirmed_gone"])

    def test_terminal_receipt_failure_cannot_bypass_exact_cleanup(self):
        docker = FakeDocker(self.directory)
        with self.assertRaises(OSError):
            self.run_fake(docker, terminal_failure=True)
        self.assert_exact_cleanup(docker)
        self.assertFalse((self.directory / "terminal.json").exists())

    def test_combined_input_budget_is_rejected_before_any_control_operation(self):
        self.script.write_bytes(b"x" * (job.INPUT_BYTES // 2 + 1))
        self.defaults.write_bytes(b"y" * (job.INPUT_BYTES // 2))
        docker = FakeDocker(self.directory)
        with self.assertRaises(job.JobFailure):
            self.run_fake(docker)
        self.assertEqual(docker.calls, [])


class PublicationTests(unittest.TestCase):
    def test_publication_keeps_exact_image_id_and_bom_tag_as_distinct_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            publication = Path(directory).resolve(strict=True)
            defaults = publication / "spark-defaults.conf"
            defaults.write_text("spark.test true\n")
            compose = publication / "compose.yml"
            compose.write_text("services: {}\n")
            environment = publication / "compose.env"
            environment.write_text("EXACT=value\n")
            manifest = {
                "compose_project": SAFE["project"], "compose_file": str(compose),
                "compose_env": str(environment),
                "runtime": {"catalog": {
                    "namespace": SAFE["owner"], "key": SAFE["key"], "kind": SAFE["kind"],
                    "images": {"spark": {"image_id": SAFE["image_id"],
                                           "tag": SAFE["image_reference"]}},
                }},
                "spark": {"image": SAFE["image_reference"], "defaults_file": str(defaults)},
            }
            path = publication / "manifest.json"
            path.write_text(json.dumps(manifest))
            _, safe, actual_defaults = job._publication(publication)
            self.assertEqual(safe["image_id"], SAFE["image_id"])
            self.assertEqual(safe["image_reference"], SAFE["image_reference"])
            self.assertNotEqual(safe["image_id"], safe["image_reference"])
            self.assertEqual(actual_defaults, defaults)
            for wrong_id, wrong_tag in (("not-an-immutable-image", SAFE["image_reference"]),
                                        (SAFE["image_id"], "foreign-bom:tag")):
                with self.subTest(image_id=wrong_id, image_reference=wrong_tag):
                    manifest["runtime"]["catalog"]["images"]["spark"]["image_id"] = wrong_id
                    manifest["spark"]["image"] = wrong_tag
                    path.write_text(json.dumps(manifest))
                    with self.assertRaises(job.JobFailure):
                        job._publication(publication)


class InputAndControlTests(unittest.TestCase):
    def test_regular_input_over_budget_and_symlink_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input"
            path.write_bytes(b"12345")
            with self.assertRaises(job.JobFailure):
                job.read_regular(path, 4)
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                job.read_regular(link, 5)
            self.assertEqual(job.read_regular(path, 5), b"12345")

    def test_control_over_budget_is_checked_before_output_decode(self):
        for destination in ("stdout", "stderr"):
            with self.subTest(stream=destination):
                def popen(args, **kwargs):
                    kwargs[destination].write(b"x" * (job.CONTROL_BYTES + 1))
                    kwargs[destination].flush()
                    return mock.Mock(returncode=0)
                with mock.patch.object(job.subprocess, "Popen", side_effect=popen), \
                     mock.patch.object(job.time, "monotonic", return_value=0):
                    with self.assertRaisesRegex(job.JobFailure, "output exceeded"):
                        job.command(["fake-control"], 1)

    def test_control_launch_installs_real_fsize_limit_and_no_parent_signal(self):
        captured = {}

        def popen(args, **kwargs):
            captured.update(kwargs)
            kwargs["stdout"].write(b"exact-control")
            kwargs["stdout"].flush()
            return mock.Mock(returncode=0)

        with mock.patch.object(job.subprocess, "Popen", side_effect=popen), \
             mock.patch.object(job.time, "monotonic", return_value=0):
            self.assertEqual(job.command(["fake-control"], 1), "exact-control")
        self.assertTrue(captured["start_new_session"])
        self.assertEqual(captured["stdin"], subprocess.DEVNULL)
        with mock.patch.object(job.resource, "setrlimit") as limit:
            captured["preexec_fn"]()
        limit.assert_called_once_with(job.resource.RLIMIT_FSIZE, (job.CONTROL_BYTES, job.CONTROL_BYTES))

    def test_timed_out_control_kills_only_its_process_group_and_keeps_unknown(self):
        process = mock.Mock(pid=12345)
        process.wait.side_effect = [subprocess.TimeoutExpired("fake", 1), None]
        with mock.patch.object(job.subprocess, "Popen", return_value=process), \
             mock.patch.object(job.time, "monotonic", return_value=0), \
             mock.patch.object(job.os, "killpg") as kill:
            with self.assertRaisesRegex(job.JobFailure, "outcome is unknown"):
                job.command(["fake-control"], 1)
        kill.assert_called_once_with(12345, 9)
        self.assertEqual(process.wait.call_count, 2)


if __name__ == "__main__":
    unittest.main()
