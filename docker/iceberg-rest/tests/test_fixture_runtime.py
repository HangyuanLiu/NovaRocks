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

"""Protocol tests with explicit failure and concurrency barriers, without Docker."""
import copy
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

MODULE = Path(__file__).resolve().parents[1] / "fixture_runtime.py"
sys.path.insert(0, str(MODULE.parent))
import fixture_runtime as runtime


class DockerServiceNetworkTests(unittest.TestCase):
    def fixture(self, service="minio", attached=False, aliases=None):
        record = {"id": "os-test", "kind": "os", "namespace": "test-owner", "key": "test-key",
                  "project": "test-project", "network": "saved-own-network", "volumes": ["saved-volume"],
                  "compose_file": "/saved/compose.yml", "compose_env": "/saved/compose.env",
                  "images": {service: {"image_id": "sha256:service", "tag": "saved-service-tag"}},
                  "service_ports": {service: {"9000/tcp": 38000} if service == "minio" else {}},
                  "ports": {"minio": 38000}, "required_services": [service], "health_urls": ["http://host-proxy"]}
        if service == "minio":
            record["images"]["mc-init"] = {"image_id": "sha256:mc", "tag": "saved-mc-tag"}
            record["required_services"].append("mc-init")
        labels = runtime.Docker.labels(record)
        network = {"Id": "exact-own-network-id", "Labels": dict(labels), "Containers": {}}
        containers = {}
        for name, image in record["images"].items():
            container = {"Id": "exact-" + name, "Image": image["image_id"], "Config": {"Labels": dict(labels)},
                         "HostConfig": {"PortBindings": {port: [{"HostPort": str(host)}]
                                        for port, host in record["service_ports"].get(name, {}).items()}},
                         "State": {"Running": True, "Status": "running"},
                         "NetworkSettings": {"Networks": {}}}
            if name == service:
                # These surviving attachments must never be disconnected during repair.
                container["NetworkSettings"]["Networks"] = {
                    "catalog-a": {"NetworkID": "other-a", "Aliases": ["minio"]},
                    "catalog-b": {"NetworkID": "other-b", "Aliases": ["minio"]}}
            if name != service or attached:
                container["NetworkSettings"]["Networks"][record["network"]] = {
                    "NetworkID": network["Id"], "Aliases": aliases if name == service else [name]}
                network["Containers"][container["Id"]] = {}
            containers[name] = container
        backend = runtime.Docker(timeout=1)
        calls = []
        def inspect(kind, identity, **kwargs):
            if kind == "container":
                return copy.deepcopy(next(value for value in containers.values() if value["Id"] == identity))
            if kind == "network":
                self.assertEqual(identity, record["network"])
                return copy.deepcopy(network)
            if kind == "volume":
                return {"Labels": dict(labels)}
            if kind == "image":
                return {"Id": next(image["image_id"] for image in record["images"].values() if image["tag"] == identity)}
            self.fail("unexpected inspect: " + kind)
        def command(args, **kwargs):
            calls.append(args)
            if args[0] == "ps":
                selected = next(arg.split("=", 1)[1] for arg in args if arg.startswith("label=com.docker.compose.service="))
                name = selected.rsplit("=", 1)[1]
                return containers[name]["Id"]
            if args[0] == "compose":
                self.assertEqual(args[-4:], ["up", "-d", "minio", "mc-init"])
                return ""
            if args[:2] == ["network", "disconnect"]:
                self.assertEqual(args[2], record["network"])
                container = next(value for value in containers.values() if value["Id"] == args[3])
                container["NetworkSettings"]["Networks"].pop(record["network"], None)
                network["Containers"].pop(container["Id"], None)
                return ""
            if args[:2] == ["network", "connect"]:
                self.assertEqual(args[-2], record["network"])
                container = next(value for value in containers.values() if value["Id"] == args[-1])
                container["NetworkSettings"]["Networks"][record["network"]] = {
                    "NetworkID": network["Id"], "Aliases": args[3:-2:2]}
                network["Containers"][container["Id"]] = {}
                if "mc-init" in containers:
                    containers["mc-init"]["State"] = {"Running": False, "Status": "exited", "ExitCode": 0}
                    # Completed initialization does not need a retained live endpoint.
                    containers["mc-init"]["NetworkSettings"]["Networks"] = {}
                    network["Containers"].pop(containers["mc-init"]["Id"], None)
                return ""
            self.fail("unexpected command: " + repr(args))
        backend.inspect = inspect
        backend.command = command
        backend.http_ready = lambda url: True
        return backend, record, containers, network, calls

    def test_exact_endpoint_repair_preserves_minio_other_networks_and_health(self):
        backend, record, containers, network, calls = self.fixture()
        original = copy.deepcopy(containers["minio"])
        ports = copy.deepcopy(record["ports"])
        containers["mc-init"]["State"] = {"Running": False, "Status": "exited", "ExitCode": 0}
        # Even an accepting host HTTP endpoint and successful prior bucket init
        # cannot mask the running service's missing own network.
        self.assertFalse(backend.healthy(record))
        containers["mc-init"]["State"] = {"Running": True, "Status": "running"}
        backend.repair_service_networks(record)
        self.assertTrue(backend.healthy(record))
        self.assertEqual(record["ports"], ports)
        self.assertEqual(backend.container(record, "minio")["Id"], original["Id"])
        for name in ("catalog-a", "catalog-b"):
            self.assertEqual(containers["minio"]["NetworkSettings"]["Networks"][name],
                             original["NetworkSettings"]["Networks"][name])
        changes = [args for args in calls if args[0] == "network"]
        self.assertEqual(changes, [["network", "connect", "--alias", "minio", "--alias", "warehouse.minio",
                                   "--alias", "novarocks.minio", record["network"], original["Id"]]])

    def test_service_aliases_are_repaired_without_recreating_the_container(self):
        backend, record, containers, network, calls = self.fixture(service="spark", attached=True, aliases=["wrong"])
        self.assertFalse(backend.healthy(record))
        backend.repair_service_networks(record)
        self.assertTrue(backend.healthy(record))
        self.assertEqual([args for args in calls if args[0] == "network"], [
            ["network", "disconnect", record["network"], "exact-spark"],
            ["network", "connect", "--alias", "spark", record["network"], "exact-spark"]])

    def test_foreign_network_is_not_repaired(self):
        backend, record, containers, network, calls = self.fixture()
        network["Labels"]["novarocks.fixture.owner"] = "foreign-owner"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
            backend.repair_service_networks(record)
        self.assertFalse(any(args[0] == "network" for args in calls))


class DockerCommandTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def assert_code(self, code, callable):
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            callable()
        self.assertEqual(caught.exception.code, code)


    def test_docker_environment_discards_inherited_compose_overrides(self):
        env = runtime.controlled_environment({"PATH": "path", "COMPOSE_PROJECT_NAME": "foreign",
            "REST_IMAGE": "foreign", "DOCKER_CONTEXT": "desktop-linux"})
        self.assertEqual(env, {"PATH": "path", "DOCKER_CONTEXT": "desktop-linux"})

    def test_private_compose_failure_retains_safe_native_output_after_entry_purge(self):
        entry = self.root / "private-entry"
        entry.mkdir()
        secret = 'fixture-SECRET-"quoted"-$value'
        record = dict(kind="isolated", project="nr-isolated-rest-unit", compose_file=str(entry / "compose.yml"),
            compose_env=str(entry / "compose.env"), config={"credentials":{"access_key":"fixture-AK", "secret_key":secret}},
            external_minio={"credentials":{"access_key":"shared-AK", "secret_key":"shared-SECRET"}})
        stderr = ("services.rest.command: invalid command configuration\n" + secret + "\n" + json.dumps(secret) +
            '\nAWS_SECRET_ACCESS_KEY="unknown-env-secret"\nMINIO_ROOT_USER=unknown-root-key\n' +
            'Authorization: Bearer unknown-bearer\n{"session_token":"unknown-token"}\n' +
            'http://unknown-user:unknown-password@minio:9000/x?X-Amz-Signature=unknown-signature&x=1\nshared-SECRET\n')
        _, environment = self.fake_docker()
        environment["XDG_STATE_HOME"] = str(self.root / "state")
        executable = self.root / "bin/docker"
        executable.write_text(f"#!{sys.executable}\nimport sys\nsys.stdout.write('private fixture-AK output\\n')\nsys.stderr.write({stderr!r})\nsys.exit(17)\n")
        backend = runtime.Docker(environment=environment)
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            backend.compose(record, ["up", "-d"])
        self.assertEqual(caught.exception.code, "DockerOperationFailed")
        directory = Path(str(caught.exception).split("sanitized diagnostics retained at ", 1)[1])
        shutil.rmtree(entry)
        metadata = json.loads((directory / "command.json").read_text())
        self.assertEqual(metadata["native_exit"], 17)
        self.assertEqual(metadata["operation"], "up")
        self.assertNotIn("args", metadata)
        retained = "\n".join(path.read_text() for path in directory.iterdir())
        self.assertIn("services.rest.command: invalid command configuration", retained)
        for value in (secret, json.dumps(secret)[1:-1], "fixture-AK", "shared-SECRET", "unknown-env-secret",
                      "unknown-root-key", "unknown-bearer", "unknown-token", "unknown-password", "unknown-signature"):
            self.assertNotIn(value, retained + str(caught.exception))
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(directory.parent.stat().st_mode & 0o777, 0o700)
        for path in directory.iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_success_and_shared_failure_do_not_persist_private_diagnostics(self):
        record = dict(kind="isolated", project="private", compose_env="saved.env", compose_file="saved.yml")
        backend = runtime.Docker(environment={"XDG_STATE_HOME":str(self.root / "state")})
        with mock.patch.object(backend, "command", return_value="ok"):
            self.assertEqual(backend.compose(record, ["up"]), "ok")
        with mock.patch.object(backend, "command", side_effect=runtime.RuntimeFailure("DockerOperationFailed", "compose exited 1")):
            with self.assertRaises(runtime.RuntimeFailure):
                backend.compose({**record, "kind":"machine"}, ["up"])
        self.assertFalse((self.root / "state").exists())

    def test_retention_io_failure_preserves_original_native_failure(self):
        backend = runtime.Docker()
        try:
            runtime.Docker.command_result(["compose"], 23, "private stdout", "private stderr")
        except runtime.RuntimeFailure as original:
            with mock.patch.object(backend, "command", side_effect=original), \
                    mock.patch.object(backend, "_retain_compose_failure", side_effect=OSError("private path")):
                with self.assertRaises(runtime.RuntimeFailure) as caught:
                    backend.compose({"kind":"isolated", "compose_env":"saved.env", "project":"private", "compose_file":"saved.yml"}, ["up"])
        self.assertEqual(caught.exception.code, "DockerOperationFailed")
        self.assertEqual(caught.exception._docker_returncode, 23)
        self.assertNotIn("private stderr", str(caught.exception))
        self.assertIn("retention failed", str(caught.exception))


    def fake_docker(self):
        executable = self.root / "bin/docker"
        executable.parent.mkdir()
        pidfile = self.root / "docker-child.pid"
        executable.write_text("#!/bin/sh\n"
            f"printf '%s' \"$$\" > {runtime.shlex.quote(str(pidfile))}\nexec sleep 3600\n")
        executable.chmod(0o700)
        environment = {"PATH": str(executable.parent) + ":" + os.environ["PATH"]}
        return pidfile, environment


    def test_command_timeout_settles_child(self):
        pidfile, environment = self.fake_docker()
        backend = runtime.Docker(timeout=2, environment=environment)
        self.assert_code("DockerOperationTimeout", lambda: backend.command(["compose", "up"]))
        pid = int(pidfile.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)


    def test_missing_docker_context_is_not_resource_absence(self):
        _, environment = self.fake_docker()
        executable = self.root / "bin/docker"
        executable.write_text("#!/bin/sh\nprintf '%s\\n' 'context foreign not found' >&2\nexit 1\n")
        backend = runtime.Docker(timeout=5, environment=environment)
        self.assert_code("DockerOperationFailed", lambda: backend.command(["network", "inspect", "network"], absent_ok=True))


    def test_sigterm_settles_child_before_releasing_owner_lock(self):
        pidfile, environment = self.fake_docker()
        context = mp.get_context("fork")
        result = context.Queue()
        lock = self.root / "cancel.lock"
        def child():
            runtime.install_signal_handlers()
            try:
                with runtime.file_lock(lock):
                    runtime.Docker(timeout=30, environment=environment).command(["compose", "up"])
            except KeyboardInterrupt:
                result.put("cancelled")
        process = context.Process(target=child)
        process.start()
        deadline = time.monotonic() + 5
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        try:
            self.assertTrue(pidfile.exists(), "fake Docker never reached startup barrier")
            pid = int(pidfile.read_text())
            os.kill(process.pid, signal.SIGTERM)
            process.join(5)
            self.assertEqual(process.exitcode, 0)
            self.assertEqual(result.get(timeout=2), "cancelled")
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
            with lock.open("a+b") as handle:
                runtime.fcntl.flock(handle, runtime.fcntl.LOCK_EX | runtime.fcntl.LOCK_NB)
        finally:
            if process.is_alive():
                process.kill()
                process.join()



if __name__ == "__main__":
    unittest.main()
