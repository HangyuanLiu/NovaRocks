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
