#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
import importlib.util
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hive_owner", HERE / "runtime_owner.py")
hive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hive)


class Backend:
    def __init__(self):
        self.calls = []
        self.containers = {}

    def daemon_id(self):
        return "test-daemon"

    def image_id(self, image):
        self.calls.append(("image", image))
        return "sha256:hms"

    def validate_resources(self, record):
        self.calls.append(("validate", record["project"]))

    def container(self, record, service):
        return self.containers.get(record["project"])

    def compose(self, record, args):
        self.calls.append(("compose", record["project"], args))
        if args[0] == "up":
            self.containers[record["project"]] = {"Id": record["project"] + "-exact-id"}
        if args[0] == "down":
            self.containers.pop(record["project"], None)


class HiveOwnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = Backend()
        self.owner = hive.runtime.RuntimeOwner(self.temp.name, daemon="test-daemon", backend=self.backend, port_start=31000, port_end=31020)
        self.owner.consumer = lambda cat, container, **kw: self.backend.calls.append(("consumer", cat, container, kw))
        self.hive = hive.HiveOwner(self.owner)
        self.hive.wait_ready = lambda record: None

    def manifest(self, cat):
        return {"runtime": {"catalog": {"id": cat, "server_warehouse": f"s3://warehouse/instances/{cat}/server",
                 "network": cat + "_iceberg_net"}},
                "minio": {"endpoint": "http://127.0.0.1:12345", "access_key_id": "key", "secret_access_key": "secret"}}

    def test_generations_have_own_records_ports_volumes_and_exact_attachments(self):
        first = self.hive.up(self.manifest("cat-a"), "local-hms")
        second = self.hive.up(self.manifest("cat-b"), "local-hms")
        self.assertNotEqual(first["project"], second["project"])
        self.assertNotEqual(first["ports"], second["ports"])
        self.assertTrue(31000 <= first["ports"]["hms"] <= 31020)
        self.assertNotEqual(first["volumes"], second["volumes"])
        self.assertNotIn("nr-iceberg-hive", repr(self.backend.calls))
        self.assertIn(("consumer", "cat-a", first["container_id"], {"alias": "hms"}), self.backend.calls)
        self.backend.calls.clear()
        self.hive.down("cat-a", volumes=True)
        operations = self.backend.calls
        disconnect = operations.index(("consumer", "cat-a", first["container_id"], {"disconnect": True}))
        down = operations.index(("compose", first["project"], ["down", "--volumes"]))
        self.assertLess(disconnect, down)
        self.assertIn(second["project"], self.backend.containers)
        self.assertEqual(self.hive.read("cat-a")["state"], "stopped")
        self.assertEqual(self.hive.read("cat-b")["state"], "ready")

    def test_prepare_without_docker_then_retry_keeps_fixed_port(self):
        first = self.hive.up(self.manifest("cat-a"), "local-hms", prepare_only=True)
        self.assertEqual(self.backend.calls, [])
        second = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.assertEqual(first["ports"], second["ports"])
        env = (self.hive.directory("cat-a") / "env.sh").read_text()
        self.assertIn("NOVA_ENV_SHARED_HMS_WAREHOUSE_URI=", env)
        self.assertIn("http://minio:9000", (self.hive.directory("cat-a") / "core-site.xml").read_text())
        self.assertIn("<value>secret</value>", (self.hive.directory("cat-a") / "core-site.xml").read_text())

    def test_interrupted_connection_replays_same_container_and_record(self):
        def fail(*args, **kwargs):
            raise hive.runtime.RuntimeFailure("RuntimeDeleting", "cat-a")
        self.owner.consumer = fail
        with self.assertRaises(hive.runtime.RuntimeFailure):
            self.hive.up(self.manifest("cat-a"), "local-hms")
        interrupted = self.hive.read("cat-a")
        self.assertEqual(interrupted["state"], "starting")
        self.assertIsNotNone(interrupted["container_id"])
        self.owner.consumer = lambda *args, **kw: None
        recovered = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.assertEqual(recovered["container_id"], interrupted["container_id"])
        self.assertEqual(recovered["ports"], interrupted["ports"])


if __name__ == "__main__":
    unittest.main()
