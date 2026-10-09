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

"""REST contract probes against preserving and dropping HTTP fixtures."""

import copy
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
import urllib.parse
import uuid


PATH = Path(__file__).resolve().parents[1] / "rest-mv/probe.py"
SPEC = importlib.util.spec_from_file_location("rest_mv_probe", PATH)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class RestFixture:
    def __init__(self, *, preserve=False, prefix="", collapse_pointer=False, retain_null=False, ordinary_drift=False):
        self.preserve = preserve
        self.prefix = prefix
        self.collapse_pointer = collapse_pointer
        self.retain_null = retain_null
        self.ordinary_drift = ordinary_drift
        self.namespaces = {}
        self.calls = []
        self.config_statuses = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def process(self):
                content = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                payload = json.loads(content) if content else None
                code, response = fixture.request(self.command, self.path, payload)
                encoded = json.dumps(response).encode() if response is not None else b""
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            do_GET = process
            do_POST = process
            do_DELETE = process

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.02), daemon=True)
        self.thread.start()
        self.uri = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def adjusted_version(self, item):
        item = copy.deepcopy(item)
        if not self.preserve or (item.get("storage-table") is None and not self.retain_null):
            item.pop("storage-table", None)
        return item

    def request(self, method, url, payload):
        self.calls.append((method, url, copy.deepcopy(payload)))
        parsed = urllib.parse.urlparse(url)
        if parsed.path == "/v1/config":
            if self.config_statuses:
                return self.config_statuses.pop(0), {}
            if "warehouse" not in urllib.parse.parse_qs(parsed.query):
                return 400, {}
            return 200, {"defaults": {"prefix": self.prefix}, "overrides": {}}
        base = "/v1/" + (self.prefix + "/" if self.prefix else "") + "namespaces"
        if parsed.path == base and method == "POST":
            namespace = payload["namespace"][0]
            self.namespaces[namespace] = {"views": {}, "tables": {}}
            return 200, {"namespace": [namespace], "properties": {}}
        if not parsed.path.startswith(base + "/"):
            return 404, {}
        parts = parsed.path[len(base) + 1:].split("/")
        namespace = self.namespaces.get(parts[0])
        if namespace is None:
            return 404, {}
        if len(parts) == 1 and method == "DELETE":
            if any(namespace.values()):
                return 409, {}
            del self.namespaces[parts[0]]
            return 204, None
        resources = namespace[parts[1]]
        if len(parts) == 2 and method == "POST":
            name = payload["name"]
            if parts[1] == "views":
                pointer = payload["view-version"].get("storage-table")
                if self.preserve and pointer is not None and (not isinstance(pointer, dict)
                        or not isinstance(pointer.get("name"), str) or not isinstance(pointer.get("namespace"), list)):
                    return 400, {}
                item = self.adjusted_version(payload["view-version"])
                if self.ordinary_drift and name == "ordinary_view":
                    item["representations"][0]["sql"] = "SELECT 999 AS id"
                metadata = {"view-uuid": str(uuid.uuid4()), "format-version": 1,
                            "location": payload["location"], "current-version-id": 1,
                            "versions": [item], "schemas": [payload["schema"]],
                            "version-log": [{"version-id": 1, "timestamp-ms": item["timestamp-ms"]}]}
            else:
                metadata = {"table-uuid": str(uuid.uuid4()), "format-version": 2,
                            "location": payload["location"], "current-schema-id": 0,
                            "schemas": [payload["schema"]], "properties": {}}
            resources[name] = {"metadata": metadata, "metadata-location": metadata["location"] + "/metadata/1.metadata.json"}
            return 200, resources[name]
        if len(parts) != 3 or parts[2] not in resources:
            return 404, {}
        result = resources[parts[2]]
        if method == "DELETE":
            del resources[parts[2]]
            return 204, None
        if method == "POST":
            metadata = result["metadata"]
            for update in payload["updates"]:
                action = update["action"]
                if action == "add-view-version":
                    item = self.adjusted_version(update["view-version"])
                    old = probe.current_version(metadata)
                    if not (self.collapse_pointer and item["representations"] == old["representations"]):
                        metadata["versions"].append(item)
                elif action == "set-current-view-version":
                    metadata["current-version-id"] = metadata["versions"][-1]["version-id"]
                    metadata["version-log"].append({"version-id": metadata["current-version-id"], "timestamp-ms": 42})
                elif action == "add-schema":
                    metadata["schemas"].append(update["schema"])
                elif action == "set-current-schema":
                    metadata["current-schema-id"] = metadata["schemas"][-1]["schema-id"]
                elif action == "set-properties":
                    metadata["properties"].update(update["updates"])
            result["metadata-location"] = metadata["location"] + "/metadata/2.metadata.json"
        return 200, result


class RestMVProbeTests(unittest.TestCase):
    def fixtures(self, *, stock=None, mv=None):
        stock = RestFixture(**(stock or {}))
        mv = RestFixture(**({"preserve": True} if mv is None else mv))
        self.addCleanup(stock.close)
        self.addCleanup(mv.close)
        return stock, mv, {"ready": True, "shared_docker": True,
                           "iceberg_rest": {"uri": stock.uri, "warehouse": "s3://warehouse/test/rest"},
                           "iceberg_rest_mv": {"uri": mv.uri, "warehouse": "s3://warehouse/test/rest-mv"}}

    def test_contract_accepts_preserving_mv_and_dropping_stock_with_config_prefix(self):
        stock, mv, manifest = self.fixtures(stock={"prefix": "tenant/stock"}, mv={"preserve": True, "prefix": "tenant/mv"})
        probe.run_probe(manifest, "contract")
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})
        self.assertTrue(any(path.startswith("/v1/tenant/mv/namespaces/") for _, path, _ in mv.calls))
        self.assertTrue(any("warehouse=" in path for _, path, _ in mv.calls))

    def test_dropping_mv_is_rejected_and_both_catalogs_are_cleaned(self):
        stock, mv, manifest = self.fixtures(mv={"preserve": False})
        with self.assertRaisesRegex(probe.ContractFailure, "storage-table was lost"):
            probe.run_probe(manifest, "contract")
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})

    def test_pointer_only_version_collapse_and_null_retention_are_rejected(self):
        for defect, message in (("collapse_pointer", "did not advance"), ("retain_null", "null storage-table")):
            with self.subTest(defect=defect):
                stock, mv, manifest = self.fixtures(mv={"preserve": True, defect: True})
                with self.assertRaisesRegex(probe.ContractFailure, message):
                    probe.run_probe(manifest, "contract")
                self.assertEqual(stock.namespaces, {})
                self.assertEqual(mv.namespaces, {})

    def test_reference_parity_requires_both_catalogs_to_drop_storage_table(self):
        stock, reference, manifest = self.fixtures(mv={"preserve": False})
        probe.run_probe(manifest, "parity", expect_dropped=True)
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(reference.namespaces, {})

    def test_ordinary_view_drift_is_not_normalized_away(self):
        stock, mv, manifest = self.fixtures(mv={"preserve": True, "ordinary_drift": True})
        with self.assertRaisesRegex(probe.ContractFailure, "ordinary view or table behavior differs"):
            probe.run_probe(manifest, "contract")
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})

    def test_lifecycle_refuses_shared_or_foreign_projects_before_any_requests(self):
        for shared, project in ((True, "nr-isolated-rest-test"), (False, "nr-fx-shared"), ("false", "nr-isolated-rest-test")):
            with self.subTest(shared=shared, project=project):
                manifest = {"ready": True, "shared_docker": shared, "compose_project": project}
                with mock.patch.object(probe, "Catalog") as catalog, mock.patch.object(probe, "docker") as docker:
                    with self.assertRaisesRegex(probe.EnvironmentFailure, "requires an isolated"):
                        probe.run_probe(manifest, "lifecycle")
                catalog.assert_not_called()
                docker.assert_not_called()

    def test_lifecycle_reads_metadata_and_recreates_only_mv_from_saved_compose(self):
        stock, mv, manifest = self.fixtures()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "compose.yml").write_text("saved fixture\n")
            (root / "compose.env").write_text("saved environment\n")
            manifest.update(shared_docker=False, compose_project="nr-isolated-rest-probe-test",
                            compose_file=str(root / "compose.yml"), compose_env=str(root / "compose.env"))
            commands = []
            def docker(command, *, binary=False):
                commands.append(command)
                if "run" in command:
                    self.assertTrue(binary)
                    namespace = next(iter(mv.namespaces.values()))
                    return json.dumps(namespace["views"]["storage_view"]["metadata"]).encode()
                mv.config_statuses.append(503)
                return ""
            with mock.patch.object(probe, "docker", side_effect=docker):
                probe.run_probe(manifest, "lifecycle")
            self.assertEqual(len(commands), 2)
            self.assertIn(str(root / "compose.yml"), commands[0])
            self.assertIn(str(root / "compose.env"), commands[0])
            self.assertIn("mc cat", commands[0][-1])
            self.assertEqual(commands[1][-5:], ["up", "-d", "--no-deps", "--force-recreate", "rest-mv"])
        invalid = [body for method, _, body in mv.calls if method == "POST" and body and body.get("name", "").startswith("invalid_view_")]
        self.assertEqual(len(invalid), 2)
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})

    def test_lifecycle_readiness_does_not_hide_non_transient_http_contract_errors(self):
        client = mock.Mock()
        client.warehouse = "s3://warehouse/test/rest-mv"
        client.request.side_effect = probe.HTTPStatusFailure(404, "missing config endpoint")
        response = {"metadata": {"view-uuid": "test"}, "metadata-location": "s3://warehouse/test/v1.metadata.json"}
        with mock.patch.object(probe, "isolated_compose", return_value=["docker", "compose"]), mock.patch.object(
            probe, "docker", side_effect=[json.dumps(response["metadata"]).encode(), ""]
        ), mock.patch.object(probe.time, "sleep") as sleep:
            with self.assertRaisesRegex(probe.HTTPStatusFailure, "missing config endpoint"):
                probe.lifecycle({}, client, response)
        sleep.assert_not_called()

    def test_metadata_reads_plain_and_both_iceberg_gzip_filename_forms(self):
        metadata = {"view-uuid": "test", "versions": [{"storage-table": {"name": "storage", "namespace": ["ns"]}}]}
        plain = json.dumps(metadata).encode()
        for filename, content in (("v1.metadata.json", plain),
                                  ("v1.gz.metadata.json", gzip.compress(plain)),
                                  ("v1.metadata.json.gz", gzip.compress(plain))):
            with self.subTest(filename=filename):
                self.assertEqual(probe.metadata_json(content, "s3://warehouse/test/" + filename), metadata)

    def test_metadata_rejects_corrupt_gzip_and_codec_mismatch_without_fallback(self):
        compressed = gzip.compress(b'{"view-uuid":"test"}')
        for filename, content in (("v1.gz.metadata.json", compressed[:-4]),
                                  ("v1.gz.metadata.json", b'{}'),
                                  ("v1.metadata.json", compressed)):
            with self.subTest(filename=filename, content=content):
                with self.assertRaisesRegex(probe.ContractFailure, "persisted metadata is not valid"):
                    probe.metadata_json(content, "s3://warehouse/test/" + filename)

    def test_docker_captures_binary_metadata_without_text_decoding(self):
        compressed = gzip.compress(b'{"view-uuid":"test"}')
        result = mock.Mock(returncode=0, stdout=compressed)
        with mock.patch.object(probe.subprocess, "run", return_value=result) as run:
            self.assertEqual(probe.docker(["docker", "compose", "run", "mc"], binary=True), compressed)
        self.assertFalse(run.call_args.kwargs["text"])

    def test_main_exit_codes_distinguish_contract_and_environment_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text("{}")
            for failure, code in ((None, 0), (probe.ContractFailure("bad contract"), 1), (probe.EnvironmentFailure("bad fixture"), 2)):
                with self.subTest(code=code), mock.patch.object(probe, "run_probe", side_effect=failure), mock.patch("sys.stdout", new_callable=io.StringIO), mock.patch("sys.stderr", new_callable=io.StringIO):
                    self.assertEqual(probe.main(["contract", "--manifest", str(path)]), code)


if __name__ == "__main__":
    unittest.main()
