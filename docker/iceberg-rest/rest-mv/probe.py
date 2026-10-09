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

"""Probe the storage-table REST contract using the saved fixture manifest."""

from __future__ import annotations

import argparse
import copy
import gzip
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib


class ContractFailure(RuntimeError):
    """The reachable service did not satisfy the fixture contract."""


class EnvironmentFailure(RuntimeError):
    """The requested fixture or its saved definition is unavailable."""


class HTTPStatusFailure(ContractFailure):
    def __init__(self, status: int, detail: str):
        self.status = status
        super().__init__(detail)


def require(condition: bool, detail: str) -> None:
    if not condition:
        raise ContractFailure(detail)


def schema() -> dict:
    return {"type": "struct", "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "required": False, "type": "long"}]}


def version(namespace: str, *, sql: str = "SELECT 1 AS id") -> dict:
    return {"version-id": 1, "timestamp-ms": int(time.time() * 1000), "schema-id": 0,
            "summary": {}, "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}],
            "default-namespace": [namespace]}


def current_version(metadata: dict) -> dict:
    selected = [item for item in metadata.get("versions", [])
                if item.get("version-id") == metadata.get("current-version-id")]
    require(len(selected) == 1, "view metadata has no unique current version")
    return selected[0]


def normalized(value):
    """Remove only nondeterministic identity, time, location and build versions."""
    ignored = {"view-uuid", "table-uuid", "location", "metadata-location", "metadata-file",
               "timestamp-ms", "last-updated-ms", "engine-version", "iceberg-version"}
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in value.items() if key not in ignored}
    if isinstance(value, list):
        return [normalized(item) for item in value]
    return value


class Catalog:
    def __init__(self, label: str, endpoint: dict):
        self.label = label
        self.uri = endpoint["uri"].rstrip("/")
        self.warehouse = endpoint["warehouse"].rstrip("/")
        # Generated fixture endpoints are local; ambient/system proxies must
        # not redirect their health and contract requests.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.prefix = ""
        self.resources: list[str] = []
        self.namespace_path: str | None = None
        config = self.request("GET", "v1/config?" + urllib.parse.urlencode({"warehouse": self.warehouse}))
        properties = {**config.get("defaults", {}), "warehouse": self.warehouse, **config.get("overrides", {})}
        prefix = properties.get("prefix", "")
        if not isinstance(prefix, str):
            raise EnvironmentFailure(f"{label}: invalid REST config prefix")
        self.prefix = prefix.strip("/")

    def request(self, method: str, path: str, payload=None, *, expected=(200,)) -> dict:
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(self.uri + "/" + path, data=data, method=method,
                                         headers={"Accept": "application/json", "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=15) as response:
                status, content = response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                status, content = error.code, error.read()
        except (OSError, urllib.error.URLError) as error:
            raise EnvironmentFailure(f"{self.label}: REST request unavailable") from error
        if status not in expected:
            raise HTTPStatusFailure(status, f"{self.label}: {method} {path.split('?')[0]} returned HTTP {status}, expected {expected}")
        if status not in (200, 201) or not content:
            return {}
        try:
            result = json.loads(content)
        except (ValueError, UnicodeError) as error:
            raise ContractFailure(f"{self.label}: REST response is not JSON") from error
        require(isinstance(result, dict), f"{self.label}: REST response must be an object")
        return result

    def create_namespace(self, namespace: str) -> None:
        base = "v1/" + (urllib.parse.quote(self.prefix, safe="/") + "/" if self.prefix else "") + "namespaces"
        self.namespace_path = base + "/" + urllib.parse.quote(namespace, safe="")
        self.request("POST", base, {"namespace": [namespace], "properties": {}}, expected=(200, 201))

    def create_view(self, name: str, view_version: dict) -> dict:
        path = self.namespace_path + "/views/" + name
        self.resources.append(path)
        result = self.request("POST", self.namespace_path + "/views", {
            "name": name, "location": self.warehouse + "/" + view_version["default-namespace"][0] + "/" + name,
            "schema": schema(), "view-version": view_version, "properties": {},
        }, expected=(200, 201))
        require("metadata" in result, f"{self.label}: create view {name} omitted metadata")
        loaded = self.request("GET", path)
        require(result["metadata"] == loaded.get("metadata"), f"{self.label}: create/load view {name} disagree")
        return loaded

    def replace_view(self, name: str, previous: dict, replacement: dict) -> dict:
        path = self.namespace_path + "/views/" + name
        result = self.request("POST", path, {
            "requirements": [{"type": "assert-view-uuid", "uuid": previous["view-uuid"]}],
            "updates": [{"action": "add-view-version", "view-version": replacement},
                        {"action": "set-current-view-version", "view-version-id": -1}],
        })
        loaded = self.request("GET", path)
        require(result.get("metadata") == loaded.get("metadata"), f"{self.label}: replace/load view {name} disagree")
        return loaded

    def ordinary(self, namespace: str) -> list[dict]:
        initial = self.create_view("ordinary_view", version(namespace))["metadata"]
        replacement = copy.deepcopy(current_version(initial))
        replacement.update({"version-id": initial["current-version-id"] + 1,
                            "timestamp-ms": int(time.time() * 1000)})
        replacement["representations"][0]["sql"] = "SELECT 2 AS id"
        changed = self.replace_view("ordinary_view", initial, replacement)["metadata"]
        require(current_version(changed)["representations"] == replacement["representations"],
                f"{self.label}: ordinary view replacement was lost")
        table_path = self.namespace_path + "/tables/ordinary_table"
        self.resources.append(table_path)
        table = self.request("POST", self.namespace_path + "/tables", {
            "name": "ordinary_table", "schema": schema(), "properties": {"format-version": "2"},
            "location": self.warehouse + "/" + namespace + "/ordinary_table",
        }, expected=(200, 201))["metadata"]
        require(self.request("GET", table_path).get("metadata") == table,
                f"{self.label}: create/load table disagree")
        next_schema = schema()
        next_schema["schema-id"] = table["current-schema-id"] + 1
        next_schema["fields"].append({"id": 2, "name": "note", "required": False, "type": "string"})
        updated = self.request("POST", table_path, {
            "requirements": [{"type": "assert-table-uuid", "uuid": table["table-uuid"]}],
            "updates": [{"action": "add-schema", "schema": next_schema},
                        {"action": "set-current-schema", "schema-id": -1},
                        {"action": "set-properties", "updates": {"probe-phase": "replacement"}}],
        })["metadata"]
        require(updated["current-schema-id"] != table["current-schema-id"]
                and updated["properties"].get("probe-phase") == "replacement",
                f"{self.label}: ordinary table replacement was lost")
        require(self.request("GET", table_path).get("metadata") == updated,
                f"{self.label}: replace/load table disagree")
        return [initial, changed, table, updated]

    def storage_contract(self, namespace: str, *, preserved: bool) -> dict:
        first_pointer = {"namespace": [namespace], "name": "storage_one"}
        sample = version(namespace)
        sample["storage-table"] = first_pointer
        first = self.create_view("storage_view", sample)
        initial = first["metadata"]
        if not preserved:
            require("storage-table" not in current_version(initial), f"{self.label}: unsupported storage-table was retained")
            return first
        require(current_version(initial).get("storage-table") == first_pointer,
                f"{self.label}: storage-table was lost or changed")
        second_pointer = {"namespace": [namespace], "name": "storage_two"}
        replacement = copy.deepcopy(current_version(initial))
        replacement.update({"version-id": initial["current-version-id"] + 1,
                            "timestamp-ms": int(time.time() * 1000), "storage-table": second_pointer})
        second = self.replace_view("storage_view", initial, replacement)
        updated = second["metadata"]
        require(updated["current-version-id"] > initial["current-version-id"],
                f"{self.label}: pointer-only replacement did not advance the view version")
        require(updated["view-uuid"] == initial["view-uuid"], f"{self.label}: view UUID changed")
        require(len(updated["versions"]) == 2, f"{self.label}: pointer-only replacement lost view history")
        require(current_version(updated).get("storage-table") == second_pointer
                and any(item["version-id"] == initial["current-version-id"] and item.get("storage-table") == first_pointer
                        for item in updated["versions"]), f"{self.label}: storage-table history is incorrect")
        null_version = version(namespace)
        null_version["storage-table"] = None
        null_view = self.create_view("null_view", null_version)["metadata"]
        require("storage-table" not in current_version(null_view), f"{self.label}: null storage-table was not omitted")
        return second

    def invalid_shapes(self, namespace: str) -> None:
        for index, pointer in enumerate(({"namespace": [namespace]}, {"namespace": namespace, "name": "storage"})):
            name = "invalid_view_" + str(index)
            self.resources.append(self.namespace_path + "/views/" + name)
            sample = version(namespace)
            sample["storage-table"] = pointer
            self.request("POST", self.namespace_path + "/views", {
                "name": name, "schema": schema(), "view-version": sample, "properties": {},
            }, expected=(400,))

    def cleanup(self) -> None:
        paths = list(reversed(self.resources)) + ([self.namespace_path] if self.namespace_path else [])
        for path in paths:
            try:
                self.request("DELETE", path, expected=(200, 204, 404))
            except (ContractFailure, EnvironmentFailure):
                print(f"WARNING: {self.label}: cleanup failed for {path}", file=sys.stderr)


def isolated_compose(manifest: dict) -> list[str]:
    project = manifest.get("compose_project", "")
    if manifest.get("shared_docker") is not False or not isinstance(project, str) or not project.startswith("nr-isolated-rest-"):
        raise EnvironmentFailure("lifecycle requires an isolated nr-isolated-rest-* fixture")
    paths = [manifest.get("compose_file"), manifest.get("compose_env")]
    if any(not isinstance(path, str) or not Path(path).is_absolute() or not Path(path).is_file() for path in paths):
        raise EnvironmentFailure("lifecycle requires the saved Compose file and environment")
    return ["docker", "compose", "--project-name", project, "--file", paths[0], "--env-file", paths[1]]


def docker(command: list[str], *, binary: bool = False) -> str | bytes:
    allowed = {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "DOCKER_CONTEXT", "DOCKER_HOST",
               "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "DOCKER_CONFIG", "XDG_CONFIG_HOME"}
    try:
        result = subprocess.run(command, env={key: value for key, value in os.environ.items() if key in allowed},
                                capture_output=True, text=not binary, timeout=120, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise EnvironmentFailure("lifecycle Docker operation unavailable") from error
    if result.returncode:
        raise EnvironmentFailure("lifecycle Docker operation failed")
    return result.stdout


def metadata_json(content: bytes, location: str) -> dict:
    """Follow Iceberg's filename-based NONE/GZIP metadata codec selection."""
    name = urllib.parse.urlparse(location).path
    if ".metadata.json" not in name:
        raise ContractFailure("rest-mv: persisted metadata has an invalid filename")
    prefix = name[:name.rfind(".metadata.json")]
    compressed = name.endswith(".metadata.json.gz") or prefix.endswith(".gz")
    try:
        if compressed:
            content = gzip.decompress(content)
        metadata = json.loads(content)
    except (OSError, EOFError, ValueError, zlib.error) as error:
        raise ContractFailure("rest-mv: persisted metadata is not valid "
                              + ("gzip JSON" if compressed else "JSON")) from error
    require(isinstance(metadata, dict), "rest-mv: persisted metadata must be an object")
    return metadata


def lifecycle(manifest: dict, client: Catalog, response: dict) -> None:
    command = isolated_compose(manifest)
    metadata = response["metadata"]
    location = response.get("metadata-location", "")
    if not isinstance(location, str) or not location.startswith("s3://"):
        raise ContractFailure("rest-mv: view metadata location is not in object storage")
    script = ('mc alias set store http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
              'mc cat ' + shlex.quote("store/" + location.removeprefix("s3://")))
    content = docker(command + ["run", "--rm", "--no-deps", "mc", "-c", script], binary=True)
    persisted = metadata_json(content, location)
    require(persisted == metadata, "rest-mv: object metadata and REST response disagree")
    # storage_contract already checked the two JSON pointer samples against metadata.
    docker(command + ["up", "-d", "--no-deps", "--force-recreate", "rest-mv"])
    deadline = time.monotonic() + 120
    while True:
        try:
            client.request("GET", "v1/config?" + urllib.parse.urlencode({"warehouse": client.warehouse}))
            break
        except (EnvironmentFailure, HTTPStatusFailure) as error:
            if isinstance(error, HTTPStatusFailure) and error.status != 503:
                raise
            if time.monotonic() >= deadline:
                raise EnvironmentFailure("rest-mv: recreated service did not become ready")
            time.sleep(0.2)
    reloaded = client.request("GET", client.namespace_path + "/views/storage_view")["metadata"]
    require(reloaded == metadata, "rest-mv: view UUID or version history changed after container recreation")


def run_probe(manifest: dict, mode: str, *, expect_dropped: bool = False) -> None:
    if not isinstance(manifest, dict) or manifest.get("ready") is not True:
        raise EnvironmentFailure("fixture manifest is not ready")
    if mode == "lifecycle":
        isolated_compose(manifest)
    if expect_dropped and mode != "parity":
        raise EnvironmentFailure("--expect-dropped is only valid with parity")
    for key in ("iceberg_rest", "iceberg_rest_mv"):
        endpoint = manifest.get(key)
        if not isinstance(endpoint, dict) or any(not isinstance(endpoint.get(field), str) or not endpoint[field] for field in ("uri", "warehouse")):
            raise EnvironmentFailure(f"fixture manifest is missing {key} endpoints")
        uri = urllib.parse.urlparse(endpoint["uri"])
        if uri.scheme not in ("http", "https") or not uri.netloc or uri.username or uri.password:
            raise EnvironmentFailure(f"fixture manifest has an invalid {key} URI")
    namespace = "nr_probe_" + uuid.uuid4().hex[:12]
    clients: list[Catalog] = []
    try:
        results = []
        for label, key in (("rest", "iceberg_rest"), ("rest-mv", "iceberg_rest_mv")):
            client = Catalog(label, manifest[key])
            clients.append(client)
            client.create_namespace(namespace)
            results.append(client.ordinary(namespace))
            response = client.storage_contract(namespace, preserved=label == "rest-mv" and not expect_dropped)
            if mode == "lifecycle" and label == "rest-mv":
                client.invalid_shapes(namespace)
                lifecycle(manifest, client, response)
        require(normalized(results[0]) == normalized(results[1]), "rest/rest-mv: ordinary view or table behavior differs")
    except (KeyError, TypeError, ValueError) as error:
        raise ContractFailure("REST response contains malformed view or table metadata") from error
    finally:
        for client in reversed(clients):
            client.cleanup()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("contract", "lifecycle", "parity"))
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--expect-dropped", action="store_true")
    args = parser.parse_args(argv)
    try:
        try:
            manifest = json.loads(args.manifest.read_text())
        except (OSError, ValueError) as error:
            raise EnvironmentFailure("cannot read fixture manifest") from error
        run_probe(manifest, args.mode, expect_dropped=args.expect_dropped)
    except ContractFailure as error:
        print(f"REST MV CONTRACT FAILED: {error}", file=sys.stderr)
        return 1
    except (EnvironmentFailure, KeyError, TypeError, ValueError) as error:
        detail = str(error) if isinstance(error, EnvironmentFailure) else "malformed fixture response or manifest"
        print(f"REST MV ENVIRONMENT ERROR: {detail}", file=sys.stderr)
        return 2
    print(f"REST MV {args.mode.upper()} PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
