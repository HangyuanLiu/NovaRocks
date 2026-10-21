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

"""Shared bounded Docker, file integrity and private fixture operations."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from typing import Any, Callable
import uuid


SCRIPT_DIR = Path(__file__).resolve().parent
ENTRY_FILES = (
    "env.sh", "manifest.json", "README.md", "fe.toml", "be.toml",
    "sql-test.toml", "ice-rest-catalog.sql", "spark-defaults.conf",
    "spark-iceberg-v3-smoke.sql",
)
ENVIRONMENT_KEYS = {
    "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SSL_CERT_FILE",
    "DOCKER_CONTEXT", "DOCKER_HOST", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH",
    "DOCKER_CONFIG", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME",
    "NOVA_FIXTURE_STORE",
}


class RuntimeFailure(RuntimeError):
    def __init__(self, code: str, detail: str):
        self.code = code
        super().__init__(f"{code}: {detail}")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def rest_services(record: dict[str, Any]) -> list[str]:
    return [service for service in ("rest", "rest-mv") if service in record["images"]]


def server_warehouse_root(record: dict[str, Any]) -> str:
    return record["server_warehouse"].rsplit("/", 1)[0] + "/"


def server_warehouse(record: dict[str, Any], service: str) -> str:
    return server_warehouse_root(record) + service


def controlled_environment(source: dict[str, str] | None = None) -> dict[str, str]:
    source = os.environ if source is None else source
    return {key: value for key, value in source.items() if key in ENVIRONMENT_KEYS}


def install_signal_handlers() -> None:
    def cancel(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, cancel)


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path: Path, value: Any) -> None:
    atomic_bytes(path, canonical(value) + b"\n")


def atomic_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    try:
        with temporary.open("xb") as out:
            os.chmod(temporary, 0o600)
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise RuntimeFailure("RuntimeStateUnreadable", str(path)) from error
    if not isinstance(value, dict):
        raise RuntimeFailure("RuntimeStateUnreadable", str(path))
    return value


@contextmanager
def file_lock(path: Path, *, shared: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def safe_component(value: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9_.:-]+", value):
        raise RuntimeFailure("InvalidIdentity", value)
    return value


class Docker:
    """Bounded Docker operations; all Compose inputs are owner-saved definitions."""

    def __init__(self, timeout: float = 90, environment: dict[str, str] | None = None):
        self.timeout = timeout
        self.environment = controlled_environment(environment)

    def command(self, args: list[str], *, absent_ok: bool = False) -> str:
        deadline = getattr(self, "operation_deadline", None)
        timeout = self.timeout if deadline is None else min(self.timeout, deadline - time.monotonic())
        if timeout <= 0:
            raise RuntimeFailure("RunBudgetExpired", "original command deadline expired")
        try:
            process = subprocess.Popen(
                ["docker", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=self.environment, start_new_session=True,
            )
        except OSError as error:
            raise RuntimeFailure("DockerUnavailable", "cannot start Docker CLI") from error
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException as error:
            # Cancellation must settle our child before releasing ownership locks.
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.communicate(timeout=2)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
            if isinstance(error, subprocess.TimeoutExpired):
                raise RuntimeFailure("DockerOperationTimeout", args[0]) from error
            raise
        return self.command_result(args, process.returncode, stdout, stderr, absent_ok=absent_ok)

    @staticmethod
    def command_result(args, returncode, stdout, stderr, *, absent_ok=False):
        if returncode:
            missing_object = re.search(r"no such (object|container|image|volume|network)\b", stderr.lower())
            daemon_absence = "error response from daemon" in stderr.lower() and (
                "not found" in stderr.lower() or "is not connected" in stderr.lower()
            )
            if absent_ok and (missing_object or daemon_absence):
                return ""
            # Do not print commands containing fixture credentials or cleanup URLs.
            code = "PortUnavailable" if any(term in stderr.lower() for term in (
                "port is already allocated", "address already in use",
            )) else "DockerOperationFailed"
            raise RuntimeFailure(code, f"{args[0]} exited with {returncode}")
        return stdout.strip()

    def daemon_id(self) -> str:
        endpoint = self.environment.get("DOCKER_HOST", "")
        if not endpoint:
            context = self.environment.get("DOCKER_CONTEXT") or self.command(["context", "show"])
            raw = self.command(["context", "inspect", context, "--format", "{{json .Endpoints.docker.Host}}"])
            try:
                endpoint = json.loads(raw)
            except ValueError as error:
                raise RuntimeFailure("UnsupportedDaemon", "invalid Docker context endpoint") from error
        if endpoint and not endpoint.startswith(("unix://", "tcp://127.0.0.1:", "tcp://localhost:")):
            raise RuntimeFailure("UnsupportedDaemon", "shared fixtures require a local daemon")
        identity = self.command(["info", "--format", "{{.ID}}"])
        if not identity:
            raise RuntimeFailure("DockerUnavailable", "daemon has no identity")
        return safe_component(identity)

    def inspect(self, kind: str, identity: str, *, absent_ok: bool = False) -> dict[str, Any] | None:
        output = self.command([kind, "inspect", identity, "--format", "{{json .}}"], absent_ok=absent_ok)
        if not output:
            return None
        try:
            value = json.loads(output)
        except ValueError as error:
            raise RuntimeFailure("RuntimeIdentityMismatch", "invalid Docker inspect result") from error
        if not isinstance(value, dict):
            raise RuntimeFailure("RuntimeIdentityMismatch", "invalid Docker inspect result")
        return value

    def image_id(self, reference: str) -> str:
        value = self.inspect("image", reference)
        if not value or not value.get("Id"):
            raise RuntimeFailure("RuntimeIdentityMismatch", "missing provisioned image")
        return value["Id"]

    def compose(self, record: dict[str, Any], args: list[str]) -> str:
        return self.command([
            "compose", "--env-file", record["compose_env"], "-p", record["project"],
            "-f", record["compose_file"], *args,
        ])

    def container(self, record: dict[str, Any], service: str) -> dict[str, Any] | None:
        ids = self.command([
            "ps", "-a", "--filter", f"label=com.docker.compose.project={record['project']}",
            "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.ID}}",
        ]).splitlines()
        if not ids:
            return None
        if len(ids) != 1:
            raise RuntimeFailure("RuntimeIdentityMismatch", f"multiple {service} containers")
        info = self.inspect("container", ids[0])
        labels = (info.get("Config") or {}).get("Labels") or {}
        if info.get("Image") != record["images"][service]["image_id"] or any(
            labels.get(key) != value for key, value in self.labels(record).items()
        ):
            raise RuntimeFailure("RuntimeIdentityMismatch", f"{service} does not match saved definition")
        port = record["service_ports"].get(service)
        if port:
            bindings = (info.get("HostConfig") or {}).get("PortBindings") or {}
            for container_port, host_port in port.items():
                if not any(item.get("HostPort") == str(host_port) for item in bindings.get(container_port, [])):
                    raise RuntimeFailure("RuntimeIdentityMismatch", f"{service} port mismatch")
        return info

    @staticmethod
    def labels(record: dict[str, Any]) -> dict[str, str]:
        return {
            "novarocks.fixture.owner": record["namespace"],
            "novarocks.fixture.key": record["key"],
            "novarocks.fixture.kind": record["kind"],
        }

    def http_ready(self, url):
        # Use the same cumulative socket owner as scratch REST enumeration;
        # progress while reading headers cannot renew the caller's clock.
        from scratch import _DeadlineConnection
        from urllib.parse import urlsplit
        target = urlsplit(url)
        if (target.scheme != "http" or target.hostname not in {"localhost", "127.0.0.1"}
                or target.username or target.password or target.fragment):
            return False
        deadline = min(time.monotonic() + 2, getattr(self, "operation_deadline", None) or float("inf"))
        connection = _DeadlineConnection(target.port or 80, deadline, time.monotonic)
        try:
            connection.request("GET", target.path or "/")
            return connection.getresponse().status == 200
        except (OSError, ValueError):
            return False
        finally:
            connection.close()

    @staticmethod
    def service_aliases(service: str) -> list[str]:
        return ["minio", "warehouse.minio", "novarocks.minio"] if service == "minio" else [service]

    def own_network(self, record: dict[str, Any]) -> dict[str, Any] | None:
        network = self.inspect("network", record["network"], absent_ok=True)
        if network and any((network.get("Labels") or {}).get(key) != value
                           for key, value in self.labels(record).items()):
            raise RuntimeFailure("RuntimeIdentityMismatch", "foreign service network")
        return network

    def service_connected(self, record: dict[str, Any], service: str,
                          container: dict[str, Any], network: dict[str, Any]) -> bool:
        endpoint = ((container.get("NetworkSettings") or {}).get("Networks") or {}).get(record["network"])
        return bool(endpoint and endpoint.get("NetworkID") == network.get("Id")
                    and container["Id"] in (network.get("Containers") or {})
                    and set(self.service_aliases(service)).issubset(endpoint.get("Aliases") or []))

    def repair_service_networks(self, record: dict[str, Any]) -> None:
        network = self.own_network(record)
        if not network:
            raise RuntimeFailure("RuntimeStateUnreadable", "saved service network is missing")
        for service in record["required_services"]:
            container = self.container(record, service)
            # Completed initialization containers do not retain live endpoints.
            if not container or not (container.get("State") or {}).get("Running"):
                continue
            if self.service_connected(record, service, container, network):
                continue
            identity = container["Id"]
            endpoints = (container.get("NetworkSettings") or {}).get("Networks") or {}
            if record["network"] in endpoints or identity in (network.get("Containers") or {}):
                self.command(["network", "disconnect", record["network"], identity], absent_ok=True)
            aliases = [argument for alias in self.service_aliases(service) for argument in ("--alias", alias)]
            self.command(["network", "connect", *aliases, record["network"], identity])
            network = self.own_network(record)
            if not network:
                raise RuntimeFailure("RuntimeStateUnreadable", "saved service network disappeared")

    def healthy(self, record: dict[str, Any]) -> bool:
        network = self.own_network(record)
        if not network:
            return False
        for service in record["required_services"]:
            info = self.container(record, service)
            if not info:
                return False
            state = info.get("State") or {}
            if service == "mc-init":
                if state.get("Status") != "exited" or state.get("ExitCode") != 0:
                    return False
            elif not state.get("Running") or not self.service_connected(record, service, info, network):
                return False
        for url in record["health_urls"]:
            if not self.http_ready(url):
                return False
        if record["kind"] == "cat":
            network = self.inspect("network", record["network"], absent_ok=True)
            if not network or record.get("object_store_container") not in (network.get("Containers") or {}):
                return False
        return True

    def attach(self, os_record: dict[str, Any], cat: dict[str, Any]) -> str:
        container = self.container(os_record, "minio")
        if not container:
            raise RuntimeFailure("DockerUnavailable", "object store has no container")
        identity = container["Id"]
        network = self.inspect("network", cat["network"])
        if identity not in (network.get("Containers") or {}):
            self.command(["network", "connect", "--alias", "minio", "--alias", "warehouse.minio",
                          "--alias", "novarocks.minio", cat["network"], identity])
        return identity

    def validate_resources(self, record: dict[str, Any]) -> None:
        for service in record["images"]:
            self.container(record, service)
        networks = [record["network"]] if record.get("network_owned", True) else []
        if not record.get("network_owned", True):
            network = self.inspect("network", record["network"])
            if not network or (record.get("network_id") and network["Id"] != record["network_id"]):
                raise RuntimeFailure("RuntimeIdentityMismatch", "external consumer network changed")
        for kind, identities in (("network", networks), ("volume", record["volumes"])):
            for identity in identities:
                info = self.inspect(kind, identity, absent_ok=True)
                if info and any((info.get("Labels") or {}).get(key) != value
                                for key, value in self.labels(record).items()):
                    raise RuntimeFailure("RuntimeIdentityMismatch", f"foreign {kind}")

    def disconnect(self, record: dict[str, Any], identity: str) -> None:
        network = self.inspect("network", record["network"], absent_ok=True)
        if network and identity in (network.get("Containers") or {}):
            self.command(["network", "disconnect", record["network"], identity], absent_ok=True)

    def connect(self, record: dict[str, Any], identity: str, alias: str) -> None:
        info = self.inspect("container", identity)
        if not info or info.get("Id") != identity:
            raise RuntimeFailure("RuntimeIdentityMismatch", "consumer requires a full container ID")
        network = self.inspect("network", record["network"])
        if identity not in (network.get("Containers") or {}):
            self.command(["network", "connect", "--alias", alias, record["network"], identity])

    def delete_resources(self, record: dict[str, Any]) -> None:
        self.validate_resources(record)
        self.compose(record, ["down", "-v"])
        if self.command(["ps", "-a", "--filter", f"label=com.docker.compose.project={record['project']}", "-q"]):
            raise RuntimeFailure("ResourcesStillPresent", record["id"])
        if record.get("network_owned", True) and self.inspect("network", record["network"], absent_ok=True):
            raise RuntimeFailure("ResourcesStillPresent", record["network"])
        for volume in record["volumes"]:
            if self.inspect("volume", volume, absent_ok=True):
                raise RuntimeFailure("ResourcesStillPresent", volume)




def endpoints(record: dict[str, Any]) -> dict[str, Any]:
    """Project host/container addresses from one machine or private snapshot."""
    from machine_runtime import endpoints as project_endpoints
    return project_endpoints(record)


def main() -> int:
    from machine_runtime import main as machine_main
    return machine_main()


if __name__ == "__main__":
    sys.modules.setdefault("fixture_runtime", sys.modules[__name__])
    raise SystemExit(main())
