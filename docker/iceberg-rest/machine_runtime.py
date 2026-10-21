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

"""One explicitly maintained fixture stack per local Docker daemon."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import fcntl
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
from typing import Any
import uuid

import fixture_runtime as runtime

PROTOCOL = 1
PROJECT = "nr-fixture-machine"
NETWORK = PROJECT + "_iceberg_net"
DEFAULT_PORTS = dict(minio=28000, minio_console=28001, rest=28002,
                     rest_mv=28003, spark=28004, hms=28005)
DEFAULT_POLICY = dict(scratch_ttl_seconds=604800, max_run_seconds=86400,
                      cleanup_grace_seconds=300)
BLOCKED = {"FixturePrerequisiteMissing", "MachineUnavailable", "MachineVersionMismatch",
           "MachineMaintaining", "RunBudgetRejected"}


def endpoints(snapshot: dict[str, Any]) -> dict[str, str]:
    ports = snapshot["ports"]
    result = {
        "minio_endpoint": f"http://127.0.0.1:{ports['minio']}",
        "minio_console": f"http://127.0.0.1:{ports['minio_console']}",
        "rest_uri": f"http://127.0.0.1:{ports['rest']}",
        "spark_ui": f"http://127.0.0.1:{ports['spark']}",
        "container_minio_endpoint": "http://minio:9000",
        "container_rest_uri": "http://rest:8181",
    }
    if "rest-mv" in snapshot["images"]:
        result.update(rest_mv_uri=f"http://127.0.0.1:{ports['rest_mv']}",
                      container_rest_mv_uri="http://rest-mv:8181")
    return result


def machine_config(value: dict[str, Any] | None = None) -> dict[str, Any]:
    value = copy.deepcopy(value or {})
    unknown = set(value) - {"ports", "policy", "benchmark_root", "minio_volume"}
    if unknown:
        raise runtime.RuntimeFailure("InvalidConfiguration", "unknown machine configuration fields")
    ports = {**DEFAULT_PORTS, **value.get("ports", {})}
    policy = {**DEFAULT_POLICY, **value.get("policy", {})}
    if set(ports) != set(DEFAULT_PORTS) or any(type(p) is not int or not 0 < p < 65536 for p in ports.values()) or len(set(ports.values())) != len(ports):
        raise runtime.RuntimeFailure("InvalidConfiguration", "machine ports must be distinct valid integers")
    if set(policy) != set(DEFAULT_POLICY) or any(type(n) is not int or n <= 0 for n in policy.values()):
        raise runtime.RuntimeFailure("InvalidConfiguration", "invalid scratch lifetime policy")
    if policy["scratch_ttl_seconds"] <= 2 * (policy["max_run_seconds"] + policy["cleanup_grace_seconds"]):
        raise runtime.RuntimeFailure("InvalidConfiguration", "scratch TTL must exceed twice the complete run budget and cleanup reserve")
    if policy["max_run_seconds"] <= policy["cleanup_grace_seconds"]:
        raise runtime.RuntimeFailure("InvalidConfiguration", "cleanup reserve exhausts the run budget")
    from scratch import canonical_s3
    try:
        benchmark = canonical_s3(value.get("benchmark_root", "s3://novarocks/shared/benchmarks"))
        if "/" not in benchmark[5:]:
            raise runtime.RuntimeFailure("InvalidScratchScope", "benchmark root has no prefix")
    except runtime.RuntimeFailure as error:
        raise runtime.RuntimeFailure("InvalidConfiguration", "benchmark root requires a canonical s3://bucket/prefix") from error
    volume = value.get("minio_volume", PROJECT + "_minio-data")
    runtime.safe_component(volume)
    return dict(ports=ports, policy=policy, benchmark_root=benchmark, minio_volume=volume)


def input_facts(bom: dict[str, Any], backend: Any) -> tuple[dict, dict]:
    """Consume already verified BOM facts; actual IDs never come from a mutable tag."""
    if not bom.get("lock_sha256"):
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "verified input lock identity is missing")
    images, facts = {}, {"lock_sha256": bom["lock_sha256"]}
    for service, group, logical in (
        ("minio", "images", "minio"), ("mc", "images", "minio-mc"),
        ("rest", "images", "iceberg-rest"),
        ("spark", "derived_images", "iceberg-spark"),
        ("rest-mv", "derived_images", "rest-mv"),
    ):
        receipt = bom.get(group, {}).get(logical)
        if not isinstance(receipt, dict):
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", logical)
        reference = receipt.get("image_id") if group == "derived_images" else receipt.get("alias")
        if not reference:
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", logical)
        actual = backend.image_id(reference)
        if group == "derived_images" and actual != receipt["image_id"]:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "derived image differs from verified receipt")
        # Times and cache paths identify supply operations, not runtime behavior.
        semantic = {k: receipt[k] for k in (
            "manifest_digest", "platform", "definition_sha256", "image_id",
        ) if k in receipt}
        if group == "derived_images" and not semantic.get("definition_sha256"):
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "derived definition identity is missing")
        facts[logical] = {"image_id": actual, "receipt": semantic}
        images[service] = {"image_id": actual, "tag": actual, "receipt": copy.deepcopy(receipt)}
    images["mc-init"] = copy.deepcopy(images["mc"])
    return images, facts


def verified_bom(repo: Path, store: Path | None = None) -> dict:
    directory = repo / "docker/fixture-inputs"
    sys.path.insert(0, str(directory))
    try:
        spec = importlib.util.spec_from_file_location("machine_fixture_verify", directory / "verify.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from fixture_inputs import fixture_store
        return module.verify(fixture_store(str(store) if store else None), repo,
                             directory / "lock.json", consumer="iceberg-rest")
    except Exception as error:
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "local fixture BOM verification failed") from error
    finally:
        sys.path.remove(str(directory))


class MachineDocker(runtime.Docker):
    """Machine operations preserve the shared network and the selected MinIO volume."""

    @staticmethod
    def labels(record):
        if record.get("kind") != "machine":
            return runtime.Docker.labels(record)
        return {"novarocks.fixture.owner": record["namespace"],
                "novarocks.fixture.key": "machine", "novarocks.fixture.kind": "machine",
                "novarocks.fixture.control-root": record["control_root"]}

    def container(self, record, service):
        info = super().container(record, service)
        if info and record.get("kind") == "machine":
            labels = info.get("Config", {}).get("Labels", {})
            if labels.get("novarocks.fixture.version") != record["version"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine container version mismatch")
            if labels.get("novarocks.fixture.activation") != record["generation"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine container activation mismatch")
            expected = record.get("containers", {}).get(service)
            if expected and info["Id"] != expected:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine container was replaced outside maintenance")
        return info

    def validate_resources(self, record):
        if record.get("kind") != "machine":
            return super().validate_resources(record)
        self.validate_minio_volume(record)
        for service in record["images"]:
            self.container(record, service)
        self.own_network(record)
        if record.get("network_id"):
            network = self.own_network(record)
            if not network or network["Id"] != record["network_id"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine network was replaced")
        for name in record["volumes"]:
            volume = self.inspect("volume", name, absent_ok=True)
            if volume and name != record["config"]["minio_volume"] and any(
                volume.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()
            ):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "foreign machine state volume")

    def validate_absent(self, record):
        if self.command(["ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}", "-q"]):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "fixed machine project already exists without saved authority")
        if self.inspect("network", NETWORK, absent_ok=True):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "fixed machine network already exists without saved authority")
        if self.inspect("volume", PROJECT + "_authority", absent_ok=True):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine authority exists without saved state")
        volume = self.inspect("volume", record["config"]["minio_volume"], absent_ok=True)
        if volume and self.command(["ps", "--filter", "volume=" + record["config"]["minio_volume"], "-q"]):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "selected MinIO volume still has an active consumer")
        if volume and record["config"]["minio_volume"] == PROJECT + "_minio-data" and any(
            (volume.get("Labels") or {}).get(k) != v for k, v in self.labels(record).items()
        ):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "default MinIO volume has foreign ownership")

    def ensure_minio_volume(self, record):
        name = record["config"]["minio_volume"]
        if not self.inspect("volume", name, absent_ok=True):
            if name != PROJECT + "_minio-data":
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "selected existing MinIO volume is missing")
            args = ["volume", "create"]
            for key, value in self.labels(record).items():
                args += ["--label", f"{key}={value}"]
            self.command([*args, name])
        return self.minio_volume_identity(record)

    def minio_volume_identity(self, record):
        """Freeze only native stable facts, never usage or mountpoint observations."""
        name = record["config"]["minio_volume"]
        info = self.inspect("volume", name, absent_ok=True)
        if not info:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "preserved MinIO volume is missing")
        if (info.get("Name") != name or any(not isinstance(info.get(key), str) or not info[key]
                for key in ("CreatedAt", "Driver", "Scope")) or any(
                info.get(key) is not None and (not isinstance(info[key], dict) or any(
                    not isinstance(k, str) or not isinstance(v, str) for k, v in info[key].items()))
                for key in ("Options", "Labels"))):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "MinIO volume native identity is incomplete")
        if name == PROJECT + "_minio-data" and any(
                (info.get("Labels") or {}).get(k) != v for k, v in self.labels(record).items()):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "default MinIO volume has foreign ownership")
        return {key: copy.deepcopy(info.get(key)) for key in
                ("Name", "CreatedAt", "Driver", "Scope", "Options", "Labels")}

    def validate_minio_volume(self, record):
        expected = record.get("minio_volume_identity")
        if not expected or self.minio_volume_identity(record) != expected:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "preserved MinIO volume native identity changed")

    def prepare_minio_volume(self, record):
        # No writer is mounted here. The owner durably saves these facts before
        # any Compose activation or root rotation can mount the selected data.
        self.ensure_authority_volume(record)
        if record.get("minio_volume_identity"):
            self.validate_minio_volume(record)
            return record["minio_volume_identity"]
        if record.get("steps", {}).get("activation_started"):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "started activation has no saved volume identity")
        return self.ensure_minio_volume(record)

    def ensure_authority_volume(self, record):
        # Volume creation is daemon-atomic. A different control directory cannot
        # race a second authority between discovery and Compose activation.
        name = PROJECT + "_authority"
        args = ["volume", "create"]
        for key, value in self.labels(record).items():
            args += ["--label", f"{key}={value}"]
        self.command([*args, name])
        marker = self.inspect("volume", name)
        if any(marker.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "another control directory owns this daemon's machine")

    def ensure_machine(self, record, previous=None):
        self.ensure_authority_volume(record)
        self.activation_preflight(record, previous)
        self.validate_minio_volume(record)
        self.compose(record, ["up", "-d", "--force-recreate", "minio", "mc-init", "rest", "rest-mv", "spark"])
        self.await_health(record)

    def activation_preflight(self, record, previous):
        """A saved activation intent authorizes its own partially created targets."""
        self.validate_minio_volume(record)
        for service in record["images"]:
            try:
                self.container(record, service)
            except runtime.RuntimeFailure:
                if not previous:
                    raise
                # A partially completed Compose switch may contain either exact
                # source containers or containers from this exact target intent.
                self.container(previous, service)
        network = self.own_network(record)
        expected = (previous or record).get("network_id")
        if expected and (not network or network["Id"] != expected):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation's shared network changed")
        for volume in record["volumes"]:
            info = self.inspect("volume", volume, absent_ok=True)
            if info and volume != record["config"]["minio_volume"] and any(
                info.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()
            ):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation has a foreign state volume")

    def await_health(self, record):
        # New activation is the only authorized place to acquire fresh exact IDs.
        record["containers"] = {}
        deadline = time.monotonic() + self.timeout
        while not self.healthy(record):
            if time.monotonic() >= deadline:
                raise runtime.RuntimeFailure("ReadinessTimeout", "machine services did not become healthy")
            time.sleep(0.1)
        record["containers"] = {s: self.container(record, s)["Id"] for s in record["required_services"]}
        record["network_id"] = self.own_network(record)["Id"]

    def stop_writers(self, record):
        self.validate_resources(record)
        self.compose(record, ["stop", "rest", "rest-mv", "spark"])

    def rotate_root(self, record, previous):
        # The durable fence intent authorizes replacing exactly this MinIO.
        # Repeating the same rotation must also recover a lost completion reply.
        expected = copy.deepcopy(previous)
        expected.get("containers", {}).pop("minio", None)
        self.validate_resources(expected)
        self.compose(record, ["up", "-d", "--force-recreate", "minio"])
        # Only MinIO was restarted. Other exact identities remain unchanged.
        record["containers"] = {k: v for k, v in previous.get("containers", {}).items() if k != "minio"}
        deadline = time.monotonic() + self.timeout
        while not self.http_ready(endpoints(record)["minio_endpoint"] + "/minio/health/live"):
            if time.monotonic() >= deadline:
                raise runtime.RuntimeFailure("ReadinessTimeout", "maintenance MinIO is unavailable")
            time.sleep(0.1)
        record["containers"]["minio"] = self.container(record, "minio")["Id"]

    def assert_old_root_revoked(self, previous):
        # Read through the saved old env file; never put credentials in argv.
        script = ('export MC_HOST_prior="http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@minio:9000"\n'
                  'set +e\nmc --json stat prior/warehouse 2>&1\nnr_fence_exit=$?\n'
                  'printf \'\\nNOVA_FIXTURE_FENCE_EXIT=%s\\n\' "$nr_fence_exit"\nexit 0')
        response = self.run_mc(previous, script)
        lines = response.splitlines()
        marker = "NOVA_FIXTURE_FENCE_EXIT="
        if not lines or not lines[-1].startswith(marker) or len(response) > 1024 * 1024:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root probe omitted its native exit")
        status = lines.pop()[len(marker):]
        if not status.isdecimal() or not 0 < int(status) <= 255:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root rejection was not observed")
        # mc prints error objects with indentation. Parse complete JSON rather
        # than lines or substrings in human-readable diagnostic messages.
        try:
            value = json.loads("\n".join(lines))
        except (ValueError, TypeError) as error:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root probe returned invalid JSON") from error
        detail = value.get("error") if isinstance(value, dict) else None
        cause = detail.get("cause") if isinstance(detail, dict) else None
        native = cause.get("error") if isinstance(cause, dict) else None
        code = native.get("Code") if isinstance(native, dict) else None
        code = code or (detail.get("code") if isinstance(detail, dict) else None)
        if not isinstance(value, dict) or value.get("status") != "error" or code not in {
                "InvalidAccessKeyId", "AccessDenied", "SignatureDoesNotMatch", "InvalidToken"}:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root rejection was not observed")

    def run_mc(self, record, script, *, deadline=None, binary=False):
        """Register, execute and actually settle one exact transient container."""
        limits = [time.monotonic() + self.timeout]
        limits += [limit for limit in (deadline, getattr(self, "operation_deadline", None)) if limit is not None]
        deadline = min(limits)
        timeout = deadline - time.monotonic()
        if timeout <= 0:
            raise runtime.RuntimeFailure("OperationDeadlineExceeded", "mc deadline elapsed before dispatch")
        name = PROJECT + "-mc-" + uuid.uuid4().hex
        journal = Path(record["compose_file"]).parent / "operations" / (name + ".json")
        fact = dict(schema=1, container_name=name, phase="intent", generation=record["generation"],
                    image_id=record["images"]["mc"]["image_id"],
                    labels={**self.labels(record), "novarocks.fixture.version": record["version"],
                            "novarocks.fixture.activation": record["generation"]})
        runtime.atomic_json(journal, fact)
        lock_fd = getattr(self, "maintenance_fd", None)
        if lock_fd is None:
            raise runtime.RuntimeFailure("MaintenanceLockRequired", "mc requires the machine maintenance lock")
        env_file = journal.with_suffix(".env")
        credentials = record["credentials"]
        runtime.atomic_bytes(env_file, ("MINIO_ROOT_USER=" + credentials["access_key"] + "\n" +
                                       "MINIO_ROOT_PASSWORD=" + credentials["secret_key"] + "\n").encode())
        result = self._supervise(journal, dict(environment=self.environment, network=record["network"],
            env_file=str(env_file), script=script, deadline_monotonic=deadline), timeout)
        if not result.get("command_succeeded"):
            raise runtime.RuntimeFailure("DockerOperationFailed", "mc command did not finish successfully: " + str(journal))
        output = journal.with_suffix(".stdout")
        return output.read_bytes() if binary else output.read_text()

    def _supervise(self, journal, request, timeout):
        lock_fd = getattr(self, "maintenance_fd", None)
        if lock_fd is None:
            raise runtime.RuntimeFailure("MaintenanceLockRequired", "machine command requires its maintenance lock")
        runtime.atomic_json(journal.with_suffix(".request"), request)
        fact = runtime.read_json(journal)
        read_fd, write_fd = os.pipe()
        monitor = None
        primary = None
        try:
            monitor = subprocess.Popen([
                sys.executable, str(Path(__file__).with_name("machine_operation.py")), str(journal),
                "--control-fd", str(read_fd), "--lock-fd", str(lock_fd), "--timeout", str(timeout),
            ], pass_fds=(read_fd, lock_fd), start_new_session=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=self.environment)
        except BaseException:
            os.close(write_fd)
            fact.update(phase="settled", command_succeeded=False, create_dispatched=False)
            runtime.atomic_json(journal, fact)
            raise
        finally:
            os.close(read_fd)
        try:
            monitor.wait()
        except BaseException as error:
            primary = error
        finally:
            os.close(write_fd)
            # The monitor owns actual Docker exit and cleanup. Releasing our
            # shared flock before its exit would reopen the upgrade race.
            while monitor.poll() is None:
                try:
                    monitor.wait()
                except KeyboardInterrupt:
                    if primary is None:
                        primary = KeyboardInterrupt()
        if primary is not None:
            raise primary
        result = runtime.read_json(journal)
        if result.get("phase") != "settled":
            code = "MachineOperationUnconfirmed" if request.get("operation") == "control" else "ContainerCleanupUnconfirmed"
            raise runtime.RuntimeFailure(code, str(journal))
        return result

    def command(self, args, *, absent_ok=False):
        deadline = min(time.monotonic() + self.timeout,
                       getattr(self, "operation_deadline", None) or float("inf"))
        timeout = deadline - time.monotonic()
        if timeout <= 0:
            raise runtime.RuntimeFailure("RunBudgetExpired", "original machine operation deadline expired")
        if getattr(self, "maintenance_fd", None) is None:
            original_timeout = self.timeout
            try:
                self.timeout = timeout
                return super().command(args, absent_ok=absent_ok)
            finally:
                self.timeout = original_timeout
        base = Path(self.maintenance_base)
        journal = base / "operations" / ("control-" + uuid.uuid4().hex + ".json")
        readonly = args[0] in {"ps", "info", "inspect", "version"} or "inspect" in args[:2] or args[:2] == ["context", "show"]
        fact = dict(schema=1, kind="control", phase="intent", mutating=not readonly,
                    control_root=str(base.parent), machine=base.name)
        state_path = base / "state.json"
        if not readonly and state_path.exists():
            state = runtime.read_json(state_path)
            if state.get("phase") == "maintenance":
                fact.update(maintenance_generation=state["generation"], maintenance_version=state["version"])
        runtime.atomic_json(journal, fact)
        result = self._supervise(journal, dict(operation="control", environment=self.environment,
            args=args, deadline_monotonic=deadline), timeout)
        if result.get("timed_out"):
            raise runtime.RuntimeFailure("DockerOperationTimeout", args[0])
        output = journal.with_suffix(".stdout")
        stderr = Path(str(output) + ".stderr")
        return self.command_result(args, result.get("command_exit", 1), output.read_text() if output.exists() else "",
            stderr.read_text() if stderr.exists() else "", absent_ok=absent_ok)

    def unsettled_operations(self, base):
        pending = []
        paths = [*Path(base).glob("activations/**/operations/*.json"), *Path(base).glob("operations/*.json")]
        for path in sorted(paths):
            if path.is_symlink():
                raise runtime.RuntimeFailure("RuntimeStateUnreadable", "operation journal is not a regular owned file")
            fact = runtime.read_json(path)
            if fact.get("schema") != 1 or fact.get("phase") not in {"intent", "creating", "create-unknown", "created", "running", "settled", "cleanup-failed", "command-unknown"}:
                raise runtime.RuntimeFailure("RuntimeStateUnreadable", "invalid operation journal")
            if fact["phase"] != "settled":
                pending.append((path, fact))
        return pending

    def reconcile_terminal_port_failure(self, path, fact, base):
        # A native daemon bind rejection is a terminal RPC result, whereas
        # transport loss or a timeout leaves a dispatched mutation unknown.
        # Reconcile only the same persisted maintenance intent, and recheck its
        # exact source/target resources before permitting that intent to retry.
        if (fact.get("phase") != "command-unknown" or fact.get("command_exit") != 1
                or fact.get("timed_out") is not False or fact.get("group_exit_observed") is not True
                or not fact.get("maintenance_generation")):
            return False
        request = runtime.read_json(path.with_suffix(".request"))
        arguments = request.get("args", [])
        if not arguments or arguments[0] != "compose" or "up" not in arguments:
            return False
        stderr = Path(str(path.with_suffix(".stdout")) + ".stderr")
        if not stderr.is_file() or stderr.stat().st_size > 1024 * 1024:
            return False
        if not re.search(r"(?m)^Error response from daemon: [^\n]*Bind for (?:[0-9.]+|\[[0-9a-fA-F:]+\]):[0-9]+ failed: port is already allocated\r?$", stderr.read_text()):
            return False
        state = runtime.read_json(Path(base) / "state.json")
        if (state.get("phase") != "maintenance"
                or state.get("generation") != fact["maintenance_generation"]
                or state.get("version") != fact.get("maintenance_version")):
            return False
        if state.get("fenced_store") and not state.get("steps", {}).get("root_rotated"):
            target = copy.deepcopy(state["fenced_store"])
            target.get("containers", {}).pop("minio", None)
            self.activation_preflight(target, state.get("previous"))
        else:
            self.activation_preflight(state, state.get("fenced_store") or state.get("previous"))
        fact.update(phase="settled", command_succeeded=False, terminal_failure="daemon-port-rejected",
                    reconciled_same_maintenance_intent=True)
        runtime.atomic_json(path, fact)
        return True

    def settle_operations(self, base):
        for path, fact in self.unsettled_operations(base):
            if fact.get("kind") == "control":
                if fact.get("control_root") != str(Path(base).parent) or fact.get("machine") != Path(base).name:
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "control operation belongs to another machine")
                if not fact.get("command_dispatched"):
                    fact.update(phase="settled", command_succeeded=False, interrupted=True)
                    runtime.atomic_json(path, fact)
                    continue
                if self.reconcile_terminal_port_failure(path, fact, base):
                    continue
                # No CLI completion fact survives a killed supervisor. A
                # timeout is equally insufficient to authorize a later ready.
                raise runtime.RuntimeFailure("MachineOperationUnconfirmed", "unsettled machine command: " + str(path))
            name, labels = fact.get("container_name", ""), fact.get("labels", {})
            if (not name.startswith(PROJECT + "-mc-") or len(name) != len(PROJECT + "-mc-") + 32
                    or labels.get("novarocks.fixture.control-root") != str(Path(base).parent)
                    or not fact.get("image_id")):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "operation has no exact machine ownership")
            container = self.inspect("container", name, absent_ok=True)
            if container:
                if ((fact.get("container_id") and container.get("Id") != fact["container_id"])
                        or container.get("Image") != fact["image_id"] or any(
                    container.get("Config", {}).get("Labels", {}).get(k) != v for k, v in labels.items()
                )):
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "orphan operation container differs from its journal")
                identity = container["Id"]
                self.command(["container", "rm", "--force", identity])
                if self.inspect("container", identity, absent_ok=True):
                    raise runtime.RuntimeFailure("ResourcesStillPresent", "orphan operation did not exit")
            elif fact.get("create_dispatched") and not fact.get("container_id"):
                raise runtime.RuntimeFailure("ContainerCleanupUnconfirmed", "unknown create outcome: " + str(path))
            fact.update(phase="settled", interrupted=True, container_absent=True)
            runtime.atomic_json(path, fact)

    def remove_rest_state(self, record):
        self.compose(record, ["rm", "-f", "rest", "rest-mv"])
        for service in ("rest", "rest-mv"):
            name = PROJECT + "_" + service + "-catalog"
            volume = self.inspect("volume", name, absent_ok=True)
            if volume:
                if any(volume.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()):
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "foreign REST state volume")
                self.command(["volume", "rm", name])
        for service in ("rest", "rest-mv"):
            record.get("containers", {}).pop(service, None)


class MachineOwner:
    def __init__(self, root=None, *, daemon=None, backend=None, template=None,
                 timeout=90, clock=time.time, sweeper_factory=None):
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise runtime.RuntimeFailure("InvalidConfiguration", "machine timeout must be finite and positive")
        state = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
        self.root = Path(root or state / "novarocks/fixture-machine").resolve()
        self.backend = backend if backend is not None else MachineDocker(timeout=timeout)
        self._daemon = daemon
        self.timeout, self.clock = timeout, clock
        self.template = Path(template or runtime.SCRIPT_DIR / "templates/machine.yml")
        self.sweeper_factory = sweeper_factory

    @property
    def daemon_id(self):
        if self._daemon is None:
            self._daemon = self.backend.daemon_id()
        return runtime.safe_component(self._daemon)

    @property
    def machine_id(self):
        return "machine-" + runtime.digest(self.daemon_id)[:16]

    @property
    def base(self):
        return self.root / self.machine_id

    @property
    def state_dir(self):
        return self.base

    def assert_live_daemon(self):
        if self.backend.daemon_id() != self.daemon_id:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine belongs to another Docker daemon")

    @contextmanager
    def maintenance_lock(self, deadline=None):
        deadline = time.monotonic() + self.timeout if deadline is None else deadline
        self.base.mkdir(parents=True, exist_ok=True)
        self.base.chmod(0o700)
        with (self.base / "maintenance.lock").open("a+b") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise runtime.RuntimeFailure("MachineMaintaining", "machine maintenance is in progress")
                    time.sleep(min(0.02, max(0, deadline - time.monotonic())))
            previous_fd = getattr(self.backend, "maintenance_fd", None)
            previous_base = getattr(self.backend, "maintenance_base", None)
            self.backend.maintenance_fd = lock.fileno()
            self.backend.maintenance_base = self.base
            try:
                yield
            finally:
                self.backend.maintenance_fd = previous_fd
                self.backend.maintenance_base = previous_base
                fcntl.flock(lock, fcntl.LOCK_UN)

    def read_saved_state(self):
        path = self.base / "state.json"
        if not path.exists():
            return None
        value = runtime.read_json(path)
        if (value.get("schema") != PROTOCOL or value.get("daemon_id") != self.daemon_id
                or value.get("control_root") != str(self.root) or value.get("phase") not in {"maintenance", "ready"}):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "invalid machine authority state")
        return value

    def load_config(self):
        state = self.read_saved_state()
        if state:
            return machine_config(state["config"])
        path = self.base / "config.json"
        return machine_config(runtime.read_json(path) if path.exists() else None)

    def required_definition(self, config, verified_bom):
        config = machine_config(config)
        images, facts = input_facts(verified_bom, self.backend)
        text = self.template.read_text()
        version = runtime.digest(dict(protocol=PROTOCOL, inputs=facts,
                                      template_sha256=runtime.digest(text), config=config))
        return dict(version=version, config=config, images=images, template=text,
                    producer_receipt={"lock_sha256": verified_bom["lock_sha256"],
                                      **copy.deepcopy(verified_bom["derived_images"]["iceberg-spark"])})

    @contextmanager
    def ready_snapshot(self, definition, deadline=None, *, expected_generation=None):
        previous = getattr(self.backend, "operation_deadline", None)
        effective = deadline if previous is None else previous if deadline is None else min(previous, deadline)
        self.backend.operation_deadline = effective
        def check_deadline():
            if effective is not None and time.monotonic() >= effective:
                raise runtime.RuntimeFailure("RunBudgetExpired", "original fixture operation deadline expired")
        try:
            check_deadline()
            self.assert_live_daemon()
            with self.maintenance_lock(effective):
                state = self.read_saved_state()
                if not state or state["phase"] != "ready":
                    raise runtime.RuntimeFailure("MachineUnavailable", "start or finish machine maintenance explicitly")
                if definition["version"] != state["version"]:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "update the worktree or explicitly upgrade the machine")
                if expected_generation is not None and state["generation"] != expected_generation:
                    raise runtime.RuntimeFailure("MachineUnavailable", "fixture activation changed")
                if self.backend.unsettled_operations(self.base):
                    raise runtime.RuntimeFailure("MachineUnavailable", "finish unsettled machine operations explicitly")
                self.backend.validate_minio_volume(state)
                healthy = self.backend.healthy(state)
                check_deadline()
                if not healthy:
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine services are not healthy")
                yield copy.deepcopy(state)
        finally:
            self.backend.operation_deadline = previous

    def _save(self, record):
        runtime.atomic_json(self.base / "state.json", record)

    def _save_scratch_observation(self, state, report):
        # Volatile observations must not change stable publication identity.
        keys = ("observed_counts", "oldest_created_unix_ms", "sampled_at_unix_ms", "counts_are_pre_cleanup",
                "unknown_counts", "enumeration_complete", "enumeration_scope", "classification_basis")
        if "sampled_at_unix_ms" in report:
            runtime.atomic_json(self.base / "scratch-observation.json", dict(schema=1, machine_id=state["id"],
                version=state["version"], generation=state["generation"], **{k: report[k] for k in keys if k in report}))

    def scratch_observation(self, state):
        path = self.base / "scratch-observation.json"
        if not path.exists():
            return {"status": "UNKNOWN", "reason": "not observed"}
        sample = runtime.read_json(path)
        if sample.get("schema") != 1 or sample.get("machine_id") != state["id"]:
            raise runtime.RuntimeFailure("RuntimeStateUnreadable", "scratch observation identity differs")
        return {"status": "OBSERVED", "sample_is_current_activation": all(sample.get(k) == state[k]
                for k in ("version", "generation")), **sample}

    def _record(self, definition, credentials=None):
        generation = uuid.uuid4().hex
        directory = self.base / "activations" / generation
        credentials = credentials or dict(access_key="nr" + secrets.token_hex(9),
                                           secret_key=secrets.token_urlsafe(32), generation=uuid.uuid4().hex)
        images = copy.deepcopy(definition["images"])
        config = copy.deepcopy(definition["config"])
        record = dict(schema=PROTOCOL, protocol=PROTOCOL, kind="machine", key="machine",
                      id=self.machine_id, daemon_id=self.daemon_id, control_root=str(self.root),
                      namespace=self.machine_id, phase="maintenance", version=definition["version"],
                      generation=generation, created_at=self.clock(), config=config,
                      credentials=credentials, project=PROJECT, network=NETWORK,
                      images=images, ports=config["ports"], policy=config["policy"], containers={},
                      compose_file=str(directory / "compose.yml"), compose_env=str(directory / "compose.env"),
                      template=definition["template"], producer_receipt=definition["producer_receipt"],
                      protected_benchmark_roots=[config["benchmark_root"]],
                      volumes=[config["minio_volume"], PROJECT + "_rest-catalog", PROJECT + "_rest-mv-catalog"],
                      required_services=["minio", "mc-init", "rest", "rest-mv", "spark"],
                      server_warehouse="s3://warehouse/fixture-machine/rest",
                      server_warehouses={"rest": "s3://warehouse/fixture-machine/rest", "rest-mv": "s3://warehouse/fixture-machine/rest-mv"},
                      steps={})
        record["service_ports"] = {"minio": {"9000/tcp": config["ports"]["minio"], "9001/tcp": config["ports"]["minio_console"]},
                                   "rest": {"8181/tcp": config["ports"]["rest"]},
                                   "rest-mv": {"8181/tcp": config["ports"]["rest_mv"]},
                                   "spark": {"4040/tcp": config["ports"]["spark"]}}
        record["health_urls"] = [endpoints(record)["minio_endpoint"] + "/minio/health/live",
                                 endpoints(record)["rest_uri"] + "/v1/config", endpoints(record)["rest_mv_uri"] + "/v1/config"]
        return record

    def _write_definition(self, record):
        credentials = record["credentials"]
        values = dict(NOVA_FIXTURE_OWNER=record["namespace"], NOVA_FIXTURE_CONTROL_ROOT=str(self.root),
                      NOVA_FIXTURE_VERSION=record["version"], NOVA_FIXTURE_PROJECT=PROJECT,
                      NOVA_FIXTURE_GENERATION=record["generation"],
                      MINIO_ROOT_USER=credentials["access_key"], MINIO_ROOT_PASSWORD=credentials["secret_key"],
                      NOVA_FIXTURE_MINIO_VOLUME=record["config"]["minio_volume"])
        values.update({s.upper().replace("-", "_") + "_IMAGE": img["image_id"] for s, img in record["images"].items()})
        values.update({"NOVA_ENV_" + n.upper() + "_PORT": v for n, v in record["ports"].items()})
        values.update(NOVA_ENV_REST_SERVER_WAREHOUSE_URI=record["server_warehouses"]["rest"],
                      NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI=record["server_warehouses"]["rest-mv"])
        text = "".join(f"{key}='{str(v).replace(chr(39), chr(92)+chr(39))}'\n" for key, v in sorted(values.items()))
        for name, data in (("compose_file", record["template"]), ("compose_env", text)):
            path = Path(record[name])
            if path.exists() and path.read_text() != data:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation definition changed")
            if not path.exists():
                runtime.atomic_bytes(path, data.encode())

    def _sweeper(self, record):
        if self.sweeper_factory:
            return self.sweeper_factory(record, self.backend)
        from scratch import ScratchSweeper
        return ScratchSweeper(record, self.backend)

    def activate(self, definition, *, upgrade=False):
        self.assert_live_daemon()
        with self.maintenance_lock():
            self.backend.settle_operations(self.base)
            old = self.read_saved_state()
            if old and old["config"]["minio_volume"] != definition["config"]["minio_volume"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "upgrade cannot replace the preserved MinIO data volume")
            if old and (old.get("minio_volume_identity") or old["phase"] == "ready"):
                self.backend.validate_minio_volume(old)
            if old and old["phase"] == "ready" and old["version"] == definition["version"] and not upgrade:
                if self.backend.healthy(old):
                    return old
                record = self._record(definition, credentials=old["credentials"])
                record["previous"] = old
                record["recover_only"] = True
            elif old and old["phase"] == "maintenance":
                if old["version"] != definition["version"]:
                    raise runtime.RuntimeFailure("MachineMaintaining", "finish the saved maintenance definition first")
                record = old
            else:
                if old and not upgrade:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "explicit upgrade is required")
                record = self._record(definition)
                if old:
                    record["previous"] = old
                else:
                    self.backend.validate_absent(record)
            if old:
                if old.get("minio_volume_identity"):
                    record["minio_volume_identity"] = copy.deepcopy(old["minio_volume_identity"])
                record["protected_benchmark_roots"] = sorted(set(
                    record.get("protected_benchmark_roots", []) + old.get("protected_benchmark_roots", [])
                    + [old["config"]["benchmark_root"], definition["config"]["benchmark_root"]]))
            self._save(record)
            self._write_definition(record)
            record["minio_volume_identity"] = copy.deepcopy(self.backend.prepare_minio_volume(record))
            self._save(record)
            previous = record.get("previous")
            if previous and not record.get("recover_only"):
                steps = record["steps"]
                if not steps.get("writers_stopped"):
                    self.backend.stop_writers(previous)
                    steps["writers_stopped"] = True
                    self._save(record)
                if "fenced_store" not in record:
                    fence = copy.deepcopy(previous)
                    fence["credentials"] = record["credentials"]
                    fence["protected_benchmark_roots"] = record["protected_benchmark_roots"]
                    directory = Path(record["compose_file"]).parent / "fence"
                    fence.update(compose_file=str(directory / "compose.yml"), compose_env=str(directory / "compose.env"))
                    record["fenced_store"] = fence
                    self._save(record)
                fence = record["fenced_store"]
                self._write_definition(fence)
                if not steps.get("root_rotated"):
                    self.backend.rotate_root(fence, previous)
                    steps["root_rotated"] = True
                    self._save(record)
                if not steps.get("root_fenced"):
                    self.backend.assert_old_root_revoked(previous)
                    steps["root_fenced"] = True
                    self._save(record)
                if not steps.get("purged"):
                    sweeper = self._sweeper(fence)
                    sweeper.revoke_users()
                    report = sweeper.sweep(expired_only=False, deadline=time.monotonic() + self.timeout)
                    self._save_scratch_observation(fence, report)
                    if not report.get("complete"):
                        raise runtime.RuntimeFailure("ScratchCleanupIncomplete", "machine remains in maintenance")
                    steps["purged"] = True
                    self._save(record)
                if not steps.get("rest_removed"):
                    self.backend.remove_rest_state(fence)
                    steps["rest_removed"] = True
                    self._save(record)
                previous = fence
            record["steps"]["activation_started"] = True
            self._save(record)
            self.backend.ensure_machine(record, previous)
            if not self.backend.healthy(record):
                raise runtime.RuntimeFailure("ReadinessTimeout", "activation health was not confirmed")
            record["phase"] = "ready"
            record.pop("previous", None)
            record.pop("fenced_store", None)
            record.pop("recover_only", None)
            self._save(record)
            return record

    def sweep(self, *, deadline=None, expected_version=None, expected_generation=None):
        deadline = time.monotonic() + min(5, self.timeout) if deadline is None else deadline
        previous_deadline = getattr(self.backend, "operation_deadline", None)
        deadline = min(deadline, previous_deadline) if previous_deadline is not None else deadline
        self.backend.operation_deadline = deadline
        try:
            self.assert_live_daemon()
            with self.maintenance_lock(deadline):
                state = self.read_saved_state()
                if not state or state["phase"] != "ready":
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine is not available for sweep")
                if expected_version is not None and state["version"] != expected_version:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "sweep publication version differs")
                if expected_generation is not None and state["generation"] != expected_generation:
                    raise runtime.RuntimeFailure("MachineUnavailable", "sweep publication activation changed")
                self.backend.validate_minio_volume(state)
                self.backend.settle_operations(self.base)
                if not self.backend.healthy(state):
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine services are not healthy")
                report = self._sweeper(state).sweep(expired_only=True, deadline=deadline)
                self._save_scratch_observation(state, report)
                return report
        finally:
            self.backend.operation_deadline = previous_deadline

    def attach_consumer(self, container, alias, expected_generation):
        self.assert_live_daemon()
        with self.maintenance_lock():
            state = self.read_saved_state()
            if not state or state["phase"] != "ready" or state["generation"] != expected_generation:
                raise runtime.RuntimeFailure("MachineUnavailable", "consumer fixture activation is unavailable")
            self.backend.validate_resources(state)
            self.backend.connect(state, container, alias)

    def detach_consumer(self, container, expected_machine_identity):
        self.assert_live_daemon()
        if expected_machine_identity != self.machine_id:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "consumer belongs to another machine")
        with self.maintenance_lock():
            state = self.read_saved_state()
            if state:
                self.backend.own_network(state)
                self.backend.disconnect(state, container)


def public_snapshot(value):
    """CLI status is safe to retain; publications use private exact snapshots."""
    if isinstance(value, dict):
        return {k: public_snapshot(v) for k, v in value.items()
                if k not in {"credentials", "previous", "fenced_store", "template", "minio_volume_identity"}}
    if isinstance(value, list):
        return [public_snapshot(v) for v in value]
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--timeout", type=float, default=90)
    commands = parser.add_subparsers(dest="operation", required=True)
    for operation in ("start", "upgrade"):
        sub = commands.add_parser(operation)
        sub.add_argument("--config", type=Path)
        sub.add_argument("--store", type=Path)
    commands.add_parser("status")
    commands.add_parser("sweep")
    args = parser.parse_args(argv)
    runtime.install_signal_handlers()
    try:
        owner = MachineOwner(args.root, timeout=args.timeout)
        if args.operation in {"start", "upgrade"}:
            config = machine_config(runtime.read_json(args.config)) if args.config else owner.load_config()
            bom = verified_bom(runtime.SCRIPT_DIR.parent.parent, args.store)
            result = owner.activate(owner.required_definition(config, bom), upgrade=args.operation == "upgrade")
        elif args.operation == "sweep":
            result = owner.sweep()
        else:
            result = owner.read_saved_state()
            if result:
                result["observed_healthy"] = owner.backend.healthy(result) if result["phase"] == "ready" else False
                result["scratch_observation"] = owner.scratch_observation(result)
        print(json.dumps(public_snapshot(result), sort_keys=True))
        return 0
    except runtime.RuntimeFailure as error:
        print(str(error), file=sys.stderr)
        return 75 if error.code in BLOCKED else 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
