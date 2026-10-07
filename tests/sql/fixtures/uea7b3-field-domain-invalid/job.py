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

"""Exact container ownership for this fixture's independent SDK actions.

The shared Spark service is inspected but never stopped. An unknown launch
without an observed exact container remains unknown even if discovery is empty.
Budgets follow the existing cluster-harness OwnedSpark implementation.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import resource
import stat
import subprocess
import tempfile
import time
import uuid

INPUT_BYTES = 256 * 1024
LOG_BYTES = 4 * 1024 * 1024
CONTROL_BYTES = 64 * 1024
PHASES = {"initialize", "observe", "prepare-cleanup", "commit-cleanup"}
HEX_ID = re.compile(r"[0-9a-f]{64}\Z")
INSPECTION = (
    '{"id":{{json .Id}},"name":{{json .Name}},'
    '"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
    '"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
    '"owner":{{json (index .Config.Labels "novarocks.fixture.owner")}},'
    '"kind":{{json (index .Config.Labels "novarocks.fixture.kind")}},'
    '"key":{{json (index .Config.Labels "novarocks.fixture.key")}},'
    '"token":{{json (index .Config.Labels "novarocks.fixture.spark-job")}},'
    '"image_id":{{json .Image}},"image_reference":{{json .Config.Image}},'
    '"status":{{json .State.Status}},"exit_code":{{json .State.ExitCode}}}'
)


class JobFailure(RuntimeError):
    """Status-only failure; command output can contain configured credentials."""


def canonical(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_regular(path, cap):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 <= before.st_size <= cap:
            raise JobFailure("Fixture input is not a bounded regular file")
        raw = source.read(cap + 1)
        after = os.fstat(source.fileno())
        if (len(raw) > cap or len(raw) != before.st_size
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            raise JobFailure("Fixture input changed or exceeded its byte bound")
        return raw


def write_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as target:
        target.write(raw)
        target.flush()
        os.fsync(target.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _publication(publication):
    publication = Path(publication).resolve(strict=True)
    manifest = json.loads(read_regular(publication / "manifest.json", 4 * 1024 * 1024))
    catalog = manifest["runtime"]["catalog"]
    safe = {"publication": str(publication), "project": manifest["compose_project"],
            "owner": catalog["namespace"], "key": catalog["key"], "kind": catalog["kind"],
            "image_id": catalog["images"]["spark"]["image_id"],
            "image_reference": manifest["spark"]["image"]}
    if not all(isinstance(v, str) and v and len(v) <= 4096 for v in safe.values()):
        raise JobFailure("Publication lacks exact safe Spark identity")
    if (not safe["image_id"].startswith("sha256:")
            or not HEX_ID.fullmatch(safe["image_id"][7:])
            or safe["image_reference"] != catalog["images"]["spark"]["tag"]):
        raise JobFailure("Publication Spark image is not an immutable local image")
    for name in ("compose_file", "compose_env"):
        if not Path(manifest[name]).is_file():
            raise JobFailure("Publication lacks its exact Compose inputs")
    defaults = Path(manifest["spark"]["defaults_file"]).resolve(strict=True)
    if defaults.parent != publication:
        raise JobFailure("Spark defaults escaped the immutable publication")
    return manifest, safe, defaults


def publication_identity(publication):
    return digest(canonical(_publication(publication)[1]))


def command(args, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise JobFailure("Owned Spark control deadline elapsed")
    # Control output is read only after a capped file-length check. Never include
    # command diagnostics in errors or receipts: Compose may contain credentials.
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        def bound_control_output():
            resource.setrlimit(resource.RLIMIT_FSIZE, (CONTROL_BYTES, CONTROL_BYTES))

        process = subprocess.Popen(args, stdout=stdout, stderr=stderr,
                                   stdin=subprocess.DEVNULL, start_new_session=True,
                                   preexec_fn=bound_control_output)
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, 9)
            process.wait()
            raise JobFailure("Owned Spark control outcome is unknown") from None
        for stream in (stdout, stderr):
            if os.fstat(stream.fileno()).st_size > CONTROL_BYTES:
                raise JobFailure("Owned Spark control output exceeded its bound")
        if process.returncode != 0:
            raise JobFailure("Owned Spark control operation was unsuccessful")
        stdout.seek(0)
        return stdout.read(CONTROL_BYTES + 1).decode("utf-8", errors="strict").strip()


def validate_identity(value, safe, name, token, previous=None):
    if (not isinstance(value, dict) or not HEX_ID.fullmatch(value.get("id", ""))
            or value.get("name") != "/" + name or value.get("project") != safe["project"]
            or value.get("service") != "spark" or value.get("owner") != safe["owner"]
            or value.get("key") != safe["key"] or value.get("kind") != safe["kind"]
            or (value.get("token") or "") != token or value.get("image_id") != safe["image_id"]
            or value.get("image_reference") != safe["image_reference"]
            or (previous is not None and value["id"] != previous)):
        raise JobFailure("Spark container identity changed; refusing removal")
    return value


def absence_can_complete(attempted, confirmed, observed_id):
    return not attempted or (confirmed and observed_id is not None)


def run(workspace, publication, script, directory, run_token, phase):
    if str(uuid.UUID(run_token)) != run_token or phase not in PHASES:
        raise JobFailure("Invalid fixture run token or phase")
    workspace = Path(workspace).resolve(strict=True)
    directory = Path(directory)
    directory.mkdir(mode=0o700)  # Each invocation has one immutable receipt owner.
    directory = directory.resolve(strict=True)
    manifest, safe, defaults_path = _publication(publication)
    script_raw = read_regular(script, INPUT_BYTES)
    defaults_raw = read_regular(defaults_path, INPUT_BYTES)
    if not script_raw or not defaults_raw or len(script_raw) + len(defaults_raw) > INPUT_BYTES:
        raise JobFailure("Owned Spark combined input exceeds its byte bound")
    input_dir, output_dir = directory / "input", directory / "output"
    for path in (input_dir, output_dir):
        path.mkdir(mode=0o700)
        if ":" in str(path):
            raise JobFailure("Owned Spark directory cannot form an exact Docker mount")
    write_new(input_dir / "query.scala", script_raw)
    write_new(input_dir / "spark-defaults.conf", defaults_raw)
    for name in ("stdout.log", "stderr.log"):
        write_new(output_dir / name, b"")
    job_token = str(uuid.uuid4())
    name = "nr-fd-job-" + job_token
    receipt = {"record": "field_domain_owned_spark_terminal", "version": 1,
               "run_token": run_token, "phase": phase, "job_token": job_token,
               "container_id": None, "image_id": safe["image_id"], "image_reference": safe["image_reference"],
               "script_sha256": digest(script_raw), "defaults_sha256": digest(defaults_raw),
               "publication_identity_sha256": digest(canonical(safe)),
               "execution_confirmed": False, "exit_code": None,
               "confirmed_gone": False, "forced": False}
    write_new(directory / "launch.json", canonical(receipt))
    deadline = time.monotonic() + 120
    observed = None
    attempted = confirmed = False
    failure = None

    def inspect(container_id, until):
        value = json.loads(command(["docker", "inspect", "--type", "container",
                                    "--format", INSPECTION, container_id], until))
        return validate_identity(value, safe, name, job_token, observed)

    def discover(until):
        ids = command(["docker", "ps", "-aq", "--no-trunc", "--filter", "name=^/" + name + "$"], until).splitlines()
        if len(ids) > 1:
            raise JobFailure("Owned Spark discovery is ambiguous")
        return inspect(ids[0], until) if ids else None

    def check_logs():
        for path in (output_dir / "stdout.log", output_dir / "stderr.log"):
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size >= LOG_BYTES:
                raise JobFailure("Owned Spark output is incomplete or exceeded its bound")

    try:
        baseline_ids = command(["docker", "ps", "-q", "--no-trunc", "--filter",
                                "label=com.docker.compose.project=" + safe["project"],
                                "--filter", "label=com.docker.compose.service=spark"], deadline).splitlines()
        if len(baseline_ids) != 1:
            raise JobFailure("Original shared Spark service identity is ambiguous")
        baseline = json.loads(command(["docker", "inspect", "--type", "container",
                                      "--format", INSPECTION, baseline_ids[0]], deadline))
        validate_identity(baseline, safe, baseline["name"].removeprefix("/"), "", baseline_ids[0])
        if baseline["status"] != "running":
            raise JobFailure("Original shared Spark service is not running")
        args = ["python3", str(workspace / "docker/iceberg-rest/runtime_entry.py"), "compose",
                "--env-file", manifest["compose_env"], "-p", safe["project"], "-f", manifest["compose_file"],
                "run", "--detach", "--pull", "never", "--no-deps", "--name", name,
                "--label", "novarocks.fixture.spark-job=" + job_token,
                "--volume", str(input_dir) + ":/uea-input:ro",
                "--volume", str(output_dir) + ":/uea-output:rw", "--entrypoint", "/bin/bash", "spark", "-lc",
                "set -euo pipefail; exec > /uea-output/stdout.log 2> /uea-output/stderr.log; "
                "ulimit -f 4096; exec /opt/spark/bin/spark-shell --properties-file "
                "/uea-input/spark-defaults.conf -i /uea-input/query.scala < /dev/null"]
        attempted = True
        returned = command(args, deadline)
        confirmed = True
        if not HEX_ID.fullmatch(returned):
            raise JobFailure("Owned Spark launch lacks an exact container ID")
        value = discover(deadline)
        if value is None or value["id"] != returned:
            raise JobFailure("Owned Spark launch and discovery disagree")
        observed = value["id"]
        receipt["container_id"] = observed
        while True:
            check_logs()
            value = inspect(observed, deadline)
            if value["status"] == "exited":
                status = int(command(["docker", "wait", observed], deadline))
                if status != value["exit_code"]:
                    raise JobFailure("Owned Spark wait and inspect disagree")
                check_logs()
                receipt.update(execution_confirmed=True, exit_code=status)
                break
            if value["status"] not in ("created", "running"):
                raise JobFailure("Owned Spark lifecycle state is unsupported")
            if time.monotonic() >= deadline:
                raise JobFailure("Owned Spark execution deadline elapsed")
            time.sleep(0.05)
    except (JobFailure, ValueError, OSError) as error:
        failure = error
    finally:
        cleanup_deadline = time.monotonic() + 20
        try:
            while True:
                value = discover(cleanup_deadline)
                if value is not None:
                    if observed is None:
                        observed = value["id"]
                        receipt["container_id"] = observed
                        # Discovery proves exact ownership even after lost launch ACK.
                        confirmed = True
                    receipt["forced"] |= value["status"] != "exited"
                    inspect(observed, cleanup_deadline)
                    command(["docker", "rm", "--force", observed], cleanup_deadline)
                elif absence_can_complete(attempted, confirmed, observed):
                    if observed is not None:
                        remaining = command(["docker", "ps", "-aq", "--no-trunc",
                                             "--filter", "id=" + observed], cleanup_deadline)
                        if remaining:
                            raise JobFailure("Owned Spark exact absence is unconfirmed")
                    receipt["confirmed_gone"] = True
                    break
                if time.monotonic() >= cleanup_deadline:
                    raise JobFailure("Owned Spark cleanup remains unknown")
                time.sleep(0.05)
        except (JobFailure, ValueError, OSError) as error:
            failure = error
        write_new(directory / "terminal.json", canonical(receipt))
    if failure is not None or not receipt["execution_confirmed"] or not receipt["confirmed_gone"]:
        raise JobFailure("Owned Spark action failed; inspect its status-only terminal receipt") from None
    return receipt
