#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. The ASF licenses this file under the Apache License, Version 2.0.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Private process receipts for the existing Hadoop/Spark interoperability smoke.

fixture_run remains the process/budget owner. This adapter records only positive
normal returns: timeout or interrupted settlement never authorizes run cleanup.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import fixture_run
import fixture_runtime as runtime


def normalized_exit(code):
    return code if code >= 0 else 128 - code


def unsafe_spark_log(path):
    # The inner fixture owner validates the actual container child, including
    # ordinary nonzero CLI exits. Its exceptional/unknown exits must survive the
    # shell boundary. A matching user diagnostic conservatively retains the run.
    markers = (
        "ContainerCleanupFailed:", "RunBudgetExpired:", "RunBudgetRejected:",
        "MachineUnavailable:", "Traceback (most recent call last):",
    )
    with Path(path).open(errors="replace") as output:
        return any(any(marker in line for marker in markers) for line in output)


def run(args, command):
    receipt = Path(args.receipt)
    # Register before admitting or starting any process. A missing final fact
    # remains unknown if this owner itself is killed or cannot settle a child.
    fact = dict(kind=args.kind, child_waited=False, returncode=None)
    runtime.atomic_json(receipt, fact)
    try:
        runtime.install_signal_handlers()
        _, _, budget = fixture_run.admit(args.env_file)
        timeout = budget.remaining_seconds(work=True) if budget else 3600
        if args.kind == "server":
            if not args.pid_file:
                raise ValueError("server requires its exact child PID file")
            # communicate owns this session until its real exit. exec preserves
            # the published PID so the shell stops this child, never its owner.
            command = ["/bin/sh", "-c", 'printf "%s\\n" "$$" > "$1"; shift; exec "$@"',
                       "hadoop-interop-server", args.pid_file, *command]
        result = fixture_run.communicate(command, timeout=timeout)
        code = normalized_exit(result.returncode)
        safe = args.kind == "server" or (
            code not in (75, 124, 125) and code < 128 and not unsafe_spark_log(args.log)
        )
        fact.update(child_waited=safe, returncode=result.returncode)
        if not safe:
            fact["reason"] = "inner fixture process exit was not confirmed"
        runtime.atomic_json(receipt, fact)
        return code
    except subprocess.TimeoutExpired:
        fact["reason"] = "original fixture work deadline expired"
        runtime.atomic_json(receipt, fact)
        return 124
    except BaseException as error:
        fact["reason"] = type(error).__name__
        runtime.atomic_json(receipt, fact)
        print(str(error), file=sys.stderr)
        return 130 if isinstance(error, KeyboardInterrupt) else 1


def check(args):
    facts = [json.loads(path.read_text()) for path in Path(args.directory).glob("*.process.json")]
    if any(fact.get("child_waited") is not True for fact in facts):
        print("WriterExitUnknown: preserve the original run and diagnostics", file=sys.stderr)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    owner = sub.add_parser("run")
    owner.add_argument("--kind", choices=("server", "spark"), required=True)
    owner.add_argument("--env-file", type=Path, required=True)
    owner.add_argument("--receipt", required=True)
    owner.add_argument("--pid-file")
    owner.add_argument("--log")
    verify = sub.add_parser("check")
    verify.add_argument("--directory", required=True)
    args, command = parser.parse_known_args()
    if args.operation == "check":
        return check(args)
    command = command[1:] if command[:1] == ["--"] else command
    if not command or (args.kind == "spark" and not args.log):
        parser.error("run requires a command and Spark requires its retained log")
    return run(args, command)


if __name__ == "__main__":
    raise SystemExit(main())
