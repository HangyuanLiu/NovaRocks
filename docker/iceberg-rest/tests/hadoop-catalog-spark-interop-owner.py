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

Spark keeps its existing fixture_run owner. The private Server owner retains
its exact child until actual wait, including stop, timeout and cancellation.
Exit observation and permission to reclaim the original run stay separate.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import signal
import time
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


def settle_server(process, complete_deadline):
    # There is one reaper and handlers only set a flag. A returncode of None
    # therefore retains the exact leader (possibly a zombie), pinning its PGID.
    if process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        term_end = min(complete_deadline, time.monotonic() + 2)
        while process.returncode is None and time.monotonic() < term_end:
            try:
                process.wait(timeout=min(0.05, max(0, term_end - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
    if process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    # A deadline is never permission to abandon an unobserved process. Kernel
    # exit observation may finish later; this grants no additional work budget.
    process.wait()


def run_server(args, command, fact):
    process = None
    cancelled = 0
    primary = None
    requested_stop = False
    complete_deadline = time.monotonic()

    def cancel(signum, frame):
        nonlocal cancelled
        if not cancelled:
            cancelled = signum

    previous = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT)}
    for number in previous:
        signal.signal(number, cancel)
    try:
        _, _, budget = fixture_run.admit(args.env_file)
        complete_deadline = budget.deadline if budget else time.monotonic() + 3600
        work_deadline = complete_deadline - (budget.cleanup_grace_seconds if budget else 0)
        if not args.stop_file:
            raise ValueError("server requires its private stop request file")
        requested_stop = Path(args.stop_file).exists()
        if cancelled:
            primary = 128 + cancelled
        elif time.monotonic() >= work_deadline:
            primary = 124
        elif not requested_stop:
            process = subprocess.Popen(command, start_new_session=True)
            fact["pid"] = process.pid
            while process.poll() is None:
                if cancelled:
                    primary = 128 + cancelled
                    break
                if time.monotonic() >= work_deadline:
                    primary = 124
                    break
                if Path(args.stop_file).exists():
                    requested_stop = True
                    break
                try:
                    process.wait(timeout=min(0.05, max(0, work_deadline - time.monotonic())))
                except subprocess.TimeoutExpired:
                    pass
    except BaseException as error:
        primary = 130 if isinstance(error, KeyboardInterrupt) else 1
        fact["reason"] = type(error).__name__
        print(str(error), file=sys.stderr)
    finally:
        try:
            if process is not None:
                settle_server(process, complete_deadline)
                fact.update(child_waited=True, returncode=process.returncode)
            else:
                fact.update(child_waited=True, never_started=True)
            if cancelled and primary is None:
                primary = 128 + cancelled
            if primary is not None:
                fact.setdefault("reason", "cancelled" if cancelled else "original work deadline or owner failure")
            elif requested_stop:
                fact["reason"] = "normal stop request"
            runtime.atomic_json(Path(args.receipt), fact)
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
    if primary is not None:
        return primary
    return 0 if requested_stop or process is None else normalized_exit(process.returncode)


def run(args, command):
    receipt = Path(args.receipt)
    # Register before admitting or starting any process. A missing final fact
    # remains unknown if this owner itself is killed or cannot settle a child.
    fact = dict(kind=args.kind, child_waited=False, returncode=None)
    runtime.atomic_json(receipt, fact)
    if args.kind == "server":
        return run_server(args, command, fact)
    try:
        runtime.install_signal_handlers()
        _, _, budget = fixture_run.admit(args.env_file)
        timeout = budget.remaining_seconds(work=True) if budget else 3600
        result = fixture_run.communicate(command, timeout=timeout)
        code = normalized_exit(result.returncode)
        safe = code not in (75, 124, 125) and code < 128 and not unsafe_spark_log(args.log)
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
        print("WriterCleanupBlocked: preserve the original run and exit diagnostics", file=sys.stderr)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    owner = sub.add_parser("run")
    owner.add_argument("--kind", choices=("server", "spark"), required=True)
    owner.add_argument("--env-file", type=Path, required=True)
    owner.add_argument("--receipt", required=True)
    owner.add_argument("--stop-file")
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
