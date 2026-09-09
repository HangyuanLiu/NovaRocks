#!/usr/bin/env python3
#
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

import argparse
import json
import math
import os
import platform
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect bounded command samples for UEA-7 B0/candidate comparison"
    )
    parser.add_argument("--label", required=True)
    parser.add_argument("--role", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--profile", choices=("dev", "dev-opt", "release"), required=True)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=3600.0)
    parser.add_argument("--dimension", action="append", default=[])
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")
    if args.samples < 7:
        parser.error("--samples must be at least 7")
    if args.warmups < 0:
        parser.error("--warmups must not be negative")
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be a positive finite number")
    if not args.output.is_absolute():
        parser.error("--output must be an absolute path")
    return args


def parse_dimensions(values: list[str]) -> dict[str, str]:
    dimensions: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"dimension must be key=value: {value!r}")
        key, item = value.split("=", 1)
        if not key or not item:
            raise ValueError(f"dimension must have a non-empty key and value: {value!r}")
        if key in dimensions:
            raise ValueError(f"duplicate dimension: {key}")
        dimensions[key] = item
    return dimensions


def command_output(command: list[str], cwd: Path) -> str | None:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def summarize(samples: list[dict[str, object]]) -> dict[str, object]:
    elapsed = [float(sample["elapsed_ms"]) for sample in samples]
    rss = [int(sample["max_rss"]) for sample in samples]
    return {
        "elapsed_ms": {
            "median": statistics.median(elapsed),
            "p95_nearest_rank": nearest_rank(elapsed, 0.95),
            "maximum": max(elapsed),
        },
        "max_rss": {
            "median": statistics.median(rss),
            "p95_nearest_rank": nearest_rank(rss, 0.95),
            "maximum": max(rss),
        },
    }


def terminate_process_group(process: subprocess.Popen[bytes]):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return None
    deadline = time.monotonic() + 2.0
    usage = None
    while time.monotonic() < deadline:
        child, status, child_usage = os.wait4(process.pid, os.WNOHANG)
        if child:
            process.returncode = os.waitstatus_to_exitcode(status)
            usage = child_usage
            break
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return usage


def run_once(
    command: list[str], cwd: Path, output: Path, timeout_seconds: float
) -> dict[str, object]:
    stdout_path = output.with_suffix(".stdout")
    stderr_path = output.with_suffix(".stderr")
    started = time.perf_counter_ns()
    timed_out = False
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        deadline = time.monotonic() + timeout_seconds
        usage = None
        while usage is None:
            child, status, child_usage = os.wait4(process.pid, os.WNOHANG)
            if child:
                process.returncode = os.waitstatus_to_exitcode(status)
                usage = child_usage
                break
            if time.monotonic() >= deadline:
                timed_out = True
                usage = terminate_process_group(process)
                if usage is None:
                    child, status, usage = os.wait4(process.pid, 0)
                    process.returncode = os.waitstatus_to_exitcode(status)
                break
            time.sleep(0.01)
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    return {
        "elapsed_ms": elapsed_ms,
        "user_cpu_ms": usage.ru_utime * 1000,
        "system_cpu_ms": usage.ru_stime * 1000,
        "max_rss": usage.ru_maxrss,
        "exit_code": process.returncode,
        "timed_out": timed_out,
        "stdout": stdout_path.name,
        "stderr": stderr_path.name,
    }


def main() -> int:
    args = parse_args()
    if not hasattr(os, "wait4"):
        raise SystemExit("measure-command.py requires a Unix platform with wait4(2)")
    try:
        dimensions = parse_dimensions(args.dimension)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise SystemExit(f"--output must be empty: {output}")

    cwd = Path.cwd().resolve()
    system = platform.system()
    git_head = command_output(["git", "rev-parse", "HEAD"], cwd)
    if git_head is None:
        raise SystemExit("measurement cwd must be inside a Git checkout")
    git_status = (
        command_output(["git", "status", "--porcelain=v1"], cwd) or ""
    ).splitlines()
    if git_status and not args.allow_dirty:
        raise SystemExit("measurement checkout must be clean; commit or remove local changes")
    report: dict[str, object] = {
        "schema_version": 1,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "label": args.label,
        "role": args.role,
        "profile": args.profile,
        "command": args.command,
        "cwd": str(cwd),
        "dimensions": dimensions,
        "warmups": args.warmups,
        "sample_count": args.samples,
        "timeout_seconds": args.timeout_seconds,
        "allow_dirty": args.allow_dirty,
        "git": {
            "head": git_head,
            "status_porcelain": git_status,
        },
        "toolchain": {
            "rustc": command_output(["rustc", "--version"], cwd),
            "cargo": command_output(["cargo", "--version"], cwd),
        },
        "platform": {
            "system": system,
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
        },
        "max_rss_unit": "bytes" if system == "Darwin" else "kibibytes",
        "warmup_results": [],
        "samples": [],
    }

    failed = False
    for index in range(1, args.warmups + 1):
        result = run_once(
            args.command, cwd, output / f"warmup-{index:02d}", args.timeout_seconds
        )
        report["warmup_results"].append(result)  # type: ignore[union-attr]
        failed |= bool(result["timed_out"]) or result["exit_code"] != 0
        if failed:
            break

    if not failed:
        for index in range(1, args.samples + 1):
            result = run_once(
                args.command, cwd, output / f"sample-{index:02d}", args.timeout_seconds
            )
            report["samples"].append(result)  # type: ignore[union-attr]
            failed |= bool(result["timed_out"]) or result["exit_code"] != 0
            if failed:
                break

    samples = report["samples"]
    if samples:
        report["summary"] = summarize(samples)  # type: ignore[arg-type]
    report["complete"] = not failed and len(samples) == args.samples
    (output / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if not report["complete"]:
        print(f"measurement failed; inspect {output / 'report.json'}", file=sys.stderr)
        return 1
    print(output / "report.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
