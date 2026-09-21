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

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SAMPLE_RESOURCE_OUTPUT = "@SAMPLE_RESOURCE_OUTPUT@"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect bounded command samples for UEA-7 B0/candidate comparison"
    )
    parser.add_argument("--label", required=True)
    parser.add_argument("--role", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--profile", choices=("dev", "dev-opt", "release"), required=True)
    parser.add_argument(
        "--workload-file",
        type=Path,
        action="append",
        required=True,
        help="repeat for every worktree-local workload definition that must match",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        help="runtime config to record for audit; its worktree-specific bytes are not compared",
    )
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


def file_identity(path: Path, worktree_root: Path) -> dict[str, str]:
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(worktree_root)
    except ValueError as error:
        raise ValueError(f"workload file must be inside the Git worktree: {resolved}") from error
    if not resolved.is_file():
        raise ValueError(f"workload file is not a regular file: {resolved}")
    return {
        "path": str(resolved),
        "relative_path": relative.as_posix(),
        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
    }


def config_identity(path: Path, worktree_root: Path) -> dict[str, str]:
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"config file is not a regular file: {resolved}")
    return {
        "path": str(resolved),
        "normalized_path": normalized_command([str(resolved)], worktree_root)[0],
        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
    }


def normalized_command(command: list[str], worktree_root: Path) -> list[str]:
    root = str(worktree_root)
    slug = re.sub(r"-+", "-", re.sub(r"[^a-z0-9]", "-", worktree_root.name.lower()))
    slug = slug.strip("-")[:24] or "novarocks"
    runtime_id = f"{slug}-{hashlib.sha1(root.encode()).hexdigest()[:8]}"
    runtime_prefix = f"${{WORKTREE}}/docker/iceberg-rest/runtime/{runtime_id}/"

    def normalize(argument: str) -> str:
        prefix = ""
        value = argument
        if argument.startswith("--") and "=" in argument:
            option, value = argument.split("=", 1)
            prefix = option + "="
        if value == root or value.startswith(root + os.sep):
            normalized = "${WORKTREE}" + value[len(root) :]
            if normalized.startswith(runtime_prefix):
                normalized = normalized.replace(
                    runtime_prefix,
                    "${WORKTREE}/docker/iceberg-rest/runtime/${RUNTIME}/",
                    1,
                )
            return prefix + normalized
        return argument

    return [normalize(argument) for argument in command]


def nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def summarize(samples: list[dict[str, object]]) -> dict[str, object]:
    elapsed = [float(sample["elapsed_ms"]) for sample in samples]
    rss = [int(sample["controller_max_rss"]) for sample in samples]
    summary = {
        "elapsed_ms": {
            "median": statistics.median(elapsed),
            "p95_nearest_rank": nearest_rank(elapsed, 0.95),
            "maximum": max(elapsed),
        },
        "controller_max_rss": {
            "median": statistics.median(rss),
            "p95_nearest_rank": nearest_rank(rss, 0.95),
            "maximum": max(rss),
        },
    }
    if all("role_resources" in sample for sample in samples):
        roles = sorted(samples[0]["role_resources"]["peak_rss_bytes"])
        if any(
            sorted(sample["role_resources"]["peak_rss_bytes"]) != roles
            for sample in samples
        ):
            raise ValueError("process resource roles changed between samples")
        summary["role_peak_rss_bytes"] = {
            role: {
                "median": statistics.median(
                    sample["role_resources"]["peak_rss_bytes"][role]
                    for sample in samples
                ),
                "p95_nearest_rank": nearest_rank(
                    [
                        sample["role_resources"]["peak_rss_bytes"][role]
                        for sample in samples
                    ],
                    0.95,
                ),
                "maximum": max(
                    sample["role_resources"]["peak_rss_bytes"][role]
                    for sample in samples
                ),
            }
            for role in roles
        }
        summary["role_cpu_time_ms"] = {
            role: {
                kind: {
                    "median": statistics.median(
                        sample["role_resources"]["cpu_time_ms"][role][kind]
                        for sample in samples
                    ),
                    "p95_nearest_rank": nearest_rank(
                        [
                            sample["role_resources"]["cpu_time_ms"][role][kind]
                            for sample in samples
                        ],
                        0.95,
                    ),
                    "maximum": max(
                        sample["role_resources"]["cpu_time_ms"][role][kind]
                        for sample in samples
                    ),
                }
                for kind in ("user", "system", "total")
            }
            for role in roles
        }
    return summary


def role_resource_identity(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 3:
        raise ValueError("process resource artifact has an unsupported schema")
    processes = payload.get("processes")
    samples = payload.get("samples")
    if (
        not isinstance(processes, dict)
        or not isinstance(samples, list)
        or any(not isinstance(sample, dict) for sample in samples)
    ):
        raise ValueError("process resource artifact has no role identities or samples")
    if "fe" not in processes or not any(role.startswith("be-") for role in processes):
        raise ValueError("process resource artifact has no native FE/BE topology")
    peak: dict[str, int] = {}
    cpu_time: dict[str, dict[str, float]] = {}
    for role, identity in processes.items():
        if (
            not isinstance(identity, dict)
            or not isinstance(identity.get("pid"), int)
            or not identity.get("process_start_token")
        ):
            raise ValueError(f"process resource artifact lacks exact {role} identity")
        matching = [sample for sample in samples if sample.get("role") == role]
        if len(matching) < 2 or any(
            sample.get("pid") != identity["pid"]
            or sample.get("process_start_token") != identity["process_start_token"]
            or not isinstance(sample.get("rss_bytes"), int)
            or sample.get("rss_bytes") <= 0
            or not isinstance(sample.get("elapsed_millis"), int)
            or sample.get("elapsed_millis") < 0
            or not isinstance(sample.get("cpu_user_nanos"), int)
            or sample.get("cpu_user_nanos") < 0
            or not isinstance(sample.get("cpu_system_nanos"), int)
            or sample.get("cpu_system_nanos") < 0
            or sample.get("unavailable_reason") is not None
            for sample in matching
        ):
            raise ValueError(f"process resource artifact has incomplete {role} samples")
        if any(
            later["elapsed_millis"] <= earlier["elapsed_millis"]
            or later["cpu_user_nanos"] < earlier["cpu_user_nanos"]
            or later["cpu_system_nanos"] < earlier["cpu_system_nanos"]
            for earlier, later in zip(matching, matching[1:])
        ):
            raise ValueError(f"process resource artifact has nonmonotonic {role} CPU samples")
        peak[role] = max(sample["rss_bytes"] for sample in matching)
        user_ms = (matching[-1]["cpu_user_nanos"] - matching[0]["cpu_user_nanos"]) / 1_000_000
        system_ms = (
            matching[-1]["cpu_system_nanos"] - matching[0]["cpu_system_nanos"]
        ) / 1_000_000
        cpu_time[role] = {
            "user": user_ms,
            "system": system_ms,
            "total": user_ms + system_ms,
        }
    if any(sample.get("role") not in processes for sample in samples):
        raise ValueError("process resource artifact includes an unknown role")
    return {
        "path": path.name,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "peak_rss_bytes": peak,
        "cpu_time_ms": cpu_time,
        "sample_count": len(samples),
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
    resource_path = (
        output.with_suffix(".resources.json")
        if SAMPLE_RESOURCE_OUTPUT in command
        else None
    )
    realized_command = [
        str(resource_path) if argument == SAMPLE_RESOURCE_OUTPUT else argument
        for argument in command
    ]
    started = time.perf_counter_ns()
    timed_out = False
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            realized_command,
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
    result = {
        "elapsed_ms": elapsed_ms,
        "controller_user_cpu_ms": usage.ru_utime * 1000,
        "controller_system_cpu_ms": usage.ru_stime * 1000,
        "controller_max_rss": usage.ru_maxrss,
        "exit_code": process.returncode,
        "timed_out": timed_out,
        "stdout": stdout_path.name,
        "stderr": stderr_path.name,
        "executed_command": realized_command,
    }
    if resource_path is not None:
        try:
            result["role_resources"] = role_resource_identity(resource_path)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            result["role_resource_error"] = str(error)
    return result


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
    git_root = command_output(["git", "rev-parse", "--show-toplevel"], cwd)
    if git_root is None:
        raise SystemExit("cannot resolve measurement Git worktree")
    worktree_root = Path(git_root).resolve()
    try:
        workloads = sorted(
            (file_identity(path, worktree_root) for path in args.workload_file),
            key=lambda item: item["relative_path"],
        )
        if len({item["relative_path"] for item in workloads}) != len(workloads):
            raise ValueError("duplicate workload file")
        config = (
            config_identity(args.config_file, worktree_root)
            if args.config_file
            else None
        )
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error
    git_status = (
        command_output(["git", "status", "--porcelain=v1"], cwd) or ""
    ).splitlines()
    if git_status and not args.allow_dirty:
        raise SystemExit("measurement checkout must be clean; commit or remove local changes")
    report: dict[str, object] = {
        "schema_version": 3,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "label": args.label,
        "role": args.role,
        "profile": args.profile,
        "command": args.command,
        "normalized_command": normalized_command(args.command, worktree_root),
        "cwd": str(cwd),
        "worktree_root": str(worktree_root),
        "workload_files": workloads,
        "config": config,
        "environment": {
            name: os.environ.get(name)
            for name in ("NOVAROCKS_BIN", "NOVAROCKS_SQL_TEST_CONFIG")
        },
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
        "resource_scope": "wait4_direct_controller_process_only",
        "warmup_results": [],
        "samples": [],
    }

    failed = False
    for index in range(1, args.warmups + 1):
        result = run_once(
            args.command, cwd, output / f"warmup-{index:02d}", args.timeout_seconds
        )
        report["warmup_results"].append(result)  # type: ignore[union-attr]
        failed |= (
            bool(result["timed_out"])
            or result["exit_code"] != 0
            or "role_resource_error" in result
        )
        if failed:
            break

    if not failed:
        for index in range(1, args.samples + 1):
            result = run_once(
                args.command, cwd, output / f"sample-{index:02d}", args.timeout_seconds
            )
            report["samples"].append(result)  # type: ignore[union-attr]
            failed |= (
                bool(result["timed_out"])
                or result["exit_code"] != 0
                or "role_resource_error" in result
            )
            if failed:
                break

    samples = report["samples"]
    if samples:
        try:
            report["summary"] = summarize(samples)  # type: ignore[arg-type]
        except ValueError as error:
            report["role_resource_error"] = str(error)
            failed = True
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
