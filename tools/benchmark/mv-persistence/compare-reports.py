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
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare sealed UEA-7 measurement reports")
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--max-ratio", type=float, default=1.10)
    parser.add_argument("--noise-ratio", type=float, default=1.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for name in ("max_ratio", "noise_ratio"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 1.0:
            parser.error(f"--{name.replace('_', '-')} must be finite and at least 1.0")
    if not args.output.is_absolute():
        parser.error("--output must be an absolute path")
    return args


def load_report(path: Path, expected_role: str) -> dict[str, object]:
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("schema_version") != 3 or report.get("complete") is not True:
        raise SystemExit(f"report is not a complete schema-v3 measurement: {path}")
    if report.get("role") != expected_role:
        raise SystemExit(f"report {path} has role {report.get('role')!r}, expected {expected_role}")
    return report


def main() -> int:
    args = parse_args()
    baseline = load_report(args.baseline, "baseline")
    candidate = load_report(args.candidate, "candidate")
    for name, report in (("baseline", baseline), ("candidate", candidate)):
        if report["git"]["status_porcelain"]:
            raise SystemExit(f"{name} report was collected from a dirty checkout")
    comparable_fields = (
        "profile",
        "normalized_command",
        "dimensions",
        "warmups",
        "sample_count",
        "toolchain",
        "platform",
        "max_rss_unit",
        "resource_scope",
    )
    mismatches = [
        field for field in comparable_fields if baseline.get(field) != candidate.get(field)
    ]
    if mismatches:
        raise SystemExit(f"reports are not comparable; fields differ: {', '.join(mismatches)}")
    def workload_signature(report: dict[str, object]) -> list[tuple[str, str]]:
        return [
            (item["relative_path"], item["sha256"])
            for item in report["workload_files"]
        ]

    if workload_signature(baseline) != workload_signature(candidate):
        raise SystemExit("reports are not comparable; workload file paths or bytes differ")
    baseline_config = baseline["config"]
    candidate_config = candidate["config"]
    if (baseline_config is None) != (candidate_config is None) or (
        baseline_config is not None
        and baseline_config["normalized_path"] != candidate_config["normalized_path"]
    ):
        raise SystemExit("reports are not comparable; normalized config paths differ")
    baseline_role_peaks = baseline["summary"].get("role_peak_rss_bytes")
    candidate_role_peaks = candidate["summary"].get("role_peak_rss_bytes")
    baseline_role_cpu = baseline["summary"].get("role_cpu_time_ms")
    candidate_role_cpu = candidate["summary"].get("role_cpu_time_ms")
    if (baseline_role_peaks is None) != (candidate_role_peaks is None) or (
        baseline_role_peaks is not None
        and sorted(baseline_role_peaks) != sorted(candidate_role_peaks)
    ):
        raise SystemExit("reports are not comparable; FE/BE resource roles differ")
    if (baseline_role_cpu is None) != (candidate_role_cpu is None) or (
        baseline_role_cpu is not None
        and (
            sorted(baseline_role_cpu) != sorted(candidate_role_cpu)
            or baseline_role_peaks is None
            or sorted(baseline_role_cpu) != sorted(baseline_role_peaks)
        )
    ):
        raise SystemExit("reports are not comparable; FE/BE CPU roles differ")

    effective_ratio = max(args.max_ratio, args.noise_ratio)
    metrics: dict[str, object] = {}
    passed = True
    for statistic in ("median", "p95_nearest_rank"):
        baseline_value = float(baseline["summary"]["elapsed_ms"][statistic])
        candidate_value = float(candidate["summary"]["elapsed_ms"][statistic])
        ratio = candidate_value / baseline_value if baseline_value else math.inf
        metric_passed = ratio <= effective_ratio
        passed &= metric_passed
        metrics[statistic] = {
            "baseline_ms": baseline_value,
            "candidate_ms": candidate_value,
            "ratio": ratio,
            "passed": metric_passed,
        }

    comparison = {
        "schema_version": 1,
        "baseline": str(args.baseline.resolve()),
        "baseline_git_head": baseline["git"]["head"],
        "candidate": str(args.candidate.resolve()),
        "candidate_git_head": candidate["git"]["head"],
        "workload_files": baseline["workload_files"],
        "commands": {
            "baseline": baseline["command"],
            "candidate": candidate["command"],
            "normalized": baseline["normalized_command"],
        },
        "configs": {
            "baseline": baseline["config"],
            "candidate": candidate["config"],
        },
        "max_ratio": args.max_ratio,
        "noise_ratio": args.noise_ratio,
        "effective_ratio": effective_ratio,
        "elapsed_ms": metrics,
        "controller_peak_rss": {
            "unit": baseline["max_rss_unit"],
            "resource_scope": baseline["resource_scope"],
            "baseline": baseline["summary"]["controller_max_rss"]["maximum"],
            "candidate": candidate["summary"]["controller_max_rss"]["maximum"],
        },
        "role_sampled_high_water_rss_bytes": (
            {
                role: {
                    "baseline": baseline_role_peaks[role],
                    "candidate": candidate_role_peaks[role],
                }
                for role in sorted(baseline_role_peaks)
            }
            if baseline_role_peaks is not None
            else None
        ),
        "role_cpu_time_ms": (
            {
                role: {
                    "baseline": baseline_role_cpu[role],
                    "candidate": candidate_role_cpu[role],
                }
                for role in sorted(baseline_role_cpu)
            }
            if baseline_role_cpu is not None
            else None
        ),
        "passed": passed,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        raise SystemExit(f"--output already exists: {args.output}")
    args.output.write_text(
        json.dumps(comparison, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
