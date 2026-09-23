#!/usr/bin/env python3
"""Reject large UEA-4A-4 regressions in two short native runs."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path


SCENARIO = "uea4/catalog-planning-performance"
WINDOWS = {(provider, mode, 0) for provider in ("iceberg", "paimon")
           for mode in ("normal", "slow-remote")}


class ComparisonError(ValueError):
    pass


def positive_number(value: object) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value > 0)


def load_run(root: Path) -> dict:
    report = json.loads((root / "uea4a4-performance.json").read_text())
    evidence = json.loads((root / "scenario-evidence.json").read_text())
    if report.get("schema_version") != 1 or report.get("status") != "short-regression-observed":
        raise ComparisonError(f"{root}: no successful short-regression report")
    if (evidence.get("scenario") != SCENARIO or evidence.get("outcome") != "passed"
            or evidence.get("cluster_size") != 3
            or evidence.get("launch_profile") != "performance"):
        raise ComparisonError(f"{root}: native 1FE+3BE scenario did not pass")
    if report.get("effective_config_sha256") != evidence.get(
            "effective_launch_config_semantics_sha256"):
        raise ComparisonError(f"{root}: report and effective config differ")
    for key in ("server_binary_sha256", "workload_sha256", "iceberg_fixture_sha256",
                "paimon_fixture_sha256", "effective_config_sha256"):
        value = report.get(key)
        if not isinstance(value, str) or len(value) != 64:
            raise ComparisonError(f"{root}: missing {key}")
    for key in ("source_revision", "source_tree_sha256", "cargo_lock_sha256",
                "runner_native_build_identity"):
        if not evidence.get(key):
            raise ComparisonError(f"{root}: missing provenance {key}")
    rss = report.get("peak_rss_bytes_by_role")
    if (not isinstance(rss, dict) or len(rss) != 4
            or not all(positive_number(value) for value in rss.values())):
        raise ComparisonError(f"{root}: incomplete FE/BE RSS observations")
    windows = report.get("windows")
    if not isinstance(windows, list) or len(windows) != len(WINDOWS):
        raise ComparisonError(f"{root}: expected one short window for each provider and mode")
    seen = set()
    for window in windows:
        key = (window.get("provider"), window.get("mode"), window.get("repetition"))
        if key not in WINDOWS or key in seen:
            raise ComparisonError(f"{root}: invalid or repeated window {key}")
        seen.add(key)
        for metric in ("throughput_per_second", "p95_micros"):
            if not positive_number(window.get(metric)):
                raise ComparisonError(f"{root}: invalid {metric} for {key}")
        if (window.get("duration_ms") != 20_000 or window.get("errors") != 0
                or not positive_number(window.get("completed"))
                or window.get("warmup_micros", 0) < 3_000_000):
            raise ComparisonError(f"{root}: incomplete or failed short window {key}")
        if key[1] == "slow-remote" and window.get("slow_gets", 0) + window.get(
                "slow_heads", 0) <= 0:
            raise ComparisonError(f"{root}: delayed S3 endpoint had no GET/HEAD for {key}")
    return {"report": report, "evidence": evidence}


def same_configuration(baseline: dict, candidate: dict) -> None:
    report_keys = ("workload_sha256", "iceberg_fixture_sha256",
                   "paimon_fixture_sha256")
    evidence_keys = ("base_config_sha256", "cargo_lock_sha256", "platform",
                     "runner_native_build_identity")
    if any(baseline["report"][key] != candidate["report"][key] for key in report_keys):
        raise ComparisonError("baseline and candidate workload or fixtures differ")
    if any(baseline["evidence"][key] != candidate["evidence"][key]
           for key in evidence_keys):
        raise ComparisonError("baseline and candidate base config, lockfile, platform, or runner differ")
    if baseline["report"]["server_binary_sha256"] == candidate["report"]["server_binary_sha256"]:
        raise ComparisonError("baseline and candidate used the same server binary")
    if (baseline["evidence"]["ended_unix_millis"] >
            candidate["evidence"]["started_unix_millis"]):
        raise ComparisonError("candidate ran before or overlapped the baseline")


def compare(baseline: dict, candidate: dict) -> dict:
    same_configuration(baseline, candidate)
    first = {(window["provider"], window["mode"]): window
             for window in baseline["report"]["windows"]}
    second = {(window["provider"], window["mode"]): window
              for window in candidate["report"]["windows"]}
    metrics = []
    for provider, mode in sorted(first):
        previous, current = first[(provider, mode)], second[(provider, mode)]
        throughput_ratio = current["throughput_per_second"] / previous["throughput_per_second"]
        p95_ratio = current["p95_micros"] / previous["p95_micros"]
        metrics.append({"provider": provider, "mode": mode,
                        "baseline_throughput_per_second": previous["throughput_per_second"],
                        "candidate_throughput_per_second": current["throughput_per_second"],
                        "throughput_ratio": throughput_ratio, "minimum_throughput_ratio": 0.5,
                        "baseline_p95_micros": previous["p95_micros"],
                        "candidate_p95_micros": current["p95_micros"],
                        "p95_ratio": p95_ratio, "maximum_p95_ratio": 2.0,
                        "passed": throughput_ratio >= 0.5 and p95_ratio <= 2.0})
    return {"kind": "short-regression-screen", "passed": all(row["passed"] for row in metrics),
            "metrics": metrics,
            "baseline_peak_rss_bytes_by_role": baseline["report"]["peak_rss_bytes_by_role"],
            "candidate_peak_rss_bytes_by_role": candidate["report"]["peak_rss_bytes_by_role"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = compare(load_run(args.baseline), load_run(args.candidate))
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"UEA-4A-4 short regression comparison invalid: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
