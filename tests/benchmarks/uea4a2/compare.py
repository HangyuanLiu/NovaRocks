#!/usr/bin/env python3
"""Validate raw UEA-4A-2 receipts and compare frozen G0 A/A with G1-G3."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import sys

from run import GROUPS, SCENARIO, sha256, validate_frozen_manifest

RECEIPT = "uea4-iceberg-range-performance/uea4a2-performance.json"
BE_ROLES = ("be-0", "be-1", "be-2")


class ComparisonError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ComparisonError(message)


def number(value: object, label: str, *, positive: bool = False) -> float:
    require(isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and (value > 0 if positive else value >= 0),
            f"invalid {label}")
    return float(value)


def percentile(values: list[float], rank: int) -> float:
    require(bool(values), "percentile needs samples")
    return sorted(values)[math.ceil(rank * len(values) / 100) - 1]


def _hash_file(root: Path, evidence: dict, key: str) -> None:
    item = evidence.get(key)
    require(isinstance(item, dict) and set(item) == {"path", "sha256"},
            f"missing {key} evidence attachment")
    path = (root / item["path"]).resolve()
    require(path.is_relative_to(root.resolve()) and path.is_file(), f"unsafe or missing {key} attachment")
    require(sha256(path) == item["sha256"], f"{key} attachment hash mismatch")


def load_group(root: Path, name: str, manifest: dict, hashes: dict) -> dict:
    base = root / name
    report_path = base / RECEIPT
    evidence_path = report_path.parent / "scenario-evidence.json"
    require(report_path.is_file() and evidence_path.is_file(), f"{name} lacks native receipts")
    report = json.loads(report_path.read_text())
    evidence = json.loads(evidence_path.read_text())
    require(evidence.get("scenario") == SCENARIO and evidence.get("outcome") == "passed"
            and evidence.get("cluster_size") == 3
            and evidence.get("launch_profile") == "performance", f"{name} native scenario failed")
    require(report.get("schema_version") == 1 and report.get("group") == name,
            f"{name} wrong report schema/group")
    require(report.get("topology") == {"fe": 1, "be": 3}, f"{name} wrong topology")
    require(report.get("workload_sha256") == hashes["workload_sha256"]
            and report.get("fixture_manifest_sha256") == hashes["fixture_manifest_sha256"]
            and report.get("base_config_sha256") == hashes["base_config_sha256"]
            and report.get("runner_sha256") == hashes["runner_sha256"]
            and report.get("binary_sha256") == hashes["binary_sha256_by_group"][name.split("-")[0]],
            f"{name} input hash mismatch")
    require(report.get("effective_config_sha256") == evidence.get("effective_launch_config_semantics_sha256"),
            f"{name} rendered config mismatch")
    for key in ("query_trace", "resource_trace", "control_trace", "operation_trace"):
        _hash_file(report_path.parent, report.get("attachments", {}), key)
    windows = report.get("windows")
    workloads = manifest["frozen"]["short_query_workloads"]
    require(isinstance(windows, list) and len(windows) == 3 * len(workloads),
            f"{name} incomplete 3x120s windows")
    seen = set()
    derived = {}
    for window in windows:
        workload, repetition = window.get("workload"), window.get("repetition")
        key = (workload, repetition)
        require(workload in workloads and repetition in (0, 1, 2) and key not in seen,
                f"{name} duplicate/unknown window {key}")
        seen.add(key)
        require(window.get("duration_ms") == 120_000 and window.get("warmup_drained") is True,
                f"{name} incomplete warmup/window {key}")
        started = number(window.get("started_ms"), f"{name} start")
        deadline = started + 120_000
        queries = window.get("queries")
        require(isinstance(queries, list) and len(queries) >= 1000,
                f"{name} insufficient query samples {key}")
        successful_latency = []
        completed_in_window = 0
        for query in queries:
            launched = number(query.get("started_ms"), "query start")
            ended = number(query.get("ended_ms"), "query terminal")
            require(started <= launched < deadline and ended >= launched, "query outside window or missing terminal")
            require(query.get("status") == "success", f"{name} failed or timed-out normal query {key}")
            successful_latency.append(ended - launched)
            completed_in_window += ended < deadline
        require(len(successful_latency) >= 1000, f"{name} insufficient successful short samples {key}")
        require(window.get("tail_drained") is True
                and number(window.get("tail_drain_ms"), "tail drain") >= 0,
                f"{name} missing tail drain {key}")
        derived[key] = {
            "throughput": completed_in_window / 120.0,
            "p95_ms": percentile(successful_latency, 95),
            "p99_ms": percentile(successful_latency, 99),
        }
    controls = report.get("controls")
    types = manifest["frozen"]["control_operation_types"]
    require(isinstance(controls, list), f"{name} lacks controls")
    control_map = {}
    for row in controls:
        key = (row.get("operation"), row.get("mode"))
        require(key[0] in types and key[1] in ("no-scan", "saturated") and key not in control_map,
                f"{name} invalid control cohort {key}")
        samples = row.get("latency_ms")
        require(isinstance(samples, list) and len(samples) >= 1000,
                f"{name} insufficient control samples {key}")
        control_map[key] = percentile([number(value, "control latency") for value in samples], 99)
        if key[1] == "saturated":
            require(row.get("hold_filled_window") is True
                    and row.get("flow_has_pending_demand") is True
                    and number(row.get("flow_bytes_growth"), "flow bytes", positive=True) > 0,
                    f"{name} lacks hold and flowing saturation evidence {key}")
    require(set(control_map) == {(operation, mode) for operation in types
                                 for mode in ("no-scan", "saturated")},
            f"{name} missing control cohort")
    rss = report.get("rss")
    require(isinstance(rss, dict) and set(rss) == set(manifest["frozen"]["rss_workloads"]),
            f"{name} RSS workload coverage")
    for workload, roles in rss.items():
        require(isinstance(roles, dict) and set(roles) == set(BE_ROLES),
                f"{name} missing per BE RSS {workload}")
        for role, rounds in roles.items():
            require(isinstance(rounds, list) and len(rounds) == 3,
                    f"{name} missing RSS rounds {workload}/{role}")
            for sample in rounds:
                for field in ("idle_bytes", "peak_bytes", "steady_bytes", "post_drain_bytes"):
                    number(sample.get(field), f"{name} {workload}/{role}/{field}", positive=True)
                require(sample.get("observed_post_drain_seconds", 0) >=
                        manifest["frozen"]["rss_limits_by_workload"][workload]["post_drain_observation_secs"],
                        f"{name} short post-drain observation")
                require(sample.get("active_current") == 0 and sample.get("active_next") == 0
                        and sample.get("active_claims") == 0 and sample.get("undrained_operations") == 0,
                        f"{name} active objects after drain")
    require(report.get("attribution_complete") is True and report.get("oracle_passed") is True,
            f"{name} missing byte/CPU/connection/position evidence")
    return {"derived": derived, "controls": control_map, "rss": rss}


def median_metric(group: dict, workload: str, metric: str) -> float:
    return statistics.median(group["derived"][(workload, repetition)][metric]
                             for repetition in range(3))


def rss_stat(rounds: list[dict], field: str) -> float:
    values = [sample[field] for sample in rounds]
    return max(values) if field == "peak_bytes" else statistics.median(values)


def compare(root: Path, manifest_path: Path) -> dict:
    manifest = validate_frozen_manifest(manifest_path)
    hashes = json.loads((root / "input-hashes.json").read_text())
    require(hashes.get("workload_sha256") == sha256(manifest_path), "workload hash mismatch")
    groups = {name: load_group(root, name, manifest, hashes) for name in GROUPS}
    checks = []
    for workload in manifest["frozen"]["short_query_workloads"]:
        for metric, limit in (("throughput", 0.10), ("p95_ms", 0.20)):
            values = [groups[name]["derived"][(workload, i)][metric]
                      for name in ("g0-a", "g0-b") for i in range(3)]
            middle = statistics.median(values)
            spread = (max(values) - min(values)) / middle if middle else math.inf
            checks.append({"gate": "G0 A/A", "workload": workload, "metric": metric,
                           "observed": spread, "limit": limit, "passed": spread <= limit})
        baseline = statistics.median(median_metric(groups[name], workload, "throughput")
                                     for name in ("g0-a", "g0-b"))
        baseline_p95 = statistics.median(median_metric(groups[name], workload, "p95_ms")
                                         for name in ("g0-a", "g0-b"))
        for name in ("g1", "g2", "g3"):
            throughput = median_metric(groups[name], workload, "throughput")
            p95 = median_metric(groups[name], workload, "p95_ms")
            if name == "g3":
                checks += [
                    {"gate": "G3 throughput", "workload": workload, "observed": 1 - throughput / baseline,
                     "limit": 0.15, "passed": throughput >= baseline * 0.85},
                    {"gate": "G3 p95", "workload": workload, "observed": p95 / baseline_p95 - 1,
                     "limit": 0.25, "passed": p95 <= baseline_p95 * 1.25},
                ]
    for name in GROUPS:
        group = groups[name]
        for operation in manifest["frozen"]["control_operation_types"]:
            p99 = group["controls"][(operation, "saturated")]
            baseline = group["controls"][(operation, "no-scan")]
            checks.append({"gate": "control p99", "group": name, "operation": operation,
                           "observed": p99, "limit_ms": min(2000, baseline * 3),
                           "passed": p99 <= 2000 and p99 <= baseline * 3})
    for workload, limits in manifest["frozen"]["rss_limits_by_workload"].items():
        for role in BE_ROLES:
            baseline = groups["g0-a"]["rss"][workload][role]
            for name in ("g1", "g2", "g3"):
                current = groups[name]["rss"][workload][role]
                for kind in ("peak", "steady"):
                    prior = rss_stat(baseline, kind + "_bytes")
                    now = rss_stat(current, kind + "_bytes")
                    growth = now - prior
                    relative = growth / prior
                    checks.append({"gate": f"RSS {kind}", "group": name, "workload": workload,
                                   "role": role, "growth_bytes": growth, "relative_growth": relative,
                                   "passed": growth <= limits[kind + "_absolute_growth_bytes"]
                                   and relative <= limits[kind + "_relative_growth_limit"]})
                excess = max(sample["post_drain_bytes"] - sample["idle_bytes"] for sample in current)
                checks.append({"gate": "RSS post-drain", "group": name, "workload": workload,
                               "role": role, "excess_bytes": excess,
                               "passed": excess <= limits["post_drain_excess_bytes_limit"]})
    return {"valid": True, "passed": all(check["passed"] for check in checks), "checks": checks}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = compare(args.output, args.manifest)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"valid": False, "reason": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
