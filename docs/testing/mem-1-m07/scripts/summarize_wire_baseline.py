#!/usr/bin/env python3
"""Summarize frozen wire windows without hiding failures or changing gates."""

import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics


def quantile(values, probability):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(probability * len(ordered)) - 1)] if ordered else None


def spread(values):
    if not values or any(value is None for value in values):
        return None
    median = statistics.median(values)
    return (max(values) - min(values)) / median if median > 0 else None


def summarize(evidence, resources, gates, window_resources=None):
    workload = evidence["workload"]
    expected = sum(workload["concurrency"]) * workload["repetitions"] * len(workload["queries"])
    queries = {query["name"]: query for query in workload["queries"]}
    groups = defaultdict(list)
    successes = 0
    for sample in evidence["samples"]:
        groups[(sample["query"], sample["concurrency"], sample["repetition"])].append(sample)
    windows = []
    for name, query in queries.items():
        for concurrency in workload["concurrency"]:
            for repetition in range(workload["repetitions"]):
                samples = groups.get((name, concurrency, repetition), [])
                valid = [sample for sample in samples if sample["observation"]["error"] is None
                         and (query["expected_rows"] is None
                              or sample["observation"]["rows"] == query["expected_rows"])]
                successes += len(valid)
                elapsed_us = (max(s["ended_micros"] for s in samples)
                              - min(s["started_micros"] for s in samples)) if samples else 0
                first_rows = [s["observation"]["first_row_micros"] for s in valid
                              if s["observation"]["first_row_micros"] is not None]
                completion = [s["observation"]["elapsed_micros"] for s in valid]
                errors = defaultdict(int)
                for sample in samples:
                    error = sample["observation"]["error"]
                    if error is not None:
                        errors[error] += 1
                windows.append({
                    "query": name, "concurrency": concurrency, "repetition": repetition,
                    "observed": len(samples), "successes": len(valid),
                    "failures": len(samples) - len(valid), "missing": max(0, concurrency - len(samples)),
                    "goodput_per_second": len(valid) * 1000000 / elapsed_us if elapsed_us > 0 else None,
                    "first_complete_row_p95_micros": quantile(first_rows, .95),
                    "completion_p95_micros": quantile(completion, .95),
                    "completion_p99_micros": quantile(completion, .99),
                    "errors": dict(sorted(errors.items())),
                    "row_digests": sorted({s["observation"]["row_sha256"] for s in valid}),
                    "metadata_digests": sorted({s["observation"]["metadata_sha256"] for s in valid}),
                    "wire_bytes": sum(s["observation"]["wire_bytes"] for s in samples),
                })
    quality = []
    bounds = gates["baseline_quality"]
    for name in queries:
        for concurrency in workload["concurrency"]:
            selected = [w for w in windows if w["query"] == name and w["concurrency"] == concurrency]
            values = {key: spread([w[key] for w in selected]) for key in (
                "goodput_per_second", "first_complete_row_p95_micros",
                "completion_p95_micros", "completion_p99_micros")}
            complete = all(w["observed"] == concurrency and w["failures"] == 0 for w in selected)
            stable = all(value is not None and value <= bound for value, bound in (
                (values["goodput_per_second"], bounds["window_goodput_max_relative_spread"]),
                (values["first_complete_row_p95_micros"], bounds["p95_max_relative_spread"]),
                (values["completion_p95_micros"], bounds["p95_max_relative_spread"]),
                (values["completion_p99_micros"], bounds["p99_max_relative_spread"])))
            quality.append({"query": name, "concurrency": concurrency,
                            "relative_spread": values, "complete_without_failures": complete,
                            "within_frozen_noise_gates": stable})
    role_samples = defaultdict(list)
    for sample in resources["samples"]:
        role_samples[sample["role"]].append(sample)
    cpu = 0
    cpu_complete = set(role_samples) == {"fe", "be-0", "be-1", "be-2"}
    role_resources = {}
    expected_windows = len(queries) * len(workload["concurrency"]) * workload["repetitions"]
    if window_resources is not None:
        expected_names = {f"{name}-{concurrency}-{repetition}"
                          for name in queries for concurrency in workload["concurrency"]
                          for repetition in range(workload["repetitions"])}
        observed_names = [window["run_id"] for window in window_resources]
        cpu_complete &= all({sample["role"] for sample in window["samples"]} == set(role_samples)
                            for window in window_resources)
        cpu_complete &= (len(observed_names) == expected_windows
                         and len(set(observed_names)) == expected_windows
                         and set(observed_names) == expected_names)
    for role, samples in sorted(role_samples.items()):
        samples.sort(key=lambda sample: sample["elapsed_millis"])
        identities = {(s["pid"], s["process_start_token"]) for s in samples}
        exact = len(identities) == 1
        groups = [samples] if window_resources is None else [
            [sample for sample in window["samples"] if sample["role"] == role]
            for window in window_resources]
        delta = 0
        for group in groups:
            readable = all(s["cpu_user_nanos"] is not None and s["cpu_system_nanos"] is not None
                           and s["unavailable_reason"] is None for s in group)
            same_identity = {(s["pid"], s["process_start_token"]) for s in group} == identities
            exact &= same_identity
            count_valid = len(group) >= 2 if window_resources is None else len(group) == 2
            if not (readable and same_identity and count_valid):
                delta = None
                break
            total = lambda sample: sample["cpu_user_nanos"] + sample["cpu_system_nanos"]
            difference = total(group[-1]) - total(group[0])
            if difference < 0:
                delta = None
                break
            delta += difference
        if delta is None or not groups:
            cpu_complete = False
        else:
            cpu += delta
        role_resources[role] = {"cpu_seconds": delta / 1e9 if delta is not None else None,
                                "rss_high_water_bytes": max((s["rss_bytes"] for s in samples
                                                             if s["rss_bytes"] is not None), default=None),
                                "exact_process_identity": exact}
    complete = len(evidence["samples"]) == expected and evidence["run_error"] is None
    paired = workload["repetitions"] == bounds["paired_runs"]
    clean = complete and paired and successes == expected and not evidence["deterministic_mismatch_queries"]
    stable = all(q["within_frozen_noise_gates"] for q in quality)
    return {"schema_version": 1, "status": "BASELINE_QUALITY_PASS" if clean and stable and cpu_complete
            else "INCONCLUSIVE_OR_FAILED_BASELINE_RETAINED",
            "expected_samples": expected, "observed_samples": len(evidence["samples"]),
            "successes": successes, "failures": len(evidence["samples"]) - successes,
            "complete_run": complete, "run_error": evidence["run_error"],
            "paired_repetitions_complete": paired,
            "deterministic_mismatch_queries": evidence["deterministic_mismatch_queries"],
            "cpu_evidence_complete": cpu_complete, "roles": role_resources,
            "cpu_scope": "explicit per-window connect through actual owner convergence" if window_resources is not None
                         else "whole-run diagnostic cumulative CPU",
            "cpu_windows": len(window_resources) if window_resources is not None else None,
            "fe_plus_be_cpu_seconds_per_success": cpu / 1e9 / successes if successes and cpu_complete else None,
            "quality": quality, "windows": windows,
            "acceptance_scope": "old-path baseline only; no candidate, transport or Linux acceptance"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--gates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--window-resources-dir", type=Path)
    args = parser.parse_args()
    window_paths = sorted(args.window_resources_dir.glob("window-cpu-*.json")) if args.window_resources_dir else None
    windows = [json.loads(path.read_text()) for path in window_paths] if window_paths is not None else None
    report = summarize(*(json.loads(path.read_text()) for path in (args.input, args.resources, args.gates)), windows)
    report["source_sha256"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in (args.input, args.resources, args.gates)}
    if window_paths is not None:
        report["window_cpu_sha256"] = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                                       for path in window_paths}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("status", "observed_samples", "successes", "failures")}))
