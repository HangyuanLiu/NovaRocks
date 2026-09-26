#!/usr/bin/env python3
"""Run the frozen UEA-4A-2 native performance protocol, failing closed."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys

SCENARIO = "uea4/iceberg-range-performance"
GROUPS = ("g0-a", "g0-b", "g1", "g2", "g3")
FROZEN_FIELDS = (
    "source_revision", "cargo_lock_sha256", "fixture_manifest_path",
    "fixture_manifest_sha256", "base_config_path", "base_config_sha256",
    "fixed_machine_memory_bytes", "warmup_and_cache_protocol", "sql_and_oracle",
    "network_protocol", "placement_and_tasks", "cpu_wait_sampling_method",
    "control_operation_types", "short_query_workloads", "rss_workloads",
    "prefetch_input_bytes_per_stream", "prefetch_max_candidates",
    "parquet_tail_probe_bytes", "prefetch_pause_release_ms", "prefetch_rearm_ms",
    "prefetch_progress_bucket_ms", "parameter_rationale", "rss_sample_interval_ms",
    "rss_limits_by_workload", "rss_limit_rationale",
)
RSS_FIELDS = (
    "peak_relative_growth_limit", "peak_absolute_growth_bytes",
    "steady_relative_growth_limit", "steady_absolute_growth_bytes",
    "post_drain_observation_secs", "post_drain_excess_bytes_limit",
)


class PreflightError(ValueError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def existing_file(path: Path, label: str) -> Path:
    if not path.is_file():
        raise PreflightError(f"{label} is missing: {path}")
    return path.resolve()


def validate_pilot_fixture(path: Path) -> None:
    """Verify the published one-file Iceberg input used for G0 A/A."""
    fixture = json.loads(path.read_text())
    if (fixture.get("fixture_kind") != "uea4a2-iceberg-short-v1"
            or fixture.get("schema_version") != 1
            or fixture.get("row_count") != 4096
            or fixture.get("data_file_count") != 1):
        raise PreflightError("pilot requires the published one-file UEA-4A-2 Iceberg fixture")
    ready = existing_file(path.parent / "READY", "fixture READY").read_text().strip()
    if ready != "sha256:" + sha256(path):
        raise PreflightError("pilot fixture READY does not match manifest")
    artifacts = fixture.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise PreflightError("pilot fixture has no hashed artifacts")
    for name, digest in artifacts.items():
        if (not isinstance(name, str) or Path(name).name != name
                or not isinstance(digest, str)
                or sha256(existing_file(path.parent / name, "fixture artifact")) != digest):
            raise PreflightError(f"pilot fixture artifact hash mismatch: {name}")


def validate_manifest(path: Path) -> dict:
    document = json.loads(existing_file(path, "workload manifest").read_text())
    if document.get("schema_version") != 1 or document.get("scenario") != SCENARIO:
        raise PreflightError("wrong workload schema or scenario")
    if document.get("topology") != {
        "frontend_count": 1, "backend_count": 3, "launch_profile": "performance"
    }:
        raise PreflightError("workload must require native 1FE+3BE performance mode")
    measure = document.get("measurement", {})
    expected = {
        "repetitions": 3, "window_seconds": 120,
        "minimum_short_query_samples_per_window": 1000,
        "minimum_control_samples_per_type_per_group": 1000,
        "short_query_throughput_regression_limit": 0.15,
        "short_query_p95_growth_limit": 0.25,
        "aa_throughput_spread_limit": 0.10,
        "aa_p95_spread_limit": 0.20,
        "control_p99_seconds_limit": 2.0,
        "control_p99_baseline_multiplier": 3.0,
    }
    for key, value in expected.items():
        if measure.get(key) != value:
            raise PreflightError(f"measurement.{key} must be {value}")
    return document


def validate_frozen_manifest(path: Path) -> dict:
    document = validate_manifest(path)
    measure = document["measurement"]
    if not isinstance(measure.get("sample_extension_rule"), str) or not measure["sample_extension_rule"].strip():
        raise PreflightError("measurement.sample_extension_rule is not frozen")
    frozen = document.get("frozen", {})
    missing = [key for key in FROZEN_FIELDS if frozen.get(key) in (None, "", [], {})]
    if missing:
        raise PreflightError("unfrozen fields: " + ", ".join(missing))
    if not isinstance(frozen["fixed_machine_memory_bytes"], int) or frozen["fixed_machine_memory_bytes"] <= 0:
        raise PreflightError("fixed machine memory must be positive")
    for key in ("prefetch_input_bytes_per_stream", "prefetch_max_candidates",
                "parquet_tail_probe_bytes", "prefetch_pause_release_ms",
                "prefetch_rearm_ms", "prefetch_progress_bucket_ms", "rss_sample_interval_ms"):
        value = frozen[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise PreflightError(f"frozen.{key} must be a positive integer")
    if frozen["prefetch_max_candidates"] < 2:
        raise PreflightError("formal multiple-small-file workload needs N >= 2")
    if frozen["prefetch_rearm_ms"] % frozen["prefetch_progress_bucket_ms"]:
        raise PreflightError("rearm must be a multiple of progress bucket")
    rss = frozen["rss_limits_by_workload"]
    if not isinstance(rss, dict) or not rss or set(rss) != set(frozen["rss_workloads"]):
        raise PreflightError("RSS limits must cover every frozen RSS workload")
    for workload, limits in rss.items():
        if not isinstance(limits, dict) or set(limits) != set(RSS_FIELDS):
            raise PreflightError(f"incomplete RSS limits for {workload}")
        for name, value in limits.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
                raise PreflightError(f"invalid RSS limit {workload}.{name}")
    for field in ("fixture_manifest", "base_config"):
        file = existing_file(Path(frozen[field + "_path"]), field)
        if sha256(file) != frozen[field + "_sha256"]:
            raise PreflightError(f"{field} SHA-256 mismatch")
    return document


def validate_pilot_manifest(path: Path) -> dict:
    """Require explicit G0 pilot queries without pre-judging formal thresholds."""
    document = validate_manifest(path)
    pilot = document.get("pilot")
    if not isinstance(pilot, dict) or pilot.get("catalog") != "from-fixture-manifest":
        raise PreflightError("pilot catalog must come from explicit fixture manifest")
    queries = pilot.get("queries")
    if not isinstance(queries, list) or not queries:
        raise PreflightError("pilot needs explicit query definitions")
    names = set()
    for query in queries:
        if not isinstance(query, dict) or set(query) != {
            "name", "sql", "expected_row_count", "clients", "min_query_interval_ms", "warmup_ms"
        }:
            raise PreflightError("pilot query has incomplete fields")
        if (not isinstance(query["name"], str) or not query["name"]
                or query["name"] in names or not isinstance(query["sql"], str)
                or "${table}" not in query["sql"]):
            raise PreflightError("pilot query name or SQL is invalid")
        names.add(query["name"])
        for key in ("expected_row_count", "clients", "min_query_interval_ms", "warmup_ms"):
            value = query[key]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise PreflightError(f"pilot query {key} must be positive")
    if set(pilot.get("rss_workloads", [])) != names:
        raise PreflightError("pilot RSS workloads must match the pilot queries")
    return document


def validate_pilot_receipt(path: Path, evidence_path: Path, name: str,
                           manifest: dict, hashes: dict) -> dict:
    """Derive each pilot window from hashed raw queries and require complete RSS."""
    report = json.loads(existing_file(path, f"{name} pilot receipt").read_text())
    evidence = json.loads(existing_file(evidence_path, f"{name} native evidence").read_text())
    if (evidence.get("scenario") != SCENARIO or evidence.get("outcome") != "passed"
            or evidence.get("cluster_size") != 3
            or evidence.get("launch_profile") != "performance"):
        raise PreflightError(f"{name} pilot did not finish native 1FE+3BE scenario")
    version = report.get("schema_version")
    if (version not in (1, 2, 3) or report.get("group") != name
            or report.get("topology") != {"fe": 1, "be": 3}
            or report.get("binary_sha256") != hashes["binary_sha256_by_group"]["g0"]
            or report.get("runner_sha256") != hashes["runner_sha256"]
            or report.get("workload_sha256") != hashes["workload_sha256"]
            or report.get("fixture_manifest_sha256") != hashes["fixture_manifest_sha256"]
            or report.get("base_config_sha256") != hashes["base_config_sha256"]):
        raise PreflightError(f"{name} pilot input/provenance mismatch")
    effective = report.get("effective_config_sha256")
    if (not isinstance(effective, str) or len(effective) != 64
            or any(character not in "0123456789abcdef" for character in effective)
            or effective != evidence.get("effective_launch_config_semantics_sha256")):
        raise PreflightError(f"{name} pilot effective config semantics mismatch")
    workloads = [query["name"] for query in manifest["pilot"]["queries"]]
    windows = report.get("windows")
    if not isinstance(windows, list) or len(windows) != len(workloads) * 3:
        raise PreflightError(f"{name} pilot has incomplete measurement windows")
    attachments = report.get("attachments")
    if not isinstance(attachments, dict):
        raise PreflightError(f"{name} pilot lacks raw trace attachments")
    raw_paths = {}
    for key in ("query_trace", "resource_trace", "proxy_event_trace",
                "proxy_connection_trace"):
        item = attachments.get(key)
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            raise PreflightError(f"{name} pilot lacks {key}")
        raw = (path.parent / item["path"]).resolve()
        if not raw.is_relative_to(path.parent.resolve()) or not raw.is_file() or sha256(raw) != item["sha256"]:
            raise PreflightError(f"{name} pilot invalid {key} attachment")
        raw_paths[key] = raw
        if key.startswith("proxy_"):
            required = ({"kind", "request_id", "connection_id", "protocol",
                         "method", "object_id", "read_class", "range", "bytes"}
                        if key == "proxy_event_trace" else {"kind", "connection_id"})
            if version >= 2:
                required.add("elapsed_millis")
            count = 0
            with raw.open() as source:
                for line in source:
                    record = json.loads(line)
                    if not isinstance(record, dict) or set(record) != required:
                        raise PreflightError(f"{name} pilot malformed {key} record")
                    if version >= 2 and (isinstance(record["elapsed_millis"], bool)
                                         or not isinstance(record["elapsed_millis"], int)
                                         or record["elapsed_millis"] < 0):
                        raise PreflightError(f"{name} pilot invalid proxy event time")
                    count += 1
            if count == 0:
                raise PreflightError(f"{name} pilot empty {key} attachment")
    raw_windows = json.loads(raw_paths["query_trace"].read_text())
    if raw_windows != windows:
        raise PreflightError(f"{name} report windows differ from hashed raw query trace")
    seen = set()
    derived = {}
    for window in raw_windows:
        key = (window.get("workload"), window.get("repetition"))
        if (key[0] not in workloads or key[1] not in (0, 1, 2) or key in seen
                or window.get("duration_ms") != 120_000
                or window.get("warmup_drained") is not True
                or window.get("tail_drained") is not True):
            raise PreflightError(f"{name} pilot invalid window {key}")
        seen.add(key)
        start = window.get("started_ms")
        if isinstance(start, bool) or not isinstance(start, (int, float)) or not math.isfinite(start) or start < 0:
            raise PreflightError(f"{name} pilot invalid window start {key}")
        deadline = start + 120_000
        queries = window.get("queries")
        if not isinstance(queries, list) or len(queries) < 1000:
            raise PreflightError(f"{name} pilot lacks raw short-query samples {key}")
        in_window = 0
        latencies = []
        for query in queries:
            if (query.get("status") != "success"
                    or not isinstance(query.get("started_ms"), (int, float))
                    or not isinstance(query.get("ended_ms"), (int, float))
                    or isinstance(query["started_ms"], bool)
                    or isinstance(query["ended_ms"], bool)
                    or not math.isfinite(query["started_ms"])
                    or not math.isfinite(query["ended_ms"])
                    or not start <= query["started_ms"] < deadline
                    or query["ended_ms"] <= query["started_ms"]):
                raise PreflightError(f"{name} pilot has failed or incomplete query {key}")
            in_window += query["ended_ms"] <= deadline
            latencies.append(query["ended_ms"] - query["started_ms"])
        if in_window < 1000:
            raise PreflightError(f"{name} pilot has only {in_window} in-window successful short queries {key}")
        if version >= 2:
            if (not isinstance(window.get("proxy_observation_start_ms"), int)
                    or not isinstance(window.get("proxy_observation_end_ms"), int)
                    or window["proxy_observation_end_ms"] <= window["proxy_observation_start_ms"]
                    or any(not isinstance(window.get(field), int) or window[field] < 0 for field in (
                        "proxy_gets", "proxy_heads", "proxy_upstream_bytes",
                        "proxy_completed_bytes", "proxy_connections_accepted", "proxy_event_overflow"))
                    or window["proxy_event_overflow"] != 0):
                raise PreflightError(f"{name} pilot proxy window counters are incomplete")
        if version >= 3:
            attempts = window.get("proxy_upstream_connect_attempts")
            established = window.get("proxy_upstream_connections_established")
            protocols = [window.get(field) for field in (
                "proxy_upstream_http1_responses", "proxy_upstream_http2_responses",
                "proxy_upstream_other_protocol_responses")]
            if (isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0
                    or isinstance(established, bool) or not isinstance(established, int)
                    or established < 0 or established > attempts
                    or any(isinstance(value, bool) or not isinstance(value, int) or value < 0
                           for value in protocols)):
                raise PreflightError(f"{name} pilot upstream connection counters are incomplete")
        latencies.sort()
        derived[key] = {"completed_in_window": in_window,
                        "throughput_per_second": in_window / 120,
                        "p95_ms": latencies[math.ceil(0.95 * len(latencies)) - 1]}
    rss = report.get("rss")
    if not isinstance(rss, dict) or set(rss) != set(manifest["pilot"]["rss_workloads"]):
        raise PreflightError(f"{name} pilot missing RSS workloads")
    for workload, roles in rss.items():
        if not isinstance(roles, dict) or set(roles) != {"be-0", "be-1", "be-2"}:
            raise PreflightError(f"{name} pilot missing per BE RSS for {workload}")
        for role, rounds in roles.items():
            if not isinstance(rounds, list) or len(rounds) != 3:
                raise PreflightError(f"{name} pilot missing RSS rounds for {workload}/{role}")
            for sample in rounds:
                for field in ("idle_bytes", "peak_bytes", "steady_bytes", "post_drain_bytes"):
                    value = sample.get(field)
                    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                        raise PreflightError(f"{name} pilot invalid RSS {workload}/{role}/{field}")
    if version >= 2:
        validate_pilot_rss_trace(name, raw_windows, rss, raw_paths["resource_trace"])
    if version >= 3:
        validate_pilot_runner_proxy_trace(name, raw_paths["resource_trace"])
        lifetime = report.get("proxy_lifetime")
        fields = ("upstream_connect_attempts", "upstream_connections_established",
                  "upstream_http1_responses", "upstream_http2_responses",
                  "upstream_other_protocol_responses")
        if (not isinstance(lifetime, dict) or set(lifetime) != set(fields)
                or any(isinstance(lifetime[field], bool)
                       or not isinstance(lifetime[field], int)
                       or lifetime[field] < 0 for field in fields)
                or lifetime["upstream_connections_established"] > lifetime["upstream_connect_attempts"]
                or lifetime["upstream_connect_attempts"] < sum(
                    window["proxy_upstream_connect_attempts"] for window in raw_windows)
                or lifetime["upstream_connections_established"] < sum(
                    window["proxy_upstream_connections_established"] for window in raw_windows)):
            raise PreflightError(f"{name} pilot proxy lifetime counters are invalid")
    return {"metrics": derived, "effective_config_sha256": effective}


def validate_pilot_runner_proxy_trace(name: str, path: Path) -> None:
    trace = json.loads(path.read_text())
    identity = trace.get("processes", {}).get("runner-proxy")
    if not isinstance(identity, dict):
        raise PreflightError(f"{name} pilot lacks runner-proxy process identity")
    samples = [sample for sample in trace.get("samples", [])
               if sample.get("role") == "runner-proxy"]
    if not samples or any(
            sample.get("pid") != identity.get("pid")
            or sample.get("process_start_token") != identity.get("process_start_token")
            or sample.get("unavailable_reason") is not None
            or any(isinstance(sample.get(field), bool)
                   or not isinstance(sample.get(field), int)
                   or sample[field] < 0
                   for field in ("elapsed_millis", "rss_bytes", "cpu_user_nanos", "cpu_system_nanos"))
            for sample in samples):
        raise PreflightError(f"{name} pilot runner-proxy resource samples are incomplete")


def validate_pilot_rss_trace(name: str, windows: list, rss: dict, path: Path) -> None:
    """Recompute every v2 BE RSS summary from the hashed process trace."""
    trace = json.loads(path.read_text())
    if trace.get("schema_version") != 3 or not isinstance(trace.get("processes"), dict):
        raise PreflightError(f"{name} pilot resource trace has the wrong schema")
    samples = trace.get("samples")
    if not isinstance(samples, list):
        raise PreflightError(f"{name} pilot resource trace has no samples")
    roles = ("be-0", "be-1", "be-2")
    by_role = {role: [] for role in roles}
    for sample in samples:
        role = sample.get("role") if isinstance(sample, dict) else None
        if role not in by_role:
            continue
        identity = trace["processes"].get(role)
        if (not isinstance(identity, dict)
                or sample.get("pid") != identity.get("pid")
                or sample.get("process_start_token") != identity.get("process_start_token")
                or sample.get("unavailable_reason") is not None
                or isinstance(sample.get("elapsed_millis"), bool)
                or not isinstance(sample.get("elapsed_millis"), int)
                or isinstance(sample.get("rss_bytes"), bool)
                or not isinstance(sample.get("rss_bytes"), int)
                or sample["rss_bytes"] <= 0):
            raise PreflightError(f"{name} pilot resource identity or RSS sample is invalid for {role}")
        by_role[role].append(sample)
    if any(not by_role[role] for role in roles):
        raise PreflightError(f"{name} pilot resource trace lacks a BE")

    def values(role: str, first: int, last: int) -> list[int]:
        result = [sample["rss_bytes"] for sample in by_role[role]
                  if first <= sample["elapsed_millis"] <= last]
        if not result:
            raise PreflightError(f"{name} pilot RSS trace has no samples for {role} at {first}..{last}")
        return sorted(result)

    def median_upper(items: list[int]) -> int:
        return items[len(items) // 2]

    for window in windows:
        timing = window.get("rss_timing")
        if not isinstance(timing, dict) or set(timing) != {
                "idle_start", "idle_end", "measure_start", "measure_end", "post_start", "post_end"}:
            raise PreflightError(f"{name} pilot lacks exact RSS markers")
        bounds = list(timing.values())
        if (any(isinstance(value, bool) or not isinstance(value, int) for value in bounds)
                or not timing["idle_start"] < timing["idle_end"] <= timing["measure_start"]
                < timing["measure_end"] <= timing["post_start"] < timing["post_end"]
                or timing["measure_start"] != window["started_ms"]
                or not isinstance(window.get("duration_ms"), int)
                or abs(timing["measure_end"] - timing["measure_start"] - window["duration_ms"]) > 1_000):
            raise PreflightError(f"{name} pilot RSS markers are invalid")
        steady_start = timing["measure_start"] + (timing["measure_end"] - timing["measure_start"]) // 2
        for role in roles:
            round_report = rss[window["workload"]][role][window["repetition"]]
            expected = {
                "idle_bytes": median_upper(values(role, timing["idle_start"], timing["idle_end"])),
                "peak_bytes": values(role, timing["idle_end"], timing["post_end"])[-1],
                "steady_bytes": median_upper(values(role, steady_start, timing["measure_end"])),
                "post_drain_bytes": median_upper(values(role, timing["post_start"], timing["post_end"])),
            }
            if any(round_report[field] != value for field, value in expected.items()):
                raise PreflightError(f"{name} pilot RSS summary differs from raw {role} trace")
            observed = (timing["post_end"] - timing["post_start"]) / 1000
            actual_observed = round_report.get("observed_post_drain_seconds")
            if (isinstance(actual_observed, bool) or not isinstance(actual_observed, (int, float))
                    or not math.isfinite(actual_observed)
                    or not math.isclose(actual_observed, observed, abs_tol=0.001)):
                raise PreflightError(f"{name} pilot post-drain observation differs from raw markers")


def compare_pilot_aa(groups: dict, manifest: dict) -> dict:
    """Check spread over all six G0 rounds; no candidate is evaluated here."""
    if groups["g0-a"]["effective_config_sha256"] != groups["g0-b"]["effective_config_sha256"]:
        raise PreflightError("G0 A/A effective config semantics differ")
    rows = []
    for workload in (query["name"] for query in manifest["pilot"]["queries"]):
        for metric, limit in (("throughput_per_second", 0.10), ("p95_ms", 0.20)):
            per_group = {
                group: [groups[group]["metrics"][(workload, repetition)][metric] for repetition in range(3)]
                for group in ("g0-a", "g0-b")
            }
            medians = {group: statistics.median(values) for group, values in per_group.items()}
            all_rounds = per_group["g0-a"] + per_group["g0-b"]
            pooled_median = statistics.median(all_rounds)
            spread = (max(all_rounds) - min(all_rounds)) / pooled_median
            rows.append({"workload": workload, "metric": metric, "rounds": per_group,
                         "medians": medians, "pooled_median": pooled_median,
                         "spread": spread, "limit": limit,
                         "passed": spread <= limit})
    return {"kind": "g0-aa-pilot", "passed": all(row["passed"] for row in rows),
            "candidate_evaluated": False, "checks": rows}


def runner_has_scenario(runner: Path) -> None:
    listing = subprocess.run([str(runner), "--list"], capture_output=True, text=True, check=False)
    if listing.returncode or SCENARIO not in listing.stdout.split():
        raise PreflightError(f"runner does not list {SCENARIO}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-pilot", action="store_true")
    for group in ("g0", "g1", "g2", "g3"):
        parser.add_argument(f"--{group}", type=Path)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path,
                        help="pilot only: exact published fixture input")
    parser.add_argument("--config", type=Path,
                        help="pilot only: common base config")
    args = parser.parse_args()
    try:
        if args.baseline_pilot:
            if not args.g0 or any((args.g1, args.g2, args.g3)) or not args.fixture_manifest or not args.config:
                raise PreflightError("pilot requires only --g0, --fixture-manifest and --config")
            manifest = validate_pilot_manifest(args.manifest)
        else:
            if not all((args.g0, args.g1, args.g2, args.g3)) or args.fixture_manifest or args.config:
                raise PreflightError("formal run requires G0-G3 and frozen manifest paths")
            manifest = validate_frozen_manifest(args.manifest)
        runner = existing_file(args.runner, "runner binary")
        binary_groups = ("g0",) if args.baseline_pilot else ("g0", "g1", "g2", "g3")
        binaries = {group: existing_file(getattr(args, group), group + " binary")
                    for group in binary_groups}
        if not args.baseline_pilot and len({sha256(binary) for binary in binaries.values()}) != 4:
            raise PreflightError("G0-G3 must be distinct checkpoint binaries")
        runner_has_scenario(runner)
        output = args.output.resolve()
        if output.exists() and any(output.iterdir()):
            raise PreflightError(f"output is not empty: {output}")
        config = existing_file(args.config, "pilot config") if args.baseline_pilot else Path(manifest["frozen"]["base_config_path"]).resolve()
        fixture = existing_file(args.fixture_manifest, "pilot fixture") if args.baseline_pilot else Path(manifest["frozen"]["fixture_manifest_path"]).resolve()
        if args.baseline_pilot:
            validate_pilot_fixture(fixture)
        provenance = {"workload_sha256": sha256(args.manifest),
                      "runner_sha256": sha256(runner),
                      "binary_sha256_by_group": {g: sha256(p) for g, p in binaries.items()},
                      "fixture_manifest_sha256": sha256(fixture),
                      "base_config_sha256": sha256(config)}
        output.mkdir(parents=True, exist_ok=True)
        (output / ("pilot-input-hashes.json" if args.baseline_pilot else "input-hashes.json")).write_text(
            json.dumps(provenance, indent=2, sort_keys=True) + "\n")
        env = os.environ.copy()
        env["NOVAROCKS_UEA4A2_WORKLOAD_MANIFEST"] = str(args.manifest.resolve())
        env["NOVAROCKS_UEA4A2_FIXTURE_MANIFEST"] = str(fixture)
        if args.baseline_pilot:
            env["NOVAROCKS_UEA4A2_BASELINE_PILOT"] = "1"
        names = ("g0-a", "g0-b") if args.baseline_pilot else GROUPS
        pilot_groups = {}
        for name in names:
            group = name.split("-")[0]
            env["NOVAROCKS_UEA4A2_GROUP"] = name
            command = [str(runner), "--only", SCENARIO, "--binary", str(binaries[group]),
                       "--config", str(config), "--artifact-root", str(output / name),
                       "--cluster-size", "3", "--launch-profile", "performance",
                       "--timeout-secs", "1800"]
            completed = subprocess.run(command, env=env, check=False)
            if completed.returncode:
                raise PreflightError(f"{name} runner failed with exit {completed.returncode}")
            # Reject a runner that exits successfully without writing the formal receipt.
            receipt = output / name / "uea4-iceberg-range-performance" / "uea4a2-performance.json"
            existing_file(receipt, f"{name} performance receipt")
            if args.baseline_pilot:
                pilot_groups[name] = validate_pilot_receipt(
                    receipt, receipt.with_name("scenario-evidence.json"),
                    name, manifest, provenance)
        if args.baseline_pilot:
            result = compare_pilot_aa(pilot_groups, manifest)
            (output / "pilot-aa.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
            print("G0 A/A pilot evaluated; no candidate gate evaluated")
            return 0 if result["passed"] else 1
        from compare import compare  # Imported after receipts exist; compare also uses this module.
        result = compare(output, args.manifest)
        (output / "comparison.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return 0 if result["passed"] else 1
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"UEA-4A-2 benchmark preflight/run failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
