#!/usr/bin/env python3
"""Recompute process CPU and data-bearing connections from a G0 pilot receipt."""

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path


BE_ROLES = ("be-0", "be-1", "be-2")


def attached(root: Path, report: dict, name: str) -> Path:
    item = report["attachments"][name]
    path = (root / item["path"]).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError(f"invalid {name} attachment path")
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if digest != item["sha256"]:
        raise ValueError(f"{name} attachment hash changed")
    return path


def cpu_windows(trace: dict, windows: list[dict]) -> list[dict]:
    if trace.get("schema_version") != 3:
        raise ValueError("unsupported resource trace")
    samples = defaultdict(list)
    for sample in trace["samples"]:
        if sample["role"] in BE_ROLES:
            identity = trace["processes"][sample["role"]]
            if (sample["pid"] != identity["pid"]
                    or sample["process_start_token"] != identity["process_start_token"]
                    or sample["unavailable_reason"] is not None
                    or sample["cpu_user_nanos"] is None
                    or sample["cpu_system_nanos"] is None):
                raise ValueError("missing or mismatched BE CPU sample")
            samples[sample["role"]].append(sample)
    output = []
    for window in windows:
        marker = window["rss_timing"]
        row = {"workload": window["workload"], "repetition": window["repetition"]}
        for role in BE_ROLES:
            selected = [s for s in samples[role]
                        if marker["measure_start"] <= s["elapsed_millis"] <= marker["measure_end"]]
            if len(selected) < 2:
                raise ValueError(f"insufficient BE CPU samples for {role}")
            first, last = selected[0], selected[-1]
            elapsed = (last["elapsed_millis"] - first["elapsed_millis"]) / 1000
            user = (last["cpu_user_nanos"] - first["cpu_user_nanos"]) / 1e9
            system = (last["cpu_system_nanos"] - first["cpu_system_nanos"]) / 1e9
            if elapsed <= 0 or user < 0 or system < 0:
                raise ValueError(f"invalid BE CPU counter progression for {role}")
            row[role] = {
                "observed_wall_seconds": elapsed,
                "user_cpu_seconds": user,
                "system_cpu_seconds": system,
                "process_core_equivalents": (user + system) / elapsed,
            }
        output.append(row)
    return output


def connections(event_path: Path, connection_path: Path) -> dict:
    requests = defaultdict(lambda: {"data": 0, "other": 0, "protocols": set()})
    data_labels = set()
    data_methods = defaultdict(int)
    with event_path.open() as source:
        for line in source:
            event = json.loads(line)
            if event["kind"] != "Arrived":
                continue
            connection = requests[event["connection_id"]]
            request_kind = "data" if event["object_id"] is not None else "other"
            connection[request_kind] += 1
            connection["protocols"].add(event["protocol"])
            if event["object_id"] is not None:
                data_labels.add(event["object_id"])
                data_methods[event["method"]] += 1
    accepted = set()
    closed = set()
    with connection_path.open() as source:
        for line in source:
            event = json.loads(line)
            if event["kind"] == "Accepted":
                accepted.add(event["connection_id"])
            elif event["kind"] == "Closed":
                closed.add(event["connection_id"])
            else:
                raise ValueError("unexpected connection event")
    if not requests or not set(requests).issubset(accepted) or not closed.issubset(accepted):
        raise ValueError("connection request and accept events disagree")
    classes = {"data_only": 0, "metadata_only": 0, "mixed": 0}
    reuse = 0
    protocols = set()
    for record in requests.values():
        reuse += max(0, record["data"] + record["other"] - 1)
        protocols.update(record["protocols"])
        key = "mixed" if record["data"] and record["other"] else (
            "data_only" if record["data"] else "metadata_only")
        classes[key] += 1
    return {
        "accepted_connections": len(accepted),
        "closed_connections_observed_before_teardown": len(closed),
        "request_bearing_connections": len(requests),
        "connection_classes": classes,
        "data_gets": data_methods["GET"],
        "data_heads": data_methods["HEAD"],
        "additional_requests_on_existing_connections": reuse,
        "observed_protocols": sorted(protocols),
        "data_object_labels": sorted(data_labels),
        "classification_limit": "Data-bearing connections are BE-facing only when every data object is labeled and native placement is verified; mixed connections remain ambiguous.",
    }


def analyze(path: Path) -> dict:
    root = path.resolve().parent
    report = json.loads(path.read_text())
    if report.get("schema_version") not in (2, 3) or report.get("pilot") is not True:
        raise ValueError("requires a G0 v2/v3 pilot receipt")
    windows = json.loads(attached(root, report, "query_trace").read_text())
    if windows != report["windows"]:
        raise ValueError("query trace and receipt disagree")
    trace = json.loads(attached(root, report, "resource_trace").read_text())
    event_path = attached(root, report, "proxy_event_trace")
    connection_path = attached(root, report, "proxy_connection_trace")
    return {
        "receipt": str(path.resolve()),
        "cpu_windows": cpu_windows(trace, windows),
        "connections": connections(event_path, connection_path),
        "proxy_lifetime": report.get("proxy_lifetime"),
        "upstream_connections_by_window": [
            {
                "repetition": window["repetition"],
                "attempts": window["proxy_upstream_connect_attempts"],
                "established": window["proxy_upstream_connections_established"],
            }
            for window in windows
        ] if report["schema_version"] >= 3 else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipts", nargs="+", type=Path)
    args = parser.parse_args()
    print(json.dumps([analyze(path) for path in args.receipts], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
