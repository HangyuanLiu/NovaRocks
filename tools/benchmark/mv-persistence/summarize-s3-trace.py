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

"""Assign MinIO S3 trace events to measured synchronous MV refresh windows."""

from __future__ import annotations

import argparse
import bisect
import calendar
import datetime as dt
import hashlib
import json
import re
from collections import Counter
from pathlib import Path


TRACE_TIME = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z$")


def event_time_ns(value: str) -> int:
    match = TRACE_TIME.fullmatch(value)
    if not match:
        raise ValueError(f"invalid MinIO trace time: {value!r}")
    seconds = dt.datetime.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S")
    return calendar.timegm(seconds.timetuple()) * 1_000_000_000 + int(
        (match.group(2) or "").ljust(9, "0")
    )


def object_kind(path: str) -> str:
    if "/metadata/snap-" in path and path.endswith(".avro"):
        return "manifest_list"
    if "/metadata/" in path and path.endswith(".metadata.json"):
        return "metadata_json"
    if "/metadata/" in path and path.endswith(".avro"):
        return "manifest"
    if "/metadata/" in path and path.endswith(".puffin"):
        return "statistics"
    if "/data/" in path:
        return "data"
    return "other"


def checked_counter(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid {label}: {value!r}")
    return value


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def summarize(traffic_path: Path, trace_path: Path) -> dict:
    windows = [json.loads(line) for line in traffic_path.read_text().splitlines()]
    if not windows:
        raise ValueError("no completed publication traffic windows")
    for index, window in enumerate(windows):
        if window["index"] != index:
            raise ValueError("publication indexes are not consecutive")
        start = checked_counter(window["start_unix_ns"], "window start")
        end = checked_counter(window["end_unix_ns"], "window end")
        if start >= end or (index and start <= windows[index - 1]["end_unix_ns"]):
            raise ValueError("publication windows overlap or have invalid time bounds")
    marker_names = {
        marker
        for window in windows
        for marker in (window.get("s3_start_marker"), window.get("s3_end_marker"))
    }
    if None in marker_names or len(marker_names) != 2 * len(windows):
        raise ValueError("publication S3 trace markers are absent or duplicated")
    marker_times: dict[str, int] = {}
    barriers: dict[str, int] = {}
    with trace_path.open(encoding="utf-8") as trace:
        for line_number, line in enumerate(trace, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid S3 trace JSON at line {line_number}") from error
            if event.get("type") != "S3":
                raise ValueError(f"non-S3 event in S3-only trace at line {line_number}")
            timestamp = event_time_ns(event["time"])
            for barrier in ("-0", "-1"):
                if "uea7-trace-barrier-" in event.get("query", "") and event["query"].endswith(
                    barrier
                ):
                    barriers[barrier] = timestamp
            marker = event.get("path", "").rsplit("/", 1)[-1]
            if marker in marker_names:
                if marker in marker_times:
                    raise ValueError(f"publication S3 trace marker appears more than once: {marker}")
                marker_times[marker] = timestamp
    if marker_times.keys() != marker_names:
        raise ValueError("publication S3 trace marker is missing")
    starts = [marker_times[window["s3_start_marker"]] for window in windows]
    ends = [marker_times[window["s3_end_marker"]] for window in windows]
    for index, (start, end) in enumerate(zip(starts, ends)):
        if start >= end or (index and start <= ends[index - 1]):
            raise ValueError("publication S3 trace markers overlap or have invalid order")
    reports = [
        {
            "index": window["index"],
            "start_unix_ns": window["start_unix_ns"],
            "end_unix_ns": window["end_unix_ns"],
            "s3_start_trace_ns": starts[index],
            "s3_end_trace_ns": ends[index],
            "s3_requests": 0,
            "by_api": Counter(),
            "by_status": Counter(),
            "by_object_kind": Counter(),
            "wire_rx_bytes": 0,
            "wire_tx_bytes": 0,
        }
        for index, window in enumerate(windows)
    ]
    outside = 0
    with trace_path.open(encoding="utf-8") as trace:
        for line_number, line in enumerate(trace, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid S3 trace JSON at line {line_number}") from error
            timestamp = event_time_ns(event["time"])
            if event.get("path", "").rsplit("/", 1)[-1] in marker_names:
                continue
            index = bisect.bisect_right(starts, timestamp) - 1
            if index < 0 or timestamp <= starts[index] or timestamp >= ends[index]:
                outside += 1
                continue
            report = reports[index]
            report["s3_requests"] += 1
            report["by_api"][event["api"]] += 1
            report["by_status"][str(checked_counter(event["statusCode"], "S3 status"))] += 1
            report["by_object_kind"][object_kind(event["path"])] += 1
            call_stats = event["callStats"]
            report["wire_rx_bytes"] += checked_counter(call_stats["rx"], "S3 wire rx")
            report["wire_tx_bytes"] += checked_counter(call_stats["tx"], "S3 wire tx")
    if "-0" not in barriers or "-1" not in barriers:
        raise ValueError("S3 trace startup or shutdown barrier is absent")
    if barriers["-0"] >= starts[0] or barriers["-1"] <= ends[-1]:
        raise ValueError("S3 trace does not bracket every publication window")
    return {
        "schema_version": 1,
        "traffic_sha256": digest(traffic_path),
        "s3_trace_sha256": digest(trace_path),
        "outside_window_s3_requests": outside,
        "windows": reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--traffic", type=Path, required=True)
    parser.add_argument("--trace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = summarize(args.traffic, args.trace)
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(f"summarized {len(report['windows'])} publication windows to {args.output}")


if __name__ == "__main__":
    main()
