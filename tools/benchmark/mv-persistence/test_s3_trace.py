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

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "summarize_s3_trace", Path(__file__).with_name("summarize-s3-trace.py")
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def event(time: str, api: str, path: str, query: str = "") -> dict:
    return {
        "type": "S3",
        "time": time,
        "api": api,
        "path": path,
        "query": query,
        "statusCode": 200,
        "callStats": {"rx": 10, "tx": 20},
    }


class S3TraceSummaryTest(unittest.TestCase):
    def test_assigns_exact_windows_and_requires_both_trace_barriers(self):
        origin = MODULE.event_time_ns("2026-09-21T00:00:00Z")
        windows = [
            {"index": 0, "start_unix_ns": origin + 1_000_000_000,
             "end_unix_ns": origin + 2_000_000_000,
             "s3_start_marker": "uea7-marker-test-0-before",
             "s3_end_marker": "uea7-marker-test-0-after"},
            {"index": 1, "start_unix_ns": origin + 3_000_000_000,
             "end_unix_ns": origin + 4_000_000_000,
             "s3_start_marker": "uea7-marker-test-1-before",
             "s3_end_marker": "uea7-marker-test-1-after"},
        ]
        events = [
            event("2026-09-21T00:00:00.500000000Z", "s3.ListObjectsV2", "/warehouse/",
                  "prefix=uea7-trace-barrier-42-0"),
            event("2026-09-21T00:00:00.900000000Z", "s3.HeadObject",
                  "/warehouse/_novarocks/trace-markers/uea7-marker-test-0-before"),
            event("2026-09-21T00:00:01.500000000Z", "s3.GetObject",
                  "/warehouse/t/metadata/snap-123.avro"),
            event("2026-09-21T00:00:02.100000000Z", "s3.HeadObject",
                  "/warehouse/_novarocks/trace-markers/uea7-marker-test-0-after"),
            event("2026-09-21T00:00:02.900000000Z", "s3.HeadObject",
                  "/warehouse/_novarocks/trace-markers/uea7-marker-test-1-before"),
            event("2026-09-21T00:00:03.500000000Z", "s3.PutObject",
                  "/warehouse/t/metadata/00001.metadata.json"),
            event("2026-09-21T00:00:04.100000000Z", "s3.HeadObject",
                  "/warehouse/_novarocks/trace-markers/uea7-marker-test-1-after"),
            event("2026-09-21T00:00:04.500000000Z", "s3.ListObjectsV2", "/warehouse/",
                  "prefix=uea7-trace-barrier-42-1"),
        ]
        with tempfile.TemporaryDirectory(prefix="uea7-s3-trace-test-") as root:
            traffic = Path(root) / "traffic.jsonl"
            trace = Path(root) / "trace.jsonl"
            traffic.write_text("".join(json.dumps(w) + "\n" for w in windows))
            trace.write_text("".join(json.dumps(e) + "\n" for e in events))
            summary = MODULE.summarize(traffic, trace)
            self.assertEqual([w["s3_requests"] for w in summary["windows"]], [1, 1])
            self.assertEqual(summary["windows"][0]["by_object_kind"], {"manifest_list": 1})
            self.assertEqual(summary["windows"][1]["by_object_kind"], {"metadata_json": 1})
            self.assertEqual(summary["outside_window_s3_requests"], 2)
            trace.write_text("".join(json.dumps(e) + "\n" for e in events[:-1]))
            with self.assertRaisesRegex(ValueError, "barrier is absent"):
                MODULE.summarize(traffic, trace)


if __name__ == "__main__":
    unittest.main()
