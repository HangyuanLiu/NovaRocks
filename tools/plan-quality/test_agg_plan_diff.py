#!/usr/bin/env python3
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

"""Standalone unit test for agg_plan_diff.agg_counts (no live server needed).

Run: python3 tools/plan-quality/test_agg_plan_diff.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from agg_plan_diff import agg_counts  # noqa: E402

NR_EXPLAIN = """
PLAN FRAGMENT 0
  HASH AGGREGATE (GLOBAL) stats={rows=3}
    HASH EXCHANGE (source: ShuffleAgg)
      HASH AGGREGATE (LOCAL) stats={rows=3}
        OLAP SCAN (t)
"""

FE_EXPLAIN = """
PLAN FRAGMENT 0
  3:AGGREGATE (merge finalize)
  |  group by: 1: k
  2:EXCHANGE
  1:AGGREGATE (update serialize)
  0:OlapScanNode
"""


def main() -> int:
    nr = agg_counts(NR_EXPLAIN, "nr")
    assert nr == {"single": 0, "local": 1, "global": 1}, nr

    fe = agg_counts(FE_EXPLAIN, "fe")
    assert fe == {"single": 0, "update": 1, "merge": 1}, fe

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
