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

"""Audit PIR plan-kind split boundaries."""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def assert_absent(pattern: str, text: str, label: str) -> None:
    if re.search(pattern, text):
        fail(f"{label}: found forbidden pattern {pattern!r}")


def main() -> int:
    plan_rs = read("src/sql/planner/plan.rs")
    planner_mod = read("src/sql/planner/mod.rs")
    distributed_node = read("src/sql/planner/distributed_node.rs")
    planner_sources = "\n".join(
        p.read_text(encoding="utf-8") for p in (ROOT / "src/sql/planner").rglob("*.rs")
    )

    assert_absent(r"enum\s+PlanNodeKind\b", plan_rs, "mixed PlanNodeKind enum")
    assert_absent(r"kind:\s*PlanNodeKind\b", plan_rs, "LogicalPlanNode kind type")
    assert_absent(r"validate_logical_plan_stage\b", plan_rs, "logical stage runtime guard")
    assert_absent(
        r"PhysicalPlanKind::Exchange\b|Exchange\s*\(\s*DistributedExchangeNode",
        plan_rs,
        "Exchange in PhysicalPlanKind",
    )
    assert_absent(r"PhysicalPlanProps\b", plan_rs, "generic physical property bag")
    assert_absent(r"PhysicalPropertySet\b", plan_rs, "optimizer property bag in planner physical type")
    assert_absent(r"ScalarArena\b", plan_rs, "optimizer scalar arena in planner physical type")
    assert_absent(r"optimizer::operator::Operator\b", plan_rs, "optimizer operator in planner physical type")
    assert_absent(r"PlanNodeKind", planner_mod, "planner public re-export")

    assert_absent(r"enum\s+DistributedPlanKind\b", planner_sources, "distributed kind enum")
    assert_absent(r"kind:\s*DistributedPlanKind\b", planner_sources, "distributed node kind type")

    if "pub(crate) enum DistributedPayload" not in distributed_node:
        fail("DistributedPayload is missing from distributed_node.rs")
    if "Physical(PhysicalPlanKind)" not in distributed_node:
        fail("DistributedPayload must wrap PhysicalPlanKind payloads")
    if "Exchange(ExchangeReceiver)" not in distributed_node:
        fail("DistributedPayload must preserve explicit Exchange payloads")
    if "payload: DistributedPayload" not in distributed_node:
        fail("DistributedNode.payload does not use DistributedPayload")

    print("PIR plan kind split audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
