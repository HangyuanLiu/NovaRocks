#!/usr/bin/env python3
"""Audit PIR-2 plan kind split boundaries."""

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

    if "enum DistributedPlanKind" not in distributed_node:
        fail("DistributedPlanKind is missing from distributed_node.rs")
    if "kind: DistributedPlanKind" not in distributed_node:
        fail("DistributedPlanNode.kind does not use DistributedPlanKind")

    print("PIR-2 plan kind split audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
