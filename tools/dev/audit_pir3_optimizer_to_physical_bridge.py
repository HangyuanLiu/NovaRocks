#!/usr/bin/env python3
"""Audit PIR-3 optimizer-to-physical bridge boundaries."""

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


def violations_for(pattern: str, text: str, path: str, label: str) -> list[str]:
    regex = re.compile(pattern)
    violations = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if regex.search(line):
            violations.append(
                f"{path}:{line_no}: {label}: found forbidden pattern {pattern!r}"
            )
    return violations


def assert_absent(
    pattern: str, text: str, path: str, label: str, violations: list[str]
) -> None:
    violations.extend(violations_for(pattern, text, path, label))


def check_path_patterns(
    path: str, patterns: list[tuple[str, str]], violations: list[str]
) -> None:
    text = read(path)
    for pattern, label in patterns:
        assert_absent(pattern, text, path, label, violations)


def main() -> int:
    violations: list[str] = []

    dto_owned_files = [
        "src/sql/planner/runtime_filter.rs",
        "src/sql/planner/stats.rs",
    ]
    dto_optimizer_path_patterns = [
        (
            r"crate::sql::optimizer|sql::optimizer",
            "planner-owned DTO must not reference optimizer modules",
        ),
    ]
    for path in dto_owned_files:
        check_path_patterns(path, dto_optimizer_path_patterns, violations)

    forbidden_dto_patterns = [
        (
            r"crate::sql::optimizer::operator::Operator",
            "planner DTO boundary must not depend on optimizer Operator",
        ),
        (
            r"crate::sql::optimizer::scalar::ScalarArena",
            "planner DTO boundary must not depend on optimizer ScalarArena",
        ),
        (
            r"crate::sql::optimizer::property::PhysicalPropertySet",
            "planner DTO boundary must not depend on optimizer PhysicalPropertySet",
        ),
        (
            r"compute_cost_estimate",
            "planner DTO boundary must not recompute optimizer cost",
        ),
        (
            r"broadcast_decision\s*\(",
            "planner DTO boundary must not recompute optimizer broadcast decisions",
        ),
        (
            r"OptimizerOptions",
            "planner DTO boundary must not read optimizer session options",
        ),
        (
            r"current_session_optimizer_settings",
            "planner DTO boundary must not read optimizer session settings",
        ),
    ]
    for path in [
        "src/sql/planner/plan.rs",
        "src/sql/planner/runtime_filter.rs",
        "src/sql/planner/stats.rs",
    ]:
        check_path_patterns(path, forbidden_dto_patterns, violations)

    bridge_cost_patterns = [
        (
            r"compute_cost_estimate",
            "Bridge 2a must convert optimizer facts, not recompute optimizer cost",
        ),
        (
            r"broadcast_decision\s*\(",
            "Bridge 2a must convert optimizer facts, not recompute broadcast decisions",
        ),
        (
            r"CostInput",
            "Bridge 2a must not rebuild optimizer cost inputs",
        ),
        (
            r"OptimizerOptions",
            "Bridge 2a must not read optimizer session options",
        ),
        (
            r"current_session_optimizer_settings",
            "Bridge 2a must not read optimizer session settings",
        ),
    ]
    check_path_patterns(
        "src/sql/planner/optimizer_bridge/physical.rs",
        bridge_cost_patterns,
        violations,
    )

    global_pir3_patterns = [
        (
            r"PhysicalPlanKind::Exchange",
            "planner physical IR must not reintroduce Exchange as a physical node kind",
        ),
        (
            r"RedistributeMode::Random",
            "planner physical IR must not reintroduce random redistribution",
        ),
        (
            r"PhysicalPlanProps",
            "planner physical IR must not reintroduce the generic physical property bag",
        ),
    ]
    planner_root = ROOT / "src/sql/planner"
    for rust_path in sorted(planner_root.rglob("*.rs")):
        path = rust_path.relative_to(ROOT).as_posix()
        check_path_patterns(path, global_pir3_patterns, violations)

    if violations:
        fail("PIR-3 optimizer-to-physical bridge audit failed:\n" + "\n".join(violations))

    print("PIR-3 optimizer-to-physical bridge audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
