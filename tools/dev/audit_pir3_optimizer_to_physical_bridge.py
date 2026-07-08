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

"""Audit PIR-3 optimizer-to-physical bridge boundaries."""

from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]

OPTIMIZER_MODULE_COMPONENT_PATTERN = (
    r"(?:^|::|[{\s,])\s*optimizer\s*(?:::|[,};]|\bas\b|$)"
)

DTO_OPTIMIZER_PATH_PATTERNS = [
    (
        OPTIMIZER_MODULE_COMPONENT_PATTERN,
        "planner-owned DTO must not reference optimizer modules",
    ),
]

PLAN_RS_FORBIDDEN_DEPENDENCY_PATTERNS = [
    (
        r"\b(?:Operator|ScalarArena|PhysicalPropertySet)\b",
        "planner physical IR must not depend on optimizer-owned plan facts",
    ),
]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def read_masked(path: str) -> str:
    return mask_rust_code(read(path))


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def starts_raw_string(line: str, i: int) -> tuple[int, int] | None:
    start = i
    if line.startswith("br", i):
        i += 2
    elif line.startswith("r", i):
        i += 1
    else:
        return None

    hashes_start = i
    while i < len(line) and line[i] == "#":
        i += 1
    if i < len(line) and line[i] == '"':
        return i - hashes_start, i + 1 - start
    return None


def char_literal_end(line: str, i: int) -> int | None:
    if i >= len(line) or line[i] != "'":
        return None
    j = i + 1
    escaped = False
    while j < len(line):
        ch = line[j]
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == "'":
            return j + 1
        j += 1
    return None


def is_lifetime_or_label_start(line: str, i: int) -> bool:
    if i + 1 >= len(line) or line[i] != "'":
        return False
    if not (line[i + 1].isalpha() or line[i + 1] == "_"):
        return False

    j = i + 2
    while j < len(line) and (line[j].isalnum() or line[j] == "_"):
        j += 1

    return j >= len(line) or line[j] != "'"


def mask_rust_non_code(line: str, state: dict[str, object]) -> tuple[str, dict[str, object]]:
    out = []
    i = 0
    while i < len(line):
        block_depth = int(state["block_depth"])
        raw_hashes = state["raw_hashes"]
        string_delim = state["string_delim"]
        escaped = bool(state["escaped"])

        if block_depth:
            if line.startswith("/*", i):
                state["block_depth"] = block_depth + 1
                out.append("  ")
                i += 2
                continue
            if line.startswith("*/", i):
                state["block_depth"] = block_depth - 1
                out.append("  ")
                i += 2
                continue
            out.append(" ")
            i += 1
            continue

        if raw_hashes is not None:
            end = '"' + ("#" * int(raw_hashes))
            if line.startswith(end, i):
                state["raw_hashes"] = None
                out.append(" " * len(end))
                i += len(end)
            else:
                out.append(" ")
                i += 1
            continue

        if string_delim is not None:
            ch = line[i]
            out.append(" ")
            if escaped:
                state["escaped"] = False
            elif ch == "\\":
                state["escaped"] = True
            elif ch == string_delim:
                state["string_delim"] = None
            i += 1
            continue

        if line.startswith("//", i):
            out.append(" " * (len(line) - i))
            break

        if line.startswith("/*", i):
            state["block_depth"] = 1
            out.append("  ")
            i += 2
            continue

        raw_start = starts_raw_string(line, i)
        if raw_start is not None:
            hashes, consumed = raw_start
            state["raw_hashes"] = hashes
            out.append(" " * consumed)
            i += consumed
            continue

        if line.startswith('b"', i):
            state["string_delim"] = '"'
            state["escaped"] = False
            out.append("  ")
            i += 2
            continue

        if line[i] == '"':
            state["string_delim"] = '"'
            state["escaped"] = False
            out.append(" ")
            i += 1
            continue

        if line.startswith("b'", i):
            end = char_literal_end(line, i + 1)
            if end is not None:
                out.append(" " * (end - i))
                i = end
                continue

        if line[i] == "'" and not is_lifetime_or_label_start(line, i):
            end = char_literal_end(line, i)
            if end is not None:
                out.append(" " * (end - i))
                i = end
                continue

        out.append(line[i])
        i += 1

    return "".join(out), state


def mask_rust_code(text: str) -> str:
    state: dict[str, object] = {
        "block_depth": 0,
        "raw_hashes": None,
        "string_delim": None,
        "escaped": False,
    }
    masked_lines = []
    for line in text.splitlines():
        masked_line, state = mask_rust_non_code(line, state)
        masked_lines.append(masked_line)
    return "\n".join(masked_lines)


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
    text = read_masked(path)
    for pattern, label in patterns:
        assert_absent(pattern, text, path, label, violations)


def brace_body_range(text: str, open_brace: int) -> tuple[int, int] | None:
    depth = 0
    for i in range(open_brace, len(text)):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return open_brace + 1, i
    return None


def enum_body_range(text: str, enum_name: str) -> tuple[int, int] | None:
    match = re.search(rf"\benum\s+{re.escape(enum_name)}\b", text)
    if not match:
        return None
    open_brace = text.find("{", match.end())
    if open_brace == -1:
        return None
    return brace_body_range(text, open_brace)


def enum_variant_violations(
    text: str, path: str, enum_name: str, variant_name: str, label: str
) -> list[str]:
    body_range = enum_body_range(text, enum_name)
    if body_range is None:
        return []
    start, end = body_range
    body = text[start:end]
    match = re.search(rf"(?m)^\s*{re.escape(variant_name)}\b", body)
    if match is None:
        return []
    line_no = text.count("\n", 0, start + match.start()) + 1
    return [
        f"{path}:{line_no}: {label}: found forbidden {enum_name}::{variant_name} variant"
    ]


def type_declaration_violations(
    text: str, path: str, type_name: str, label: str
) -> list[str]:
    pattern = rf"\b(?:struct|enum|type)\s+{re.escape(type_name)}\b"
    match = re.search(pattern, text)
    if match is None:
        return []
    line_no = text.count("\n", 0, match.start()) + 1
    return [f"{path}:{line_no}: {label}: found forbidden {type_name} declaration"]


def main() -> int:
    violations: list[str] = []

    dto_owned_files = [
        "src/sql/planner/runtime_filter.rs",
        "src/sql/planner/stats.rs",
    ]
    for path in dto_owned_files:
        check_path_patterns(path, DTO_OPTIMIZER_PATH_PATTERNS, violations)

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

    plan_path = "src/sql/planner/plan.rs"
    plan_rs = read_masked(plan_path)
    for pattern, label in PLAN_RS_FORBIDDEN_DEPENDENCY_PATTERNS:
        assert_absent(pattern, plan_rs, plan_path, label, violations)
    violations.extend(
        enum_variant_violations(
            plan_rs,
            plan_path,
            "PhysicalPlanKind",
            "Exchange",
            "planner physical IR must not declare Exchange as a physical node kind",
        )
    )
    violations.extend(
        enum_variant_violations(
            plan_rs,
            plan_path,
            "RedistributeMode",
            "Random",
            "planner physical IR must not declare random redistribution",
        )
    )
    violations.extend(
        type_declaration_violations(
            plan_rs,
            plan_path,
            "PhysicalPlanProps",
            "planner physical IR must not reintroduce the generic physical property bag",
        )
    )

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
