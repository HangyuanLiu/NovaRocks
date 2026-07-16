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

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
BLOCKED_NATIVE_ENCODER_REFERENCES = [
    "OptimizerPhysicalNode",
    "optimizer::operator::Operator",
    "optimizer::physical_tree",
]


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def mask_range(chars: list[str], start: int, end: int) -> None:
    for index in range(start, end):
        if chars[index] != "\n":
            chars[index] = " "


def rust_lexically_sanitized(source: str) -> str:
    """Mask comments and literals while preserving offsets and line structure."""
    chars = list(source)
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = len(source) if end == -1 else end
            mask_range(chars, index, end)
            index = end
            continue
        if source.startswith("/*", index):
            depth = 1
            cursor = index + 2
            while cursor < len(source) and depth:
                if source.startswith("/*", cursor):
                    depth += 1
                    cursor += 2
                elif source.startswith("*/", cursor):
                    depth -= 1
                    cursor += 2
                else:
                    cursor += 1
            mask_range(chars, index, cursor)
            index = cursor
            continue

        prefix_length = 0
        if source.startswith("br", index) or source.startswith("rb", index):
            prefix_length = 2
        elif source[index] == "r":
            prefix_length = 1
        raw_cursor = index + prefix_length
        if prefix_length and raw_cursor < len(source):
            hashes = 0
            while raw_cursor + hashes < len(source) and source[raw_cursor + hashes] == "#":
                hashes += 1
            quote = raw_cursor + hashes
            if quote < len(source) and source[quote] == '"':
                terminator = '"' + ("#" * hashes)
                end = source.find(terminator, quote + 1)
                end = len(source) if end == -1 else end + len(terminator)
                mask_range(chars, index, end)
                index = end
                continue

        quote_index = index
        if source.startswith('b"', index) or source.startswith('c"', index):
            quote_index += 1
        if quote_index < len(source) and source[quote_index] == '"':
            cursor = quote_index + 1
            while cursor < len(source):
                if source[cursor] == "\\":
                    cursor += 2
                elif source[cursor] == '"':
                    cursor += 1
                    break
                else:
                    cursor += 1
            mask_range(chars, index, min(cursor, len(source)))
            index = cursor
            continue

        # A Rust lifetime has no closing quote. Only mask a character literal
        # when a closing quote is present within the next escaped character.
        quote_index = index + 1 if source.startswith("b'", index) else index
        if quote_index < len(source) and source[quote_index] == "'":
            cursor = quote_index + 1
            if cursor < len(source) and source[cursor] == "\\":
                cursor += 2
            else:
                cursor += 1
            if cursor < len(source) and source[cursor] == "'":
                cursor += 1
                mask_range(chars, index, cursor)
                index = cursor
                continue
        index += 1
    return "".join(chars)


def cfg_tokens(predicate: str) -> list[str]:
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[(),=]", predicate)


def cfg_requires_test(predicate: str) -> bool:
    tokens = cfg_tokens(predicate)

    def parse(cursor: int) -> tuple[bool, int]:
        if cursor >= len(tokens):
            return False, cursor
        name = tokens[cursor]
        cursor += 1
        if cursor >= len(tokens) or tokens[cursor] != "(":
            return name == "test", cursor
        cursor += 1
        children: list[bool] = []
        while cursor < len(tokens) and tokens[cursor] != ")":
            child, cursor = parse(cursor)
            children.append(child)
            if cursor < len(tokens) and tokens[cursor] == ",":
                cursor += 1
            elif cursor < len(tokens) and tokens[cursor] == "=":
                cursor += 2
        cursor += int(cursor < len(tokens) and tokens[cursor] == ")")
        if name == "all":
            return any(children), cursor
        if name == "any":
            return bool(children) and all(children), cursor
        return False, cursor

    required, _ = parse(0)
    return required


def balanced_end(source: str, start: int, opening: str, closing: str) -> int:
    depth = 0
    for index in range(start, len(source)):
        if source[index] == opening:
            depth += 1
        elif source[index] == closing:
            depth -= 1
            if depth == 0:
                return index + 1
    return len(source)


def rust_item_end(sanitized: str, start: int) -> int:
    """Find the end of the item following an outer attribute."""
    paren = bracket = angle = 0
    index = start
    while index < len(sanitized):
        char = sanitized[index]
        if char == "(":
            paren += 1
        elif char == ")":
            paren = max(paren - 1, 0)
        elif char == "[":
            bracket += 1
        elif char == "]":
            bracket = max(bracket - 1, 0)
        elif char == "<":
            angle += 1
        elif char == ">":
            angle = max(angle - 1, 0)
        elif paren == bracket == angle == 0 and char == ";":
            return index + 1
        elif paren == bracket == angle == 0 and char == "{":
            end = balanced_end(sanitized, index, "{", "}")
            cursor = end
            while cursor < len(sanitized) and sanitized[cursor].isspace():
                cursor += 1
            if cursor < len(sanitized) and sanitized[cursor] == ";":
                cursor += 1
            return cursor
        index += 1
    return len(sanitized)


def rust_production_text(source: str) -> str:
    sanitized = rust_lexically_sanitized(source)
    production = list(sanitized)
    index = 0
    while index < len(sanitized):
        if sanitized[index] != "#":
            index += 1
            continue
        cursor = index + 1
        inner = cursor < len(sanitized) and sanitized[cursor] == "!"
        cursor += int(inner)
        while cursor < len(sanitized) and sanitized[cursor].isspace():
            cursor += 1
        if cursor >= len(sanitized) or sanitized[cursor] != "[":
            index += 1
            continue
        attribute_end = balanced_end(sanitized, cursor, "[", "]")
        attribute = sanitized[cursor + 1 : max(cursor + 1, attribute_end - 1)].strip()
        cfg_match = re.fullmatch(r"cfg\s*\((.*)\)", attribute, flags=re.DOTALL)
        if not cfg_match or not cfg_requires_test(cfg_match.group(1)):
            index = attribute_end
            continue
        if inner:
            mask_range(production, index, len(production))
            break

        item_start = attribute_end
        while True:
            while item_start < len(sanitized) and sanitized[item_start].isspace():
                item_start += 1
            if not sanitized.startswith("#[", item_start):
                break
            item_start = balanced_end(sanitized, item_start + 1, "[", "]")
        item_end = rust_item_end(sanitized, item_start)
        mask_range(production, index, item_end)
        index = item_end
    return "".join(production)


def native_encoder_production_inventory(
    sources: list[tuple[str, str]],
) -> tuple[list[str], str]:
    if not sources:
        raise AssertionError("native encoder production inventory is empty")
    production = [(name, rust_production_text(source)) for name, source in sources]
    nonempty = [name for name, text in production if text.strip()]
    if not nonempty:
        raise AssertionError("native encoder production inventory has no production code")
    return nonempty, "\n".join(text for _, text in production)


def native_encoder_violations(sources: list[tuple[str, str]]) -> list[str]:
    _, production = native_encoder_production_inventory(sources)
    violations = [
        needle for needle in BLOCKED_NATIVE_ENCODER_REFERENCES if needle in production
    ]
    if re.search(
        r"fn\s+encode_native_fragment_bundle\s*\([^)]*OptimizerPhysicalNode",
        production,
    ):
        violations.append(
            "encode_native_fragment_bundle must not accept OptimizerPhysicalNode"
        )
    return violations


def run_self_tests() -> None:
    early_test_then_forbidden = """
#[cfg(test)]
fn test_helper() {}
fn forbidden(_: OptimizerPhysicalNode) {}
"""
    forbidden_only_in_test = """
#[cfg(test)]
fn test_helper(_: OptimizerPhysicalNode) {}
fn production_inventory_marker() {}
"""
    assert native_encoder_violations(
        [("early_test_then_forbidden.rs", early_test_then_forbidden)]
    ) == ["OptimizerPhysicalNode"]
    assert not native_encoder_violations(
        [("forbidden_only_in_test.rs", forbidden_only_in_test)]
    )
    inventory, _ = native_encoder_production_inventory(
        [("inventory.rs", "fn production_inventory_marker() {}")]
    )
    assert inventory == ["inventory.rs"]
    print("plan IR codegen boundary audit self-tests passed")


def run_audit() -> None:
    planner_sources = "\n".join(
        p.read_text(encoding="utf-8")
        for p in (ROOT / "src/sql/planner").rglob("*.rs")
    )
    if "enum DistributedPlanKind" in planner_sources:
        fail("DistributedPlanKind must not be reintroduced")
    if "struct PlanNodeStats" in planner_sources:
        fail("migration PlanNodeStats must not be reintroduced")
    if "scalar_arena" in read("src/sql/planner/distributed/seal.rs"):
        fail("DistributedPlan must not carry scalar_arena")

    encoder_root = ROOT / "src/protocol/native/encode"
    encoder_sources = [
        (str(path.relative_to(ROOT)), path.read_text(encoding="utf-8"))
        for path in sorted(encoder_root.rglob("*.rs"))
    ]
    try:
        violations = native_encoder_violations(encoder_sources)
    except AssertionError as error:
        fail(str(error))
    for violation in violations:
        if violation.startswith("encode_native_fragment_bundle"):
            fail(violation)
        fail(f"native encoder production code must not reference {violation}")

    if not (ROOT / "src/sql/planner/optimizer_bridge/id_binding.rs").is_file():
        fail("id binding verification must remain under planner::optimizer_bridge")

    print("plan IR codegen boundary audit passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        run_self_tests()
    elif sys.argv[1:]:
        fail(f"unsupported arguments: {' '.join(sys.argv[1:])}")
    else:
        run_audit()
