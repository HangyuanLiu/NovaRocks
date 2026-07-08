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


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


planner_sources = "\n".join(
    p.read_text(encoding="utf-8") for p in (ROOT / "src/sql/planner").rglob("*.rs")
)
if "enum DistributedPlanKind" in planner_sources:
    fail("DistributedPlanKind must not be reintroduced")
if "struct PlanNodeStats" in planner_sources:
    fail("migration PlanNodeStats must not be reintroduced")
if "scalar_arena" in read("src/sql/planner/distributed_fragment.rs"):
    fail("DistributedPlan must not carry scalar_arena")

fragment_builder = read("src/sql/codegen/fragment_builder.rs")
production_prefix = fragment_builder.split("#[cfg(test)]", 1)[0]
blocked = [
    "OptimizerPhysicalNode",
    "optimizer::operator::Operator",
    "optimizer::physical_tree",
]
for needle in blocked:
    if needle in production_prefix:
        fail(f"fragment_builder production code must not reference {needle}")

if re.search(
    r"fn\s+build_via_distributed_plan\s*\([^)]*OptimizerPhysicalNode",
    fragment_builder,
):
    fail("build_via_distributed_plan must not accept OptimizerPhysicalNode")

if (ROOT / "src/sql/codegen/id_binding_verifier.rs").exists():
    fail("id_binding_verifier must live under planner::optimizer_bridge, not codegen")

print("plan IR codegen boundary audit passed")
