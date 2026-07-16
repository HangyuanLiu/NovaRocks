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
if "scalar_arena" in read("src/sql/planner/distributed/seal.rs"):
    fail("DistributedPlan must not carry scalar_arena")

native_encoder = "\n".join(
    p.read_text(encoding="utf-8").split("#[cfg(test)]", 1)[0]
    for p in (ROOT / "src/protocol/native/encode").rglob("*.rs")
)
blocked = [
    "OptimizerPhysicalNode",
    "optimizer::operator::Operator",
    "optimizer::physical_tree",
]
for needle in blocked:
    if needle in native_encoder:
        fail(f"native encoder production code must not reference {needle}")

if re.search(
    r"fn\s+encode_native_fragment_bundle\s*\([^)]*OptimizerPhysicalNode",
    native_encoder,
):
    fail("encode_native_fragment_bundle must not accept OptimizerPhysicalNode")

if not (ROOT / "src/sql/planner/optimizer_bridge/id_binding.rs").is_file():
    fail("id binding verification must remain under planner::optimizer_bridge")

print("plan IR codegen boundary audit passed")
