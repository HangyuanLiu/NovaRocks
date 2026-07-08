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

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

FORBIDDEN = {
    "DirectExecutionReason": "engine direct-exec reason enum must not exist",
    "RuntimeLocalTerminalSink": "terminal sink must be mainline request/runtime capability, not direct-exec reason",
    "RuntimeLocalIcebergRegistry": "Iceberg registry must be mainline runtime service, not direct-exec reason",
    "UnitTestNoExchangeBackend": "tests must install loopback exchange/backend instead of no-exchange fallback",
    "direct_execution_reason": "engine must not choose a planner/codegen bypass by local runtime conditions",
    "execute_query_direct_for_explicit_exception": "direct-exec branch must be deleted",
    "single_fragment_plan": "engine must not require a single-fragment PlanBuildResult",
    "collapse_distribution_enforcers_for_single_fragment": "engine must not rewrite optimizer physical trees",
    "collapse_distribution_enforcers_for_single_fragment_inner": "engine must not rewrite optimizer physical trees",
    "DirectLocalCteBinding": "direct-local CTE inline support belonged to the deleted direct path",
    "project_direct_local_columns": "direct-local CTE projection support belonged to the deleted direct path",
}

SEARCH_PATHS = (
    ROOT / "src" / "engine",
    ROOT / "src" / "sql" / "codegen",
    ROOT / "src" / "sql" / "planner",
)


def main() -> int:
    violations: list[tuple[Path, str, str]] = []

    for search_path in SEARCH_PATHS:
        if not search_path.exists():
            continue
        for path in sorted(search_path.rglob("*.rs")):
            text = path.read_text(encoding="utf-8")
            rel = path.relative_to(ROOT)
            for token, reason in FORBIDDEN.items():
                if token in text:
                    violations.append((rel, token, reason))

    if violations:
        print("PIR-7e direct-exec guard failed:", file=sys.stderr)
        for rel, token, reason in violations:
            print(
                f"  - {rel}: forbidden token {token!r}: {reason}",
                file=sys.stderr,
            )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
