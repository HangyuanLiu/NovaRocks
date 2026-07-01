#!/usr/bin/env python3
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
