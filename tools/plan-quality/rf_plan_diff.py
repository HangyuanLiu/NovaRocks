#!/usr/bin/env python3
"""Collect FE-vs-NovaRocks runtime-filter EXPLAIN VERBOSE differences."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


RF_PATTERNS = (
    "runtime filter",
    "runtime filters",
    "build runtime filters",
    "probe runtime filters",
    "build_expr",
    "probe_expr",
)


DEFAULT_CASES = (
    "tpc-h/q4",
    "tpc-h/q22",
    "tpc-ds/q41",
    "tpc-ds/q72",
    "ssb/q1.1",
    "ssb/q1.2",
    "ssb/q1.3",
    "ssb/q2.1",
    "ssb/q2.2",
    "ssb/q2.3",
    "ssb/q3.1",
    "ssb/q3.2",
    "ssb/q3.3",
    "ssb/q3.4",
    "ssb/q4.1",
    "ssb/q4.2",
    "ssb/q4.3",
)


@dataclass(frozen=True)
class Endpoint:
    name: str
    host: str
    port: str
    user: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def case_to_sql_path(case_id: str) -> Path:
    suite, case_name = case_id.split("/", 1)
    return repo_root() / "sql-tests" / suite / "sql" / f"{case_name}.sql"


def explain_sql(raw_sql: str) -> str:
    stripped = raw_sql.strip().rstrip(";")
    return f"EXPLAIN VERBOSE {stripped};"


def run_mysql(endpoint: Endpoint, sql: str, timeout: int) -> str:
    cmd = [
        "mysql",
        "-h",
        endpoint.host,
        "-P",
        endpoint.port,
        "-u",
        endpoint.user,
        "--batch",
        "--raw",
        "--skip-column-names",
        "-e",
        sql,
    ]
    env = os.environ.copy()
    env.update(
        {
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "HTTP_PROXY": "",
            "HTTPS_PROXY": "",
            "ALL_PROXY": "",
            "http_proxy": "",
            "https_proxy": "",
            "all_proxy": "",
        }
    )
    result = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{endpoint.name} mysql failed for port {endpoint.port}: {result.stderr.strip()}"
        )
    return result.stdout


def rf_lines(explain: str) -> list[str]:
    lines = []
    for line in explain.splitlines():
        lowered = line.lower()
        if any(pattern in lowered for pattern in RF_PATTERNS):
            lines.append(line)
    return lines


def safe_file_name(case_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", case_id)


def collect_case(
    case_id: str,
    endpoints: Iterable[Endpoint],
    output_dir: Path,
    timeout: int,
) -> dict[str, object]:
    sql_path = case_to_sql_path(case_id)
    raw_sql = sql_path.read_text()
    sql = explain_sql(raw_sql)
    entry: dict[str, object] = {"case": case_id, "sql_path": str(sql_path)}
    for endpoint in endpoints:
        explain = run_mysql(endpoint, sql, timeout)
        out_path = output_dir / endpoint.name / f"{safe_file_name(case_id)}.out"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(explain)
        lines = rf_lines(explain)
        entry[endpoint.name] = {
            "file": str(out_path),
            "runtime_filter_line_count": len(lines),
            "runtime_filter_lines": lines,
        }
    return entry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fe-host", default="127.0.0.1")
    parser.add_argument("--fe-port", required=True)
    parser.add_argument("--nr-host", default="127.0.0.1")
    parser.add_argument("--nr-port", required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--case", action="append", dest="cases")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cases = args.cases or list(DEFAULT_CASES)
    output_dir = Path(args.output_dir)
    endpoints = (
        Endpoint("fe", args.fe_host, args.fe_port, args.user),
        Endpoint("nr", args.nr_host, args.nr_port, args.user),
    )
    summary = []
    for case_id in cases:
        summary.append(collect_case(case_id, endpoints, output_dir, args.timeout))
    status_dir = output_dir / "status"
    status_dir.mkdir(parents=True, exist_ok=True)
    (status_dir / "aggregate_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n"
    )
    rows = [
        "| case | FE RF lines | NR RF lines |",
        "|---|---:|---:|",
    ]
    for item in summary:
        fe_count = item["fe"]["runtime_filter_line_count"]  # type: ignore[index]
        nr_count = item["nr"]["runtime_filter_line_count"]  # type: ignore[index]
        rows.append(f"| {item['case']} | {fe_count} | {nr_count} |")
    (status_dir / "representative_queries.md").write_text("\n".join(rows) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
