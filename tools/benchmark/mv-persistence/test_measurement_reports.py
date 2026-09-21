#!/usr/bin/env python3
#
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

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parent


def run(*arguments: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(arguments), cwd=cwd, text=True, capture_output=True, check=False
    )


class MeasurementReportComparisonTest(unittest.TestCase):
    def test_cross_worktree_comparison_preserves_identity_and_rejects_drift(self):
        with tempfile.TemporaryDirectory(prefix="uea7-measurement-test-") as root_name:
            root = Path(root_name).resolve()
            reports = []
            for role in ("baseline", "candidate"):
                worktree = root / role
                worktree.mkdir()
                (worktree / "workload.sql").write_text("SELECT 1;\n", encoding="utf-8")
                (worktree / "suite.toml").write_text("mode = 'smoke'\n", encoding="utf-8")
                (worktree / ".gitignore").write_text(
                    "docker/iceberg-rest/runtime/\n", encoding="utf-8"
                )
                runtime_id = (
                    f"{role}-{hashlib.sha1(str(worktree).encode()).hexdigest()[:8]}"
                )
                config = (
                    worktree
                    / "docker/iceberg-rest/runtime"
                    / runtime_id
                    / "sql-test.toml"
                )
                config.parent.mkdir(parents=True)
                config.write_text(f"port = '{role}'\n", encoding="utf-8")
                for command in (
                    ("git", "init", "-q"),
                    ("git", "add", "workload.sql", "suite.toml", ".gitignore"),
                    (
                        "git",
                        "-c",
                        "user.name=UEA7 Test",
                        "-c",
                        "user.email=uea7-test@example.invalid",
                        "commit",
                        "-qm",
                        "fixture",
                    ),
                ):
                    result = run(*command, cwd=worktree)
                    self.assertEqual(result.returncode, 0, result.stderr)

                output = root / f"{role}-report"
                command = (
                    sys.executable,
                    str(TOOLS / "measure-command.py"),
                    "--label",
                    role,
                    "--role",
                    role,
                    "--profile",
                    "dev-opt",
                    "--workload-file",
                    "workload.sql",
                    "--workload-file",
                    "suite.toml",
                    "--config-file",
                    str(config),
                    "--samples",
                    "7",
                    "--warmups",
                    "0",
                    "--dimension",
                    "case=metadata-smoke",
                    "--output",
                    str(output),
                    "--",
                    sys.executable,
                    "-c",
                    (
                        "import json,pathlib,sys; "
                        "assert pathlib.Path(sys.argv[1]).is_file(); "
                        "roles=['fe','be-0','be-1','be-2']; "
                        "processes={role:{'pid':index+1,'process_start_token':role} "
                        "for index,role in enumerate(roles)}; "
                        "samples=[{'role':role,'pid':item['pid'],"
                        "'process_start_token':item['process_start_token'],"
                        "'rss_bytes':(index+1)*1024,'elapsed_millis':tick*100,"
                        "'cpu_user_nanos':tick*100000000+index,"
                        "'cpu_system_nanos':tick*20000000+index,"
                        "'unavailable_reason':None} "
                        "for tick in (1,2) "
                        "for index,(role,item) in enumerate(processes.items())]; "
                        "pathlib.Path(sys.argv[2]).write_text(json.dumps("
                        "{'schema_version':3,'processes':processes,'samples':samples}))"
                    ),
                    str(config),
                    "@SAMPLE_RESOURCE_OUTPUT@",
                )
                result = run(*command, cwd=worktree)
                self.assertEqual(result.returncode, 0, result.stderr)
                reports.append(output / "report.json")

            baseline = json.loads(reports[0].read_text(encoding="utf-8"))
            candidate = json.loads(reports[1].read_text(encoding="utf-8"))
            self.assertNotEqual(baseline["command"], candidate["command"])
            self.assertEqual(
                baseline["normalized_command"], candidate["normalized_command"]
            )
            self.assertNotEqual(baseline["config"]["sha256"], candidate["config"]["sha256"])
            self.assertEqual(
                baseline["config"]["normalized_path"],
                candidate["config"]["normalized_path"],
            )
            self.assertTrue(
                baseline["config"]["normalized_path"].endswith(
                    "/${RUNTIME}/sql-test.toml"
                )
            )
            self.assertEqual(
                [item["sha256"] for item in baseline["workload_files"]],
                [item["sha256"] for item in candidate["workload_files"]],
            )
            self.assertEqual(
                baseline["resource_scope"], "wait4_direct_controller_process_only"
            )
            self.assertIn("controller_max_rss", baseline["summary"])
            self.assertNotIn("max_rss", baseline["summary"])
            self.assertEqual(
                sorted(baseline["summary"]["role_peak_rss_bytes"]),
                ["be-0", "be-1", "be-2", "fe"],
            )
            self.assertEqual(
                baseline["summary"]["role_cpu_time_ms"]["fe"]["total"]["median"],
                120,
            )
            self.assertEqual(
                len(list(reports[0].parent.glob("sample-*.resources.json"))), 7
            )
            resource_path = reports[0].parent / "sample-01.resources.json"
            bad_resource = json.loads(resource_path.read_text(encoding="utf-8"))
            fe_samples = [
                sample for sample in bad_resource["samples"] if sample["role"] == "fe"
            ]
            fe_samples[-1]["cpu_user_nanos"] = fe_samples[0]["cpu_user_nanos"] - 1
            bad_path = root / "nonmonotonic-cpu.resources.json"
            bad_path.write_text(json.dumps(bad_resource), encoding="utf-8")
            spec = importlib.util.spec_from_file_location(
                "uea7_measure_command", TOOLS / "measure-command.py"
            )
            self.assertIsNotNone(spec)
            measure = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(measure)
            with self.assertRaisesRegex(ValueError, "nonmonotonic fe CPU samples"):
                measure.role_resource_identity(bad_path)

            comparison = root / "comparison.json"
            result = run(
                sys.executable,
                str(TOOLS / "compare-reports.py"),
                "--baseline",
                str(reports[0]),
                "--candidate",
                str(reports[1]),
                "--max-ratio",
                "100",
                "--output",
                str(comparison),
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            compared = json.loads(comparison.read_text(encoding="utf-8"))
            self.assertTrue(compared["passed"])
            self.assertEqual(compared["configs"]["baseline"], baseline["config"])
            self.assertEqual(compared["configs"]["candidate"], candidate["config"])
            self.assertIn("controller_peak_rss", compared)
            self.assertEqual(
                sorted(compared["role_sampled_high_water_rss_bytes"]),
                ["be-0", "be-1", "be-2", "fe"],
            )
            self.assertEqual(
                compared["role_cpu_time_ms"]["fe"]["baseline"]["total"]["median"],
                120,
            )

            for field, value, expected_error in (
                (
                    "workload_files",
                    [
                        {**item, "sha256": "0" * 64}
                        if item["relative_path"] == "suite.toml"
                        else item
                        for item in candidate["workload_files"]
                    ],
                    "workload file paths or bytes differ",
                ),
                ("normalized_command", ["different-command"], "normalized_command"),
                ("dimensions", {"case": "different-case"}, "dimensions"),
                (
                    "config",
                    {
                        **candidate["config"],
                        "normalized_path": "${WORKTREE}/docker/iceberg-rest/runtime/${RUNTIME}/fe.toml",
                    },
                    "normalized config paths differ",
                ),
                (
                    "summary",
                    {
                        **candidate["summary"],
                        "role_peak_rss_bytes": {
                            "fe": candidate["summary"]["role_peak_rss_bytes"]["fe"]
                        },
                    },
                    "FE/BE resource roles differ",
                ),
                (
                    "cpu_summary",
                    {
                        **candidate["summary"],
                        "role_cpu_time_ms": {
                            "fe": candidate["summary"]["role_cpu_time_ms"]["fe"]
                        },
                    },
                    "FE/BE CPU roles differ",
                ),
            ):
                with self.subTest(field=field):
                    changed = {
                        **candidate,
                        "summary" if field == "cpu_summary" else field: value,
                    }
                    changed_path = root / f"changed-{field}.json"
                    changed_path.write_text(json.dumps(changed), encoding="utf-8")
                    result = run(
                        sys.executable,
                        str(TOOLS / "compare-reports.py"),
                        "--baseline",
                        str(reports[0]),
                        "--candidate",
                        str(changed_path),
                        "--max-ratio",
                        "100",
                        "--output",
                        str(root / f"rejected-{field}.json"),
                        cwd=root,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(expected_error, result.stderr)


if __name__ == "__main__":
    unittest.main()
