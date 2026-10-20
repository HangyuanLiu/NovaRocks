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
"""Admission entry contracts, exercised through synthetic native attempts.

These owner-local correctness tests join the existing verification test stage.
No cluster, Cargo build or historical evidence is an input. Fake native scripts
own isolated temporary roots; unittest cleanup removes them after each case.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification import admission, admission_entry as entry, run
from verification.model import Scope, UnitKey

HEAD = "a" * 40
BASE = "b" * 40
IDENTITY = {"platform": {"system": "Linux", "machine": "x86_64"}, "input": "fixture"}


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n")


class Entry(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="admission-entry-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.declaration = self.repo / "tools/ci/baselines/admission-scope.json"
        put(self.declaration, {"schema": 1, "enabled": {"stage": ["fixture"],
            "rust-test": "all", "rust-doctest": "all", "sql-suite": [], "system-scenario": []},
            "pending": [{"selector": {"kind": "sql-suite"}, "owner": "sql-owner", "enable_when": "main passes"}],
            "enumeration_gaps": [{"name": "binding-only", "kind": "system-scenario",
                                  "reason": "binding construction", "closes_with": "native discovery"}]})
        put(self.repo / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [], "quarantine": []})
        self.out = self.root / "output"
        self.attempt = self.repo / "logs/ci-full/native-fixture"
        self.scope = Scope("stage:fixture", (UnitKey("stage", "fixture"),), IDENTITY, "immutable")
        self.pending = Scope("sql-suite:pending", (UnitKey("sql-case", "pending/case"),), IDENTITY, "isolated")
        self.declarations = [self.scope, self.pending]
        self.native_exit = 0
        self.stage_exit = 0
        self.extra_files = {}
        self.event = self.root / "event.json"
        put(self.event, {"merge_group": {"head_sha": HEAD, "base_sha": BASE}})

    def environment(self, event="merge_group", ref="refs/heads/gh-readonly-queue/main/pr-1-test"):
        return dict(os.environ, GITHUB_ACTIONS="true", GITHUB_REPOSITORY="NovaRocks/NovaRocks",
            GITHUB_EVENT_NAME=event, GITHUB_REF=ref, GITHUB_SHA=HEAD,
            GITHUB_EVENT_PATH=str(self.event), GITHUB_RUN_ID="42", GITHUB_RUN_ATTEMPT="1",
            GITHUB_WORKFLOW_REF="NovaRocks/NovaRocks/.github/workflows/admission.yml@" + ref,
            GITHUB_WORKFLOW_SHA=HEAD, RUNNER_ENVIRONMENT="github-hosted", RUNNER_OS="Linux",
            RUNNER_ARCH="X64", NOVA_ADMISSION_RUNNER_LABEL="ubuntu-24.04")

    def write_native(self):
        # A fake native shell produces producer-shaped facts and its raw exit;
        # the real material reader and composition determine the entry verdict.
        source = {"schema": 1, "source": {"commit": HEAD, "dirty": False}}
        invocation = {"schema": 1, "seq": 1, "kind": "stage", "name": "fixture",
            "units": ["stage:fixture"], "started_at": "start", "ended_at": "end",
            "exit": {"code": self.stage_exit}, "inputs_start": IDENTITY,
            "inputs_end": IDENTITY, "inputs_stable": True,
            "cleanup": {"summary": "ok"}, "materials": {}}
        script = self.repo / "tools/ci/local-full-ci.sh"
        script.parent.mkdir(parents=True, exist_ok=True)
        # Fixture JSON is encoded as shell single-quoted literals without shell evaluation.
        import shlex
        rows = ["#!/bin/bash", "set -eu", "mkdir -p logs/ci-full/native-fixture/identity logs/ci-full/native-fixture/invocations",
            "printf '%s\\n' " + shlex.quote(json.dumps(source)) + " > logs/ci-full/native-fixture/identity/source-start.json",
            "cp logs/ci-full/native-fixture/identity/source-start.json logs/ci-full/native-fixture/identity/source-end.json",
            "printf '%s\\n' " + shlex.quote(json.dumps(invocation)) + " > logs/ci-full/native-fixture/invocations/0001-fixture.json"]
        for name, value in self.extra_files.items():
            path = "logs/ci-full/native-fixture/" + name
            text = value if isinstance(value, str) else json.dumps(value)
            rows += ["mkdir -p " + shlex.quote(str(Path(path).parent)),
                     "printf '%s\\n' " + shlex.quote(text) + " > " + shlex.quote(path)]
        rows.append("exit " + str(self.native_exit))
        script.write_text("\n".join(rows) + "\n")

    def run_entry(self, environment=None):
        self.write_native()
        real_compose = admission.compose_attempt
        def compose(repo, attempt, declaration, identity, output, **kwargs):
            return real_compose(repo, attempt, declaration, identity, output,
                                declarations=self.declarations, **kwargs)
        with mock.patch.object(entry, "compose_attempt", side_effect=compose):
            return entry.run_admission(self.out, self.repo, environment or {"PATH": os.environ["PATH"]})

    def test_native_success_has_real_composition_and_explicit_pending_gaps(self):
        self.assertEqual(self.run_entry(), 0)
        record = json.loads((self.out / "composition.json").read_text())
        self.assertTrue(record["layer_ok"])
        self.assertEqual(record["evidence_class"], "local-rehearsal")
        text = (self.out / "summary.md").read_text()
        self.assertIn("sql-suite:pending", text)
        self.assertIn("binding-only", text)
        self.assertIn("not formal admission evidence", text)
        saved = json.loads((self.out / "entry.json").read_text())
        self.assertEqual(saved["attempt_root"], str(self.attempt))
        self.assertEqual(saved["native_exit"], 0)

    def test_native_fail_is_not_overwritten_by_entry(self):
        self.native_exit = self.stage_exit = 1
        self.assertEqual(self.run_entry(), 1)
        self.assertIn("FAIL", (self.out / "summary.md").read_text())
        self.assertFalse(json.loads((self.out / "composition.json").read_text())["layer_ok"])
        self.assertEqual(json.loads((self.attempt / "invocations/0001-fixture.json").read_text())["exit"]["code"], 1)

    def test_quarantined_rust_failure_passes_through_real_native_material_reader(self):
        aggregate = "stage:workspace nextest and doctests"
        rust = "rust-test:p::bad"
        doc = "rust-doctest:workspace"
        identity = dict(IDENTITY, executable_sha256="binary", nextest_profile="default",
                        nextest_config_sha256="config")
        self.declarations += [Scope(aggregate, (UnitKey.parse(aggregate),), IDENTITY, "immutable"),
                              Scope(rust, (UnitKey.parse(rust),), identity, "isolated"),
                              Scope(doc, (UnitKey.parse(doc),), IDENTITY, "isolated")]
        declaration = json.loads(self.declaration.read_text())
        declaration["enabled"]["stage"].append("workspace nextest and doctests")
        put(self.declaration, declaration)
        put(self.repo / "tools/ci/baselines/dispositions.json", {"schema": 1, "known_failures": [],
            "quarantine": [{"unit": rust, "reason": "tracked instability", "owner": "rust-owner",
                            "exit_condition": "fix and revalidate", "review_by": "2999-01-01"}]})
        cleanup = {"schema": 1, "registration_closed": True, "complete": True, "ok": True,
                   "records": [], "errors": []}
        def confirmed(domain, key, value):
            return {"schema": 1, "domain": domain, "scopes": {key: {"declared": True,
                    "inputs_start": value, "inputs_end": value, "inputs_stable": True}}}
        self.extra_files = {
            "rust/inputs/prepared.json": {"schema": 1, "profile": "default", "config_sha256": "config"},
            "rust/inputs/test-artifacts.json": {"schema": 1, "binaries": {"p": {"sha256": "binary"}}},
            "rust/inputs/selected-tests.json": {"rust-suites": {"p": {"testcases": {
                "bad": {"filter-match": {"status": "matches"}}}}}},
            "rust/inputs/tool-config.toml": '[store]\ndir = "/private/materials"\n',
            "rust/identity/nextest.json": confirmed("nextest", "p::bad", identity),
            "rust/identity/doctest.json": confirmed("doctest", "workspace", IDENTITY),
            "rust/rust-run/cleanup-summary.json": cleanup,
            "rust/doctest-run/cleanup-summary.json": cleanup,
            "rust/nextest/junit.xml": '<testsuites><testsuite name="p"><testcase name="bad" classname="p"><failure/></testcase></testsuite></testsuites>',
            "rust/execution-index.json": {"schema": 1, "complete": True, "cancelled": None, "exit_code": 1,
                "stages": {name: {"started": True, "returncode": 100 if name == "nextest" else 0,
                                  "errors": []} for name in ("prepare", "error_manifest", "nextest", "doctest", "final_prepare")},
                "materials": {"inputs": "inputs", "selection": "inputs/selected-tests.json",
                    "test_bindings": "inputs/test-artifacts.json", "tool_config": "inputs/tool-config.toml",
                    "junit": {"path": "nextest/junit.xml", "complete": True},
                    "rust_cleanup": "rust-run/cleanup-summary.json", "doctest_cleanup": "doctest-run/cleanup-summary.json"}},
            "invocations/0002-batch.json": {"schema": 1, "seq": 2, "kind": "stage",
                "units": [aggregate], "started_at": "start", "ended_at": "end", "exit": {"code": 1},
                "inputs_start": IDENTITY, "inputs_end": IDENTITY, "inputs_stable": True,
                "cleanup": {"summary": "ok"}, "materials": {"execution_index": str(self.attempt / "rust/execution-index.json")}},
        }
        self.native_exit = 1
        self.assertEqual(self.run_entry(), 0)
        scopes = {item["key"]: item for item in json.loads((self.out / "composition.json").read_text())["scopes"]}
        self.assertEqual(scopes[rust]["verdict"], "excluded")
        self.assertEqual(scopes[rust]["observations"][0]["result"], "fail")
        self.assertEqual(scopes[aggregate]["verdict"], "satisfied")
        self.assertEqual(scopes[aggregate]["native_exit"][0]["code"], 1)

    def test_invalid_scope_saves_all_entry_outputs_without_running_native(self):
        put(self.declaration, {"schema": 999})
        with mock.patch.object(entry, "run_native") as native:
            self.assertEqual(entry.run_admission(self.out, self.repo, {}), 2)
        native.assert_not_called()
        for name in ("entry.json", "composition.json", "summary.md"):
            self.assertTrue((self.out / name).is_file())

    def test_existing_or_relative_output_is_rejected(self):
        self.out.mkdir()
        with self.assertRaises(FileExistsError):
            entry.run_admission(self.out, self.repo, {})
        with self.assertRaises(ValueError):
            entry.run_admission(Path("relative-output"), self.repo, {})

    def test_native_exit_is_retained_when_no_attempt_directory_was_created(self):
        with mock.patch.object(entry, "run_native", return_value=([], 37)):
            self.assertEqual(entry.run_admission(self.out, self.repo, {}), 2)
        saved = json.loads((self.out / "entry.json").read_text())
        self.assertEqual(saved["native_exit"], 37)
        self.assertEqual(saved["attempt_candidates"], [])

    def test_formal_context_and_diagnostic_contexts(self):
        cases = [(self.environment(), "formal-admission"),
                 (self.environment("workflow_dispatch", "refs/heads/main"), "formal-admission"),
                 (self.environment("workflow_dispatch", "refs/heads/topic"), "diagnostic"),
                 (self.environment("pull_request", "refs/pull/1237/merge"), "diagnostic"),
                 ({}, "local-rehearsal")]
        for env, expected in cases:
            with self.subTest(expected=expected, event=env.get("GITHUB_EVENT_NAME")):
                self.assertEqual(entry.execution_context(env)[0], expected)
        for override in ({"GITHUB_REPOSITORY": "fork/NovaRocks"},
                         {"RUNNER_ENVIRONMENT": "self-hosted"},
                         {"GITHUB_WORKFLOW_REF": "NovaRocks/NovaRocks/.github/workflows/other.yml@refs/heads/main"},
                         {"GITHUB_WORKFLOW_REF": ""}, {"GITHUB_WORKFLOW_SHA": ""}):
            with self.subTest(override=override):
                self.assertEqual(entry.execution_context(dict(self.environment(), **override))[0], "diagnostic")

    def test_merge_head_mismatch_is_error_with_retained_record(self):
        env = self.environment()
        env["GITHUB_SHA"] = BASE
        with mock.patch.object(entry, "run_native") as native:
            self.assertEqual(entry.run_admission(self.out, self.repo, env), 2)
        native.assert_not_called()
        self.assertIn("differs", (self.out / "entry.json").read_text())

    def test_formal_entry_uses_only_this_attempt_and_records_identity(self):
        self.assertEqual(self.run_entry(self.environment()), 0)
        record = json.loads((self.out / "composition.json").read_text())
        self.assertEqual(record["evidence_class"], "formal-admission")
        self.assertEqual(record["formal_run"]["head_sha"], HEAD)
        self.assertEqual(len(record["attempts"]), 1)

    def test_preparation_failure_retains_blocking_composition_and_native_observations(self):
        self.write_native()
        with mock.patch.object(run, "prepare", side_effect=ValueError("candidate does not compile")):
            code = entry.run_admission(self.out, self.repo, {"PATH": os.environ["PATH"]})
        self.assertNotEqual(code, 0)
        record = json.loads((self.out / "composition.json").read_text())
        self.assertFalse(record["layer_ok"])
        self.assertTrue(record["admission_gaps"]["unenumerated"])
        self.assertIn("candidate does not compile", (self.out / "summary.md").read_text())
        self.assertTrue(record["attempts"])

    def test_entry_shell_help_from_another_directory(self):
        script = Path(__file__).resolve().parents[1] / "admission.sh"
        result = subprocess.run(["bash", str(script), "--help"], cwd=self.root,
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--output", result.stdout)


if __name__ == "__main__":
    unittest.main()
