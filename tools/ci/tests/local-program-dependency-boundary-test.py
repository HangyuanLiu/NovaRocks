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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise capability mutations using real Cargo metadata, without compiling."""

import copy
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path


CHECKER = Path(__file__).resolve().parents[1] / "check-local-program-dependency-boundary.py"
spec = importlib.util.spec_from_file_location("local_program_boundary", CHECKER)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class BoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        support = guard.metadata_support()
        cls.repository_metadata = support.cargo_metadata(guard.REPOSITORY_ROOT / "Cargo.toml")

    def fixture(self, root_dependency="", types_dependency="", extra="", build_script=False,
                result_dependency=""):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / "Cargo.toml").write_text(
            '[workspace]\nresolver = "2"\nmembers = ["program", "types", "runtime", "result"]\n')
        for directory, name, dependencies in (
                ("program", "novarocks-local-program", root_dependency),
                ("types", "novarocks-types", types_dependency),
                ("result", "novarocks-result-contract", result_dependency),
                ("runtime", "tokio", "")):
            package = root / directory
            (package / "src").mkdir(parents=True)
            (package / "src/lib.rs").write_text("")
            (package / "Cargo.toml").write_text(
                f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2024"\n'
                + "[dependencies]\n" + dependencies
                + (extra if directory == "program" else ""))
        if build_script:
            (root / "program/build.rs").write_text("fn main() {}\n")
        subprocess.run(["cargo", "generate-lockfile", "--offline", "--manifest-path",
                        str(root / "Cargo.toml")], check=True, capture_output=True)
        return subprocess.run(["python3", str(CHECKER), "--manifest-path",
                               str(root / "Cargo.toml")], capture_output=True, text=True)

    def assert_rejected(self, result, diagnostic):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(diagnostic, result.stderr)

    def test_pure_contract_and_unrelated_workspace_runtime_are_allowed(self):
        result = self.fixture('novarocks-types = { path = "../types" }\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_dependency_free_result_contract_is_allowed(self):
        result = self.fixture('novarocks-result-contract = { path = "../result" }\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_result_contract_cannot_acquire_runtime_even_through_a_pure_root(self):
        result = self.fixture('novarocks-result-contract = { path = "../result" }\n',
                              result_dependency='tokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "novarocks-result-contract must remain dependency-free")
        self.assertIn("runtime/wire/provider/storage capability: tokio", result.stderr)

    def test_result_contract_cannot_add_an_otherwise_pure_dependency(self):
        result = self.fixture('novarocks-result-contract = { path = "../result" }\n',
                              result_dependency='novarocks-types = { path = "../types" }\n')
        self.assert_rejected(result, "novarocks-result-contract must remain dependency-free")

    def test_transitive_runtime_is_rejected(self):
        result = self.fixture('novarocks-types = { path = "../types" }\n',
                              'tokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_optional_renamed_runtime_is_not_hidden(self):
        result = self.fixture('executor = { package = "tokio", path = "../runtime", optional = true }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_inactive_target_runtime_is_not_hidden(self):
        result = self.fixture(extra='[target.\'cfg(target_os = "none")\'.dependencies]\n'
                              'tokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_build_runtime_is_rejected(self):
        result = self.fixture(extra='[build-dependencies]\ntokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_dev_runtime_is_rejected(self):
        result = self.fixture(extra='[dev-dependencies]\ntokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_transitive_dev_runtime_is_rejected(self):
        result = self.fixture(types_dependency='tokio = { path = "../runtime" }\n',
                              extra='[dev-dependencies]\nnovarocks-types = { path = "../types" }\n')
        self.assert_rejected(result, "runtime/wire/provider/storage capability: tokio")

    def test_pure_owner_cannot_execute_build_script(self):
        self.assert_rejected(self.fixture(build_script=True), "executes a custom build script")

    @staticmethod
    def external(name, source=guard.REGISTRY_SOURCE):
        return {"id": f"{source}#{name}@1.0.0", "name": name, "source": source,
                "dependencies": [], "features": {}, "targets": []}

    def test_arrow_backing_is_allowed_without_exact_closure_snapshot(self):
        for name in ("arrow-array", "arrow-buffer", "arrow-data", "half", "num-traits"):
            self.assertEqual(guard.verify_package(self.external(name), set()), [])

    def actual_vendors(self):
        packages = {package["name"]: package for package in self.repository_metadata["packages"]
                    if package["name"] in guard.VENDORED_BACKING_VERSIONS
                    and package["source"] is None}
        self.assertEqual(set(packages), set(guard.VENDORED_BACKING_VERSIONS))
        return packages.values()

    def test_actual_vendored_backings_have_exact_audited_cargo_identities(self):
        for package in self.actual_vendors():
            with self.subTest(package=package["name"]):
                self.assertTrue(guard.audited_vendored_backing(package))
                self.assertEqual(guard.verify_package(package, set()), [])

    def test_same_named_vendor_at_an_unrelated_path_is_rejected(self):
        for original in self.actual_vendors():
            with self.subTest(package=original["name"]):
                package = copy.deepcopy(original)
                directory = Path("/tmp/unaudited-vendor") / original["name"]
                # All fields agree with each other, but none attests the actual
                # repository vendor. Name/source alone must not allow this.
                package["manifest_path"] = str(directory / "Cargo.toml")
                package["id"] = f"path+{directory.as_uri()}#{package['name']}@{package['version']}"
                self.assertTrue(guard.verify_package(package, set()))

    def test_vendor_manifest_version_source_and_id_cannot_be_mutated(self):
        for original in self.actual_vendors():
            mutations = (
                ("manifest_path", str(Path(original["manifest_path"]).with_name("other.toml"))),
                ("manifest_path", str(Path(original["manifest_path"]).parent.parent
                                      / "replacement" / "Cargo.toml")),
                ("version", "99.0.0"),
                ("id", original["id"].rsplit("@", 1)[0] + "@99.0.0"),
                ("id", f"{guard.REGISTRY_SOURCE}#{original['name']}@{original['version']}"),
                ("source", guard.REGISTRY_SOURCE),
                ("source", "git+https://example.invalid/backing?rev=other#012345"),
            )
            for field, value in mutations:
                with self.subTest(package=original["name"], field=field, value=value):
                    package = copy.deepcopy(original)
                    package[field] = value
                    self.assertFalse(guard.audited_vendored_backing(package))
                    self.assertTrue(guard.verify_package(package, set()))

    def test_non_audited_path_backing_does_not_inherit_the_vendor_exception(self):
        package = self.external("arrow-data", source=None)
        self.assertTrue(guard.verify_package(package, set()))

    def test_same_named_pure_contract_replacement_is_rejected(self):
        package = self.external("novarocks-types")
        self.assertIn("not the workspace-owned pure package", " ".join(
            guard.verify_package(package, set())))

    def test_provider_and_wire_capabilities_are_rejected(self):
        for name in ("novarocks-connector-iceberg", "novarocks-spi",
                     "novarocks-proto-models", "novarocks-worker", "tonic", "object_store"):
            self.assertTrue(guard.verify_package(self.external(name), set()), name)


if __name__ == "__main__":
    unittest.main()
