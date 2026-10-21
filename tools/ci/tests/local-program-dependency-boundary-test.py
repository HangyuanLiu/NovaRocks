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
    def fixture(self, root_dependency="", types_dependency="", extra="", build_script=False,
                result_dependency=None, functions_dependency="", functions_build=None,
                functions_extra="", failure_dependency="", failure_extra="", result_extra="",
                owner_builds=()):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / "Cargo.toml").write_text(
            '[workspace]\nresolver = "2"\nmembers = ["program", "types", "runtime", "result", "functions", "failure"]\n')
        if result_dependency is None:
            result_dependency = 'novarocks-failure-contract = { path = "../failure" }\n'
        for directory, name, dependencies in (
                ("program", "novarocks-local-program", root_dependency),
                ("types", "novarocks-types", types_dependency),
                ("result", "novarocks-result-contract", result_dependency),
                ("runtime", "tokio", ""),
                ("failure", "novarocks-failure-contract", failure_dependency),
                ("functions", "novarocks-functions", functions_dependency)):
            package = root / directory
            (package / "src").mkdir(parents=True)
            (package / "src/lib.rs").write_text("")
            (package / "Cargo.toml").write_text(
                f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2024"\n'
                + "[dependencies]\n" + dependencies
                + (extra if directory == "program" else "")
                + (failure_extra if directory == "failure" else "")
                + (result_extra if directory == "result" else "")
                + (functions_extra if directory == "functions" else ""))
        if build_script:
            (root / "program/build.rs").write_text("fn main() {}\n")
        for owner in owner_builds:
            (root / owner / "build.rs").write_text("fn main() {}\n")
        if functions_build is not None:
            (root / "functions" / functions_build).write_text("fn main() {}\n")
            manifest = root / "functions/Cargo.toml"
            manifest.write_text(manifest.read_text().replace(
                '[package]\n', f'[package]\nbuild = "{functions_build}"\n'))
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

    def test_result_projection_over_failure_fact_leaf_is_allowed(self):
        result = self.fixture('novarocks-result-contract = { path = "../result" }\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_failure_fact_leaf_is_allowed_directly(self):
        result = self.fixture('novarocks-failure-contract = { path = "../failure" }\n')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_result_projection_requires_its_failure_fact_dependency(self):
        self.assert_rejected(self.fixture('novarocks-result-contract = { path = "../result" }\n',
                                          result_dependency=""),
                             "requires exactly one normal dependency on novarocks-failure-contract")

    def test_result_contract_cannot_acquire_runtime_even_through_a_pure_root(self):
        result = self.fixture('novarocks-result-contract = { path = "../result" }\n',
                              result_dependency='tokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "requires exactly one normal dependency on novarocks-failure-contract")
        self.assertIn("runtime/wire/provider/storage capability: tokio", result.stderr)

    def test_result_contract_cannot_add_an_otherwise_pure_dependency(self):
        result = self.fixture('novarocks-result-contract = { path = "../result" }\n',
                              result_dependency='novarocks-types = { path = "../types" }\n')
        self.assert_rejected(result, "requires exactly one normal dependency on novarocks-failure-contract")

    def test_result_projection_cannot_add_edges_of_any_kind(self):
        root_dependency = 'novarocks-result-contract = { path = "../result" }\n'
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            with self.subTest(section=section):
                if section == "dependencies":
                    result = self.fixture(root_dependency, result_dependency=(
                        'novarocks-failure-contract = { path = "../failure" }\n'
                        'novarocks-types = { path = "../types" }\n'))
                else:
                    result = self.fixture(root_dependency, result_extra=(
                        f'[{section}]\nnovarocks-types = {{ path = "../types" }}\n'))
                self.assert_rejected(result, "requires exactly one normal dependency on novarocks-failure-contract")

    def test_failure_fact_leaf_cannot_acquire_even_a_pure_dependency(self):
        root_dependency = 'novarocks-failure-contract = { path = "../failure" }\n'
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            with self.subTest(section=section):
                dependency = 'novarocks-types = { path = "../types" }\n'
                kwargs = {"failure_dependency": dependency} if section == "dependencies" else {
                    "failure_extra": f'[{section}]\n' + dependency}
                self.assert_rejected(self.fixture(root_dependency, **kwargs),
                                     "novarocks-failure-contract must remain dependency-free")

    def test_failure_fact_cannot_transitively_import_runtime(self):
        self.assert_rejected(self.fixture(
            'novarocks-result-contract = { path = "../result" }\n',
            failure_dependency='tokio = { path = "../runtime" }\n'),
            "runtime/wire/provider/storage capability: tokio")

    def test_failure_and_result_cannot_hide_feature_target_or_macro_authority(self):
        for owner in ("failure", "result"):
            root_dependency = f'novarocks-{owner}-contract = {{ path = "../{owner}" }}\n'
            variants = [
                ("features", '[features]\nhidden = []\n', "unaudited Cargo feature variants"),
                ("target", '[target.\'cfg(target_os = "none")\'.dependencies]\n'
                 'novarocks-types = { path = "../types" }\n',
                 "must remain dependency-free" if owner == "failure" else "requires exactly one normal dependency"),
                ("macro", '[lib]\nproc-macro = true\n', "executes a proc-macro target"),
            ]
            for variant, extra, diagnostic in variants:
                with self.subTest(owner=owner, variant=variant):
                    self.assert_rejected(self.fixture(root_dependency, **{owner + "_extra": extra}), diagnostic)

    def test_result_failure_edge_cannot_be_optional_or_target_specific(self):
        root_dependency = 'novarocks-result-contract = { path = "../result" }\n'
        variants = [
            'novarocks-failure-contract = { path = "../failure", optional = true }\n',
            '[target.\'cfg(target_os = "none")\'.dependencies]\n'
            'novarocks-failure-contract = { path = "../failure" }\n',
        ]
        for dependency in variants:
            with self.subTest(dependency=dependency):
                self.assert_rejected(self.fixture(root_dependency, result_dependency=dependency),
                                     "hides a normal edge behind a feature/target")

    def test_failure_and_result_cannot_execute_build_scripts(self):
        for owner in ("failure", "result"):
            with self.subTest(owner=owner):
                self.assert_rejected(self.fixture(
                    f'novarocks-{owner}-contract = {{ path = "../{owner}" }}\n',
                    owner_builds=(owner,)), "executes a custom build script")

    def test_result_failure_edge_preserves_dependency_feature_policy(self):
        self.assert_rejected(self.fixture(
            'novarocks-result-contract = { path = "../result" }\n',
            result_dependency='novarocks-failure-contract = { path = "../failure", default-features = false }\n'),
            "novarocks-result-contract disables dependency default features")

    def test_failure_fact_cannot_hide_optional_runtime(self):
        self.assert_rejected(self.fixture(
            'novarocks-failure-contract = { path = "../failure" }\n',
            failure_dependency='runtime = { package = "tokio", path = "../runtime", optional = true }\n'),
            "novarocks-failure-contract must remain dependency-free")

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

    def test_functions_audited_build_host_is_allowed(self):
        result = self.fixture('novarocks-functions = { path = "../functions" }\n',
                              functions_build="build.rs")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_functions_build_host_cannot_select_another_source(self):
        result = self.fixture('novarocks-functions = { path = "../functions" }\n',
                              functions_build="other.rs")
        self.assert_rejected(result, "novarocks-functions declares a custom build target")

    def test_functions_build_host_cannot_acquire_build_runtime(self):
        result = self.fixture('novarocks-functions = { path = "../functions" }\n',
                              functions_build="build.rs",
                              functions_extra='[build-dependencies]\ntokio = { path = "../runtime" }\n')
        self.assert_rejected(result, "novarocks-functions declares a build dependency")
        self.assertIn("runtime/wire/provider/storage capability: tokio", result.stderr)

    def test_functions_build_host_cannot_acquire_macro_authority(self):
        result = self.fixture('novarocks-functions = { path = "../functions" }\n',
                              functions_build="build.rs", functions_extra='[lib]\nproc-macro = true\n')
        self.assert_rejected(result, "novarocks-functions declares a proc-macro target")

    def test_audited_build_host_does_not_admit_functions_to_physical_plan(self):
        self.assertNotIn("novarocks-functions", guard.metadata_support().DIRECT_INTERNAL_ALLOW_LIST)
        self.assertNotIn("novarocks-functions", guard.metadata_support().DIRECT_PACKAGE_ALLOW_LIST)
        package = {"dependencies": [{"name": "novarocks-functions", "kind": None,
                                    "optional": False, "target": None, "features": [],
                                    "uses_default_features": True}], "features": {}}
        _, violations = guard.metadata_support().verify_declared_dependencies(package)
        self.assertIn("outside the exact direct allow-list", " ".join(violations))

    @staticmethod
    def external(name, version="1.0.0", source=guard.REGISTRY_SOURCE):
        return {"id": f"{source}#{name}@{version}", "name": name, "source": source,
                "version": version,
                "manifest_path": f"/registry/{name}-{version}/Cargo.toml",
                "dependencies": [], "features": {}, "targets": []}

    def test_exact_arrow_backing_identities_are_allowed(self):
        for name, version in (("arrow-array", "2.0.0"), ("arrow-buffer", "2.0.0"),
                              ("arrow-data", "2.0.0"), ("half", "2.0.0"),
                              ("num-traits", "2.0.0")):
            self.assertEqual(guard.verify_package(self.external(name, version), set()), [])

    def test_same_named_pure_contract_replacement_is_rejected(self):
        for name in ("novarocks-types", "novarocks-functions", "novarocks-failure-contract"):
            package = self.external(name)
            self.assertIn("not the workspace-owned pure package", " ".join(
                guard.verify_package(package, set())))

    def test_provider_and_wire_capabilities_are_rejected(self):
        for name in ("novarocks-connector-iceberg", "novarocks-spi",
                     "novarocks-proto-models", "novarocks-worker", "tonic", "object_store"):
            self.assertTrue(guard.verify_package(self.external(name), set()), name)

    def test_same_name_foreign_arrow_and_unknown_package_are_rejected(self):
        for package in (self.external("arrow-array", "2.0.0", "git+https://example.invalid/arrow"),
                        self.external("unknown-pure-looking-package")):
            self.assertIn("unaudited external package identity", " ".join(
                guard.verify_package(package, set())))

    def test_registry_membership_does_not_authorize_new_build_or_macro(self):
        for kind in ("custom-build", "proc-macro"):
            package = self.external("bytes", "2.0.0")
            package["targets"] = [{"name": "unaudited", "kind": [kind],
                                   "src_path": "/registry/bytes-2.0.0/build.rs"}]
            self.assertIn(f"unaudited {kind} authority", " ".join(
                guard.verify_package(package, set())))

    def test_exact_arrow_build_and_macro_sources_are_allowed(self):
        for name, version, kind in (("ahash", "2.0.0", "custom-build"),
                                    ("zerocopy-derive", "2.0.0", "proc-macro"),
                                    ("serde_derive", "2.0.0", "proc-macro")):
            package = self.external(name, version)
            package["targets"] = [{"name": name, "kind": [kind],
                                   "src_path": f"/registry/{name}-{version}/src/lib.rs"}]
            self.assertEqual(guard.verify_package(package, set()), [])
            package["source"] = None
            self.assertTrue(guard.verify_package(package, set()))

    def test_audited_build_source_cannot_escape_package(self):
        package = self.external("ahash", "2.0.0")
        package["targets"] = [{"name": "build", "kind": ["custom-build"],
                               "src_path": "/foreign/build.rs"}]
        self.assertIn("source escapes its audited package", " ".join(
            guard.verify_package(package, set())))

    def test_arrow_geometry_keeps_only_its_audited_build_authority(self):
        for name in ("arrow-row", "arrow-ipc", "rustc_version", "semver"):
            self.assertEqual(guard.verify_package(self.external(name), set()), [])
        package = self.external("flatbuffers")
        package["targets"] = [{"name": "build", "kind": ["custom-build"],
                               "src_path": "/registry/flatbuffers-1.0.0/build.rs"}]
        package["dependencies"] = [{"name": "rustc_version", "kind": "build"}]
        self.assertEqual(guard.verify_package(package, set()), [])
        package["dependencies"].append({"name": "tokio", "kind": "build"})
        self.assertIn("unaudited build dependencies: tokio", " ".join(
            guard.verify_package(package, set())))

    def test_audited_registry_build_owner_cannot_acquire_runtime(self):
        package = self.external("ahash", "2.0.0")
        package["dependencies"] = [{"name": "tokio", "kind": "build"}]
        self.assertIn("unaudited build dependencies: tokio", " ".join(
            guard.verify_package(package, set())))

    def test_pure_owner_cannot_execute_proc_macro(self):
        result = self.fixture(extra='[lib]\nproc-macro = true\n')
        self.assert_rejected(result, "executes a proc-macro target")

    def test_constant_owner_has_no_normal_feature_or_build_escape(self):
        package = {"id": "constant-local", "name": "novarocks-constant-contract",
                   "source": None, "dependencies": [], "features": {}, "targets": []}
        self.assertEqual(guard.verify_package(package, {"constant-local"}), [])
        package["features"] = {"optional-runtime": []}
        self.assertIn("unaudited Cargo feature variants", " ".join(
            guard.verify_package(package, {"constant-local"})))
        package["features"] = {}
        package["dependencies"] = [{"name": "tokio", "kind": None, "optional": True,
                                    "target": None, "features": [], "uses_default_features": True}]
        violations = " ".join(guard.verify_package(package, {"constant-local"}))
        self.assertIn("hides a normal edge", violations)
        self.assertIn("runtime/wire/provider/storage capability: tokio", violations)
        package["dependencies"][0].update(optional=False, target='cfg(target_os="none")')
        self.assertIn("hides a normal edge", " ".join(
            guard.verify_package(package, {"constant-local"})))
        package["dependencies"][0].update(kind="build", target=None)
        self.assertIn("declares a build dependency", " ".join(
            guard.verify_package(package, {"constant-local"})))


class ActualMetadataTests(unittest.TestCase):
    """Mutate real current Cargo facts, never a dependency-free Result surrogate."""

    @classmethod
    def setUpClass(cls):
        cls.manifest = CHECKER.parents[2] / "Cargo.toml"
        support = guard.metadata_support()
        cls.metadata = support.cargo_metadata(cls.manifest)
        cls.graph = support.Graph(cls.metadata)
        cls.selected_ids = [p["id"] for p in guard.selected_packages(cls.manifest, cls.graph)]

    def package(self, metadata, name):
        graph = guard.metadata_support().Graph(metadata)
        return graph.workspace_package(name)

    def violations(self, metadata):
        graph = guard.metadata_support().Graph(metadata)
        packages = [graph.packages_by_id[identity] for identity in self.selected_ids]
        return " ".join(guard.verify_selected_packages(packages, graph))

    def failure_edge(self):
        result = self.graph.workspace_package("novarocks-result-contract")
        return copy.deepcopy(next(d for d in result["dependencies"]
                                  if d["name"] == guard.FAILURE_CONTRACT))

    def test_actual_selected_closure_accepts_only_the_neutral_leaf(self):
        self.assertIn(self.graph.workspace_package(guard.FAILURE_CONTRACT)["id"],
                      self.selected_ids)
        self.assertEqual(self.violations(self.metadata), "")
        result = self.graph.workspace_package("novarocks-result-contract")
        self.assertEqual({d["name"] for d in result["dependencies"]},
                         {guard.FAILURE_CONTRACT})
        self.assertEqual(self.graph.workspace_package(guard.FAILURE_CONTRACT)["dependencies"], [])

    def test_actual_result_contract_cannot_hide_other_edge_kinds(self):
        for kind in ("dev", "build"):
            with self.subTest(kind=kind):
                data = copy.deepcopy(self.metadata)
                edge = self.failure_edge()
                edge["kind"] = kind
                self.package(data, "novarocks-result-contract")["dependencies"].append(edge)
                self.assertIn("requires exactly one normal dependency on novarocks-failure-contract",
                              self.violations(data))

    def test_actual_failure_leaf_rejects_all_declared_edge_kinds(self):
        for kind, optional, target, renamed in (
                (None, False, None, None),
                (None, True, None, "renamed_pure"),
                (None, False, 'cfg(target_os = "none")', None),
                ("build", False, None, None),
                ("dev", False, None, None)):
            with self.subTest(kind=kind, optional=optional, target=target):
                data = copy.deepcopy(self.metadata)
                edge = self.failure_edge()
                # An otherwise pure dependency is sufficient to violate the
                # zero-edge leaf; no runtime-name denylist can make this pass.
                edge.update(name="novarocks-type-contract", kind=kind,
                            optional=optional, target=target, rename=renamed)
                self.package(data, guard.FAILURE_CONTRACT)["dependencies"].append(edge)
                self.assertIn("novarocks-failure-contract must remain dependency-free",
                              self.violations(data))

    def test_actual_failure_leaf_rejects_resolve_only_authority(self):
        for kind in (None, "build", "dev"):
            with self.subTest(kind=kind):
                data = copy.deepcopy(self.metadata)
                graph = guard.metadata_support().Graph(data)
                leaf = graph.workspace_package(guard.FAILURE_CONTRACT)
                target = graph.workspace_package("novarocks-type-contract")
                graph.resolve_nodes[leaf["id"]]["deps"].append({
                    "name": "novarocks_type_contract", "pkg": target["id"],
                    "dep_kinds": [{"kind": kind, "target": None}]})
                self.assertIn("resolved closure must remain dependency-free",
                              self.violations(data))
        data = copy.deepcopy(self.metadata)
        graph = guard.metadata_support().Graph(data)
        leaf = graph.workspace_package(guard.FAILURE_CONTRACT)
        graph.resolve_nodes[leaf["id"]]["dependencies"].append(
            graph.workspace_package("novarocks-type-contract")["id"])
        self.assertIn("resolved closure must remain dependency-free", self.violations(data))

    def test_actual_failure_leaf_missing_resolve_facts_fail_closed(self):
        data = copy.deepcopy(self.metadata)
        leaf_id = self.graph.workspace_package(guard.FAILURE_CONTRACT)["id"]
        data["resolve"]["nodes"] = [n for n in data["resolve"]["nodes"]
                                    if n["id"] != leaf_id]
        self.assertIn("resolve graph omits dependency facts", self.violations(data))

    def test_actual_fact_consumers_still_reject_runtime_wire_provider_storage(self):
        for owner in sorted(guard.FAILURE_FACT_CONSUMERS):
            for forbidden in ("tokio", "tonic", "novarocks-connector-iceberg", "object_store"):
                with self.subTest(owner=owner, forbidden=forbidden):
                    data = copy.deepcopy(self.metadata)
                    edge = self.failure_edge()
                    edge.update(name=forbidden, optional=True, rename="hidden_capability")
                    self.package(data, owner)["dependencies"].append(edge)
                    self.assertIn("runtime/wire/provider/storage capability: " + forbidden,
                                  self.violations(data))

    def test_actual_failure_leaf_cannot_select_a_foreign_authority(self):
        data = copy.deepcopy(self.metadata)
        self.package(data, guard.FAILURE_CONTRACT)["source"] = guard.REGISTRY_SOURCE
        self.assertIn("not the workspace-owned pure package", self.violations(data))


if __name__ == "__main__":
    unittest.main()
