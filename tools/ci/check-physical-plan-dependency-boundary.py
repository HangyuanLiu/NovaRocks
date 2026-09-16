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

"""Verify the pure final physical-plan Cargo dependency boundary.

The final physical plan is a carrier-neutral semantic contract.  It may reuse
the neutral type and Connector handle vocabularies, but it must not acquire an
application, execution kernel, provider implementation, wire codec, RPC stack,
or task runtime.

The checker deliberately combines Cargo metadata with Cargo's package-selected
dependency tree:

* every declared dependency kind includes optional dependencies that default
  feature resolution does not activate, and preserves a dependency's canonical
  package name even when the crate is renamed;
* `cargo tree -p --target all` computes the resolved normal closure for every
  target in the physical-plan package's own feature context, so inactive target
  edges remain visible while features enabled only by unrelated workspace
  members cannot create false dependencies;
* every package in the selected normal closure is inspected for build
  dependencies and build scripts, because either executes while compiling the
  contract even though it is absent from a normal-only dependency tree;
* the two repository-owned neutral contracts expose no Cargo feature or target
  variation, so optional and target-specific edges cannot hide an unaudited
  closure behind a different build configuration.

Neither source is sufficient on its own.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


PACKAGE_NAME = "novarocks-physical-plan"
TYPE_CONTRACT = "novarocks-type-contract"
CONNECTOR_CONTRACT = "novarocks-connector-contract"

# These are allowed direct internal dependencies, not required dependencies.
# Removing one as the contract gets smaller remains legal.
DIRECT_INTERNAL_ALLOW_LIST = frozenset({TYPE_CONTRACT, CONNECTOR_CONTRACT})
DIRECT_PACKAGE_ALLOW_LIST = frozenset(
    {"arrow-schema", TYPE_CONTRACT, CONNECTOR_CONTRACT}
)

# Dependency direction is part of the architecture. The type contract is the
# lower-level vocabulary; the Connector contract may consume it, but neither
# contract may acquire physical-plan or application authority.
INTERNAL_CONTRACT_NORMAL_ALLOW_LISTS = {
    TYPE_CONTRACT: frozenset({"arrow-schema"}),
    CONNECTOR_CONTRACT: frozenset({"bytes", TYPE_CONTRACT}),
}

# This vocabulary is used only for declared-edge diagnostics. Resolved closure
# admission below uses exact Cargo package identities and must never fall back
# to these names.
RESOLVED_PACKAGE_ALLOW_LIST = frozenset(
    {
        "arrow-schema",
        "bytes",
        "novarocks-connector-contract",
        "novarocks-type-contract",
    }
)

CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
EXTERNAL_PACKAGE_ALLOW_LIST = {
    "arrow-schema": {
        "id": f"{CRATES_IO_SOURCE}#arrow-schema@58.2.0",
        "source": CRATES_IO_SOURCE,
        "version": "58.2.0",
    },
    "bytes": {
        "id": f"{CRATES_IO_SOURCE}#bytes@1.11.0",
        "source": CRATES_IO_SOURCE,
        "version": "1.11.0",
    },
}

NORMAL = None


class Capability:
    """A forbidden capability, matched by exact package name or prefix."""

    def __init__(self, label, exact=(), prefixes=(), excluded=()):
        self.label = label
        self.exact = frozenset(exact)
        self.prefixes = tuple(prefixes)
        self.excluded = frozenset(excluded)

    def hits(self, names):
        return sorted(
            name
            for name in names
            if name not in self.excluded
            and (name in self.exact or name.startswith(self.prefixes))
        )


WIRE_AND_RPC = Capability(
    "wire/RPC capability",
    exact={"prost", "tonic"},
    prefixes=("prost-", "tonic-"),
)

TASK_RUNTIME = Capability(
    "task runtime capability",
    exact={"async-std", "rayon", "smol", "tokio"},
)

APPLICATION_OWNER = Capability(
    "application/execution owner",
    exact={
        "novarocks-backend",
        "novarocks-execution",
        "novarocks-frontend",
        "novarocks-server",
        "novarocks-sql",
    },
)

WIRE_OWNER = Capability(
    "Native wire owner",
    exact={
        "novarocks-plan-codec",
        "novarocks-proto-codec",
        "novarocks-proto-models",
        "novarocks-task-codec",
    },
)

PROVIDER_OR_STORAGE_OWNER = Capability(
    "provider/storage owner",
    prefixes=("novarocks-connector-", "novarocks-state-store-"),
    excluded={CONNECTOR_CONTRACT},
)

FORBIDDEN_CAPABILITIES = (
    WIRE_AND_RPC,
    TASK_RUNTIME,
    APPLICATION_OWNER,
    WIRE_OWNER,
    PROVIDER_OR_STORAGE_OWNER,
)


def fail(messages):
    for message in messages:
        print(f"physical-plan dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def cargo_metadata(manifest_path):
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--offline",
        "--manifest-path",
        str(manifest_path),
    ]
    try:
        return json.loads(
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ).stdout
        )
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error


def resolved_normal_packages(manifest_path, graph):
    """Return the package-selected normal closure with exact Cargo identities."""

    command = [
        "cargo",
        "tree",
        "--package",
        PACKAGE_NAME,
        "--edges",
        "normal",
        "--target",
        "all",
        "--no-dedupe",
        "--locked",
        "--offline",
        "--prefix",
        "depth",
        "--format",
        "|{p}",
        "--manifest-path",
        str(manifest_path),
    ]
    try:
        output = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error

    root = graph.workspace_package(PACKAGE_NAME)
    packages = {}
    parents = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"(\d+)\|(.*)", line.strip())
        if match is None:
            fail([f"cannot parse Cargo tree depth and package identity: {line}"])
        depth = int(match.group(1))
        label = match.group(2)
        if depth == 0:
            package = graph.package_from_tree_label(label, {root["id"]})
        else:
            parent = parents.get(depth - 1)
            if parent is None:
                fail([f"Cargo tree has no parent for depth {depth}: {line}"])
            package = graph.package_from_tree_label(
                label, graph.normal_dependency_ids(parent["id"])
            )
        parents[depth] = package
        parents = {
            candidate_depth: candidate
            for candidate_depth, candidate in parents.items()
            if candidate_depth <= depth
        }
        if depth != 0:
            packages[package["id"]] = package
    return packages


class Graph:
    def __init__(self, metadata):
        self.packages_by_id = {
            package["id"]: package for package in metadata["packages"]
        }
        self.packages_by_name = {}
        for package in metadata["packages"]:
            self.packages_by_name.setdefault(package["name"], []).append(package)
        self.workspace_members = frozenset(metadata["workspace_members"])
        self.resolve_nodes = {
            node["id"]: node for node in metadata.get("resolve", {}).get("nodes", [])
        }

    def workspace_package(self, name):
        matches = [
            package
            for package in self.packages_by_name.get(name, [])
            if package["id"] in self.workspace_members
        ]
        if len(matches) != 1:
            fail([f"Cargo workspace must contain exactly one {name} package"])
        return matches[0]

    def normal_dependency_ids(self, package_id):
        node = self.resolve_nodes.get(package_id)
        if node is None:
            fail([f"Cargo metadata resolve graph omits package: {package_id}"])
        return {
            dependency["pkg"]
            for dependency in node["deps"]
            if any(kind["kind"] is NORMAL for kind in dependency["dep_kinds"])
        }

    def package_from_tree_label(self, label, candidate_ids=None):
        match = re.fullmatch(r"(\S+) v(\S+?)(?: \((.+)\))?", label)
        if match is None:
            fail([f"cannot parse Cargo tree package identity: {label}"])
        name, version, location = match.groups()
        matches = [
            package
            for package in self.packages_by_name.get(name, [])
            if package["version"] == version
            and (candidate_ids is None or package["id"] in candidate_ids)
        ]
        if location is not None:
            location_path = Path(location)
            path_matches = [
                package
                for package in matches
                if package["source"] is None
                and Path(package["manifest_path"]).parent.resolve()
                == location_path.resolve()
            ]
            if path_matches:
                matches = path_matches
            else:
                source_matches = [
                    package
                    for package in matches
                    if package["source"] is not None
                    and location in package["source"]
                ]
                if source_matches:
                    matches = source_matches
        if len(matches) != 1:
            fail([f"Cargo tree package identity is ambiguous: {label}"])
        return matches[0]

    def external_package(self, name, expected):
        matches = [
            package
            for package in self.packages_by_name.get(name, [])
            if package["id"] == expected["id"]
            and package["source"] == expected["source"]
            and package["version"] == expected["version"]
            and Path(package["manifest_path"]).name == "Cargo.toml"
            and Path(package["manifest_path"]).parent.name
            == f"{name}-{expected['version']}"
        ]
        if len(matches) > 1:
            fail(
                [
                    "Cargo metadata contains more than one audited external "
                    f"package identity for {name}: {expected['id']}"
                ]
            )
        return matches[0] if matches else None


def package_identity(package):
    """Return every Cargo field that distinguishes one package authority."""

    return (
        package["id"],
        package["source"],
        package["version"],
        str(Path(package["manifest_path"]).resolve()),
    )


def describe_package_identity(package):
    source = package["source"] if package["source"] is not None else "local"
    return (
        f"{package['name']} v{package['version']} "
        f"(id={package['id']}, source={source}, manifest={package['manifest_path']})"
    )


def resolved_package_allow_list(graph):
    """Resolve the audited identities without trusting a dependency's name."""

    packages = [
        graph.workspace_package(TYPE_CONTRACT),
        graph.workspace_package(CONNECTOR_CONTRACT),
    ]
    packages.extend(
        package
        for name, expected in sorted(EXTERNAL_PACKAGE_ALLOW_LIST.items())
        if (package := graph.external_package(name, expected)) is not None
    )
    return frozenset(package_identity(package) for package in packages)

def declared_dependencies_by_kind(package):
    """Return canonical package names, including optional and renamed edges."""

    dependencies = {NORMAL: set(), "dev": set(), "build": set()}
    unknown_kinds = set()
    for dependency in package["dependencies"]:
        kind = dependency["kind"]
        if kind in dependencies:
            dependencies[kind].add(dependency["name"])
        else:
            unknown_kinds.add(str(kind))
    return dependencies, unknown_kinds


def capability_violations(names, location):
    violations = []
    for capability in FORBIDDEN_CAPABILITIES:
        hits = capability.hits(names)
        if hits:
            violations.append(
                f"{location} contains forbidden {capability.label}: " + ", ".join(hits)
            )
    return violations


def verify_dependency_feature_policy(package, owner):
    violations = []
    configured_features = sorted(
        f"{dependency['name']}=[{','.join(dependency['features'])}]"
        for dependency in package["dependencies"]
        if dependency["kind"] is NORMAL and dependency["features"]
    )
    if configured_features:
        violations.append(
            f"{owner} enables dependency features, but its dependency semantics "
            "must be invariant: " + ", ".join(configured_features)
        )
    disabled_defaults = sorted(
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] is NORMAL and not dependency["uses_default_features"]
    )
    if disabled_defaults:
        violations.append(
            f"{owner} disables dependency default features, but the audited surface "
            "uses each dependency's default feature policy: "
            + ", ".join(disabled_defaults)
        )
    return violations


def verify_declared_dependencies(package):
    dependencies, unknown_kinds = declared_dependencies_by_kind(package)
    normal = dependencies[NORMAL]
    dev = dependencies["dev"]
    build = dependencies["build"]
    violations = capability_violations(normal, "declared normal dependencies")
    unexpected = sorted(normal - DIRECT_PACKAGE_ALLOW_LIST)
    if unexpected:
        violations.append(
            "declares packages outside the exact direct allow-list "
            f"({', '.join(sorted(DIRECT_PACKAGE_ALLOW_LIST))}): "
            + ", ".join(unexpected)
        )
    unexpected_internal = sorted(
        name
        for name in normal
        if name.startswith("novarocks-") and name not in DIRECT_INTERNAL_ALLOW_LIST
    )
    if unexpected_internal:
        violations.append(
            "declares internal normal dependencies outside the direct allow-list "
            f"({', '.join(sorted(DIRECT_INTERNAL_ALLOW_LIST))}): "
            + ", ".join(unexpected_internal)
        )
    if build:
        violations.extend(
            capability_violations(build, "declared build dependencies")
        )
        violations.append(
            "declares build dependencies, but the physical-plan contract permits none: "
            + ", ".join(sorted(build))
        )
    if dev:
        violations.extend(capability_violations(dev, "declared dev dependencies"))
        violations.append(
            "declares dev dependencies, but the physical-plan contract permits none: "
            + ", ".join(sorted(dev))
        )
    if unknown_kinds:
        violations.append(
            "Cargo metadata contains unknown dependency kinds: "
            + ", ".join(sorted(unknown_kinds))
        )
    optional = sorted(
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["optional"]
    )
    if optional:
        violations.append(
            "declares optional dependencies, but the physical-plan contract "
            "requires one closed dependency surface: " + ", ".join(optional)
        )
    targeted = sorted(
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["target"] is not None
    )
    if targeted:
        violations.append(
            "declares target-specific dependencies, but the physical-plan contract "
            "must be target invariant: " + ", ".join(targeted)
        )
    if package.get("features"):
        violations.append(
            "declares Cargo features, but the physical-plan contract requires one "
            "closed dependency surface: " + ", ".join(sorted(package["features"]))
        )
    violations.extend(verify_dependency_feature_policy(package, PACKAGE_NAME))
    return dependencies, violations


def verify_package_targets(package, owner=PACKAGE_NAME):
    custom_build_targets = sorted(
        target["name"]
        for target in package.get("targets", [])
        if "custom-build" in target.get("kind", [])
    )
    if not custom_build_targets:
        return []
    return [
        f"{owner} declares a custom build target, but the physical-plan closure "
        "permits no build.rs: "
        + ", ".join(custom_build_targets)
    ]


def verify_internal_contract_surface(graph):
    """Keep repository-owned neutral contracts invariant across configurations."""

    violations = []
    for name in sorted(DIRECT_INTERNAL_ALLOW_LIST):
        package = graph.workspace_package(name)
        dependencies, unknown_kinds = declared_dependencies_by_kind(package)
        normal_dependencies = dependencies[NORMAL]
        owner_allow_list = INTERNAL_CONTRACT_NORMAL_ALLOW_LISTS[name]
        unexpected = sorted(normal_dependencies - owner_allow_list)
        if unexpected:
            violations.append(
                f"{name} declares normal dependencies outside its exact owner "
                f"allow-list ({', '.join(sorted(owner_allow_list))}): "
                + ", ".join(unexpected)
            )
        optional = sorted(
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is NORMAL and dependency["optional"]
        )
        if optional:
            violations.append(
                f"{name} declares optional normal dependencies, but neutral contract "
                "features must not alter the physical-plan closure: "
                + ", ".join(optional)
            )
        targeted = sorted(
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is NORMAL and dependency["target"] is not None
        )
        if targeted:
            violations.append(
                f"{name} declares target-specific normal dependencies, but the "
                "physical-plan closure must be target invariant: "
                + ", ".join(targeted)
            )
        if package.get("features"):
            violations.append(
                f"{name} declares Cargo features, but the physical-plan contract "
                "requires one closed dependency surface: "
                + ", ".join(sorted(package["features"]))
            )
        violations.extend(verify_dependency_feature_policy(package, name))
        build_dependencies = dependencies["build"]
        if build_dependencies:
            violations.append(
                f"{name} declares build dependencies, but the physical-plan closure "
                "permits none: " + ", ".join(sorted(build_dependencies))
            )
        dev_dependencies = dependencies["dev"]
        if dev_dependencies:
            violations.extend(
                capability_violations(
                    dev_dependencies, f"{name} declared dev dependencies"
                )
            )
            violations.append(
                f"{name} declares dev dependencies, but the physical-plan closure "
                "permits none: " + ", ".join(sorted(dev_dependencies))
            )
        if unknown_kinds:
            violations.append(
                f"{name} contains unknown dependency kinds: "
                + ", ".join(sorted(unknown_kinds))
            )
        violations.extend(verify_package_targets(package, name))
    return violations


def verify_closure_declared_boundary(closure):
    """Audit build authority and target variants for every resolved package."""

    violations = []
    for package in sorted(closure.values(), key=lambda item: item["id"]):
        name = package["name"]
        dependencies, unknown_kinds = declared_dependencies_by_kind(package)
        build_dependencies = dependencies["build"]
        if build_dependencies:
            violations.append(
                f"resolved normal dependency {name} declares build dependencies: "
                + ", ".join(sorted(build_dependencies))
            )
        if unknown_kinds:
            violations.append(
                f"resolved normal dependency {name} contains unknown dependency kinds: "
                + ", ".join(sorted(unknown_kinds))
            )
        targeted_normal = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is NORMAL and dependency["target"] is not None
        }
        violations.extend(
            capability_violations(
                targeted_normal,
                f"resolved normal dependency {name} target-specific dependencies",
            )
        )
        unexpected_targeted = sorted(
            targeted_normal - RESOLVED_PACKAGE_ALLOW_LIST
        )
        if unexpected_targeted:
            violations.append(
                f"resolved normal dependency {name} declares target-specific normal "
                "dependencies outside the exact audited closure allow-list: "
                + ", ".join(unexpected_targeted)
            )
        violations.extend(verify_package_targets(package, name))
    return violations


def verify_resolved_closure(manifest_path, graph):
    closure = resolved_normal_packages(manifest_path, graph)
    names = {package["name"] for package in closure.values()}
    violations = capability_violations(names, "resolved normal dependency closure")
    allowed_identities = resolved_package_allow_list(graph)
    unexpected = sorted(
        (
            package
            for package in closure.values()
            if package_identity(package) not in allowed_identities
        ),
        key=lambda package: package["id"],
    )
    if unexpected:
        violations.append(
            "resolved normal dependency closure contains package identities outside "
            "the exact audited allow-list: "
            + "; ".join(describe_package_identity(package) for package in unexpected)
        )
    return closure, violations


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description="Verify the final physical-plan Cargo dependency boundary."
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    arguments = parser.parse_args()
    manifest_path = (arguments.manifest_path or default_manifest_path()).resolve()

    graph = Graph(cargo_metadata(manifest_path))
    package = graph.workspace_package(PACKAGE_NAME)
    declared, declared_violations = verify_declared_dependencies(package)
    target_violations = verify_package_targets(package)
    contract_surface_violations = verify_internal_contract_surface(graph)
    closure, closure_violations = verify_resolved_closure(manifest_path, graph)
    closure_declared_violations = verify_closure_declared_boundary(closure)
    violations = (
        declared_violations
        + target_violations
        + contract_surface_violations
        + closure_violations
        + closure_declared_violations
    )
    if violations:
        fail(violations)

    closure_names = {package["name"] for package in closure.values()}
    internal = sorted(name for name in closure_names if name.startswith("novarocks-"))
    print(
        "novarocks-physical-plan declared normal dependencies: "
        + (", ".join(sorted(declared[NORMAL])) if declared[NORMAL] else "none")
    )
    print(
        f"novarocks-physical-plan resolved normal dependency closure: "
        f"{len(closure)} package identities; internal crates: "
        + (", ".join(internal) if internal else "none")
    )
    print("physical-plan dependency boundary: PASS")


if __name__ == "__main__":
    main()
