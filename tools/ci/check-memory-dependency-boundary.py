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

"""Verify runtime-neutral memory and byte-oriented StateStore Cargo boundaries.

The memory core declares no normal dependencies. Its dev closure may use
neutral tooling, but never first-party, Arrow or Tokio capabilities. The
StateStore API's normal closure must remain independent of the memory core.

Checks use both Cargo's resolved graph and declared dependency edges, including
optional edges not enabled by default. They protect capability reachability,
not source spelling, retired type names, or one historical crate layout.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


MEMORY_CORE = "novarocks-memory"
STATE_STORE_API = "novarocks-state-store-api"
FIRST_PARTY_PREFIX = "novarocks-"


class Capability:
    """A forbidden capability, matched by exact name or name prefix."""

    def __init__(self, label, exact=(), prefixes=()):
        self.label = label
        self.exact = frozenset(exact)
        self.prefixes = tuple(prefixes)

    def matches(self, name):
        return name in self.exact or name.startswith(self.prefixes)

    def hits(self, names):
        return sorted(name for name in names if self.matches(name))


# Columnar execution is outside the carrier-neutral core, including tests.
COLUMNAR_RUNTIME = Capability(
    "columnar runtime",
    prefixes=("arrow",),
)
# Async runtime. `tokio` by exact name is sufficient: every `tokio-*` helper
# crate carries `tokio` in its own normal closure, so the closure walk catches
# them without this guard enumerating the family.
ASYNC_RUNTIME = Capability("async runtime", exact={"tokio"})
# Any workspace crate at all. The core is a leaf of the first-party graph.
FIRST_PARTY = Capability("first-party crate", prefixes=(FIRST_PARTY_PREFIX,))

# Assertion 2's forbidden set for the core's dev closure.
CORE_DEV_FORBIDDEN_CAPABILITIES = (
    FIRST_PARTY,
    COLUMNAR_RUNTIME,
    ASYNC_RUNTIME,
)

NORMAL = None
DEV = "dev"
BUILD = "build"
REPORTED_KINDS = (
    (NORMAL, "normal", "production, enforced"),
    (DEV, "dev", "test-only, enforced for the core"),
    (BUILD, "build", "test-only, reported"),
)


def fail(message):
    print(f"memory dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def cargo_metadata(manifest_path):
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
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


class Graph:
    """Resolved Cargo graph, indexed for closure walks."""

    def __init__(self, metadata):
        resolve = metadata.get("resolve")
        if resolve is None:
            fail("Cargo metadata must include resolve nodes; do not pass --no-deps")
        self.metadata = metadata
        self.names_by_id = {package["id"]: package["name"] for package in metadata["packages"]}
        self.nodes = {node["id"]: node for node in resolve.get("nodes", [])}
        self.packages_by_name = {}
        for package in metadata["packages"]:
            self.packages_by_name.setdefault(package["name"], []).append(package)

    def package(self, name):
        matches = self.packages_by_name.get(name, [])
        if len(matches) != 1:
            fail(f"Cargo metadata must contain exactly one {name} package")
        return matches[0]

    def has_package(self, name):
        return len(self.packages_by_name.get(name, [])) == 1

    def node(self, package_id):
        node = self.nodes.get(package_id)
        if node is None:
            fail(f"Cargo metadata resolve graph is missing package id {package_id}")
        return node

    def name_of(self, package_id):
        name = self.names_by_id.get(package_id)
        if name is None:
            fail(f"Cargo metadata packages are missing resolved id {package_id}")
        return name

    def closure(self, root_name, kind):
        """Names reachable from ``root_name`` over one dependency kind.

        The root's own edges are filtered to ``kind``; everything deeper is
        followed over normal edges only, because a dependency's dev- and
        build-dependencies are never compiled into the dependent.
        """

        root = self.package(root_name)
        frontier = [
            dependency["pkg"]
            for dependency in self.node(root["id"]).get("deps", [])
            if any(entry.get("kind") == kind for entry in dependency.get("dep_kinds", []))
        ]
        visited = set()
        while frontier:
            package_id = frontier.pop()
            if package_id in visited:
                continue
            visited.add(package_id)
            for dependency in self.node(package_id).get("deps", []):
                if any(
                    entry.get("kind") is NORMAL
                    for entry in dependency.get("dep_kinds", [])
                ):
                    frontier.append(dependency["pkg"])
        return {self.name_of(package_id) for package_id in visited}

    def declared_closure(self, root_name, kind):
        """Follow root edges of ``kind``, then all declared normal edges.

        Optional and target-specific edges count as reachable capabilities.
        A declared package name counts even if inactive in the resolved graph;
        available package manifests provide its transitive declared edges.
        """

        frontier = list(declared_dependency_names(self.package(root_name), kind))
        visited = set()
        while frontier:
            name = frontier.pop()
            if name in visited:
                continue
            visited.add(name)
            for package in self.packages_by_name.get(name, []):
                frontier.extend(declared_dependency_names(package, NORMAL))
        return visited


def declared_dependency_names(package, kind):
    """Declared dependencies of one kind, optional ones included.

    ``cargo metadata`` resolves default features, so an optional dependency
    behind a non-default feature never appears in the resolve graph.  Declaring
    one is still an edge somebody can turn on.
    """

    return {
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] == kind
    }


def capability_hits(names, capabilities):
    """Return ``[(label, [names...])]`` for every capability the set hits."""

    found = []
    for capability in capabilities:
        hits = capability.hits(names)
        if hits:
            found.append((capability.label, hits))
    return found


def describe_hits(hits):
    if not hits:
        return "none"
    return "; ".join(f"{label} ({', '.join(names)})" for label, names in hits)


def verify_core_has_no_dependencies(graph, violations):
    """Assertion 1: the neutral memory core is a graph leaf."""

    closure = graph.closure(MEMORY_CORE, NORMAL)
    if closure:
        violations.append(
            f"{MEMORY_CORE} normal dependency closure must be empty; the neutral "
            "memory core carries no dependencies at all, but it now reaches: "
            + ", ".join(sorted(closure))
        )
    declared = declared_dependency_names(graph.package(MEMORY_CORE), NORMAL)
    if declared:
        violations.append(
            f"{MEMORY_CORE} declares normal dependencies ("
            + ", ".join(sorted(declared))
            + "); the neutral memory core declares none, optional ones included"
        )


def verify_core_dev_closure(graph, violations):
    """Core tests stay runtime-neutral, including optional capability edges."""

    for source, closure in (
        ("dev dependency closure", graph.closure(MEMORY_CORE, DEV)),
        ("declared dev dependency closure", graph.declared_closure(MEMORY_CORE, DEV)),
    ):
        for label, hits in capability_hits(closure, CORE_DEV_FORBIDDEN_CAPABILITIES):
            violations.append(
                f"{MEMORY_CORE} {source} contains a forbidden "
                f"{label}: " + ", ".join(hits)
            )


def verify_state_store_api_is_memory_free(graph, violations):
    """The byte-oriented storage contract has no memory capability edge."""

    for source, closure in (
        ("normal dependency closure", graph.closure(STATE_STORE_API, NORMAL)),
        ("declared normal dependency closure", graph.declared_closure(STATE_STORE_API, NORMAL)),
    ):
        if MEMORY_CORE in closure:
            violations.append(
                f"{STATE_STORE_API} {source} contains a memory crate: {MEMORY_CORE}"
            )


def report(graph):
    """Report resolved and declared capability reach by dependency kind."""

    print("memory dependency boundary report (resolved from Cargo metadata)")
    print(f"  {MEMORY_CORE}")
    for kind, label, disposition in REPORTED_KINDS:
        closure = graph.closure(MEMORY_CORE, kind)
        declared = graph.declared_closure(MEMORY_CORE, kind)
        hits = capability_hits(closure | declared, CORE_DEV_FORBIDDEN_CAPABILITIES)
        line = (
            f"    {label:<6} ({disposition}): {len(closure)} resolved packages; "
            f"{len(declared)} declared reachable packages; "
            f"forbidden capabilities: {describe_hits(hits)}"
        )
        if hits and kind == BUILD:
            line += " [build-only reach, reported not enforced]"
        print(line)
    reached = graph.closure(STATE_STORE_API, NORMAL) | graph.declared_closure(STATE_STORE_API, NORMAL)
    print(
        f"  {STATE_STORE_API}: memory crates in normal closure: "
        + (MEMORY_CORE if MEMORY_CORE in reached else "none")
    )


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description="Verify runtime-neutral memory and byte-oriented StateStore Cargo boundaries."
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    arguments = parser.parse_args()

    manifest_path = (arguments.manifest_path or default_manifest_path()).resolve()
    graph = Graph(cargo_metadata(manifest_path))

    if not graph.has_package(MEMORY_CORE):
        fail(f"Cargo metadata must contain exactly one {MEMORY_CORE} package")
    if not graph.has_package(STATE_STORE_API):
        fail(f"Cargo metadata must contain exactly one {STATE_STORE_API} package")

    violations = []
    verify_core_has_no_dependencies(graph, violations)
    verify_core_dev_closure(graph, violations)
    verify_state_store_api_is_memory_free(graph, violations)
    report(graph)

    if violations:
        for violation in violations:
            print(
                f"memory dependency boundary violation: {violation}",
                file=sys.stderr,
            )
        raise SystemExit(1)

    print("memory dependency boundary: verified")
    print(f"  - {MEMORY_CORE} normal closure and declared table are empty")
    print(f"  - {MEMORY_CORE} resolved and declared dev closures are free of novarocks-*, arrow*, and tokio")
    print(f"  - {STATE_STORE_API} resolved and declared normal closures contain no memory crate")
    print("memory dependency boundary: PASS")


if __name__ == "__main__":
    main()
