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

"""Verify that every NovaRocks Cargo graph uses one DataSketches release.

The contract is intentionally expressed in Cargo's resolved package graph and
lockfiles.  Manifest spelling, consumer count, and source-tree shape are not
dependency identities and therefore are not inspected here.
"""

import argparse
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path


PACKAGE = "datasketches"
VERSION = "0.5.0-rc.1"
SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
CHECKSUM = "407f3fe0c32e6547cb8637b11a8a765ff027afa31e5f6f732b23f8d74672087b"

# Build outputs, disposable test output, and checked-in third-party sources are
# not NovaRocks-owned workspace roots.  A vendored package can still appear in
# a Nova graph through the owning workspace's metadata; walking into its own
# upstream workspace would instead validate an unrelated development graph.
EXCLUDED_DIRECTORY_NAMES = {
    ".git",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "logs",
    "reports",
    "target",
    "tmp",
    "vendor",
}


def fail(message: str) -> None:
    print(f"DataSketches source violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def excluded_directory(name: str) -> bool:
    return name in EXCLUDED_DIRECTORY_NAMES or name.startswith(".tmp")


def load_toml(path: Path, description: str) -> dict:
    try:
        with path.open("rb") as stream:
            return tomllib.load(stream)
    except OSError as error:
        fail(f"cannot read {description} {path}: {error}")
    except tomllib.TOMLDecodeError as error:
        fail(f"invalid {description} {path}: {error}")


def discover_workspace_manifests(repo_root: Path) -> list[Path]:
    manifests = []
    for directory, child_directories, filenames in os.walk(repo_root, topdown=True):
        child_directories[:] = sorted(
            name
            for name in child_directories
            if not excluded_directory(name)
            and not (Path(directory) / name).is_symlink()
        )
        if "Cargo.toml" not in filenames:
            continue
        manifest = Path(directory) / "Cargo.toml"
        if "workspace" in load_toml(manifest, "Cargo manifest"):
            manifests.append(manifest.resolve())

    if not manifests:
        fail(f"no Cargo workspace roots found under {repo_root}")
    return sorted(manifests)


def cargo_metadata(cargo: str, manifest: Path) -> dict:
    command = [
        cargo,
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--manifest-path",
        str(manifest),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as error:
        fail(f"cannot execute Cargo for workspace {manifest.parent}: {error}")
    except subprocess.CalledProcessError as error:
        diagnostic = error.stderr.strip() or "Cargo emitted no stderr"
        fail(
            f"cargo metadata --locked failed for workspace {manifest.parent} "
            f"(exit {error.returncode}): {diagnostic}"
        )

    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        fail(f"Cargo returned invalid metadata for workspace {manifest.parent}: {error}")


def describe_package(package: dict) -> str:
    return (
        f"id={package.get('id', '<missing>')} "
        f"version={package.get('version', '<missing>')} "
        f"source={package.get('source', '<path>')} "
        f"manifest={package.get('manifest_path', '<missing>')}"
    )


def verify_metadata_package(package: dict, workspace: Path) -> None:
    if package.get("version") != VERSION:
        fail(
            f"workspace {workspace} package version must be {VERSION}: "
            f"{describe_package(package)}"
        )
    if package.get("source") != SOURCE:
        fail(
            f"workspace {workspace} package source must be crates.io ({SOURCE}): "
            f"{describe_package(package)}"
        )
    metadata_checksum = package.get("checksum")
    if metadata_checksum is not None and metadata_checksum != CHECKSUM:
        fail(
            f"workspace {workspace} metadata checksum must be {CHECKSUM}: "
            f"{describe_package(package)} checksum={metadata_checksum}"
        )


def verify_lock_record(record: dict, workspace: Path) -> None:
    if record.get("version") != VERSION:
        fail(
            f"workspace {workspace} Cargo.lock version must be {VERSION}: "
            f"version={record.get('version', '<missing>')} "
            f"source={record.get('source', '<path>')}"
        )
    if record.get("source") != SOURCE:
        fail(
            f"workspace {workspace} Cargo.lock source must be crates.io ({SOURCE}): "
            f"version={record.get('version', '<missing>')} "
            f"source={record.get('source', '<path>')}"
        )
    if record.get("checksum") != CHECKSUM:
        fail(
            f"workspace {workspace} Cargo.lock checksum must be {CHECKSUM}: "
            f"version={record.get('version', '<missing>')} "
            f"checksum={record.get('checksum', '<missing>')}"
        )


def verify_workspace(cargo: str, manifest: Path) -> bool:
    workspace = manifest.parent
    metadata = cargo_metadata(cargo, manifest)
    packages = metadata.get("packages")
    if not isinstance(packages, list):
        fail(f"Cargo metadata packages are missing for workspace {workspace}")

    metadata_packages = [package for package in packages if package.get("name") == PACKAGE]
    if len(metadata_packages) > 1:
        details = "; ".join(describe_package(package) for package in metadata_packages)
        fail(
            f"workspace {workspace} must contain exactly one resolved {PACKAGE} "
            f"package, found {len(metadata_packages)}: {details}"
        )

    lock_path = workspace / "Cargo.lock"
    lock = load_toml(lock_path, "Cargo lockfile")
    lock_packages = lock.get("package", [])
    if not isinstance(lock_packages, list):
        fail(f"Cargo lockfile package records are invalid for workspace {workspace}")
    lock_records = [record for record in lock_packages if record.get("name") == PACKAGE]
    if len(lock_records) > 1:
        details = "; ".join(
            f"version={record.get('version', '<missing>')} "
            f"source={record.get('source', '<path>')}"
            for record in lock_records
        )
        fail(
            f"workspace {workspace} must contain exactly one locked {PACKAGE} package, "
            f"found {len(lock_records)}: {details}"
        )

    if bool(metadata_packages) != bool(lock_records):
        fail(
            f"workspace {workspace} metadata/Cargo.lock disagree about {PACKAGE}: "
            f"metadata_nodes={len(metadata_packages)} lock_records={len(lock_records)}"
        )
    if not metadata_packages:
        return False

    package = metadata_packages[0]
    record = lock_records[0]
    verify_metadata_package(package, workspace)
    verify_lock_record(record, workspace)
    if (
        package.get("version") != record.get("version")
        or package.get("source") != record.get("source")
    ):
        fail(
            f"workspace {workspace} metadata package does not match Cargo.lock: "
            f"{describe_package(package)}"
        )
    return True


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify the resolved DataSketches source in every Cargo workspace."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=default_repo_root(),
        help="repository root to scan (default: the checker repository)",
    )
    parser.add_argument(
        "--cargo",
        default=os.environ.get("CARGO", "cargo"),
        help="Cargo executable (default: $CARGO or cargo)",
    )
    arguments = parser.parse_args()

    repo_root = arguments.repo_root.resolve()
    if not repo_root.is_dir():
        fail(f"repository root is not a directory: {repo_root}")

    manifests = discover_workspace_manifests(repo_root)
    canonical_workspaces = [
        manifest.parent
        for manifest in manifests
        if verify_workspace(arguments.cargo, manifest)
    ]
    if not canonical_workspaces:
        fail(
            f"no canonical {PACKAGE} {VERSION} package was resolved by any of "
            f"the {len(manifests)} discovered workspaces"
        )

    print(
        "DataSketches source: PASS "
        f"({len(manifests)} workspaces, {len(canonical_workspaces)} canonical graphs, "
        f"{VERSION}, crates.io, checksum {CHECKSUM})"
    )


if __name__ == "__main__":
    main()
