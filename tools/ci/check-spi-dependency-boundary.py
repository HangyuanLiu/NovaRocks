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

import argparse
import json
import subprocess
import sys
from pathlib import Path


PACKAGE_NAME = "novarocks-spi"
REQUIRED_NORMAL_DEPENDENCIES = ("async-trait", "bytes", "sha2", "uuid")
OPTIONAL_NORMAL_DEPENDENCIES = ("tokio",)
CONFORMANCE_FEATURE = "state-store-conformance"


def fail(message):
    print(f"SPI dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def cargo_output(manifest_path, *arguments):
    command = [
        "cargo",
        *arguments,
        "--manifest-path",
        str(manifest_path),
    ]
    try:
        return subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error


def package_from_metadata(metadata):
    matches = [
        package
        for package in metadata["packages"]
        if package["name"] == PACKAGE_NAME
    ]
    if len(matches) != 1:
        fail(f"Cargo metadata must contain exactly one {PACKAGE_NAME} package")
    return matches[0]


def normal_dependency_names(package, optional):
    return tuple(
        sorted(
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is None and dependency["optional"] is optional
        )
    )


def feature_enables_tokio(
    feature,
    features,
    visited=None,
):
    visited = set() if visited is None else visited
    if feature in visited:
        return False
    visited.add(feature)
    for member in features.get(feature, []):
        if (
            member in {"tokio", "dep:tokio"}
            or member.startswith("tokio/")
            or member.startswith("tokio?/")
        ):
            return True
        if member in features and feature_enables_tokio(member, features, visited):
            return True
    return False


def verify_declared_boundary(package):
    tokio_dependencies = [
        dependency
        for dependency in package["dependencies"]
        if dependency["kind"] is None and dependency["name"] == "tokio"
    ]
    if len(tokio_dependencies) != 1 or not tokio_dependencies[0]["optional"]:
        fail("Tokio must be an optional normal dependency")

    required = normal_dependency_names(package, optional=False)
    if required != REQUIRED_NORMAL_DEPENDENCIES:
        fail(
            "required normal dependencies must be exactly: "
            + ", ".join(REQUIRED_NORMAL_DEPENDENCIES)
        )

    optional = normal_dependency_names(package, optional=True)
    if optional != OPTIONAL_NORMAL_DEPENDENCIES:
        fail(
            "optional normal dependencies must be exactly: "
            + ", ".join(OPTIONAL_NORMAL_DEPENDENCIES)
        )

    features = package["features"]
    if feature_enables_tokio("default", features):
        fail("default feature graph must not enable Tokio")

    tokio_owners = {
        feature
        for feature in features
        if feature != "default" and feature_enables_tokio(feature, features)
    }
    if tokio_owners != {CONFORMANCE_FEATURE}:
        fail(
            f"Tokio must be owned only by the {CONFORMANCE_FEATURE} feature"
        )


def verify_default_dependency_dag(manifest_path):
    output = cargo_output(
        manifest_path,
        "tree",
        "-p",
        PACKAGE_NAME,
        "-e",
        "normal",
        "--depth",
        "1",
        "--prefix",
        "none",
        "--format",
        "{p}",
    )
    package_names = tuple(
        sorted(line.split(maxsplit=1)[0] for line in output.splitlines()[1:])
    )
    if package_names != REQUIRED_NORMAL_DEPENDENCIES:
        if "tokio" in package_names:
            fail("default feature graph must not enable Tokio")
        fail(
            "default normal dependency DAG must contain exactly: "
            + ", ".join(REQUIRED_NORMAL_DEPENDENCIES)
        )


def main():
    parser = argparse.ArgumentParser(
        description="Verify the novarocks-spi production dependency boundary."
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        required=True,
        help="Cargo manifest containing the novarocks-spi package.",
    )
    arguments = parser.parse_args()
    manifest_path = arguments.manifest_path.resolve()

    metadata = json.loads(
        cargo_output(
            manifest_path,
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
        )
    )
    verify_declared_boundary(package_from_metadata(metadata))
    verify_default_dependency_dag(manifest_path)
    print("novarocks-spi dependency boundary: PASS")


if __name__ == "__main__":
    main()
