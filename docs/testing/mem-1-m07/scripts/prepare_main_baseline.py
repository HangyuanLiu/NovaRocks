#!/usr/bin/env python3
"""Install measurement-only probes after building a clean old-main server."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def prepare(source, baseline, expected_revision, output):
    source, baseline = source.resolve(strict=True), baseline.resolve(strict=True)
    if source == baseline or git(baseline, "rev-parse", "HEAD") != expected_revision:
        raise ValueError("baseline must be a separate checkout at the frozen revision")
    if not git(baseline, "branch", "--show-current").startswith("codex/mem-1-m07-"):
        raise ValueError("baseline probes require a task-specific local branch")
    if git(baseline, "status", "--porcelain=v1"):
        raise ValueError("baseline must be clean before probe installation")
    binary = baseline / "target/release/novarocks"
    if not binary.is_file():
        raise ValueError("build the clean release server before installing test probes")
    prefix = Path("tests/system-test-runner/src")
    copied = (
        prefix / "actors/mysql_stream.rs",
        prefix / "scenarios/result_delivery_baseline.rs",
    )
    inputs = {str(path): (source / path).read_bytes() for path in copied}
    registry = baseline / prefix / "scenarios/mod.rs"
    content = registry.read_text()
    module_anchor = "mod query_output;\n"
    registration_anchor = "    scenarios.extend(uea1_performance::scenarios());\n"
    if content.count(module_anchor) != 1 or content.count(registration_anchor) != 1:
        raise ValueError("old-main registry differs from the reviewed probe entry points")
    if "result_delivery_baseline" in content:
        raise ValueError("baseline already has result measurement probes")
    manifest = baseline / "tests/system-test-runner/Cargo.toml"
    if "sha2.workspace = true" not in manifest.read_text():
        raise ValueError("measurement probes must not introduce baseline dependencies")
    # Retain the immutable server identity before any test-only source changes.
    receipt = {
        "schema_version": 1,
        "server_revision": expected_revision,
        "server_source_clean_at_build_and_probe_install": True,
        "server_binary": str(binary),
        "server_sha256": digest(binary),
        "build_command": "CARGO_INCREMENTAL=0 cargo build --release --locked -p novarocks-server",
        "probe_source_revision": git(source, "rev-parse", "HEAD"),
        "probe_files_sha256": {
            str(path): hashlib.sha256(data).hexdigest() for path, data in inputs.items()
        },
        "scope": "test observer and scenario only; old native harness and server unchanged",
        "measurement_status": "NOT_RUN",
    }
    for path, data in inputs.items():
        (baseline / path).write_bytes(data)
    registry.write_text(content.replace(
        module_anchor, module_anchor + "mod result_delivery_baseline;\n"
    ).replace(
        registration_anchor,
        registration_anchor + "    scenarios.extend(result_delivery_baseline::scenarios());\n",
    ))
    receipt["probe_diff_sha256"] = hashlib.sha256(
        subprocess.check_output(["git", "-C", str(baseline), "diff", "--binary"])
    ).hexdigest()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"server_revision": expected_revision, "server_sha256": receipt["server_sha256"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source, args.baseline, args.expected_revision, args.output)
