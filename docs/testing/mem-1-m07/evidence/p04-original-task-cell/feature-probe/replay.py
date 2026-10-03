#!/usr/bin/env python3
"""Offline actual Tokio current/multi/unstable-tracing API feature probes.

Marker lifecycle facts only: no allocator, original authority, complete task or
Native per-connection funding claim. Run after the parent Cargo session exits.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

PACKAGE = "m07-original-task-cell-feature-probe"
MATRIX = (
    ("A-current-owner", "owner", ""),
    ("B-multi-owner", "owner,multi", ""),
    ("C-unstable-tracing-refusal", "owner,trace", "--cfg tokio_unstable"),
)


def repository_root():
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "Cargo.toml").is_file() and (
            candidate / "vendor/tokio-1.52.3/src/runtime/handle.rs"
        ).is_file():
            return candidate
    raise RuntimeError("Cannot locate the production repository root")


def identity(package):
    return (package["name"], package["version"],
            package.get("source"), package.get("checksum"))


def source_identity(repo, evidence):
    sources = [repo / "Cargo.toml", repo / "Cargo.lock", repo / "rust-toolchain.toml",
               evidence / "features.rs", evidence / "Cargo.toml.in", evidence / "replay.py"]
    for vendor in ("tokio-1.52.3", "bytes-1.11.0"):
        base = repo / "vendor" / vendor
        sources.append(base / "Cargo.toml")
        sources.extend(sorted((base / "src").rglob("*.rs")))
    return {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sources}


def main():
    repo = repository_root()
    evidence = Path(__file__).resolve().parent
    toolchain = tomllib.loads((repo / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    if not isinstance(toolchain, str) or not toolchain:
        raise RuntimeError("Production toolchain channel is missing")
    scratch = Path(tempfile.mkdtemp(prefix="m07-task-cell-feature-probe-"))
    original = source_identity(repo, evidence)
    (scratch / "source-sha256.json").write_text(json.dumps(original, indent=2) + "\n")
    escaped_repo = str(repo).replace("\\", "\\\\").replace('"', '\\"')
    (scratch / "Cargo.toml").write_text(
        (evidence / "Cargo.toml.in").read_text().replace("__REPO__", escaped_repo))
    shutil.copy2(evidence / "features.rs", scratch / "features.rs")
    expected = {identity(p) for p in tomllib.loads((repo / "Cargo.lock").read_text())["package"]}
    receipt = {"scope": "actual Tokio task API feature behavior",
               "privateScopeFactsNoFunding": True,
               "toolchain": toolchain, "scratch": str(scratch), "matrix": []}
    print(f"Scratch source and logs: {scratch}", flush=True)

    def save():
        (scratch / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")

    for label, features, rustflags in MATRIX:
        # Each feature configuration independently reconciles an exact copy of
        # the current production lock. Do not rely on the previous row's graph.
        shutil.copy2(repo / "Cargo.lock", scratch / "Cargo.lock")
        env = os.environ.copy()
        removed = []
        for name in ("CARGO_ENCODED_RUSTFLAGS", "CARGO_ENCODED_RUSTDOCFLAGS", "RUSTDOCFLAGS"):
            if name in env:
                removed.append(name)
                del env[name]
        overrides = {"CARGO_TARGET_DIR": str(repo / "target"),
                     "CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0",
                     "CARGO_NET_OFFLINE": "true", "RUSTUP_TOOLCHAIN": toolchain,
                     "RUSTFLAGS": rustflags}
        env.update(overrides)
        row = {"label": label, "features": features,
               "environment_overrides": overrides,
               "removed_environment_keys": removed, "commands": []}
        receipt["matrix"].append(row)
        save()

        def run(arguments, stage):
            log = scratch / f"{label}-{stage}.log"
            with log.open("wb") as output:
                result = subprocess.run(arguments, cwd=scratch, env=env,
                                        stdout=output, stderr=subprocess.STDOUT)
            row["commands"].append({"argv": arguments, "exit": result.returncode,
                                    "log": str(log)})
            save()
            print(f"{label} {stage}: exit {result.returncode}; {log}", flush=True)
            if result.returncode:
                raise RuntimeError(f"{label} {stage} failed; no download or fallback is permitted")

        common = ["--manifest-path", str(scratch / "Cargo.toml"),
                  "--no-default-features", "--features", features]
        run(["cargo", "metadata", "--offline", "--format-version", "1"] + common, "metadata")
        observed = [identity(p) for p in tomllib.loads((scratch / "Cargo.lock").read_text())["package"]
                    if p["name"] != PACKAGE]
        unexpected = set(observed) - expected
        if unexpected:
            raise RuntimeError(f"Scratch lock drift: {sorted(unexpected, key=str)}")
        row["production_package_identities"] = len(observed)
        shutil.copy2(scratch / "Cargo.lock", scratch / f"{label}-Cargo.lock")
        save()
        run(["cargo", "test", "--offline", "--locked"] + common +
            ["--test", "features", "--", "--test-threads=1", "--nocapture"], "tests")
        if source_identity(repo, evidence) != original:
            raise RuntimeError("Production or probe source changed during replay")
        row["source_identity_unchanged"] = True
        row["status"] = "passed"
        save()
    receipt["status"] = "passed"
    save()
    print(f"Actual Tokio feature matrix passed; receipt: {scratch / 'receipt.json'}", flush=True)


if __name__ == "__main__":
    main()
