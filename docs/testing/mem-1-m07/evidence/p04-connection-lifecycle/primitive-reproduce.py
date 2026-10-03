#!/usr/bin/env python3
"""Run exact-current lifecycle primitive System/Miri probes in a private crate.

No root Cargo/target, downloads, toolchain installs, or source substitutions.
This verifies Core/observer requested backing only, not outer IO/task graphs.
"""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
import json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--miri", action="store_true")
    parser.add_argument("--clippy", action="store_true")
    parser.add_argument("--mutation", choices=["owner-before-observer", "bind-not-once", "commit-before-callback", "retained-early-release"])
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    source = repo / "vendor/h2-0.4.12/src/connection_lifecycle.rs"
    print(f"Source SHA256: {hashlib.sha256(source.read_bytes()).hexdigest()}", flush=True)
    root = Path(tempfile.mkdtemp(prefix="m07-lifecycle-primitive-"))
    (root / "src").mkdir()
    copied = source.read_text()
    selected_test = None
    if args.mutation:
        if args.mutation == "owner-before-observer":
            before, after = "            drop(observer);\n            drop(acquisition_owner);\n            drop(owner);", "            drop(owner);\n            drop(observer);\n            drop(acquisition_owner);"
            selected_test = "actual_core_and_observer_layouts_exit_before_original_owner"
        elif args.mutation == "bind-not-once":
            before, after = "compare_exchange(UNBOUND, BOUND, Ordering::AcqRel, Ordering::Acquire)", "compare_exchange(UNBOUND, UNBOUND, Ordering::AcqRel, Ordering::Acquire)"
            selected_test = "once_binding_race_returns_exactly_one_noncloneable_lease"
        elif args.mutation == "retained-early-release":
            before, after = "if previous == UNBOUND {", "if previous == UNBOUND || previous == BOUND {"
            selected_test = "bound_retirement_keeps_acquisition_until_actual_lease_drop"
        else:
            before = "            from,\n            transient,\n            Ordering::AcqRel,"
            after = "            from,\n            completed,\n            Ordering::AcqRel,"
            selected_test = "blocked_failing_initial_callback_cannot_publish_acquisition"
        if copied.count(before) != 1:
            raise SystemExit("Mutation anchor changed; refuse an inexact scratch mutation")
        copied = copied.replace(before, after, 1)
        if args.mutation == "commit-before-callback":
            before_commit = "compare_exchange(transient, completed, Ordering::AcqRel, Ordering::Acquire)"
            after_commit = "compare_exchange(completed, completed, Ordering::AcqRel, Ordering::Acquire)"
            if copied.count(before_commit) != 1:
                raise SystemExit("Mutation commit anchor changed")
            copied = copied.replace(before_commit, after_commit, 1)
        print(f"SCRATCH-ONLY mutation: {args.mutation}", flush=True)
    (root / "src/lib.rs").write_bytes(copied.encode() + b"\n" + (evidence / "primitive-tests.rs").read_bytes())
    (root / "Cargo.toml").write_text('[package]\nname="m07-connection-lifecycle-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\n[lib]\ndoctest=false\n[dependencies]\nbytes="=1.11.0"\n[patch.crates-io]\nbytes={path=' + json.dumps(str(repo / "vendor/bytes-1.11.0")) + '}\n')
    lock = evidence / "primitive-Cargo.lock"
    shutil.copyfile(lock, root / "Cargo.lock")
    production = {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in tomllib.loads((repo / "Cargo.lock").read_text())["package"]}
    for package in tomllib.loads(lock.read_text())["package"]:
        if package["name"] == "m07-connection-lifecycle-probe":
            continue
        key = (package["name"], package["version"], package.get("source"), package.get("checksum"))
        if key not in production:
            raise SystemExit(f"Unpinned dependency: {key}")
    print(f"Exact-source private crate: {root}", flush=True)
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    common = ["--manifest-path", str(root / "Cargo.toml"), "--target-dir", str(root / "target"), "--offline", "--locked"]
    subprocess.run(["cargo", "test", *common, *([selected_test] if selected_test else []), "--", "--test-threads=1", "--nocapture"], check=True, env=env)
    if args.clippy:
        subprocess.run(["cargo", "clippy", *common, "--all-targets", "--", "-D", "warnings"], check=True, env=env)
    if args.miri:
        subprocess.run(["rustup", "run", "nightly", "rustc", "--version"], check=True, env=env)
        installed = subprocess.check_output(["rustup", "component", "list", "--toolchain", "nightly", "--installed"], text=True, env=env)
        if "miri-" not in installed or "rust-src" not in installed:
            raise SystemExit("Existing Miri/rust-src required; no installation permitted")
        subprocess.run(["rustup", "run", "nightly", "cargo", "miri", "test", *common, "--", "--test-threads=1", "--nocapture"], check=True, env=env)


if __name__ == "__main__":
    main()
