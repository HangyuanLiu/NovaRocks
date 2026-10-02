#!/usr/bin/env python3
"""Probe the current unmodified fixed HTTP/2 raw input source with locked dependencies.

No toolchain installation, network access, product/source edits or workspace
Cargo changes. The raw grant covers only Vec/Core; frame copies, ownership
carrier metadata, IO/task metadata and physical allocator/RSS remain separate.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    source = repo / "vendor/h2-0.4.12/src/receive_frame.rs"
    lock_packages = tomllib.loads((repo / "Cargo.lock").read_text())["package"]
    expected = {"tokio": "1.52.3", "bytes": "1.11.0", "pin-project-lite": "0.2.16"}
    for name, version in expected.items():
        versions = {package["version"] for package in lock_packages if package["name"] == name}
        if versions != {version}:
            raise SystemExit(f"Production lock changed: {name} expected {version}, found {versions}")
    target = Path(tempfile.mkdtemp(prefix="m07-h2-raw-input-probe-"))
    (target / "src").mkdir()
    source_bytes = source.read_bytes()
    # Exact copied production implementation; tests are a separate appended module.
    (target / "src/lib.rs").write_bytes(
        b"#![allow(dead_code)]\n" + source_bytes
        + b"\n\n" + (evidence / "probe-tests.rs").read_bytes()
    )
    (target / "Cargo.toml").write_text(
        '[package]\nname="m07-h2-raw-input-probe"\nversion="0.1.0"\nedition="2021"\n'
        '[workspace]\n[lib]\ndoctest=false\n[dependencies]\n'
        'bytes="=1.11.0"\n'
        'tokio={version="=1.52.3",default-features=false,features=["io-util"]}\n'
        'pin-project-lite="=0.2.16"\n[patch.crates-io]\n'
        + "bytes={path=" + json.dumps(str(repo / "vendor/bytes-1.11.0")) + "}\n"
    )
    lock = evidence / "probe-Cargo.lock"
    if lock.exists():
        shutil.copyfile(lock, target / "Cargo.lock")
    print(f"Probe workspace: {target}", flush=True)
    print(f"Current source SHA256: {hashlib.sha256(source_bytes).hexdigest()}", flush=True)
    print("Production-pinned tokio=1.52.3 io-util only; patched bytes=1.11.0; no runtime", flush=True)
    if args.prepare_only:
        return
    if not lock.exists():
        raise SystemExit("Missing checked-in probe-Cargo.lock; prepare-only cannot execute tests")
    common = ["--manifest-path", str(target / "Cargo.toml"), "--offline", "--locked"]
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    subprocess.run(["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"],
                   check=True, env=env)
    if args.miri:
        # rustup run refuses a missing toolchain instead of installing it.
        subprocess.run(["rustup", "run", "nightly", "rustc", "--version"], check=True, env=env)
        components = subprocess.check_output(
            ["rustup", "component", "list", "--toolchain", "nightly", "--installed"],
            text=True, env=env)
        if "miri-" not in components or "rust-src" not in components:
            raise SystemExit("Miri and rust-src must already be installed; this script installs nothing")
        subprocess.run(["rustup", "run", "nightly", "cargo", "miri", "test", *common, "--",
                        "--test-threads=1", "--nocapture"], check=True, env=env)


if __name__ == "__main__":
    main()
