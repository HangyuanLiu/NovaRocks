#!/usr/bin/env python3
"""Probe the exact current SendHeaderBlockPool and shared pool source.

The ledger covers fixed block/slot/Core allocations and checked-out wrappers.
It excludes HeaderMap, HPACK tables/codec, writer, carrier metadata and RSS.
All dependencies are cached, production-pinned, offline and locked. No tools
or dependencies are installed. Miri is optional and must already be installed.
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


def identity(package):
    return tuple(package.get(key) for key in ("name", "version", "source", "checksum"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    production = tomllib.loads((repo / "Cargo.lock").read_text())
    probe = tomllib.loads((evidence / "probe-Cargo.lock").read_text())
    identities = {identity(package) for package in production["package"]}
    for package in probe["package"]:
        if package["name"] != "m07-h2-send-header-block-probe" and identity(package) not in identities:
            raise SystemExit(f"Probe dependency differs from production: {identity(package)}")
    target = Path(tempfile.mkdtemp(prefix="m07-h2-send-header-block-probe-"))
    (target / "src").mkdir()
    sources = ("receive_pool.rs", "send_header_block.rs")
    for name in sources:
        source = (repo / "vendor/h2-0.4.12/src" / name).read_bytes()
        (target / "src" / name).write_bytes(source)
        print(f"Current {name} SHA256: {hashlib.sha256(source).hexdigest()}", flush=True)
    (target / "src/lib.rs").write_bytes(
        b"#![allow(dead_code)]\nmod receive_pool;\npub use receive_pool::ReceiveBufferPool;\n"
        b"mod send_header_block;\npub use send_header_block::SendHeaderBlockPool;\n"
        + (evidence / "probe-tests.rs").read_bytes()
    )
    (target / "Cargo.toml").write_text(
        '[package]\nname="m07-h2-send-header-block-probe"\nversion="0.1.0"\nedition="2021"\n'
        '[workspace]\n[dependencies]\n'
        + "bytes={path=" + json.dumps(str(repo / "vendor/bytes-1.11.0")) + "}\n"
        + 'atomic-waker="=1.1.2"\n'
    )
    shutil.copyfile(evidence / "probe-Cargo.lock", target / "Cargo.lock")
    print(f"Probe workspace: {target}", flush=True)
    if args.prepare_only:
        return
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    common = ["--manifest-path", str(target / "Cargo.toml"), "--offline", "--locked"]
    subprocess.run(["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"], check=True, env=env)
    if args.miri:
        subprocess.run(["rustup", "run", "nightly", "rustc", "--version"], check=True, env=env)
        components = subprocess.check_output(
            ["rustup", "component", "list", "--toolchain", "nightly", "--installed"], text=True, env=env)
        if "miri-" not in components or "rust-src" not in components:
            raise SystemExit("Miri and rust-src must already be installed")
        subprocess.run(["rustup", "run", "nightly", "cargo", "miri", "test", *common,
                        "--", "--test-threads=1", "--nocapture"], check=True, env=env)


if __name__ == "__main__":
    main()
