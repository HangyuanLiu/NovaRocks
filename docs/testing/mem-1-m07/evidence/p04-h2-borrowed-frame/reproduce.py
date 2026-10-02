#!/usr/bin/env python3
"""Probe exact current borrowed fixed-frame input with locked normal dependencies.

System observes only raw Vec/Core and separately covered carrier allocations.
Successful borrowed reads/control callbacks must request zero output allocations.
HPACK, HeaderMap, retained DATA, IO/task metadata and allocator RSS are excluded.
The negative compile check proves a safe borrowed slice cannot escape this API.
No dependencies or toolchains are installed; all Cargo calls are offline/locked.
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
    production = tomllib.loads((repo / "Cargo.lock").read_text())["package"]
    identities = {identity(package) for package in production}
    locked = tomllib.loads((evidence / "probe-Cargo.lock").read_text())["package"]
    for package in locked:
        if package["name"] != "m07-h2-borrowed-frame-probe" and identity(package) not in identities:
            raise SystemExit(f"Probe dependency differs from production: {identity(package)}")
    target = Path(tempfile.mkdtemp(prefix="m07-h2-borrowed-frame-probe-"))
    (target / "src").mkdir()
    source = (repo / "vendor/h2-0.4.12/src/receive_frame.rs").read_bytes()
    lib = target / "src/lib.rs"
    complete = b"#![allow(dead_code)]\n" + source + b"\n" + (evidence / "probe-tests.rs").read_bytes()
    lib.write_bytes(complete)
    (target / "Cargo.toml").write_text(
        '[package]\nname="m07-h2-borrowed-frame-probe"\nversion="0.1.0"\nedition="2021"\n'
        '[workspace]\n[lib]\ndoctest=false\n[dependencies]\n'
        'bytes="=1.11.0"\n'
        'tokio={version="=1.52.3",default-features=false,features=["io-util"]}\n'
        'pin-project-lite="=0.2.16"\n[patch.crates-io]\n'
        + "bytes={path=" + json.dumps(str(repo / "vendor/bytes-1.11.0")) + "}\n"
    )
    shutil.copyfile(evidence / "probe-Cargo.lock", target / "Cargo.lock")
    print(f"Probe workspace: {target}", flush=True)
    print(f"Current receive_frame source SHA256: {hashlib.sha256(source).hexdigest()}", flush=True)
    print("Production-pinned tokio=1.52.3 io-util only; patched bytes=1.11.0; no runtime", flush=True)
    if args.prepare_only:
        return
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    common = ["--manifest-path", str(target / "Cargo.toml"), "--offline", "--locked"]
    subprocess.run(["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"], check=True, env=env)
    # Only scratch source is changed; this must fail specifically because the
    # callback's borrow cannot become the API's independent return type R.
    try:
        lib.write_bytes(complete + b"\nfn borrowed_slice_cannot_escape<T: AsyncRead + Unpin>(reader: &mut FixedFrameRead<T>, cx: &mut Context<'_>) {\n    let _ = reader.poll_frame_with(cx, |frame| frame);\n}\n")
        negative = subprocess.run(["cargo", "check", *common, "--lib"], env=env, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print("Safe borrowed-slice escape negative compile check:", flush=True)
        print(negative.stdout, flush=True)
        if negative.returncode == 0 or "lifetime may not live long enough" not in negative.stdout:
            raise SystemExit("Borrow escape did not fail with the expected lifetime diagnostic")
        print(f"Expected lifetime refusal exit={negative.returncode}", flush=True)
    finally:
        lib.write_bytes(complete)
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
