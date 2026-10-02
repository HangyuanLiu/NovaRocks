#!/usr/bin/env python3
"""Run the current shared GOAWAY/DATA pool's physical-allocation and lease probes.

Six retained-backing probes are reused unchanged; three diagnostic-admission
probes exercise the new helper. Real h2/Hyper protocol checks live separately
in native_h2_goaway_backing.rs. This script installs and downloads nothing.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    source = repo / "vendor/h2-0.4.12/src/receive_pool.rs"
    target = Path(tempfile.mkdtemp(prefix="m07-h2-goaway-pool-probe-"))
    (target / "src").mkdir()
    source_bytes = source.read_bytes()
    (target / "src/lib.rs").write_bytes(
        b"#![allow(dead_code)]\n" + source_bytes
        + b"\n\n" + (evidence / "probe-tests.rs").read_bytes()
    )
    (target / "Cargo.toml").write_text(
        '[package]\nname="m07-h2-goaway-pool-probe"\nversion="0.1.0"\nedition="2021"\n'
        '[workspace]\n[dependencies]\n'
        + "bytes={path=" + json.dumps(str(repo / "vendor/bytes-1.11.0")) + "}\n"
        + 'atomic-waker="=1.1.2"\n'
    )
    shutil.copyfile(evidence / "probe-Cargo.lock", target / "Cargo.lock")
    print(f"Probe workspace: {target}", flush=True)
    print(f"Current receive_pool source SHA256: {hashlib.sha256(source_bytes).hexdigest()}", flush=True)
    if args.prepare_only:
        return
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    common = ["--manifest-path", str(target / "Cargo.toml"), "--offline", "--locked"]
    subprocess.run(["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"],
                   check=True, env=env)
    if args.miri:
        # Refuse missing toolchains/components instead of fetching or installing.
        subprocess.run(["rustup", "run", "nightly", "rustc", "--version"], check=True, env=env)
        components = subprocess.check_output(
            ["rustup", "component", "list", "--toolchain", "nightly", "--installed"],
            text=True, env=env)
        if "miri-" not in components or "rust-src" not in components:
            raise SystemExit("Miri and rust-src must already be installed")
        subprocess.run(["rustup", "run", "nightly", "cargo", "miri", "test", *common,
                        "--", "--test-threads=1", "--nocapture"], check=True, env=env)


if __name__ == "__main__":
    main()
