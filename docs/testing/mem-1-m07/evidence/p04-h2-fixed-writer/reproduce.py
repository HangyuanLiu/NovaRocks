#!/usr/bin/env python3
"""Probe exact current send carrier source and the actual public h2 writer.

No source edits, downloads, toolchain installs or root Cargo/target access.
Pure-source probes alone run under Miri; the real h2 dependency writer oracle
uses System. Only Core/fixed writer Vec are proved, not HPACK, connection,
socket/task, ownership-carrier metadata, allocator caches or RSS.
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


def prepare(repo, evidence, kind):
    root = Path(tempfile.mkdtemp(prefix=f"m07-h2-fixed-writer-{kind}-"))
    (root / "src").mkdir()
    if kind == "core":
        source = (repo / "vendor/h2-0.4.12/src/send_frame_buffer.rs").read_bytes()
        (root / "src/lib.rs").write_bytes(b"#![allow(dead_code)]\n" + source + b"\n" + (evidence / "probe-tests.rs").read_bytes())
        manifest = '[package]\nname="m07-fixed-send-core-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\n[lib]\ndoctest=false\n[dependencies]\nbytes="=1.11.0"\n'
        lock_name = "probe-Cargo.lock"
    else:
        (root / "src/lib.rs").write_bytes((evidence / "writer-probe.rs").read_bytes())
        manifest = '[package]\nname="m07-fixed-send-writer-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\n[lib]\ndoctest=false\n[dependencies]\n'
        manifest += 'h2={path=' + json.dumps(str(repo / "vendor/h2-0.4.12")) + ',version="=0.4.12"}\n'
        manifest += 'bytes="=1.11.0"\ntokio={version="=1.52.3",default-features=false,features=["io-util"]}\n'
        # Pin the real h2 normal dependency identities to production. This does
        # not import the vendor package's dev-dependency graph or Tokio runtime.
        for name, version in {"atomic-waker": "1.1.2", "fnv": "1.0.7", "futures-core": "0.3.32", "futures-sink": "0.3.32", "http": "1.4.0", "indexmap": "2.12.1", "slab": "0.4.11", "pin-project-lite": "0.2.16", "itoa": "1.0.15", "tracing-core": "0.1.35"}.items():
            manifest += f'{name}={{version="={version}",default-features=false}}\n'
        manifest += 'tokio-util={version="=0.7.17",default-features=false,features=["codec","io"]}\ntracing={version="=0.1.43",default-features=false,features=["std"]}\n'
        lock_name = "writer-Cargo.lock"
    manifest += '[patch.crates-io]\nbytes={path=' + json.dumps(str(repo / "vendor/bytes-1.11.0")) + '}\n'
    (root / "Cargo.toml").write_text(manifest)
    lock = evidence / lock_name
    if lock.exists():
        shutil.copyfile(lock, root / "Cargo.lock")
    print(f"{kind} probe workspace: {root}", flush=True)
    return root, lock


def verify_identities(repo, lock):
    production = tomllib.loads((repo / "Cargo.lock").read_text())["package"]
    exact = {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in production}
    for package in tomllib.loads(lock.read_text())["package"]:
        if package["name"].startswith("m07-fixed-send-"):
            continue
        key = (package["name"], package["version"], package.get("source"), package.get("checksum"))
        if key not in exact:
            raise SystemExit(f"Probe identity is not production-pinned: {key}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--kind", choices=["core", "writer", "all"], default="all")
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    for relative in ["vendor/h2-0.4.12/src/send_frame_buffer.rs", "vendor/h2-0.4.12/src/codec/framed_write.rs", "vendor/h2-0.4.12/src/codec/mod.rs", "vendor/h2-0.4.12/src/client.rs"]:
        print(f"Source SHA256 {relative}: {hashlib.sha256((repo / relative).read_bytes()).hexdigest()}", flush=True)
    selected = ["core", "writer"] if args.kind == "all" else [args.kind]
    prepared = [(kind, *prepare(repo, evidence, kind)) for kind in selected]
    if args.prepare_only:
        return
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    for kind, root, lock in prepared:
        if not lock.exists():
            raise SystemExit(f"Missing checked-in {lock.name}; refuse an unlocked run")
        verify_identities(repo, lock)
        common = ["--manifest-path", str(root / "Cargo.toml"), "--target-dir", str(root / "target"), "--offline", "--locked"]
        subprocess.run(["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"], check=True, env=env)
        if args.miri and kind == "core":
            subprocess.run(["rustup", "run", "nightly", "rustc", "--version"], check=True, env=env)
            installed = subprocess.check_output(["rustup", "component", "list", "--toolchain", "nightly", "--installed"], text=True, env=env)
            if "miri-" not in installed or "rust-src" not in installed:
                raise SystemExit("Miri and rust-src must already be installed; this script installs nothing")
            subprocess.run(["rustup", "run", "nightly", "cargo", "miri", "test", *common, "--", "--test-threads=1", "--nocapture"], check=True, env=env)


if __name__ == "__main__":
    main()
