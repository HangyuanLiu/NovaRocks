#!/usr/bin/env python3
"""Run the current pinned, complete normal HTTP/Bytes dependencies offline.

This is a new allocator-fix receipt; historical source receipts stay unchanged.
The optional Miri run exercises original extents and alias retirement without
real sockets, clocks, copied allocator algorithms or a whole product wallet.
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


PACKAGES = ("http-1.4.0", "bytes-1.11.0")


def source_hashes(repo):
    result = {}
    for package in PACKAGES:
        source = repo / "vendor" / package
        # Generated target files are build output, not the normal dependency
        # source snapshot. Copy every other vendor file without modification.
        paths = sorted(path for path in source.rglob("*.rs")
                       if "target" not in path.relative_to(source).parts)
        for path in paths + [source / "Cargo.toml"]:
            key = package + "/" + path.relative_to(source).as_posix()
            result[key] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def verify_identities(repo, lock):
    production = {
        (p["name"], p["version"], p.get("source"), p.get("checksum"))
        for p in tomllib.loads((repo / "Cargo.lock").read_text())["package"]
    }
    for package in tomllib.loads(lock.read_text())["package"]:
        if package["name"] == "m07-header-map-probe":
            continue
        identity = (package["name"], package["version"],
                    package.get("source"), package.get("checksum"))
        if identity not in production:
            raise SystemExit(f"Dependency is not production pinned: {identity}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    evidence = Path(__file__).resolve().parent
    repo = args.repo.resolve(strict=True)
    pins = json.loads((evidence / "vendor-source-sha256.json").read_text())
    expected = {key: value for key, value in pins.items()
                if key.split("/", 1)[0] in PACKAGES
                and "target" not in Path(key).parts}
    actual = source_hashes(repo)
    if actual != expected:
        raise SystemExit("Product source differs from the final vendor-source snapshot")
    historical = evidence.parent / "p04-status-field-arena"
    lock = historical / "probe-Cargo.lock"
    verify_identities(repo, lock)
    root = Path(tempfile.mkdtemp(prefix="m07-field-arena-lock-"))
    for package in PACKAGES:
        shutil.copytree(repo / "vendor" / package, root / package,
                        ignore=shutil.ignore_patterns("target", ".git", "__pycache__"))
    copied = {}
    for package in PACKAGES:
        source = root / package
        for path in sorted(source.rglob("*.rs")) + [source / "Cargo.toml"]:
            copied[package + "/" + path.relative_to(source).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if copied != actual:
        raise SystemExit("Scratch source copy differs from the exact verified product files")
    (root / "source-sha256.json").write_text(json.dumps(actual, indent=2) + "\n")
    (root / "src").mkdir()
    shutil.copyfile(evidence / "field-arena-probe-driver.rs", root / "src/lib.rs")
    shutil.copyfile(lock, root / "Cargo.lock")
    (root / "Cargo.toml").write_text('''[package]
name="m07-header-map-probe"
version="0.1.0"
edition="2021"
[workspace]
exclude=["http-1.4.0","bytes-1.11.0"]
[lib]
doctest=false
[dependencies]
http={path="http-1.4.0",version="=1.4.0"}
bytes="=1.11.0"
itoa="=1.0.15"
[patch.crates-io]
bytes={path="bytes-1.11.0"}
''')
    print(f"Probe workspace: {root}", flush=True)
    print("Actual field pool SHA256: " + actual["http-1.4.0/src/header/field_pool.rs"], flush=True)
    commands = [["cargo", "test", "--manifest-path", str(root / "Cargo.toml"),
                 "--offline", "--locked", "--lib"]]
    if args.miri:
        commands.append(["cargo", "+nightly", "miri", "test", "--manifest-path",
                         str(root / "Cargo.toml"), "--offline", "--locked", "--lib"])
    for index, command in enumerate(commands):
        print("Command: " + " ".join(command), flush=True)
        with (root / f"run-{index}.log").open("w") as log:
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, env=os.environ) as process:
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    print(line, end="", flush=True)
                code = process.wait()
        if code:
            raise SystemExit(code)


if __name__ == "__main__":
    main()
