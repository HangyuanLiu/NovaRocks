#!/usr/bin/env python3
"""Probe the actual shared HPACK decoder in a complete scratch normal dependency.

No algorithm replacement, upstream cfg(test) fixtures, product edits, network,
tool installation or workspace Cargo. Helpers and mutants exist only in scratch.
"""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


def prepare(repo, evidence):
    root = Path(tempfile.mkdtemp(prefix="m07-h2-borrowed-hpack-"))
    (root / "src").mkdir()
    scratch = root / "h2-source"
    shutil.copytree(repo / "vendor/h2-0.4.12", scratch)
    source_hashes = {}
    for source in sorted((scratch / "src").rglob("*.rs")):
        source_hashes[source.relative_to(scratch).as_posix()] = hashlib.sha256(source.read_bytes()).hexdigest()
    source_hashes["Cargo.toml"] = hashlib.sha256((scratch / "Cargo.toml").read_bytes()).hexdigest()
    (root / "source-sha256.json").write_text(json.dumps(source_hashes, indent=2) + "\n")
    shutil.copytree(repo / "vendor/bytes-1.11.0", root / "bytes-source")
    additions = {
        "src/hpack/decoder.rs": (evidence / "decoder-helper.rs").read_bytes(),
        "src/hpack/mod.rs": b"#[doc(hidden)]\npub use self::decoder::{M07HpackDecoder, M07HpackField, M07HpackError};\n",
        "src/lib.rs": b"#[doc(hidden)]\npub use crate::hpack::{M07HpackDecoder, M07HpackField, M07HpackError};\n",
    }
    for relative, addition in additions.items():
        path = scratch / relative
        original = path.read_bytes()
        changed = original + b"\n" + addition
        path.write_bytes(changed)
        (root / (relative.replace("/", "-") + ".helper.diff")).write_text("".join(difflib.unified_diff(original.decode().splitlines(True), changed.decode().splitlines(True), fromfile=relative, tofile=relative)))
    (root / "src/lib.rs").write_bytes((evidence / "driver.rs").read_bytes())
    manifest = '[package]\nname="m07-borrowed-hpack-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\nexclude=["h2-source"]\n[lib]\ndoctest=false\n[dependencies]\n'
    manifest += 'h2={path=' + json.dumps(str(scratch)) + ',version="=0.4.12"}\n'
    manifest += 'bytes="=1.11.0"\ntokio={version="=1.52.3",default-features=false,features=["io-util"]}\n'
    for name, version in {"atomic-waker": "1.1.2", "fnv": "1.0.7", "futures-core": "0.3.32", "futures-sink": "0.3.32", "http": "1.4.0", "indexmap": "2.12.1", "slab": "0.4.11", "pin-project-lite": "0.2.16", "itoa": "1.0.15", "tracing-core": "0.1.35"}.items():
        manifest += f'{name}={{version="={version}",default-features=false}}\n'
    manifest += 'tokio-util={version="=0.7.17",default-features=false,features=["codec","io"]}\ntracing={version="=0.1.43",default-features=false,features=["std"]}\n'
    manifest += '[patch.crates-io]\nbytes={path=' + json.dumps(str(root / "bytes-source")) + '}\n'
    (root / "Cargo.toml").write_text(manifest)
    shutil.copyfile(evidence / "probe-Cargo.lock", root / "Cargo.lock")
    print(f"Probe workspace: {root}", flush=True)
    print("Actual decoder SHA256: " + source_hashes["src/hpack/decoder.rs"], flush=True)
    return root, scratch


def verify_identities(repo, lock):
    production = tomllib.loads((repo / "Cargo.lock").read_text())["package"]
    exact = {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in production}
    for package in tomllib.loads(lock.read_text())["package"]:
        if package["name"] == "m07-borrowed-hpack-probe":
            continue
        identity = (package["name"], package["version"], package.get("source"), package.get("checksum"))
        if identity not in exact:
            raise SystemExit(f"Probe identity is not production-pinned: {identity}")


def run_logged(command, root, name, env):
    print("Command: " + " ".join(command), flush=True)
    lines = []
    with (root / (name + ".log")).open("w") as log:
        with subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
            for line in process.stdout:
                lines.append(line)
                log.write(line)
                log.flush()
                print(line, end="", flush=True)
            status = process.wait()
    print(f"{name} exit: {status}", flush=True)
    return status, "".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--reuse", type=Path, help="Reuse an already-prepared source snapshot while shared product sources are undergoing coordinated mutation")
    parser.add_argument("--negative-commit", action="store_true")
    parser.add_argument("--quality", action="store_true")
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    if args.reuse:
        root = args.reuse.resolve(strict=True)
        scratch = root / "h2-source"
        print(f"Reused source snapshot: {root}", flush=True)
    else:
        root, scratch = prepare(repo, evidence)
    verify_identities(repo, root / "Cargo.lock")
    if args.prepare_only:
        return
    common = ["--manifest-path", str(root / "Cargo.toml"), "--target-dir", str(root / "target"), "--offline", "--locked"]
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    ordinary = ["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"]
    if run_logged(ordinary, root, "ordinary", env)[0] != 0:
        raise SystemExit("Actual decoder tests failed")
    if args.negative_commit:
        path = scratch / "src/hpack/decoder.rs"
        original = path.read_bytes()
        mutants = [
            ("advance-commits-incomplete", b"        self.cursor.advance(cnt);", b"        self.cursor.advance(cnt);\n        self.committed = self.cursor.position() as usize;", "huffman_name_complete_value_need_more_retries_whole_literal", "incomplete literal must not commit name or advanced value cursor"),
            ("commit-not-updated", b"        self.committed = self.position() as usize;", b"        // Scratch negative: successful representation not committed.", "every_byte_split_matches_owned_output_table_and_commit", "complete decoder must commit entire full input"),
        ]
        for name, needle, replacement, test, oracle in mutants:
            assert original.count(needle) == 1
            mutant = original.replace(needle, replacement)
            (root / (name + ".diff")).write_text("".join(difflib.unified_diff(original.decode().splitlines(True), mutant.decode().splitlines(True), fromfile="actual-decoder-plus-helper.rs", tofile=name + ".rs")))
            try:
                path.write_bytes(mutant)
                status, output = run_logged(["cargo", "test", *common, test, "--", "--test-threads=1", "--nocapture"], root, name, env)
                assert status == 101 and "test result: FAILED. 0 passed; 1 failed;" in output and "panicked at" in output and oracle in output
            finally:
                path.write_bytes(original); assert path.read_bytes() == original
        before = hashlib.sha256(original).hexdigest()
        (root / "restore-sha256.json").write_text(json.dumps({"before": before, "restored": hashlib.sha256(path.read_bytes()).hexdigest(), "byte_exact": path.read_bytes() == original}, indent=2) + "\n")
        if run_logged(ordinary, root, "restored-ordinary", env)[0] != 0:
            raise SystemExit("Restored actual decoder tests failed")
    if args.quality:
        if run_logged(["cargo", "clippy", *common, "--all-targets"], root, "clippy", env)[0] != 0:
            raise SystemExit("Isolated decoder Clippy failed")
        if run_logged(["rustfmt", "--edition", "2021", "--check", *[str(evidence / name) for name in ["decoder-helper.rs", "driver.rs"]]], root, "helper-fmt", env)[0] != 0:
            raise SystemExit("New helper/driver formatting failed")
        if run_logged(["cargo", "fmt", "--manifest-path", str(root / "Cargo.toml"), "--check"], root, "driver-fmt", env)[0] != 0:
            raise SystemExit("Isolated driver formatting failed")
    if args.miri:
        installed = subprocess.check_output(["rustup", "component", "list", "--toolchain", "nightly", "--installed"], text=True)
        if not any(line.startswith("miri-") for line in installed.splitlines()):
            raise SystemExit("Nightly Miri is not installed; no installation attempted")
        if run_logged(["cargo", "+nightly", "miri", "test", *common, "--", "--test-threads=1", "--nocapture"], root, "miri-private-driver", env)[0] != 0:
            raise SystemExit("Private actual decoder Miri tests failed")


if __name__ == "__main__":
    main()
