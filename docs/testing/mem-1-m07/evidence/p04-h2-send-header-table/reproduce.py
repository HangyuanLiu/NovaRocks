#!/usr/bin/env python3
"""Run actual-source outbound HPACK table probes offline in an isolated target.

Copies complete production HPACK algorithm source unchanged. The fixture/fuzz
module wiring is omitted (unrelated dev dependencies); frame::Error is extracted
verbatim from the production enum. Only probe tests are appended to encoder.rs.
No product edits, downloads, root Cargo operations or reconstructed algorithms.
"""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib


def prepare(repo, evidence):
    root = Path(tempfile.mkdtemp(prefix="m07-h2-send-header-table-"))
    (root / "src").mkdir()
    shutil.copytree(repo / "vendor/h2-0.4.12/src/hpack", root / "src/hpack")
    shutil.copyfile(repo / "vendor/h2-0.4.12/src/ext.rs", root / "src/ext.rs")
    module = root / "src/hpack/mod.rs"
    original = module.read_bytes()
    fixture_wiring = b"#[cfg(test)]\nmod test;\n\n"
    assert original.count(fixture_wiring) == 1
    module.write_bytes(original.replace(fixture_wiring, b""))
    frame = (repo / "vendor/h2-0.4.12/src/frame/mod.rs").read_text()
    start = frame.index("#[derive(Debug, Clone, PartialEq, Eq)]\npub enum Error {")
    end = frame.index("\n}", start) + 2
    (root / "src/frame.rs").write_text("use crate::hpack;\n" + frame[start:end] + "\n")
    (root / "src/lib.rs").write_text("#![allow(dead_code)]\nmod ext;\nmod frame;\nmod hpack;\n")
    encoder = root / "src/hpack/encoder.rs"
    encoder.write_bytes(encoder.read_bytes() + b"\n" + (evidence / "probe-tests.rs").read_bytes())
    source_hashes = {}
    for path in sorted((repo / "vendor/h2-0.4.12/src/hpack").rglob("*.rs")):
        source_hashes[str(path.relative_to(repo))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for relative in ["vendor/h2-0.4.12/src/ext.rs", "vendor/h2-0.4.12/src/frame/mod.rs"]:
        source_hashes[relative] = hashlib.sha256((repo / relative).read_bytes()).hexdigest()
    (root / "source-sha256.json").write_text(json.dumps(source_hashes, indent=2) + "\n")
    manifest = '[package]\nname="m07-send-header-table-probe"\nversion="0.1.0"\nedition="2021"\n[workspace]\n[lib]\ndoctest=false\n[dependencies]\nbytes="=1.11.0"\nfnv="=1.0.7"\nhttp="=1.4.0"\nitoa="=1.0.15"\ntracing={version="=0.1.43",default-features=false,features=["std"]}\ntracing-core={version="=0.1.35",default-features=false}\n'
    manifest += '[patch.crates-io]\nbytes={path=' + json.dumps(str(repo / "vendor/bytes-1.11.0")) + '}\n'
    (root / "Cargo.toml").write_text(manifest)
    lock = evidence / "probe-Cargo.lock"
    if lock.exists():
        shutil.copyfile(lock, root / "Cargo.lock")
    print(f"Probe workspace: {root}", flush=True)
    print("Actual encoder SHA256: " + source_hashes["vendor/h2-0.4.12/src/hpack/encoder.rs"], flush=True)
    return root, lock


def verify_identities(repo, lock):
    production = tomllib.loads((repo / "Cargo.lock").read_text())["package"]
    exact = {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in production}
    for package in tomllib.loads(lock.read_text())["package"]:
        if package["name"] == "m07-send-header-table-probe":
            continue
        identity = (package["name"], package["version"], package.get("source"), package.get("checksum"))
        if identity not in exact:
            raise SystemExit(f"Probe identity is not production-pinned: {identity}")


def run_logged(command, root, name, env):
    print("Command: " + " ".join(command), flush=True)
    result = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (root / (name + ".log")).write_text(result.stdout)
    print(result.stdout, end="", flush=True)
    print(f"{name} exit: {result.returncode}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--negative-local-clamp", action="store_true", help="Remove only peer-update clamping in the isolated source copy; require the real allocation/wire test to fail")
    parser.add_argument("--miri", action="store_true", help="Run only the seven new probes with an already-installed nightly Miri; never install components")
    parser.add_argument("--quality", action="store_true", help="Run isolated Clippy and formatting checks; report copied upstream formatting separately")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    root, lock = prepare(repo, evidence)
    if args.prepare_only:
        return
    if not lock.exists():
        raise SystemExit("Missing checked-in probe-Cargo.lock; refuse unlocked run")
    verify_identities(repo, lock)
    common = ["--manifest-path", str(root / "Cargo.toml"), "--target-dir", str(root / "target"), "--offline", "--locked"]
    env = dict(os.environ, CARGO_NET_OFFLINE="true")
    ordinary = ["cargo", "test", *common, "--", "--test-threads=1", "--nocapture"]
    if run_logged(ordinary, root, "ordinary", env).returncode != 0:
        raise SystemExit("Actual-source ordinary tests failed")
    if args.negative_local_clamp:
        encoder = root / "src/hpack/encoder.rs"
        original = encoder.read_bytes()
        guarded = b"self.queue_size_update(self.local_max_size.map_or(val, |local| local.min(val)));"
        assert original.count(guarded) == 1
        original_hash = hashlib.sha256(original).hexdigest()
        mutant = original.replace(guarded, b"self.queue_size_update(val);")
        (root / "omit-local-clamp.diff").write_text("".join(difflib.unified_diff(original.decode().splitlines(keepends=True), mutant.decode().splitlines(keepends=True), fromfile="actual-encoder.rs", tofile="isolated-omit-clamp.rs")))
        try:
            encoder.write_bytes(mutant)
            result = run_logged(["cargo", "test", *common, "huge_peer_cannot_allocate_zero_table", "--", "--test-threads=1", "--nocapture"], root, "omit-local-clamp", env)
            required = ["test result: FAILED. 0 passed; 1 failed;", "send_table_probe::huge_peer_cannot_allocate_zero_table_or_change_duplicate_sensitive_values", "assertion `left == right` failed", "right: 0"]
            actual = re.search(r"actual encode allocations=(\d+), requested=(\d+)B", result.stdout)
            if result.returncode != 101 or not all(marker in result.stdout for marker in required) or actual is None or int(actual[1]) == 0 or int(actual[2]) == 0:
                raise SystemExit("The isolated mutant must reach the actual runtime allocation assertion, not fail compilation")
        finally:
            encoder.write_bytes(original)
            restored_hash = hashlib.sha256(encoder.read_bytes()).hexdigest()
            assert restored_hash == original_hash
            (root / "restore-sha256.json").write_text(json.dumps({"before": original_hash, "restored": restored_hash, "byte_exact": encoder.read_bytes() == original}, indent=2) + "\n")
            print(f"Byte-exact isolated encoder restore SHA256: {restored_hash}", flush=True)
        if run_logged(ordinary, root, "restored-ordinary", env).returncode != 0:
            raise SystemExit("Restored actual-source ordinary tests failed")
    if args.quality:
        if run_logged(["cargo", "clippy", *common, "--all-targets"], root, "clippy", env).returncode != 0:
            raise SystemExit("Isolated Clippy failed")
        if run_logged(["rustfmt", "--edition", "2021", "--check", str(evidence / "probe-tests.rs")], root, "probe-fmt", env).returncode != 0:
            raise SystemExit("New probe formatting failed")
        # Keep copied algorithms byte-for-byte: do not auto-format upstream files.
        if run_logged(["cargo", "fmt", "--manifest-path", str(root / "Cargo.toml"), "--check"], root, "isolated-fmt", env).returncode != 0:
            raise SystemExit("Isolated actual-source formatting failed; no automatic rewrite attempted")
    if args.miri:
        installed = subprocess.run(["rustup", "component", "list", "--toolchain", "nightly", "--installed"], capture_output=True, text=True, check=True).stdout
        if not any(line.startswith("miri-") for line in installed.splitlines()):
            raise SystemExit("Nightly Miri is not installed; no installation attempted")
        if run_logged(["cargo", "+nightly", "miri", "test", *common, "send_table_probe::", "--", "--test-threads=1", "--nocapture"], root, "miri-seven", env).returncode != 0:
            raise SystemExit("Seven new actual-source Miri probes failed")


if __name__ == "__main__":
    main()
