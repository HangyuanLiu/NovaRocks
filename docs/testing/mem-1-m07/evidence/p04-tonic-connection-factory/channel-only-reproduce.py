#!/usr/bin/env python3
"""Reproduce the vendored Tonic channel-only check without a workspace feature request.

Use --prepare-lock once to seed the private lock from the production lock.
All resolved versions, registry sources, and checksums must match production.
No dependency is downloaded and no production manifest or lock is written.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import tomllib


PROBE = "m07-tonic-channel-only-probe"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_lock(candidate, production):
    current = tomllib.loads(candidate.read_text())["package"]
    expected = tomllib.loads(production.read_text())["package"]
    identities = {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in expected}
    for package in current:
        if package["name"] == PROBE:
            assert package["version"] == "0.0.0" and "source" not in package
            continue
        identity = (package["name"], package["version"], package.get("source"), package.get("checksum"))
        if identity not in identities:
            raise RuntimeError(f"Dependency differs from production lock: {identity!r}")
    return len(current) - 1


def verify_upstream(root):
    vendor = root / "vendor/tonic-0.12.3"
    pin = json.loads((vendor / "UPSTREAM.json").read_text())
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    sources = list((cargo_home / "registry/src").glob("*/tonic-0.12.3"))
    crates = list((cargo_home / "registry/cache").glob("*/tonic-0.12.3.crate"))
    assert len(sources) == len(crates) == 1, "Expected one exact cached upstream package"
    assert digest(crates[0]) == pin["registry_checksum"]
    with tarfile.open(crates[0]) as archive:
        packaged = {entry.name.removeprefix("tonic-0.12.3/"): entry
                    for entry in archive.getmembers() if entry.isfile()}
        assert set(packaged) - {".cargo_vcs_info.json"} == set(pin["source_files"])
        for name, original in pin["source_files"].items():
            assert hashlib.sha256(archive.extractfile(packaged[name]).read()).hexdigest() == original
    changed = []
    for name, original in pin["source_files"].items():
        assert digest(sources[0] / name) == original, f"Upstream source hash mismatch: {name}"
        if digest(vendor / name) != original:
            changed.append(name)
    return {"original_files": len(pin["source_files"]), "crate_checksum": pin["registry_checksum"],
            "modified_originals": changed}


def run(command, log, env):
    # Never overwrite the first run or a failure receipt during reproduction.
    original = log
    attempt = 1
    while log.exists():
        log = original.with_name(f"{original.stem}-rerun{attempt}{original.suffix}")
        attempt += 1
    with log.open("w") as output:
        output.write("$ " + " ".join(command) + "\n")
        output.flush()
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, env=env, check=False)
    print(f"exit={result.returncode} log={log} sha256={digest(log)}", flush=True)
    if result.returncode:
        raise RuntimeError(f"Command failed; preserve first failure at {log}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-lock", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[5]
    private = Path(__file__).with_name("channel-only-Cargo.lock")
    production = root / "Cargo.lock"
    root_hash = digest(production)
    provenance = verify_upstream(root)
    print(json.dumps(provenance, indent=2), flush=True)
    env = dict(os.environ)
    env["CARGO_TARGET_DIR"] = "/tmp/m07-tonic-channel-only-target"
    with tempfile.TemporaryDirectory(prefix="m07-tonic-channel-only-") as scratch:
        work = Path(scratch)
        (work / "src").mkdir()
        path_literal = lambda p: json.dumps(str(p))
        manifest = (
            '[package]\nname = "' + PROBE + '"\nversion = "0.0.0"\nedition = "2021"\n'
            '\n[dependencies]\ntonic = { path = ' + path_literal(root / "vendor/tonic-0.12.3")
            + ', default-features = false, features = ["channel"] }\n\n[patch.crates-io]\n'
        )
        for name, version in [("bytes", "1.11.0"), ("h2", "0.4.12"), ("hyper", "1.8.1")]:
            manifest += name + ' = { path = ' + path_literal(root / f"vendor/{name}-{version}") + ' }\n'
        (work / "Cargo.toml").write_text(manifest)
        (work / "src/lib.rs").write_text(
            'pub fn configured_endpoint() -> tonic::transport::Endpoint {\n'
            '    tonic::transport::Endpoint::from_static("http://127.0.0.1:1")\n'
            '        .http2_connection_factory(|| Ok::<_, std::io::Error>(\n'
            '            tonic::transport::Http2ConnectionConfig::default()))\n}\n'
        )
        lock = work / "Cargo.lock"
        lock.write_bytes(production.read_bytes() if args.prepare_lock else private.read_bytes())
        if args.prepare_lock:
            run(["cargo", "metadata", "--manifest-path", str(work / "Cargo.toml"),
                 "--offline", "--format-version", "1"], Path("/tmp/m07-tonic-channel-only-metadata.log"), env)
            count = verify_lock(lock, production)
            private.write_bytes(lock.read_bytes())
        else:
            count = verify_lock(lock, production)
        print(f"verified {count} dependency identities against production before compilation", flush=True)
        common = ["--manifest-path", str(work / "Cargo.toml"), "--offline", "--locked"]
        run(["cargo", "check", *common], Path("/tmp/m07-tonic-channel-only-check.log"), env)
        run(["cargo", "clippy", *common, "--", "-D", "warnings"],
            Path("/tmp/m07-tonic-channel-only-clippy.log"), env)
        run(["cargo", "clippy", *common, "--package", "tonic", "--lib", "--", "-D", "warnings"],
            Path("/tmp/m07-tonic-channel-only-clippy-lib.log"), env)
        assert lock.read_bytes() == private.read_bytes(), "Private lock changed during locked checks"
    assert digest(production) == root_hash, "Production lock changed during reproduction"
    print(f"private_lock_sha256={digest(private)}", flush=True)


if __name__ == "__main__":
    main()
