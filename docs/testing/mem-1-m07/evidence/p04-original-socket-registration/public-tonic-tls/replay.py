#!/usr/bin/env python3
"""Replay actual public Tonic TLS parity offline after the workspace CI exits.

This runs two public API probes, not the two private cfg(tls) unit tests.
The static Bytes owner verifies facts, not original funding or allocator bounds.
No successful TLS handshake, Native outbound registration or complete connection
funding claim follows from these tests. The HTTP case uses a real H2 peer.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


def repository_root():
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "Cargo.toml").is_file() and (
            candidate / "vendor/tonic-0.12.3/src/lib.rs"
        ).is_file():
            return candidate
    raise RuntimeError("Cannot locate the production repository root")


def identity(package):
    return (
        package["name"], package["version"],
        package.get("source"), package.get("checksum"),
    )


def source_identity(repo, evidence):
    sources = [repo / "Cargo.lock", repo / "rust-toolchain.toml", evidence / "parity.rs",
               evidence / "Cargo.toml.in", evidence / "replay.py"]
    for vendor in ("tonic-0.12.3", "hyper-1.8.1", "h2-0.4.12",
                   "bytes-1.11.0", "http-1.4.0", "tokio-1.52.3"):
        base = repo / "vendor" / vendor
        sources.append(base / "Cargo.toml")
        sources.extend(sorted((base / "src").rglob("*.rs")))
    return {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sources}


def main():
    repo = repository_root()
    evidence = Path(__file__).resolve().parent
    toolchain = tomllib.loads((repo / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    if not isinstance(toolchain, str) or not toolchain:
        raise RuntimeError("Production toolchain channel is missing")
    scratch = Path(tempfile.mkdtemp(prefix="m07-public-tonic-tls-"))
    original = source_identity(repo, evidence)
    (scratch / "source-sha256.json").write_text(json.dumps(original, indent=2) + "\n")
    escaped_repo = str(repo).replace("\\", "\\\\").replace('"', '\\"')
    (scratch / "Cargo.toml").write_text(
        (evidence / "Cargo.toml.in").read_text().replace("__REPO__", escaped_repo))
    shutil.copy2(evidence / "parity.rs", scratch / "parity.rs")
    shutil.copy2(repo / "Cargo.lock", scratch / "Cargo.lock")
    env = os.environ.copy()
    overrides = {"CARGO_TARGET_DIR": str(repo / "target"),
                 "CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0",
                 "CARGO_NET_OFFLINE": "true", "RUSTUP_TOOLCHAIN": toolchain}
    env.update(overrides)
    receipt = {"scope": "public cfgTLS Endpoint parity; no original funding claim",
               "private_cfg_tls_unit_tests_executed": False,
               "toolchain": toolchain, "environment_overrides": overrides,
               "scratch": str(scratch), "commands": []}
    print(f"Scratch source and logs: {scratch}", flush=True)

    def run(arguments, label):
        log = scratch / (label + ".log")
        with log.open("wb") as output:
            result = subprocess.run(arguments, cwd=scratch, env=env,
                                    stdout=output, stderr=subprocess.STDOUT)
        receipt["commands"].append({"argv": arguments, "exit": result.returncode,
                                     "log": str(log)})
        (scratch / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(f"{label}: exit {result.returncode}; {log}", flush=True)
        if result.returncode:
            raise RuntimeError(f"{label} failed; no download or fallback is permitted")
        return log

    metadata_log = run(["cargo", "metadata", "--offline", "--format-version", "1",
                        "--manifest-path", str(scratch / "Cargo.toml")], "metadata")
    # Metadata reconciles only the scratch copy of the production lock. Refuse
    # any new source/version/checksum before compiling or making a test claim.
    expected = {identity(p) for p in tomllib.loads((repo / "Cargo.lock").read_text())["package"]}
    observed = [identity(p) for p in tomllib.loads((scratch / "Cargo.lock").read_text())["package"]
                if p["name"] != "m07-tonic-attempt-tls-public-parity"]
    unexpected = set(observed) - expected
    if unexpected:
        raise RuntimeError(f"Scratch lock drift: {sorted(unexpected, key=str)}")
    receipt["production_package_identities"] = len(observed)
    # cargo metadata stdout contains the exact path sources as well; its full
    # output is kept untrimmed in metadata.log (along with any Cargo warnings).
    receipt["metadata_log"] = str(metadata_log)
    run(["cargo", "test", "--offline", "--locked", "--manifest-path",
         str(scratch / "Cargo.toml"), "--test", "parity", "--",
         "--test-threads=1", "--nocapture"], "public-parity")
    if source_identity(repo, evidence) != original:
        raise RuntimeError("Production or probe source changed during replay")
    receipt["source_identity_unchanged"] = True
    receipt["status"] = "passed"
    (scratch / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"Public parity passed; receipt: {scratch / 'receipt.json'}", flush=True)


if __name__ == "__main__":
    main()
