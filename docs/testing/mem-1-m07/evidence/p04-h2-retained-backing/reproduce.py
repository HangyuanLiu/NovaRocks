#!/usr/bin/env python3
"""Reproduce actual pool allocation/exit probes in an isolated workspace."""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--miri", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    evidence = Path(__file__).resolve().parent
    source = repo / "vendor/h2-0.4.12/src/receive_pool.rs"
    patched_bytes = repo / "vendor/bytes-1.11.0"
    target = Path(tempfile.mkdtemp(prefix="m07-h2-pool-ownership-"))
    (target / "src").mkdir()
    (target / "src/lib.rs").write_text(
        source.read_text() + "\n\n" + (evidence / "probe-tests.rs").read_text()
    )
    (target / "Cargo.toml").write_text(
        '[package]\nname="m07-h2-pool-ownership-probe"\n'
        'version="0.1.0"\nedition="2021"\n[workspace]\n[dependencies]\n'
        + "bytes={path=" + json.dumps(str(patched_bytes)) + "}\n"
        + 'atomic-waker="=1.1.2"\n'
    )
    shutil.copyfile(evidence / "ownership-Cargo.lock", target / "Cargo.lock")
    print(f"Probe workspace: {target}", flush=True)
    common = [
        "--manifest-path", str(target / "Cargo.toml"), "--offline", "--locked",
        "--", "--test-threads=1",
    ]
    subprocess.run(["cargo", "test", *common], check=True)
    if args.miri:
        subprocess.run(["cargo", "+nightly", "miri", "test", *common], check=True)


if __name__ == "__main__":
    main()
