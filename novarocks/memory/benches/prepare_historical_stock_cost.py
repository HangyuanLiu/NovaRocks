#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Prepare an external-only benchmark package against the frozen historical checkout."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

BASELINE_SHA = "af9d0591676c75ede5fb53c2d64949e2d80667c5"
FROZEN_HASH = "cd3f06768bd6b757860b8c17c724a31469eb880ab2674cd608d7b13693a0d676"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-checkout", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()
    baseline = args.baseline_checkout.resolve(strict=True)
    sha = subprocess.check_output(["git", "-C", str(baseline), "rev-parse", "HEAD"], text=True).strip()
    if sha != BASELINE_SHA:
        parser.error("baseline checkout HEAD differs from frozen exact SHA")
    # The adapter lives outside both workspaces. No tracked or untracked Rust
    # core source edits may masquerade as an exact-source comparison.
    status = subprocess.check_output(["git", "-C", str(baseline), "status", "--porcelain", "--", "novarocks/memory/src", "novarocks/memory/Cargo.toml", "Cargo.toml"], text=True)
    if status:
        parser.error("baseline core or manifests have local changes")
    package = args.output.resolve()
    if package == baseline or baseline in package.parents:
        parser.error("adapter output must be outside the historical checkout")
    if package.exists() and any(package.iterdir()):
        parser.error("adapter output must be empty or absent")
    package.mkdir(parents=True, exist_ok=True)
    (package / "src").mkdir()
    bench = Path(__file__).resolve().parent
    manifest = (bench / "stock_cost_manifest.json").read_bytes()
    if hashlib.sha256(manifest).hexdigest() != FROZEN_HASH:
        parser.error("frozen manifest hash mismatch")
    template = (bench / "historical_stock_cost.rs.in").read_text()
    template_hash = hashlib.sha256(template.encode()).hexdigest()
    code = template.replace("@BASELINE_CHECKOUT@", json.dumps(str(baseline), ensure_ascii=False)).replace("@ADAPTER_HASH@", json.dumps(template_hash))
    (package / "src/main.rs").write_text(code)
    (package / "benches").mkdir()
    (package / "benches/stock_cost_manifest.json").write_bytes(manifest)
    # A standalone external workspace avoids modifying historical Cargo files.
    (package / "Cargo.toml").write_text('[package]\nname = "mem-m02a-historical-comparator"\nversion = "0.1.0"\nedition = "2024"\n\n[workspace]\n\n[dependencies]\nnovarocks-memory = { path = '+json.dumps(str(baseline / "novarocks/memory"), ensure_ascii=False)+' }\nserde_json = "1"\nsha2 = "0.10"\n\n[profile.release]\nopt-level = 3\nlto = "thin"\ncodegen-units = 1\n')
    receipt = {"kind": "external_historical_harness_preparation", "historical_sha": sha,
               "baseline_checkout": str(baseline), "external_package": str(package),
               "manifest_sha256": FROZEN_HASH, "template_sha256": template_hash,
               "materialized_adapter_sha256": hashlib.sha256(code.encode()).hexdigest(),
               "baseline_core_dirty": False, "matrix_complete": False, "formal_acceptance": False,
               "build_status": "not_requested", "binary": str(package / "target/release/mem-m02a-historical-comparator")}
    receipt_path = package / "preparation.json"
    receipt_path.write_text(json.dumps(receipt, indent=2)+"\n")
    if args.build:
        with (package / "build.log").open("w") as log:
            completed = subprocess.run(["cargo", "build", "--release", "--manifest-path", str(package / "Cargo.toml"), "--target-dir", str(package / "target")], stdout=log, stderr=subprocess.STDOUT, check=False)
        receipt["build_status"] = "passed" if completed.returncode == 0 else "failed"
        receipt["build_exit_code"] = completed.returncode
        receipt_path.write_text(json.dumps(receipt, indent=2)+"\n")
        print(json.dumps(receipt))
        return completed.returncode
    print(json.dumps(receipt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
