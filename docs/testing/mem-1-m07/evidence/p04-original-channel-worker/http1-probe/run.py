"""Recreate the scoped standalone HTTP1-only compile probe with local pinned sources."""
from pathlib import Path
import json, os, shutil, subprocess, tempfile, tomllib
repo = Path(__file__).resolve().parents[6]
work = Path(tempfile.mkdtemp(prefix="m07-hyper-http1-probe-"))
(work / "src").mkdir()
manifest = '[package]\nname="m07-hyper-http1-probe"\nversion="0.1.0"\nedition="2024"\n[workspace]\n[dependencies]\nhyper={path=' + json.dumps(str(repo / "vendor/hyper-1.8.1")) + ',default-features=false,features=["client","server","http1"]}\n[patch.crates-io]\n'
for name, config in tomllib.loads((repo / "Cargo.toml").read_text())["patch"]["crates-io"].items():
    manifest += name + '={path=' + json.dumps(str(repo / config["path"])) + '}\n'
(work / "Cargo.toml").write_text(manifest)
shutil.copyfile(repo / "Cargo.lock", work / "Cargo.lock")
shutil.copyfile(Path(__file__).parent / "src/main.rs", work / "src/main.rs")
subprocess.run(["cargo", "+1.92.0", "check", "--offline", "--manifest-path", str(work / "Cargo.toml"), "--target-dir", str(work / "target")], env=dict(os.environ, CARGO_BUILD_JOBS="4", CARGO_INCREMENTAL="0"), check=True)
prod = {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in tomllib.loads((repo / "Cargo.lock").read_text())["package"]}
probe = tomllib.loads((work / "Cargo.lock").read_text())["package"]
assert all(p["name"] == "m07-hyper-http1-probe" or (p["name"], p["version"], p.get("source"), p.get("checksum")) in prod for p in probe)
print("Scoped standalone HTTP1 probe and production lock identities passed:", work)
