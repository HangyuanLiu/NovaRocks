#!/usr/bin/env bash
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

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-datasketches-source.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fake_cargo="$tmpdir/cargo"
cat >"$fake_cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [ "${CARGO_NET_OFFLINE:-}" != "true" ]; then
  echo "mutation test must run Cargo offline" >&2
  exit 97
fi
if [ "$#" -ne 6 ] || [ "$1" != "metadata" ] || \
   [ "$2" != "--format-version" ] || [ "$3" != "1" ] || \
   [ "$4" != "--locked" ] || [ "$5" != "--manifest-path" ]; then
  printf 'unexpected Cargo arguments:' >&2
  printf ' %q' "$@" >&2
  printf '\n' >&2
  exit 98
fi
cat "$(dirname "$6")/metadata.json"
EOF
chmod +x "$fake_cargo"

python3 - "$tmpdir" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
registry = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "1" * 64


def create(name, nodes, lock_nodes, requirement="=0.5.0", table_requirement=False):
    case = root / name
    case.mkdir(parents=True)
    manifest = '[workspace]\nresolver = "2"\n'
    if requirement is not None:
        value = json.dumps(requirement)
        if table_requirement:
            value = f"{{ version = {value}, default-features = false }}"
        manifest += f"[workspace.dependencies]\ndatasketches = {value}\n"
    (case / "Cargo.toml").write_text(manifest)
    packages = []
    for index, (version, source, node_checksum) in enumerate(nodes):
        packages.append(
            {
                "name": "datasketches",
                "version": version,
                "source": source,
                "checksum": node_checksum,
                "id": f"{source or 'path'}#datasketches@{version}-{index}",
                "manifest_path": str(case / f"datasketches-{index}" / "Cargo.toml"),
            }
        )
    (case / "metadata.json").write_text(
        json.dumps(
            {
                "packages": packages,
                "workspace_root": str(case),
                "resolve": {"nodes": []},
            }
        )
    )

    lock = ["version = 4", ""]
    for version, source, lock_checksum in lock_nodes:
        lock.extend(["[[package]]", 'name = "datasketches"', f'version = "{version}"'])
        if source is not None:
            lock.append(f'source = "{source}"')
        if lock_checksum is not None:
            lock.append(f'checksum = "{lock_checksum}"')
        lock.append("")
    (case / "Cargo.lock").write_text("\n".join(lock))


canonical = [("0.5.0", registry, checksum)]
create("canonical", canonical, canonical)
create("table-requirement", canonical, canonical, table_requirement=True)
create("spaced-exact-requirement", canonical, canonical, requirement="= 0.5.0")
create("version-02", [("0.2.0", registry, "old")], [("0.2.0", registry, "old")])
create(
    "other-version",
    [("0.5.1", registry, "next")],
    [("0.5.1", registry, "next")],
)
# A prerelease resolved against a stable root requirement must remain rejected.
create(
    "retired-prerelease",
    [("0.5.0-rc.1", registry, "2" * 64)],
    [("0.5.0-rc.1", registry, "2" * 64)],
)
git_source = "git+https://example.invalid/datasketches-rust?rev=deadbeef#deadbeef"
create("git-source", [("0.5.0", git_source, None)], [("0.5.0", git_source, None)])
create("path-source", [("0.5.0", None, None)], [("0.5.0", None, None)])
create("bad-checksum", canonical, [("0.5.0", registry, "bad-checksum")])
create("missing-checksum", canonical, [("0.5.0", registry, None)])
create("bad-metadata-checksum", [("0.5.0", registry, "bad-checksum")], canonical)
create("metadata-without-checksum", [("0.5.0", registry, None)], canonical)
create(
    "dual-source",
    canonical + [("0.5.0", git_source, None)],
    canonical + [("0.5.0", git_source, None)],
)
create("dual-lock-identity", canonical, canonical + [("0.5.0", git_source, None)])
create("metadata-without-lock", canonical, [])
create("lock-without-metadata", [], canonical)
create("wrong-lock-version", canonical, [("0.5.1", registry, checksum)])
create("git-lock-source", canonical, [("0.5.0", git_source, checksum)])
create("no-resolved-package", [], [])

for name, requirement in (
    ("range-short", "0.5"),
    ("range-caret", "^0.5.0"),
    ("prerelease-requirement", "=0.5.0-rc.1"),
):
    create(name, canonical, canonical, requirement=requirement)
create("missing-root-requirement", canonical, canonical, requirement=None)

# These identities are synthetic test data. A coherent stable release change
# must not require editing the guard, and all participating graphs must agree.
next_release = [("0.5.1", registry, "3" * 64)]
create("coherent-release-change", next_release, next_release, requirement="=0.5.1")
create("matching-workspace-checksums", canonical, canonical)
create("matching-workspace-checksums/nested", canonical, canonical)
create("different-workspace-checksums", canonical, canonical)
different_checksum = [("0.5.0", registry, "4" * 64)]
create("different-workspace-checksums/nested", different_checksum, different_checksum)

# Discovery must not enter build output, disposable test directories, or
# third-party vendor workspaces.  Missing metadata makes accidental discovery
# fail closed through the fake Cargo executable.
for ignored in ("target/upstream", ".tmp-case/upstream", "vendor/upstream"):
    directory = root / "canonical" / ignored
    directory.mkdir(parents=True)
    (directory / "Cargo.toml").write_text("[workspace]\n")
PY

run_checker() {
  CARGO_NET_OFFLINE=true python3 "$CHECKER" \
    --repo-root "$1" \
    --cargo "$fake_cargo"
}

assert_rejected() {
  local name="$1"
  local expected="$2"
  local case_root="$tmpdir/$name"
  if run_checker "$case_root" >"$case_root/stdout" 2>"$case_root/stderr"; then
    echo "DataSketches source mutation was accepted: $name" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$case_root/stderr"; then
    echo "DataSketches source mutation produced the wrong diagnostic: $name" >&2
    cat "$case_root/stderr" >&2
    exit 1
  fi
}

run_checker "$tmpdir/canonical" | grep -Fq "DataSketches source: PASS"
run_checker "$tmpdir/table-requirement" | grep -Fq "DataSketches source: PASS"
run_checker "$tmpdir/spaced-exact-requirement" | grep -Fq "DataSketches source: PASS"
run_checker "$tmpdir/metadata-without-checksum" | grep -Fq "DataSketches source: PASS"
run_checker "$tmpdir/coherent-release-change" | grep -Fq "0.5.1, crates.io, checksum"
run_checker "$tmpdir/matching-workspace-checksums" | grep -Fq "2 canonical graphs"
assert_rejected version-02 "package version must match the root manifest requirement"
assert_rejected other-version "package version must match the root manifest requirement"
assert_rejected retired-prerelease "package version must match the root manifest requirement"
assert_rejected git-source "package source must be crates.io"
assert_rejected path-source "package source must be crates.io"
assert_rejected bad-checksum "metadata checksum must match Cargo.lock checksum"
assert_rejected missing-checksum "Cargo.lock checksum must be present"
assert_rejected bad-metadata-checksum "metadata checksum must match Cargo.lock checksum"
assert_rejected dual-source "must contain exactly one resolved datasketches package"
assert_rejected dual-lock-identity "must contain exactly one locked datasketches package"
assert_rejected metadata-without-lock "metadata/Cargo.lock disagree about datasketches"
assert_rejected lock-without-metadata "metadata/Cargo.lock disagree about datasketches"
assert_rejected wrong-lock-version "Cargo.lock version must match the root manifest requirement"
assert_rejected git-lock-source "Cargo.lock source must be crates.io"
assert_rejected no-resolved-package "no canonical datasketches"
assert_rejected range-short "must pin an exact stable release (=X.Y.Z)"
assert_rejected range-caret "must pin an exact stable release (=X.Y.Z)"
assert_rejected prerelease-requirement "must pin an exact stable release (=X.Y.Z)"
assert_rejected missing-root-requirement "root manifest must declare datasketches in [workspace.dependencies]"
assert_rejected different-workspace-checksums "workspaces disagree on the datasketches checksum"

echo "datasketches-source-test: PASS"
