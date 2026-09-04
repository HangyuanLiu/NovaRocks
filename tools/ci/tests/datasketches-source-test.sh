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
checksum = "407f3fe0c32e6547cb8637b11a8a765ff027afa31e5f6f732b23f8d74672087b"


def create(name, nodes, lock_nodes):
    case = root / name
    case.mkdir()
    (case / "Cargo.toml").write_text("[workspace]\nresolver = \"2\"\n")
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


canonical = [("0.5.0-rc.1", registry, checksum)]
create("canonical", canonical, canonical)
create("version-02", [("0.2.0", registry, "old")], [("0.2.0", registry, "old")])
create(
    "other-version",
    [("0.5.0-rc.2", registry, "next")],
    [("0.5.0-rc.2", registry, "next")],
)
git_source = "git+https://example.invalid/datasketches-rust?rev=deadbeef#deadbeef"
create("git-source", [("0.5.0-rc.1", git_source, None)], [("0.5.0-rc.1", git_source, None)])
create("path-source", [("0.5.0-rc.1", None, None)], [("0.5.0-rc.1", None, None)])
create("bad-checksum", canonical, [("0.5.0-rc.1", registry, "bad-checksum")])
create(
    "dual-source",
    canonical + [("0.5.0-rc.1", git_source, None)],
    canonical + [("0.5.0-rc.1", git_source, None)],
)

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
assert_rejected version-02 "package version must be 0.5.0-rc.1"
assert_rejected other-version "package version must be 0.5.0-rc.1"
assert_rejected git-source "package source must be crates.io"
assert_rejected path-source "package source must be crates.io"
assert_rejected bad-checksum "Cargo.lock checksum must be"
assert_rejected dual-source "must contain exactly one resolved datasketches package"

echo "datasketches-source-test: PASS"
