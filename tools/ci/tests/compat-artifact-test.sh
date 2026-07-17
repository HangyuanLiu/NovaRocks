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
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fake_builder="$tmpdir/fake-builder.sh"
cat >"$fake_builder" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

expected='cargo build --profile dev-opt --features compat --bin novarocks --bin starrocks-compat-probe'
if [ "$*" != "$expected" ]; then
  echo "unexpected compat build command: $*" >&2
  exit 1
fi
: "${CARGO_TARGET_DIR:?CARGO_TARGET_DIR must be set}"
: "${SCT_EXPECTED_TARGET_DIR:?SCT_EXPECTED_TARGET_DIR must be set}"
if [ "$CARGO_TARGET_DIR" != "$SCT_EXPECTED_TARGET_DIR" ]; then
  echo "unexpected CARGO_TARGET_DIR: $CARGO_TARGET_DIR" >&2
  exit 1
fi
mkdir -p "$CARGO_TARGET_DIR/dev-opt"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$CARGO_TARGET_DIR/dev-opt/novarocks"
chmod +x "$CARGO_TARGET_DIR/dev-opt/novarocks"
printf '%s\n' '#!/usr/bin/env bash' 'exit 0' >"$CARGO_TARGET_DIR/dev-opt/starrocks-compat-probe"
chmod +x "$CARGO_TARGET_DIR/dev-opt/starrocks-compat-probe"
EOF
chmod +x "$fake_builder"

output_dir="$tmpdir/output"
mkdir -p "$output_dir"
expected_target_dir="$(cd "$output_dir" && pwd -P)/target"
SCT_EXPECTED_TARGET_DIR="$expected_target_dir" SCT_COMPAT_BUILD_HOOK="$fake_builder" \
  "$REPO_ROOT/tools/ci/build-compat-artifact.sh" \
  --profile dev-opt \
  --output-dir "$output_dir"

manifest="$output_dir/manifest.txt"
test -f "$manifest"

actual_keys="$(cut -d= -f1 "$manifest")"
expected_keys="$(printf '%s\n' format binary sha256 git_head profile features)"
if [ "$actual_keys" != "$expected_keys" ]; then
  echo "compat manifest keys differ" >&2
  diff -u <(printf '%s\n' "$expected_keys") <(printf '%s\n' "$actual_keys") >&2 || true
  exit 1
fi

declare -A artifact=()
while IFS='=' read -r key value; do
  artifact["$key"]="$value"
done <"$manifest"

if [ "${artifact[format]}" != 'novarocks-compat-artifact-v1' ]; then
  echo "unexpected compat manifest format" >&2
  exit 1
fi
if [[ "${artifact[binary]}" != /* ]] || [ ! -x "${artifact[binary]}" ]; then
  echo "compat binary must be an absolute executable path" >&2
  exit 1
fi
expected_binary="$(cd "$output_dir/bin" && pwd -P)/novarocks-compat"
if [ "${artifact[binary]}" != "$expected_binary" ]; then
  echo "compat binary path differs: ${artifact[binary]}" >&2
  exit 1
fi
if [ ! -x "$output_dir/bin/starrocks-compat-probe" ]; then
  echo "compat probe must be installed next to the compat binary" >&2
  exit 1
fi
if [ "${artifact[profile]}" != dev-opt ] || [ "${artifact[features]}" != compat ]; then
  echo "compat build metadata differs" >&2
  exit 1
fi
if ! [[ "${artifact[sha256]}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "compat SHA-256 is not 64 lowercase hex" >&2
  exit 1
fi
if [ "${artifact[sha256]}" != "$(shasum -a 256 "${artifact[binary]}" | awk '{print $1}')" ]; then
  echo "compat SHA-256 does not match the copied binary" >&2
  exit 1
fi
current_head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
if ! [[ "${artifact[git_head]}" =~ ^[0-9a-f]{40}$ ]] \
  || [ "${artifact[git_head]}" != "$current_head" ]; then
  echo "compat git head does not match the current checkout" >&2
  exit 1
fi
if find "$output_dir" -maxdepth 1 -name 'manifest.txt.*' -print -quit | grep -q .; then
  echo "temporary manifest file was left behind" >&2
  exit 1
fi

echo "compat-artifact-test: PASS"
