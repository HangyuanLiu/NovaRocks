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
CHECKER="$REPO_ROOT/tools/ci/check-legacy-branding.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fixture="$tmpdir/repo"
mkdir -p "$fixture/tools/ci"
cp "$CHECKER" "$fixture/tools/ci/check-legacy-branding.sh"
chmod +x "$fixture/tools/ci/check-legacy-branding.sh"
git -C "$fixture" init --quiet
git -C "$fixture" add tools/ci/check-legacy-branding.sh

assert_rejected() {
  if "$fixture/tools/ci/check-legacy-branding.sh" "$fixture" >"$tmpdir/stdout" 2>"$tmpdir/stderr"; then
    echo "legacy-branding guard accepted a forbidden fixture" >&2
    exit 1
  fi
}

"$fixture/tools/ci/check-legacy-branding.sh" "$fixture" >/dev/null

printf '%s\n' "sta""rust" >"$fixture/content.txt"
git -C "$fixture" add content.txt
assert_rejected
git -C "$fixture" rm --quiet --force content.txt

printf '%s\n' "STA""RUST" >"$fixture/case.txt"
git -C "$fixture" add case.txt
assert_rejected
git -C "$fixture" rm --quiet --force case.txt

printf '%s\n' 'path fixture' >"$fixture/sta""rust-path.txt"
git -C "$fixture" add .
assert_rejected
git -C "$fixture" rm --quiet --force "sta""rust-path.txt"

mkdir -p "$fixture/baselines" "$fixture/docs/workflow/archive"
printf '%s\n' "sta""rust" >"$fixture/baselines/historical.txt"
printf '%s\n' "sta""rust" >"$fixture/docs/workflow/archive/historical.md"
git -C "$fixture" add baselines docs
"$fixture/tools/ci/check-legacy-branding.sh" "$fixture" >/dev/null

echo "legacy-branding-test: PASS"
