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
CHECKER="$REPO_ROOT/tools/ci/check-generated-artifacts.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fixture="$tmpdir/repo"
mkdir -p "$fixture/tools/ci"
cp "$CHECKER" "$fixture/tools/ci/check-generated-artifacts.sh"
chmod +x "$fixture/tools/ci/check-generated-artifacts.sh"
cat >"$fixture/.gitignore" <<'EOF'
.superpowers/
/logs/
/reports/
EOF

git -C "$fixture" init --quiet
git -C "$fixture" add .gitignore tools/ci/check-generated-artifacts.sh
"$fixture/tools/ci/check-generated-artifacts.sh" >/dev/null

mkdir -p "$fixture/reports"
printf 'generated\n' >"$fixture/reports/probe.md"
git -C "$fixture" add --force reports/probe.md

if "$fixture/tools/ci/check-generated-artifacts.sh" --staged >"$tmpdir/stdout" 2>"$tmpdir/stderr"; then
  echo "forced generated artifact was accepted by the staged guard" >&2
  exit 1
fi
grep -q 'reports/probe.md' "$tmpdir/stderr"

echo "generated-artifact-hygiene-test: PASS"
