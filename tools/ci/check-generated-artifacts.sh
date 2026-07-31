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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FORBIDDEN_ROOTS=(.superpowers logs reports)
MODE="tracked"

usage() {
  cat <<'EOF'
Usage: tools/ci/check-generated-artifacts.sh [--staged]

Reject generated plans, logs, and reports under .superpowers/, logs/, and
reports/. Use --staged for a pre-commit check; without it, validate the
repository index and ignore rules.
EOF
}

if [ "$#" -gt 1 ]; then
  usage >&2
  exit 2
fi

if [ "$#" -eq 1 ]; then
  if [ "$1" != "--staged" ]; then
    usage >&2
    exit 2
  fi
  MODE="staged"
fi

if [ "$MODE" = "staged" ]; then
  offending_paths="$(git -C "$REPO_ROOT" diff --cached --name-only --diff-filter=ACMR -- "${FORBIDDEN_ROOTS[@]}")"
else
  offending_paths="$(git -C "$REPO_ROOT" ls-files -- "${FORBIDDEN_ROOTS[@]}")"
fi

if [ -n "$offending_paths" ]; then
  echo "error: generated artifacts must not be $MODE in the repository:" >&2
  printf '%s\n' "$offending_paths" >&2
  echo "Move durable design material to the project documentation root and keep raw output outside the repository." >&2
  exit 1
fi

if [ "$MODE" = "tracked" ]; then
  for probe in \
    .superpowers/.artifact-hygiene-probe \
    logs/.artifact-hygiene-probe \
    reports/.artifact-hygiene-probe; do
    if ! git -C "$REPO_ROOT" check-ignore --quiet --no-index "$probe"; then
      echo "error: generated artifact path is not ignored: $probe" >&2
      exit 1
    fi
  done
fi

echo "generated-artifact-hygiene: PASS ($MODE)"
