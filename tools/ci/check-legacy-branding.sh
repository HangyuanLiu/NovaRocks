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

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
LEGACY_PATTERN='[sS][tT][aA][rR][uU][sS][tT]'
EXCLUDED_PATHS=(
  ':(exclude)baselines/**'
  ':(exclude)docs/workflow/archive/**'
)

if [ "$#" -gt 1 ] || ! git -C "$REPO_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "usage: tools/ci/check-legacy-branding.sh [repo-root]" >&2
  exit 2
fi

content_matches="$(git -C "$REPO_ROOT" grep -I -n -E "$LEGACY_PATTERN" -- . "${EXCLUDED_PATHS[@]}" || true)"
if [ -n "$content_matches" ]; then
  echo "error: active tracked content still uses the retired pre-NovaRocks brand:" >&2
  printf '%s\n' "$content_matches" >&2
  exit 1
fi

path_matches=()
while IFS= read -r path; do
  case "$path" in
    baselines/*|docs/workflow/archive/*)
      continue
      ;;
  esac
  if printf '%s\n' "$path" | grep -Eiq "$LEGACY_PATTERN"; then
    path_matches+=("$path")
  fi
done < <(git -C "$REPO_ROOT" ls-files)

if [ "${#path_matches[@]}" -gt 0 ]; then
  echo "error: active tracked path still uses the retired pre-NovaRocks brand:" >&2
  printf '%s\n' "${path_matches[@]}" >&2
  exit 1
fi

echo "legacy-branding: PASS"
