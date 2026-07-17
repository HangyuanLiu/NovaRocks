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

source "$REPO_ROOT/tools/ci/lib/sql_suites.sh"

all="$(ci_discover_sql_suites "$REPO_ROOT")"
if grep -qx 'starrocks-compat' <<<"$all"; then
  echo "explicit-only starrocks-compat leaked into shell discovery" >&2
  exit 1
fi
ci_suite_exists "$REPO_ROOT" starrocks-compat
ci_suite_is_explicit_only "$REPO_ROOT" starrocks-compat
! ci_suite_is_explicit_only "$REPO_ROOT" filter
! grep -qx 'starrocks-compat' "$REPO_ROOT/tools/ci/suites/stable-sql-suites.txt"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

assert_metadata_error() {
  local repo_root="$1"
  local suite="$2"
  local status

  if ci_suite_is_explicit_only "$repo_root" "$suite" 2>"$repo_root/predicate.err"; then
    echo "malformed metadata was accepted for suite: $suite" >&2
    exit 1
  else
    status=$?
  fi
  if [ "$status" -eq 1 ]; then
    echo "malformed metadata was treated as explicit_only=false for suite: $suite" >&2
    exit 1
  fi
  if ci_discover_sql_suites "$repo_root" >/dev/null 2>"$repo_root/discovery.err"; then
    echo "shell discovery ignored malformed metadata for suite: $suite" >&2
    exit 1
  fi
}

duplicate_root="$tmpdir/duplicate"
mkdir -p "$duplicate_root/sql-tests/duplicate/sql"
printf '%s\n' \
  'explicit_only = true' \
  'explicit_only = false' \
  >"$duplicate_root/sql-tests/duplicate/suite.toml"
assert_metadata_error "$duplicate_root" duplicate

non_boolean_root="$tmpdir/non-boolean"
mkdir -p "$non_boolean_root/sql-tests/non-boolean/sql"
printf '%s\n' 'explicit_only = "true"' \
  >"$non_boolean_root/sql-tests/non-boolean/suite.toml"
assert_metadata_error "$non_boolean_root" non-boolean

missing_assignment_root="$tmpdir/missing-assignment"
mkdir -p "$missing_assignment_root/sql-tests/missing-assignment/sql"
printf '%s\n' 'server_mode = "native"' \
  >"$missing_assignment_root/sql-tests/missing-assignment/suite.toml"
assert_metadata_error "$missing_assignment_root" missing-assignment

missing_manifest_root="$tmpdir/missing-manifest"
mkdir -p "$missing_manifest_root/sql-tests/starrocks-compat/sql"
assert_metadata_error "$missing_manifest_root" starrocks-compat

echo "sql-suite-metadata-test: PASS"
