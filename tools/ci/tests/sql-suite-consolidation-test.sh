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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
STABLE_SUITES="$REPO_ROOT/tools/ci/suites/stable-sql-suites.txt"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for retired_suite in complex-type-native function-native; do
  if [ -d "$REPO_ROOT/sql-tests/$retired_suite" ]; then
    fail "retired SQL suite directory still exists: $retired_suite"
  fi
  if grep -qx "$retired_suite" "$STABLE_SUITES"; then
    fail "stable SQL suites must not include retired suite $retired_suite"
  fi
done

for stable_suite in complex-type function; do
  grep -qx "$stable_suite" "$STABLE_SUITES" \
    || fail "stable SQL suites must include $stable_suite"
done

complex_type_cases=(
  complex_binary_comparison
  complex_group_by
  complex_in_predicate
)
function_cases=(
  bitmap_hll_type_restrictions
  function_bitmap_binary
  function_bitmap_replace_if_not_null
  function_bitmap_sub_bitmap
  function_bitmap_to_array
  function_bitmap_to_string
  function_bitmap_unnest
  function_typeof
)

for case_name in "${complex_type_cases[@]}"; do
  test -f "$REPO_ROOT/sql-tests/complex-type/sql/$case_name.sql" \
    || fail "complex-type must own $case_name.sql"
done
for case_name in "${function_cases[@]}"; do
  test -f "$REPO_ROOT/sql-tests/function/sql/$case_name.sql" \
    || fail "function must own $case_name.sql"
done

echo "PASS: complex-type and function SQL suites have canonical ownership"
