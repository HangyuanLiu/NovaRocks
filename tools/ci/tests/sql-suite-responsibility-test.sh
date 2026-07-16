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
AGGREGATE="$REPO_ROOT/sql-tests/aggregate"
STATISTICS="$REPO_ROOT/sql-tests/statistics"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

grep -qx 'aggregate' "$STABLE_SUITES" \
  || fail "stable SQL suites must include aggregate"
grep -qx 'statistics' "$STABLE_SUITES" \
  || fail "stable SQL suites must include statistics"
for retired_suite in aggregate-native statistics-native low-cardinality-native; do
  if grep -qx "$retired_suite" "$STABLE_SUITES"; then
    fail "stable SQL suites must not include retired suite $retired_suite"
  fi
done

test -f "$AGGREGATE/sql/compressed_key.sql" \
  || fail "aggregate must own compressed_key.sql"
test -f "$AGGREGATE/sql/compressed_key2.sql" \
  || fail "aggregate must own compressed_key2.sql"
if rg -n 'ANALYZE|@result_(not_)?contains=(min-max stats|DECODE)|EXPLAIN' \
  "$AGGREGATE/sql/compressed_key.sql" \
  "$AGGREGATE/sql/compressed_key2.sql"; then
  fail "aggregate compressed-key cases must not carry statistics or retired low-cardinality plan assertions"
fi

STATS_CASE="$STATISTICS/sql/largeint_minmax.sql"
test -f "$STATS_CASE" || fail "statistics must preserve focused LARGEINT min-max coverage"
grep -q 'LARGEINT' "$STATS_CASE" \
  || fail "statistics case must cover LARGEINT"
grep -q 'ANALYZE TABLE' "$STATS_CASE" \
  || fail "statistics case must collect statistics explicitly"
grep -q '@result_contains=min-max stats' "$STATS_CASE" \
  || fail "statistics case must assert min-max scan statistics"

echo "PASS: SQL suite responsibilities are separated"
