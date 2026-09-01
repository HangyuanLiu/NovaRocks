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

discovered="$(ci_discover_sql_suites "$REPO_ROOT")"

grep -qx 'filter' <<<"$discovered"
if grep -Eq '^(ssb|tpc-h|tpc-ds)$' <<<"$discovered"; then
  echo "correctness discovery returned a benchmark workload" >&2
  exit 1
fi

ci_suite_exists "$REPO_ROOT" filter
if ci_suite_exists "$REPO_ROOT" ssb; then
  echo "benchmark workload was accepted as a correctness suite" >&2
  exit 1
fi
sql_root="$REPO_ROOT/tests/sql"
if [ -e "$sql_root/suites" ]; then
  echo "retired SQL suite root still exists" >&2
  exit 1
fi

echo "sql-correctness-discovery-test: PASS"
