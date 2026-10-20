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
source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
python3 - "$test_root" <<'FIXTURES'
import copy,json,sys
from pathlib import Path
root=Path(sys.argv[1])
def records(status='pass',code=0):
    return [
      {'kind':'run','schema':1,'mode':'verify','verification':{'verify_enabled':True,'record_from':'reference'},'platform':{'system':'fixture','machine':'fixture','release':'fixture'}},
      {'kind':'selection','suites':[{'suite':'s','inputs_sha256':'inputs','verify_enabled':True,'query_timeout_secs':60,'declaration':{'scope':'suite','initial_state':'isolated','env_inputs':[]},'cases':[{'case':'a'}]}]},
      {'kind':'case','suite':'s','case':'a','lifecycle_id':'L','status':status,'oracle':{'required':[{'step':1,'kind':'expected_error'}],'executed':[{'step':1,'kind':'expected_error','outcome':'pass','policy':{}}]},'failure':{'class':'error_code','error_code':'NR-FAIL','statement_index':1,'actual_digest':None} if status=='fail' else None},
      {'kind':'suite','suite':'s','lifecycle_id':'L','inputs_end_sha256':'inputs','inputs_stable':True,'executed_cases':['a'],'cleanup':{'status':'ok'}},
      {'kind':'server','cleanup':{'status':'ok'}},{'kind':'fixture','shutdown':{'status':'ok'},'resources':[]},
      {'kind':'end','completed':True,'exit_code':code}]
def put(name,rows):
    (root/(name+'.results.jsonl')).write_text(''.join(json.dumps(row)+'\n' for row in rows))
put('pass',records());put('fail',records('fail',1));put('truncated',records()[:-1])
violation=records();violation[4]['cleanup']['status']='failed';put('cleanup',violation)
empty={'schema':1,'known_failures':[],'quarantine':[]}
(root/'empty.json').write_text(json.dumps(empty))
known=copy.deepcopy(empty);known['known_failures']=[{'unit':'sql-case:s/a','failure':{'class':'error_code','code':'NR-FAIL'},'reason':'independent failing fixture','owner':'test owner','expires':'9999-12-31'}]
(root/'known.json').write_text(json.dumps(known));known['known_failures'][0]['expires']='2000-01-01';(root/'expired.json').write_text(json.dumps(known))
FIXTURES
ci_init_summary_state
check() {
  local file="$1" dispositions="$2" expected="$3" expected_code="$4" code=0
  DISPOSITIONS_FILE="$test_root/$dispositions.json"
  ci_classify_sql_results s "$test_root/$file.results.jsonl" || code=$?
  test "$CI_SQL_RESULT_STATUS" = "$expected"
  test "$code" -eq "$expected_code"
}
check pass empty PASS 0
ci_sql_native_exit_acceptable 0
if ci_sql_native_exit_acceptable 1; then echo 'unexplained invocation exit accepted' >&2; exit 1; fi
check fail known KNOWN_FAIL 0
ci_sql_native_exit_acceptable 1
if ci_sql_native_exit_acceptable 0; then echo 'contradictory invocation exit accepted' >&2; exit 1; fi
if ci_sql_native_exit_acceptable 143; then echo 'cancelled invocation accepted' >&2; exit 1; fi
check fail expired FAIL 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *EXPIRED_KNOWN_FAILURE* ]]
check fail empty FAIL 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *NEW_FAILURE* ]]
check pass known UNEXPECTED_PASS 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *STALE_KNOWN_FAILURE* ]]
check cleanup empty FAIL 1
check truncated empty FAIL 1

# Terminal text is deliberately false. Reclassification must use native records.
mkdir -p "$test_root/reclassify/sql"
cp "$test_root/pass.results.jsonl" "$test_root/reclassify/sql/s.results.jsonl"
printf 'fail=99\n    [s] a FAIL 1s\n' >"$test_root/reclassify/sql/s.log"
DISPOSITIONS_FILE="$test_root/empty.json"
CI_FROM_RUN_DIR="$test_root/reclassify"
raw_before="$(shasum -a 256 "$test_root/reclassify/sql/s.results.jsonl")"
reclassify_existing_run >"$test_root/reclassify.log"
grep -F '| s | PASS |' "$test_root/reclassify/summary.md" >/dev/null
test "$raw_before" = "$(shasum -a 256 "$test_root/reclassify/sql/s.results.jsonl")"

# The default tier delegates to native default discovery.
cat >"$test_root/sql-runner" <<'RUNNER'
#!/usr/bin/env bash
[ "$1" = --list-suites ] && [ "$2" = --suite ] && [ "$3" = all ] || exit 2
printf '%s\n' analytic decimal iceberg-mv-scheduler mv-rewrite runtime-filter-distributed sql-reject subquery
RUNNER
chmod +x "$test_root/sql-runner"
ci_bound_binary() { printf '%s\n' "$test_root/sql-runner"; }
defaults="$(ci_tier_suites full "$REPO_ROOT")"
for name in decimal iceberg-mv-scheduler mv-rewrite runtime-filter-distributed sql-reject subquery; do
  grep -Fx "$name" <<<"$defaults" >/dev/null
done

echo 'local-full-ci-classification-test: PASS'
