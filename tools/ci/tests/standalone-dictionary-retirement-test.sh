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

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for retired_path in \
  src/engine/dictionary \
  src/meta/repository/dictionary.rs \
  src/sql/common/dictionary.rs \
  src/meta/avro/schemas/dictionary.snapshot \
  src/meta/avro/schemas/dictionary.lookup; do
  if test -e "$REPO_ROOT/$retired_path"; then
    fail "retired standalone dictionary path still exists: $retired_path"
  fi
done

if rg -n \
  'DictionaryQueryProvider|QueryDictionaryProvider|with_dictionary_provider|dictionary_manager|rebuild_for_analyze_full|mark_target_stale|mark_starrocks_table_stale|dictionary\.(snapshot|lookup)' \
  "$REPO_ROOT/src" "$REPO_ROOT/tests" "$REPO_ROOT/sql-tests"; then
  fail "retired standalone dictionary symbols still exist"
fi

grep -q 'build_query_global_dict_map' "$REPO_ROOT/src/lower/compat/node/decode.rs" \
  || fail "FE-compatible global dictionary map builder must remain"
grep -q 'TPlanNodeType::DECODE_NODE' "$REPO_ROOT/src/lower/compat/node/mod.rs" \
  || fail "FE-compatible DECODE_NODE lowering must remain"
grep -q 'encode_batch_with_query_global_dicts' "$REPO_ROOT/src/exec/dict_encode.rs" \
  || fail "FE-compatible scan dictionary encoding must remain"
grep -q 'hydrate_dictionary_columns_except' "$REPO_ROOT/src/exec/chunk/hydrate.rs" \
  || fail "runtime Arrow dictionary carrier hydration must remain"

echo "PASS: standalone dictionary snapshots are retired and runtime dictionary paths remain"
