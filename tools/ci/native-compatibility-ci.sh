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
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/lib/logging.sh"
source "$SCRIPT_DIR/lib/command.sh"
source "$SCRIPT_DIR/lib/system_scenarios.sh"

main() {
  local selectors=() code=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --only) [ "$#" -ge 2 ] && [ -n "$2" ] || { echo "error: --only requires an exact scenario" >&2; return 2; }; selectors+=("$2"); shift 2 ;;
      -h|--help) echo "usage: native-compatibility-ci.sh [--only native-compatibility/<exact-scenario>]"; return 0 ;;
      *) echo "error: unknown compatibility option: $1" >&2; return 2 ;;
    esac
  done
  cd "$REPO_ROOT"
  CI_RUN_DIR="$(mktemp -d "$REPO_ROOT/logs/native-compatibility-XXXXXXXX")"
  CI_SUMMARY="$CI_RUN_DIR/summary.md"
  ci_init_summary_state
  ci_set_repo_context "$REPO_ROOT" "$(git symbolic-ref --short -q HEAD || echo HEAD)" "$(git rev-parse HEAD)"
  NOVA_CI_ARTIFACT_RECEIPT="$CI_RUN_DIR/artifacts.json"
  export NOVA_CI_ARTIFACT_RECEIPT
  ci_prepare_main "$NOVA_CI_ARTIFACT_RECEIPT" || code=$?
  if [ "$code" -eq 0 ]; then
    local runner binary
    runner="$(ci_bound_binary novarocks-system-tests)" || code=$?
    binary="$(ci_bound_binary novarocks)" || code=$?
    if [ "$code" -eq 0 ]; then
      ci_select_system_scenarios "$runner" compatibility "$CI_RUN_DIR/selected.txt" "$CI_RUN_DIR/requirements.txt" "${selectors[@]}" || code=$?
    fi
    if [ "$code" -eq 0 ]; then
      ci_record_stage "compatibility scenario selection" "PASS" "0" "$CI_RUN_DIR/selected.txt"
      ci_prepare_system_scenario_binaries "$binary" dev "$CI_RUN_DIR/fixtures" "$(git rev-parse --short HEAD)-$$" \
        "$CI_RUN_DIR/variant-preparation.log" "$CI_RUN_DIR/requirements.txt" || code=$?
    fi
    if [ "$code" -eq 0 ]; then
      runner="$(ci_bound_binary novarocks-system-tests)" || code=$?
      if [ "$code" -eq 0 ]; then
        ci_capture_main_builds "$NOVA_CI_ARTIFACT_RECEIPT" "$CI_RUN_DIR/build-info/main" || code=$?
      fi
      if [ "$code" -eq 0 ]; then
        ci_run_system_scenarios "$runner" "$SYSTEM_SCENARIO_PRIMARY_BINARY" \
          "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" \
          "$SCRIPT_DIR/fixtures/system-scenarios-base.toml" "$CI_RUN_DIR/artifacts" \
          "${NOVA_CI_SYSTEM_CLUSTER_SIZE:-3}" "${NOVA_CI_SYSTEM_TIMEOUT_SECS:-900}" "$CI_RUN_DIR/selected.txt" || code=$?
      fi
    fi
  fi
  printf '%s\n' "$code" >"$CI_RUN_DIR/exit"
  if [ "$code" -eq 0 ]; then ci_render_summary PASS; else ci_render_summary FAIL; fi
  echo "compatibility result: $CI_SUMMARY"
  return "$code"
}
if [ "${1:-}" != --source-only ]; then
  mkdir -p "$REPO_ROOT/logs"
  main "$@"
fi
