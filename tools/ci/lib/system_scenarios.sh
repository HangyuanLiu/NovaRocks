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

# Native 1FE+NBE System Scenario stage.
#
# This file selects, invokes and reports. It deliberately owns no cluster
# behaviour: configuration rendering, process spawn, readiness, topology
# barriers, restart, fault injection, artifact retention and cleanup all belong
# to `novarocks-cluster-harness`, reached through the
# `novarocks-system-test-runner` frontend. Do not reimplement any of that here.

# Discover the currently registered scenarios. The registry is the source of
# truth; the stage never hardcodes scenario names or an expected count.
ci_system_scenario_list() {
  local runner="$1"

  "$runner" --list
}

# Run every registered scenario serially, one `--only` invocation each, and
# record an independent summary row per scenario.
#
# Returns non-zero on the first failing scenario. The caller decides how to fail
# the run; this function never exits the shell itself.
ci_run_system_scenarios() {
  local runner="$1"
  local binary="$2"
  local base_config="$3"
  local artifact_root="$4"
  local cluster_size="$5"
  local timeout_secs="$6"

  local scenarios=()
  local scenario
  local list_log="$CI_RUN_DIR/system/list.log"

  mkdir -p "$CI_RUN_DIR/system" "$artifact_root"

  if ! ci_system_scenario_list "$runner" >"$list_log" 2>&1; then
    ci_record_system_scenario "<registry>" "FAIL" "0" "$list_log" "-"
    ci_mark_failure_tail "system scenario discovery failed" "$list_log"
    return 1
  fi

  while IFS= read -r scenario; do
    [ -n "$scenario" ] || continue
    scenarios+=("$scenario")
  done <"$list_log"

  if [ "${#scenarios[@]}" -eq 0 ]; then
    ci_record_system_scenario "<registry>" "FAIL" "0" "$list_log" "-"
    ci_mark_failure_tail "system scenario registry is empty" "$list_log"
    return 1
  fi

  local slug
  local log_path
  local artifact_dir
  local start
  local code
  local duration

  for scenario in "${scenarios[@]}"; do
    slug="${scenario//\//-}"
    log_path="$CI_RUN_DIR/system/$slug.log"
    artifact_dir="$artifact_root/$slug"
    mkdir -p "$artifact_dir"

    start="$(ci_epoch)"
    ci_run_logged "$log_path" \
      "$runner" \
      --binary "$binary" \
      --config "$base_config" \
      --artifact-root "$artifact_dir" \
      --cluster-size "$cluster_size" \
      --timeout-secs "$timeout_secs" \
      --only "$scenario"
    code=$?
    duration=$(($(ci_epoch) - start))

    if [ "$code" -ne 0 ]; then
      ci_record_system_scenario "$scenario" "FAIL" "$duration" "$log_path" "$artifact_dir"
      # The runner already printed the action history, process diagnostics and
      # an exact rerun command; preserve that tail verbatim rather than
      # rebuilding a second, weaker failure report here.
      ci_mark_failure_tail "system scenario $scenario failed" "$log_path"
      return "$code"
    fi

    ci_record_system_scenario "$scenario" "PASS" "$duration" "$log_path" "$artifact_dir"
    ci_render_summary "RUNNING"
  done

  return 0
}
