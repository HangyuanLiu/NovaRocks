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

CI_INVOCATION_LIBRARY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$CI_INVOCATION_LIBRARY_DIR/build_capabilities.sh"

# Persist the invocation before launching, retaining the wrapped command's exit.
ci_invocation_start() {
  local kind="$1" name="$2"; shift 2
  local spec_args=() unit_args=()
  [ "$kind" = sql-suite ] || unit_args=(--unit "$kind:$name")
  [ -z "${CI_INVOCATION_SPEC:-}" ] || spec_args=(--spec "$CI_INVOCATION_SPEC")
  python3 "$CI_INVOCATION_LIBRARY_DIR/invocation_record.py" start \
    --root "$CI_RUN_DIR" --repo "$REPO_ROOT" --kind "$kind" --name "$name" \
    "${unit_args[@]}" "${spec_args[@]}" -- "$@"
}

ci_invocation_finish() {
  local record="$1" code="$2" log="$3" materials="$4"
  python3 "$CI_INVOCATION_LIBRARY_DIR/invocation_record.py" finish \
    --record "$record" --code "$code" --log "$log" --materials "$materials" >/dev/null
}

ci_run_recorded_logged() {
  local log="$1" kind="$2" name="$3" materials="$4"; shift 4
  local record code=0
  record="$(ci_invocation_start "$kind" "$name" "$@")" || return $?
  ci_run_logged "$log" "$@" || code=$?
  ci_invocation_finish "$record" "$code" "$log" "$materials" || return $?
  return "$code"
}

# Native 1FE+NBE System Scenario stage.
#
# This file selects, invokes and reports. It deliberately owns no cluster
# behaviour: configuration rendering, process spawn, readiness, topology
# barriers, restart, fault injection, artifact retention and cleanup all belong
# to `novarocks-cluster-harness`, reached through the
# `novarocks-system-test-runner` frontend. Do not reimplement any of that here.

# Discover the currently registered default scenarios. The registry is the source of
# truth; the stage never hardcodes scenario names or an expected count.
ci_system_scenario_list() {
  local runner="$1"

  "$runner" --list-default
}

# Resolve the complete selected lifecycle before building any variant.
ci_select_system_scenarios() {
  local runner="$1" scope="$2" selection="$3" requirements="$4"; shift 4
  local args=() selector
  if [ "$scope" = compatibility ]; then
    args=(--list-compatibility)
    for selector in "$@"; do args+=(--only "$selector"); done
  else
    args=(--list-default)
  fi
  "$runner" "${args[@]}" >"$selection" || return $?
  [ -s "$selection" ] || { echo "error: scenario selection is empty" >&2; return 2; }
  args=(--list-binary-requirements)
  while IFS= read -r selector; do args+=(--only "$selector"); done <"$selection"
  "$runner" "${args[@]}" >"$requirements" || return $?
  [ -s "$requirements" ] || { echo "error: selected program requirements are empty" >&2; return 2; }
  while IFS= read -r selector; do
    case "$selector" in primary|compatible|other-island) ;; *) echo "error: unknown program requirement: $selector" >&2; return 2 ;; esac
  done <"$requirements"
}

# Variants share the main Cargo cache; only independent final programs survive.
ci_prepare_system_scenario_binaries() {
  local binary="$1" profile="$2" fixture_root="$3" build_suffix="$4" log_path="$5" requirements="$6"
  local requirement destination code=0 built=false
  SYSTEM_SCENARIO_PRIMARY_BINARY="$binary"
  SYSTEM_SCENARIO_COMPATIBLE_BINARY=""
  SYSTEM_SCENARIO_OTHER_ISLAND_BINARY=""
  [ "$profile" = dev ] || { echo "error: compatibility preparation requires the primary dev profile" >&2; return 2; }
  [ -x "$binary" ] || { echo "error: primary binary is missing" >&2; return 1; }
  mkdir -p "$fixture_root" || return $?
  : >"$log_path"
  while IFS= read -r requirement; do
    [ "$requirement" != primary ] || continue
    ci_set_default_build_jobs || return $?
    built=true
    destination="$(ci_target_directory variants)/$requirement/novarocks"
    mkdir -p "$(dirname "$destination")" || { code=$?; break; }
    case "$requirement" in
      compatible)
        printf '%s\n' "NOVAROCKS_SERVER_BUILD_IDENTITY=ci-compatible-$build_suffix cargo build --workspace --tests --bins --profile dev --locked --target-dir $(ci_target_directory main) --message-format=json" >"$fixture_root/$requirement.cargo.command"
        env NOVAROCKS_SERVER_BUILD_IDENTITY="ci-compatible-$build_suffix" \
          cargo build --workspace --tests --bins --profile dev --locked --target-dir "$(ci_target_directory main)" --message-format=json \
          >"$fixture_root/$requirement.cargo.jsonl" 2>>"$log_path" || code=$? ;;
      other-island)
        printf '%s\n' "NOVAROCKS_SERVER_BUILD_IDENTITY=ci-other-island-$build_suffix cargo build -p novarocks-server --bin novarocks --features native-compatibility-test-fixture --profile dev --locked --target-dir $(ci_target_directory main) --message-format=json" >"$fixture_root/$requirement.cargo.command"
        env NOVAROCKS_SERVER_BUILD_IDENTITY="ci-other-island-$build_suffix" \
          cargo build -p novarocks-server --bin novarocks --features native-compatibility-test-fixture \
          --profile dev --locked --target-dir "$(ci_target_directory main)" --message-format=json \
          >"$fixture_root/$requirement.cargo.jsonl" 2>>"$log_path" || code=$? ;;
      *) echo "error: unsupported variant requirement: $requirement" >>"$log_path"; code=2 ;;
    esac
    printf '%s\n' "$code" >"$fixture_root/$requirement.cargo.exit"
    [ "$code" -eq 0 ] || break
    python3 "${REPO_ROOT:-$PWD}/tools/ci/cargo-artifacts.py" copy-server "$fixture_root/$requirement.cargo.jsonl" "$destination" || { code=$?; break; }
    ci_capture_build_info "$destination" "$fixture_root/$requirement" "ci-$requirement-$build_suffix" || { code=$?; break; }
    case "$requirement" in
      compatible) SYSTEM_SCENARIO_COMPATIBLE_BINARY="$destination" ;;
      other-island) SYSTEM_SCENARIO_OTHER_ISLAND_BINARY="$destination" ;;
    esac
  done < <(sort -u "$requirements")
  [ "$built" = true ] || return 0
  # Restore the exact main graph even after a failed variant preparation.
  local restore_code=0
  ci_prepare_main "$NOVA_CI_ARTIFACT_RECEIPT" || restore_code=$?
  printf '%s\n' "$restore_code" >"$fixture_root/main-restore.exit"
  [ "$code" -eq 0 ] || return "$code"
  [ "$restore_code" -eq 0 ] || return "$restore_code"
  SYSTEM_SCENARIO_PRIMARY_BINARY="$(ci_bound_binary novarocks)" || return $?
  ci_capture_main_builds "$NOVA_CI_ARTIFACT_RECEIPT" "$fixture_root/restored"
}

# Run every default registered scenario serially, one `--only` invocation each, and
# record an independent summary row per scenario.
#
# Returns non-zero on the first failing scenario. The caller decides how to fail
# the run; this function never exits the shell itself.
ci_run_system_scenarios() {
  local runner="$1"
  local binary="$2"
  local compatible_binary="$3"
  local other_island_binary="$4"
  local base_config="$5"
  local artifact_root="$6"
  local cluster_size="$7"
  local timeout_secs="$8"
  local selection="${9:-}"
  local primary_binary

  local scenarios=()
  local scenario
  local list_log="$CI_RUN_DIR/system/list.log"

  mkdir -p "$CI_RUN_DIR/system" "$artifact_root"

  local list_code=0
  if [ -n "$selection" ]; then
    cp "$selection" "$list_log" || return $?
  else
    ci_system_scenario_list "$runner" >"$list_log" 2>&1 || list_code=$?
  fi
  if [ "$list_code" -ne 0 ]; then
    ci_record_system_scenario "<registry>" "FAIL" "0" "$list_log" "-"
    ci_mark_failure_tail "system scenario discovery failed" "$list_log"
    return "$list_code"
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

  python3 - "$CI_INVOCATION_LIBRARY_DIR" "$CI_RUN_DIR/selection.json" "${scenarios[@]}" <<'SELECTION'
import json, sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from identity_snapshot import atomic_json
path=Path(sys.argv[2])
value=json.loads(path.read_text()) if path.is_file() else {"schema":1,"units":[]}
for name in sys.argv[3:]:
    unit={"kind":"system-scenario","id":name,"mode":"default"}
    if unit not in value["units"]: value["units"].append(unit)
atomic_json(path,value)
SELECTION

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

    primary_binary="$binary"
    local variant_args=()
    [ -z "$compatible_binary" ] || variant_args+=(--compatible-binary "$compatible_binary")
    [ -z "$other_island_binary" ] || variant_args+=(--other-island-binary "$other_island_binary")
    start="$(ci_epoch)"
    local scenario_materials
    scenario_materials="$(python3 - "$artifact_dir/$slug" <<'MATERIALS'
import json, sys
from pathlib import Path
root=Path(sys.argv[1])
print(json.dumps({"evidence":str(root/'scenario-evidence.json'),"teardown":str(root/'scenario-teardown.json')}))
MATERIALS
)"
    local declaration_file="$CI_RUN_DIR/system/$slug.declaration.json" spec_file="$CI_RUN_DIR/system/$slug.inputs.json"
    local record needs_file="$CI_RUN_DIR/system/$slug.requirements.txt" requirement required_binary
    "$runner" --list-declarations --only "$scenario" >"$declaration_file" || return $?
    python3 - "$declaration_file" "$spec_file" "$scenario" <<'INPUTS'
import json, sys
value=json.load(open(sys.argv[1]))
selected=[s for s in value['scenarios'] if s['name']==sys.argv[3]]
if len(selected)!=1: raise ValueError('scenario declaration must identify one selected scenario')
declaration=selected[0]['declaration']
with open(sys.argv[2], 'w') as stream:
    json.dump({'inputs':[], 'env_inputs':declaration['env_inputs'] or [], 'declaration':declaration, 'declared':declaration['env_inputs'] is not None},stream)
INPUTS
    local invocation_args=("$runner" --binary "$primary_binary" "${variant_args[@]}" --config "$base_config"
      --artifact-root "$artifact_dir" --cluster-size "$cluster_size" --timeout-secs "$timeout_secs" --only "$scenario")
    record="$(CI_INVOCATION_SPEC="$spec_file" ci_invocation_start system-scenario "$scenario" "${invocation_args[@]}")" || return $?
    code=0
    "$runner" --list-binary-requirements --only "$scenario" >"$needs_file" 2>"$log_path" || code=$?
    if [ "$code" -eq 0 ]; then
      while IFS= read -r requirement; do
        case "$requirement" in
          primary) required_binary="$binary" ;;
          compatible) required_binary="$compatible_binary" ;;
          other-island) required_binary="$other_island_binary" ;;
          *) echo "error: unknown selected program requirement: $requirement" >>"$log_path"; code=2; break ;;
        esac
        if [ -z "$required_binary" ] || [ ! -x "$required_binary" ]; then
          echo "error: selected scenario is missing $requirement program" >>"$log_path"
          code=1; break
        fi
        if [ "$requirement" != primary ]; then
          python3 "${REPO_ROOT}/tools/ci/cargo-artifacts.py" verify-copy "$required_binary.binding.json" "$required_binary" >>"$log_path" 2>&1 || { code=$?; break; }
        fi
      done <"$needs_file"
    fi
    if [ "$code" -eq 0 ]; then
      ci_bound_binary novarocks >>"$log_path" 2>&1 || code=$?
      ci_bound_binary novarocks-system-tests >>"$log_path" 2>&1 || code=$?
    fi
    if [ "$code" -eq 0 ]; then
      ci_run_logged "$log_path" "${invocation_args[@]}" || code=$?
    fi
    ci_invocation_finish "$record" "$code" "$log_path" "$scenario_materials" || return $?
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
