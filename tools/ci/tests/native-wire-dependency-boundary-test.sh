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
CHECKER="$REPO_ROOT/tools/ci/check-native-wire-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

base_metadata="$tmpdir/base-metadata.json"
cargo metadata --manifest-path "$REPO_ROOT/Cargo.toml" --format-version 1 >"$base_metadata"
"$CHECKER" --metadata-path "$base_metadata"

package_id() {
  local package_name="$1"
  jq -er --arg package_name "$package_name" \
    '.packages[] | select(.name == $package_name) | .id' "$base_metadata"
}

assert_rejected() {
  local metadata_path="$1"
  local expected_error="$2"

  if "$CHECKER" --metadata-path "$metadata_path" \
      >"$metadata_path.stdout" 2>"$metadata_path.stderr"; then
    echo "Native wire dependency boundary mutation was accepted: $metadata_path" >&2
    exit 1
  fi
  grep -Fq "$expected_error" "$metadata_path.stderr"
}

# The boundary concerns dependency ownership, not external feature spelling.
# Reaching Types and libc through local aliases remains a lawful typed graph.
failpoint_legal_platform="$tmpdir/failpoint-legal-platform.json"
failpoint_id="$(package_id novarocks-failpoint)"
jq --arg failpoint_id "$failpoint_id" '
  (.packages[] | select(.name == "novarocks-failpoint") | .features) |= (
    .typed = ["neutral-identities", "receipt-platform"] |
    .["neutral-identities"] = ["dep:novarocks-types"] |
    .["receipt-platform"] = ["dep:libc"]
  ) |
  (.resolve.nodes[] | select(.id == $failpoint_id) | .features) += [
    "neutral-identities", "receipt-platform"
  ]
' "$base_metadata" >"$failpoint_legal_platform"
"$CHECKER" --metadata-path "$failpoint_legal_platform"

# Mirror of add_normal_resolve_edge: remove every normal edge from the source
# owner to the target package, so a fixture can exercise what happens once a
# dependency the checker still expects has actually been cut.
drop_normal_resolve_edge() {
  local source_package="$1"
  local target_package="$2"
  local output_path="$3"
  local source_id
  local target_id

  source_id="$(package_id "$source_package")"
  target_id="$(package_id "$target_package")"
  jq --arg source_id "$source_id" --arg target_id "$target_id" '
    .resolve.nodes |= map(
      if .id == $source_id then
        .deps |= map(select(.pkg != $target_id))
      else
        .
      end
    )
  ' "$base_metadata" >"$output_path"
}

add_normal_resolve_edge() {
  local source_package="$1"
  local target_package="$2"
  local output_path="$3"
  local source_id
  local target_id

  source_id="$(package_id "$source_package")"
  target_id="$(package_id "$target_package")"
  jq --arg source_id "$source_id" --arg target_id "$target_id" '
    .resolve.nodes |= map(
      if .id == $source_id then
        .deps += [{
          name: "native_wire_boundary_mutation",
          pkg: $target_id,
          dep_kinds: [{kind: null, target: null}]
        }]
      elif .id != $source_id then
        # Keep this mutation local to the requested source owner.  Otherwise
        # an existing lower-layer consumer of that owner could fail first and
        # conceal which root closure the fixture is exercising.
        .deps |= map(select(.pkg != $source_id))
      else
        .
      end
    )
  ' "$base_metadata" >"$output_path"
}

models_internal="$tmpdir/models-internal.json"
jq '
  (.packages[] | select(.name == "novarocks-proto-models") | .dependencies) += [{
    name: "novarocks-spi", kind: null, optional: false
  }]
' "$base_metadata" >"$models_internal"
assert_rejected "$models_internal" \
  "novarocks-proto-models has forbidden internal normal dependencies: novarocks-spi"

proto_missing_spi="$tmpdir/proto-missing-spi.json"
jq '
  (.packages[] | select(.name == "novarocks-proto-codec") | .dependencies) |= map(
    select(.name != "novarocks-spi")
  )
' "$base_metadata" >"$proto_missing_spi"
assert_rejected "$proto_missing_spi" \
  "novarocks-proto-codec internal normal dependencies must be exactly"

proto_missing_result_contract="$tmpdir/proto-missing-result-contract.json"
jq '
  (.packages[] | select(.name == "novarocks-proto-codec") | .dependencies) |= map(
    select(.name != "novarocks-result-contract")
  )
' "$base_metadata" >"$proto_missing_result_contract"
assert_rejected "$proto_missing_result_contract" \
  "missing: novarocks-result-contract"

task_codec_missing_result_contract="$tmpdir/task-codec-missing-result-contract.json"
jq '
  (.packages[] | select(.name == "novarocks-task-codec") | .dependencies) |= map(
    select(.name != "novarocks-result-contract")
  )
' "$base_metadata" >"$task_codec_missing_result_contract"
assert_rejected "$task_codec_missing_result_contract" \
  "missing: novarocks-result-contract"

# The real graph above includes the neutral failure vocabulary. Both contracts
# are required, while unrelated direct dependencies remain forbidden.
for required_contract in novarocks-result-contract novarocks-failure-contract; do
  execution_contract_missing="$tmpdir/execution-contract-missing-${required_contract}.json"
  drop_normal_resolve_edge novarocks-execution-contract "$required_contract" \
    "$execution_contract_missing"
  jq --arg required_contract "$required_contract" '
    (.packages[] | select(.name == "novarocks-execution-contract") | .dependencies) |= map(
      select(.name != $required_contract)
    )
  ' "$execution_contract_missing" >"$tmpdir/execution-contract-missing-consistent.json"
  mv "$tmpdir/execution-contract-missing-consistent.json" "$execution_contract_missing"
  assert_rejected "$execution_contract_missing" \
    "novarocks-execution-contract internal normal dependencies must be exactly"
done

execution_contract_extra="$tmpdir/execution-contract-extra.json"
jq '
  (.packages[] | select(.name == "novarocks-execution-contract") | .dependencies) += [{
    name: "novarocks-spi", kind: null, optional: false
  }]
' "$base_metadata" >"$execution_contract_extra"
assert_rejected "$execution_contract_extra" \
  "novarocks-execution-contract internal normal dependencies must be exactly"

# The new failure fact owner is required, not an arbitrary extra neutral edge.
for owner in novarocks-execution-contract novarocks-result-contract; do
  missing_failure="$tmpdir/$owner-missing-failure-contract.json"
  jq --arg owner "$owner" '
    (.packages[] | select(.name == $owner) | .dependencies) |= map(
      select(.name != "novarocks-failure-contract")
    )
  ' "$base_metadata" >"$missing_failure"
  if [ "$owner" = novarocks-execution-contract ]; then
    assert_rejected "$missing_failure" \
      "$owner internal normal dependencies must be exactly"
  else
    assert_rejected "$missing_failure" \
      "$owner normal dependencies must be exactly: novarocks-failure-contract"
  fi

  optional_failure="$tmpdir/$owner-optional-failure-contract.json"
  jq --arg owner "$owner" '
    (.packages[] | select(.name == $owner) | .dependencies[] |
      select(.name == "novarocks-failure-contract") | .optional) = true
  ' "$base_metadata" >"$optional_failure"
  assert_rejected "$optional_failure" \
    "$owner must directly declare required normal dependency novarocks-failure-contract"
done

result_missing_failure_edge="$tmpdir/result-missing-failure-edge.json"
drop_normal_resolve_edge novarocks-result-contract novarocks-failure-contract \
  "$result_missing_failure_edge"
assert_rejected "$result_missing_failure_edge" \
  "novarocks-result-contract normal dependency closure must be exactly: novarocks-failure-contract"

result_extra_internal="$tmpdir/result-extra-internal.json"
jq '
  (.packages[] | select(.name == "novarocks-result-contract") | .dependencies) += [{
    name: "novarocks-types", kind: null, optional: false
  }]
' "$base_metadata" >"$result_extra_internal"
assert_rejected "$result_extra_internal" \
  "novarocks-result-contract normal dependencies must be exactly: novarocks-failure-contract"

result_contract_direct="$tmpdir/result-contract-direct.json"
jq '
  (.packages[] | select(.name == "novarocks-result-contract") | .dependencies) += [{
    name: "bytes", kind: null, optional: true
  }]
' "$base_metadata" >"$result_contract_direct"
assert_rejected "$result_contract_direct" \
  "novarocks-result-contract normal dependencies must be exactly: novarocks-failure-contract"
grep -Fq "unexpected: bytes" "$result_contract_direct.stderr"

result_contract_missing_failure="$tmpdir/result-contract-missing-failure.json"
drop_normal_resolve_edge novarocks-result-contract novarocks-failure-contract \
  "$result_contract_missing_failure"
jq '
  (.packages[] | select(.name == "novarocks-result-contract") | .dependencies) |= map(
    select(.name != "novarocks-failure-contract")
  )
' "$result_contract_missing_failure" >"$tmpdir/result-contract-missing-consistent.json"
mv "$tmpdir/result-contract-missing-consistent.json" "$result_contract_missing_failure"
assert_rejected "$result_contract_missing_failure" \
  "novarocks-result-contract normal dependencies must be exactly: novarocks-failure-contract"
grep -Fq "missing: novarocks-failure-contract" "$result_contract_missing_failure.stderr"

result_contract_missing_resolved_failure="$tmpdir/result-contract-missing-resolved-failure.json"
drop_normal_resolve_edge novarocks-result-contract novarocks-failure-contract \
  "$result_contract_missing_resolved_failure"
assert_rejected "$result_contract_missing_resolved_failure" \
  "novarocks-result-contract normal dependency closure must be exactly: novarocks-failure-contract"
grep -Fq "missing: novarocks-failure-contract" "$result_contract_missing_resolved_failure.stderr"

for forbidden in novarocks-proto-models novarocks-execution tonic; do
  result_contract_closure="$tmpdir/result-contract-${forbidden}.json"
  add_normal_resolve_edge novarocks-result-contract "$forbidden" "$result_contract_closure"
  assert_rejected "$result_contract_closure" \
    "novarocks-result-contract normal dependency closure must be exactly: novarocks-failure-contract"
  grep -Fq "$forbidden" "$result_contract_closure.stderr"
done

# FailureContract stays below every runtime and wire owner. An inactive
# optional external declaration is forbidden even when resolve omits its edge.
for dependency in bytes novarocks-execution novarocks-worker novarocks-native-adapter; do
  for optional in true false; do
    failure_direct="$tmpdir/failure-direct-$dependency-$optional.json"
    jq --arg dependency "$dependency" --argjson optional "$optional" '
      (.packages[] | select(.name == "novarocks-failure-contract") | .dependencies) += [{
        name: $dependency, kind: null, optional: $optional
      }]
    ' "$base_metadata" >"$failure_direct"
    assert_rejected "$failure_direct" \
      "novarocks-failure-contract must not declare normal dependencies: $dependency"
  done
done

for forbidden in bytes novarocks-types novarocks-proto-models novarocks-execution novarocks-worker novarocks-native-adapter tonic; do
  failure_closure="$tmpdir/failure-closure-$forbidden.json"
  add_normal_resolve_edge novarocks-failure-contract "$forbidden" "$failure_closure"
  assert_rejected "$failure_closure" \
    "novarocks-failure-contract normal dependency closure must be empty:"
  grep -Fq "$forbidden" "$failure_closure.stderr"
done

# A second hop must not hide a forbidden runtime/wire owner behind Types.
# These are independent graph mutations; declarations remain the clean base.
for forbidden in novarocks-execution novarocks-worker novarocks-native-adapter novarocks-proto-models; do
  failure_transitive="$tmpdir/failure-transitive-$forbidden.json"
  add_normal_resolve_edge novarocks-failure-contract novarocks-types "$failure_transitive"
  types_id="$(package_id novarocks-types)"
  forbidden_id="$(package_id "$forbidden")"
  jq --arg types_id "$types_id" --arg forbidden_id "$forbidden_id" '
    (.resolve.nodes[] | select(.id == $types_id) | .deps) += [{
      name: "native_wire_boundary_transitive_mutation",
      pkg: $forbidden_id, dep_kinds: [{kind: null, target: null}]
    }]
  ' "$failure_transitive" >"$failure_transitive.updated"
  mv "$failure_transitive.updated" "$failure_transitive"
  assert_rejected "$failure_transitive" \
    "novarocks-failure-contract normal dependency closure must be empty:"
  grep -Fq "$forbidden" "$failure_transitive.stderr"
done

task_codec_execution="$tmpdir/task-codec-execution.json"
jq '
  (.packages[] | select(.name == "novarocks-task-codec") | .dependencies) += [{
    name: "novarocks-execution", kind: null, optional: false
  }]
' "$base_metadata" >"$task_codec_execution"
assert_rejected "$task_codec_execution" \
  "novarocks-task-codec internal normal dependencies must be exactly"

for role in novarocks-frontend-application novarocks-native-adapter; do
  role_missing_models="$tmpdir/${role}-missing-models.json"
  jq --arg role "$role" '
    (.packages[] | select(.name == $role) | .dependencies) |= map(
      select(.name != "novarocks-proto-models")
    )
  ' "$base_metadata" >"$role_missing_models"
  assert_rejected "$role_missing_models" \
    "$role must directly declare normal dependencies on: novarocks-proto-models"
done

# Models may never reach Tonic.  Proto is checked against every forbidden
# application/role/provider/state-store/Tonic package so each protected edge
# has an independent mutation witness.
models_tonic="$tmpdir/models-tonic.json"
add_normal_resolve_edge novarocks-proto-models tonic "$models_tonic"
assert_rejected "$models_tonic" \
  "novarocks-proto-models normal dependency closure contains forbidden packages: tonic"

for forbidden in \
  tonic \
  novarocks-connector-starrocks \
  novarocks-execution \
  novarocks-frontend-application \
  novarocks-server \
  novarocks-server-application \
  novarocks-sql \
  novarocks-state-store-foundationdb \
  novarocks-state-store-mysql \
  novarocks-state-store-sqlite; do
  proto_forbidden="$tmpdir/proto-${forbidden}.json"
  add_normal_resolve_edge novarocks-proto-codec "$forbidden" "$proto_forbidden"
  assert_rejected "$proto_forbidden" \
    "novarocks-proto-codec normal dependency closure contains forbidden packages:"
  grep -Fq "$forbidden" "$proto_forbidden.stderr"
done

# Task Codec owns the exact Task wire conversion and may depend on the neutral
# execution contract, but neither a direct nor transitive normal edge may pull
# the execution kernel into its closure.
task_codec_execution_closure="$tmpdir/task-codec-execution-closure.json"
add_normal_resolve_edge novarocks-task-codec novarocks-execution "$task_codec_execution_closure"
assert_rejected "$task_codec_execution_closure" \
  "novarocks-task-codec normal dependency closure contains forbidden packages:"
grep -Fq "novarocks-execution" "$task_codec_execution_closure.stderr"

contract_execution_closure="$tmpdir/contract-execution-closure.json"
add_normal_resolve_edge novarocks-execution-contract novarocks-execution \
  "$contract_execution_closure"
assert_rejected "$contract_execution_closure" \
  "novarocks-execution-contract normal dependency closure contains forbidden packages:"
grep -Fq "novarocks-execution" "$contract_execution_closure.stderr"

contract_wire_closure="$tmpdir/contract-wire-closure.json"
add_normal_resolve_edge novarocks-execution-contract novarocks-proto-models \
  "$contract_wire_closure"
assert_rejected "$contract_wire_closure" \
  "novarocks-execution-contract normal dependency closure contains forbidden packages:"
grep -Fq "novarocks-proto-models" "$contract_wire_closure.stderr"

# Permitting the neutral vocabulary must not permit a wire/runtime dependency
# hidden behind it. Keep the declaration and resolved normal edge consistent.
failure_contract_id="$(package_id novarocks-failure-contract)"
for forbidden in novarocks-proto-models novarocks-execution; do
  failure_contract_closure="$tmpdir/failure-contract-${forbidden}.json"
  forbidden_id="$(package_id "$forbidden")"
  jq --arg failure_contract_id "$failure_contract_id" \
     --arg forbidden "$forbidden" --arg forbidden_id "$forbidden_id" '
    (.packages[] | select(.name == "novarocks-failure-contract") | .dependencies) += [{
      name: $forbidden, kind: null, optional: false
    }] |
    (.resolve.nodes[] | select(.id == $failure_contract_id) | .deps) += [{
      name: ($forbidden | gsub("-"; "_")), pkg: $forbidden_id,
      dep_kinds: [{kind: null, target: null}]
    }]
  ' "$base_metadata" >"$failure_contract_closure"
  assert_rejected "$failure_contract_closure" \
    "novarocks-failure-contract must not declare normal dependencies:"
  grep -Fq "$forbidden" "$failure_contract_closure.stderr"
done

for forbidden in \
  novarocks-frontend-application \
  novarocks-native-adapter \
  novarocks-worker \
  novarocks-server \
  novarocks-server-application; do
  plan_codec_forbidden="$tmpdir/plan-codec-${forbidden}.json"
  add_normal_resolve_edge novarocks-plan-codec "$forbidden" "$plan_codec_forbidden"
  assert_rejected "$plan_codec_forbidden" \
    "novarocks-plan-codec normal dependency closure contains forbidden planning or application packages:"
  grep -Fq "$forbidden" "$plan_codec_forbidden.stderr"
done

# The final-plan encoder must not reacquire SQL through a normal edge.
plan_codec_sql_edge="$tmpdir/plan-codec-sql-edge.json"
add_normal_resolve_edge novarocks-plan-codec novarocks-sql "$plan_codec_sql_edge"
assert_rejected "$plan_codec_sql_edge" \
  "novarocks-plan-codec normal dependency closure contains forbidden planning or application packages:"
grep -Fq "novarocks-sql" "$plan_codec_sql_edge.stderr"

# Lower-layer owners must never acquire either wire crate, including through a
# transitive normal edge. ADR-0114 deliberately excludes Iceberg and
# Execution: Iceberg owns typed provider conversion and Execution owns the
# typed per-attempt split queue. StarRocks is also excluded because its
# provider-owned role-binding factory deliberately depends on the generic
# binding contract. Server is excluded because it composes FE/BE and therefore
# has a wire-containing transitive closure by design.
for lower_layer in \
  novarocks-spi \
  novarocks-types \
  novarocks-sql \
  novarocks-state-store-foundationdb \
  novarocks-state-store-mysql \
  novarocks-state-store-sqlite; do
  lower_layer_wire="$tmpdir/${lower_layer}-wire.json"
  add_normal_resolve_edge "$lower_layer" novarocks-proto-models "$lower_layer_wire"
  assert_rejected "$lower_layer_wire" \
    "$lower_layer normal dependency closure contains forbidden wire packages:"
  grep -Fq "novarocks-proto-models" "$lower_layer_wire.stderr"
done

starrocks_missing_spi="$tmpdir/starrocks-missing-spi.json"
jq '
  (.packages[] | select(.name == "novarocks-connector-starrocks") | .dependencies) |= map(
    select(.name != "novarocks-spi")
  )
' "$base_metadata" >"$starrocks_missing_spi"
assert_rejected "$starrocks_missing_spi" \
  "novarocks-connector-starrocks must directly declare normal dependency on novarocks-spi"

starrocks_direct_wire="$tmpdir/starrocks-direct-wire.json"
jq '
  (.packages[] | select(.name == "novarocks-connector-starrocks") | .dependencies) += [{
    name: "novarocks-proto-models", kind: null, optional: false
  }]
' "$base_metadata" >"$starrocks_direct_wire"
assert_rejected "$starrocks_direct_wire" \
  "novarocks-connector-starrocks must not directly depend on wire packages"

starrocks_optional_direct_wire="$tmpdir/starrocks-optional-direct-wire.json"
jq '
  (.packages[] | select(.name == "novarocks-connector-starrocks") | .dependencies) += [{
    name: "novarocks-proto-codec", kind: null, optional: true
  }]
' "$base_metadata" >"$starrocks_optional_direct_wire"
assert_rejected "$starrocks_optional_direct_wire" \
  "novarocks-connector-starrocks must not directly depend on wire packages"

starrocks_server="$tmpdir/starrocks-server.json"
add_normal_resolve_edge novarocks-connector-starrocks novarocks-server "$starrocks_server"
assert_rejected "$starrocks_server" \
  "novarocks-connector-starrocks normal dependency closure contains application owners:"
grep -Fq "novarocks-server" "$starrocks_server.stderr"

for owner in novarocks-server novarocks-server-application; do
  server_direct_wire="$tmpdir/$owner-direct-wire.json"
  jq --arg owner "$owner" '
    (.packages[] | select(.name == $owner) | .dependencies) += [{
      name: "novarocks-proto-codec", kind: null, optional: false
    }]
  ' "$base_metadata" >"$server_direct_wire"
  assert_rejected "$server_direct_wire" \
    "$owner must not directly declare normal wire dependencies: novarocks-proto-codec"
done

for forbidden in novarocks-proto-codec novarocks-proto-models novarocks-spi; do
  failpoint_forbidden="$tmpdir/failpoint-${forbidden}.json"
  add_normal_resolve_edge novarocks-failpoint "$forbidden" "$failpoint_forbidden"
  assert_rejected "$failpoint_forbidden" \
    "novarocks-failpoint typed normal dependency closure contains forbidden packages:"
  grep -Fq "$forbidden" "$failpoint_forbidden.stderr"
done

failpoint_typed_feature="$tmpdir/failpoint-typed-feature.json"
add_normal_resolve_edge novarocks-failpoint novarocks-spi "$failpoint_typed_feature"
jq --arg failpoint_id "$failpoint_id" '
  (.packages[] | select(.name == "novarocks-failpoint")) |= (
    .dependencies += [{name: "novarocks-spi", kind: null, optional: true}] |
    .features.typed += ["dep:novarocks-spi"]
  ) |
  (.resolve.nodes[] | select(.id == $failpoint_id) | .deps[-1].name) = "novarocks_spi"
' "$failpoint_typed_feature" >"$tmpdir/failpoint-typed-feature-consistent.json"
mv "$tmpdir/failpoint-typed-feature-consistent.json" "$failpoint_typed_feature"
assert_rejected "$failpoint_typed_feature" \
  "novarocks-failpoint typed normal dependency closure contains forbidden packages:"
grep -Fq "novarocks-spi" "$failpoint_typed_feature.stderr"

# Removing the neutral identity dependency must remove its activation, declaration
# and resolved edge, including when the declaration is unconditional.
failpoint_missing_types="$tmpdir/failpoint-missing-types.json"
drop_normal_resolve_edge novarocks-failpoint novarocks-types "$failpoint_missing_types"
jq '
  (.packages[] | select(.name == "novarocks-failpoint")) |= (
    .features.typed |= map(select(. != "dep:novarocks-types")) |
    .dependencies |= map(select(.name != "novarocks-types"))
  )
' "$failpoint_missing_types" >"$tmpdir/failpoint-missing-types-consistent.json"
mv "$tmpdir/failpoint-missing-types-consistent.json" "$failpoint_missing_types"
assert_rejected "$failpoint_missing_types" \
  "novarocks-failpoint feature typed must activate neutral normal dependency novarocks-types"

echo "native-wire-dependency-boundary-test: PASS"
