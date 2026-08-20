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

if [[ "$#" -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MYSQL_ENV="$WORKSPACE_ROOT/docker/mysql-state-store/runtime/current/env.sh"
if [[ ! -f "$MYSQL_ENV" ]]; then
  echo "MySQL state-store environment is not initialized; run docker/mysql-state-store/up.sh first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$MYSQL_ENV"
test -n "$NOVA_MYSQL_ENV_ID"
test -n "$NOVA_MYSQL_COMPOSE_PROJECT"
test -n "$NOVA_MYSQL_COMPOSE_FILE"
test -n "$NOVA_MYSQL_RUNTIME_DIR"
test -n "$NOVAROCKS_MYSQL_HOST"
test -n "$NOVAROCKS_MYSQL_PORT"
test -n "$NOVAROCKS_MYSQL_DATABASE"
test -n "$NOVAROCKS_MYSQL_USERNAME"
test -n "$NOVAROCKS_MYSQL_PASSWORD_ENV"
test "$NOVAROCKS_MYSQL_PASSWORD_ENV" = "NOVAROCKS_MYSQL_PASSWORD"
test -n "${!NOVAROCKS_MYSQL_PASSWORD_ENV}"
test -n "$NOVAROCKS_MYSQL_VERSION"
test -n "$NOVAROCKS_MYSQL_IMAGE"

cd "$WORKSPACE_ROOT"

cargo fmt --all -- --check
cargo test -p novarocks-spi
cargo check -p novarocks-spi --no-default-features
"$SCRIPT_DIR/check-spi-dependency-boundary.py" \
  --manifest-path "$WORKSPACE_ROOT/Cargo.toml"
cargo test -p novarocks-state-store-mysql --lib --features state-store-test-hooks
cargo test -p novarocks-server --test state_store_app_config -- --list | \
  awk '$1 == "foundationdb_config_feature_off_open_fails_without_fallback:" { n++ } END { exit(n != 1) }'
cargo test -p novarocks-server --test state_store_app_config foundationdb_config_feature_off_open_fails_without_fallback -- --exact
cargo check -p novarocks-state-store-mysql --all-features
cargo build -p novarocks-server --profile dev-opt

READINESS_DB="$NOVAROCKS_MYSQL_DATABASE"
PROBE_DB="$(docker/mysql-state-store/provision-test-database.sh create production-gate-probes)"
cleanup_probe_db() {
  docker/mysql-state-store/provision-test-database.sh drop "$PROBE_DB"
}
cleanup_probe_db_on_exit() {
  local gate_status="$?"
  cleanup_probe_db || true
  exit "$gate_status"
}
trap cleanup_probe_db_on_exit EXIT
export NOVAROCKS_MYSQL_DATABASE="$PROBE_DB"
docker/mysql-state-store/probes/contract.sh
cleanup_probe_db
export NOVAROCKS_MYSQL_DATABASE="$READINESS_DB"
trap - EXIT

cargo test -p novarocks-state-store-mysql --test state_store_mysql_runtime --features state-store-test-hooks -- --nocapture --test-threads=1
cargo test -p novarocks-state-store-mysql --test state_store_mysql --features state-store-test-hooks -- --list | \
  awk '$1 == "mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io:" { n++ } END { exit(n != 1) }'
cargo test -p novarocks-state-store-mysql --features state-store-test-hooks \
  --test state_store_mysql mysql_provider_state_store_accepts_3072_and_rejects_3073_before_io \
  -- --exact --nocapture --test-threads=1
cargo test -p novarocks-state-store-mysql --features state-store-test-hooks --test state_store_mysql -- --list | \
  awk '$1 == "mysql_suite:" { n++ } END { exit(n != 1) }'
cargo test -p novarocks-state-store-mysql --features state-store-test-hooks \
  --test state_store_mysql mysql_suite -- --exact --nocapture --test-threads=1
cargo test -p novarocks-state-store-mysql --features state-store-test-hooks --test state_store_mysql_cross_process -- --list | \
  awk '$1 == "mysql_cross_process_suite:" { n++ } END { exit(n != 1) }'
cargo test -p novarocks-state-store-mysql --features state-store-test-hooks \
  --test state_store_mysql_cross_process mysql_cross_process_suite \
  -- --exact --nocapture --test-threads=1
cargo build -p novarocks-server --profile dev-opt --features mysql-state-store-provider

# Feature-binary coexistence smoke, NOT provider runtime evidence.
#
# This proves only that a MySQL-StateStore-feature-enabled `novarocks` binary
# still completes a standard native 1FE+3BE topology, query and cleanup. It runs
# on the provider-neutral SQLite base config, so it says nothing about whether a
# query used the MySQL StateStore. Provider correctness is established above by
# the provider contract, runtime and cross-process tests.
cargo build -p novarocks-system-test-runner --profile dev-opt
MYSQL_BASELINE_ARTIFACTS="$(mktemp -d)"
trap 'rm -rf "$MYSQL_BASELINE_ARTIFACTS"' EXIT
target/dev-opt/novarocks-system-tests \
  --binary target/dev-opt/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$MYSQL_BASELINE_ARTIFACTS" \
  --cluster-size 3 \
  --timeout-secs 300 \
  --only query-lifecycle/distributed-baseline
git diff --check
