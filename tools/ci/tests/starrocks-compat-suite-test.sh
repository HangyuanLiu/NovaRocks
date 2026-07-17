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

for function_name in run_starrocks_compat_suite validate_starrocks_compat_suite_log; do
  if ! declare -F "$function_name" >/dev/null; then
    echo "local-full-ci must define $function_name" >&2
    exit 1
  fi
done

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

default_output="$({
  WITH_COMPAT="false"
  ci_record_stage() { printf 'record:%s:%s\n' "$1" "$2"; }
  ci_render_summary() { :; }
  run_fail_fast_stage() { printf 'run:%s\n' "$1"; }
  run_compat_gates
})"
if grep -q '^run:' <<<"$default_output"; then
  echo "default local-full-ci must not execute compatibility stages" >&2
  exit 1
fi
for stage in \
  "cargo clippy compat" \
  "cargo build compat artifact" \
  "cargo test compat" \
  "starrocks-compat E2E"; do
  if ! grep -qx "record:$stage:SKIP" <<<"$default_output"; then
    echo "default local-full-ci must record $stage as SKIP" >&2
    exit 1
  fi
done

explicit_output="$({
  WITH_COMPAT="true"
  SKIP_CARGO_TEST="false"
  CI_RUN_DIR="$tmpdir/ci-run"
  NOVA_CI_CARGO_PROFILE="dev-opt"
  mkdir -p "$CI_RUN_DIR"
  ci_record_stage() { printf 'record:%s:%s\n' "$1" "$2"; }
  ci_render_summary() { :; }
  run_fail_fast_stage() { printf 'run:%s|%s\n' "$1" "${*:3}"; }
  run_compat_gates
})"
expected_order="$(printf '%s\n' \
  'cargo clippy compat' \
  'cargo build compat artifact' \
  'cargo test compat' \
  'starrocks-compat E2E')"
actual_order="$(sed -n 's/^run:\([^|]*\).*/\1/p' <<<"$explicit_output")"
if [ "$actual_order" != "$expected_order" ]; then
  echo "--with-compat stage order differs" >&2
  printf 'expected:\n%s\nactual:\n%s\n' "$expected_order" "$actual_order" >&2
  exit 1
fi
grep -q 'build-compat-artifact.sh --profile dev-opt --output-dir ' <<<"$explicit_output"
grep -q 'cargo clippy -p novarocks-server -p novarocks --all-targets --features compat' <<<"$explicit_output"
grep -q 'cargo test -p novarocks-server -p novarocks --profile dev-opt --features compat -- --test-threads=1' <<<"$explicit_output"
grep -q 'run_starrocks_compat_suite .*manifest.txt' <<<"$explicit_output"

default_binary="$tmpdir/novarocks-default"
compat_binary="$tmpdir/novarocks-compat"
probe_binary="$tmpdir/starrocks-compat-probe"
printf '%s\n' '#!/usr/bin/env bash' 'echo default' >"$default_binary"
printf '%s\n' '#!/usr/bin/env bash' 'echo compat' >"$compat_binary"
printf '%s\n' '#!/usr/bin/env bash' 'echo probe' >"$probe_binary"
chmod +x "$default_binary" "$compat_binary" "$probe_binary"
manifest="$tmpdir/manifest.txt"
printf 'binary=%s\n' "$compat_binary" >"$manifest"

runner_hook="$tmpdir/fake-runner.sh"
cat >"$runner_hook" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'manifest=%s\n' "${NOVAROCKS_COMPAT_ARTIFACT_MANIFEST:-}"
printf 'artifact_profile=%s\n' "${NOVAROCKS_COMPAT_ARTIFACT_PROFILE:-}"
printf 'default_binary=%s\n' "${NOVAROCKS_BIN:-}"
printf 'args=%s\n' "$*"
case "${SCT_FAKE_COMPAT_RESULT:-pass}" in
  pass)
    printf '%s\n' \
      'starrocks-compat topology barrier PASS: SHOW BACKENDS 3/3 Alive; heartbeat_ports=[1, 2, 3]' \
      'cases=7' 'total=7' 'pass=7' 'fail=0'
    ;;
  missing-barrier)
    printf '%s\n' 'cases=7' 'total=7' 'pass=7' 'fail=0'
    ;;
  duplicate-barrier)
    printf '%s\n' \
      'starrocks-compat topology barrier PASS: SHOW BACKENDS 3/3 Alive; heartbeat_ports=[1, 2, 3]' \
      'starrocks-compat topology barrier PASS: SHOW BACKENDS 3/3 Alive; heartbeat_ports=[1, 2, 3]' \
      'cases=7' 'total=7' 'pass=7' 'fail=0'
    ;;
  two-backends)
    printf '%s\n' \
      'starrocks-compat topology barrier PASS: SHOW BACKENDS 2/3 Alive; heartbeat_ports=[1, 2]' \
      'cases=7' 'total=7' 'pass=7' 'fail=0'
    ;;
  four-backends)
    printf '%s\n' \
      'starrocks-compat topology barrier PASS: SHOW BACKENDS 4/3 Alive; heartbeat_ports=[1, 2, 3, 4]' \
      'cases=7' 'total=7' 'pass=7' 'fail=0'
    ;;
  decoy-barrier)
    printf '%s\n' \
      '# starrocks-compat topology barrier PASS: SHOW BACKENDS 3/3 Alive; heartbeat_ports=[1, 2, 3]' \
      'cases=7' 'total=7' 'pass=7' 'fail=0'
    ;;
  zero-cases)
    printf '%s\n' \
      'starrocks-compat topology barrier PASS: SHOW BACKENDS 3/3 Alive; heartbeat_ports=[1, 2, 3]' \
      'cases=0' 'total=0' 'pass=0' 'fail=0'
    ;;
esac
EOF
chmod +x "$runner_hook"

run_dir="$tmpdir/runner"
mkdir -p "$run_dir"
CI_RUN_DIR="$run_dir"
NOVA_CI_CARGO_PROFILE="release"
NOVAROCKS_SQL_TEST_CONFIG="$tmpdir/sql-test.toml"
NOVA_CI_DEFAULT_BINARY="$default_binary"
SCT_STARROCKS_COMPAT_RUNNER_HOOK="$runner_hook"

pass_output="$(run_starrocks_compat_suite "$manifest")"
grep -qx "manifest=$manifest" <<<"$pass_output"
grep -qx 'artifact_profile=release' <<<"$pass_output"
grep -qx "default_binary=$default_binary" <<<"$pass_output"
grep -q 'args=.*--suite starrocks-compat --mode verify.*-j 1' <<<"$pass_output"

for bad_result in \
  missing-barrier \
  duplicate-barrier \
  two-backends \
  four-backends \
  decoy-barrier \
  zero-cases; do
  if SCT_FAKE_COMPAT_RESULT="$bad_result" run_starrocks_compat_suite "$manifest" >/dev/null 2>&1; then
    echo "starrocks-compat validation accepted $bad_result" >&2
    exit 1
  fi
done

cp "$default_binary" "$compat_binary"
chmod +x "$compat_binary"
if run_starrocks_compat_suite "$manifest" >/dev/null 2>&1; then
  echo "starrocks-compat validation accepted identical default/compat SHA-256" >&2
  exit 1
fi

echo "starrocks-compat-suite-test: PASS"
