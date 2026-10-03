#!/usr/bin/env bash
set -euo pipefail
cd /Users/harbor/.codex/worktrees/b92b/NovaRocks
export CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0
binary_dir=logs/mem-1-m07/p04-sql-scalar-compat-binaries
artifact_dir=logs/mem-1-m07/p04-sql-scalar-semantics-system
mkdir -p "$binary_dir" "$artifact_dir"
cp target/debug/novarocks "$binary_dir/novarocks-primary"
NOVAROCKS_NATIVE_BUILD_IDENTITY=ci-compatible-m07-sql-final \
  cargo build --locked -p novarocks-server --bin novarocks --profile dev > /tmp/m07-sql-root-compatible-build.log 2>&1
cp target/debug/novarocks "$binary_dir/novarocks-compatible"
NOVAROCKS_NATIVE_BUILD_IDENTITY=ci-other-island-m07-sql-final \
  cargo build --locked -p novarocks-server --bin novarocks --profile dev --features native-compatibility-test-fixture > /tmp/m07-sql-root-other-island-build.log 2>&1
cp target/debug/novarocks "$binary_dir/novarocks-other-island"
cp "$binary_dir/novarocks-primary" target/debug/novarocks
for scenario in \
  native-trust/plaintext-ip \
  native-trust/automatic-dns \
  native-trust/pem-ip \
  native-ingress/outer-preflight-rejection \
  native-ingress/partial-body-deadline \
  native-ingress/registry-contention-control \
  native-ingress/blocking-saturation-control \
  native-compatibility/island-drain-and-replacement \
  query-lifecycle/distributed-baseline
do
  case_dir="${scenario//\//-}"
  target/debug/novarocks-system-tests \
    --binary "$binary_dir/novarocks-primary" \
    --compatible-binary "$binary_dir/novarocks-compatible" \
    --other-island-binary "$binary_dir/novarocks-other-island" \
    --config tools/ci/fixtures/system-scenarios-base.toml \
    --artifact-root "$artifact_dir/$case_dir" \
    --cluster-size 3 --timeout-secs 300 --only "$scenario" \
    > "/tmp/m07-sql-root-system-$case_dir.log" 2>&1
  printf '%s PASS\n' "$scenario"
done
