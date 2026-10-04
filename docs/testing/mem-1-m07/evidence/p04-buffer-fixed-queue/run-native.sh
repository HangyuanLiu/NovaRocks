#!/usr/bin/env bash
set -euo pipefail
work="$PWD/logs/mem-1-m07/p04-buffer-fixed-queue"
product="$PWD/logs/mem-1-m07/p04-response-port/product-target/debug"
python3 - <<'PY'
from pathlib import Path
import json,hashlib
r=Path.cwd();w=r/'logs/mem-1-m07/p04-buffer-fixed-queue'
pins=json.loads((w/'source-pins.json').read_text())
assert all(hashlib.sha256((r/p).read_bytes()).hexdigest()==h for p,h in pins['pins'].items())
b={n:hashlib.sha256((r/'logs/mem-1-m07/p04-response-port/product-target/debug'/n).read_bytes()).hexdigest() for n in ['novarocks','novarocks-system-tests','novarocks-sql-test']}
(w/'binary-pins-final.json').write_text(json.dumps(b,indent=2)+'\n')
PY
"$product/novarocks-system-tests" \
  --only query-lifecycle/distributed-baseline \
  --only native-trust/plaintext-ip \
  --only native-trust/automatic-dns \
  --only native-trust/pem-ip \
  --only native-ingress/outer-preflight-rejection \
  --only native-ingress/blocking-saturation-control \
  --only native-ingress/partial-body-deadline \
  --only native-ingress/registry-contention-control \
  --binary "$product/novarocks" \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$PWD/logs/mem-1-m07/p04-buffer-fixed-queue-system" \
  --cluster-size 3 --timeout-secs 900 >"$work/system-final.log" 2>&1
source docker/iceberg-rest/runtime/current/env.sh
export NOVAROCKS_BIN="$product/novarocks"
"$product/novarocks-sql-test" --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite analytic --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 \
  --only window_result_domain_handoff,analytic_test_window_hll_bitmap --fail-fast >"$work/sql-analytic-final.log" 2>&1
"$product/novarocks-sql-test" --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite aggregate --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 \
  --only opaque_result_domain_handoff,agg_test_hll,agg_test_percentile_union,array_agg_json_semantic_schema --fail-fast >"$work/sql-aggregate-final.log" 2>&1
"$product/novarocks-sql-test" --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 \
  --only variant_insert,insert_exact_numeric_input --fail-fast >"$work/sql-iceberg-dml-final.log" 2>&1
python3 - <<'PY'
from pathlib import Path
import json,hashlib
r=Path.cwd();w=r/'logs/mem-1-m07/p04-buffer-fixed-queue'
pins=json.loads((w/'source-pins.json').read_text())
assert all(hashlib.sha256((r/p).read_bytes()).hexdigest()==h for p,h in pins['pins'].items())
before=json.loads((w/'binary-pins-final.json').read_text())
assert all(hashlib.sha256((r/'logs/mem-1-m07/p04-response-port/product-target/debug'/n).read_bytes()).hexdigest()==h for n,h in before.items())
print('Final source and binary identities unchanged after native verification.')
PY
