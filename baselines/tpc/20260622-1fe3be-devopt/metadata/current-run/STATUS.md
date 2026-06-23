# TPC Baseline Run Status

Date: 2026-06-22
Workspace: `/Users/harbor/.codex/worktrees/4f87/NovaRocks`
HEAD: `dd4884203d88d1455c722de90466e7eea97155f6`

## Goal

Collect the current NovaRocks baseline for SSB, TPC-H, and TPC-DS using
cross-process `1 FE + 3 BE` with a `dev-opt` NovaRocks binary:

- normal query runtime
- `EXPLAIN` plan
- `EXPLAIN ANALYZE` plan with actual rows/time/memory
- slow operator/node summary
- StarRocks FE plan comparison when available

## Completed

- Built current NovaRocks binary with `cargo build --profile dev-opt --bin novarocks`.
- Built SQL runner with `cargo build --manifest-path tests/sql-test-runner/Cargo.toml --profile dev-opt --bin sql-tests`.
- Copied runnable binaries into this run directory:
  - `bin/novarocks-dev-opt`
  - `bin/sql-tests-dev-opt`
- Removed regenerated `target/dev-opt` and `tests/sql-test-runner/target` directories after copying binaries to free disk for benchmark data.
- Generated temporary SQL files:
  - `generated-sql/explain/{ssb,tpc-h,tpc-ds}`
  - `generated-sql/analyze/{ssb,tpc-h,tpc-ds}`
  - `generated-sql/manifest.json`
- Regenerated temporary SQL on 2026-06-23 so TPC-DS multi-section files
  (`q14`, `q23`, `q24`, `q39`) preserve runner-compatible `-- query N`
  section boundaries while wrapping each section with `EXPLAIN ANALYZE`.
- Restarted the Docker-backed Iceberg/MinIO/Spark fixture after user cleared
  disk. Verified MinIO and Iceberg REST health before resuming SQL runs.
- Added run-local bootstrap/config artifacts only under this baseline directory:
  - `bootstrap_benchmark_data_spark4g.sh`
  - `sql-test-baseline.conf`
  These keep Spark benchmark bootstrap at `--driver-memory 4g`,
  `--master local[4]`, and lower Spark parallelism without changing repo
  bootstrap source files.
- Ran `EXPLAIN ANALYZE` baseline through cross-process `1 FE + 3 BE`:
  - SSB: 13 / 13 pass, wall time 50.39s
  - TPC-H: 22 / 22 pass, wall time 178.11s
  - TPC-DS: 99 / 99 files pass, 103 query sections, wall time 709.04s
- Wrote parsed reports under `reports/`:
  - `reports/SUMMARY.md`
  - `reports/suite_summary.csv`
  - `reports/case_timings.csv`
  - `reports/step_timings.csv`
  - `reports/explain_sections.csv`
  - `reports/slow_nodes.csv`
  - `reports/slow_nodes_top5_per_query.csv`
  - `reports/analyze-status-complete.tsv`

## Final Status

The NovaRocks `EXPLAIN ANALYZE` baseline is complete for SSB, TPC-H, and
TPC-DS on the requested dev-opt cross-process `1 FE + 3 BE` shape.

Plan text and query-level Planning/Execution/Rows headers are in:

`plans/analyze/{ssb,tpc-h,tpc-ds}/*.result`

The parsed slow case and slow node summaries are in:

`reports/SUMMARY.md`

Current `act={time=...}` values are per-node runtime counters from the plan
output. They identify hot operators but do not necessarily sum to case wall
time, especially when distributed wait/exchange/driver scheduling overhead is
significant.

## Reusable Existing Plan Baseline

An older plan comparison exists at:

`/Users/harbor/project/NovaRocks/logs/plan-compare/20260613-201518`

That run contains both NovaRocks and StarRocks FE plans for:

- SSB: 13 / 13
- TPC-H: 22 / 22
- TPC-DS: 103 / 103 statements

It is useful as a historical plan-shape comparison, but it is not a replacement
for the current runtime and `EXPLAIN ANALYZE` baseline requested for this run.

## Resume Command Shape

To rerun one suite, use the copied binaries and run-local config instead of
rebuilding:

```bash
RUN_DIR=/Users/harbor/.codex/worktrees/4f87/NovaRocks/logs/baseline/20260622-231416-tpc-baseline
source /Users/harbor/.codex/worktrees/4f87/NovaRocks/docker/iceberg-rest/runtime/current/env.sh

NO_PROXY=127.0.0.1,localhost \
no_proxy=127.0.0.1,localhost \
NOVAROCKS_BIN="$RUN_DIR/bin/novarocks-dev-opt" \
"$RUN_DIR/bin/sql-tests-dev-opt" \
  --config "$RUN_DIR/sql-test-baseline.conf" \
  --suite ssb \
  --sql-dir "$RUN_DIR/generated-sql/analyze/ssb" \
  --result-dir "$RUN_DIR/plans/analyze/ssb" \
  --mode record \
  --record-from target \
  --update-expected \
  --only q1.1 \
  --cluster-mode cross-process \
  --cluster-size 3 \
  --query-timeout 300 \
  -j 1
```
