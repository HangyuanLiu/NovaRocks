# UEA-7 MV persistence measurements

These task-local tools collect command-level wall-time samples for the UEA-7
baseline/candidate comparison. They do not enable any
fault injection and they do not own a workload; the exact workload command is
an explicit argument so the same revision of the tool can measure the adjacent
handoff baseline and the UEA-7 candidate.

Use a clean checkpoint, keep heavyweight builds and clusters idle, and pass at
least seven measured samples. Record non-secret topology, data-size, profile,
and workload facts with repeated `--dimension key=value` arguments.
Repeat `--workload-file` for each workload-defining file; every relative path
and file digest must match across compared worktrees. Give `--config-file` the
generated runtime config used by the command; its path and SHA-256 are recorded
for audit. Its normalized path must match, while its worktree-specific bytes
need not. The command is compared after replacing each worktree root with
`${WORKTREE}`. The exact runtime ID generated for that root by
`docker/iceberg-rest/up.sh` becomes `${RUNTIME}` under
`docker/iceberg-rest/runtime/`; other path segments and arguments must match.
Matching these files is a necessary identity check, not proof that suite hooks,
runner, fixtures, binary, data, or deployment semantics are equivalent. Review
and record those inputs separately before using a comparison for V14.

The following is a **candidate-only functional smoke** for one filtered-out
metadata-only MV refresh after an initial publication. It has no fixed manifest
count and does not satisfy the plan's V14 scale or cost gate. This SQL case does
not exist in the fixed B0 checkout at `355bd3164`, so this smoke has no B0
comparison. Source this worktree's runtime environment and set `NOVAROCKS_BIN`
to its built `dev-opt` server first.

```bash
tools/benchmark/mv-persistence/measure-command.py \
  --label metadata-only-smoke \
  --role candidate \
  --profile dev-opt \
  --workload-file tests/sql/correctness/mv-storage-contract/sql/mv_storage_contract_metadata_only_publication.sql \
  --workload-file tests/sql/correctness/mv-storage-contract/suite.toml \
  --config-file "$NOVAROCKS_SQL_TEST_CONFIG" \
  --samples 7 \
  --warmups 1 \
  --dimension topology=1fe+3be \
  --dimension case=metadata-only-one-filtered-refresh \
  --output /tmp/uea7-candidate-metadata-smoke \
  -- cargo run --locked --profile dev-opt --manifest-path tests/sql/runner/Cargo.toml -- \
       --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite mv-storage-contract \
       --only mv_storage_contract_metadata_only_publication \
       --cluster-mode cross-process --cluster-size 3 --mode verify -j 1
```

For a workload committed in both revisions with identical workload-defining
files, run the same command and dimensions once per worktree and compare the
sealed reports. Each worktree's generated config path may differ. `--noise-ratio`
is the upper edge measured and fixed from repeated B0 runs; it must be chosen
before collecting the candidate. The following paths and ratios are illustrative
for such a shared workload; they do not compare the smoke above and are not a
sealed V14 threshold.

```bash
tools/benchmark/mv-persistence/compare-reports.py \
  --baseline /tmp/uea7-b0-shared-workload/report.json \
  --candidate /tmp/uea7-candidate-shared-workload/report.json \
  --max-ratio 1.10 \
  --noise-ratio 1.03 \
  --output /tmp/uea7-shared-workload-comparison.json
```

`measure-command.py` never invokes a shell: arguments after `--` are executed
directly. Each warmup/sample gets separate stdout and stderr artifact files. The
schema-v2 report records the raw and worktree-normalized commands, workload
digest, runtime config path and digest, Git state, toolchain, platform,
dimensions, wall time, exit status, and `wait4(2)` CPU/RSS **for the direct
controller process only**. With `cargo run` or the SQL runner as that process,
these CPU/RSS values exclude the 1FE+3BE server processes. They are diagnostic
controller figures, not the FE/BE CPU or peak-memory evidence required by V14.
A timeout or non-zero sample still produces a report but makes the command fail.
Controller RSS is reported in bytes on macOS and KiB on Linux, matching
`wait4(2)` on each platform.
Measurements reject a dirty checkout by default; `--allow-dirty` exists only
for developing the tool, and comparison rejects such reports.

The comparison rejects different normalized commands, workload-file relative
paths or bytes, normalized config paths, dimensions, profiles, toolchains,
platforms, sample counts, or RSS units. It checks both median and nearest-rank
p95 wall time and preserves both raw commands and config identities in its
output for review. Controller RSS is
reported separately from wall-time acceptance. Workload-specific FE/BE CPU and
peak memory, request counts, metadata sizes, manifest counts, retention
outcomes, and codec budgets remain workload output and must be preserved
alongside this report. V14 additionally requires dedicated scale workloads and
release-profile samples; this functional smoke does not provide them.
