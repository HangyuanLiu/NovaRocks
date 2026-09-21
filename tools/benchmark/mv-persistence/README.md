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

For V14 scale preparation, `generate-scale-sql.py` creates a separate native
SQL-runner case for each requested starting count (0, 1, 100, or 1000) and
number of filtered-out source publications. Each positive source append is
followed by one MV refresh. Before the filtered-out publications, Spark reads
the target Iceberg `manifests` metadata table and asserts the **actual** count;
afterward it records the actual count without assuming it remains fixed. The
case also checks the final MV row count. Use a unique output directory and
observation file per run. The SQL runner's `mv-storage-contract` suite supplies
an isolated REST Catalog and MinIO fixture; run it in native 1FE+3BE mode.

```bash
source docker/iceberg-rest/runtime/current/env.sh
export NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks"
python3 tools/benchmark/mv-persistence/generate-scale-sql.py \
  --manifests 100 --metadata-publications 1000 \
  --output /tmp/uea7-scale-100/sql/mv_scale_generated.sql
UEA7_SCALE_OBSERVATION_FILE=/tmp/uea7-scale-100/manifest-counts.txt \
  cargo run --locked --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite mv-storage-contract \
  --sql-dir /tmp/uea7-scale-100/sql --result-dir /tmp/uea7-scale-100/result \
  --cluster-mode cross-process --cluster-size 3 --mode verify -j 1
```

The observation file is created once and contains
`MV_MANIFEST_COUNT_BEFORE` and `MV_MANIFEST_COUNT_AFTER`; the command rejects
an existing file. A small native probe observed 1 initial manifest becoming 3
after two filtered-out publications, and 0 becoming 1. These publications
therefore cannot be labeled as preserving a fixed manifest count. The 100
manifest preparation case passed with 100 observed before and after zero
filtered-out publications. The first 1000 manifest preparation attempt timed
out on the 328th target refresh. After the driver/sink completion fix below,
one clean native 1FE+3BE run passed all 2008 steps in 4968.12 seconds; Spark
observed exactly 1000 target manifests before and after zero filtered-out
publications. Its observation file is
`/tmp/uea7-manifest-probe/thousand-driver-race.observations`, and its runner log
is `/tmp/uea7-manifest-probe/generated-thousand-driver-race.log`. The 1000
filtered-out publication case exposed a pipeline
completion race on a native 1FE+3BE cluster. Unmodified runs stopped after 109,
222, 299, or 427 completed refreshes; clean runs of 223 and 350 publications
passed, with observed target manifest counts of 222 and 349. In every stopped
run, a stage 5 driver could complete while its exchange sink still owed the
shared EOS, so the final stage 6 task waited until the statement timeout. After
fixing that driver/sink completion race, one clean 1000-publication run passed
all 2008 steps in 3599.11 seconds. Its final manifest query returned a numeric
count and the final MV row-count assertion passed; that run did not set an
observation file, so the numeric final manifest count was not retained as a
separate artifact. The runner resource report is
`/tmp/uea7-zero-thousand-driver-race.resources.json`.

The default generated case does not collect per-publish request counts,
manifest-list bytes, or metadata file sizes. The SQL runner can collect
FE/BE CPU and RSS with `--process-resource-output`, but these observations
alone are not V14 acceptance evidence or release-profile B0 comparisons.

Add `--measure-rest-traffic` when generating a case to bracket each
filtered-out MV refresh with cumulative snapshots from the runner's transparent
REST Catalog proxy. Set `UEA7_SCALE_REST_TRAFFIC_FILE` to a new JSONL artifact
path before running the native SQL suite. The proxy counts requests it actually
forwards to the real catalog, including failures and retries, with HTTP method,
proxy response status, request-body bytes, and downloaded catalog response-body
bytes. Proxy-generated error bodies are excluded. Fixture control calls and
known-before-dispatch refusals are excluded. The generated case writes one
delta per refresh, with zero-based `index`; an incomplete refresh leaves a
`.pending` snapshot for diagnosis. The synchronous SQL runner step supplies
refresh latency in its log. This measurement does **not** count S3 requests,
manifest-list object bytes, metadata file sizes, or background traffic outside
the bracket. A private REST/MinIO fixture limits unrelated work, but attribution
still requires checking each measured window.

```bash
python3 tools/benchmark/mv-persistence/generate-scale-sql.py \
  --manifests 0 --metadata-publications 1000 --measure-rest-traffic \
  --output /tmp/uea7-scale-rest/sql/mv_scale_generated.sql
mkdir -p /tmp/uea7-scale-rest/result
export UEA7_SCALE_OBSERVATION_FILE=/tmp/uea7-scale-rest/manifest-counts.txt
export UEA7_SCALE_REST_TRAFFIC_FILE=/tmp/uea7-scale-rest/rest-traffic.jsonl
# Run the native 1FE+3BE SQL command above with these SQL and result directories.
```

One clean 0-manifest, one-publication smoke passed 12/12 SQL steps in native
1FE+3BE mode. Its single refresh window forwarded 8 REST requests (7 GET,
1 POST), 4,475 request-body bytes, and 65,896 response-body bytes; the final
Spark manifest count was 0. The artifact is
`/tmp/uea7-scale-traffic-smoke/rest-traffic.jsonl`. These are one-run
diagnostic counts, not a fixed-cost claim or V14 acceptance evidence.

With ten successive publications, the same native topology passed 48/48 steps
from 0 starting manifests and 248/248 steps from 100 starting manifests. Each
of the twenty windows forwarded 8 REST requests (7 GET, 1 POST). REST response
body bytes rose from 65,866 to 342,860 across the 0-starting run, and from
3,160,928 to 3,422,968 across the 100-starting run. Spark observed target
manifest counts 0→9 and 100→110. The artifacts are
`/tmp/uea7-scale-traffic-ten/rest-traffic.jsonl` and
`/tmp/uea7-scale-traffic-hundred/rest-traffic.jsonl`. These values describe
the full REST response bodies in each refresh window; they do not identify
which bytes are Iceberg manifest lists or metadata files.

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
       --cluster-mode cross-process --cluster-size 3 --mode verify -j 1 \
       --process-resource-output '@SAMPLE_RESOURCE_OUTPUT@'
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
schema-v3 report records the raw and worktree-normalized commands, workload
digest, runtime config path and digest, Git state, toolchain, platform,
dimensions, wall time, exit status, and `wait4(2)` CPU/RSS **for the direct
controller process only**. With `cargo run` or the SQL runner as that process,
these CPU/RSS values exclude the 1FE+3BE server processes. They are diagnostic
controller figures, not FE/BE measurements.
When the command includes the exact `@SAMPLE_RESOURCE_OUTPUT@` argument,
`measure-command.py` supplies a distinct artifact path for each warmup and
sample. The cross-process SQL runner writes 100 ms FE/BE process-identity
samples there. Each sample records RSS and cumulative user/system CPU time for
the exact launched process. The measurement report checks role identities,
nondecreasing CPU counters and positive RSS, then records each artifact digest,
sampled high-water RSS and first-to-last CPU deltas per role. Sampling may miss
a shorter RSS peak and starts after cluster readiness; CPU deltas also exclude
cold startup. The runner rejects an
existing output path and an unowned or all-in-one cluster.
A timeout or non-zero sample still produces a report but makes the command fail.
Controller RSS is reported in bytes on macOS and KiB on Linux, matching
`wait4(2)` on each platform.
Measurements reject a dirty checkout by default; `--allow-dirty` exists only
for developing the tool, and comparison rejects such reports.

The comparison rejects different normalized commands, workload-file relative
paths or bytes, normalized config paths, dimensions, profiles, toolchains,
platforms, sample counts, RSS units, or sampled FE/BE role sets. It checks
both median and nearest-rank p95 wall time and preserves both raw commands,
config identities, and sampled role RSS/CPU distributions in its output for review.
Controller RSS is
reported separately from wall-time acceptance. Sampled FE/BE CPU is diagnostic;
per-publish CPU and exact peak memory, request counts, metadata sizes, manifest counts, retention
outcomes, and codec budgets remain workload output and must be preserved
alongside this report. V14 additionally requires dedicated scale workloads and
release-profile samples; this functional smoke does not provide them.
