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
empty-output incremental MV refresh after an initial publication. It has no fixed manifest
count and does not satisfy the plan's V14 scale or cost gate. This SQL case does
not exist in the fixed B0 checkout at `355bd3164`, so this smoke has no B0
comparison. Source this worktree's runtime environment and set `NOVAROCKS_BIN`
to its built `dev-opt` server first.

For V14 scale preparation, `generate-scale-sql.py` creates a separate native
SQL-runner case for each requested starting count (0, 1, 100, or 1000) and
publication count. Each positive source append is followed by one MV refresh.
`--publication-mode unchanged` leaves the source snapshot fixed and exercises
the actual `MetadataOnly` publication. At zero starting manifests it seeds one
filtered-out source row and publishes it before the measured window, so a P
already exists. The historical default `filtered-source` inserts a negative
source row before every measured refresh; this is an **empty-output incremental
publication**, not the `MetadataOnly` path. Before the measured publications, Spark reads
the target Iceberg `manifests` metadata table and asserts the **actual** count;
afterward it records the actual count without assuming it remains fixed. The
case also checks the final MV row count. Use a unique output directory and
observation file per run. The SQL runner's `mv-storage-contract` suite supplies
an isolated REST Catalog and MinIO fixture; run it in native 1FE+3BE mode.

```bash
source docker/iceberg-rest/runtime/current/env.sh
export NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks"
python3 tools/benchmark/mv-persistence/generate-scale-sql.py \
  --manifests 100 --metadata-publications 1000 --publication-mode unchanged \
  --output /tmp/uea7-scale-100/sql/mv_scale_generated.sql
mkdir -p /tmp/uea7-scale-100/result
UEA7_SCALE_OBSERVATION_FILE=/tmp/uea7-scale-100/manifest-counts.txt \
  cargo run --locked --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite mv-storage-contract \
  --sql-dir /tmp/uea7-scale-100/sql --result-dir /tmp/uea7-scale-100/result \
  --cluster-mode cross-process --cluster-size 3 --mode verify -j 1
```

The observation file is created once and contains
`MV_MANIFEST_COUNT_BEFORE` and `MV_MANIFEST_COUNT_AFTER`; the command rejects
an existing file. The following historical measurements all used the
`filtered-source` mode. A small native probe observed 1 initial manifest becoming 3
after two filtered-out incremental publications, and 0 becoming 1. These publications
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

The default `filtered-source` case does not collect per-publish request counts,
manifest-list bytes, or metadata file sizes. The SQL runner can collect
FE/BE CPU and RSS with `--process-resource-output`, but these observations
alone are not V14 acceptance evidence or release-profile B0 comparisons.

Add `--measure-rest-traffic` when generating a case to bracket each
measured MV refresh with cumulative snapshots from the runner's transparent
REST Catalog proxy. Set `UEA7_SCALE_REST_TRAFFIC_FILE` to a new JSONL artifact
path before running the native SQL suite. The proxy counts requests it actually
forwards to the real catalog, including failures and retries, with HTTP method,
proxy response status, request-body bytes, and downloaded catalog response-body
bytes. It also records the duration of each forwarded target table-commit REST
roundtrip through receipt of the response body; the recorder requires exactly
one such request per measured refresh and writes that window's duration in
`table_commit_roundtrip_nanos`. This is the externally observed catalog commit
request latency, including network and response transfer, not isolated JDBC
CAS time. Proxy-generated error bodies are excluded. Fixture control calls and
known-before-dispatch refusals are excluded. The generated case writes one
delta per refresh, with zero-based `index`; an incomplete refresh leaves a
`.pending` snapshot for diagnosis. The synchronous SQL runner step supplies
refresh latency in its log. This measurement does **not** count S3 requests,
manifest-list object bytes, metadata file sizes, or background traffic outside
the bracket. A private REST/MinIO fixture limits unrelated work, but attribution
still requires checking each measured window.

```bash
python3 tools/benchmark/mv-persistence/generate-scale-sql.py \
  --manifests 0 --metadata-publications 1000 --publication-mode filtered-source \
  --measure-rest-traffic \
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

For per-publication S3 request counts, set `UEA7_SCALE_S3_TRACE_FILE` to a new
JSONL path before starting the same native SQL runner, and generate the SQL
case with both `--measure-rest-traffic` and `--measure-s3-trace`. The runner
requires a host `mc` command, starts an S3-only MinIO admin trace against its
**private** MinIO fixture, proves startup and shutdown with unique read-only
barrier requests, and reaps its own trace process before removing that
fixture. Each refresh also gets two signed read-only S3 marker requests. Their
timestamps come from the same MinIO trace as the measured traffic, so host
and container clock offsets cannot move an event into another refresh. After
the suite, assign the raw trace events to those markers:

```bash
export UEA7_SCALE_S3_TRACE_FILE=/tmp/uea7-scale-rest/s3-trace.jsonl
# Run the native 1FE+3BE SQL command above with REST traffic enabled.
python3 tools/benchmark/mv-persistence/summarize-s3-trace.py \
  --traffic "$UEA7_SCALE_REST_TRAFFIC_FILE" \
  --trace "$UEA7_SCALE_S3_TRACE_FILE" \
  --objects "${UEA7_SCALE_S3_TRACE_FILE}.objects.jsonl" \
  --output /tmp/uea7-scale-rest/s3-summary.json
```

The summary rejects missing trace barriers and overlapping markers, excludes
the marker requests, records S3 request counts by API/status and object-path
class, and preserves SHA-256
digests of its inputs. `wire_rx_bytes` and `wire_tx_bytes` are MinIO trace
transport counts; they are **not** exact manifest-list or metadata object sizes.
After stopping the trace and before removing its private MinIO fixture, the
runner also writes a recursive `mc ls --json` listing to
`$UEA7_SCALE_S3_TRACE_FILE.objects.jsonl`. The optional `--objects` argument
joins each immutable manifest-list and metadata JSON `PutObject` path within a
refresh window to that final S3 object listing, rejects missing or reused
paths, and reports `written_object_bytes` and `written_object_counts` by kind.
The listing is captured after every measured window, so its own list requests
cannot change the per-publication S3 counts. It measures bytes of **newly
written objects**, not bytes transferred by reads or total retained storage.
An initial 0-manifest, one-publication native smoke passed 13/13 steps with
one 15-request S3 window and reported **1,800 exact manifest-list bytes** and
**14,068 exact metadata JSON bytes** for its published objects. Evidence:
`/tmp/uea7-object-sizes-smoke/`.

The first 100-starting-manifest, 1000-publication attempt stopped after 504
complete windows because the live `record-rest-traffic.py` was edited to
require a new counter while that run's already-started proxy binary still
served the old counter schema. This is a measurement-tool mismatch, not an MV
refresh failure; its partial artifacts are diagnostic only in
`/tmp/uea7-true-metadata-hundred-thousand/`. After making the proxy and
recorder changes together and freezing them for the run, a new native
0-manifest, one-publication smoke passed 13/13 steps. It reported exactly one
table-commit REST request with a **12,905,416 ns** proxy roundtrip, plus a
15-request S3 window and exact new-object sizes. Evidence:
`/tmp/uea7-commit-timing-smoke/`.
One native 0-manifest, two-publication **filtered-source incremental** smoke
passed 16/16 steps; its two refresh
windows contained **22 and 52 S3 requests** in the raw trace, including
manifest-list and metadata paths, while Spark observed 0→1 manifests. Evidence:
`/tmp/uea7-s3-marker-smoke/{rest-traffic.jsonl,s3-trace.jsonl,s3-summary.json}`.
At 100 starting manifests, the same filtered-source mode passed 216/216 steps
and exposed **1554/1561 S3 requests** in its two windows, mostly reads of
source and existing target data/manifest files; Spark observed 100→102
manifests. Evidence: `/tmp/uea7-s3-marker-hundred/`. This is not the V14
metadata-only cost curve.

For the actual metadata-only path, generate with `--publication-mode unchanged`
and both traffic flags. Native 1FE+3BE short runs passed **16/16** steps from
0 starting manifests and **214/214** from 100. The REST document-graph oracle
confirmed `metadata-only-last=true`, three and 102 exact P attachments, and
one target table commit per publication. Each of the four measured refreshes
used **8 REST requests and 15 S3 requests**; the manifest counts moved 0→2
and 100→102. REST response bodies were 92,042/115,911 bytes at zero and
3,143,325/3,165,656 bytes at 100. Evidence:
`/tmp/uea7-true-metadata-{smoke,hundred}/`. These short dev-opt runs establish
the technique and measurement path; the longer zero-starting run below adds
one scale point, while the full V14 matrix and cost comparison remain open.

One clean **true metadata-only** run with zero starting manifests and 1000
unchanged-source publications passed **3010/3010 SQL steps** in 288.10 seconds
on native 1FE+3BE, with no retries. Spark observed target manifests **0→1000**.
All 1000 complete refresh windows forwarded exactly **8 REST requests**
(7 GET, 1 POST) and **15 S3 requests** (10 GetObject, 5 PutObject); the S3
trace classified two requests per window as manifest-list paths. REST response
bodies grew from **92,236** to **22,466,084** bytes per window (median
11,360,345; nearest-rank p95 21,354,844). Synchronous SQL `REFRESH` step times
had median **0.18s**, nearest-rank p95 **0.31s**, and maximum **0.33s**. These
rounded runner step times include more than the target commit and cannot be
used as isolated commit latency. The runner collected **2556/2556 valid**
samples per FE/BE role: cumulative FE CPU **94.47s**, BE CPU
**0.59/0.59/0.57s**, and RSS high water FE **204,029,952**, BE
**46,432,256/51,888,128/47,988,736** bytes. Evidence is in
`/tmp/uea7-true-metadata-thousand/` (`run.log`, `manifest-counts.txt`,
`rest-traffic.jsonl`, `s3-trace.jsonl`, `s3-summary.json`, and
`resources.json`). This is one dev-opt candidate run with zero starting
manifests. It does not supply exact manifest-list or metadata object bytes,
release-profile repetition, or a matching B0 comparison.

The analogous one-starting-manifest candidate run passed **3010/3010 steps**
in 304.85 seconds, and Spark observed **1→1001** manifests. All 1000 REST
windows had 8 requests. The S3 trace had 15 requests in 999 windows and 19
in window 859: four read-only HeadObject probes issued by the investigator
landed inside that window. This run is valid as a functional scale probe, but
its S3 request series is not a clean cost sample. Evidence is in
`/tmp/uea7-true-metadata-one-thousand/`. A live S3 HEAD also showed that the
MinIO trace `size` field differs from the object's `Content-Length`; the trace
field must not be used for exact manifest-list or metadata object sizes.

MinIO Prometheus snapshots in a separate diagnostic probe lagged real writes
and its `incoming_requests` value decreased, so they are not used as a
per-publication counter.

```bash
tools/benchmark/mv-persistence/measure-command.py \
  --label filtered-source-incremental-smoke \
  --role candidate \
  --profile dev-opt \
  --workload-file tests/sql/correctness/mv-storage-contract/sql/mv_storage_contract_metadata_only_publication.sql \
  --workload-file tests/sql/correctness/mv-storage-contract/suite.toml \
  --config-file "$NOVAROCKS_SQL_TEST_CONFIG" \
  --samples 7 \
  --warmups 1 \
  --dimension topology=1fe+3be \
  --dimension case=empty-output-incremental-one-filtered-refresh \
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
