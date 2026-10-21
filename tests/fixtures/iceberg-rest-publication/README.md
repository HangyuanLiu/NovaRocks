# UEA-7 Iceberg REST publication fixture

This test-only image extends the pinned
`apache/iceberg-rest-fixture:1.10.1` JDBC catalog. It does not add a private
catalog protocol and it is not a production configuration.

The custom `JdbcCatalog` wraps the real `TableOperations` returned by
`JdbcCatalog.newTableOps`. A one-shot hold is entered at
`TableOperations.commit(base, updated)`, before the delegated JDBC commit.
For the REST server's standard table-commit path this point is reached only
after the request requirements have been checked against that request's base
and its updates have produced `updated`. The release continues the exact same
`base` and `updated` values; it never refreshes or substitutes them.

This location matters. The SQL runner's existing transparent proxy can pause a
request before forwarding it or discard a response after downstream success,
but it cannot prove the interval between server-side requirement validation and
the persistent JDBC compare-and-swap.

## Local prerequisites

The build is deliberately offline with respect to container registries. These
exact images must already exist locally:

- `apache/iceberg-rest-fixture:1.10.1`
- `apache/spark:3.5.5-java17`

Build and run the low-level V07 oracle:

```bash
docker build --pull=false \
  -t novarocks/iceberg-rest-publication-fixture:test \
  tests/fixtures/iceberg-rest-publication

tests/fixtures/iceberg-rest-publication/run-v07.sh
```

Set `UEA7_SKIP_BUILD=1` to reuse the already built image. The script creates one
uniquely named container, publishes random loopback ports, uses an isolated
SQLite catalog and local warehouse, and removes the container on every exit.
It never alters the versioned shared catalog project.

For plan-level V07 evidence, bind the shared fixture and set
`UEA7_USE_SHARED_MINIO=1`. The fixture then joins only the shared Docker
catalog network, reads one resolved publication, creates a unique bucket through
its exact object-store container ID, delegates
the tracing wrapper to the real `S3FileIO`, and removes that exact bucket on
exit. It still owns a private SQLite catalog, endpoint, namespace, and
container; it never sends catalog requests to the shared REST service.

```bash
source docker/iceberg-rest/runtime/current/env.sh
UEA7_USE_SHARED_MINIO=1 \
  tests/fixtures/iceberg-rest-publication/run-v07.sh
```

Set `UEA7_ARTIFACT_DIR` to a new or empty absolute directory to retain the
bounded NDJSON trace, exact mutation/object-I/O counters, request/response
bodies, container log, and a manifest binding the evidence to the Git HEAD and
exact image identities.

The directory is checked before fixture actions. A failed run captures bounded
live trace/metrics diagnostics before cleanup, retains the available work files,
container log and identity, and records the original exit status separately from
the container cleanup status. INT and TERM retain exit statuses 130 and 143.
Diagnostic or cleanup failures do not replace the original failure. If copying
the work files fails, the script preserves their original directory and prints
its exact path. Missing diagnostics remain unavailable; a cleanup status alone
does not prove that every service or HTTP actor has exited.

For the V11 main-ref requirement oracle, run `run-v11.sh` with the same
environment and artifact options. It includes the V07 scenarios, then tests
four format-v3 tables. Two start without a main snapshot; two first publish a
seed snapshot. Each receives two distinct `add-snapshot`/`set-snapshot-ref`
requests whose original `assert-ref-snapshot-id(main, expected)` condition is
frozen before the service hold. Both absent and non-null expected main values
run in both commit orders. The losing response must name its original main
requirement, and a request that loses the JDBC compare-and-swap must refresh
without delegating a second commit. The successful snapshot ID is read back
from REST. This proves the standard REST service's exact main condition and
retry behavior; the SQL suite separately verifies NovaRocks's D/L/P/C graph
and one target mutation per publication.

```bash
source docker/iceberg-rest/runtime/current/env.sh
UEA7_USE_SHARED_MINIO=1 \
  tests/fixtures/iceberg-rest-publication/run-v11.sh
```

The explicit `mv-publication-v11` SQL suite uses the same checked-in hook in a
runner-owned private REST and MinIO project. The runner builds an image tagged
with the hook source digest, starts the project with the publication-hook profile, and
records the live image ID. One case holds an actual NovaRocks MV target commit
after service-side requirement validation while Spark advances `main`. Three
cases hold a frozen external request at that same service boundary while the
MV publishes a new P, through full, incremental append, and metadata-only
refreshes. All require
the original held request to encounter a JDBC conflict and prevent a second
delegation after metadata refresh.

```bash
source docker/iceberg-rest/runtime/current/env.sh
NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks" \
  cargo run --locked --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite mv-publication-v11 \
  --cluster-mode cross-process --cluster-size 3 --mode verify \
  --query-timeout 300 -j 1
```

## Evidence

The control endpoint keeps at most 512 NDJSON trace events. Each held commit
records:

1. `requirements-passed-before-persistent-commit`;
2. `hold-reached` before any delegated commit;
3. `hold-released`;
4. one `delegate-commit-start` using the recorded base and updated identities;
5. `delegate-commit-success` or `delegate-commit-conflict`.

The V07 script covers both orderings from the same schema base. When the newer
request commits first, releasing the old request must produce a JDBC base
conflict; the REST handler then refreshes and rejects the unchanged original
`assert-current-schema-id` requirement. When the held request commits first,
the frozen competing request is rejected by that same requirement. A third
case terminates the waiting HTTP client and proves that the server-owned held
request can still be released and committed.

The configured test-only `TracingFileIO` delegates to the image's real
`HadoopFileIO` in local smoke mode and `S3FileIO` in shared-MinIO evidence mode.
It counts actual input/output stream opens and bytes as they occur. The
`/metrics` endpoint also counts every delegated table mutation by outcome. The
script requires the exact seven mutations from its three table creates and four
schema commit attempts, including one real JDBC conflict, and requires positive
object-read and object-write counters.

`run-v11.sh` additionally requires nineteen delegated attempts in total:
sixteen successes, three real JDBC conflicts, and zero unclassified failures.
In the two old-first cases the later frozen request is rejected by its main
requirement before delegation; both new-first cases retry only after the real
JDBC conflict and then reject the original requirement.

## Publication lifecycle observations

`tests/cluster-harness/src/publication.rs` owns the private Java REST service,
transparent proxy, their cancellation, and actual joins for native system
scenarios. SQL suites consume the same `publication_catalog` and
`publication_service` owners; the original MV conflict oracle still requires
its original six ordered events, exact base/updated identity, one competitor,
and no second stale delegation.

The service accepts `POST /arm?table=namespace.table&stage=...`:

- `before-persistent-commit` is the existing requirements-passed hold.
- `after-persistent-commit` holds after the real delegate succeeds and before
  `TableOperations.commit` returns to the REST handler's metadata reload.

A post-commit arm is claimed by exactly one request before persistence. Its
trace records `delegate-commit-success`,
`persistent-commit-before-handler-reload`, and `hold-reached`. Releasing it
allows the handler to load a successor committed by a competitor. Every held
request separately records `commit-method-exited`. `Control::wait_for_original_exit`
returns the actual original success/conflict/failure; it does not prescribe a
conflict. A release ACK alone is not exit evidence. Owner cleanup may pass
`cancel=true` to disarm an unused hold; ordinary release before the boundary
continues to return 409.

The shared proxy's `stream-response` fault forwards the real request once,
then streams each byte of the real successful body every 50 ms. Each interval
is below the ordinary read idle timeout. The proxy records stream start,
chunks, and actual body drop. A client deadline, explicit release, or fixture
shutdown ends the stream. Bodyless success statuses retain their decisive
status. This fault never invents catalog state or a successful mutation.

These facilities are test-only. Quick proxy/control tests belong to the normal
cluster-harness publication unit filter; Java/native cases are explicit and
run when publication or maintenance semantics change and at final acceptance.
The image uses local pinned inputs and `--pull=false`. Missing input is a
fixture precondition failure. Keep raw failed attempts, trace, image identity,
container logs, SQL error/result, release result and actual-exit evidence;
retries use a fresh artifact directory and preserve the original failure.

The native `catalog-publication/expiry-concurrency` scenario uses fresh tables
for each property. It checks an external move of an existing ref (actual REST
409 and a fresh successful attempt), a new branch followed by an append (exact
branch snapshot and manifest-list plus branch reads), and an externally added
tag removed by the frozen expiry (Applied error containing the lost tag,
snapshot and manifest-list). For the lost tag it independently registers the
pre-expiry metadata under a source name, then reads that exact old snapshot
after GC. The seed ends with a full overwrite so this first snapshot's data
is absent from current main. This other catalog entry is invisible to the expiry's protection
scan: its successful read and the exact manifest-list HEAD prove retained
source files, rather than inferring preservation from current main row counts.

The same scenario observes a local ref in `SHOW FULL PROCESSLIST` while a
same-catalog expiry is held, requires zero ref dispatch before conclusion,
and requires rejection of the removed snapshot afterwards. A separate short
budget phase observes expiry Unknown, successfully creates a local tag in
that same logical catalog, then releases the original request and observes
the late expiry remove it. Files must remain readable. This phase documents
the explicit limit of local serialization after Unknown; it does not claim
that the lock prevents the late application window. Cross-generation lock
identity, cancellation and different-table progress are tested by the
catalog lifecycle owner, not inferred from this same-generation scenario.

For every slow-body mutation, `stream-deadline` preserves the complete arm
trace (`sequence`, `arm_id`, `event`), SQL outcome and before/after actual HTTP
counters before asserting one forwarded mutation. Stream exit verification
still runs if saving the initial trace fails. Native service-held actors
aggregate primary phase, release, SQL join, exact original Java wrapper exit
and trace-write failures, and retain each result in `*-actors.json`. A release
failure never skips joining the SQL actor. These are executable assertions;
changing them does not constitute a same-SHA native acceptance receipt. Run
each of the four explicit scenario names individually with `--only` and a
fresh artifact root when collecting final evidence.

共享 MinIO 模式须事先供给 fixture image，并设置 `UEA7_SKIP_BUILD=1`；运行期不构建或下载替代输入。它继承原 `NOVA_FIXTURE_RUN_CONTEXT`，按实际 image Config argv 启动容器内期限 guard，迟到启动拒绝，原期限到达后 PID 1 自主退出。共享数据只写原 scratch scope，artifact 记录实际 container image ID、原 context、machine version/generation；root 凭据只写临时 0600 env-file，启动成功即删除，失败 evidence 不复制它。默认 local-Hadoop 模式仍沿原独立 owner。
