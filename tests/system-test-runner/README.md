# NovaRocks system scenarios

`novarocks-system-tests` is the imperative, process-boundary test frontend.
It owns scenario discovery, actors, bounded deadlines, oracle composition and
failure reporting. `novarocks-cluster-harness` remains the only owner of
1FE+NBE configuration, spawn, readiness, topology, faults, restart, logs and
cleanup.

List all registered scenarios (including explicit stages):

```bash
cargo run -p novarocks-system-test-runner -- --list
```

List the default functional baseline:

```bash
cargo run -p novarocks-system-test-runner -- --list-default
```

The default list uses the same registry classification as a run without
`--only`. Explicit stages, including performance baselines and external
fixtures, remain discoverable through `--list` and selectable by exact `--only`.

Run one scenario against the native 1FE+3BE default:

```bash
cargo build --workspace --profile dev-opt
cargo run -p novarocks-system-test-runner --profile dev-opt -- \
  --binary target/dev-opt/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$(mktemp -d)" \
  --cluster-size 3 \
  --timeout-secs 300 \
  --only query-lifecycle/mysql-disconnect
```

Most scenarios build their Iceberg warehouse on the local filesystem under
the harness runtime directory. The vended credential scenarios start an
isolated Iceberg REST and MinIO fixture and require the fixture image and
Docker service. The base config supplies SQLite StateStore and no shared
`[connector.object_store]` credentials. The generated
`$NOVAROCKS_FE_CONFIG` / `$NOVAROCKS_BE_CONFIG` pair also works
when started through the exact `--role all-in-one --fe-config ... --be-config
...` command; it adds object-store settings that local-filesystem scenarios
do not read.

The consumer credential acceptance cases are
`connector/vended-credential-refresh`,
`connector/vended-credential-targeted-deadline`,
`connector/vended-credential-late-close`, and
`connector/vended-credential-unreachable`. Run each with an exact `--only`
filter and `--cluster-size 3 --timeout-secs 180`. The fixture identifies the
BE principal and holds its refresh only after that request arrives. The
late-close case enables a debug-only, exact-authority trigger on BE[0].

Scenarios run sequentially. On failure the runner prints action history,
process diagnostics, the retained runtime/log directory and an exact rerun
command. Successful scenarios explicitly stop their own process group and
remove the generated runtime directory.

## In CI

`tools/ci/local-full-ci.sh` runs this registry as its own stable stage,
between the server binary smoke and the SQL suites. The stage discovers
default scenarios through `--list-default` and runs each one with a single `--only`
invocation, so every scenario gets an independent summary row, log and
artifact directory. It selects and reports only — `tools/ci/lib/system_scenarios.sh`
holds no cluster lifecycle of its own.

The same `query-lifecycle/distributed-baseline` scenario is reused by the
FoundationDB and MySQL StateStore provider gates as a feature-binary
coexistence smoke. Read it narrowly: it proves a feature-enabled binary still
completes a standard native topology, query and cleanup, not that any query
used that provider.

The `native-ingress/*` scenarios require the real 1FE+3BE launch. They send
authenticated raw gRPC requests to a BE to check header-stage refusal,
pre-prost resource checks, and the ordinary/control message boundaries. The
current-gate case reads the BE management `/metrics?type=json` surface before,
during, and after a runner-held ordinary Worker closure, so an available gate
can be distinguished from an unknown, unimplemented source. The explicit
`blocking-saturation-control` case fills the ordinary blocking pool and checks
that a small Cancel still completes while a ninth ordinary request waits.
`resident-envelope-calibration` records coarse BE process RSS before, during,
and after one legal 48 MiB outer request plus seven small-body closures in
`process-resources.json`. Its padding is a legal protobuf unknown field, not a
retained FrozenFragment carrier; it does not establish a hard RSS bound or a
mixed ordinary/control peak.
`async-scheduling-pressure` runs four distributed SQL queries, waits for real
Exchange shuffle bytes while one remains active, and checks a small control
receipt; it is a liveness check under ordinary async work, not proof that all
async worker threads can be saturated safely.
`partial-body-deadline` leaves a gRPC request body half open and checks bounded
termination, the `running_deadline` counter, holder release, and a later
control receipt. HTTP/2 may end that half-open stream with a reset, leaving no
readable gRPC status; a fully received unary request has separate status checks.
`registry-contention-control` uses a runner-owned, debug-only rendezvous while
the Worker registry mutex is held. It distinguishes an already-started control
executor job waiting for that mutex from a control job still queued at ingress,
then checks the receipt and lock-wait observation after release.
Each selected case records its individual probes in `scenario-evidence.json`;
the scenario must execute at least one probe to count as passed. These are
correctness and coarse regression checks, not throughput benchmarks.

The `native-creation/*` scenarios also require the real 1FE+3BE launch and
check frozen task creation across the process boundary. An Accepted receipt
proves Worker ownership; Installed is a separate runtime-installation fact.
The `accepted-preparation-control-races` case checks preparation/control races
and authenticated covered-observation refusals. Native subscriptions require
nonzero generations; cursor-only requests cannot enter the production stream.
Preparation count, byte and per-context position charges remain until the job
actually exits. FE deployment positions remain occupied after Accepted or an
unknown RPC outcome, and its window W must fit the exact backend's advertised
per-context preparation capacity P. These are scenario and implementation
contracts; a listed scenario does not establish a passing acceptance gate.
`frozen-replay-and-membership` sends authenticated raw creates to one BE: a
legal create, the identical request, and the same task identity with every
body fact changed. Both replays must retain the winner's exact entity and
return its current monotonic status, with no second apply marker and no lease
renewal. Creates under another
frontend process, another attempt or another backend's identity must read no
receipt. An initial domain naming an edge the descriptor never froze fails
preparation after Accepted; that spent identity remains retained, so a later
legal body cannot replace it. A replay after the context is released must
apply nothing.
`creation-payload-lifetime` reads the FE task-creation gauges, which fall only
when a payload's last holder drops it. An answered create must release its FE
replay payload while its statement still runs, and the static plans must stay until
the statement ends. With `create-task-ack-drop` armed, the lost
acknowledgement must be resent as the same frozen create and answered by
identity; every task must be priced, frozen and applied exactly once. A
cancelled statement must release everything it froze. BE preparation input and
its P/count/bytes charge remain owned until the actual preparation job exits;
these FE gauges do not prove BE budget release.
`fixed-plan-recovery` runs one delayed read cleanly and once with its admitted
BE killed before any row is read. The recovered run must complete on attempt
2 and freeze exactly as many static plans as the clean run, so the recovery
attempt encoded none of its own. A full per-target transport window cannot be
produced at the default task transport budget without a capacity probe. That
cross-target admission is therefore checked by the frontend's real admission
pass and transport supervisor composition tests, not by these scenarios.

### JSON value membership

Three default scenarios drive the JSON `IN` / `NOT IN` value-subquery owner
(`novarocks/execution/src/exec/operators/membership`) on native 1FE+3BE. Each
creates its own local Hadoop Iceberg warehouse with persisted JSON columns: a
60 000-row probe in three files (two object key orders, an array shape and an
SQL NULL every 97th row) and a nine-row RHS in two files whose matches are
split across both files and which holds one SQL NULL. Every expected value is
derived in the scenario from those fixed inputs by the three-valued membership
table; nothing is recorded. Run each one alone with an exact `--only`, for
example:

```bash
target/dev-opt/novarocks-system-tests --binary target/dev-opt/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$(mktemp -d)" --cluster-size 3 --timeout-secs 300 \
  --only query-lifecycle/json-membership-full-eos
```

- `query-lifecycle/json-membership-full-eos` checks the RHS with its SQL NULL,
  without it, and with a filter that selects nothing, each as `IN` and
  `NOT IN`. The per-row form compares all 60 000 probe rows; the grouped form
  compares count, sum and sum of squares per result and must show a
  `MEMBERSHIP` fed by a `BROADCAST EXCHANGE`, with at least two stages placed
  on every backend. Iceberg scans place one task per backend, so the two RHS
  files leave at least one of the three RHS producers without a split: it
  contributes only its EOS. Each statement must be a single attempt whose
  established contexts all released.
- `query-lifecycle/json-membership-cancel` sleeps 60 seconds per RHS row, so
  every producer with a split stays short of EOS while each broadcast-fed
  probe task waits on the `Building` RHS. Once every created task of the
  attempt has installed, `KILL QUERY` goes through the harness control
  session. The statement must end with MySQL 1317 before any producer could
  have finished, every established context must record
  `NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED` and
  `NOVAROCKS_TASK_CONTEXT_TERMINATION_COMPLETED` for that execution, the
  resource oracle must converge, and the next statement on the same
  connection must return its exact groups.
- `query-lifecycle/json-membership-capacity` freezes the existing
  `/*+ SET_VAR(query_mem_limit=33554432) */` hint, which installs that limit
  on each backend's query memory tracker. It adds a 400 000-row RHS in eight
  files whose values each carry 150 bytes, so one RHS copy retains at least
  60 000 000 bytes. Under the same limit a streaming read of every large-RHS
  value and the grouped membership over the small RHS must succeed first.
  The grouped membership over the large RHS must then fail with MySQL 1105
  carrying the `CAPACITY_REFUSED` task-failure category on its only attempt;
  every established context must leave `Active`, resources must converge,
  and the next statement on the same connection must return its exact groups.
  The case proves the typed refusal and its release under a limit the
  streaming path fits; it does not attribute which allocation crossed it.

### 内存归属观测

默认场景 `memory-attribution/observation-families` 启动原生 1FE+3BE，验证分布式查询及连接退出取消前后的每个 BE `/metrics`。证据写入场景目录的 `memory-attribution.json`，关联独立 PID 与 HTTP endpoint；两个进程尺寸段、固定归属分类、记录状态、故障与批量采样族均须存在。S1 未接生产 lane，query/residual/service 记录和事实为零，16 条 immortal unattributed 记录单独存在；unattributed 字节大于零，生命周期与孤儿故障为零。签名的对账及账本盲区为独立采样，不要求在并发 scrape 期间瞬时归零。

`mv/recursive-type-restart` uses a private REST/S3/Spark publication and native
1FE+3BE. Run it explicitly with `--cluster-size 3 --timeout-secs 1200`.
It freezes SDK schema UUID/IDs, required children, actual Parquet field IDs and
complete ordered bags checked independently by SDK and Spark. It proves native
lake-document loading, wipes the MV Accelerator, replaces only FE, and reads the
existing result before any refresh. D/L/P/E and exact provider bindings must
remain equal. The subsequent real source DV plus additions and FULL are checked
against complete independent endpoint bags; FULL also requires zero deletes and
exact summary totals. A per-job Spark container has an exact ownership token,
image and container identity, bounded execution, and an independently bounded
cleanup receipt. Logs and stage receipts are retained under `recursive-spark`.
Physical dictionary/plain page evidence belongs to the separate compatibility
SQL cases; native result values are compared across restart, while SDK/Spark
supply the independent content oracle.
