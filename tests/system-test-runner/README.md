# NovaRocks system scenarios

`novarocks-system-tests` is the imperative, process-boundary test frontend.
It owns scenario discovery, actors, bounded deadlines, oracle composition and
failure reporting. `novarocks-cluster-harness` remains the only owner of
1FE+NBE configuration, spawn, readiness, topology, faults, restart, logs and
cleanup.

Follow [test governance](../../docs/guides/development/test-governance.md) for
asset admission, approval snapshots, new exclusive attempt roots, retention and
report interpretation. Pass a fresh root through the existing `--artifact-root`;
the runner's `create_dir_all` does not enforce exclusive ownership. Existing
ignored report directories and the commands below remain supported. Preserve
unresolved failure materials and record the actual output location; a local
path is not a shared evidence handoff.

List all registered scenarios (including explicit stages):

```bash
cargo run -p novarocks-system-test-runner -- --list
```

List the default functional baseline:

```bash
cargo run -p novarocks-system-test-runner -- --list-default
```

The default list uses the same registry classification as a run without
`--only`. Explicit functional stages with external fixtures or higher cost remain
discoverable through `--list` and selectable by exact `--only`. 当前阶段不维护
测量基线；`performance` launch profile只选择clean harness配置，不表示性能实验。

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
the harness runtime directory. The vended credential scenarios and the default
`table-maintenance/target-replacement` scenario start an isolated Iceberg REST
and MinIO fixture and require the prepared fixture inputs and Docker service.
Target replacement uses REST because it deliberately overlaps a detached
OPTIMIZE job with another writer; Hadoop OPTIMIZE waits for completion.
The base config supplies SQLite StateStore and no shared
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

两个按需期限场景为 `result-delivery/producer-wait-query-deadline-separation`
与 `result-delivery/original-query-deadline-cancels-producer-wait`。分别用正常
MySQL session 的 `query_timeout=120` 和 `2` 执行 `SLEEP(35)`，验证独立预期
Boolean true / 原 ERR 1317，再在同一 socket 执行 `SELECT 1`。每例独占
native 1FE+3BE，使用 `--launch-profile performance --timeout-secs 300` 和独立
artifact root；不构建数据 fixture，不参与默认基线。公开 root census、BE
query owner 计数与双零 FE/BE barrier 分别证明实际排空；ERR、客户端关闭和
进程 shutdown 保留为不同事实。35 秒是旧期限错误的复现输入，不是性能门。

`result-delivery/many-small-rows-cross-segment` 验证 200000 小行跨 Native
1 MiB 段时的准确顺序与字节；`result-delivery/large-row-cross-u24` 验证一个
17825860 字节大行跨 MySQL U24 packet（17 个合法 1 MiB 字符串字段）。输入在
`docs/testing/mem-1-m07/inputs/result-delivery-wire-boundary-v4.json` 冻结，客户端
明确声明 frozen profile 的 64 MiB logical packet allowance，独立
Python literal oracle 生成期待摘要。原始 actor 以 64 KiB scratch 流式校验
packet sequence、列数、行数、length-encoded 字段及数据摘要，不积累整行。
每项还要求真实 BE Task 创建，并保留前后两次连续零值的 FE window/governance
和 BE reservation/ingress 快照。这里只证明 wire 正确性与公开资源收敛；closing
满池、精确 alias 退出、全部 W=2 容量矩阵和 transport 测量仍是独立验收。

`result-delivery/producer-exit-context-retention` 在严格校验 metadata 后暂停
客户端读行。客户端在 connect 前申请 4 KiB receive buffer，并记录 OS 实际值；
不修改 server profile。一个合法 1 MiB 字符串形成 S+8 Native 行字节，要求
连续两次观测同一个仍由 context 持有的 root：两个 Data position、End 已发布、
producer guard 已退出且 task 已为退休记录、最终 End 尚未 ACK。恢复后核对
独立冻结 schema/packet/行字节摘要并等待公开 owner 收敛。输入为
`inputs/root-context-retention-freeze-v1.json`。新增 BE ownership 指标只在
1024 个 context/root 扫描位置内作非阻塞 census；锁忙或覆盖不完整时明确
不可用并省略 ownership family。它不包含移出 context 的固定 backing 或其它
Arc 尾部，归零不证明 allocator dealloc，也不代替 ACK-only/ClosingRow 满池门。

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

### 内存归属观测

默认场景 `memory-attribution/observation-families` 启动原生 1FE+3BE，验证分布式查询及连接退出取消前后的每个 BE `/metrics`。证据写入场景目录的 `memory-attribution.json`，关联独立 PID 与 HTTP endpoint；两个进程尺寸段、固定归属分类、记录状态、故障与批量采样族均须存在。S1 未接生产 lane，query/residual/service 记录和事实为零，16 条 immortal unattributed 记录单独存在；unattributed 字节大于零，生命周期与孤儿故障为零。签名的对账及账本盲区为独立采样，不要求在并发 scrape 期间瞬时归零。

## 迁入的显式功能保护与维护频率

下列场景独占原生1FE+3BE，以有限输入和独立行袋/协议预期检验性质；timeout只停止挂死，不是性能门。选择时用准确selector，每次独占artifact root；`--launch-profile performance`是clean配置，原有需要fault的场景继续使用自己的fault-scenario要求。

| 性质 | 原生selector | 输入/实际退出 |
|---|---|---|
| 慢读与完全不读客户端 | `result-delivery/slow-client-progress`、`result-delivery/unread-client-progress` | 正常MySQL、冻结行/字节预期，进展/取消与owner收敛分开 |
| 独立并发结果 | `result-delivery/concurrent-results-drain` | 有限actors，失败取消其他live peers、实际join，保留primary及cleanup错误 |
| range/短查询 | `uea4/iceberg-range-functional`、`connector/iceberg-short-range-concurrent` | 多文件A4及独立4096行单文件manifest；准确行序/算术、LIMIT后退出、并发结果 |
| 同provider排队进展 | `catalog/provider-read-queued-progress` | held真实Parquet读、同provider waiting，释放后两个查询正确并退出 |
| planning责任与取消 | `uea4/catalog-planning-responsibility`、`uea4/catalog-planning-no-fe-quota`、`uea4/iceberg-held-read-cancellation`、`uea4/catalog-planning-smoke` | Iceberg/Paimon真实READY；FE/BE责任、held请求精确终态及公开收敛 |
| warm异质delete | `connector/iceberg-delete-warm-concurrent` | typed独立Java corpus receipt，固定旧/latest snapshot及私有mutation，有限并发和真实退出 |
| cold delete | `connector/iceberg-delete-applicability` | 完整冻结native manifest包含corpus/anomaly/scale；真实多文件与split/page-source退出 |

range/planning输入准备与校验见保留的 [range fixture](../benchmarks/uea4a2/README.md) 和 [planning fixture](../benchmarks/uea4a4/README.md)。原目录名不赋予测量责任。正常held转发仍要求真正forward完成；取消路径按同request精确connection的上游终态观察，不能把其他连接关闭或ERR当退出。

结果owner相关writer/buffer/ACK/取消/helper改动必跑对应场景；range/delete/planning/DOP由Connector/scan领域按provider/cache/range/admission/fixture改动必跑。高成本显式组每周由维护者手工相关领域轮次，记录缺输入与raw FAIL，不扩默认registry选择。周期调度与共享存储尚未交付；G7a/G7b已取消，不作为未来服务承诺。FE statement observation/auth保留，运行级计时collector退出；G2发布能力/profile自报及门控仍未知。
