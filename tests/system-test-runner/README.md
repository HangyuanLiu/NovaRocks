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
NOVAROCKS_BUILD_PROFILE=dev-opt cargo run -p novarocks-system-test-runner \
  --locked --profile dev-opt --target-dir "${NOVA_CI_NORMAL_TARGET_DIR:-$PWD/target/normal}" -- --list
```

List the default functional baseline:

```bash
NOVAROCKS_BUILD_PROFILE=dev-opt cargo run -p novarocks-system-test-runner \
  --locked --profile dev-opt --target-dir "${NOVA_CI_NORMAL_TARGET_DIR:-$PWD/target/normal}" -- --list-default
```

The default list uses the same registry classification as a run without
`--only`. Explicit functional stages with external fixtures or higher cost remain
discoverable through `--list` and selectable by exact `--only`, except disabled
scenarios: Paimon connector scenes and all mixed UEA4 catalog-planning/scan-readiness
scenes retain their assets, appear with `DISABLED` in `--list`, and refuse explicit
selection before fixture preparation or role launch. They are suspended pending
detailed Paimon design; the production provider remains registered. 当前阶段不维护
测量基线；`performance` launch profile只选择clean harness配置，不表示性能实验。

Run one scenario against the native 1FE+3BE default:

```bash
normal_target="${NOVA_CI_NORMAL_TARGET_DIR:-$PWD/target/normal}"
control_target="${NOVA_CI_CONTROL_TARGET_DIR:-$PWD/target/test-controls}"
export NOVAROCKS_BUILD_PROFILE=dev-opt
cargo build --workspace --locked --profile dev-opt --target-dir "$normal_target"
cargo build -p novarocks-server --bin novarocks --no-default-features \
  --features jemalloc,test-controls --locked --profile dev-opt --target-dir "$control_target"
cargo run -p novarocks-system-test-runner --locked --profile dev-opt --target-dir "$normal_target" -- \
  --binary "$normal_target/dev-opt/novarocks" \
  --ordinary-binary "$normal_target/dev-opt/novarocks" \
  --test-control-binary "$control_target/dev-opt/novarocks" \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$(mktemp -d)" \
  --cluster-size 3 \
  --timeout-secs 300 \
  --only query-lifecycle/distributed-baseline
```

## 产物与有限授权

普通、控制及兼容性变体使用独立 target，默认 `target/normal`、`target/test-controls`、`target/variants`；CI 的三种 `NOVA_CI_*_TARGET_DIR` 可复用已有 owner 管理的互斥缓存根，运行 artifact root 仍每次独占。构建同时声明 `--profile` 与 `NOVAROCKS_BUILD_PROFILE`，`dev` 映射到 `debug` 目录；不能用目录、feature 或 runner profile 代替实际 FE/BE 自报。[构建 helper](../../tools/ci/lib/build_capabilities.sh) 统一现有 CI 选择，不维护第二份能力目录。

`--binary` 是场景 Primary，`--ordinary-binary` 是普通对照/普通后继，`--test-control-binary` 是具有所需内部能力的产物。场景从本次所选有限动作冻结必要角色需求；缺能力在 spawn 前失败，旧环境、FaultScenario 名称与 feature 编入都不发授权。内部 statement 观察独立授权，观察不触发注入；正常 READY、MySQL、公开 metrics 不需要内部观察权。普通场景可只提供普通产物；上例的 distributed-baseline 同场景追加普通 peer 的三次 public SQL/资源收敛，随后完整执行受控三 statement 的 typed namespace/sequence/attempt 与 Native 断言，两个阶段各自记录实际产物。

兼容性场景的 `Primary/Compatible/OtherIsland` 使用启动前解析的实际选中产物，包括 peer 和 replacement；不能用基础 controls 自动替换用户选中的兼容性变体。CI 明确给 `native-compatibility/*` 选择 controls Primary，仍传独立 ordinary 及 compatible/other-island 路径，普通 workspace binary 不被覆写后恢复。

`--build-info` 不依赖 config/fixture；harness 保留真实自报双流、实际退出和各角色 launch/restart 映射。声明 profile、binary `actual_cfg`、package 局部事实与 null/unknown 分列；最终 LTO/codegen/dependency override/额外 flags 等仍未知，`not-certified-by-self-report` 不构成发布认证。本文的接口说明不宣称 G2 全部完成或验收通过。历史私有 exact/held/HMS 原输入缺失时完整矩阵记 `NOT_RUN`，局部机制、明确授权动作与实际退出证据另列，不能伪造历史 receipt。

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

凭据消费者场景为 `connector/vended-credential-refresh`、
`connector/vended-credential-targeted-deadline`、`connector/vended-credential-late-close`
与 `connector/vended-credential-unreachable`。逐个用准确 `--only` 和
`--cluster-size 3 --timeout-secs 180` 运行。fixture 识别真实 BE principal，
仅在对应刷新请求到达后 hold；late-close 在 BE[0] 使用 test-controls 下
显式授权的 exact-authority trigger。

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
`async-scheduling-pressure` runs four distributed SQL queries, waits for real
Exchange shuffle bytes while one remains active, and checks a small control
receipt; it is a liveness check under ordinary async work, not proof that all
async worker threads can be saturated safely.
`partial-body-deadline` leaves a gRPC request body half open and checks bounded
termination, the `running_deadline` counter, holder release, and a later
control receipt. HTTP/2 may end that half-open stream with a reset, leaving no
readable gRPC status; a fully received unary request has separate status checks.
`registry-contention-control` 在真实 Worker registry mutex 被持有时，使用
runner-owned、test-controls 下显式授权的 rendezvous。它区分已开始且等待
mutex 的 control executor job 与仍排在 ingress 的 job，再验证释放后的
receipt 和 lock-wait 观察。
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

结果owner相关writer/buffer/ACK/取消/helper改动必跑对应场景；range/delete/planning/DOP由Connector/scan领域按provider/cache/range/admission/fixture改动必跑。高成本显式组每周由维护者手工相关领域轮次，记录缺输入与raw FAIL，不扩默认registry选择。周期调度与共享存储尚未交付；G7a/G7b已取消，不作为未来服务承诺。FE statement observation/auth保留，运行级计时collector退出；实际能力/profile见每次产物自报，未运行范围不能据接口存在宣称通过。

普通 Rust 默认测试保持普通构建；full CI 的显式七 control-owner stage 不选择 Server，与 normal workspace tests 分开 target/feature。HMS/MySQL 旧 feature 专属测试另选 owner，不以七包通过代替。维护命令见 [测试治理](../../docs/guides/development/test-governance.md#构建选择与控制授权)。

MV 旧场景及独立 oracle 保留，禁止扩展旧屏障/CALL/directive。7B3 唯一承接非聚合冷恢复、配置/paused、known-committed、Unknown/迟到提交和管理接续；7B4 承接 spine SUM/COUNT、聚合状态、聚合冷恢复与后续刷新；7B5 承接 V/S 身份、发现/CREATE/DROP、REST 路由和实际 materialized S/fallback；7B11 同格式切换承接 `mv_recovery.rs` / `mv_uea7_storage.rs` 的 document graph 与 shared-document-revisions。替代保护实际通过后逐项退出，最后消费者退出再删共享载体；G2 不新建启动期缓存缺失或通用擦除接口。

### MV documents (explicit)

The explicit MV document stages are `mv/document-expiry-channel`,
`mv/document-consistent-read`, and `mv/document-keyed-writes`. They require private
REST/S3/Spark fixtures and native 1FE+3BE. The expiry and consistent-read stages
also require the matching test-controls FE artifact, with finite target/token
authorizations created by the scenario. They are excluded from `--list-default`.
Choose the original deadline before launch; the consistent-read stage performs
300 source commits plus 300 ordinary refreshes and can use `--timeout-secs 1800`.

These stages independently parse the frozen inline wire protocol; they do not
call product document codecs. Every external mutation retains before/request/
HTTP-status/response/after files in a new `document-evidence` subdirectory. Spark
retains its exact SQL, command locators, stdout/stderr and process exit. The
private recording REST proxy retains method/path/query only, forwards request
and response bodies unchanged, and distinguishes ordinary `snapshots=refs`
loads from the Full expiry load. Scenario evidence records inline property and
snapshot-record byte counts and historical interpretation diagnostics. A unit
or registry pass does not replace running these native stages on the integrated
candidate.

### Catalog publication (explicit)

The `catalog-publication/*` scenarios require the local publication Java image
inputs, the normal fixture BOM, and native 1FE+3BE. They are excluded from the
default suite. Select each exact name with a fresh `--artifact-root`:

- `catalog-publication/late-commit-observation` holds an ordinary ALTER after
  server-side requirements validation. SQL must report Unknown while metadata
  remains old and exactly one mutation was forwarded. Releasing the original
  request must yield a real service success and actual exit. A second phase
  holds an already committed request before handler reload, commits successor
  metadata externally, then requires the original SQL success to preserve its
  applied schema and the successor's property.
- `catalog-publication/stream-deadline` streams real responses for table commit,
  create table, namespace and view operations under an explicit 500 ms catalog
  evidence budget. SQL must return Unknown while the slow body still makes
  progress; bodyless drop responses must preserve Applied. Each body owner
  must exit. The outer query/scenario deadline must leave at least 15 seconds
  of margin; the normal production policy remains unchanged.
- `catalog-publication/expiry-concurrency` freezes expiry at the real JDBC
  boundary, adds a tag pointing to an exact candidate snapshot externally,
  observes the original CAS conflict, and requires reprepare to retain that
  ref, snapshot, and manifest-list object. Exact source rows remain readable.
- `catalog-publication/manifest-reprepare` freezes rewrite manifests, appends
  through a separate logical catalog, observes a real CAS conflict, then
  requires successful reprepare to preserve the append snapshot, its exact
  manifest-list object and the independently specified row set.

Each scenario owns a private catalog/warehouse/proxy and retains its original
SQL actor through actual join. Service NDJSON and before/after metadata are
written inside its normal scenario artifacts. Expected row/snapshot/ref facts
come from the scenario inputs and the competitor's real REST result, not the
maintenance candidate planner. Object checks sign independent HEAD requests
against the owned MinIO bucket. These cases do not claim coverage for every
expiry interleaving, all protection roots, or cross-generation coordination;
those have additional provider/application tests and separate acceptance rows.
A before-forward proxy hold cannot substitute for the late-request scene.
