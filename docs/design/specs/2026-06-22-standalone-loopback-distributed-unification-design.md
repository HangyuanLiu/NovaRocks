# Standalone all-in-one 走 loopback 分布式执行统一设计

日期：2026-06-22
状态：Draft，等待评审
范围：NovaRocks `standalone-server --role all-in-one|fe|be` 这套 standalone cluster，不包含 StarRocks FE 兼容入口。

---

## 1. 背景

当前 standalone cluster 已经具备 `role=fe` / `role=be` 的分布式骨架：FE 负责 MySQL 协议、SQL parser/analyzer/optimizer、`DistributedPlan` 构建、fragment scheduling 与 result fetch；BE 通过 NovaRocksGrpc 接收 `SubmitFragment`、写入 result buffer，并处理 exchange、runtime filter、cancel 与 report。

但 `all-in-one` 仍保留几类 standalone 专属逻辑：

1. 普通 SQL 如果降成单 fragment，会绕过 `ExecutionCoordinator`，直接 `execute_plan`。
2. `InProcessDispatcher` 对 root fragment 使用 `ResultSinkHandle` 直接收 Arrow `Chunk`，没有经过 typed result buffer + fetch_result。
3. `terminal_sink`、`iceberg_catalogs` 等 runtime-local handle 会强制 collapse distribution，继续保留 direct local execution。

这些专属路径降低了维护成本的可预期性：同一条 SQL 在 all-in-one 与 FE/BE 下可能走不同 result、cancel、timeout、report、query state 清理路径。长期目标是删除这些分歧，让 all-in-one 成为 `role=fe + 一个本进程 loopback BE` 的退化形态。

## 2. 目标

1. 普通 standalone SQL 查询不再走 direct `execute_plan` fast path。
2. `all-in-one` 默认走 `ExecutionCoordinator + FragmentScheduler + RemoteDispatcher`。
3. `all-in-one` 在同一进程内启动一个 full-execution NovaRocksGrpc endpoint，并把它作为唯一 live backend。
4. root fragment 结果统一通过 `RESULT_SINK + novarocks_typed_result_sink + result_buffer + fetch_result(typed_result=true)` 返回。
5. cancel、timeout、client disconnect、query failure、write report 与 profile collection 都以 coordinator/dispatcher 路径为主。
6. 保留少数明确命名的 direct execution 例外，但它们不能是普通 SELECT 或写入 pipeline 的默认逃生口。

## 3. 非目标

1. 不把 StarRocks FE 兼容入口改成 standalone SQL planner。
2. 不在本设计里完成多 BE 动态调度、heartbeat、decommission 的语义升级；这些仍属于 FE/BE cluster 控制面。
3. 不优化 loopback 小查询性能；正确性和路径统一优先。
4. 不继续保留一个默认关闭的 legacy fast path flag。
5. 不要求所有 MV/IVM runtime-local 内部查询一次性 remote 化；先隔离并命名这些例外。

## 4. 目标架构

### 4.1 角色模型

`all-in-one` 进程内部拆成两个逻辑角色：

- Embedded FE：MySQL server、standalone SQL engine、optimizer、`ExecutionCoordinator`、backend registry。
- Embedded BE：同进程 NovaRocksGrpc full-execution service，监听 loopback 端口，执行 submitted fragments。

`role=fe` 和 `role=be` 保持现有跨进程职责：

- `role=fe`：MySQL server + optimizer + coordinator + report-only gRPC。
- `role=be`：full-execution NovaRocksGrpc。

统一后的普通查询链路：

```text
MySQL request
  -> StandaloneSession::execute_in_context
  -> analyzer / optimizer
  -> build_distributed_plan
  -> lower_distributed_plan
  -> ExecutionCoordinator
  -> FragmentScheduler
  -> RemoteDispatcher
  -> NovaRocksGrpc SubmitFragment
  -> submit_exec_plan_fragment
  -> result_buffer
  -> NovaRocksGrpc FetchResult typed
  -> coordinator typed chunk alignment
  -> MySQL response
```

在 `all-in-one` 中，`RemoteDispatcher` 的目标地址是同进程 loopback endpoint；在 `role=fe` 中，目标地址来自 live backend registry。

### 4.2 all-in-one backend registry

`all-in-one` 不再把 backend 作为隐式展示对象只用于 `SHOW BACKENDS`。启动时应注册一个真实 live backend entry：

- `be_id = 0`
- endpoint = 当前 full-execution gRPC bound address
- state = `Live`
- start_epoch = 当前进程 epoch

这样 `coordinated_execution_services()` 不需要用 `ClusterRole::AllInOne` 手写 `127.0.0.1:{exchange_port}` 特例，而是和 `role=fe` 一样从 registry 读取 live endpoint。区别只在 registry 的来源：all-in-one 是启动期内嵌注册，role=fe 是配置、SQL 管理与 heartbeat 管理。

## 5. 主要改动

### 5.1 删除普通查询的 single-fragment fast path

`choose_standalone_execution` 的普通查询分支应删除。`MultiFragmentBuildResult` 即使只有一个 fragment，也交给 `ExecutionCoordinator`。Scheduler 会生成一个 fragment instance，root backend 为唯一 backend，行为自然退化。

保留 `DirectExecution` 只能用于以下显式场景：

- 测试构造没有启动 gRPC backend 的纯 unit helper。
- 当前无法序列化 runtime-local handle 的 MV/IVM 内部查询。
- 明确的 metadata-only 操作，且它们不是普通 SQL scan/compute pipeline。

这些例外必须通过独立函数名表达，例如 `execute_query_direct_for_runtime_local_handle`，不能继续由 `exchange_port == 0` 或 `terminal_sink.is_some()` 在主查询路径里隐式触发。

### 5.2 all-in-one 改用 RemoteDispatcher

`dispatcher_for_role(AllInOne)` 应返回 `RemoteDispatcher`，目标 backend 来自 all-in-one registry。`InProcessDispatcher` 不应继续作为 test-only fallback 存在；相关 root `ResultSinkHandle` 直连执行逻辑应直接删除。

这样 root fragment 与 non-root fragment 都通过同一套 gRPC submit/fetch/cancel 语义运行，避免测试继续覆盖一套产品不会再使用的本地执行语义。

### 5.3 full-execution gRPC 启动顺序

`all-in-one` 必须在 MySQL listener ready 前完成 full-execution NovaRocksGrpc 启动，并把实际 bound port 写入 backend registry。启动失败必须 fail fast，不能降级到 direct execution。

启动顺序：

1. 安装配置并初始化日志。
2. 打开 `StandaloneNovaRocks`。
3. 启动 full-execution NovaRocksGrpc endpoint。
4. 等待 loopback endpoint 可连接。
5. 安装 all-in-one backend registry entry。
6. 启动 MySQL listener 并输出 `NOVAROCKS_READY mysql_port=...`。

这样外部 readiness marker 仍代表查询链路可用，而不是只代表 MySQL 端口已 bind。

### 5.4 避免同进程自调用死锁

loopback 方案允许同进程自调用，因此必须保证 coordinator blocking client 不占用服务端执行所需线程。

约束：

1. `RemoteDispatcher` 的 blocking submit/fetch/cancel 调用不得在 tonic runtime worker 上执行。
2. gRPC handler 中 CPU-bound 和 blocking result-buffer wait 继续使用 `spawn_blocking`。
3. `submit_and_fetch_loop` 必须保持超时和 cancel 清理，任何 submit/fetch transport error 都要 fan-out cancel 已提交实例。
4. gRPC server 与 query worker 使用的 runtime/blocking pool 不能因为单查询递归等待耗尽。必要时为 loopback client 使用独立 blocking thread 或独立 runtime handle。

必须补回归测试来覆盖：`SELECT 1`、简单 scan、简单 aggregate 在 all-in-one 下不挂死，并且 gRPC submit/fetch 计数递增。

### 5.5 错误分类

统一路径会让简单查询也暴露 transport 错误。错误信息应按优先级归类：

1. BE 执行错误：保留原始执行错误文本，作为用户可见主因。
2. Coordinator 超时、cancel、client disconnect：保留现有 query-level 文案。
3. gRPC transport / decode / endpoint unavailable：明确带 backend id 和 endpoint。

不允许在 all-in-one 失败时 fallback 到 direct execution。fallback 会重新引入隐藏分叉。

### 5.6 Profile / EXPLAIN ANALYZE

旧的 in-process profile collection 依赖本地 dispatcher 的 thread join 收集 fragment profile。改成 loopback 后，这条本地 profile 路径应删除；all-in-one 与 remote FE/BE 一样必须通过远程 report/profile API 获取 fragment profile。

阶段性策略：

1. 普通执行先统一。
2. `EXPLAIN ANALYZE` 如果需要 fragment profile，必须走 reportExecStatus/profile report 或一个明确的 remote profile collection API。
3. 在 remote profile API 完成前，`EXPLAIN ANALYZE` 对 coordinated remote path fail fast，不能返回空 actuals。

这保持与现有 remote dispatcher 的语义一致，也避免 all-in-one 独享 profile 能力。

## 6. 数据流细节

### 6.1 SELECT 查询

1. `execute_query_with_options_and_imv_validator_with_catalog_provider` 完成 analyze、optimize、`build_via_distributed_plan`。
2. 不再调用 `choose_standalone_execution`。
3. `ExecutionCoordinator` 接收 `MultiFragmentBuildResult`。
4. `FragmentScheduler` 对一个 backend 生成一个 root instance，scan fragment 也是一个 instance。
5. `RemoteDispatcher` submit root fragment 到 loopback BE。
6. BE 通过 `submit_exec_plan_fragment` 注册 typed result buffer，并执行 pipeline。
7. FE 通过 `fetch_result(typed_result=true)` 拉取 Arrow IPC，再对齐 root output metadata。

### 6.2 INSERT / DML / write report

所有普通 write pipeline 也应走 coordinator。writer final report 通过 `novarocks_report_addr` 回到 coordinator gRPC。all-in-one 的 coordinator report endpoint 与 full-execution endpoint 可以是同一个服务实例，因为该服务同时支持 execution 与 report。

如果某个 write flow 依赖不可序列化的本地 registry 或 sink handle，应被归入 `DirectExecution` 例外，并在调用点写清原因。

### 6.3 Metadata-only 语句

`SHOW BACKENDS`、`CREATE CATALOG`、`CREATE DATABASE` 这类不进入 pipeline 的语句仍可由 standalone engine 直接处理。它们不是 execution fast path，不影响查询执行统一目标。

## 7. 测试计划

### 7.1 单元测试

1. `dispatcher_for_role(AllInOne)` 返回 remote dispatcher，并能读到一个 live loopback backend。
2. all-in-one registry 初始化后 `live_backend_dispatch_entries()` 返回 be_id 0。
3. `ExecutionCoordinator` 对单 fragment build result 仍 submit/fetch 一次。
4. `choose_standalone_execution` 删除后，没有普通查询代码引用 `execute_plan` fast path。

### 7.2 集成测试

1. all-in-one `SELECT 1` 断言经过 NovaRocksGrpc `SubmitFragment` 和 typed `FetchResult`。
2. all-in-one 简单 scan、aggregate、join、order by 与现有 golden 一致。
3. all-in-one query timeout 会 cancel loopback BE 上的 submitted fragments。
4. all-in-one BE execution panic/error 会通过 result buffer/fetch path 传回 MySQL。
5. cross-process `role=fe/be` smoke 保持通过，证明 loopback 统一没有破坏 remote path。

### 7.3 SQL suite

默认 SQL suite 不再主要验证 direct execution；它验证 coordinator path。需要保留少量 direct-exception unit tests，只覆盖明确隔离的 runtime-local 内部查询。

## 8. 迁移顺序

1. 新增 all-in-one backend registry 初始化，保留现有执行路径不变，先验证启动和 `SHOW BACKENDS`。
2. 让 `dispatcher_for_role(AllInOne)` 使用 remote loopback dispatcher，并保留 single-fragment fast path，先跑强制 multi-fragment query。
3. 删除普通查询 single-fragment fast path，所有普通 SQL 进入 coordinator。
4. 删除 `InProcessDispatcher` 及其 root `ResultSinkHandle` 专属逻辑，不保留 test-only 实现。
5. 隔离 `terminal_sink`、`iceberg_catalogs` 等 direct exceptions，给每个调用点命名并补测试。
6. 修复 `EXPLAIN ANALYZE` remote profile 能力，或在无 remote profile 时保持 fail fast。

每一步都禁止 fallback 到 legacy direct execution。若 loopback backend 不可用，启动或查询应明确失败。

## 9. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 同进程 gRPC 自调用死锁 | blocking client 不跑在 tonic worker；handler 阻塞逻辑 `spawn_blocking`；增加 `SELECT 1` deadlock 回归 |
| 端口冲突或 readiness 过早 | gRPC endpoint ready 后才启动 MySQL readiness marker；registry 使用实际 bound port |
| 小查询变慢 | 接受性能损失；后续用 loopback transport 优化，不恢复 direct execution |
| 错误信息变成 transport 噪声 | BE 执行错误优先，transport 错误带 backend id/endpoint |
| profile 能力退化 | remote profile API 前 fail fast，不返回空 actuals |
| runtime-local handle 无法 remote 化 | 明确隔离为 direct exception，不混入普通 SQL 主路径 |

## 10. 成功标准

1. 普通 standalone SQL 查询不再引用 direct `execute_plan` fast path。
2. all-in-one 与 role=fe/be 都通过 `RemoteDispatcher` submit/fetch root result。
3. all-in-one `SELECT 1`、简单 scan、aggregate、join 都能证明经过 gRPC submit/fetch。
4. cross-process FE/BE smoke 与 SQL suite 保持通过。
5. direct execution 只剩少量命名清楚、测试覆盖的 runtime-local 例外。
