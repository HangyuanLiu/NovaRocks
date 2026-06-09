# IW-7：Standalone 分布式 INSERT INTO Iceberg Cutover — 设计文档

> 状态：设计已与项目负责人对齐（2026-06-09）。下一步：writing-plans 出实现计划。

## 1. 背景与动机

NovaRocks 的产品路线包含「standalone 模式发展为 1FE + N BE 的真分布式集群引擎」（已与负责人确认）。Iceberg Distributed Write Pipeline（IW 系列）把 iceberg 写从「单进程收集 chunks 后本地写文件 / commit」升级为 StarRocks 式 **多 BE async sink 写 staged files + FE/coordinator 单次 metadata commit**。

**IW-7 是这条线的第一条用户写路径 cutover**：`INSERT INTO <iceberg>` append。选它作为第一条，是因为 append 的 commit strategy 最简单，适合验证 writer → coordinator → commit → finalize 的完整分布式闭环。

**现状基线**（2026-06-09 调研，IW-1~6 + FE-compat 路径已落地）：执行侧组件大都就位，缺的是 standalone codegen 生成分布式 INSERT plan + engine 入口接线。详见第 4 节。

## 2. 前提与非目标（重要）

**前提**：
- 目前**无真实多机生产部署**（默认且唯一真实运行形态是 `ClusterRole::AllInOne` 单机）。IW-7 的正确性/一致性验证依赖 `sql-test-runner` 的 `--cluster-mode cross-process --cluster-size 2` 测试 harness——与 D2 分布式**读**核心同款验证方式。这是已知且可接受的验证手段。
- IW-7 为产品路线**铺路**，不是救火；可从容分阶段落地，不必赶。

**硬约束（一等设计约束）**：
- **all-in-one 不得显著性能回归**。单机是当前唯一真实运行形态。统一架构会把单机 INSERT 从「直接本地写」变成「codegen → partition exchange → sink operator → gRPC 本地回环 → coordinator 收齐 → commit」，开销天然上升。因此：
  - 新分布式路径 **behind flag**，老 `execute_iceberg_insert_or_overwrite` 本地写路径**保留可一键回退**。
  - flag 默认值待定（倾向：先默认走老路径灰度，待 all-in-one 性能 gate + 多 BE 一致性验证通过后再翻默认开）。
  - 测试设 **all-in-one 性能红线**：单机 INSERT 抽查不显著回退（阈值在 writing-plans 阶段定），超线则默认回退老路径。

**非目标**：
- `INSERT OVERWRITE`（IW-8）、`DELETE/UPDATE/MERGE`（IW-9）、MV refresh 写统一（IW-9）。它们复用本设计的 plan/sink/coordinator，只换 commit strategy。
- 小文件合并 / 写后 compaction 优化。
- 真实多机部署运维（k8s/部署脚本等）。
- D2「OVERWRITE 多 BE 挂起」：已于 2026-06-09 验证在 #270 后不复现，**不是本设计的触发点**；回归 guard 已固化在 `docker/iceberg-rest/d2-overwrite-regression.sh`。

## 3. 范围

- `INSERT INTO <iceberg> SELECT`、`INSERT INTO <iceberg> VALUES`（append，`CommitStrategy::Append`）。
- partition-aware shuffle：按 iceberg partition key 把上游数据 shuffle 到各 writer，使「同 partition → 同 writer」，控制小文件。
- 统一架构：all-in-one = N=1 特例，走同一条 sink + coordinator + gRPC 本地回环路径（现状 all-in-one 本就走本地回环 report，无独立 in-process 短路径）。
- branch/ref INSERT 的现有 format v3 校验保留。

## 4. 架构与数据流

**端到端数据流**：

```
INSERT INTO ice_tbl SELECT ...
  │  codegen（本设计新增的主要工作）
  ▼
[source fragments]  SELECT plan（多 BE 并行 scan/join/agg）
  │  partition-hash exchange（按 iceberg partition key shuffle）
  ▼
[sink fragments × N]  IcebergTableSink = AsyncSinkOperator<IcebergTableSinkBackend>
  │  各 writer 在 sink_io 异步写 staged parquet → sink_commit_info（file metadata）
  ▼  report：多 BE 走 gRPC exec_status_report；all-in-one 走 gRPC 本地回环
[FE/coordinator]  WriteCoordinator 注册 expected writers → 收齐所有 writer report
  │  → 真实 WriteCommitInput（multi-writer，替换 synthetic_write_commit_input）
  ▼
[IcebergWriteTransactionRunner（#270）]  operation lifecycle（Preparing→Committing→…→Finalized）
  │  → commit service run_iceberg_commit_typed：单次 Append metadata commit
  ▼  finalize：cache invalidation + dictionary stale
```

**现状基线（已就位，不重写）**：
- `AsyncSinkOperator` async sink contract（backpressure / pending-finish / 协作让出）：`src/exec/pipeline/async_sink.rs:96`
- `IcebergTableSinkBackend`（已实现 `AsyncSinkBackend`）：`src/connector/iceberg/sink.rs:298`
- `IcebergTableSinkFactory::try_new`（lower output/partition exprs + 组装 plan）：`src/connector/iceberg/sink.rs:121`
- staged writer kernel `write_record_batches` + 专用 `sink_io` runtime：`src/connector/iceberg/data_writer.rs`、`src/runtime/execution_services.rs:197`
- lower 已处理 `ICEBERG_TABLE_SINK`（建 factory + 跑 pipeline）：`src/lower/fragment.rs:510`
- scheduler 按 hash-partition instance_count 复制 sink fragment：`src/runtime/scheduler.rs:165`
- coordinator 注册 expected writers（`is_write_sink` → WriterKey）+ `WriteCoordinator`：`src/runtime/coordinator.rs:377`、`:397`
- report 路径：`src/service/grpc_server.rs:420` `report_exec_status` → `src/runtime/write_coordinator.rs:425` `handle_report_exec_status` → `apply_report` → CommitReady
- `commit_write_input` 已能消费「多 writer、每 writer 多文件」：`src/engine/write_transaction.rs:123`

## 5. 组件与改动清单

**A. 引入「INSERT 作为带 sink 的分布式 plan」能力（阶段 1 主体，比初版设想深）** —— optimizer + codegen + engine 入口

> **现状澄清（2026-06-09 writing-plans 阶段核实，修正本节初版的乐观）**：standalone INSERT 是 engine 命令式处理，**不进 optimizer**（只其内部 SELECT 进）；optimizer/codegen 无任何 INSERT/sink/distribution 的 plan 表示（所有 fragment 的 `output_sink` 只有 `build_result_sink`/`build_noop_sink`）。所以阶段 1 的真正地基是**引入一个 physical iceberg sink operator**，而非「加个 `build_iceberg_table_sink`」。

**实现路径：B（engine 入口包 sink）** —— 保持 INSERT 在 engine 层入口，不改 standalone DML 命令式范式。
1. **optimizer 新增 `PhysicalIcebergSinkOp`**（`src/sql/optimizer/operator.rs:509` 的 PhysicalOperator 枚举加变体）：携带 target table / partition spec / 输出列；对输入要求 `DistributionSpec::HashPartitioned{ cols = iceberg partition key 对应的输出列 }`（`src/sql/optimizer/property.rs:198`）。复用现有 distribution enforcement 自动在其下插 `PhysicalDistributionOp`（`operator.rs:381`）。非分区表要求 `Any`/`Random`。
2. **engine 入口**：INSERT iceberg 时，拿到 SELECT 的 optimized physical plan，在顶部包 `PhysicalIcebergSinkOp` → 重跑 property/distribution enforcement → codegen。
3. **codegen visit**：`PhysicalIcebergSinkOp` → root fragment 的 `output_sink = build_iceberg_table_sink()`（新增，类比 `build_result_sink` @ `fragment_builder.rs:4799`，产出 `TDataSink{ ICEBERG_TABLE_SINK, iceberg_table_sink: TIcebergTableSink{...} }`）；其下的 `PhysicalDistributionOp` 经现有 `visit_distribution`（`fragment_builder.rs:4243`）生成 partition exchange。
- **关键对齐**：`PhysicalIcebergSinkOp` 的 partition distribution cols 必须与 sink 内部 `build_partition_exprs`（`sink.rs:137`）推导的 iceberg partition 列一致，保证「同 partition → 同 writer」。

**阶段 1 因此再细分（供 writing-plans）**：1a optimizer `PhysicalIcebergSinkOp` + distribution requirement；1b codegen visit → `ICEBERG_TABLE_SINK` fragment；1c engine 入口包 sink + 跑分布式 plan（与下文 B 改造合并）。

**B. engine 入口改造** —— `src/engine/iceberg_writer.rs:259`
- `InsertOrOverwriteWriteExecutor::run_coordinated_write`：删掉「本地同步写 + `synthetic_write_commit_input()`」，改成构造分布式 INSERT plan → 经 `ExecutionCoordinator::execute_with_write_outcome` 跑 → 返回 `WriteCoordinator` 收齐的**真实** `WriteCommitInput`。
- `commit()` 不动（`commit_write_input` 已支持多 writer）。
- behind flag：保留老 `execute_iceberg_insert_or_overwrite` 本地写路径，按第 2 节硬约束可回退。

**C. all-in-one 一致性** —— 现状 all-in-one 已走 gRPC 本地回环 report，统一架构天然成立（N=1 特例）。代价是单机开销上升，由第 2 节 all-in-one 性能红线 + flag 兜底。

## 6. 数据流边界

1. **`INSERT INTO ... VALUES`**：常量小数据、无分布式 source。走统一 sink 路径但用 `UNPARTITIONED` exchange 汇到**单 writer**，避免对小数据无谓 shuffle。
2. **空输入（SELECT 0 行）**：writer 无文件 → 真实 WriteCommitInput 无 sink_commit_info → 复用 runner 已有 empty→Aborted 处理（`write_transaction.rs:276`），无需新增。
3. **非分区表**：无 partition key → `RANDOM`/`UNPARTITIONED` exchange，N writer 各写文件。
4. **branch/ref INSERT**：保留现有 format v3 校验（`spec.validation.require_v3_for_branch`），在 engine 入口校验，不下放 sink。

## 7. 错误处理（大部分复用 IW-4 / IW-6）

- **writer 失败**：某 BE sink writer 报错 → `WriteCoordinator::apply_report` 标记 Failed → `ExecutionCoordinator` fail query + cancel 其余已提交 fragment（IW-4 已有）→ runner 记 `FailedKnownUncommitted`（`write_transaction.rs:251`）。
- **timeout / client disconnect**：query timeout → cancel writers → `WriteAbortInput` → `record_writer_abort_fact`，错误明确。
- **commit unknown / finalize 失败**：进 commit 后由 IW-6 runner 状态机处理（`CommitUnknown` 不误报为 definite failure；finalize 失败记 `FinalizeFailedKnownCommitted`）。已有，不新增。
- **IW-7 特有 — staged files cleanup**：writer 失败/cancel 时各 BE 已写的 staged parquet 需清理（避免孤儿文件）。复用 sink 的 abort 路径（`AbortLog`/cleanup）；分布式下各 writer 的 staged 清理是否已覆盖，writing-plans 阶段 verify，缺则补 BE 侧 sink abort 清理。

## 8. 测试

- **正确性**：all-in-one INSERT INTO iceberg（分区/非分区 × VALUES/SELECT）结果与文件正确。
- **分布式一致性（核心验收）**：1FE+2BE 与 all-in-one 输出 byte-identical；多 BE 下观察到多 writer 各产 staged files、FE commit 一次。复用 `sql-test-runner --cluster-mode cross-process --cluster-size 2`（D2 guard 那条 `iceberg_rest_insert_select` + 新增 append 专项 case）。
- **分区布局**：分区表 INSERT 后同 partition 文件来自同一 writer（partition shuffle 生效）。
- **all-in-one 性能红线（硬约束）**：单机 INSERT 抽查不显著回退；超阈值 → flag 默认回退老路径。
- **回归**：iceberg / iceberg-rest 整套不回退；`cargo test --test cluster_mvp` 不回退。
- **error path**：writer 失败 → query 失败 + peers cancel + 无孤儿文件；timeout → abort 状态明确。
- **flag 可逆**：新分布式路径与老本地写路径结果一致，flag 可来回切。

## 9. 落地节奏（供 writing-plans 细化）

建议分阶段 PR（每个独立可验收、可回退）：
1. **地基（阶段 1，路径 B，behind flag 默认关）**：1a optimizer 新增 `PhysicalIcebergSinkOp` + partition distribution requirement；1b codegen visit → `ICEBERG_TABLE_SINK` fragment（`build_iceberg_table_sink`）；1c engine 入口在 SELECT optimized plan 顶部包 sink + `run_coordinated_write` 跑分布式 plan。
2. **all-in-one 验证 + 性能 gate**：单机正确性 + 性能红线达标。
3. **多 BE 验证**：1FE+2BE 一致性 + 分区布局 + error path；达标后翻 flag 默认开。
4. **清理**：稳定 ≥ 一段时间后删老本地写路径。

## 10. 可复用资产索引

- 当前同步写入口：`src/engine/iceberg_writer.rs`
- async sink contract / iceberg sink backend：`src/exec/pipeline/async_sink.rs`、`src/connector/iceberg/sink.rs`
- staged writer kernel / sink_io：`src/connector/iceberg/data_writer.rs`、`src/runtime/execution_services.rs`
- coordinator / write coordinator / report：`src/runtime/coordinator.rs`、`src/runtime/write_coordinator.rs`、`src/service/grpc_server.rs`
- 写事务 runner / commit：`src/engine/write_transaction.rs`、`src/connector/iceberg/commit/**`
- codegen fragment 切分 / 分区：`src/sql/codegen/fragment_builder.rs`
- D2 回归 guard：`docker/iceberg-rest/d2-overwrite-regression.sh`
- StarRocks 参考：`~/project/starrocks/be/src/exec/data_sinks/iceberg_table_sink.cpp`、`async_data_sink.h`
