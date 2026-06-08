# Iceberg 写入生命周期 Cutover 设计

## 目标

在一个 PR 中收尾 Iceberg distributed write lifecycle 这条线：让
writer/coordinator/commit/lifecycle 共享链路成为用户级 Iceberg 写入的默认路径。

本 PR 范围较大，但边界明确：

- `INSERT INTO` append 默认走 distributed writer lifecycle。
- `INSERT OVERWRITE` 和 `INSERT OVERWRITE PARTITIONS` 默认走同一套 lifecycle，并保留 overwrite 特有校验。
- RowDelta-family DML（当前支持的 `DELETE`、`UPDATE`、`MERGE`，包括 DV 和 COW update 模式）默认走同一套 lifecycle。
- 不新增 session 或 config fallback 回旧同步 writer 路径。
- 不启动 partition MV、后台恢复调度器、新 optimizer rule 或无关 maintenance 工作。

预期结果是：IW-6 correctness 语义和第一批用户可见 IW-7/IW-8/IW-9 写入 cutover 作为一条主线落地，而不是在多个路径里重复做表面接入。

## 当前上下文

`origin/main` 已经具备 typed Iceberg commit service facade 和 shared Iceberg operation lifecycle repository。MV refresh 已经通过 `StoredMvRefresh.operation_id` 和 `mv.refresh/0002.avsc` 接入 shared lifecycle。

当前分支已经加入第一层 writer lifecycle adapter：

- `src/runtime/write_operation_lifecycle.rs` 将 `WriteCommitInput` / `WriteAbortInput` 转成 operation request 和 pre-commit abort fact。
- `src/engine/write_operation_lifecycle.rs` 在 engine boundary 持久化 writer operation，但要求调用方显式提供 `WriteOperationContext`。
- `src/service/grpc_server.rs` 的 report-status 测试使用 `write_registry_test_guard()`，避免污染全局 write registry。

尚未闭合的是生产路径 cutover：

- `ExecutionCoordinator::execute()` 当前只记录 `WriteCommitInput` 日志，然后返回 `QueryResult` 时丢弃 writer outcome。
- 同步 Iceberg SQL 写入路径仍直接构造 data/delete files 并调用 `run_iceberg_commit`。
- 用户 SQL writer operation 的 commit success、known-uncommitted、commit-unknown、post-commit finalize failure 尚未写入 operation record。

## 已确认决策

1. 使用单一 engine-owned transaction runner，不在每个 SQL flow 内各自接 lifecycle。
2. 当前支持的用户级 Iceberg SQL 写入路径全部默认切换。
3. 不提供 fallback 开关回旧同步 writer。
4. runtime execution state 不依赖 metadata state。
5. `WriteCommitInput` 本身不足以决定 commit；engine 必须显式提供 target、commit strategy、base snapshot guard 和 validation policy。

## 架构

新增 engine-owned `IcebergWriteTransactionRunner`，作为需要 file writer output、metadata commit、lifecycle persistence、cache invalidation 的 Iceberg SQL 写入默认边界。

runner 统一负责以下流程：

1. 创建 `Preparing` 状态的 Iceberg operation record。
2. 将 source query 或 mutation plan 降成 coordinated writer plan。
3. 将 operation 推进到 `Writing`。
4. 执行 coordinated plan 并收集 writer outcome。
5. 将 `WriteCommitInput` 转成 commit input 和 staged artifact evidence。
6. 将 operation 推进到 `Committing`。
7. 调用 `run_iceberg_commit_typed`。
8. 持久化最终 operation fact。
9. 执行 post-commit finalization，例如 cache invalidation 和 dictionary stale marking。

SQL-specific flow 不直接拥有 lifecycle。它们只构造 `IcebergWriteTransactionSpec`，表达：

- target catalog / namespace / table / ref
- operation kind
- commit op kind
- base snapshot id 和 base sequence guard
- validation policy
- sink mode
- source query 或 mutation plan
- snapshot properties
- post-commit finalization policy

runtime 保持 metadata-agnostic。`src/runtime/coordinator.rs` 只暴露 writer outcome，不创建 operation record、不读取 Iceberg catalog、不调用 commit service。

## 组件边界

### `src/engine/write_transaction.rs`

新增 runner 和 write spec 模块。

核心类型：

- `IcebergWriteTransactionSpec`
- `IcebergWriteSource`
- `IcebergWriteCommitPolicy`
- `IcebergWriteValidationPolicy`
- `IcebergWriteTransactionRunner`
- `IcebergWriteTransactionOutcome`

职责：

- 解析 target table 和 commit service 所需 base metadata。
- 构造 `WriteOperationContext`。
- 创建和推进 operation record。
- 调用 coordinated execution 和 typed commit service。
- 持久化 commit / failure fact。
- 执行 post-commit finalization。

依赖边界：可以依赖 `StandaloneState`、Iceberg catalog/table handle、operation repository 和 typed commit service。

### `src/runtime/coordinator.rs`

拆分 coordinator 返回结构：

- `execute()` 保留兼容 wrapper，仍返回 `QueryResult`。
- `execute_with_write_outcome()` 返回 `CoordinatedQueryResult { query_result, write_commit, write_abort }`。

`WriteAbortInput` 必须能暴露给 engine，用于 writer registration 之后发生的失败路径。coordinator 仍负责取消已提交 fragment 和收集 writer final report，但不持久化 lifecycle fact。

### `src/runtime/write_operation_lifecycle.rs`

保留已有 adapter，并只扩展 runtime writer output 相关能力：

- staged artifact extraction
- 从 `WriteCommitInput` 创建 operation request
- 从 `WriteAbortInput` 创建 pre-commit abort fact
- 将 writer sink commit info 转成 commit collector 所需 written-file metadata

该模块不能依赖 `StandaloneState` 或 Iceberg catalog registry。

### `src/engine/write_operation_lifecycle.rs`

继续作为 writer operation fact 的小型持久化桥。新 transaction runner 应调用它，而不是在 runner 内重复写 repository transaction 逻辑。

### `src/connector/iceberg/operation_lifecycle.rs`

继续负责 typed commit service result 到 operation fact 的映射：

- committed outcome -> `Committed`
- known-uncommitted -> `FailedKnownUncommitted`
- unknown -> `CommitUnknown`
- finalize failure -> `FinalizeFailedKnownCommitted`

如果 runner 需要 operation-id-aware wrapper，应加在这里或 `src/engine/write_operation_lifecycle.rs`，不能散落在 SQL flow 中手写 fact merge。

### 现有 SQL 写入 flow

这些模块转为 spec builder 和 SQL-specific validation owner：

- `src/engine/iceberg_writer.rs`
- `src/engine/delete_flow.rs`
- `src/engine/mutation_flow.rs`
- `src/engine/equality_delete_flow.rs`

它们保留 SQL-specific validation 和 planning，但最终 writer、metadata commit、lifecycle、finalization 都通过 `IcebergWriteTransactionRunner`。

旧同步 writer implementation 能删则删；删不掉的函数只作为内部 helper，不能作为可切换执行路径继续存在。

## 数据流

### Append

`INSERT INTO iceberg_table SELECT ...` 构造 append transaction spec：

- operation kind: `InsertAppend`
- commit op kind: `FastAppend`
- source: 产出 target-aligned rows 的 query
- sink mode: data files
- validation: append-compatible schema 和 ref 检查

runner 执行 coordinated writer plan，并通过 typed commit service commit 收集到的 data files。

### Overwrite

`INSERT OVERWRITE` 构造 overwrite transaction spec：

- operation kind: `InsertOverwrite`
- commit op kind: `Overwrite`
- validation: 保留当前 overwrite 限制，包括 variant、partition spec 等已有校验

empty input 不是 no-op。它仍必须 commit overwrite 语义并清空目标范围。

`INSERT OVERWRITE PARTITIONS` 使用同一 runner，但 commit op kind 为 `OverwritePartitions`，并使用 partitioned-table validation policy。

### RowDelta / DML

DELETE、UPDATE、MERGE 继续拥有 DML-specific planning：

- touched file discovery
- MOR/COW mode selection
- row-lineage 和 DV validation
- replacement 或 delete-file plan construction

它们输出 `IcebergWriteTransactionSpec`，并选择正确 commit op kind：

- `RowDelta`
- `RowDeltaDv`
- `CowUpdate`

runner 负责 writer execution、commit 和 lifecycle persistence。

### Empty Input

empty input policy 固定如下：

- `INSERT INTO empty SELECT`：不创建 operation record，直接返回 OK。
- `INSERT OVERWRITE empty SELECT`：创建并 commit overwrite operation。
- empty RowDelta mutation：不创建 operation record，直接返回 OK。

这样可以避免在 operation repository 里产生 append/mutation no-op 噪声，同时保留 overwrite 的语义。

## 错误与恢复语义

Iceberg operation record 是用户可见写入事实的 source of truth。Writer coordinator state 只是内部 collection state，不能替代 operation state。

### Pre-write failure

runner 创建 operation 后、提交 writer fragment 前失败时，记录 `FailedKnownUncommitted`，staged artifacts 为空，`next_action=None`。

### Writer failure / timeout / client disconnect

writer fragments 已提交但 metadata commit 之前失败时，获取或构造 `WriteAbortInput`，记录 `FailedKnownUncommitted`。

如果存在 staged artifacts，`next_action=RetryAbort`；否则 `next_action=None`。

### Commit-ready failure

已有 `WriteCommitInput` 但 writer metadata 不完整或不一致，导致无法构造 commit input 时，记录 `FailedKnownUncommitted`。这是 pre-commit known-uncommitted failure。

### Commit success

`run_iceberg_commit_typed` 成功时，记录 `Committed`，包含 snapshot id 和 written manifest paths。cache invalidation 和 dictionary stale marking 只在该 fact 持久化后执行。

### Known-uncommitted commit failure

`CommitServiceError::KnownUncommitted` 记录 `FailedKnownUncommitted`、cleanup outcome、failure kind 和 next action。

cleanup error 不改变主事实，只影响 diagnostic 和是否继续保留 `RetryAbort`。

### Commit unknown

`CommitServiceError::Unknown` 记录 `CommitUnknown` 和 recovery evidence。runner 不能清理 staged files，也不能 invalidate table cache。用户可见错误必须包含 operation id、state、failure kind 和 `ManualInspect` next action。

### Post-commit finalize failure

`Committed` 之后的 finalization failure 进入 `FinalizeFailedKnownCommitted`。用户可见错误必须说明 metadata commit 已 known committed，避免用户盲目重试写入。

普通 INSERT / OVERWRITE / RowDelta 的 finalization 主要是 cache 和 dictionary maintenance。MV refresh 仍是 domain-specific finalization 更重的路径。

## 无回退策略

不新增 session variable 或 config 返回旧同步 writer 路径。

这意味着实现必须依赖 deterministic tests 捕获主要回归，而不是依赖开关规避风险。旧 helper code 也应删除或降级成内部 helper，避免后续贡献者重新绕回旧路径。

## 测试策略

### Unit Tests

覆盖：

- runner operation creation 和 state transition
- writer commit -> staged artifact / commit request conversion
- writer abort -> known-uncommitted fact conversion
- commit success -> committed fact
- known-uncommitted -> cleanup outcome / retry action
- commit unknown -> recovery evidence / no cleanup
- finalize failure -> known-committed failure
- empty append / empty overwrite / empty RowDelta policy

### Runtime Harness Tests

使用 fake 或 in-process coordinated writer outcome 验证：

- successful append 产生 committed operation
- writer timeout 记录带 abort evidence 的 known-uncommitted
- writer final-report mismatch 记录 known-uncommitted
- commit unknown 记录 recovery evidence 且不清理 staged files

这些测试必须 deterministic。不要在 SQL test 中依赖真实 timing 或 flaky network failure。

### SQL Tests

新增或更新 Iceberg SQL tests：

- `INSERT INTO ... SELECT`
- `INSERT OVERWRITE ... SELECT`
- `INSERT OVERWRITE PARTITIONS ... SELECT`
- 当前支持的 DELETE RowDelta / DV 输出路径
- 当前支持的 UPDATE / MERGE RowDelta 或 COW update 路径

SQL tests 验证表内容。若 test runner 能稳定读取 metadata repository，至少增加一个 operation diagnostic 断言；如果当前 runner 不支持该断言，则 diagnostic 覆盖放到 deterministic harness。

### Regression Tests

保持现有 focused tests 绿色：

- `write_commit`
- `writer_abort`
- `staged_artifacts`
- `writer_operation`
- `write_coordinator`
- `report_exec_status`
- commit service typed error tests
- 被 shared operation helper 触及的 MV lifecycle tests

## 风险与缓解

### PR 面较大

本 PR 有意同时 cut over append、overwrite 和 RowDelta。缓解方式是保持 shared runner 足够小，每个 SQL flow 只做 spec builder，而不是第二套 lifecycle implementation。

### Writer metadata 不足

`WriteCommitInput` 可能尚未携带所有 commit kind 所需字段。runner 必须 fail fast 并记录 `FailedKnownUncommitted`，不能猜。implementation plan 需要先盘点 append、overwrite、RowDelta、DV、COW 所需字段，再切默认 SQL routes。

### Commit unknown 误分类

commit service 已经提供 typed unknown error。runner 必须消费 enum，不能解析 legacy string。

### Commit 后 cache invalidation 失败

metadata commit 成功后的 cache invalidation 失败不能表现成写入失败。它要记录 known-committed finalize failure。

### 旧路径回流

因为没有 fallback，实现应删除或降级旧入口。测试应断言用户 SQL path 调用 transaction runner，而不是旧同步 writer function。

## 实现计划提示

后续 implementation plan 应拆成这些 checkpoint：

1. 定义 runner/spec types，用 fake writer 和 fake commit service outcome 测 state transition。
2. 扩展 coordinator return shape 暴露 writer outcome，同时保留兼容 wrapper。
3. 将 writer output 转成 append / overwrite commit collector input。
4. cut over append 并补 SQL 覆盖。
5. cut over overwrite / overwrite partitions 并补 SQL 覆盖。
6. cut over RowDelta-family DML 并补 SQL 覆盖。
7. 删除或降级旧同步 default path。
8. 跑 focused unit tests、Iceberg SQL tests 和 lifecycle regression tests。

plan 不能引入 fallback switch。如果某条路径缺少安全 cutover 所需 writer metadata，正确动作是补齐 metadata 或让 implementation checkpoint 失败，而不是保留旧 default path。
