# P6/P7 Iceberg 分布式 DML 写路径强终态设计

- 日期: 2026-06-12
- 状态: 设计已评审，待写实施计划
- 范围标签: iceberg, distributed-execution, dml, write-lifecycle, metadata

## 1. 背景与问题

`docs/design/2026-06-12-distributed-execution-target-architecture.md` 将 P6
定义为 Iceberg 写元数据 descriptor 端到端，将 P7 定义为分布式 DML 写与写生命周期剩余。
这两个支柱必须在同一个 PR 内按固定顺序完成：P6 先消除 writer report 的 lossy metadata
carrier，P7 再把仍在 coordinator 本地执行的 DML/MV 写文件路径切到 distributed sink。

当前代码的核心问题有两类。

第一类是 Iceberg partition metadata 的权威分裂。`TIcebergDataFile` 仍通过
`partition_path` 和 `partition_null_fingerprint` 携带 partition 信息，collector 再调用
`parse_partition_path` 反解为 `Struct`。`collector.rs` 已读到 `df.partition_spec_id`，
但仍用当前 `self.partition_spec` 解 path。partition evolution 下，旧文件按旧 spec 写 path，
用当前 spec 反解会得到错误 partition value。transform partition、delete file、ADD FILES
等路径也会复制这个风险。

第二类是写文件执行位置不统一。INSERT append/overwrite 已经通过 `execute_query_as_iceberg_write`
走 `ExecutionCoordinator`、distributed sink、`WriteCoordinator` 和
`IcebergWriteTransactionRunner`。但 DELETE、UPDATE、MERGE、ADD EQUALITY DELETE、部分 MV
refresh 路径仍在 coordinator 本地写 data/delete/equality-delete 文件，然后通过
`local_writer_commit_input` 伪造成单 writer output。这个 shim 让 operation lifecycle 看起来统一，
但真实文件生产仍绕过 distributed execution。

## 2. 目标与非目标

### 2.1 目标

- NovaRocks 内部 writer->coordinator report 无损携带 Iceberg partition `Struct`。
- collector 只从 descriptor 解码 partition values，不再从 `partition_path` 推断内部 commit
  correctness。
- DELETE、UPDATE、MERGE、MV refresh 的文件生产统一通过 pipeline sink 执行。
- coordinator 只负责 `WriteCommitInput` 聚合、typed commit、operation state transition、
  cleanup/finalize，不再本地产生 Iceberg data/delete/equality-delete 文件。
- PR 结束时删除 `local_writer_commit_input` 和 `new_local_writer_write_id`。
- 无法完整表达的 distributed DML shape 必须 fail fast，不允许 fallback 到 coordinator 本地写。

### 2.2 非目标

- 不在本 PR 完成完整 P8 `EngineError` 架构；本 PR 只为 P6/P7 的 fail-fast 点定义稳定 error code
  和局部 typed enum，后续可并入 P8。
- 不扩展 Iceberg DML 语义覆盖面。当前不支持或无法完整下推的 WHERE、assignment、COW rewrite
  形状保持不支持，但错误必须明确。
- 不引入新的 writer protocol fallback 或 feature flag。迁移完成后旧本地 writer shim 删除。
- 不解决读侧列级 schema evolution；P6 只解决写文件 partition-spec evolution。

## 3. 总体架构

PR 分两个强依赖层落地。

### 3.1 P6: Iceberg write descriptor

新增 NovaRocks 内部 writer report descriptor，表达每个写出文件的 partition values：

```text
TIcebergDataFile
  path
  format
  record_count
  file_size_in_bytes
  file_content
  referenced_data_file / equality_ids / first_row_id / key_metadata
  partition_spec_id
  partition_values_descriptor   <-- new internal authority
```

`partition_values_descriptor` 是按 partition field 顺序排列的 primitive payload 列表。每个元素只表示：

- 是否为 null；
- 非 null 时的 `Datum::to_bytes()` payload。

descriptor 不携带 `primitive_type_tag`。decode 时的唯一类型来源是：

```text
table.metadata()
  .partition_spec_by_id(partition_spec_id)
  .partition_type(table.metadata().current_schema())
```

collector 用 partition type 中每个 field 的 primitive type 调 `Datum::try_from_bytes`，
恢复 `Struct::from_iter(...)`。descriptor field 数、null/payload 组合、primitive decode 任一不一致，
都报 `IcebergWriteDescriptorMismatch`。`partition_path` 和 `partition_null_fingerprint` 可以暂时留在
thrift 结构中保持边界兼容，但 NovaRocks 内部 writer report 不消费它们，也不以它们作为 fallback。

### 3.2 P7: distributed DML/MV write cutover

DML/MV writer 只通过 distributed sink 产出文件：

- data file: `ICEBERG_TABLE_SINK`，`IcebergSinkMode::Data`；
- position-delete file: `ICEBERG_DELETE_SINK`，`IcebergSinkMode::PositionDeletes`；
- equality-delete file: 仅当已有 ADD EQUALITY DELETE 路径能完整表达时才增加 sink mode，否则保持
  fail-fast，不新增投机路径。

`ExecutionCoordinator` 收集所有 writer 的 `TSinkCommitInfo`，`WriteCoordinator` 聚合为
`WriteCommitInput`。`IcebergWriteTransactionRunner` 是 DELETE、UPDATE、MERGE、MV refresh 的唯一
lifecycle 入口。

强终态：

- 没有任何 DELETE/UPDATE/MERGE/MV executor 在 coordinator 进程内写 Iceberg 文件；
- `local_writer_commit_input` 和 `new_local_writer_write_id` 删除；
- legacy `run_iceberg_commit` 不再被 DML/MV refresh 新路径直接调用；
- 无法构建完整 distributed writer output 的语义直接 fail fast。

## 4. 组件设计

### 4.1 Descriptor codec

新增一个小模块，建议位置：

```text
src/connector/iceberg/write_descriptor.rs
```

职责：

- `encode_partition_descriptor(values: &Struct) -> TIcebergPartitionDescriptor`
- `decode_partition_descriptor(desc, spec_id, metadata) -> Result<Struct, IcebergWriteDescriptorError>`
- 单测覆盖所有 primitive `Datum::to_bytes` / `Datum::try_from_bytes` 支持类型。

codec 不读取或生成 `partition_path`。`partition_path_from_struct` 只保留给外部兼容字段填充，
不再作为内部权威。`parse_partition_path` 从内部 commit 路径删除；若仍有非内部边界需要它，
必须放在明确命名的 compat helper 中，并且不被 `IcebergCommitCollector::convert` 调用。

### 4.2 Writer report construction

收敛两条 forward encode 路径：

- `data_file_to_iceberg_thrift`
- `written_file_to_sink_commit_info` / `written_file_to_sink_commit_info_for_metadata`

它们都调用同一个 descriptor encoder。`written_file_to_sink_commit_info_for_metadata` 只负责按
`file.partition_spec_id` 找到 spec 并校验 descriptor，不再自行构造第二套 partition path 语义。

Data、PositionDeletes、EqualityDeletes 三种 `DataContentType` 必须都带 descriptor。unpartitioned 表
带空 descriptor，不能省略字段；这样 collector 可以区分“合法空 partition”和“缺少 descriptor”。

### 4.3 Collector

`IcebergCommitCollector::convert` 的 partition 恢复逻辑改成：

1. 读取 `partition_spec_id`，缺失时报错；
2. 通过 table metadata 查找 matching spec，缺失时报错；
3. 用 matching spec 的 `partition_type(schema)` 解 descriptor；
4. 生成 `WrittenFile { partition_values, partition_spec_id, ... }`。

collector 不再使用 `self.partition_spec` 解 writer-reported 文件，也不再根据当前 default spec
推断旧文件 partition。

### 4.4 Position-delete planning descriptor

`build_position_delete_output_schema()` 当前硬编码 `[file_path Utf8, pos Int64]`，是第二个 schema
权威。P7 中 position-delete sink 的 output schema 改由 planning descriptor 生成：

- planner 明确产生 `file_path`, `pos`, `<partition source cols>` 的 output exprs；
- sink 只消费 planning 给出的 descriptor 和 Iceberg spec 常量 field id；
- 硬编码 helper 最终降级为 spec constant builder 或删除，不再决定 runtime output shape。

### 4.5 DML planners and executors

DELETE：

- 现有 coordinator-local `scan_for_position_deletes_at` 逐行评估 WHERE AST 的能力不能静默弱化。
- cutover 前先将 DELETE WHERE 分为 supported distributed shape 和 unsupported shape。
- supported shape 生成 row-identity scan，输出 `file_path`, `pos`, `<partition cols>` 到
  `ICEBERG_DELETE_SINK`。
- unsupported shape 报 `UnsupportedDistributedDmlShape`，不走本地 fallback。

MOR UPDATE / MERGE matched UPDATE：

- matched rows 计划拆出 replacement data output 和 old-row position-delete output。
- data sink 与 delete sink 的 writer output 进入同一个 `WriteCommitInput`。
- commit 前校验 row identity、replacement data、position delete 的完整性；不完整时报
  `DistributedWriteOutputMismatch`。

MERGE matched DELETE：

- 与 DELETE 共用 position-delete distributed sink。
- matched-side filter/condition 若不能完整下推，报 `UnsupportedDistributedDmlShape`。

COW UPDATE / MERGE COW：

- distributed writer 必须上报足够信息重建 `CowUpdateRewriteSet`，至少包括 touched old data file、
  touched row ids、replacement files、replacement row ids 与 partition spec id。
- 如果某个 COW shape 无法从 writer output 完整重建 rewrite set，本 PR 中直接 fail fast。
- 不能保留 coordinator-local COW writer 作为后门。

ADD EQUALITY DELETE：

- 如果能以 distributed sink 完整表达 equality delete columns、batch schema、partition descriptor，
  则迁移到 sink mode。
- 如果不能完整表达，入口报 `UnsupportedDistributedDmlShape`。不新增投机性的
  `IcebergSinkMode::EqualityDeletes` 半成品。

### 4.6 MV refresh lifecycle

MV refresh 不再手动注入 collector 后直接调用 `run_iceberg_commit`。新路径把 refresh writer output
包装为标准 `WriteCommitInput`，交给 `IcebergWriteTransactionRunner`：

```text
refresh planning
  -> distributed writer sink(s)
  -> WriteCoordinator
  -> IcebergWriteTransactionRunner
  -> typed commit
  -> finalize / operation fact
```

恢复 worker 后续只需要扫描 `iceberg_operation` 表，不需要理解 MV 私有 commit 状态。

## 5. 数据流

### 5.1 Writer report

```text
Iceberg sink writes DataFile
  -> build TIcebergDataFile with partition_spec_id + partition_values_descriptor
  -> RuntimeState::add_sink_commit_info
  -> ReportExecStatus / WriteCoordinator
  -> WriteCommitInput
  -> IcebergCommitCollector::convert
  -> WrittenFile
  -> commit action
```

`partition_path` 不参与这条 correctness flow。

### 5.2 DELETE

```text
DELETE WHERE
  -> validate supported distributed predicate shape
  -> row identity scan: file_path, pos, partition cols
  -> ICEBERG_DELETE_SINK writes position-delete parquet
  -> descriptor-complete TSinkCommitInfo
  -> RowDelta / RowDeltaDv commit
```

### 5.3 MOR UPDATE / MERGE

```text
matched-row planning
  -> replacement rows -> ICEBERG_TABLE_SINK
  -> old row ids      -> ICEBERG_DELETE_SINK
  -> one WriteCommitInput
  -> one transaction runner operation
```

### 5.4 COW UPDATE

```text
matched-row planning
  -> replacement writer output + rewrite metadata
  -> reconstruct CowUpdateRewriteSet
  -> CowUpdateCommit
```

无法重建 `CowUpdateRewriteSet` 的 case 不执行写入。

## 6. 错误处理

本 PR 定义局部 typed error code，后续可并入 P8：

| code | 触发条件 |
|---|---|
| `IcebergWriteDescriptorMismatch` | descriptor 缺失、spec id 不存在、field 数不匹配、payload decode 失败 |
| `UnsupportedDistributedDmlShape` | WHERE / assignment / MERGE branch / COW rewrite metadata 无法完整分布式表达 |
| `DistributedWriteOutputMismatch` | 同一 transaction 中 data/delete writer output 不完整或互相矛盾 |
| `WriteCoordinatorGone` | writer report 到达时 coordinator 已不存在 |

内部控制流使用 typed enum，不新增依赖 error string 的分支。若现有边界仍需要 `String`，只在边界层
格式化，operation fact 和测试断言保留 stable code。

## 7. 验证计划

### 7.1 Unit tests

- descriptor round-trip: unpartitioned、identity partition、bucket/day/month transform、null、
  string escape、binary、decimal、timestamp。
- partition evolution: writer 用旧 `partition_spec_id` 的 descriptor，collector 用旧 spec 解码成功；
  错误地用当前 default spec 的旧测试应删除或改成失败断言。
- Data / PositionDeletes / EqualityDeletes 三种 content 都带 descriptor。
- collector 不消费 `partition_path`: descriptor 正确但 path 错误时按 descriptor 成功；descriptor 缺失时报
  `IcebergWriteDescriptorMismatch`。
- COW rewrite metadata reconstruction 单测覆盖 replacement file 与 touched old-file 映射。

### 7.2 SQL tests

- `iceberg-rest`: 现有 distributed INSERT append 保持通过。
- distributed DELETE: 非分区表、identity 分区表、transform 分区表。
- MOR UPDATE: replacement data + position delete 同 transaction。
- MERGE: matched update + unmatched insert；matched delete。
- unsupported WHERE / MERGE branch shape fail fast，错误 code 稳定。
- MV refresh: 现有 IVM/MV refresh case 通过，并验证 operation state 走 runner。
- Spark/Iceberg compatibility: 读回 NovaRocks 写出的 data/delete files，覆盖 position-delete schema 与
  descriptor partition correctness。

### 7.3 删除性检查

PR 末尾必须满足：

```bash
rg "local_writer_commit_input|new_local_writer_write_id" src
```

结果为 0。

`run_iceberg_commit` 可以继续存在于非 DML/MV legacy maintenance path，但 DELETE、UPDATE、MERGE、
MV refresh 新路径不得直接调用它。

## 8. 落地顺序

1. 新增 descriptor thrift/internal structs 与 codec，先只接 unit tests。
2. writer report forward encode 全部带 descriptor。
3. collector 翻转到 descriptor decode，并删除内部 `parse_partition_path` 依赖。
4. position-delete sink schema 改为 planning descriptor 供给。
5. DELETE cutover 到 distributed position-delete sink。
6. MOR UPDATE / MERGE cutover 到 data + position-delete distributed sinks。
7. COW UPDATE/MERGE COW 完整上报 rewrite metadata；无法完整表达的 shape fail fast。
8. MV refresh 接入 `IcebergWriteTransactionRunner`。
9. 删除 `local_writer_commit_input`、`new_local_writer_write_id` 和相关测试 fixture。
10. 跑 targeted SQL suites 与 Spark compatibility cases，重录必要 golden。

## 9. 接受标准

- P6: `IcebergCommitCollector::convert` 不再从 `partition_path` 恢复 partition values。
- P7: DELETE/UPDATE/MERGE/MV refresh 不再在 coordinator 本地产生 Iceberg 文件。
- PR 结束时没有 coordinator-local writer shim。
- 所有无法分布式表达的 DML shape 都 fail fast，并带 stable error code。
- Data / PositionDeletes / EqualityDeletes 的 partition descriptor 全部可 round-trip。
- distributed write 与 Spark 读回兼容性测试通过。
