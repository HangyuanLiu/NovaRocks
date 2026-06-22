# Query Stats Provider Architecture Design

- 日期: 2026-06-22
- 状态: 设计已收敛, 待实现计划
- 范围: standalone SQL optimizer 的 scan/table statistics 输入边界
- 目标: 用 catalog 真实统计替换按表名猜行数的 fallback, 并同步修正 unknown NDV 语义, 以长期架构清晰为第一优先级。

## 1. 背景

当前 `src/sql/optimizer/stats.rs` 在 scan 没有命中 `TableStatistics` 时会调用
`estimate_default_row_count(table_name)`。这个函数按 TPC/TPC-DS 表名和子串估行数:
`store_sales`/`lineitem`/`orders` 给大表默认值, `_dim` 给小表默认值, 未知表给固定默认值。
这能保护早期 TPC golden, 但对真实业务表是系统性误导。

更严重的是普通查询路径和 EXPLAIN 路径已经分裂:

- 普通 `SELECT` 使用 `TableLookupMode::SchemaOnly`, `CatalogMgrProvider` 返回不含 data files 的
  schema metadata, `ScanSource::IcebergDataFiles.files` 为空。
- `EXPLAIN` / `EXPLAIN ANALYZE` 使用 `TableLookupMode::ExplainStats`, 会 full load Iceberg table,
  进而抽取 data files 和 file-level row count。
- `build_table_stats_from_plan()` 只从 `ScanSource::IcebergDataFiles.files` 汇总统计; 普通执行路径拿不到
  files 时, 即使 catalog 有真实行数, optimizer 仍会落到表名 heuristic。

所以问题不是单个 fallback 函数, 而是 optimizer statistics input boundary 错了。修复必须把统计获取变成
query-level 的显式输入, 而不是把 schema lookup、scan binding、optimizer costing 混在一起。

## 2. 非目标

- 不做短期止血: 不只删除 `estimate_default_row_count`, 不只把默认值从按表名改成固定常量。
- 不把统计字段塞进 `TableDef`。`TableDef` 是 analyzer/schema 边界; query stats 是某次优化的快照,
  两者生命周期不同。
- 不让 `src/sql/optimizer` lazy fetch catalog 或 connector。optimizer 独立模块目标要求 optimizer 只消费
  native IR 和显式输入。
- 不引入 histogram、multi-column combined statistics、sampling service 或 runtime feedback producer。
  这些是后续扩展点, 不是这次架构切换的前置条件。
- 不保留 name-based row-count fallback 作为长期 escape hatch。

## 3. 设计原则

1. **统计是 query snapshot, 不是 schema metadata。** 同一张表在不同 snapshot/branch/time-travel 下统计不同;
   schema-only cache 不应该携带它。
2. **optimizer 不知道 catalog。** engine/planner bridge 负责收集统计, optimizer 只通过 stable key 查
   `QueryStatsSnapshot`。
3. **缺失必须显式。** Missing row count 和 unknown NDV 不能伪装成 `100000` 或 `1.0`。
4. **所有入口一致。** 普通 SELECT、EXPLAIN、EXPLAIN ANALYZE、INSERT SELECT、MV rewrite 必须共享同一个
   stats snapshot 构造入口。
5. **来源可观测。** 每个 row count / column statistic 都要能说明 source 和 confidence, 便于区分 catalog
   缺失、统计公式误差和 cost 公式问题。

## 4. 推荐架构

新增 query-scoped stats 边界:

```text
Analyzer / Planner
  |
  | produces logical plan with table identities
  v
Planner Optimizer Bridge
  |
  | produces owned OptExpr with unbound ScanOp.stats_ref
  v
QueryStatsCollector
  |
  | single mutable OptExpr traversal:
  |   allocate StatsRef, write ScanOp.stats_ref, ask provider
  v
QueryStatsSnapshot + bound OptExpr
  |
  | stats_ref -> QueryStatsSnapshot lookup
  v
derive_scan_statistics
```

### 4.1 StatsRef

`StatsRef` 是 optimizer scan 与 query stats snapshot 的唯一关联键。它不能是裸表名或 alias。

推荐结构:

```rust
pub(crate) struct StatsRef(u32);
```

由 `QueryStatsCollector` 在 optimizer bridge 生成 `OptExpr` 之后分配。collector 对同一棵 owned `OptExpr`
做一次 mutable traversal, 遇到 `ScanOp` 时当场分配 `StatsRef`, 写入 `scan.stats_ref`, 并把 provider 返回的
`BaseTableStatistics` 插入 `QueryStatsSnapshot`。每个 optimizer scan leaf 一个 `StatsRef`。即使两个 scan
引用同一张表, 也允许共享同一个底层 table stats value, 但 scan 自身仍持有明确的 `StatsRef`。这样可以支持:

- self join 和 alias 不冲突;
- time-travel / branch scan 与 current snapshot scan 不混淆;
- MV candidate target scan 和 query base scan 不靠表名碰撞检测;
- 后续 connector predicate/projection pushdown 后的 scan-specific stats。

`ScanOp.stats_ref` 在 bridge 刚生成时应是 `None`, 只有 collector 绑定后才变成 `Some(StatsRef)`。
`optimize()` 入口必须 validate 所有 scan 都已绑定, 防止入口忘记调用 collector 时静默走 fallback。

### 4.2 QueryStatsSnapshot

推荐放在 optimizer 或 planner/optimizer bridge 的中性边界里:

```rust
pub(crate) struct QueryStatsSnapshot {
    table_stats: HashMap<StatsRef, BaseTableStatistics>,
}
```

optimizer API 从:

```rust
optimize(opt_expr, scalar_arena, &HashMap<String, TableStatistics>, ...)
```

演进为:

```rust
optimize(opt_expr, scalar_arena, &QueryStatsSnapshot, ...)
```

`rewrite::context`、aggregate pushdown、multi-join reorder、search 都消费同一份 snapshot, 不再维护各自的
`HashMap<String, TableStatistics>`。

### 4.3 BaseTableStatistics

现有 `TableStatistics { row_count: u64, column_stats: HashMap<String, ColumnStatistic> }` 应拆成带缺失语义的
结构。

建议目标类型:

```rust
pub(crate) struct BaseTableStatistics {
    pub row_count: StatValue<u64>,
    pub columns: HashMap<String, BaseColumnStatistics>,
    pub source: StatsSource,
}

pub(crate) struct BaseColumnStatistics {
    pub nulls_fraction: StatValue<f64>,
    pub average_row_size: StatValue<f64>,
    pub min_value: StatValue<f64>,
    pub max_value: StatValue<f64>,
    pub ndv: StatValue<f64>,
}

pub(crate) enum StatValue<T> {
    Known { value: T, confidence: Confidence, source: StatsSource },
    Missing { reason: StatsMissingReason },
}
```

`StatsSource` 至少覆盖:

- `IcebergManifest`: data-file record count、null count、size、min/max。
- `IcebergPuffin`: Puffin theta sketch NDV。
- `ManagedLakeMetadata`: managed-lake metadata store row count, 后续扩展。
- `StarRocksTableMetadata`: StarRocks native table/tablet metadata, 后续扩展。
- `ConnectorEstimate`: JDBC/external connector 自报估算, 后续扩展。
- `Derived`: optimizer 从真实输入公式推导出来的值。
- `Fallback`: 公式无法估算时的保守默认, 只在 operator 估算层出现, 不作为 base table row count。

`StatsMissingReason` 至少覆盖:

- `NoCurrentSnapshot`
- `NoDataFiles`
- `ManifestMissingRowCount`
- `StatsFileMissing`
- `ConnectorUnsupported(String)`
- `CatalogLoadError`
- `ColumnNotReported`

## 5. StatsProvider 边界

在 connector/catalog 层新增统计能力, 与 schema lookup 和 scan binding 分离。早期 connector-first 设计曾有
`SupportsStatistics::estimate_statistics(&ScanHandle) -> Option<TableStatistics>` 的方向; 这次将其收敛为
query stats collector 使用的正式能力。

推荐 trait:

```rust
pub(crate) trait TableStatsProvider: Send + Sync {
    fn estimate_table_statistics(
        &self,
        request: &TableStatsRequest,
    ) -> Result<BaseTableStatistics, StatsProviderError>;
}
```

`TableStatsRequest` 需要包含稳定 table identity 和可选 snapshot/time-travel 信息:

```rust
pub(crate) struct TableStatsRequest {
    pub catalog: Option<String>,
    pub database: String,
    pub table: String,
    pub source: ScanSourceIdentity,
    pub snapshot: Option<TableSnapshotRef>,
}
```

注意: provider 返回的是 query planning statistics, 不是 executable scan splits。Iceberg provider 可以为了统计
读取 manifests, 但不能把 data files 塞回 analyzer 的 schema metadata。

## 6. QueryStatsCollector

新增 `QueryStatsCollector`, 由 engine 在 logical plan 转为 optimizer `OptExpr` 之后、`optimize()` 之前调用。

职责:

1. 遍历 owned `OptExpr` scan leaves, 为每个 scan 分配 `StatsRef`。
2. 把 `StatsRef` 写入 `ScanOp.stats_ref`。
3. 根据 scan source 构造 `TableStatsRequest`。
4. 调用对应 provider, 生成 `BaseTableStatistics`。
5. 对 provider error 做 warn-and-missing, 不让 advisory stats 阻塞查询。

推荐位置:

- stats provider trait: `src/connector/backend.rs` 或 `src/connector/stats.rs`
- collector: `src/engine/query_stats.rs`
- optimizer native types: `src/sql/optimizer/statistics.rs` 或拆到 `src/sql/optimizer/stats_input.rs`
- planner bridge: 只负责生成 `ScanOp.stats_ref = None`, 不负责 catalog stats 和 ref 分配

collector 需要同时服务以下入口:

- `execute_query_with_catalog_provider`
- `explain_query`
- `explain_analyze_query`
- `execute_query_as_iceberg_write`
- MV rewrite candidate preparation

这些入口现在各自构造 `table_stats`; 目标是全部改成同一个 collector 调用。

### 6.1 StatsRef wiring

`LogicalPlanNode` 不应长期携带 optimizer-only 字段。`StatsRef` 也不应靠 logical plan 与 optimizer bridge
的两次独立遍历来对齐, 因为 scan ordinal 一旦和 bridge traversal 顺序分叉就会 silent 错配统计。

目标 wiring 是:

1. `try_logical_plan_to_opt_expr` 保持单一职责, 只把 logical plan 转成 owned `OptExpr`。
2. `ScanOp` 增加 `stats_ref: Option<StatsRef>`, bridge 构造 scan 时填 `None`。
3. `QueryStatsCollector::collect(&mut opt_expr)` 对 `OptExpr` 做单次 mutable traversal:
   - 为当前 scan 分配 `StatsRef`;
   - 写入 `scan.stats_ref = Some(stats_ref)`;
   - 构造 `TableStatsRequest`;
   - 调用 provider 并写入 `QueryStatsSnapshot`。
4. collector 完成后满足强不变式: `QueryStatsSnapshot` 的 key 集合等于 `OptExpr` 内所有 bound
   `ScanOp.stats_ref`。
5. `optimize()` 入口 validates all scans bound。未绑定是入口 bug, 应直接报错或 debug assert, 不走 row-count
   fallback。

MV rewrite candidate target scan 不是输入 `OptExpr` 的原始 scan, 仍需在 MV candidate preparation 中单独分配
`StatsRef` 并写入 snapshot; rule 注入 MV scan 时必须使用 candidate 自己的 stats ref。缺 target stats 时,
candidate 使用独立 `Missing` stats ref 或放弃 rewrite, 不能借用原 base scan 的 stats ref。

## 7. Iceberg 首个 provider

Iceberg 是第一阶段必须打通的真实来源, 因为当前代码已经有完整素材:

- `registry::extract_data_files_with_stats_at` 可提取 data files 和 record count。
- `build_table_statistics_with_ndv` 已能把 files + Puffin NDV 转成 optimizer stats。
- `StatsLoader::load_ndv` 可读取 Puffin theta sketch。
- `IcebergTableInfo.serialized_metadata` 和 table metadata 可提供 current snapshot。

目标变化:

1. 普通 SELECT 不再依赖 `SchemaOnly` table def 的 empty files。
2. collector 对 Iceberg table 单独 load metadata/manifests 来构造 stats snapshot。
3. EXPLAIN 和普通 SELECT 用同一份 collector 结果。
4. `build_table_statistics_with_ndv` 可保留作为 Iceberg provider 内部 helper, 但 provider 输出类型改为带
   `StatValue`/missing/source 语义。
5. provider 必须复用 `IcebergCatalogEntry` 的 data-file cache 或 collector 内 per-query cache, 避免普通
   SELECT 每次重复 full manifest extraction。

对于空表:

- 如果 current snapshot 无 data files, row count 应是 `Known(0, Exact, IcebergManifest)`。
- 如果没有 current snapshot, row count 是 `Missing(NoCurrentSnapshot)`。scan runtime 仍返回空结果的语义由
  scan binding 处理, stats 层不伪造行数。

## 8. Unknown NDV 语义(ST-2)

当前 `ColumnStatistic::unknown()` 把 `distinct_values_count` 设置为 `1.0`。虽然部分消费者已有 `> 1.0`
guard, 但 sentinel 本身仍危险:

- 新 consumer 容易误信 `1.0`;
- aggregate/group cardinality 容易把 unknown 当单值列;
- `Fallback` confidence 与 numeric sentinel 混在一起, 不利于 explain 和测试。

目标:

```rust
pub(crate) enum DistinctValueCount {
    Known { value: f64, confidence: Confidence, source: StatsSource },
    Unknown { reason: StatsMissingReason },
}
```

如果不想引入专门 enum, 也可以先统一使用 `StatValue<f64>`。无论实现形态如何, 语义必须满足:

- `ColumnStatistic::unknown()` 不再暴露可被直接当真值的 NDV。
- join key NDV、group expr NDV、predicate equality selectivity 都显式处理 Missing。
- 默认 NDV 只能在公式层作为 `Fallback` conservative value 出现, 不能写入 base column stats。
- `cap_ndv_at_rows` 接受 `Known` 才 cap; `Unknown` 保持 unknown。
- 旧 `distinct_values_count: f64` 字段最终必须删除或私有化; production consumer 只能通过
  `trusted_ndv()` / `ndv_value()` 这类访问器读取。

这部分必须和 row-count fallback 删除成对落地。只删除行数 heuristic 而保留 unknown NDV=1.0, 会把问题从
row count 迁移到 NDV。

## 9. derive_scan 行为

目标 scan 推导规则:

1. 用 `scan.stats_ref` 从 `QueryStatsSnapshot` 查 `BaseTableStatistics`。
2. `row_count=Known` 时作为 scan base rows, confidence/source 来自 stats value。
3. `row_count=Missing` 时返回明确的 fallback scan statistics:
   - `output_row_count` 使用统一保守默认, 仅用于 cost 防崩溃;
   - `row_count_confidence=Fallback`;
   - fallback reason 记录为 `MissingBaseRowCount(reason)`;
   - 不参考表名、alias、子串。
4. scan predicates 使用统一 selectivity kernel。只有可信列 stats 才参与 NDV/min/max 选择率。
5. 输出列 stats 按 ColumnId 映射, 但 source key 来自 catalog column name; bridge 负责建立 scan output column
   到 base column name 的对应关系。

最终删除:

- `estimate_default_row_count(table_name)`
- alias/table-name `HashMap<String, TableStatistics>` lookup
- MV target stats 的 table-name collision 逻辑

## 10. 与现有 per-group statistics roadmap 的关系

本设计解决的是 **base table stats 输入边界**。2026-06-16 optimizer statistics roadmap 解决的是
**memo group 内统计缓存、代表选择和 confidence 传播**。两者正交, 但有依赖关系:

- QueryStatsSnapshot 先让 scan leaf 有正确 base rows/NDV。
- per-group roadmap 再决定 group 统计如何缓存、坍缩和被 cost 消费。
- cost model redesign 只消费 `Statistics`, 不重新拉 catalog stats。

推荐顺序:

1. 本设计 Phase 1-3: 引入 StatsRef/QueryStatsSnapshot 并接通 Iceberg row count/NDV。
2. 统计 roadmap Phase 0/1: stage-idempotent aggregate + per-group confidence argmax。
3. 本设计 Phase 4: 删除 name-based fallback 和 unknown NDV sentinel。
4. cost model redesign: 更积极地使用 confidence/source/fallback reason。

## 11. 分阶段落地

### Phase 1: 类型与 API 骨架

- 新增 `StatsRef`、`QueryStatsSnapshot`、`BaseTableStatistics`、`StatValue`、`StatsSource`、
  `StatsMissingReason`。
- optimizer public API 接受 `&QueryStatsSnapshot`。
- `ScanOp` 增加 `stats_ref: Option<StatsRef>`, bridge 初始填 `None`, collector 绑定后填 `Some`。
- 在同一实现 arc 内允许一个旧 `HashMap<String, TableStatistics>` 到 `QueryStatsSnapshot` 的测试/迁移 adapter,
  但必须有删除任务和审计; 生产新入口不得调用它。

验收:

- optimizer 单测可以用 `QueryStatsSnapshot::for_test(...)` 构造 scan stats。
- 旧测试仍通过。

### Phase 2: Iceberg provider + collector

- 新增 `TableStatsProvider` 能力。
- Iceberg 实现从 current snapshot manifests 和 Puffin stats 构造 `BaseTableStatistics`。
- 新增 `QueryStatsCollector`, 遍历 optimizer `OptExpr` scan leaves, 绑定 `stats_ref` 并生成 snapshot。
- 普通 SELECT 和 EXPLAIN 改用同一个 collector。

验收:

- 同一 Iceberg 表的普通 `EXPLAIN COSTS SELECT ...` 与普通执行优化路径使用同一 row count。
- 新增 sql-test 证明非 TPC 表名的 Iceberg 表行数来自实际插入数据, 不受表名影响。

### Phase 3: 入口统一

- `execute_query_with_catalog_provider`
- `explain_query`
- `explain_analyze_query`
- `execute_query_as_iceberg_write`
- MV rewrite candidate preparation

全部迁移到 collector。

验收:

- 删除 `build_table_stats_from_plan()` 或将其改成 collector 内部 Iceberg helper。
- `rewrite::context` 和 aggregate pushdown 不再接收 table-name keyed stats map。

### Phase 4: ST-2 unknown NDV

- 将 `ColumnStatistic.distinct_values_count: f64` 改成 missing-aware representation。迁移中可以短暂增加
  `ndv: StatValue<f64>` 或 `DistinctValueCount`, 但最终必须删除或私有化旧字段。
- join/selectivity/aggregate/grouping consumers 显式处理 Missing。
- 默认 NDV 只存在公式层, 不写回 base stats。

验收:

- `ColumnStatistic::unknown()` 不再能被误用为真实 NDV=1。
- `rg "distinct_values_count" src/sql/optimizer` 不再命中 production 裸读; 如有测试/compat helper, 必须通过
  `trusted_ndv()` 等访问器隔离。
- aggregate pushdown、join cardinality、predicate selectivity 对 missing NDV 均走可观测 fallback。

### Phase 5: 删除 name-based fallback

- 删除 `estimate_default_row_count`。
- 删除所有 `sales`/`lineitem`/`_dim` 表名 heuristic 单测或改成验证 missing/fallback reason。
- 重录受影响 optimizer goldens。

验收:

- `rg "estimate_default_row_count|store_sales|lineitem|_dim" src/sql/optimizer` 不再命中统计 fallback。
- TPC suites 需要真实 catalog stats 或显式 test fixture stats, 不再靠表名特判。

### Phase 6: 扩展 provider

按优先级补:

1. managed-lake metadata store row count。
2. StarRocks native table/tablet metadata row count。
3. JDBC/external connector optional estimate。
4. future measured stats/runtime feedback。

这些 provider 都只接入 `TableStatsProvider`, 不改变 optimizer API。

## 12. 测试策略

### Rust 单测

- `StatsRef` self join 不冲突。
- `QueryStatsSnapshot` missing row count 不走表名 heuristic。
- Iceberg provider 空表返回 known zero, no snapshot 返回 missing。
- unknown NDV 不被 `get_expr_ndv`/join/selectivity 当成 real value。
- derive_scan 对 `MissingBaseRowCount` 产生 `Confidence::Fallback` 和 fallback reason。

### SQL tests

- 非 TPC 表名 `random_business_events` 插入 3 行, `EXPLAIN COSTS` scan rows=3。
- 表名包含 `sales` 但实际 2 行, rows=2, 证明不走子串 heuristic。
- 表名包含 `_dim` 但实际大于 10000 行的 fixture rows 使用真实统计。
- 普通 SELECT 与 EXPLAIN 的 plan stats 一致。
- Iceberg Puffin NDV 在 join/aggregate cost gate 生效。

### Audit

新增或扩展 optimizer dependency audit:

- `src/sql/optimizer` 不 import `engine`、`connector`、`catalog_mgr`。
- optimizer stats code 不出现 table-name heuristic。
- `QueryStatsSnapshot` 是 optimizer 统计输入的唯一入口。

## 13. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| Iceberg manifest stats collection 增加 planning latency | collector 使用 per-query cache; catalog entry 继续复用已有 data-file cache; EXPLAIN/SELECT 共享路径避免双实现 |
| provider error 阻塞查询 | stats provider error 降为 Missing + warn/debug reason; 执行语义不依赖 stats |
| 大量 golden 变化 | 分阶段启用, 每阶段只重录对应 suite; `EXPLAIN COSTS` 增加 source/confidence 便于解释 |
| unknown NDV 类型改动面大 | 先引入 `StatValue` 并提供 compatibility accessors, 然后逐步删除旧 `distinct_values_count` |
| MV rewrite target stats 依赖表名 key | 用 `StatsRef` 消除碰撞; candidate target scan 由 collector 分配独立 stats ref |
| INSERT SELECT write path 忘记迁移 | 把 optimizer API 改成必须传 `QueryStatsSnapshot`, 编译强制所有入口处理 |

Phase 1 的 Iceberg provider 必须至少复用 `IcebergCatalogEntry` 的 data-file cache 或在 collector 内做 per-query
cache, 避免普通 SELECT 每次重复 full manifest extraction。若某个 connector 暂时没有可复用 cache, 需要在
provider 内显式记录为后续性能扩展项, 不能隐藏在 engine 入口。

## 14. 成功标准

1. optimizer scan row count 不再依赖表名、alias 或子串。
2. 普通 SELECT 和 EXPLAIN 使用同一个 stats snapshot。
3. Iceberg 普通查询能使用 manifest row count 和 Puffin NDV。
4. unknown NDV 不再以 `1.0` sentinel 暴露给新 consumer。
5. `src/sql/optimizer` 不引入 catalog/connector 依赖。
6. 缺失统计可观测, 但不会阻塞查询。
7. 后续 managed-lake、StarRocks native、JDBC stats 只需实现 provider, 不需要改 optimizer API。
