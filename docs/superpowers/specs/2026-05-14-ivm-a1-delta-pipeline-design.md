# IVM-A1 Delta SELECT 到 Iceberg Sink 一体 pipeline 设计

- 状态：草稿（brainstorm 通过，待审）
- 日期：2026-05-14
- 范围：Iceberg-backed MV 增量刷新执行层
- 依赖：A4（`__change_op` delta source，#121）、A7（branch-staged refresh transaction，#125）、A9（target row identity apply，#126）、A10（unified MV target commit，#107）、A11（MV schema/field-id contract，#130/#131/#132）、A5/A6（MvBackend lifecycle split + IcebergMvBackend，#133）
- 阻塞 / 后续：A2 任意 snapshot range；A3 多基表 snapshot pin；POC 路线一 plan rewrite 完整版

---

## 1. 背景与问题

iceberg-backed MV 当前增量刷新由 [src/engine/mv/iceberg_refresh.rs](../../../src/engine/mv/iceberg_refresh.rs) 主导，靠两条独立的 `execute_query` 各自跑一遍 MV `physical_select_sql`：

- INSERT 侧 [mv_flow.rs:230 `execute_query_for_mv_incremental_refresh`](../../../src/engine/mv_flow.rs:230)：把 snapshot diff 拿到的 Iceberg 新 data files 包成 `IcebergFileForQuery`，注册到一次性 `InMemoryCatalog`，再跑 MV SELECT。
- DELETE 侧 [mv_flow.rs:387 `execute_query_for_mv_incremental_deletes`](../../../src/engine/mv_flow.rs:387)：把反向重建出来的内存 `Vec<RecordBatch>` 用 [mv_flow.rs:282 `write_mv_delete_temp_parquet`](../../../src/engine/mv_flow.rs:282) 落到 `std::env::temp_dir()`，再走同一套一次性 catalog + SELECT。

两条 pipeline 跑完各自吐出 `QueryResult`，[iceberg_refresh.rs:2049-2052](../../../src/engine/mv/iceberg_refresh.rs:2049) 把它们转成 `Vec<Chunk>`，再 `data_block_on` 异步写 Iceberg DataFile / 调 A9 target locator / 调 `run_iceberg_commit`。

### 这套架构的真实代价

1. **事务边界撕扯**：两次 `execute_query` 在不同 query context（`query_id`、`runtime_state`、`cancellation` 都不联动）。INSERT 侧成功 + DELETE 侧失败时，已经在内存里的 insert chunks 浪费。A7 branch-staged 只保最终 metadata 一致性，跑完的 SELECT 资源是真损失。
2. **性能损失**：parse + analyze + lowering 重复两遍；多基表 join MV（POC 路线一目标）下 base 表会被各自扫两遍，没有 runtime filter 复用、shared scan、hashtable 复用。
3. **本地磁盘强依赖**：`write_mv_delete_temp_parquet` 用 `std::fs::create_dir_all` + `std::fs::File::create` 绕过 `src/fs/**` 抽象。云原生（无本地磁盘 / 只读 rootfs）部署不通。
4. **大 delta 内存峰值无解**：`materialize_changes` 一次性把所有反向重建的行进内存 `Vec<RecordBatch>`；100MB delta → 100MB+ Arrow buffer 常驻；spill 触发不到（数据已在算子外）。
5. **下游 IVM 算子无处挂**：未来 join IVM、聚合 IVM 要求 delta 以流的形式进入算子，"两次 SELECT 收 `QueryResult` 再装 chunk"这种数据流形状杜绝了 streaming 路径。

A1 把这套重构为"**一次 ExecPlan、一条 pipeline、一个 sink，refresh driver 协调 A7 staging + A10 commit**"。

---

## 2. 目标 / 非目标

### 目标

- iceberg-backed MV 增量刷新由**一次** `execute_plan` 完成，从底部 scan 到顶部 sink 是同一条 streaming pipeline
- 反向重建逻辑从 refresh layer 下沉到新算子 `IcebergDeltaScan`，pull-driven streaming，不预物化全部 delete 行
- 写 DataFile / 写 PositionDeleteGroup 的工作下沉到新算子 `IcebergMergeSinkFactory`，按 `__change_op` 路由
- 删除 `write_mv_delete_temp_parquet` / `execute_query_for_mv_incremental_deletes` / `materialize_changes` 的 `QueryResult` 通路、清理一次性 `InMemoryCatalog` 魔术
- iceberg-ivm SQL 测试 suite 保持 16/16 通过，并新增 2 个 case 覆盖大 delta + INSERT/UPDATE/DELETE 混合

### 非目标

- **managed-lake MV 不动**——[ivm_delta_source.rs:81 `write_mv_delete_temp_parquet`](../../../src/connector/starrocks/managed/ivm_delta_source.rs:81) 那条仍保留
- **A2 任意 `[from, to]` snapshot range 不做**——A1 仍是 prev→curr 单步
- **A3 多基表 snapshot pin 不做**——A1 仍单基表
- **POC 文档路线一 plan rewrite framework 完整版不做**——A1 交付的 leaf-swap pass 是它的萌芽，但不是 framework
- **Spill 实际触发验证不做**——streaming 把内存峰值压到单 chunk 给 spill 提供了前提，但 spill metric / chaos 测试另起 PR

---

## 3. 决策汇总

| # | 决策点 | 选择 | 关键理由 |
|---|---|---|---|
| 1 | PR 切分 | 一刀切（scan + sink 同 PR） | 避免两轮回归；scan 与 sink 接缝（change_op 路由）只磨合一次 |
| 2 | 覆盖面 | 仅 iceberg-MV | managed-lake MV 用另一套 sink 体系（tablet/publish version），并入会让 PR 双体量、双回归面 |
| 3 | Sink 与 commit 边界 | Sink 只写入；A7 staging + A10 commit 留 refresh driver | A7 事务边界不被 pipeline 吸走；与现有 `IcebergTableSinkFactory` "sink 产 WrittenFile、commit 在外"语义一致 |
| 4 | Scan 形态 | 形态 1：厚 trait / IVM-aware scan | 一个 source leaf 内吞所有 INSERT+DELETE 源，避免 UnionAll 在 plan 层拼接 |
| 5 | Scan 算子住哪 | 1b：新算子 `IcebergDeltaScan` | 不动 HDFS_SCAN；IVM 知识隔离；普通 Iceberg 查询零认知开销 |
| 6 | Plan 拼接 | P1：Leaf-swap | SQL 走 analyzer/codegen 正常路径；refresh layer 在 ExecPlan 上做 post-rewrite 替换 base scan leaf；MV SELECT 复杂化（aggregate/join）后自动跟进 |
| 7 | Scan 执行模式 | Streaming | 满足 A1 文档"无本地磁盘 / 100MB+ delta / spill 可触发"全部 3 条验收 |
| 8 | Sink 算子 | S2：新建 `IcebergMergeSinkFactory` | 与 `IcebergDeltaScan` 选 1b 对称；不污染 FE-thrift-driven 的 `IcebergTableSinkFactory` |
| 9 | A9 locator 状态生命周期 | L1：refresh driver 预加载，sink config 注入 | 与"sink 只写入"边界一致；locator load 是 refresh driver commit-准备阶段责任 |
| 10 | `__change_op` 暴露 + 验收 | C-a 透明列（A4 同款）+ T-a 删 temp parquet 代码 + suite 加 2 case | 与 A4 行为一致，零认知分歧；硬保证靠删代码不靠测试 |

---

## 4. 架构与数据流

### 4.1 ExecPlan 形态

以 `CREATE MATERIALIZED VIEW mv_high_value AS SELECT region, amount FROM iceberg.db.orders WHERE amount > 100` 为例。`iceberg_mv_physical_select_sql` 改写后 `physical_select_sql` 是 `SELECT region, amount, _row_id AS __nova_base_row_id FROM iceberg.db.orders WHERE amount > 100`。

```
IcebergMergeSink                                          [新 operator]
   │   按 __change_op 路由：
   │     INSERT 行 → DataFileWriter → collector.inject_written_file
   │     DELETE 行 → 提取 __nova_base_row_id → A9 locator → collector.inject_delete_group
   ↑
ProjectOp { region, amount, __nova_base_row_id }          [现有 operator]
   ↑    __change_op 列透明透传
FilterOp { amount > 100 }                                 [现有 operator]
   ↑
IcebergDeltaScan {                                        [新 operator]
   base_table_ident: iceberg.db.orders,
   from_snapshot_id: v1,
   to_snapshot_id: v2,
   base_projection: [_row_id, region, amount],
   change_files: [
     (s3://.../00001.parquet,  role=DataFile,        change_op=+1),
     (s3://.../pd-001.parquet, role=PositionDelete,  change_op=-1, targets=...),
     (s3://.../ed-001.parquet, role=EqualityDelete,  change_op=-1, targets=...),
   ],
   apply_key_source: BaseRowId,
   ...
}
```

整棵 ExecPlan 在一个 `execute_plan` 调用内执行，pipeline 跨越 scan → filter → project → sink 一条流。

### 4.2 Refresh driver 流程（重构后）

```
1.  acquire mv_refresh_lock                                          [不变]
2.  load mv_definition; ensure A11 schema/lineage contract           [不变]
3.  snapshot diff: batch = iceberg_changes(from=v_last, to=v_curr)   [不变]
4.  早返回：batch 完全空 → 仅推进 lineage，不进 pipeline               [不变]
5.  if has_deletes (batch.deletes/eq_deletes/deleted_data_files 非空):
       locator_state = load_target_apply_locator_inputs(target_entry, &target_table)
    else:
       locator_state = None
6.  begin A7 staged refresh intent → staging branch                  [不变]
7.  ensure staging branch ready                                      [不变]
8.  collector = Arc::new(IcebergCommitCollector::for_mv_apply(...))  [不变]
9.  build ExecPlan：
       a. parse + analyze + codegen(physical_select_sql) → Project/Filter/Scan 三层
       b. leaf-swap: ExecPlan walk 找到唯一的 base scan leaf，替换为
          IcebergDeltaScan { base_table_ident, from_snap, to_snap,
                             base_projection, change_files, ... }
       c. 顶部包 IcebergMergeSink { target_table, collector, locator_state, ... }
10. execute_plan synchronously
       └─ Streaming：IcebergDeltaScan → Filter → Project → MergeSink
          Sink 持续注入 WrittenFile / PositionDeleteGroup 到 collector
11. pipeline 完成后 collector 已 ready
12. run_iceberg_commit(collector, staging_branch, ...) → new_snapshot_id   [不变，A10]
13. publish + record + finalize                                            [不变，A7]
```

### 4.3 代码搬家清单

| 当前位置 | A1 后 |
|---|---|
| [changes.rs:568-679 `materialize_changes`](../../../src/connector/iceberg/changes.rs:568) 反向重建 | 搬入 `IcebergDeltaScan` operator，改 streaming |
| [changes.rs `scan_deletes_with_*`](../../../src/connector/iceberg/changes.rs) helpers | 保留为 `IcebergDeltaScan` 调用的纯 helper |
| [mv_flow.rs:230 `execute_query_for_mv_incremental_refresh`](../../../src/engine/mv_flow.rs:230) | **删除** |
| [mv_flow.rs:282 `write_mv_delete_temp_parquet`](../../../src/engine/mv_flow.rs:282) | **删除** |
| [mv_flow.rs:387 `execute_query_for_mv_incremental_deletes`](../../../src/engine/mv_flow.rs:387) | **删除** |
| [iceberg_refresh.rs:2030-2076 双 execute_query](../../../src/engine/mv/iceberg_refresh.rs:2030) | 单 `execute_plan` |
| [iceberg_refresh.rs:2166 `write_chunks_as_iceberg_data_files`](../../../src/engine/mv/iceberg_refresh.rs:2166) | 拆出 chunk → DataFile 的 helper，sink 用；外部显式调用点删 |
| [iceberg_refresh.rs:2169 `locate_target_rows_by_apply_key`](../../../src/engine/mv/iceberg_refresh.rs:2169) | 拆出 row_id → PositionDeleteGroup 的 helper，sink 用 |
| 一次性 `InMemoryCatalog::default()` + `register` + `strip_catalog_from_three_part_names` | **删除**（leaf-swap 路径不再需要） |

---

## 5. 新算子契约

### 5.1 `IcebergDeltaScan`

```rust
// src/exec/node/iceberg_delta_scan.rs（新增）
pub struct IcebergDeltaScanNode {
    pub base_table_ident: TableIdent,                  // catalog.namespace.table
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    pub base_projection: Vec<SlotId>,                  // _row_id + SELECT 引用列
    pub change_files: Vec<DeltaSourceFile>,
    pub apply_key_source: ApplyKeySource,              // BaseRowId（A9）
    pub object_store_config: Option<ObjectStoreConfig>,
    pub iceberg_runtime_handles: IcebergRuntimeHandles,
    pub node_id: i32,
}

pub enum DeltaSourceRole {
    DataFile,                                          // → __change_op = +1
    PositionDelete { target_data_files: ... },         // → 反向重建为 __change_op = -1
    EqualityDelete { equality_keys: ... },             // → 反向重建为 __change_op = -1
    DeletedDataFile,                                   // → 全文件输出 __change_op = -1
}

pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<...>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}
```

**OperatorFactory 行为**：

- `is_source() == true`
- 创建 `IcebergDeltaScanOperator`，per-driver 持有一段 `pending_files` 切片（multi-driver 时 morsel-style 分配；A1 第一版可以单 driver 跑通再开 multi-driver）
- 输出 schema = `base_projection` 对应的列 + 隐式 `__change_op: Int8`（C-a 透明列，不在 schema 显式声明，每个 chunk 携带）

**`pull_chunk` 行为**（streaming state machine）：

```rust
fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
    loop {
        if self.current_scanner.is_none() {
            let Some(next) = self.pending_files.pop_front() else { return Ok(None); };
            self.current_scanner =
                Some(open_scanner_for_role(next, &self.locator_index, ...)?);
        }
        match self.current_scanner.as_mut().unwrap().next_chunk(&self.base_projection)? {
            Some(chunk) => {
                // chunk 内已带 __change_op 常量列
                return Ok(Some(chunk));
            }
            None => { self.current_scanner = None; /* 进下一个文件 */ }
        }
    }
}
```

**Scanner per-role 行为**：

- `DataFile`：标准 parquet reader（复用 `FileScanContext` / opendal reader），每个出来的 RecordBatch 加 `__change_op = +1` 常量列。
- `PositionDelete`：读 delete file 得 `(file_path, pos)` → 按 target file group → 对每个 target file 用 base row_id index 反向查 → 输出每行加 `__change_op = -1`。
- `EqualityDelete`：读 delete file 得 equality value set → 扫 target data files 应用 equality match → 输出每行加 `__change_op = -1`。
- `DeletedDataFile`：parquet reader 顺序读全文件，每行加 `__change_op = -1`。

### 5.2 `IcebergMergeSinkFactory`

```rust
// src/engine/mv/iceberg_merge_sink.rs（新增）
pub struct IcebergMergeSinkFactory {
    target_table: iceberg::table::Table,
    target_ident: TableIdent,
    collector: Arc<IcebergCommitCollector>,
    locator_state: Option<TargetLocatorState>,    // L1：refresh driver 预加载注入
    apply_key_column: String,                     // __nova_base_row_id
    apply_key_field_id: i32,
    output_layout: Layout,
    output_schema: SchemaRef,
    file_format: String,                          // 目前仅 "parquet"
    compression: TCompressionType,
    object_store_config: Option<ObjectStoreConfig>,
}

pub struct TargetLocatorState {
    pub existing_deletes_by_file: HashMap<String, ExistingDeleteState>,
    pub referenced_data_file_partitions: ...,
}
```

**OperatorFactory 行为**：

- `is_sink() == true`
- 创建 `IcebergMergeSinkOperator`，per-driver 持有 sink-local writer 状态

**`push_chunk` 行为**：

```rust
fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
    let change_op_arr = chunk.column_by_name("__change_op")?.as_int8();
    let (insert_rows, delete_rows) = partition_by_change_op(&chunk, change_op_arr)?;

    if !insert_rows.is_empty() {
        self.data_writer.write(insert_rows).await?;       // rolling 时调 collector.inject_written_file
    }
    if !delete_rows.is_empty() {
        let apply_keys = extract_apply_key_values(&delete_rows, self.apply_key_field_id)?;
        let groups = locate_target_rows_by_apply_key(
            self.locator_state.as_ref().expect("delete arrived but no locator loaded"),
            apply_keys,
        )?;
        for g in groups { self.collector.inject_delete_group(g); }
    }
    Ok(())
}
```

**`set_finishing` 行为**：

- Flush 剩余 DataFileWriter，对每个 WrittenFile 调 `collector.inject_written_file`
- **不调** `run_iceberg_commit`——该调用留 refresh driver

### 5.3 错误传播

- `IcebergDeltaScan` 中途失败（IO / 反扫错误）→ pipeline error → `execute_plan` 返回 Err → refresh driver 捕获 → `abort_iceberg_mv_refresh` + cleanup（A7 已有路径）
- Sink 中途失败 → 同上；collector 累计的 staged files 由 abort path 清理
- collector 在 sink finalize 后仍未 commit → refresh driver 错误处理走 [iceberg_refresh.rs:2152 `handle_iceberg_mv_commit_error`](../../../src/engine/mv/iceberg_refresh.rs:2152)（已有）

---

## 6. 验收测试（T-a）

### 6.1 实现层硬保证（不依赖测试）

A1 PR 直接**删除**：

- [mv_flow.rs:282 `write_mv_delete_temp_parquet`](../../../src/engine/mv_flow.rs:282)
- [mv_flow.rs:387 `execute_query_for_mv_incremental_deletes`](../../../src/engine/mv_flow.rs:387)
- [mv_flow.rs:230 `execute_query_for_mv_incremental_refresh`](../../../src/engine/mv_flow.rs:230)
- 相关 `std::env::temp_dir` / `std::fs::create_dir_all` / `std::fs::File::create` 调用

删干净之后"无本地磁盘也跑得通"是编译期 + 类型系统保证的事实。

### 6.2 SQL 测试新增

`tests/sql-test/iceberg-ivm/` 新增两个 case：

- **`large_delta_mixed.sql`**：base 表预填 100MB+（≥ 1M 行），执行 INSERT 1k 行 + DELETE 1k 行（用 position-delete 触发反向重建），refresh MV，验证结果与 full refresh 等价。
- **`update_only.sql`**：base 表 UPDATE 一批行（Iceberg merge-on-read 产生 new data file + position-delete 对），refresh MV，验证 target 表对应行被正确替换。

### 6.3 既有 suite 保持

iceberg-ivm suite 现有 4 个 case 必须全过；mv-on-iceberg suite 当前 12/17 baseline 不允许下降。

### 6.4 Spill 验证（非 A1 范围）

A1 把内存峰值压到单 chunk 是 spill 的前提；spill 实际触发的 metric 验证 + chaos 测试另起 PR。

---

## 7. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Streaming state machine bug（多文件、跨 chunk 边界、locator 索引共享、driver 间 morsel 分配） | iceberg-ivm 现有 4 case + 新 2 case 覆盖；先在单 driver 跑通再开 multi-driver |
| 反向重建 helper 从 changes.rs 搬家时语义漂移 | helper 保留单元测试；搬家时 `cargo test` 全套必跑 |
| Locator 状态在 multi-driver sink 下并发访问 | `locator_state` 包 `Arc`（只读）；`collector` 注入已带内部锁（[collector.rs:122](../../../src/connector/iceberg/commit/collector.rs:122)） |
| DataFileWriter rolling boundary 与 chunk 边界不一致 → 小文件 | 沿用 [sink.rs](../../../src/connector/iceberg/sink.rs) 现有 rolling 策略（per-driver buffer 到阈值再 flush） |
| Iceberg writer API 是 async，pipeline 是 sync | sink 内 `data_block_on` 包 async，与现有 sink 一致 |
| Leaf-swap pass 找错 leaf | A1 阶段 MV 限定单 base + 单 scan leaf（A11 contract 已强制）；leaf-swap 找不到唯一 base scan 时 fail fast |
| `__change_op` 透明列被 optimizer 误丢 | A4 透明列保留机制必须保住；新 SQL case 断言 sink 端 chunk 含 `__change_op` |

---

## 8. A1 不解决的（明示）

- managed-lake MV 的 temp parquet 路径（[ivm_delta_source.rs:81](../../../src/connector/starrocks/managed/ivm_delta_source.rs:81)）保留
- Spill 实际触发验证（runtime metric / chaos 测试）
- 多基表 / join IVM（依赖 A3）
- 任意 `[from, to]` snapshot range（依赖 A2）
- POC 文档"路线一"完整 plan rewrite framework
- 上游 `iceberg-rust` 新增 `IncrementalChangelogScan` 后是否切换（A1 在 NovaRocks 端手写反向重建，未来若 iceberg-rust 0.10+ 提供同款 API 可考虑迁移）

---

## 9. 参考

- POC 路线方案（Google Doc，2026-05-14 brainstorm 阶段读取）
- [IVM-A1 待办文档](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/IVM-A1-delta-pipeline.md)
- [IVM-A4 设计](2026-05-11-ivm-a4-changeop-execplan-design.md)
- [IVM-A7 设计](2026-05-13-ivm-a7-branch-staged-refresh-transaction-design.md)
- [IVM-A9 设计](2026-05-13-ivm-a9-iceberg-target-row-identity-apply-design.md)
- [IVM-A11 设计](2026-05-14-ivm-a11-mv-schema-field-id-contract-design.md)
- StarRocks 参考 [IvmDeltaIcebergScanRule.java](file:///Users/harbor/project/starrocks/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/ivm/IvmDeltaIcebergScanRule.java)（scan + trait + project 模式，append-only first；NovaRocks 形态 1 是它在没有 `IncrementalChangelogScan` API 时的等价实现）
