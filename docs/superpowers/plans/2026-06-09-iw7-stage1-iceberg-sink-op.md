# IW-7 阶段 1 实现计划：PhysicalIcebergSinkOp + 分布式 INSERT plan（路径 B）

> **For agentic workers:** REQUIRED SUB-SKILL：用 superpowers:subagent-driven-development 逐 task 执行。Steps 用 `- [ ]` 跟踪。
>
> **诚实说明（重要）**：这是「引入一个新 physical operator + 跨 optimizer/codegen/engine 接线」的**探索性工程**，不是照现有模式抄。计划里**已确认的真实代码锚点**直接给（见各 task 的「锚点」），但有 3 个机制是 Explore 阶段确认为「需实现时随编译器/真实接口定」的——它们作为**显式 SPIKE task**（不是 placeholder，是真实工程步骤），必须先验证再写下游代码。逐 task 用 subagent-driven 执行、每步编译验证。

**Goal：** 让 standalone `INSERT INTO <iceberg> SELECT/VALUES` 经「SELECT → partition-hash exchange → ICEBERG_TABLE_SINK fragment」的分布式 plan 执行（behind flag，默认关），替代当前的 engine 本地收集写。

**Architecture：** 路径 B——保持 INSERT 在 engine 入口；engine 拿 SELECT 的 optimized physical plan 后在顶部包 `PhysicalIcebergSinkOp`（require partition distribution），复用 optimizer distribution enforcement 插 exchange，codegen 把它 visit 成 root fragment 的 `ICEBERG_TABLE_SINK`。

**Tech Stack：** Rust；NovaRocks optimizer（cascades）/codegen（fragment_builder）/engine；现有 `AsyncSinkOperator`+`IcebergTableSinkFactory`+`WriteCoordinator`（已就位，本阶段只负责「生成并分发带 sink 的 plan」）。

---

## 文件结构（本阶段触及）

- `src/sql/optimizer/operator.rs`：新增 `PhysicalIcebergSinkOp` struct + `Operator::PhysicalIcebergSink` 变体。
- `src/sql/optimizer/derive/mod.rs`：`derive_required` / `derive_output` dispatch 加分支。
- `src/sql/optimizer/operator.rs`（或 derive 子模块）：`PhysicalIcebergSinkOp::derive_required`（require HashPartitioned）/`derive_output`。
- `src/sql/codegen/fragment_builder.rs`：`visit` dispatch 加 `visit_iceberg_sink`；新增 `build_iceberg_table_sink()`；root `output_sink` 按 op 设。
- `src/engine/iceberg_writer.rs`：`execute_iceberg_insert_or_overwrite` 新增 behind-flag 的分布式分支。
- `src/engine/mod.rs`：query 执行链暴露「拿 optimized physical plan + 包 sink op + enforce + codegen」的入口。

---

## SPIKE 区（必须先验证，再写下游 task）

> 这 3 点决定下游 complete-code 的真实形态。每个 SPIKE 产出一段「最小验证代码 + 结论」，写进本文件对应位置后再继续。

### SPIKE-A：optimize() 之后怎么对新包的 sink op 触发 distribution enforcement

**问题**：`needed_enforcers`/插 `PhysicalDistributionOp` 跑在 `optimize()` 的 cascades 搜索内（`src/sql/optimizer/derive/mod.rs:183`、`search.rs`）。路径 B 在 optimize **之后**包 `PhysicalIcebergSinkOp`，需要让它下方按 partition key 出现一个 `PhysicalDistributionOp`。

**验证三选一并定论**（读 `search.rs` + `derive/mod.rs` 后选）：
- (a) 暴露一个 `enforce_required_on_root(plan, required: PhysicalPropertySet) -> plan` 的轻量函数，engine 包 sink 后调它补 enforcer；
- (b) codegen `visit_iceberg_sink` 时，依据 op 的 partition requirement **直接构造**一个 `PhysicalDistributionOp` 套在子树上，复用 `visit_distribution`（`fragment_builder.rs:4243`）；
- (c) 让 INSERT 走一个「带 sink 的 logical plan 进 optimize()」的最小入口（偏路径 A，仅当 a/b 都不可行时）。

**锚点**：`needed_enforcers`（derive/mod.rs:183）、`DistributionSpec::hash_partitioned`（property.rs:209）、`collapse_distribution_enforcers_for_single_fragment`（engine/mod.rs，all-in-one 折叠 enforcer 的现有逻辑——读它理解 enforcer 在 plan 里的形态）。
**产出**：选定 (a)/(b)/(c) + 一段把「SELECT plan + 顶部 sink op」跑出「sink 下有 HashPartitioned 分布」的最小测试。

**✅ 结论（2026-06-09）：选 (a)，且比预想轻。** `optimize()`（mod.rs:61）产出的 physical tree 里 enforcer 已是实体节点（`PhysicalDistributionOp{ spec }`，operator.rs:381）；`needed_enforcers`（derive/mod.rs:183）是纯函数。故 optimize 后包 sink op 时**手动补 root enforcer**：拿 SELECT root 的 provided distribution（`derive_output`）vs sink 要求的 `HashPartitioned{partition keys}`，`needed_enforcers` 算差异，需要就在 sink 与 SELECT root 之间插 `Operator::PhysicalDistribution(PhysicalDistributionOp{ spec: DistributionSpec::hash_partitioned(partition_keys, ..) })`。**不侵入 cascades 搜索、不让 INSERT 进 optimize。** 额外收益：all-in-one 下现有 `collapse_distribution_enforcers_for_single_fragment`（engine/mod.rs:2525）会折叠该 exchange → 单 writer、无 shuffle 开销，直接缓解 all-in-one 性能红线。

### SPIKE-B：INSERT target 信息怎么从 engine 入口传到「包 sink」处

**问题**：`execute_query_with_options`（engine/mod.rs:2750）入参是 SELECT 的 `sqlparser::ast::Query`，**不含** INSERT target。target（iceberg 表元数据、partition spec、输出列对齐）在 `execute_iceberg_insert_or_overwrite`（iceberg_writer.rs:61）的 `ResolvedTable`。

**验证**：设计一个把 target（`target_table_id`/partition key 列/输出列）带入「拿 optimized plan 后包 sink」那一步的入参或新入口（例如给 `execute_query_with_options` 加一个 `Option<IcebergSinkSpec>` 参数，或新增 `execute_insert_to_iceberg_distributed(...)`）。
**锚点**：`run_select_to_chunks`（iceberg_writer.rs:431）、`execute_query_with_options_and_imv_validator_with_catalog_provider`（engine/mod.rs:2804，optimize 在 :2844 之后、codegen 在其后）。
**产出**：定下入口签名 + target 如何映射到 `PhysicalIcebergSinkOp` 字段。

**✅ 结论（2026-06-09）：有现成参照。** `execute_query_with_options`（mod.rs:2750）已带 `terminal_sink: Option<Box<dyn OperatorFactory>>` 参数位（现成的「给 query 套终端 sink」机制，但属单 fragment / pipeline operator 层）。路径 B 在更上游：给 execute_query 链加 `Option<IcebergSinkSpec>{ target_table_id, partition_key_column_ids, output_exprs }`，optimize 后据它包 `PhysicalIcebergSinkOp`。target 源自 `execute_iceberg_insert_or_overwrite`（iceberg_writer.rs:61）的 `ResolvedTable` + iceberg 表元数据；`ResolvedTable` 确切字段在 Task 5 实现时定位对齐。

### SPIKE-C：desc table 怎么注册 iceberg target table（target_table_id → 元数据）

**问题**：`IcebergTableSinkFactory::try_new`（sink.rs:121）用 `resolve_iceberg_table(desc_tbl, target_table_id)` 从 descriptor table 取 iceberg 表。standalone codegen 当前不注册写 target 表到 desc table。
**验证**：确认 desc table builder 怎么加 iceberg table 条目（FE-compat 侧 desc table 里 iceberg 表怎么来的，standalone 怎么复刻）。
**锚点**：`DescriptorTableBuilder`（fragment_builder.rs，`add_slot_with_type_desc`/`add_tuple` 等）、`resolve_iceberg_table`（sink.rs）。
**产出**：standalone codegen 注册 iceberg target table 到 desc table 的最小路径。

**✅ 结论（2026-06-09）：复用现有 scan 侧机制。** codegen 已有 iceberg table → desc table 注册：`iceberg_table_info`（fragment_builder.rs:374）+ `resolve_iceberg_table`（sink.rs:720，从 desc table 取 `descriptors::TIcebergTable`）。sink 复用同一注册路径把 target iceberg table 放进 desc table（`target_table_id` → `TIcebergTable`）；`iceberg_table_info` 复用细节在 Task 4 实现时确认。

---

## Task 1：新增 `PhysicalIcebergSinkOp`（op 定义，可独立编译）

**Files：** Modify `src/sql/optimizer/operator.rs`

- [ ] **Step 1：定义 struct**（锚点：`PhysicalProjectOp` @ operator.rs:311-314；字段类型 `items: Vec<ProjectItem>` 的 `ProjectItem`/`TypedExpr` 在写时按 operator.rs 现有定义对齐）。

```rust
#[derive(Clone, Debug)]
pub(crate) struct PhysicalIcebergSinkOp {
    pub target_table_id: i64,
    pub output_exprs: Vec<ProjectItem>,        // 复用 PhysicalProject 的表达式承载类型
    pub partition_key_column_ids: Vec<ColumnId>, // require HashPartitioned 用
}
```

- [ ] **Step 2：加 `Operator` 枚举变体**（operator.rs:509 附近，紧邻 `PhysicalDistribution`）：`PhysicalIcebergSink(PhysicalIcebergSinkOp),`
- [ ] **Step 3：补 `Operator` 的 `is_physical()` / `Debug` / children 访问等**——按编译器报的「missing match arm」逐个补（这些 match 在 operator.rs；编译器会精确指出位置）。
- [ ] **Step 4：`cargo build -p ...` 编译通过**（dev profile）。Expected：无 missing-arm 错误。
- [ ] **Step 5：commit**（`feat(iw7): add PhysicalIcebergSinkOp operator variant`）。

## Task 2：`derive_required` / `derive_output`（require partition distribution）

**Files：** Modify `src/sql/optimizer/operator.rs`（impl）、`src/sql/optimizer/derive/mod.rs`（dispatch）

- [ ] **Step 1：写失败测试**（锚点：derive tests @ derive/mod.rs:227+）——构造 `PhysicalIcebergSinkOp{partition_key_column_ids:[ColumnId(1)]}`，断言 `derive_required(..)` 返回的单子节点 required distribution 是 `HashPartitioned{cols:[ColumnId(1)]}`；非分区（空 partition keys）返回 `Any`。
- [ ] **Step 2：跑测试看失败**（`cargo test -p <crate> physical_iceberg_sink_derive`）。Expected：FAIL（dispatch 未加 / 方法未实现）。
- [ ] **Step 3：实现 `PhysicalIcebergSinkOp::derive_required`**：

```rust
pub(crate) fn derive_required(
    &self, _parent: &PhysicalPropertySet, _num_children: usize,
) -> Vec<PhysicalPropertySet> {
    let dist = if self.partition_key_column_ids.is_empty() {
        DistributionSpec::Any
    } else {
        DistributionSpec::hash_partitioned(
            self.partition_key_column_ids.iter().copied(),
            HashSource::ShuffleAgg, // 复用现有 shuffle 语义；SPIKE-A 若引入专用 source 再改
        )
    };
    vec![PhysicalPropertySet { distribution: dist, ordering: OrderingSpec::Any }]
}
```

并加 `derive_output`（sink 输出 distribution 取子节点的；ordering Any）。
- [ ] **Step 4：在 `derive/mod.rs:125` 的 `derive_required` match + `derive_output` match 加 `Operator::PhysicalIcebergSink(o) => o.derive_required(...)` 分支。**
- [ ] **Step 5：跑测试通过。** Expected：PASS。
- [ ] **Step 6：commit**（`feat(iw7): PhysicalIcebergSinkOp requires hash-partitioned input`）。

## Task 3：执行 SPIKE-A，定 enforcement 触发方式并落地

- [ ] **Step 1：** 按 SPIKE-A 读 `search.rs` + `collapse_distribution_enforcers_for_single_fragment`，选定 (a)/(b)/(c)。
- [ ] **Step 2：** 写一个测试：输入「简单 SELECT 的 optimized physical plan + 顶部包 `PhysicalIcebergSinkOp(partition_keys=[c1])`」，断言处理后 sink 之下存在 `PhysicalDistribution(HashPartitioned[c1])`。
- [ ] **Step 3：** 实现选定方案（若 (a)：在 optimizer 暴露 `enforce_required_on_root`；若 (b)：留到 Task 4 在 codegen 处理，本 task 只产出结论与测试）。
- [ ] **Step 4：** 测试通过 + commit。

## Task 4：codegen —— `visit_iceberg_sink` + `build_iceberg_table_sink`

**Files：** Modify `src/sql/codegen/fragment_builder.rs`（依赖 SPIKE-C 的 desc table 结论）

- [ ] **Step 1：写 `build_iceberg_table_sink()`**（锚点：`build_result_sink` @ fragment_builder.rs:4799 的完整 16-arg `TDataSink::new`；`TIcebergTableSink` 字段见 idl/thrift/DataSinks.thrift:248）。除被填字段外其余 sink 全 `None`，type=`ICEBERG_TABLE_SINK`。
- [ ] **Step 2：写 `visit_iceberg_sink`**（锚点：`visit_project` @ fragment_builder.rs:1879 的表达式 lower 模式 `ExprCompiler::compile_typed`；`visit_distribution` @ :4243 的 fragment 切分）：visit 子树 → lower output_exprs → 把 **root fragment** 的 `output_sink` 设为 `build_iceberg_table_sink(..)`（依据 SPIKE-C 注册 desc table + 分配 tuple_id）。若 SPIKE-A 选 (b)，在此构造 `PhysicalDistribution` 套子树。
- [ ] **Step 3：`visit` dispatch（fragment_builder.rs:1145）加 `Operator::PhysicalIcebergSink(op) => self.visit_iceberg_sink(op, node),`**
- [ ] **Step 4：单测**（锚点：fragment_builder tests @ :4888+）：构造含 `PhysicalIcebergSinkOp` 的 `PhysicalPlanNode`，`PlanFragmentBuilder::build(..)` 后断言 root `output_sink.type_ == ICEBERG_TABLE_SINK` 且 `iceberg_table_sink.target_table_id` 正确、子 fragment `output_partition` 为 HashPartitioned。
- [ ] **Step 5：测试通过 + commit。**

## Task 5：engine 入口 —— 包 sink + 跑分布式 plan（behind flag）

**Files：** Modify `src/engine/iceberg_writer.rs`、`src/engine/mod.rs`（依赖 SPIKE-B）

- [ ] **Step 1：加 session/config flag**（默认关，关时走现有本地写路径）。锚点：现有 session 变量机制（`src/sql/optimizer/options.rs` 风格）。
- [ ] **Step 2：** 按 SPIKE-B 定的入口，在 `execute_iceberg_insert_or_overwrite`（iceberg_writer.rs:61）flag 开时：构造 `IcebergSinkSpec`（target_table_id/partition keys/输出列）→ 拿 SELECT optimized plan → 顶部包 `PhysicalIcebergSinkOp` → enforce（SPIKE-A）→ codegen → `ExecutionCoordinator::execute_with_write_outcome` → 用返回的真实 `WriteCommitInput` 喂 `IcebergWriteTransactionRunner`（替换 `synthetic_write_commit_input`，write_transaction.rs:202）。
- [ ] **Step 3：** flag 关时路径不变（断言：现有 iceberg INSERT 单测不回归）。
- [ ] **Step 4：commit。**

## Task 6：all-in-one end-to-end（behind flag 开）

**Files：** Test only（锚点：tests/standalone_mysql_server.rs INSERT iceberg 测试；或 sql-tests iceberg-rest）

- [ ] **Step 1：** all-in-one 开 flag 跑 `INSERT INTO <iceberg 分区表> VALUES`、`INSERT INTO ... SELECT`，断言：写成功 + 后续 SELECT 结果正确 + 文件落在对的 partition。
- [ ] **Step 2：** 关 flag vs 开 flag 结果一致。
- [ ] **Step 3：** all-in-one 性能抽查（spec 第 2 节红线）：单机 INSERT 不显著回退。
- [ ] **Step 4：commit。**

> **多 BE 验证（1FE+2BE 一致性 + 分区布局 + error path）= spec 第 9 节阶段 3**，本 plan 不含；阶段 1 落地 + all-in-one 绿后另起 plan。

---

## Self-Review 摘要

- **Spec 覆盖**：覆盖 spec 第 5 节 A 的 1a（Task 1-3）/1b（Task 4）/1c（Task 5）+ all-in-one 验收（Task 6）。多 BE（阶段 3）、OVERWRITE/DELETE（IW-8/9）明确不在本 plan。
- **Placeholder**：3 个 SPIKE 是显式工程步骤（探索性任务的真实需要），非含糊占位；其余 task 给了真实锚点（文件:行 + 现有模式）。下游 task（4/5）显式依赖 SPIKE 结论，顺序已锁。
- **类型一致**：`PhysicalIcebergSinkOp` 字段（target_table_id/output_exprs/partition_key_column_ids）在 Task 1 定义、Task 2/4/5 一致引用。

## Execution Handoff

推荐 **subagent-driven-development**：每个 task（尤其 3 个 SPIKE）一个 fresh subagent，读真实代码 → 写 → 编译/测试 → review。理由：本阶段是「引入新 op + 跨子系统接线」的探索性工程，多处接口要随编译器反馈和真实类型定，边做边确认远胜提前写死。
