# PlanNode IR + EXPLAIN Observability — Design

Date: 2026-06-15
Status: Design (approved direction; ready for implementation-plan decomposition)
Scope: `src/sql/codegen/**`, `src/sql/explain.rs`, `src/engine/mod.rs` explain/execute entrypoints.

## 1. 背景与问题

NovaRocks standalone 的计划下沉今天是**两层**，缺了中间层：

- 优化器产出 `PhysicalPlanNode`（`src/sql/optimizer/physical_plan.rs`，携带 `TypedExpr` + `ColumnId`，**分片前、无 node id**）。
- `fragment_builder.rs` 遍历 `PhysicalPlanNode`，**直接吐 thrift `TPlan`/`TPlanNode`**（`VisitResult.plan_nodes: Vec<TPlanNode>`），并在同一次遍历里完成分片、id 分配、slot 分配、表达式编译、RF/dict/CTE 接线。
- EXPLAIN 从**另一棵树**渲染：`format_physical_node` 直接走 `PhysicalPlanNode`（分片前），所以"EXPLAIN 显示的"和"实际执行的"是两棵不同树的两套独立渲染。

由此带来两个可观测性缺口（本设计要补的两点）：

1. **EXPLAIN VERBOSE** 缺少 StarRocks 的 fragment 维度结构：没有 `PLAN FRAGMENT N`、没有 input/output partition、没有 node id 前缀、没有 distribution/cost 的可解释呈现。它渲染的是分片前的 `PhysicalPlanNode`，结构上根本展示不了真正的分布式形态。
2. **EXPLAIN ANALYZE** 是"假 ANALYZE"：`explain_analyze_query` 重复 plan、以 `profiler=None` 执行、只打印估算值（`stats={rows=N}`），没有任何 per-operator 实际值。`ExplainLevel::Analyze` 的节点文本与 Verbose 完全相同（见 `explain.rs` 测试 `analyze_level_matches_verbose_for_exact_stats`）。

根因是**缺少 StarRocks `PlanNode`/`PlanFragment` 那层"分片后、带 id、explain 与 thrift 共同派生"的中间 IR**。

## 2. 目标与非目标

### 目标
- 引入真正的三层 IR：`PhysicalPlanNode → PlanNode/PlanFragment(单一来源) → thrift`，EXPLAIN 与执行都从 IR 派生。`node_id` 全程不重分配。
- EXPLAIN VERBOSE 呈现 StarRocks **结构形态**（fragment 分组、partition、node id 前缀、distribution、cost），**保留 NovaRocks 现有节点词汇**（结构对齐，不追求逐 token 一致）。
- EXPLAIN ANALYZE 单次 build + 带 profiler 执行 + 按 `node_id` 关联，呈现 per-node **actual vs estimate**。
- 长期可演进：IR 为后续"分片后计划变换"提供落点（本设计不引入任何变换）。

### 非目标
- 不改优化器（`PhysicalPlanNode`/`Operator`/cost 冻结）。
- 不改 thrift wire 格式（`TPlan`/`TPlanNode`/`TDescriptorTable`/`TPlanFragmentExecParams`/`TDataSink` 不变；`src/lower/**` thrift→ExecNode 与 `src/exec/**` 执行不变）。
- 不改 FE-compatible 路径（`internal_service`/`lower/fragment.rs` 接收 FE thrift，不受影响）。本重构是 standalone 模式的计划下沉。
- 不扩展 per-operator 运行时计数器 schema：ANALYZE 复用现有 `Profiler` 已有计数器（rows/time/peak_mem）；更丰富的指标是后续工作。
- 无向后兼容 shim（NovaRocks 无历史用户，直接改格式）。

## 3. 目标架构

### 3.1 三层与数据流

```
LAYER 1  PhysicalPlanNode (优化器输出, 不动)
           • Operator enum + Statistics + execution_props，TypedExpr + ColumnId
                 │  build_ir()  —— Pass 1（唯一结构遍历）
                 ▼
LAYER 2  PlanNode / PlanFragment / FragmentedPlan  ←—— 单一来源 (src/sql/codegen/ir/**)
           • PlanNode: node_id, fragment_id, tuple_ids, nullable_tuple_ids, limit,
             children, conjuncts(TypedExpr), stats(owned), body(enum)
           • PlanFragment: root, data_partition(in/out), sink, output_exprs, dicts, direct_exec
           • FragmentedPlan: Vec<PlanFragment> + edges + root_fragment_id + rf_plan
                 │                                        │
        explain_fragmented(&fp)                   lower_fragmented(&fp)  —— Pass 2
                 ▼                                        ▼
LAYER 3a EXPLAIN 文本                          LAYER 3b thrift TPlan/TPlanNode + TDescriptorTable
         (Normal/Verbose/Costs/Analyze)                  + TPlanFragmentExecParams + TDataSink
                                                         → src/lower/node/lower_plan（不变）
```

单次 build、两个消费者：

```
optimize() → PhysicalPlanNode
               │ build_ir()   (Pass 1：结构/身份/拓扑)
               ▼
            FragmentedPlan  ← 单一来源
            ┌──────────────┴───────────────┐
   explain_fragmented(&fp)          lower_fragmented(&fp)   (Pass 2：编译/绑定/序列化)
      → Vec<String>                    → MultiFragmentBuildResult
   (VERBOSE/ANALYZE)                   → execute_plan / coordinator（不变）
```

EXPLAIN ANALYZE 额外：用 profiler 跑执行路径，再按 `node_id` 把合并后的 profile 贴回 IR。

### 3.2 StarRocks 对应

| StarRocks (Java) | NovaRocks 目标 (Rust) |
|---|---|
| `PhysicalOperator` / `OptExpression` | `PhysicalPlanNode` / `Operator`（Layer 1）|
| `PlanNode`（抽象基类 + 每算子子类）| `PlanNode` struct + `PlanNodeBody` enum |
| `PlanFragment` | `PlanFragment` |
| `DataPartition`（UNPARTITIONED/RANDOM/HASH/BUCKET_SHUFFLE）| `DataPartition` |
| `DataSink`→`DataStreamSink`/`ResultSink` | `DataSink` enum：`DataStream`/`Result`/`Iceberg`/`Noop` |
| `ExecPlan`（id 生成器 + DescriptorTable + fragments）| `FragmentedPlan` + `IrBuilder`/`LoweringCtx`（避开与运行时 `ExecPlan` 重名）|
| `PhysicalPlanTranslator.visit*` | `fragment_builder::IrBuilder` 的 visit* |
| `PlanNode.treeToThrift()` | `lower_fragmented` / `PlanNode → TPlanNode`（Pass 2）|
| `getVerboseExplain()`/`getCostExplain()` | `PlanNode::explain(node, level, ctx)`|

**与 StarRocks 的一处刻意分歧**：StarRocks 在翻译时即编译 `Expr` 并 eager 绑定 slot——正是今天 NovaRocks 的做法、也是我们要解开的耦合。我们让 IR 持 `TypedExpr`，把编译/slot 绑定收进 Pass 2。注意（见 §5.1 修正）：这里不是"Pass 2 完全延迟、slot id 逐字节相同"，而是"Pass 2 内部仍逐节点 compile→定类型→分配 slot"，只是由 IR 驱动、与结构遍历解耦。

## 4. IR 类型设计

新模块树 `src/sql/codegen/ir/`（`mod.rs, node.rs, body.rs, fragment.rs, partition.rs, sink.rs, expr_source.rs, lowering.rs, explain.rs`），可见性 `pub(crate)`。

### 4.1 `PlanNode`

```rust
// ir/node.rs
pub(crate) struct PlanNode {
    pub node_id: i32,                  // Pass 1 分配；全程不重分配。在被 lower 的 fragment 内，
                                       // 每个 PlanNode 都恰好产出一个 thrift TPlanNode，故 node_id
                                       // == TPlanNode.node_id == profile 的 plan_node_id（1:1）。
    pub fragment_id: FragmentId,       // fragment close 时确定
    pub tuple_ids: Vec<i32>,           // thrift row_tuples / explain tuple ids
    pub nullable_tuple_ids: Vec<i32>,  // outer-join 加宽侧
    pub limit: i64,                    // -1 = 无 limit（与 thrift 约定一致）
    pub children: Vec<PlanNode>,       // 仅 fragment 内子节点；exchange 是叶子，跨片连接在 FragmentedPlan.edges
    pub conjuncts: Vec<TypedExpr>,
    pub stats: PlanNodeStats,          // owned 拷贝，explain trailer + ANALYZE estimate 列
    pub body: PlanNodeBody,
}
```

- `PlanNodeStats` 是 owned 小拷贝（`output_row_count: f64`、`confidence`、Costs 用的 per-column 拷贝），**不引用** Layer 1——IR 必须自包含，explain/ANALYZE 永不回触 `PhysicalPlanNode`。
- **不设独立的 `explain_extra` 字段**（critique LOW-2）：所有显示所需事实都进 body，维持"单一来源"。仅当某个纯 build-time 事实在 body 里确无落点时，才在该 body 内用 `Option` 字段承载。

### 4.2 `PlanNodeBody`——enum 不用 trait

```rust
// ir/body.rs
pub(crate) enum PlanNodeBody {
    Scan(Box<ScanBody>),
    Project(ProjectBody),
    // 无 Filter 变体：Filter 谓词在 Pass 1 折叠进子节点的 conjuncts / ScanBody.predicates
    // （镜像今天的 visit_filter，0 thrift 节点），与 StarRocks「谓词挂在节点 conjuncts 上」一致。
    HashJoin(Box<HashJoinBody>),
    NestLoopJoin(NestLoopJoinBody),
    HashAggregate(Box<AggregateBody>),
    Sort(SortBody),
    TopN(TopNBody),
    Limit(LimitBody),
    Window(Box<WindowBody>),
    AssertOneRow(AssertOneRowBody),
    Exchange(ExchangeBody),             // PhysicalDistribution / CTE consume / TopN-split / Limit-offset 的统一 IR 面
    Values(ValuesBody),
    GenerateSeries(GenerateSeriesBody),
    TableFunction(TableFunctionBody),
    Repeat(Box<RepeatBody>),
    SetOperation(SetOpBody),            // UnionAll/Intersect/Except；kind 在内部
    Decode(DecodeBody),
    CteConsume(CteConsumeBody),         // thrift 上是 EXCHANGE；单独 body 仅为 explain 文本
    AggregateStateMerge(AggStateMergeBody),  // 不经 to_thrift，走 DirectExecPlan
}
```

**决策：enum。** 理由：算子集封闭且小（~20）；穷尽 `match` 强制每个新算子在 `to_thrift` 与 `explain` 都被处理（正是"IR 是单一来源"要的不变量）；兄弟节点 pattern-match（Limit 并入子 Sort、TopN-split 关 partial fragment、Filter 推入兄弟 Scan）用 enum 干净，trait 需 downcast 更糟；`to_thrift` 一个大 `match` 与现有 `nodes::build_*_node` 近 1:1，迁移机械。**重变体预先 box**（critique LOW-1：`ScanBody/HashJoinBody/AggregateBody/WindowBody/RepeatBody` 直接 `Box`，不等 clippy 报）。

### 4.3 算子 body（覆盖全部 24 个 inventory 条目）

每个 body 持 **NovaRocks 词汇的结构化字段**，足以 (a) 渲染与今天一致的 explain 文本、(b) 再生精确的 `TPlanNode`。规则：存 `TypedExpr` + `ExprSource`（§5.3），**绝不存预编译 `TExpr`**。

要点 body（完整列表见实现）：
- `ScanBody`：database/table/alias/columns/predicates(TypedExpr)/dict_columns/variant_columns/mv_rewritten_from/scan_kind/`planned: Option<PlannedConnectorScan>`（Pass 1 捕获连接器规划结果，§5.6）/min_max_hints。1 thrift 节点。
- Filter：**无 body 变体、不产 IR 节点**。Pass 1 把谓词折叠进子节点的 `PlanNode.conjuncts`（或 `ScanBody.predicates`），镜像今天 `visit_filter` 的 0-thrift-节点行为；谓词在子节点的 explain 行上呈现。这消除了"有 IR 节点却无 thrift 节点"的特例，使每个被 lower 的节点 1:1 对应 thrift。
- `ProjectBody`：items/common_subexprs/`dict_passthrough: Vec<DictPassthrough>`(§5.5)/output_qualifier。1 节点。
- `HashJoinBody`：join_type/eq_conditions(TypedExpr)/other_condition/distribution/`exec_distribution`(explain label)/build_runtime_filters(结构化)/output_columns。1 节点。
- `NestLoopJoinBody`、`AggregateBody`（mode/group_by/aggregates/`is_merge`/needs_finalize/intermediate_tuple_id/output_tuple_id/output_columns）、`SortBody`、`TopNBody`（含 phase/is_split）、`LimitBody`、`WindowBody`（多 group，每组 partition/order/functions + int/out tuple id）、`AssertOneRowBody`。
- `ExchangeBody`：partition_type/partition_exprs(TypedExpr)/source_fragment_id/merge/limit/offset/`flavor: {Distribution|TopNSplit|LimitOffset|CteMulticast{cte_id}}`。1 节点。
- `ValuesBody`、`GenerateSeriesBody`（TABLE_FUNCTION + param UNION，2 节点）、`TableFunctionBody`（TABLE_FUNCTION + PROJECT，2 节点）、`RepeatBody`、`SetOpBody`、`DecodeBody`、`CteConsumeBody`（thrift 上 EXCHANGE/UNPARTITIONED）、`AggStateMergeBody`（不经 to_thrift；fragment 携 `direct_exec`，to_thrift 对此 body no-op 并断言 `direct_exec.is_some()`）。

**覆盖核对**：Scan/Filter(折叠进 conjuncts、无节点)/Project/HashJoin/NestLoopJoin/HashAggregate/Sort/TopN/Limit/Window/AssertOneRow/Distribution(→Exchange{Distribution})/Values/GenerateSeries/TableFunction/Repeat/Union(→SetOp{UnionAll})/Intersect/Except/Decode/CTEAnchor(无 body)/CTEProduce(无 body)/CTEConsume(→CteConsume)/AggregateStateMerge ✓。CTEAnchor 的 visit 返回 consumer 子树、CTEProduce 关一个 fragment 并返回空节点列表——与今天 `VisitResult{plan_nodes: vec![]}` 一致；二者均不产 PlanNode。

**DISTINCT-union 修正（critique CRITICAL-3）**：现状 `emit_distinct_on_top`（`fragment_builder.rs:5119`）只发**一个** `AGGREGATION_NODE`（group-by-all、空聚合、`need_finalize=true`），**没有 exchange**。因此 IR 上 DISTINCT-union 表示为"子树之上加一个 group-by-all 的 `HashAggregate`"，**它不是分片关注点**，归入单节点里程碑（M0-S2/S4），不得在多片里程碑里凭空加 exchange。

### 4.4 `PlanFragment` / `FragmentedPlan` / `DataSink` / `DataPartition`

```rust
// ir/fragment.rs
pub(crate) struct PlanFragment {
    pub fragment_id: FragmentId,
    pub root: PlanNode,
    pub data_partition: DataPartition,    // 本 fragment 的输入分布
    pub output_partition: DataPartition,  // 本 fragment 输出分布
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,  // None = 整行
    pub output_columns: Vec<OutputColumn>,
    pub cte_id: Option<CteId>,
    pub cte_exchange_nodes: Vec<(CteId, i32)>,
    pub global_dicts: Vec<GlobalDictSpec>,
    pub global_dict_exprs: BTreeMap<ColumnId, TypedExpr>,
    pub direct_exec: Option<Box<DirectExecPlan>>,
    pub boundary_schemas: Vec<BoundarySchemaReport>,
}

pub(crate) struct FragmentedPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<FragmentEdge>,         // 复用现有 FragmentEdge（mod.rs:111）
    pub rf_plan: Option<RuntimeFilterIrPlan>,  // 结构化、未编译（§5.4）
}

// ir/sink.rs
pub(crate) enum DataSink {
    Result(ResultSinkSpec),
    DataStream(DataStreamSinkSpec),   // dest 交换节点 + 输出分布
    Iceberg(IcebergSinkSpec),
    Noop,                             // coordinator 改写（今天的 build_noop_sink）
}

// ir/partition.rs
pub(crate) struct DataPartition { pub kind: PartitionKind, pub exprs: Vec<TypedExpr> }
// kind: Unpartitioned | Random | Hash | BucketShuffleHash；to_thrift / explain_label 各一
```

`FragmentedPlan` 等价 StarRocks `ExecPlan`；不复用 `ExecPlan` 名（NovaRocks 运行时已有 `ExecPlan{arena, root}`，`src/exec/node/mod.rs`）。

## 5. Build 流程：`fragment_builder` 变成 `PhysicalPlanNode → PlanNode` 翻译器

### 5.1 两段式（含 critique CRITICAL-1 的中心前提修正）

- **Pass 1 —— `build_ir`（结构遍历）**：单次走 `PhysicalPlanNode`，产 `FragmentedPlan`。负责：**node_id / fragment_id / tuple_id 分配；分片（exchange/CTE 边界）；nullable-tuple 加宽指令；RF 拓扑；dict 流拓扑；连接器扫描结果（exec-param 种子）**。表达式以 `TypedExpr` + `ExprSource` 保留。**不分配 slot、不编译表达式。**
- **Pass 2 —— `lower_fragmented`（绑定/序列化）**：DFS 走 `FragmentedPlan`，产 `MultiFragmentBuildResult`。负责：**slot 分配、descriptor table 构建、ExprCompiler `TypedExpr→TExpr` 编译、RF 描述符编译、dict 落地、`TPlanNode`/`TPlan`/`TDataSink`/`exec_params` 装配。**

**中心前提修正**：原设想"Pass 1 完全无 slot 无编译、Pass 2 重新派生出逐字节相同的 slot id"对**聚合**不成立——slot 的 `type_desc` 来自*已编译*的 `texpr.nodes.first().type_`（`fragment_builder.rs:2802-2820`），聚合中间 slot 类型来自编译后 TExpr 内的 `agg_fn.intermediate_type`（经 `aggregate_slot_contract_for_phase`，`:2910-2923`）。结论：

> Pass 2 在每个节点上仍是**编译 → 读结果类型 → 分配 slot → 注册**的交错序列（即今天的逻辑），只是这套序列被搬进 Pass 2、由 IR 驱动、与结构遍历解耦。相应**放弃"slot id 逐字节相同"的保证**；M0 等价性判定改为"**规范化 id 后 thrift 等价 + 全套件通过**"（见 §6、§7）。

这不改变架构（IR 仍是单一来源、explain 仍无 slot 依赖、thrift 仍从 IR 再生），只是把 Pass 2 诚实地描述为"compile-then-bind per node"。

### 5.2 各关注点归属

| 关注点 | Pass 1（build_ir） | Pass 2（lower_fragmented） |
|---|---|---|
| node_id / fragment_id / tuple_id | **分配** | 原样用（不重分配）|
| slot_id | 不分配 | **编译后按结果类型分配** |
| descriptor table | 记录 table 描述 + tuple 壳 | **建 slot、finalize** |
| `TypedExpr → TExpr` | 存 `TypedExpr` + `ExprSource` | **ExprCompiler 编译** |
| ExprScope（ColumnId→slot）| 记录 `ExprSource` provenance | **DFS 物化 scope、解析** |
| runtime filter | 记录 `RfBuildSpec`/`RfProbeSpec`(TypedExpr) + `target_node_id` 拓扑 | **目标节点 scope 建好后**编译 probe/build 表达式、装配 `TRuntimeFilterDescription`（§5.4）|
| dict 传播 | 记录 `DictPassthrough{src_col,dst_col}` 流 | 解析到 slot、发 `TGlobalDict`（§5.5）|
| CTE / exchange 分片 | **定边界、记 edge** | 接 `TDataSink` dest + exchange payload |
| nullable 加宽 | 记 `nullable_tuple_ids` | **应用到 slot**（`widen_tuple_nullable`）|
| exec_params / scan ranges | **调连接器 begin_scan/plan_splits 存进 `ScanBody.planned`** | 装配 `TPlanFragmentExecParams`（不重算，§5.6）|

### 5.3 中心耦合的解法：`ExprSource`（含 critique CRITICAL-2 修正）

父节点对子输出 slot 的依赖，通过"保留 `TypedExpr` + 为每个引用记 `ExprSource`，Pass 2 用 DFS 建的 scope 把 `ColumnId→slot_id` 解析"解开：

```rust
// ir/expr_source.rs
pub(crate) enum ExprSource {
    /// 子树产出的列，按 ColumnId 在子的 Pass-2 输出 scope 解析。
    Column(ColumnId),
    /// 按子输出 scope 的「位置」绑定（merge 聚合 / DISTINCT-union 这样消费
    /// Local 中间列的场景；该列可能没有稳定的下游 ColumnId）。
    ChildOutputOrdinal(usize),
    /// lambda/局部参数，Pass 2 按需分配新 slot（镜像今天 ExprCompiler 的 lambda-param 分配）。
    Local(LocalParamId),
}
```

`ChildOutputOrdinal`（critique CRITICAL-2）是必需的：global-merge 聚合 `fragment_builder.rs:2873-2887` 用 `child.scope.iter_columns().get(agg_start_col+idx)` **按位置**取中间输入；`emit_distinct_on_top:5136` 同样按位置遍历。这些绑定不是 ColumnId 可寻址的，必须显式建模，否则 merge 聚合与 DISTINCT-union 会错绑。

机制：Pass 2 DFS（子先于父），每个节点产出 `OutputScope: ColumnId→slot_id`（外加可按位置索引的有序列表）；父合并子 scope，用现有 `ExprCompiler::new(slot_alloc, &scope)` 编译自身表达式。这是今天同一套 `ExprScope`/`ExprCompiler`（`expr_compiler.rs`/`resolve.rs`），只是跑在 Pass 2、对 Pass-2 分配的 slot。`next_slot_id` 共享 cell 的 owner 移到 Pass-2 `LoweringCtx`。复用 `id_binding_verifier`（`src/sql/codegen/id_binding_verifier.rs`）对每个未解析 `ColumnId` fail-fast（符合 CLAUDE.md 规则 2）。

### 5.4 Runtime filter（含 critique HIGH-3 修正）

- Pass 1 在 `RuntimeFilterIrPlan` 记录每个 filter：`filter_id`、`build_expr: TypedExpr`(+ 所属 fragment)、`probe_targets: [{target_node_id, probe_expr: TypedExpr, fragment_id}]`。`target_node_id` 在 Pass 1 已分配，故 probe→build 拓扑无需编译即可完整捕获；walk-order 依赖（probe 先于 build 被记录）由 Pass 1 与今天相同的 DFS 顺序天然保留。
- Pass 2 的 RF 装配是**第二阶段**（critique HIGH-3）：必须在 DFS 已经产出每个目标节点的 `OutputScope` **之后**运行，再针对该 scope 编译 `probe_expr`（与 `build_expr`），装配 `TRuntimeFilterDescription` 及 `RuntimeFilterPlanResult`（`all_filters`/`build_side_filters`/`probe_side_filters`，`mod.rs:140`）。即"先 DFS 建完所有节点 scope，再统一编译 RF"，不是免费副产物。

### 5.5 Dict 传播（两段式）

Pass 1 在 `ProjectBody` 记 `DictPassthrough{src_column_id,dst_column_id}`、在产出 `ScanBody`/`PlanFragment` 记 `GlobalDictSpec{column_id,strings,ids,version}`。Pass 2 分配 slot 后：`GlobalDictSpec` 解析 `column_id→slot_id`；`DictPassthrough` 把 dict 重绑到目标 slot（今天的 `propagate_dict_to_slot`，`fragment_builder.rs:1377`）。输出仍是 per-fragment `query_global_dicts: Vec<TGlobalDict>`（`mod.rs:81`）。仅 keying 从"遍历中按 slot 记"改为"Pass 2 按 column_id 解析"。

### 5.6 Exec-params / 连接器扫描

`begin_scan`/`plan_splits` 必须在 Pass 1 跑（catalog 此时是活的；Pass 2 重跑有 drift 风险）。Pass 1 把结果存进 `ScanBody.planned`；Pass 2 的 `build_exec_params_multi_with_refresh_context`（`fragment_builder.rs:883`）消费已存结果，不重新规划。纯属"已算结果存哪"的搬移。

### 5.7 Builder 形态

```rust
// Pass 1
pub(crate) struct IrBuilder<'a> {
    ids: IdAlloc,                  // next_node, next_fragment, next_tuple（无 slot）
    connectors: &'a ConnectorRegistry, catalog: &'a InMemoryCatalog,
    fragment_stack: Vec<FragmentId>, completed_fragments: Vec<PlanFragment>,
    cte_fragments: HashMap<CteId, usize>, edges: Vec<FragmentEdge>,
    rf_plan: RuntimeFilterIrPlan, mv_refresh_ctx: Option<...>,
}
// Pass 2
pub(crate) struct LoweringCtx { slot_alloc: Rc<RefCell<i32>>, desc: DescriptorTableBuilder, scopes: ScopeStack }
pub(crate) fn lower_fragmented(fp: &FragmentedPlan, /*...*/) -> Result<MultiFragmentBuildResult, String>;
```

`PlanSubtree`（Pass-1 visit 返回）替代今天的 `VisitResult`，去掉 scope/slot 数据，携 IR `PlanNode` 树 + 输出 `ColumnId` + tuple_ids + CTE/ordering 簿记。

## 6. `to_thrift`（Pass 2）设计

`lower_fragmented(&FragmentedPlan) -> MultiFragmentBuildResult` 再生今天的输出。逐 fragment、按 fragment 顺序：

1. DFS `PlanFragment.root`，子先于父，收集子 `OutputScope`。
2. 为本节点 tuple 分配输出 slot（`DescriptorTableBuilder::add_slot`），建本节点 `OutputScope`。
3. 用 `ExprCompiler::new(ctx.slot_alloc, &merged_scope)` 编译 body 表达式 → `TExpr`（现有编译器不变）。
4. `match &node.body` → 现有 `nodes::build_*_node` 助手建 `TPlanNode`；`node.node_id` 直传（**不重分配**）。
5. 前序输出（父再子）进 `TPlan`，与今天 `VisitResult.plan_nodes` 顺序一致。多 thrift 节点 body（Window→Analytic+Sort、GenerateSeries→TableFunction+Union、TableFunction→TableFunction+Project、TopN-split→Sort+兄弟 Exchange）在此按同样前序展开。
6. nullable 加宽：`nullable_tuple_ids` → `desc.widen_tuple_nullable`。
7. 建 sink：`DataSink::to_thrift`。
8. 从 `ScanBody.planned` 装配 `exec_params`。
9. RF（§5.4 第二阶段）与 dict（§5.5）落地。
10. `desc_tbl = desc.build()`，clone 进每个 fragment 结果（今天行为，`fragment_builder.rs:929`）。

输出是**不变**的 `MultiFragmentBuildResult`，`single_fragment_child_plan`（`:87`）与 `execute_plan`/`lower_plan` 原样消费。`AggregateStateMerge` body 短路：断言 `direct_exec.is_some()`、不发 thrift 节点。

**等价 oracle（M0 验收，critique HIGH-1）**：不声称"逐字节相同"。判定为"**规范化后 thrift 等价 + 套件全绿**"：
- M0 全程**新旧 builder 双留**，由 flag 切换。
- 定义一个**规范化**：按前序首次出现把 slot/tuple/node id 稳定重标号，再比较规范化后的 `TPlan`/`desc_tbl`/`exec_params`/`edges`/`rf_plan`。
- cutover（切到新 builder）以"新路径下 CLAUDE.md 列出的 SQL 套件全绿、且与旧路径规范化对拍一致"为 gate。
- **删除旧 visitor 是 cutover 之后的独立子任务**（cutover 失败可回退）。

## 7. EXPLAIN 渲染（从 IR）

### 7.1 VERBOSE 从 `FragmentedPlan` 渲染

新 `explain_fragmented(fp, level) -> Vec<String>`，按 StarRocks 顺序走 fragment（root 最后构建、最先打印为 `PLAN FRAGMENT 0`）：

```
PLAN FRAGMENT <n>
  OUTPUT EXPRS: <output_exprs 或 "*">
  PARTITION: <data_partition.explain_label()>          ← 输入分布
  STREAM DATA SINK
    EXCHANGE ID: <dest_node_id>
    <output_partition.explain_label()>                  ← 输出分布
  <root PlanNode 树, node_id 前缀>
```

per-node 渲染 `PlanNode::explain(node, level, ctx)`——一个 `match &node.body`，**复用 `format_physical_node` 现有富文本**（`explain.rs:455`），改读 `PlanNodeBody`。body 已按需携带今天 `format_physical_node` 读的字段（`HashJoinBody.exec_distribution` 喂 `join_distribution_label`、`ScanBody.min_max_hints` 喂 verbose min/max 行）。节点行加 **`node_id:` 前缀**（`<id>:HASH JOIN ...`），对齐 StarRocks `N:OP`，也与现有 logical `format_node` 的 `0:SCAN` 风格一致。`stats={rows=N}` trailer 从 `PlanNode.stats` 渲染。

因为 exchange 是跨 fragment 拆开的真实 IR 节点，VERBOSE 现在能显示真正的 fragment 边界（输入/输出分布、exchange id、distribution）——这是今天 `explain_physical_plan`（渲染分片前 `PhysicalPlanNode`）做不到的，也是本设计的主要用户可见升级。

**`node_id:` 前缀加在全部级别（含 Normal）**（已确认决策；critique MEDIUM-1）：对齐 StarRocks 形态。代价是 Normal 级 golden 也会 churn，M1 安排专门的 golden 重录子任务（reviewed diff），用 CLAUDE.md `--mode record --record-from target`。

### 7.2 退役 `format_physical_node`

- Normal 级也走 `explain_fragmented(fp, Normal)`，只是抑制 fragment 头/分布/stats trailer/RF 行，塌成单棵扁平树（带 node_id 前缀）。删除 `format_physical_node` 与 `explain_physical_plan`，消除双源。`explain_query`/`explain_analyze_query`（`engine/mod.rs:3289,3217`）改为 build IR 再调 `explain_fragmented`。
- `explain_plan`（over `LogicalPlan`，`explain.rs:112`）无关，保留。
- `format_boundary_schema_reports`（`explain.rs:407`）保留，改由 `PlanFragment.boundary_schemas` 供给。

净结果：一个渲染器、一个来源。

## 8. EXPLAIN ANALYZE

### 8.1 今天的缺口

`explain_analyze_query`（`engine/mod.rs:3217`）实际 **plan 三次**（critique MEDIUM-3）：`:3256` optimize、`:3261` `execute_query_with_catalog_provider` 内部重 plan、`:3280` `PlanFragmentBuilder::build` 为 boundary schema 第三次构建。执行用 `profiler=None`（`execute_plan` `:4134` 硬编码无 profiler），只渲染估算。

### 8.2 目标流程：单次 build + 带 profiler 执行 + node_id 关联

1. **Plan 一次**：`analyze → plan_query → optimize → build_ir` 产一个 `FragmentedPlan`，explain 与执行都从它派生（消除三次 plan，含 :3280 的 boundary-schema 重建）。
2. **Lower 一次**：`lower_fragmented(&fp)`，经现有 `choose_standalone_execution` 执行。
3. **挂 profiler**：给 `execute_plan` 加 `profiler: Option<Profiler>`（Report 4 §6.1 指出的一行签名缺口），传入已具能力的 `execute_plan_with_pipeline(profiler=...)`（`exec/pipeline/executor.rs:49`）。ANALYZE 或 `enable_profile` 时开启。
4. **合并**：执行后从 `FragmentContext`（`fragment_context.rs:91`）取 profiler，`merge_pipeline_profiles_for_fe`（`fe_report.rs:515`）把 per-driver DOP 实例塌成每算子一份，再 `normalize_profile_tree_for_fe`（`fe_report.rs:552`）把名字规范成 `(plan_node_id=N)`。
5. **按 `node_id` 关联**：算子 profile 名在 `lower_plan` 时即嵌 `plan_node_id=N`，建 `HashMap<i32, MergedOpProfile>`。因 IR 的 `node_id` 与 `to_thrift` 盖进 `TPlanNode.node_id`、再传入算子名的是同一个 id，关联精确无歧义。
6. **渲染 actual vs estimate**：`explain_fragmented(&fp, Analyze)` 走 IR，对每个节点用其 `node_id` 查 profile map，把 actual 贴在 estimate 旁：
   ```
   3:HASH JOIN (PARTITIONED, INNER, eq: [a=b])  est_rows=124  act_rows=131  time=2.3ms  peak_mem=4MB
   ```

**关联是 1:1**（因 Filter 已折叠进 conjuncts、不再有"无 thrift 节点的 IR 节点"，§4.3）：被 lower 的 fragment 内每个 PlanNode 的 `node_id` 都等于其 thrift `TPlanNode.node_id`、也等于 profile 的 `plan_node_id`，无翻译、无缺口。唯一例外是 **direct-exec fragment（AggregateStateMerge，IMV 路径）**：整片走 `DirectExecPlan`、不产 thrift 节点、不参与 per-node profile 关联，ANALYZE 对其作 fragment 级特殊渲染（不逐节点贴 actual）。query 级 `Planning/Execution/Rows` 头保留。

现有 `Profiler`/`merge_isomorphic_profiles`/`normalize_profile_tree_for_fe` 原样复用；新代码只有 `node_id→profile` 关联和 IR 渲染器的 actual 列。

## 9. 里程碑与子任务（竖切；critique HIGH-2）

排序原则：**先在现有 thrift 输出背后引入 IR（M0）并证等价，再把 EXPLAIN 切到读 IR（M1），再丰富 ANALYZE（M2）。** 按**算子组端到端竖切**（不横切 Pass2-then-Pass1，避免隐藏的 big-bang）。

### M0 —— 行为保持地引入 IR（无 explain 变化）
目标：`build_ir` + `lower_fragmented` 完全替代 eager thrift 发射，产规范化等价的 `MultiFragmentBuildResult`，套件全绿。**无用户可见变化。**

- **M0-S1 IR 类型骨架**：`src/sql/codegen/ir/*`（仅类型，无逻辑）。验收：`cargo build` + 各 body 构造单测。
- **M0-S2 竖切①：Scan+Filter+Project**：两段端到端走通（Pass 1 译这些算子；Pass 2 `LoweringCtx`/scope 物化/slot 分配/编译/`nodes::build_*`）。新旧 builder 双留 + flag。验收：这组算子的查询规范化 thrift 等价 + 相关单测。
- **M0-S3 竖切②：Sort+Limit+HashAggregate（含 DISTINCT-union 的 group-by-all agg）**：含 merge 聚合的 `ChildOutputOrdinal` 绑定。验收：聚合/排序/distinct 查询等价。
- **M0-S4 竖切③：HashJoin+NestLoopJoin+SetOp+Repeat+Values+Decode+AssertOneRow+Window+TopN(unsplit)+GenerateSeries+TableFunction**。验收：join/filter/sort/set-op/window 套件等价。
- **M0-S5 竖切④：分片**：Exchange body + `FragmentEdge`（Pass 1）；`DataStreamSink`/exchange payload（Pass 2）；CTE produce/consume；TopN-split；Limit-offset；多片 dict + RF 拓扑捕获 + RF 第二阶段编译。验收：distributed/CTE/window/topn 套件多片等价；`edges`/`rf_plan` 结构相等。
- **M0-S6 等价 harness + cutover**：规范化对拍 harness（代表性 query 语料）；切 `engine/mod.rs` 执行路径到新 builder（旧 builder 仍在）。验收：`cargo test` 全绿 + CLAUDE.md 套件（`ssb`/`tpc-h`/`tpc-ds`/`join`/`cte`/`iceberg*`）`--mode verify` 不变。
- **M0-S7 AggregateStateMerge + DirectExecPlan 透传**：`AggStateMergeBody` + `PlanFragment.direct_exec`；to_thrift no-op + 断言。验收：IMV refresh/mv 套件不变。
- **M0-S8 删除旧 visitor**（cutover 之后的独立子任务）：移除旧 `VisitResult`/`visit`/eager 发射。验收：套件仍全绿。

### M1 —— EXPLAIN VERBOSE/Normal/Costs 从 IR
- **M1-S1** `explain_fragmented` 骨架 + 全级别 `node_id:` 前缀 + `PLAN FRAGMENT N` 头/分布/sink；per-node 文本从 `format_physical_node` 移植到 `PlanNode::explain`。
- **M1-S2** Costs + stats trailer + RF/dict 行从 IR 渲染；移植现有 explain RF 测试。
- **M1-S3** 退役 `format_physical_node`/`explain_physical_plan`，`explain_query` 切到 IR；**golden 重录子任务**（reviewed diff，含 Normal 级因前缀产生的 churn）。

### M2 —— EXPLAIN ANALYZE 真 profile
- **M2-S1** 给 `execute_plan` 穿 `profiler: Option<Profiler>`，传入 `execute_plan_with_pipeline`。验收：trivial query 执行后 profiler 被填充。
- **M2-S2** 单次 build ANALYZE：重写 `explain_analyze_query`，build IR 一次、带 profiler 执行同一 lowered plan，**消除全部三次 plan（含 :3280 boundary-schema）**。验收：timing 头仍在；plan 计数断言无重复。
- **M2-S3** profile 合并 + `node_id` 关联 map：复用 `merge_pipeline_profiles_for_fe`/`normalize_profile_tree_for_fe`。验收：join query 的 `plan_node_id` 键与 IR node id 对齐。
- **M2-S4** actual-vs-estimate 渲染：每个被 lower 的节点按 `node_id` 贴 `act_rows/time/peak_mem`；direct-exec fragment（AggStateMerge）作 fragment 级特殊渲染。验收：对活的 standalone server 跑 `EXPLAIN ANALYZE`，per-node actual 出现且行数大致吻合；加 `-- @normalize_explain_timing` 的 sql-test。

## 10. 风险与缓解
- **Pass-2 DFS 顺序不变量**（父在子绑定后编译）：`to_thrift` 结构上子先于父；复用 `id_binding_verifier` 对未解析 `ColumnId` fail-fast。最大风险，结构上可控。
- **M0 期间 thrift drift**：规范化对拍 harness gate cutover；node/tuple id 不重分配；Pass 2 的 per-node compile→type→alloc 序列镜像今天遍历顺序。**不声称逐字节相同**，以规范化等价 + 套件全绿为准。
- **RF walk-order 敏感**：Pass 1 保留今天 DFS 顺序、记 `target_node_id`；Pass 2 第二阶段在所有目标 scope 建好后编译，顺序无关。
- **连接器扫描结果在 Pass 1 捕获、Pass 2 前 stale**：两段在同一 `execute`/`explain` 调用内同步执行，其间无 catalog 变更；结果只捕获一次。
- **M1 golden churn**（含 Normal 前缀）：专门重录子任务，reviewed diff。
- **大 enum 变体**：重 body 预先 `Box`。

## 11. 已决策记录
- IR body：**enum**（封闭集 + 穷尽 match + 兄弟 pattern-match）。
- id/fragment：在新 IR 层、**不**放 `PhysicalPlanNode`（保持其封装）。
- thrift 由 IR 再生（完全忠实 `PhysicalPlanNode → PlanNode → thrift`），但 Pass 2 内部 compile-then-bind，等价性以规范化对拍判定。
- Filter **不作为 IR 节点**：谓词折叠进子节点 `conjuncts`（贴 StarRocks），于是每个被 lower 的节点 1:1 对应 thrift/profile，无需 `thrift_node_id` 特例字段；direct-exec fragment（AggStateMerge）是 per-node 关联的唯一例外。
- `node_id:` 前缀：**全级别**（含 Normal），对齐 StarRocks 形态。
- 旧 builder 删除独立于 cutover。

## 12. 关键文件锚点
- IR（新）：`src/sql/codegen/ir/{node,body,fragment,partition,sink,expr_source,lowering,explain}.rs`
- Pass 1：`src/sql/codegen/fragment_builder.rs`（`IrBuilder`，替代 eager `VisitResult`/`visit` `:65`/`:1413`）
- Pass 2 复用：`src/sql/codegen/{expr_compiler.rs, descriptors.rs, nodes.rs, resolve.rs, id_binding_verifier.rs}`
- 结果类型（消费者不变）：`src/sql/codegen/mod.rs`（`PlanBuildResult:64`/`FragmentBuildResult:151`/`MultiFragmentBuildResult:122`）
- EXPLAIN：`src/sql/explain.rs`（`format_physical_node:455`/`explain_physical_plan:401` 退役；`format_boundary_schema_reports:407` 保留）
- 执行/ANALYZE 入口：`src/engine/mod.rs`（`explain_analyze_query:3217`/`explain_query:3289`/`execute_plan:4134`/`lower_plan_build_result:4031`）
- profiling 复用：`src/exec/pipeline/executor.rs:49`、`src/service/fe_report.rs:{515,552}`、`src/exec/pipeline/fragment_context.rs:91`
- StarRocks 参照：`~/project/starrocks/fe/fe-core/.../planner/{PlanNode,PlanFragment,DataPartition,DataSink}.java`、`.../sql/plan/{ExecPlan,PlanFragmentBuilder}.java`
