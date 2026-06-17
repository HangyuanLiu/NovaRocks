# 设计：统一 Plan-Node 体系 + 优化器封装（两桥）

状态：已确认方向，待实施（2026-06-17）
关联：`docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md`（scalar IR，本设计接棒其 M1.5/M3）

---

## 0. 背景：我们怎么走到这一步

1. scalar IR（`ScalarArena` + `ScalarId`，hash-consed）的 M0/M1 已合入 main（#331/#335）。M1
   把 memo 算子的标量字段从按值 `TypedExpr` 改成 `Copy` 的 `ScalarId`，根治了 memo 表达式
   深拷贝的内存放大；副作用是 gap2（传递等值谓词，原 OOM 根因）的重做也被解锁为内存安全。

2. 收尾 M1 时发现 codegen 仍在 `ScalarId → TypedExpr` materialize（`fragment_builder.rs`、
   `build_distributed_plan`）。最初判断这是"不合理的往返"，准备用 `compile_scalar`（codegen
   直接吃 `ScalarId`）消除。

3. 顺藤摸瓜发现更本质的结构问题：NovaRocks 的 plan 表示其实有**三/四套**，其中 `LogicalPlanNode`
   与 `DistributedPlanNode` 是同一套 `Operator` taxonomy 的**冗余投影**。`compile_scalar` 只是
   在错误的边界打补丁。

4. 经多轮讨论收敛出本设计：**planner 作为主线持有一套统一的 `PlanNode` 体系；optimizer 退化为
   一个只认 `Operator` 的封闭模块，planner 用两个桥接进出。** 本文把目标形态、关键设计点、阶段
   切分、与 scalar IR 的关系、StarRocks 对照一次讲死。

---

## 1. 目标与非目标

### 目标

- **G1 优化器封装**：依赖方向单向 `planner → optimizer`。优化器只认自己的 `Operator`，**不知道**
  外部的 `PlanNode` / `LogicalPlanNode` / `DistributedPlanNode` 是什么。
- **G2 planner 侧统一 `PlanNodeKind`**：把 `LogicalPlanNodeKind` 与 `DistributedPlanNodeKind`
  合并成一套枚举；逐字段重复的"直通节点"（Filter/Project/Scan/…）去重为共享 struct；优化器决策
  节点（Join↔HashJoin 等）保留为各自独立变体。
- **G3 两桥边界清晰**：入口 Bridge 1、出口 Bridge 2 是 planner 与 optimizer 之间仅有的两个接触点。
- **G4 标量边界承诺**：`TypedExpr` 是 planner/codegen 的语言，`ScalarId` 是优化器私有；Bridge 2
  处的 `ScalarId → TypedExpr` materialize 是**正当且永久**的封装边界，不是 wart。
- **G5 优化器外挂化原则**：planner 是主线，optimizer 是其上的 `PlanNode → PlanNode` 改写。理论上
  朴素的 PlanNode（手搓或跳过优化器）也能直达可执行 thrift（默认物理下降为可选后续）。

### 非目标

- **不删 `Operator`**：它就是优化器私有的统一 IR（logical+physical 合一，M1 已是 `ScalarId`）。
- **不删 `PlanFragment`**：分片是图切割（fragment 边界 + Exchange），保留为统一 PlanNode 之上的
  覆盖层。本设计删的是 `DistributedPlanNodeKind` 这个**重复的节点枚举**，不是 fragment 概念。
- **不让 codegen 变 `ScalarId`-native**：`compile_scalar` 会把优化器私有的 `ScalarId` 泄漏进
  codegen，破坏封装。明确放弃。
- **不动 `PhysicalPlanNode → DistributedPlanNode` 的转换算法**：只把它定位成 Bridge 2、迁入
  planner、并把输出类型的 kind 换成统一枚举；分片/Exchange 展开/runtime filter 装配逻辑不变。
- **不在本设计内做 CSE / gap2（M4）**：它们坐在本设计与 scalar IR 之上，单独立项。

---

## 2. 现状：三/四套表示与冗余投影（根因）

| 表示 | 角色 | 标量 | 变体数 | 位置 |
|---|---|---|---|---|
| **`Operator`** | **真正统一的 taxonomy（logical+physical 合一）** | `ScalarId` | **46**（22 logical + 24 physical） | `optimizer/operator.rs` |
| `LogicalPlanNode` / `LogicalPlanNodeKind` | optimizer 入口；`Operator` 的 **logical 投影** | `TypedExpr` | ~24 | `planner/plan.rs` |
| `DistributedPlanNode` / `DistributedPlanNodeKind` | codegen IR；`Operator` 的 **physical 投影 + 分布式元数据** | `TypedExpr` | 16 | `codegen/ir/node.rs` |
| `PhysicalPlanNode` | optimizer 出口；`{op: Operator, children, …}` = **`Operator` 的树 wrapper（OptExpr）** | `ScalarId` | — | `optimizer/physical_plan.rs:42` |

转换链：

```
analyzer → LogicalPlanNode ──optimize()──> PhysicalPlanNode ──build_distributed_plan()──> DistributedPlanNode ──lower──> thrift TPlan
            (planner,TypedExpr)  └ convert::logical_plan_to_memo  (op:Operator,ScalarId)   (codegen,TypedExpr,+fragment) 
                                   → Memo(Operator) → 搜索 → extract
```

**冗余的证据（逐字段比对）**：

| 节点 | Logical（`plan.rs`） | Distributed（`codegen/ir/kind.rs`） | 判定 |
|---|---|---|---|
| Filter | `predicate: TypedExpr` | `predicate: TypedExpr` | **完全相同** |
| Project | `items: Vec<ProjectItem>, output_qualifier` | 同左 | **完全相同** |
| Scan | db/table/alias/columns/`predicates: Vec<TypedExpr>`/… | 同左 **+`mv_rewritten_from`** | **近乎相同** |
| Aggregate | `group_by: Vec<TypedExpr>`, aggregates, output_columns, +`already_pushed` | +`mode`, +`is_merge` | **相近** |
| Join | `join_type, condition: Option<TypedExpr>`（抽象） | (HashJoin) `eq_conditions` 拆分 + `distribution`（具体） | **本质不同** |

两个关键事实：
1. **直通节点逐字段重复**，分别躺在 `plan.rs` 与 `kind.rs`；**两边标量都已是 `TypedExpr`**，无表示错配。
2. **分歧只发生在优化器决策节点**（Join↔HashJoin/NestLoopJoin、Aggregate↔HashAggregate、
   Union/Intersect/Except↔SetOp、Exchange 无逻辑态）。

**结论**：`LogicalPlanNode` 与 `DistributedPlanNode` 不该"互相合并"，而是它们各自都是 `Operator`
的投影；该做的是 (a) 把两套 `*Kind` 合成一套 planner 统一枚举（直通去重、决策节点保留双变体），
(b) 把 optimizer 收口成只认 `Operator` 的封闭模块，planner 用两桥进出。

**注（同一冗余在 `Operator` 内部再现一层）**：`optimizer/operator.rs` 的 `Operator` 枚举里，
**19 对 `Logical*Op`/`Physical*Op` 逐字段完全相同**（Scan、Filter、Project、Sort、Limit、TopN、
Window、Union、Intersect、Except、Values、GenerateSeries、TableFunction、Repeat、AssertOneRow、
CTEAnchor/Produce/Consume、Decode），只有 **Join**（1 逻辑 → HashJoin + NestLoopJoin + distribution）、
**Aggregate**（logical `stage`/`is_split` vs physical `mode`）、**Distribution**（物理独有 enforcer）
真分歧。即同构化原则在 planner 层（§4.1）与 optimizer 层（§4.8）各适用一次。

---

## 3. 目标架构

```
┌─────────────────────────── planner（主线，持有统一 PlanNode 体系）───────────────────────────┐
│                                                                                              │
│   analyzer ──> LogicalPlanNode { kind: PlanNodeKind(logical 子集), children }  [TypedExpr]    │
│                     │                                                                        │
│                     │  Bridge 1（入口）：LogicalPlanNode → LogicalOperator(OptExpr)  [intern]  │
│                     ▼                                                                        │
│        ┌──────────────── optimizer（封闭模块，只认 Operator）────────────────┐                 │
│        │  LogicalOperator                                                    │                 │
│        │     → RBO rewrite（M3：规则跑在 Operator/OptExpr 上，ScalarId）       │                 │
│        │     → CBO memo（MExpr = Operator + GroupId）                         │                 │
│        │     → extract → PhysicalPlanNode { op: Operator, … }  [ScalarId]    │                 │
│        └─────────────────────────────────────────────────────────────────┘                 │
│                     │  Bridge 2（出口）：PhysicalPlanNode(PhysicalOperator) → DistributedPlanNode │
│                     │   = 今天的 build_distributed_plan：materialize(ScalarId→TypedExpr) +       │
│                     │     分配 node_id/fragment_id/tuple_ids + 展开 Exchange + 装配 runtime filter │
│                     ▼                                                                        │
│        DistributedPlanNode { kind: PlanNodeKind(distributed 子集), children, node_id,         │
│                              fragment_id, tuple_ids, … }  [TypedExpr]                         │
│                     │                                                                        │
│        PlanFragment 覆盖层（fragment 列表 + Exchange 边）——保留                                  │
│                     ▼                                                                        │
│        codegen lower → thrift TPlan                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘

依赖方向：planner ──知道──> optimizer 的 Operator/PhysicalPlanNode/ScalarArena（单向）
          optimizer ──不知道──> planner 的 PlanNode 体系
```

要点：
- **planner** 拥有唯一面向结构/codegen 的 `PlanNode` taxonomy（统一 `PlanNodeKind` + 两个 wrapper），
  标量用 `TypedExpr`。
- **optimizer** 只认 `Operator`（`ScalarId`），是一个 `LogicalOperator → PhysicalPlanNode` 的纯
  函数式服务，对外不暴露也不依赖 planner 的类型。
- planner 与 optimizer 仅有两个接触点：**Bridge 1（入口 intern）**、**Bridge 2（出口
  materialize + 分片）**。

---

## 4. 关键设计点

### 4.1 统一 `PlanNodeKind`（合并清单）

合并后 `PlanNodeKind` ≈ **30 个变体**，分三类：

- **共享直通节点（约 11，去重为单一 struct）**：Scan、Filter、Project、Sort、Values、Decode、
  Repeat、Window、GenerateSeries、TableFunction、AssertOneRow。这些今天在 `plan.rs` 与 `kind.rs`
  逐字段重复，合并无损（个别字段差异如 Scan 的 `mv_rewritten_from`、Aggregate 的 `mode/is_merge`
  vs `already_pushed`，用"字段并集 + 阶段约定"承载）。
- **logical-only 变体**：Join、Aggregate、Union、Intersect、Except、Limit、CTEAnchor、
  CTEProduce、CTEConsume、Apply、AggregateStateMerge、ImvDelta、ImvVersion。
- **distributed-only 变体**：HashJoin、NestLoopJoin、HashAggregate、Exchange、SetOp、TopN。

**stage 合法性靠约定不靠类型**（Calcite RelNode 同款）：`LogicalPlanNode` 实际只持 logical 子集，
`DistributedPlanNode` 实际只持 distributed 子集；类型系统不会阻止把 `HashJoin` 放进
`LogicalPlanNode`，由 wrapper 约定 + 校验保证。这是本设计接受的取舍（换来单一节点 taxonomy）。

### 4.2 两个 wrapper（都保留）

```rust
// 薄 wrapper：入口/逻辑阶段
struct LogicalPlanNode { kind: PlanNodeKind, children: Vec<LogicalPlanNode>, /* 现有逻辑元数据 */ }

// 厚 wrapper：出口/分布式阶段（从 codegen 迁入 planner）
struct DistributedPlanNode {
    kind: PlanNodeKind,
    children: Vec<DistributedPlanNode>,
    node_id: i32, fragment_id: FragmentId,
    tuple_ids: Vec<i32>, nullable_tuple_ids: Vec<i32>, limit: i64,
    execution_join_distribution: Option<JoinExecutionDistribution>,
    build_runtime_filters: Vec<RuntimeFilterDesc>,
    probe_runtime_filters: Vec<RuntimeFilterProbe>,
    stats: PlanNodeStats,
}
```

两个 wrapper 共享 `PlanNodeKind`，但 `DistributedPlanNode` 多带分布式/执行元数据（`node_id`、
`fragment_id`、`tuple_ids`、runtime filter、join distribution、stats）——这正是"物理 Operator 不够、
DistributedPlanNode 必须保留"的原因。

### 4.3 Bridge 1（入口）

`LogicalPlanNode → LogicalOperator(OptExpr)`，遍历建树并 intern 标量（`TypedExpr → ScalarId`）。
**这就是 scalar IR / M3 的"入口 intern"**：取代今天 `convert::logical_plan_to_memo` 处的晚 intern，
让 RBO 也跑在 `Operator` 上。arena 生命周期复刻 `ColumnRefFactory`（`Rc<RefCell<ScalarArena>>` 贯穿
rewrite，convert 进 memo 时 unwrap）。

### 4.4 Bridge 2（出口）

`PhysicalPlanNode(PhysicalOperator) → DistributedPlanNode`。**= 今天的 `build_distributed_plan`
（`codegen/ir/build.rs:1219`），算法逐字不动**：materialize `ScalarId → TypedExpr`、分配
`node_id/fragment_id/tuple_ids`、把 `PhysicalDistribution` 展开成 `Exchange`、装配 runtime filter。
本设计只做两件事：① 把它定位成 planner 的出口桥（连同 `DistributedPlanNode` 迁入 planner）；
② 输出的 kind 从 `DistributedPlanNodeKind` 改成统一 `PlanNodeKind`。

### 4.5 优化器封闭性（跨边界的类型）

- 入边界：planner 把 `LogicalOperator`（OptExpr）传给优化器入口。
- 出边界：优化器返回 `PhysicalPlanNode`（`op: Operator` + 共享 `ScalarArena`）。
- **planner 知道** `Operator` / `PhysicalPlanNode` / `ScalarArena`（它要建 `LogicalOperator`、在
  Bridge 2 消费 `PhysicalOperator` 并用 arena materialize）。
- **optimizer 不知道** `PlanNode` / `LogicalPlanNode` / `DistributedPlanNode`。它内部不再出现
  `convert::logical_plan_to_memo` 这种"从外部 plan 类型翻进来"的代码——入口直接收 `LogicalOperator`。

### 4.6 标量边界承诺（为何 `compile_scalar` 是错的）

- `TypedExpr` = planner/codegen 的语言（`LogicalPlanNode` 与 `DistributedPlanNode` 都用它）。
- `ScalarId` = 优化器私有（`Operator` / memo / `PhysicalPlanNode`）。
- Bridge 1 = `TypedExpr → ScalarId`（intern）；Bridge 2 = `ScalarId → TypedExpr`（materialize）。
- 因此 codegen 永远吃 `TypedExpr`；Bridge 2 的 materialize 是封装边界、正当且永久。
  `compile_scalar`（让 codegen 直接吃 `ScalarId`）会把私有 `ScalarId` 泄漏过桥，**违反 G1**，明确放弃。

### 4.7 `PlanFragment` 保留

分片是对 PlanNode 树的图切割（fragment 边界 + Exchange 连边），不是节点属性。`PlanFragment`
作为统一 PlanNode 之上的薄覆盖层保留；本设计删的是 `DistributedPlanNodeKind` 这个重复枚举。

### 4.8 Operator 层同构化（Level 1）

与 §4.1 在 planner 层做的事**对称**：optimizer 层的 `Operator` 也有同样的逐字段重复——19 对
`Logical*Op`/`Physical*Op` 完全相同（见 §2 注）。但**这里的 logical/physical 区分是 Cascades 承重
语义**（`Group.logical_exprs`/`physical_exprs`、transformation vs implementation 规则、成本只挂物理、
enforcer 只在物理），**结构相同 ≠ 角色相同**。因此分两档，本设计取 **Level 1**：

- **Level 1（取）**：19 个透传算子各只保留一个 payload struct（`FilterOp`/`ProjectOp`/…），但
  `enum Operator` 仍保留 `LogicalFilter(FilterOp)` 与 `PhysicalFilter(FilterOp)` 两个变体。**memo
  核心不动**——`is_physical()` 与 logical/physical 分桶照旧按变体匹配；透传 handler 用 or-pattern
  `LogicalFilter(f) | PhysicalFilter(f) => …` 合并。`Operator` 的 struct 数从 ~46 降到 ~25，
  construction/match 重复一并消除。Join/Aggregate/Distribution 保留各自分歧 struct。
- **Level 2（不取）**：收成单变体 + `MExpr` 上挂 phase 标记。可进一步砍变体数 + 把 19 条 trivial
  implementation 规则收成一条泛型透传规则，但要重接 memo 的 explore/implement/cost/enforce 对 phase
  的判断——动搜索主循环、风险在核心、偏离 StarRocks 更远。放弃。

StarRocks 对照：StarRocks 是 `LogicalXxxOperator`/`PhysicalXxxOperator` 分开的类 + 每算子一条
implementation 规则。Level 1（共享 payload、保留变体）是温和偏离，仍保有变体级 logical/physical 区分。

---

## 5. 与 scalar IR / M3 的关系

- **M3 = 本设计的"优化器封装"那一半**：RBO 规则从 `LogicalPlanNode` 迁到 `Operator`/`OptExpr`，
  入口 intern（= Bridge 1），优化器收口成只认 `Operator`。
- scalar IR spec 里原先设想的 **M1.5（新建平行 ScalarId logical IR）被本设计取代**：不新建平行 IR，
  而是复用已有的 `Operator`（它本就是 logical+physical 合一的 taxonomy）。这更省、更对。
- 前提：scalar M1（#335）已合入 main。

---

## 6. 阶段切分（两条 arc，在两桥汇合）

### Arc A = M3（优化器封装 + RBO 迁移）

地基，含 scalar 收尾。高价值/高风险（动 ~54 条 RBO 规则）。
- **A0（地基，机械低风险，先行）**：Operator 层同构化（Level 1，§4.8）——19 对 `Logical*Op`/
  `Physical*Op` 共享单一 payload struct，保留 `Logical*`/`Physical*` 变体区分，memo 核心不动；
  透传 handler 收 or-pattern。先做，使后续 A1/A2 的规则一次性落在已同构的 `Operator` 上。
- A1：引入 `OptExpr { op: Operator, children, props }`（逻辑树 wrapper；物理侧 `PhysicalPlanNode`
  已是同形）+ 入口 Bridge 1（`LogicalPlanNode → LogicalOperator`，intern；arena 复刻
  `ColumnRefFactory` 生命周期）。
- A2：把 RBO 规则（含 imv 规则）从 `LogicalPlanNode` 迁到 `OptExpr`。多数结构型规则机械透传
  `ScalarId`；少数构造/拆解标量的规则（谓词拆 AND、常量折叠、子查询去关联、低基数字典改写）真 port。
- A3：`convert` 收敛成 memo copy-in（`OptExpr → MExpr`，标量已是 `ScalarId`，无再 intern）；优化器
  入口签名收口成 `LogicalOperator → PhysicalPlanNode`；`LogicalPlanNode` 退出优化器内部。
- A4：估计器层的瞬态 materialize（stats/logical_props）评估保留（不放大内存，可后续清理，不阻塞）。

### Arc B = planner 侧统一（合并 + 迁移 + 形式化 Bridge 2）

较机械/低风险、即得去重。**不依赖 scalar IR，可并行或作低风险暖场先行。**
- B1：定义统一 `PlanNodeKind`；直通节点去重为共享 struct；决策节点保留 logical/distributed 双变体。
- B2：`LogicalPlanNode` / `DistributedPlanNode` 两个 wrapper 改用统一 `PlanNodeKind`。
- B3：`DistributedPlanNode` + `build_distributed_plan` 从 codegen 迁入 planner，形式化为 Bridge 2；
  逻辑不动，仅改 kind 类型 + 归位。
- B4：explain 渲染统一到 `PlanNode`（pre-opt 的 `LogicalPlanNode` 与 post-opt 的
  `DistributedPlanNode` 同源渲染）。

### 排序建议

**A（M3）先行作为地基**——它立起"优化器 = 封闭 Operator 服务 + 两桥"的脊梁，又是已认可的 scalar
收尾。B 可并行或紧随；若想要一个不碰 scalar、纯收益的低风险暖场，B 也可先行。两条在 Bridge 1/2 汇合。

每个 sub-stage 独立可编译、可跑全套件、可单独开 PR。

---

## 7. StarRocks 对照

- **对齐（逻辑可照搬）**：`Operator` 基类 + Logical/Physical 子类、`OptExpression` 树、`GroupExpression`
  memo、`ScalarOperator` 标量、RBO `TransformationRule.transform(OptExpression)`、CBO `Memo.copyIn`
  ——规则/成本/memo 的算法逻辑继续照搬 StarRocks。
- **偏离（有界，仅封装层）**：StarRocks 是「`Operator`/`OptExpression` + 独立 `PlanFragment`/`PlanNode`
  + 优化器强制」；本设计是 Calcite 式「planner 主线统一 PlanNode + 优化器外挂可选」。分歧只在"节点
  taxonomy 的封装 + 优化器是否可选"，不在算法。复用本就 StarRocks 对齐的 `Operator` 贯穿优化器内部，
  反而比现状三套投影更一致。

---

## 8. 风险

- **统一 enum 体量与约定合法性**：~30 变体大枚举，stage 合法性靠 wrapper 约定 + 校验，非类型强约束
  （Calcite 先例）。缓解：wrapper 边界 + debug 校验 + golden。
- **返工刚落地的 DistributedPlan IR（#318–#330）**：B3 把 `DistributedPlanNode` 迁入 planner 并改
  kind。是"拆掉刚加的重复投影"，方向对，但确实动新代码。缓解：逻辑不动，只迁移+改类型，plan 逐字节
  等价 harness 兜底。
- **M3 规则漂移**：~54 规则迁移期行为可能漂移。缓解：现有 optimizer golden + plan 逐字节比对 +
  分 sub-stage 小步提交。
- **多 stage 协调**：多月 arc。缓解：每 sub-stage 独立可交付、独立验收。

---

## 9. 验收标准

每个 sub-stage：
- `cargo fmt` / `cargo clippy` 干净；`cargo build` 通过。
- 优化器模块全 lib 单测通过。
- `sql-tests/optimizer` plan-golden 全绿（59/59 或当时基线）。
- TPC-DS SF1 全 99 query verify 通过（dev-opt 串行 `-j1`）。
- **plan 逐字节等价 harness**：迁移类改动产出的 thrift/EXPLAIN 与改前逐字节一致（行为不变）。
- q72 不 OOM（M1 已 banked，回归守门）。

整体 arc 完成判据：
- 优化器入口签名为 `LogicalOperator → PhysicalPlanNode`，内部无 `LogicalPlanNode`/`PlanNode` 出现。
- planner 持单一 `PlanNodeKind`；`LogicalPlanNode`/`DistributedPlanNode` 两 wrapper 共享之。
- planner→optimizer 单向依赖；optimizer crate/模块不 `use` 任何 planner PlanNode 类型。

---

## 10. 已定决策（2026-06-17）

1. **优化器封闭、只认 `Operator`**；planner 是主线、知道 `Operator`；依赖单向 planner→optimizer。
2. **合并 `LogicalPlanNodeKind` + `DistributedPlanNodeKind` 成统一 `PlanNodeKind`**；直通节点去重，
   决策节点（Join/HashJoin 等）保留各自变体。
3. **两个 wrapper（`LogicalPlanNode` 薄 / `DistributedPlanNode` 厚）都保留**；`DistributedPlanNode`
   迁入 planner。
4. **`PhysicalPlanNode → DistributedPlanNode`（build_distributed_plan）逻辑不动**，定位成 Bridge 2、
   迁入 planner、输出改用统一 kind。
5. **`TypedExpr` = planner/codegen 语言，`ScalarId` = 优化器私有**；Bridge 2 的 materialize 正当且
   永久；**放弃 `compile_scalar`**（会破坏封装）。
6. **保留 `Operator` 与 `PlanFragment`**；不删、不并入统一 PlanNode。
7. **M3 取代 scalar spec 的 M1.5**：用 `Operator` 做 RBO，不新建平行 ScalarId logical IR。
8. **Operator 层同构化取 Level 1**：19 对逐字段相同的 `Logical*Op`/`Physical*Op` 共享单一 payload
   struct，**保留 `Logical*`/`Physical*` 变体区分**（memo 核心 `is_physical()`/logical-physical 分桶
   不动）；Join/Aggregate/Distribution 保留分歧。放弃 Level 2（单变体 + phase 标记，碰搜索核心）。
9. **排序**：Arc A 从 **A0（Operator 同构化 Level 1，机械低风险）起步**，再 A1–A4（封装 + 54 规则
   迁移，规则一次性落在已同构 `Operator`）；Arc B（planner 侧统一）可并行/紧随，不依赖 scalar IR。
10. **不在本设计内做 CSE / gap2（M4）**；它们坐在本设计之上单独立项。

---

## 11. 执行交接

下一步：写 **M3（Arc A）实施 plan** 到 `docs/superpowers/plans/2026-06-17-m3-rbo-on-operator.md`，
bite-sized TDD 步骤，按 sub-stage **A0→A4** 切分（A0 = Operator 同构化 Level 1，先行地基）；每步
fmt/clippy/build/单测 + 必要 golden + plan 逐字节等价；用户经 codex 实施，我 review。Arc B 待 A 立稳
或并行时另出 plan。
