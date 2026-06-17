# 设计：优化器原生标量表达式 IR（`ScalarArena` + `ScalarId`，hash-consed）

日期：2026-06-16
状态：设计（用于驱动实现；XL 架构重构，分里程碑落地）
范围：standalone optimizer `src/sql/optimizer/**`、analyzer→optimizer 边界、codegen 边界
参照：StarRocks `fe/fe-core/.../sql/optimizer/operator/scalar/ScalarOperator.java`、`rule/tree/exprreuse/ScalarOperatorsReuse.java`；NovaRocks exec `src/exec/expr/mod.rs`（`ExprArena`）
起因：gap2（传递等值谓词）在 TPC-DS q72 上打满宿主机内存被回滚（见 `2026-06-15-join-reorder-in-memo-multi-candidate.md`）；根因调研定位为「优化器直接复用 analyzer 的 `TypedExpr`、按值深拷贝」这一**缺层耦合**

本设计基于对 NovaRocks（优化器表示、克隆放大点、exec `ExprArena` 先例、优化器 API 面）与 StarRocks（`ScalarOperator` 共享/判等模型、`ScalarOperatorsReuse` CSE）两套代码库的多路并行通读。

---

## 0. 背景：我们怎么走到这一步

1. join-reorder 重构（#316–#321）落地后，gap2（在 `flatten_join_chain` 里算等值闭包、把全 pairwise 传递边 AND 进 join 条件）在 q72 上 **OOM 被回滚**。

2. 根因不在（已证有界的）reorder 枚举，而在**下游**：注入的候选条件巨大，进 memo 后被深拷贝放大。进一步定位到表示层——

3. **优化器算子直接按值持有 analyzer 的 `analysis::TypedExpr`**（`Box`/`Vec` 子节点、每节点扛一份 Arrow `DataType`、`#[derive(Clone)]` 整树深拷贝、~120–160 B/节点）。memo 用 `GroupId` 共享**计划结构**，但算子拥有的**标量表达式在每次算子 clone 时逐字深拷贝**。于是同一个 join 条件 C 被复制成 `O(候选数)` 份（commutativity / associativity / reorder `join_cells`+`copy_in_join_tree` / implement），`总拷贝字节 ≈ 候选数 × 条件大小`。gap2 同时放大两个因子 → 爆炸。

4. 对照 StarRocks：它**有一层 NovaRocks 缺失的优化器原生标量 IR**——`ScalarOperator`（不是 parser/analyzer 的 AST）。子节点是对象引用（共享指针），memo `copyIn` 传同一引用，规则用 `BaseScalarOperatorShuttle` 只重建改动路径、其余返回原对象。所以 StarRocks 没有「按值深拷贝整棵条件树」的放大。

5. **结论**：把 `TypedExpr` 塞进优化器是缺层耦合；补上优化器原生标量 IR 层（本设计的 `ScalarArena`/`ScalarId`），让算子按 4 字节 `Copy` 句柄引用表达式，即可：(a) 根治内存放大（句柄复制 = O(1)）；(b) 通过 hash-consing 获得 canonical id 判等，顺带收口 ~20 处「`Debug` 序列化整条表达式当 hashkey」的 CPU wart；(c) 为 CSE 提供「结构相同 ⟺ 同一 id」的基础（CSE 本身留作设计附录，本项目不实现）。

---

## 1. 目标与非目标

### 目标
- **G1（内存，首要）**：消除 memo 中标量表达式「每候选深拷贝」放大。算子改为按 `ScalarId`（`Copy` u32 句柄）引用表达式，`MExpr`/`Operator` clone 对表达式变成 O(1)。
- **G2（架构）**：引入优化器原生标量 IR 层（`ScalarArena`），优化器内部不再直接持有 analyzer 的 `TypedExpr`；analyzer→optimizer 之间建立显式 intern 边界，codegen 处建立 materialize 边界。
- **G3（去重收口）**：hash-consing 使「结构相同 ⟺ 同一 `ScalarId`」，把现有 ~20 处 `format!("{:?}", op/kind)` 结构判等 dedup 站点换成 `ScalarId` 比较（既正确又廉价）。
- **G4（CSE 基础，设计-only）**：表示层提供 id 判等，使未来 CSE 检测退化成 id 频次统计。**本项目只交付基础 + CSE 设计附录（§8），不实现 CSE 改写。**

### 非目标
- **不实现 CSE 改写**（用户决策：CSE 重要但不急；本项目顺手把它的基础设施做出来 + 落设计，不投入实现工作）。
- **不重落 gap2**（传递等值谓词）。它是表示层稳固后的独立后续里程碑（§7 M4），本 spec 仅论证「表示稳固后它为何安全」，不在实现范围。
- **不改 exec 层**（`src/exec/**`、`src/lower/**`）。exec 的 `ExprArena` 是物理执行层（leaf=`SlotId`、绑 Arrow kernel/eval），与本层无关；codegen 仍按今天的方式产 thrift `TExpr`。
- **不动 analyzer 的 `TypedExpr` 定义本身**——它继续作为 analyzer 的产出类型；优化器在边界把它 intern 进 `ScalarArena`。

---

## 2. 根因（精确）

- `analysis::TypedExpr { kind: ExprKind, data_type: arrow::DataType, nullable: bool }`（`src/sql/analysis/mod.rs:294`），`ExprKind` 递归子节点为 `Box<TypedExpr>`/`Vec<TypedExpr>`（`mod.rs:309-418`），`#[derive(Clone)]` ⇒ 整树深拷贝；全树**零 `Rc`/`Arc`/intern**（grep `Rc<TypedExpr>`/`Arc<TypedExpr>` 全仓零命中）。
- 算子按值内嵌表达式（`src/sql/optimizer/operator.rs`，`Operator` enum 派生 `Clone`，`MExpr.op: Operator` 按值存于 `memo.rs:163`）。持有点（穷举）：
  - Logical：`LogicalScanOp.predicates`、`LogicalFilterOp.predicate`、`LogicalProjectOp.items`（`ProjectItem.expr`）、`LogicalAggregateOp.group_by`/`.aggregates`（`AggregateCall.args`+`.order_by`）、`LogicalJoinOp.condition`、`LogicalSortOp.items`（`SortItem.expr`）/`.analytic_partition_exprs`、`LogicalTopNOp.items`、`LogicalWindowOp.window_exprs`（`WindowExpr.args`/`.partition_by`/`.order_by`）、`LogicalValuesOp.rows`、`LogicalTableFunctionOp.args`。
  - Physical：上述镜像 + `PhysicalHashJoinOp.{eq_conditions(left/right), other_condition}`、`PhysicalNestLoopJoinOp.condition`。
- 放大点（每个把 condition/op 深拷贝进新 `MExpr`/`NewExpr` 的站点）：`join_commutativity.rs:71`、`join_associativity.rs:128-143`、reorder `algo.rs:166-192/325-334`、`stats.rs:786`（`copy_in_join_tree`）、`implement.rs:275-303`。
- 二阶 CPU wart（与放大同源、本设计 G3 收口）：`op_equal`（`mod.rs:410`）、implement 的 application key（`mod.rs:374`）、`join_group_index`（`memo.rs:60`）、`predicate_group` 的 `canonical_expr_key`、以及 ~20 处 `format!("{:?}",a.kind)==format!("{:?}",b.kind)`，都在用「`Debug` 串」做结构判等/hashkey。

---

## 3. 架构：`ScalarArena` + `ScalarId`（hash-consed）

> 命名为暂定；`ScalarArena`=优化器原生标量 IR 的拥有者，`ScalarId`=句柄，`ScalarNode`=节点（即用户提议的 "ExprOperator" 层）。

### 3.1 类型
```rust
// src/sql/optimizer/scalar/mod.rs (新建)

/// 优化器标量表达式句柄：4 字节 Copy，跨 MExpr/候选复制 = O(1)。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ScalarId(u32);

/// 优化器原生标量节点。叶子是 query-global 的 ColumnId / 字面量；
/// 内部节点与 analysis::ExprKind 一一对应，但子节点用 ScalarId 引用（不内嵌）。
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum ScalarNode {
    // 叶子
    ColumnRef(ColumnId),
    Literal(LiteralValue),
    // 内部（子节点皆为 ScalarId）
    BinaryOp { op: BinOp, left: ScalarId, right: ScalarId },
    UnaryOp  { op: UnaryOp, child: ScalarId },
    Cast     { child: ScalarId },               // 目标类型见并行 types[id]
    FunctionCall { name: String, args: Vec<ScalarId>, distinct: bool },
    AggregateCall { name: String, args: Vec<ScalarId>, distinct: bool, order_by: Vec<SortKey> },
    IsNull   { child: ScalarId, negated: bool },
    InList   { child: ScalarId, list: Vec<ScalarId>, negated: bool },
    Between  { child: ScalarId, low: ScalarId, high: ScalarId, negated: bool },
    Like     { child: ScalarId, pattern: ScalarId, negated: bool },
    Case     { operand: Option<ScalarId>, when_then: Vec<(ScalarId, ScalarId)>, else_expr: Option<ScalarId> },
    IsTruthValue { child: ScalarId, value: TruthValue, negated: bool },
    Nested(ScalarId),
    WindowCall { name: String, args: Vec<ScalarId>, distinct: bool,
                 partition_by: Vec<ScalarId>, order_by: Vec<SortKey>,
                 window_frame: Option<WindowFrame>, ignore_nulls: bool },
    Lambda   { params: Vec<String>, body: ScalarId },
    LambdaParamRef(/* ... */),
    SubqueryPlaceholder { /* id, kind, ... 与 ExprKind 对应 */ },
}
// SortKey { expr: ScalarId, asc: bool, nulls_first: bool } —— 取代 SortItem 的按值 TypedExpr

/// 拥有所有标量节点；intern 时 hash-cons。
pub(crate) struct ScalarArena {
    nodes:    Vec<ScalarNode>,        // index = ScalarId.0
    types:    Vec<DataType>,          // 并行：每 id 的 Arrow 类型
    nullable: Vec<bool>,              // 并行：每 id 的 nullable
    intern:   HashMap<ScalarNode, ScalarId>,  // hash-cons 表
}

impl ScalarArena {
    /// 核心：结构相同（含子 ScalarId、类型、nullable 一致）则返回既有 id。
    pub(crate) fn intern(&mut self, node: ScalarNode, ty: DataType, nullable: bool) -> ScalarId;
    pub(crate) fn node(&self, id: ScalarId) -> &ScalarNode;
    pub(crate) fn data_type(&self, id: ScalarId) -> &DataType;
    pub(crate) fn nullable(&self, id: ScalarId) -> bool;
}
```

### 3.2 hash-consing 与 canonical 不变式
- `intern(node)`：子节点已是 `ScalarId`（已 intern），故哈希/比较是**浅层** O(arity)；命中表则复用，未命中追加。返回的 id 满足 **id 相等 ⟺ 结构相等**——这是 G3（去重收口）与 §8（CSE）依赖的核心性质。
- **交换律规范化**：`AND`/`OR`/`Eq` 等可交换算子在 intern 前对子 `ScalarId` 排序，使 `a AND b` 与 `b AND a` intern 成同一 id（对齐 StarRocks `normalizeChildrenGroup`）。
- **不变式（必须全程成立，见风险 §9）**：优化器内**所有**标量构造都必须经 `intern`（analyzer 边界、规则重写产物、未来派生谓词），否则 id 判等静默失效。
- **类型/nullable 一致性**：intern 命中要求 `(node, ty, nullable)` 完全一致；两个结构相同但类型/nullable 不同的表达式**不得**共享 id（debug_assert 兜底）。

### 3.3 与 exec `ExprArena` 的关系
- 形态参照 exec `ExprArena`（`src/exec/expr/mod.rs`：`Vec<ExprNode>` + `Copy` id + 子节点按 id），但**不复用**：exec 的 leaf 是物理 `SlotId(u32)`、节点绑 Arrow kernel/`eval`/dict/timezone 等执行关切；优化器层 leaf 是 query-global 逻辑 `ColumnId`（`src/sql/column_id.rs`，跨 Project/Window 透传稳定）。两者是「同构但不同关切」的孪生层。exec `ExprArena` **不做** hash-cons（纯 append + 靠 FE `common_slot_map` 复用）；本层 **做** hash-cons。

---

## 4. 边界与生命周期

### 4.1 三段边界
1. **analyzer → optimizer（intern，边界在优化器入口）**：analyzer 仍产 `TypedExpr`；**由优化器在入口**（`optimize()` 收到 `TypedExpr` 计划后、建 memo 前）调 `intern_typed(arena, &TypedExpr) -> ScalarId` 把整棵树 intern。**analyzer 不 import arena**（arena 优化器私有，`src/sql/optimizer/scalar/`）；优化器自此**只见 `ScalarId`**。
2. **optimizer 内部**：算子字段全为 `ScalarId`/`Vec<ScalarId>`/`Option<ScalarId>`；规则读 `arena.node(id)`、产 `arena.intern(...)`。
3. **optimizer → codegen（materialize/compile）**：codegen 的 `ExprCompiler` 增加 `compile_scalar(&ScalarArena, ScalarId)`，按 arena 节点递归产 thrift `TExpr`（首选，免 round-trip）；或在边界 `materialize(arena, id) -> TypedExpr` 还原后走现有 `compile_typed`（过渡期可用）。

### 4.2 arena 生命周期 = 完全复刻 `ColumnRefFactory`
`ColumnRefFactory` 现有路径（`mod.rs:102/118/153`）：`Rc<RefCell<…>>` 挂在 `RewriteContext` 上贯穿 rewrite → 在建 memo 前 `try_unwrap` 移交给 `Memo.factory` → 交给 codegen。`ScalarArena` 走**同一条路**：
- `RewriteContext` 增 `scalars: Rc<RefCell<ScalarArena>>`（`context.rs`）；
- 建 memo 时移交 `Memo.scalars`（`memo.rs`，与 `factory` 并列）；
- 交给 codegen（随 `PhysicalPlanNode`）。

因两大规则 trait 已分别传 `&mut Memo`（Cascades `Rule::apply`，40 impl）与 `&mut RewriteContext`（`LogicalRewriteRule::apply`，54 impl），arena 经 `memo.scalars`/`ctx.scalars` 搭车，**trait 签名零改动**；唯一需补参的是 `PlanRewriteRule`（2 impl）——并入 `LogicalRewriteRule` 或加 `&mut ScalarArena` 参数。

---

## 5. 规则层迁移（最大工作量与风险）

这是本项目的主要成本：**从「就地 `match &mut expr.kind` mutate」迁到「functional rebuild + re-intern」**（持久化重写风格，对齐 StarRocks `BaseScalarOperatorShuttle`：只重建改动路径，未改子树复用原 `ScalarId`）。

- 规模（调研）：~96 rule impl（40 Cascades + 54 LogicalRewriteRule + 2 PlanRewriteRule）、~347 处构造点（改为 `arena.intern`）、~1304 处 `ExprKind` 读点（改为 `arena.node(id)` 匹配）。热点文件：`predicate_pushdown/deriver.rs`(57)、`join_pushdown.rs`(45)、`subquery/apply_to_window.rs`(44)、`predicate_apply_util.rs`(39)、`mv_rewrite/descriptor.rs`(38)、`implement.rs`(38)、`stats.rs`(37)、`rewrite/rules/utils.rs`(36)、`push_through_project.rs`(36)。
- 模式：提供重写辅助 `rebuild(arena, id, f)`：自底向上 visit，叶子/未改子树返回原 id，改动处 `intern` 新节点——使绝大多数规则改动是机械的「`expr.clone()` → `id`（Copy）」「`match &expr.kind` → `match arena.node(id)`」「构造 `TypedExpr{..}` → `arena.intern(ScalarNode{..})`」。

---

## 6. 去重站点收口（G3）

hash-cons 落地后，把下列结构判等/hashkey 从「`Debug` 串」换成 `ScalarId` 比较：
- `op_equal`（`mod.rs:410`）、implement application key（`mod.rs:374`）→ 比较算子的 `ScalarId` 字段；
- `join_group_index`（`memo.rs:60`）的 key → 用 `ScalarId`；
- `predicate_group` 的 `canonical_expr_key` → `ScalarId` 即 key；
- ~20 处 `format!("{:?}",a.kind)==format!("{:?}",b.kind)` → `id_a == id_b`。

收益：去重既正确（canonical）又廉价（id hash/eq），消除整条表达式反复 `Debug` 序列化的 CPU 开销。

---

## 7. 分阶段里程碑

> 迁移策略：**staged（带过渡 bridge）**，非 big-bang（理由见 §11）。每个里程碑结束时全 lib + optimizer golden + TPC-DS SF1 verify 必须绿，plan golden 逐字节不变（intern 是语义保持的）。

- **M0（基础类型 + bridge，先行）**：建 `src/sql/optimizer/scalar/` 模块——`ScalarId`/`ScalarNode`/`ScalarArena`（hash-cons + 交换律规范化 + 并行 type/nullable）；写双向 bridge `intern_typed(&TypedExpr)->ScalarId` 与 `materialize(ScalarId)->TypedExpr`；单测断言「往返相等」且「两个独立构造的结构相同 `TypedExpr` intern 成**同一** `ScalarId`」（id 判等 ⟺ 结构相等的核心 gate）。**不碰任何算子字段。**
- **M1（表示落地，根治内存 = 首要目标）**：算子/wrapper/planner-IR 字段 `TypedExpr → ScalarId`；analyzer 边界 intern；`Rc<RefCell<ScalarArena>>` 贯穿 RewriteContext→Memo→codegen；`ExprCompiler` 加 `compile_scalar`。过渡期用 M0 的 bridge 让未迁移站点仍可 `materialize`，保持 always-green。**验收：q72 重启 gap2 全闭包不再 OOM（内存随条件大小而非候选数）；全 lib + golden 59/59 + TPC-DS 99/99 绿；plan golden 不变。**
- **M2（去重收口）**：§6 的站点改 `ScalarId` 比较，删 `Debug`-串 hashkey。验收：golden 不变 + 微基准显示 explore/implement 的 O(候选²) 判等开销下降。
- **M3（CSE，设计-only，本项目不实现）**：见 §8 附录。仅落设计，不写码。
- **M4（gap2 重落，独立后续，非本 spec 范围）**：表示稳固后重做传递等值谓词——派生谓词全部 intern（相同派生边去重成一个 id 而非倍增）；仍需按 StarRocks `equivalenceDerive` 调研结果加闭包规模硬 bound（见 join-reorder spec memo）。

---

## 8. CSE 设计附录（design-only，不实现）

未来做 CSE 时的方向（已调研 StarRocks，记此备用）：
- **StarRocks 形态**：`ScalarOperatorsReuseRule`，post-CBO 物理树 rewrite（`physicalRuleRewrite` 最后），**逐节点**：对一个算子的表达式集，按结构判等分组，给每个出现 ≥2 次的公共子表达式 mint 一个新列，重复处改引用；产 `commonSubOperatorMap` → 序列化成 thrift `common_slot_map`（exec 侧 `src/lower/expr/mod.rs` 的 `CommonSlotLoweringCtx` **已现成消费**，standalone codegen 现传 `None`）。
- **本层带来的简化**：因 `ScalarArena` hash-cons，「结构相同 ⟺ 同一 `ScalarId`」，CSE 检测退化成 **id 频次统计**（`HashMap<ScalarId,usize>`，count≥2 即公共子表达式），无需 StarRocks 那套 `OperatorId`(equalsSelf+child-group-id) 机制。
- **阶段灵活性**：检测随时可做（不再被迫 post-CBO）；但 CSE **改写**（插 compute-once 列）何时落，仍是 plan-quality 设计题（通常仍偏晚，对最终计划做）。
- **产出对接**：mint 新 `ColumnId` → 建每节点 `commonSubOperatorMap`（新 ColumnId → 公共 `ScalarId`）→ codegen 序列化进 `common_slot_map` → exec 复用现成 compute-once 路径。**零新增 exec 机制。**

---

## 9. 风险

- **canonical 不变式是承重墙**：~20+ 去重站点切到 id 判等后，若有任一构造路径绕过 `intern`（漏了 analyzer 某变体、某规则直接拼 `ScalarNode` 不 intern、未来派生谓词忘 intern），id 判等会静默退化——要么漏判（多余重复、内存回涨）要么误判（错误去重 = 正确性 bug）。M0 的「独立构造→同一 id」单测 + 全路径走 intern 是硬要求。
- **爆炸半径**：三层串联（数据模型 + ~96 规则 + codegen）。staged bridge 控住「长期红 build」，但 bridge 本身要维护一段时间。
- **type/nullable 一致性**：intern 时必须正确推/带 type+nullable；结构相同但类型/nullable 不同者不得共享 id。
- **`RefCell` 借用纪律**：规则持 arena 不可变借用读节点时又 `intern` 会运行时 panic——必须先把 `ScalarId` 拷出再 intern（`Copy` 使这很自然，但要守纪律）。
- **mutation→functional 风格迁移**：~11 处 `match &mut expr.kind` + 各规则的就地改写要改成 rebuild+re-intern；语义等价但是风格切换，是最易藏 bug 处（尤其 move-out-of-`Box` 的旧惯用法）。
- **lambda / subquery placeholder / agg order-by 等变体**：intern/materialize 边界必须**无损覆盖所有 ExprKind 变体**，否则边界丢信息。
- **gap2 重落（M4）仍需独立硬 bound**：intern 去重相同派生边，但**不 bound 组合上不同的 pairwise 边总数**——q72 那次正是这个组合数爆的。

---

## 10. 验收标准

- **内存（首要）**：临时分支重启 gap2 全闭包跑 q72，峰值堆从「随候选数×条件大小」降到「随条件大小」——这是 G1 达成的硬证据。
- **正确性/无回归**：全 lib 单测绿；`sql-tests/optimizer` golden 59/59；TPC-DS SF1 全 99 query verify 99/99（dev-opt 串行 -j1）；**plan golden 逐字节不变**（intern 语义保持）。
- **CPU（M2）**：explore/implement 的判等路径不再 `Debug` 序列化整树。

---

## 11. 已定决策（2026-06-16）

1. **迁移策略 = staged**：保留 `TypedExpr↔ScalarId` 过渡 bridge，逐里程碑 always-green（~96 规则/~347 构造点体量下 big-bang 风险过高）。
2. **`TypedExpr` 在优化器内彻底消失**：M1 后优化器**只认 `ScalarId`**；`materialize()` 仅作 codegen/EXPLAIN 的瞬时视图 + 过渡 bridge，**不**作为 EXPLAIN 的长期视图。
3. **arena 模块 = 优化器私有 `src/sql/optimizer/scalar/`**：analyzer 不 import；intern 在优化器入口完成（见 §4.1）。
4. **arena 所有权 = `Rc<RefCell<ScalarArena>>`**：完全照搬 `ColumnRefFactory` 的现成模式（挂 `RewriteContext` → 移交 `Memo.scalars` → 交 codegen），零新概念、规则 trait 零签名 churn。
5. **范围**：CSE 仅落设计附录（§8）、gap2 重落（M4）独立后续——**两项均不进本项目实现**。

---

## 12. 执行交接

本 spec 经评审通过后，**逐里程碑**用 writing-plans 写 bite-sized 实现计划（M0 先行，含 TDD 步骤）。不在 spec 阶段一次性铺开 ~96 规则的 task。
