# ScalarArena M1（memo-IR cutover：根治内存放大）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`).

**Goal:** 把 memo 的 `Operator`/`MExpr` 所持有的标量表达式从按值 `TypedExpr` 改成 `ScalarId`（引用 `Memo.scalars: ScalarArena`），消除「每候选深拷贝条件」的内存放大（gap2 q72 OOM 根因）。**rewrite 阶段（`LogicalPlanNode` + 54 个 LogicalRewriteRule）本里程碑暂留 `TypedExpr`**（那是 M1.5）。

**Architecture:** intern 边界放在 `logical_plan_to_memo`（convert.rs，`LogicalPlanNode`→`Operator` 转换处）：每个算子的 `TypedExpr` 字段在建 `MExpr` 时 `intern_typed` 成 `ScalarId`。`Memo` 新增**按值**字段 `scalars: ScalarArena`（镜像 `Memo.factory: ColumnRefFactory` 的按值持有；rewrite 期不用它，故 **M1 无需 `Rc<RefCell<>>`**——那是 M1.5 把 rewrite 也切过去时才需要，与 ColumnRefFactory「memo 上按值、rewrite 期 Rc<RefCell>」完全一致）。40 个 Cascades 规则 + search + stats + extract 通过 **bridge**（`materialize` 读、`intern_typed` 写）保持现有 `TypedExpr` 逻辑不变即可编译通过（always-green）；codegen 在 physical-op 边界 `materialize` 回 `TypedExpr` 后走现有 `compile_typed`（零改 codegen）。原生 `ScalarId` 重写（去掉过渡 materialize）是 M1.5+ 的增量优化，**非内存目标所需**。

**Tech Stack:** Rust；M0 的 `crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode, SortKey, intern_typed, materialize}`（已落地、94 测试绿）。

**参照:** spec `docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md` §4/§5/§7；ColumnRefFactory 生命周期 `src/sql/optimizer/mod.rs:102/118/146-153`、`memo.rs:54`。

---

## 关键设计决策（M1 内）

1. **intern 边界 = `logical_plan_to_memo`**（不是 optimize() 入口；入口 intern 是 M1.5 当 rewrite 也切 ScalarId 时）。`LogicalPlanNode`（TypedExpr）进、`Operator`（ScalarId）出。
2. **`Memo.scalars: ScalarArena` 按值**（非 Rc<RefCell>）。Cascades 规则已收 `&mut Memo` → 经 `&mut memo.scalars` 读写，零 trait 签名改动。借用纪律：先把 `ScalarId`（Copy）拷出、再 `memo.scalars.intern(...)`（编译期借用检查，比 RefCell 更安全）。
3. **算子侧 wrapper 类型**：算子当前用 analyzer 的 `SortItem`/`ProjectItem`/`AggregateCall`/`WindowExpr`（含按值 `TypedExpr`），而 `LogicalPlanNode` 仍用它们 → M1 给**算子**引入 ScalarId 版 wrapper：`SortKey`（M0 已有）、新增 `ScalarProjectItem`/`ScalarAggregateSpec`/`ScalarWindowSpec`。analyzer 的原 wrapper 不动（rewrite-IR 继续用）。
4. **bridge 优先**：规则不改逻辑，读处 `materialize(&memo.scalars, id)`、写处 `intern_typed(&mut memo.scalars, &te)`。内存放大消除来自「**memo 存 ScalarId**」（候选间共享），与规则是否原生无关。
5. **codegen** 用 `materialize` 边界（零改 `compile_typed`）；`compile_scalar` 留 M1.5+。

---

## File Structure

- Modify: `src/sql/optimizer/memo.rs` — `Memo` 加 `scalars: ScalarArena` + `Memo::new()` 初始化。
- Modify: `src/sql/optimizer/operator.rs` — 算子表达式字段 `TypedExpr*` → `ScalarId*`；新增 `ScalarProjectItem`/`ScalarAggregateSpec`/`ScalarWindowSpec`。
- Modify: `src/sql/optimizer/convert.rs` — `logical_plan_to_memo` 各算子分支 intern。
- Modify: 40 个 Cascades 规则 + `search.rs`/`stats.rs`/`extract.rs`/`logical_props.rs`/`derive/*`/`cost.rs` — bridge 修编译。
- Modify: codegen `physical_plan.rs`→thrift 边界（physical-op → TExpr 处）。

---

### Task 1: `Memo.scalars: ScalarArena` + 算子侧 wrapper 类型

**Files:** `src/sql/optimizer/memo.rs`、`src/sql/optimizer/operator.rs`

- [ ] **Step 1: `Memo` 加 `scalars` 字段**

`memo.rs`：
```rust
use crate::sql::optimizer::scalar::ScalarArena;

pub(crate) struct Memo {
    pub(crate) groups: Vec<Group>,
    pub(crate) cte_produce_groups: HashMap<CteId, GroupId>,
    pub(crate) factory: ColumnRefFactory,
    pub(crate) join_group_index: HashMap<(String, Vec<GroupId>), GroupId>,
    pub(crate) reorder_owned_groups: HashSet<GroupId>,
    /// Interned scalar expressions for all operators in this memo. Operators
    /// reference expressions by `ScalarId` so cloning an MExpr/Operator across
    /// plan alternatives is O(1) (no deep TypedExpr copy). Held by value like
    /// `factory`; the rewrite phase does not use it (M1.5 will).
    pub(crate) scalars: ScalarArena,
}
```
`Memo::new()` 加 `scalars: ScalarArena::new(),`。

- [ ] **Step 2: 新增算子侧 ScalarId-版 wrapper**

`operator.rs`（紧邻算子定义；`SortKey` 复用 `scalar::SortKey`）：
```rust
use crate::sql::optimizer::scalar::{ScalarId, SortKey};

#[derive(Clone, Debug)]
pub(crate) struct ScalarProjectItem {
    pub expr: ScalarId,
    pub output_name: String,
    pub output_column_id: ColumnId,
}
#[derive(Clone, Debug)]
pub(crate) struct ScalarAggregateSpec {
    pub name: String,
    pub args: Vec<ScalarId>,
    pub distinct: bool,
    pub order_by: Vec<SortKey>,
}
#[derive(Clone, Debug)]
pub(crate) struct ScalarWindowSpec {
    pub name: String,
    pub args: Vec<ScalarId>,
    pub distinct: bool,
    pub partition_by: Vec<ScalarId>,
    pub order_by: Vec<SortKey>,
    pub window_frame: Option<WindowFrame>,
    pub ignore_nulls: bool,
}
```
> 字段与 analyzer 的 `ProjectItem`/`AggregateCall`/`WindowExpr` 一一对应，仅把 `TypedExpr`→`ScalarId`、`SortItem`→`SortKey`。

- [ ] **Step 3: 编译确认（暂不动算子字段，wrapper 仅定义）**

Run: `cargo build --lib 2>&1 | grep -E '^error' | head`
Expected: 无 error（新增字段/类型，未被使用挂 dead_code 警告可接受）。

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/memo.rs src/sql/optimizer/operator.rs
git commit -m "feat(optimizer): add Memo.scalars arena + ScalarId operator wrappers (M1 task 1)"
```

---

### Task 2: 翻转算子表达式字段 `TypedExpr*` → `ScalarId*`

**Files:** `src/sql/optimizer/operator.rs`

- [ ] **Step 1: 逐字段翻转（Logical + Physical）**

按下表改字段类型（`Operator` enum 派生 `Clone` 不变；改完会有大量 compile error，Task 3/4 修）：

| 算子.字段 | 旧类型 | 新类型 |
|---|---|---|
| `LogicalScanOp.predicates` | `Vec<TypedExpr>` | `Vec<ScalarId>` |
| `LogicalFilterOp.predicate` | `TypedExpr` | `ScalarId` |
| `LogicalProjectOp.items` | `Vec<ProjectItem>` | `Vec<ScalarProjectItem>` |
| `LogicalAggregateOp.group_by` | `Vec<TypedExpr>` | `Vec<ScalarId>` |
| `LogicalAggregateOp.aggregates` | `Vec<AggregateCall>` | `Vec<ScalarAggregateSpec>` |
| `LogicalJoinOp.condition` | `Option<TypedExpr>` | `Option<ScalarId>` |
| `LogicalSortOp.items` | `Vec<SortItem>` | `Vec<SortKey>` |
| `LogicalSortOp.analytic_partition_exprs` | `Vec<TypedExpr>` | `Vec<ScalarId>` |
| `LogicalTopNOp.items` | `Vec<SortItem>` | `Vec<SortKey>` |
| `LogicalWindowOp.window_exprs` | `Vec<WindowExpr>` | `Vec<ScalarWindowSpec>` |
| `LogicalValuesOp.rows` | `Vec<Vec<TypedExpr>>` | `Vec<Vec<ScalarId>>` |
| `LogicalTableFunctionOp.args` | `Vec<TypedExpr>` | `Vec<ScalarId>` |
| `PhysicalScanOp.predicates` | `Vec<TypedExpr>` | `Vec<ScalarId>` |
| `PhysicalFilterOp.predicate` | `TypedExpr` | `ScalarId` |
| `PhysicalProjectOp.items` | `Vec<ProjectItem>` | `Vec<ScalarProjectItem>` |
| `PhysicalHashJoinEqCondition.{left,right}` | `TypedExpr` | `ScalarId` |
| `PhysicalHashJoinOp.other_condition` | `Option<TypedExpr>` | `Option<ScalarId>` |
| `PhysicalNestLoopJoinOp.condition` | `Option<TypedExpr>` | `Option<ScalarId>` |
| `PhysicalHashAggregateOp.{group_by,aggregates}` | `Vec<TypedExpr>`/`Vec<AggregateCall>` | `Vec<ScalarId>`/`Vec<ScalarAggregateSpec>` |
| `PhysicalSortOp.{items,analytic_partition_exprs}` | `Vec<SortItem>`/`Vec<TypedExpr>` | `Vec<SortKey>`/`Vec<ScalarId>` |
| `PhysicalTopNOp.items` | `Vec<SortItem>` | `Vec<SortKey>` |
| `PhysicalWindowOp.window_exprs` | `Vec<WindowExpr>` | `Vec<ScalarWindowSpec>` |
| `PhysicalValuesOp.rows` | `Vec<Vec<TypedExpr>>` | `Vec<Vec<ScalarId>>` |
| `PhysicalTableFunctionOp.args` | `Vec<TypedExpr>` | `Vec<ScalarId>` |

> 用 `grep -n 'TypedExpr\|SortItem\|ProjectItem\|AggregateCall\|WindowExpr' src/sql/optimizer/operator.rs` 核对无遗漏。

- [ ] **Step 2: 不单独编译**（编译留到 Task 3/4 后；此 task 只改类型）。Commit 改型：
```bash
git add src/sql/optimizer/operator.rs
git commit -m "feat(optimizer): flip operator expression fields to ScalarId (M1 task 2, build red until task 4)"
```
> 这是 staged 迁移里唯一允许「中途红」的一步；下一 task 立即接 intern 边界 + bridge 修绿。

---

### Task 3: `logical_plan_to_memo` intern 边界

**Files:** `src/sql/optimizer/convert.rs`

- [ ] **Step 1: 各算子分支把 `TypedExpr` 字段 intern**

模式（以 Filter / Project / Join 为例，其余照此）：
```rust
use crate::sql::optimizer::scalar::{intern_typed, ScalarProjectItem, /* … */};

// Filter
let predicate = intern_typed(&mut memo.scalars, &node.predicate);
let op = Operator::LogicalFilter(LogicalFilterOp { predicate });

// Scan.predicates / Aggregate.group_by / Values.rows / TableFunction.args: 逐项 intern
let predicates = node.predicates.iter().map(|e| intern_typed(&mut memo.scalars, e)).collect();

// Join.condition (Option)
let condition = node.condition.as_ref().map(|c| intern_typed(&mut memo.scalars, c));

// Project.items: TypedExpr ProjectItem -> ScalarProjectItem
let items = node.items.iter().map(|it| ScalarProjectItem {
    expr: intern_typed(&mut memo.scalars, &it.expr),
    output_name: it.output_name.clone(),
    output_column_id: it.output_column_id,
}).collect();

// Sort/TopN.items: SortItem -> SortKey
let items = node.items.iter().map(|s| SortKey {
    expr: intern_typed(&mut memo.scalars, &s.expr), asc: s.asc, nulls_first: s.nulls_first,
}).collect();

// Aggregate.aggregates -> ScalarAggregateSpec ; Window.window_exprs -> ScalarWindowSpec（同构映射）
```
> ⚠️ 借用：`intern_typed(&mut memo.scalars, ...)` 借 `&mut memo.scalars`；同分支若还需 `&mut memo`（如 `new_group`/`next_expr_id`）须**先 intern 拿到 `ScalarId`、再建 `MExpr`/`new_group`**（先算子级 intern 完，再碰 memo group）。`logical_plan_to_memo` 对子节点已是「先递归子、再建父」，intern 插在「建 op 之前」即可。

- [ ] **Step 2: 不单独编译**（规则/codegen 仍红）。本 task 完成「memo 入口产 ScalarId 算子」。

---

### Task 4: bridge 修绿 Cascades 规则 + search + stats + extract

**Files:** 40 个 Cascades 规则（`cascades_rules/**`）、`search.rs`、`stats.rs`、`extract.rs`、`logical_props.rs`、`derive/*`、`cost.rs` —— 由编译器报错驱动，逐处套 bridge 模式。

- [ ] **Step 1: 套用 bridge 模式修每个编译错误**

- **读**算子表达式（规则要看条件做判断/估计）：
  ```rust
  let cond_te = op.condition.map(|id| materialize(&memo.scalars, id)); // ScalarId -> TypedExpr
  // …沿用现有基于 TypedExpr 的逻辑…
  ```
- **写**新算子（规则产新条件）：
  ```rust
  let new_id = intern_typed(&mut memo.scalars, &new_te);
  Operator::LogicalFilter(LogicalFilterOp { predicate: new_id })
  ```
- **直接搬运**（规则只是把子算子的条件原样挪/换 children，不改表达式）：直接拷 `ScalarId`（`Copy`），**无需 materialize**——这是最常见、也最省的情形（join 交换/结合、列裁剪等多数只动结构不动标量）。
- `estimate_join_condition`/`stats.rs` 取 condition：`materialize` 后喂现有估计器（M1 保持估计逻辑不变）。
- `extract.rs`（memo→PhysicalPlanNode）：physical-op 的 `ScalarId` 字段**原样保留**（PhysicalPlanNode 也持 `ScalarId`），到 codegen 再 materialize（见 Task 5）。

> 注意借用纪律：需要 `materialize`（借 `&memo.scalars`）与 `intern_typed`（借 `&mut memo.scalars`）同段出现时，先把 `materialize` 结果拿到本地 `TypedExpr`、结束不可变借用，再 `intern_typed`。

- [ ] **Step 2: 全 lib 编译 + 单测**

Run: `cargo build --lib 2>&1 | grep -E '^error' | head`
Expected: 无 error。
Run: `cargo test --lib 2>&1 | grep -E 'test result|FAILED' | tail`
Expected: 全绿（plan golden 在外部 sql-tests，见 Task 6）。

- [ ] **Step 3: Commit**

```bash
cargo fmt
git add -A
git commit -m "feat(optimizer): intern at logical_plan_to_memo + bridge cascades/search/stats to ScalarId (M1 tasks 3-4)"
```

---

### Task 5: codegen 边界 materialize

**Files:** codegen 中把 physical-op 表达式编译成 thrift `TExpr` 的位置（`physical_plan.rs`/`codegen/**` 调 `ExprCompiler::compile_typed` 处）

- [ ] **Step 1: 在 physical-op → TExpr 边界把 `ScalarId` materialize 回 `TypedExpr`**

凡现在 `compile_typed(&te)` 而 `te` 来自 physical-op 字段的，改为：
```rust
let te = materialize(&scalars, id);   // scalars: 从 Memo 交给 codegen 的 ScalarArena
compiler.compile_typed(&te)
```
> codegen 需能拿到 `ScalarArena`：随 `PhysicalPlanNode` 一起从 Memo 交付（codegen 入口加 `&ScalarArena` 参数，或把 arena 移交给 codegen 上下文，镜像 `factory` 的交付路径）。`compile_typed` 本身**零改动**。

- [ ] **Step 2: 编译 + 单测**

Run: `cargo build 2>&1 | grep -E '^error' | head` ；`cargo test --lib 2>&1 | grep 'test result' | tail`
Expected: 无 error、全绿。

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat(optimizer): materialize ScalarId at codegen boundary (M1 task 5)"
```

---

### Task 6: 验收（内存 + 无回归 + plan 逐字节不变）

- [ ] **Step 1: fmt/clippy + 全 lib 单测**

Run: `cargo fmt --check` ；`cargo clippy --lib 2>&1 | grep -E '^error'` ；`cargo test --lib 2>&1 | grep 'test result' | tail`
Expected: clean、全绿。

- [ ] **Step 2: dev-opt 重编 + 起 server**

参照 CLAUDE.md：`cargo build --profile dev-opt --bin novarocks`；`source docker/iceberg-rest/runtime/current/env.sh`；起 server 等 `NOVAROCKS_READY`。

- [ ] **Step 3: optimizer golden + TPC-DS 99/99（plan 逐字节不变是核心验收）**

Run optimizer 套件（`--suite optimizer --mode verify -j 1`）：Expected **59/59**（intern 语义保持 → plan golden 不变；若有 diff 说明 bridge 往返不保真，须修）。
Run TPC-DS（`--suite tpc-ds --mode verify -j 1`，分批，参照之前）：Expected **99/99**。

- [ ] **Step 4: 内存验收（G1 硬证据）**

临时分支重启 gap2 全闭包（或用一个大 join + 多候选的合成查询），跑 q72：确认峰值堆**随条件大小而非候选数**、不再 OOM。记录 before（main，TypedExpr）/after（M1，ScalarId）的优化器峰值内存对比。

- [ ] **Step 5: 推送 + PR**

```bash
git push -u fork claude/<m1-branch>
gh pr create --repo NovaRocks/NovaRocks --base main --head HangyuanLiu:claude/<m1-branch> --title "feat(optimizer): memo-IR scalar cutover to ScalarId (M1) — fixes per-alternative deep-clone OOM" --body-file <body>
```

---

## Self-Review

- **Spec 覆盖**：M1 = spec §7 M1 的「memo 部分」（算子字段→ScalarId、建 memo 边界 intern、arena 挂 Memo、codegen materialize、bridge always-green、验收 q72 不 OOM + golden/TPC-DS + plan 逐字节不变）。**有意把 rewrite-IR（LogicalPlanNode + 54 规则）切分到 M1.5**（内存放大只在 memo；rewrite 是单树无放大）——这是对 spec §7 M1 的 staged 细化，blast radius 减半。
- **决策一致**：`Memo.scalars` 按值（非 Rc<RefCell>）= 镜像 `Memo.factory` 按值持有；decision #4 的 Rc<RefCell> 是 M1.5 rewrite 期线程化时引入（与 ColumnRefFactory 路径一致），不矛盾。
- **占位扫描**：Task 2 改型表逐字段列全；Task 4 的规则迁移是「编译器报错驱动 + bridge 模式（读 materialize / 写 intern / 搬运直接拷 id）」——模式具体、非 hand-wave，但 ~40 规则站点不逐一展开（机械）。Task 3/5 给了具体 intern/materialize 代码。
- **类型一致**：`ScalarId`/`SortKey`/`ScalarProjectItem`/`ScalarAggregateSpec`/`ScalarWindowSpec`/`intern_typed`/`materialize` 全计划一致。

---

## 后续（非本计划）

- **M1.5**：rewrite-IR cutover——`Rc<RefCell<ScalarArena>>` 挂 RewriteContext、intern 上移到 `optimize()` 入口、`LogicalPlanNode` + 54 个 LogicalRewriteRule 切 `ScalarId`，**兑现「TypedExpr 在优化器内彻底消失」**（决策 #2 完成态）。
- **M2**：去重站点（`op_equal`/`application_key`/`join_group_index`/`canonical_expr_key`/~20 处 `Debug`-串 hashkey）换 `ScalarId` 比较 + `compile_scalar`（去 codegen materialize）+ 原生规则迁移（去 bridge materialize）。
- **M3/M4**：CSE（设计附录）、gap2 重落——均独立后续。

---

## Execution Handoff

M1 计划完成。两种执行：
1. **Subagent-Driven（推荐）**：每 task 派新 subagent、task 间审查（Task 4 规则迁移量大，建议拆给 subagent 分文件做）。
2. **Inline**：本会话 executing-plans 批量 + checkpoint。
