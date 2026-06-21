# OptExpr + Bridge 1（A1）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 引入优化器逻辑算子树类型 `OptExpr { op: Operator, children: Vec<OptExpr> }`，并把 `convert::logical_plan_to_memo` 拆成 **Bridge 1**（`logical_plan_to_opt_expr`：建 Operator 树 + intern 标量）+ **copy-in**（`opt_expr_to_memo`：铸 memo group），为 A2 的 RBO-on-OptExpr 迁移铺地基。

**Architecture:** 行为保持的纯重构。当前 `logical_plan_to_memo(&LogicalPlanNode, &mut Memo)` 在一次自底向上遍历里同时 intern 标量（进 `memo.scalars`）并铸 memo group。A1 把这次遍历拆成两步：Bridge 1 只建 `OptExpr` 树并 intern，copy-in 只铸 group。`logical_plan_to_memo` 退化成 `{ let e = logical_plan_to_opt_expr(plan, &mut memo.scalars); opt_expr_to_memo(&e, memo) }` 的薄 wrapper——**所有现有调用点（~20 处：stats 测试、mv_rewrite、descriptor 等）零改动**。可证逐字节等价：标量 id 空间与 group id 空间相互独立，Bridge 1 的 intern 顺序 = 旧遍历的 intern 顺序，copy-in 的铸组顺序 = 旧遍历的铸组顺序。**rewrite pipeline 与 54 规则本阶段不碰**（仍在 `LogicalPlanNode` 上跑，Bridge 1 仍在 rewrite 之后）。

**Tech Stack:** Rust；`cargo build`（dev）；`cargo test --lib`；sql-test runner（optimizer golden）。

**范围边界：** A1 只引入类型 + 拆函数。把 Bridge 1 移到 rewrite 之前、把 54 规则迁到 OptExpr 是 **A2**（另出 plan）；`OptExpr` 届时再加 derived logical-property 字段（A1 暂为 `{op, children}`）。`optimize()` 入口本阶段不改（wrapper 兜住）。

**关键事实（已核实，post-A0 main）：**
- `convert.rs:20` `pub(crate) fn logical_plan_to_memo(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId`，~24 个 `LogicalPlanNodeKind` arm；每 arm：recurse 子节点→GroupId、构造 `Operator::LogicalX(...)`（经 `intern_typed`/`intern_*` 写入 `memo.scalars`）、`MExpr { id: memo.next_expr_id(), op, children }`、`memo.new_group(expr)`。部分 arm 还调 `memo.scalars.remember_source_column_display(...)` / `remember_column_display_from_scalar(...)`。
- `Memo`（`memo.rs:45`）含 `pub(crate) scalars: ScalarArena`（`memo.rs:71`）、`next_expr_id()`、`new_group(MExpr)`。
- `MExpr { id, op: Operator, children: Vec<GroupId> }`（`memo.rs:170`）。
- Operator 已 post-A0 同构（`FilterOp`/`ScanOp`/… 共享，`LogicalFilter(FilterOp)` 变体）。

---

## File Structure

- Create: `src/sql/optimizer/opt_expr.rs` — `OptExpr` 类型（仅 `{op, children}` + Clone/Debug）。
- Modify: `src/sql/optimizer/mod.rs` — 加 `pub(crate) mod opt_expr;`。
- Modify: `src/sql/optimizer/convert.rs` — 新增 `logical_plan_to_opt_expr`（Bridge 1）与 `opt_expr_to_memo`（copy-in）；`logical_plan_to_memo` 改为 wrapper。

---

## Task 1: 建分支 + 基线绿

**Files:** 无代码改动。

- [ ] **Step 1: 从 main 建分支**

```bash
git fetch origin && git switch -c claude/optexpr-bridge-a1 origin/main
```

- [ ] **Step 2: 基线绿 + 记录 optimizer 单测数**

Run: `cargo test --lib sql::optimizer 2>&1 | tail -5`
Expected: 全 PASS。记下 `N passed`。

---

## Task 2: 定义 `OptExpr` 类型

**Files:**
- Create: `src/sql/optimizer/opt_expr.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: 写 `opt_expr.rs`**

```rust
//! `OptExpr` — the optimizer's concrete logical operator tree.
//!
//! Mirrors StarRocks `OptExpression`: an `Operator` payload plus child
//! `OptExpr`s. Scalars inside the operator are already interned `ScalarId`
//! handles into the owning `ScalarArena`. This is the tree the RBO rewrite
//! phase will operate on (A2); `convert::opt_expr_to_memo` copies it into the
//! Memo for CBO.

use super::operator::Operator;

#[derive(Clone, Debug)]
pub(crate) struct OptExpr {
    pub op: Operator,
    pub children: Vec<OptExpr>,
}

impl OptExpr {
    pub(crate) fn new(op: Operator, children: Vec<OptExpr>) -> Self {
        Self { op, children }
    }

    pub(crate) fn leaf(op: Operator) -> Self {
        Self { op, children: Vec::new() }
    }
}
```

- [ ] **Step 2: 在 `mod.rs` 注册模块**

在 `src/sql/optimizer/mod.rs` 的模块声明区（`pub(crate) mod convert;` 附近）加：

```rust
pub(crate) mod opt_expr;
```

- [ ] **Step 3: 编译**

Run: `cargo build 2>&1 | tail -10`
Expected: PASS（`OptExpr` 暂未使用，可能有 dead_code warning，下个任务即用；若 CI 视 warning 为 error，给类型加 `#[allow(dead_code)]` 临时压制，Task 3 用上后删）。

- [ ] **Step 4: 提交**

```bash
git add -A && git commit -m "feat(optimizer): add OptExpr logical operator tree type"
```

---

## Task 3: Bridge 1 + copy-in，`logical_plan_to_memo` 退化成 wrapper

**Files:**
- Modify: `src/sql/optimizer/convert.rs`

- [ ] **Step 1: 加 `opt_expr_to_memo`（copy-in，trivial）**

在 `convert.rs` 顶部 import 加 `use super::opt_expr::OptExpr;`，并新增：

```rust
/// Copy an `OptExpr` tree into the Memo as Groups (one Group per node).
/// The operator already holds interned `ScalarId`s, so no scalar interning
/// happens here — this is the trivial StarRocks-style `copyIn`.
pub(crate) fn opt_expr_to_memo(expr: &OptExpr, memo: &mut Memo) -> GroupId {
    let children: Vec<GroupId> =
        expr.children.iter().map(|c| opt_expr_to_memo(c, memo)).collect();
    let mexpr = MExpr {
        id: memo.next_expr_id(),
        op: expr.op.clone(),
        children,
    };
    memo.new_group(mexpr)
}
```

- [ ] **Step 2: 把 `logical_plan_to_memo` 的 body 转成 `logical_plan_to_opt_expr`（Bridge 1）**

把现有 `pub(crate) fn logical_plan_to_memo(plan, memo) -> GroupId { match ... }` 整体复制成新函数 `logical_plan_to_opt_expr`，签名改为 `(plan: &LogicalPlanNode, scalars: &mut ScalarArena) -> OptExpr`，并对**每个 arm** 做 3 处机械替换：

1. 递归调用 `logical_plan_to_memo(child, memo)` → `logical_plan_to_opt_expr(child, scalars)`（返回 `OptExpr` 而非 `GroupId`）。
2. intern / display 调用里的 `&mut memo.scalars` / `memo.scalars` → `scalars`。
3. arm 尾部 `let expr = MExpr { id: memo.next_expr_id(), op, children: vec![..] }; memo.new_group(expr)` → `OptExpr { op, children: vec![..] }`（children 现在是 `OptExpr` 列表）。

`use` 区加 `use crate::sql::optimizer::scalar::ScalarArena;`（若未引入）。函数签名与样板 arm：

```rust
pub(crate) fn logical_plan_to_opt_expr(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> OptExpr {
    match &plan.kind {
        // leaf 示例（Scan）
        LogicalPlanNodeKind::Scan(node) => {
            for column in &node.columns {
                scalars.remember_source_column_display(
                    column.column_id,
                    node.alias.clone(),
                    column.name.clone(),
                );
            }
            let op = Operator::LogicalScan(ScanOp {
                database: node.database.clone(),
                table: node.table.clone(),
                alias: node.alias.clone(),
                columns: node.columns.clone(),
                predicates: intern_exprs(scalars, &node.predicates),
                required_columns: node.required_columns.clone(),
                dict_columns: node.dict_columns.clone(),
                variant_columns: node.variant_columns.clone(),
                mv_rewritten_from: None,
            });
            OptExpr::leaf(op)
        }

        // unary 示例（Filter）
        LogicalPlanNodeKind::Filter(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let op = Operator::LogicalFilter(FilterOp {
                predicate: intern_typed(scalars, &node.predicate),
            });
            OptExpr::new(op, vec![child])
        }

        // binary 示例（Join）
        LogicalPlanNodeKind::Join(node) => {
            let left = logical_plan_to_opt_expr(plan.left(), scalars);
            let right = logical_plan_to_opt_expr(plan.right(), scalars);
            let op = Operator::LogicalJoin(LogicalJoinOp {
                join_type: node.join_type,
                condition: node
                    .condition
                    .as_ref()
                    .map(|condition| intern_typed(scalars, condition)),
            });
            OptExpr::new(op, vec![left, right])
        }

        // 含 display 副作用示例（Aggregate）
        LogicalPlanNodeKind::Aggregate(node) => {
            let child = logical_plan_to_opt_expr(plan.unary_input(), scalars);
            let group_by = intern_exprs(scalars, &node.group_by);
            for (scalar_id, output) in group_by.iter().zip(node.output_columns.iter()) {
                scalars.remember_column_display_from_scalar(output.column_id, *scalar_id);
            }
            let aggregates = intern_aggregate_calls(scalars, &node.aggregates);
            let op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                group_by,
                aggregates,
                node.output_columns.clone(),
            ));
            OptExpr::new(op, vec![child])
        }

        // …… 其余所有 arm（Project / Sort / Limit / TopN / Window / Union /
        // Intersect / Except / Values / GenerateSeries / TableFunction / Repeat /
        // AssertOneRow / CTEAnchor / CTEProduce / CTEConsume / Decode /
        // AggregateStateMerge 等）按上面 3 条替换规则逐一转换；arm 内部逻辑、
        // intern 调用、display 副作用、子节点访问器（unary_input/left/right/child(i)）
        // 全部保持不变，只把「铸 group」换成「建 OptExpr」、把 memo.scalars 换成 scalars。
    }
}
```

- [ ] **Step 3: 把 `logical_plan_to_memo` 改成 wrapper**

```rust
/// Convert a `LogicalPlanNode` tree into Memo groups (Bridge 1 + copy-in).
/// Kept as a thin wrapper so existing call sites are unchanged.
pub(crate) fn logical_plan_to_memo(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId {
    let opt_expr = logical_plan_to_opt_expr(plan, &mut memo.scalars);
    opt_expr_to_memo(&opt_expr, memo)
}
```

- [ ] **Step 4: 编译**

Run: `cargo build 2>&1 | tail -20`
Expected: PASS。常见错误：某 arm 漏改 `memo.scalars`→`scalars`（借用错误）、漏把尾部换成 `OptExpr`（类型不匹配）——按提示逐一修。

- [ ] **Step 5: 跑 optimizer 单测（byte-identical 守门）**

Run: `cargo test --lib sql::optimizer 2>&1 | tail -10`
Expected: 全 PASS，N 与 Task 1 基线一致（convert 的现有测试经 wrapper 仍覆盖）。

- [ ] **Step 6: 跑全 lib 单测（convert 被 stats/mv_rewrite 等广泛调用）**

Run: `cargo test --lib 2>&1 | tail -15`
Expected: 全 PASS。

- [ ] **Step 7: 提交**

```bash
git add -A && git commit -m "refactor(optimizer): split logical_plan_to_memo into OptExpr Bridge 1 + copy-in"
```

---

## Task 4: 验收门 + PR

**Files:** 无改动，纯验证。

- [ ] **Step 1: fmt + clippy**

Run: `cargo fmt && cargo clippy --lib 2>&1 | tail -20`
Expected: fmt 无 diff；clippy 无 error（确认 `OptExpr` 无残留 dead_code/allow，已被 wrapper 使用）。

- [ ] **Step 2: optimizer golden（plan 逐字节不变 —— A1 行为保持的核心证据）**

按 CLAUDE.md 起 standalone-server（`source docker/iceberg-rest/runtime/current/env.sh`，等 `NOVAROCKS_READY`），再：
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
```
Expected: 全 PASS（plan-golden 逐字节不变）。

- [ ] **Step 3: TPC-DS SF1 抽样（确认端到端无回归）**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-ds --only q1,q4,q11,q72 --mode verify -j 1
```
Expected: 全 PASS。

- [ ] **Step 4: 确认 wrapper 不变契约（调用点零改动）**

Run: `grep -rn 'logical_plan_to_memo' src/ | grep -v 'fn logical_plan_to_memo' | wc -l`
Expected: > 0（所有旧调用点仍调 wrapper，未被改写）。

- [ ] **Step 5: 推 fork + 开 PR**

```bash
git push fork claude/optexpr-bridge-a1
gh pr create --repo NovaRocks/NovaRocks --base main --head HangyuanLiu:claude/optexpr-bridge-a1 \
  --title "feat(optimizer): A1 — introduce OptExpr + split convert into Bridge 1 + copy-in" \
  --body "Arc A step 1 (spec §6, docs/design/specs/2026-06-17-unified-plan-node-and-optimizer-encapsulation.md). Introduces OptExpr { op: Operator, children } and factors logical_plan_to_memo into logical_plan_to_opt_expr (Bridge 1: build Operator tree + intern) + opt_expr_to_memo (copy-in). logical_plan_to_memo kept as a thin wrapper — zero call-site churn. Behavior-preserving: optimizer golden byte-identical, all lib tests green. RBO rules untouched (A2 migrates them onto OptExpr)."
```

---

## Self-Review

**1. Spec coverage（对 §6 Arc A 之 A1）：**
- 「引入 `OptExpr { op: Operator, children, props }`」→ Task 2 引入 `{op, children}`；props 显式 defer 到 A2（架构注已说明）。✓
- 「入口 Bridge 1（`LogicalPlanNode → LogicalOperator`，intern）」→ Task 3 `logical_plan_to_opt_expr`。本阶段 Bridge 1 仍在 rewrite 之后（A2 移到之前），符合「A1 不碰 rewrite/规则」边界。✓
- 「copy-in」→ Task 3 `opt_expr_to_memo`。✓
- arena 生命周期复刻 ColumnRefFactory → A1 仍用 `memo.scalars`（wrapper 内 `&mut memo.scalars`）；`Rc<RefCell>` 跨 rewrite 是 A2 把 Bridge 1 前移时引入，A1 不需要。✓（对齐边界）

**2. Placeholder 扫描：** Bridge 1 给了 4 个代表性 arm（leaf/unary/binary/含 display 副作用）的完整代码 + 3 条精确替换规则覆盖其余 arm；copy-in/wrapper/类型全量给出。无 TBD/“类似上文”（替换规则是精确机械过程）。✓

**3. 类型一致性：** `OptExpr::new(op, children)` / `OptExpr::leaf(op)` 构造子在 Task 2 定义、Task 3 使用一致；`logical_plan_to_opt_expr(_, scalars: &mut ScalarArena) -> OptExpr`、`opt_expr_to_memo(&OptExpr, &mut Memo) -> GroupId`、wrapper 三者签名互洽；`memo.scalars: ScalarArena` 字段名与 memo.rs 一致。✓

**4. byte-identical 论证复核：** 标量 id 空间（arena）与 group id 空间（memo）相互独立，互不读写；Bridge 1 的 intern 遍历顺序 = 旧 `logical_plan_to_memo` 的 intern 顺序（同一 match、同一子节点访问顺序）；copy-in 的铸组顺序（children-first 递归）= 旧铸组顺序 → 同 GroupId、同 `next_expr_id()`。故 memo 结构与 plan 输出逐字节不变。Task 3 Step 5/6 + Task 4 Step 2 守门。✓

---

## Execution Handoff

**Plan complete and saved to `docs/design/plans/2026-06-17-optexpr-bridge-a1.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — 每个 Task 派新 subagent，任务间 review。

**2. Inline Execution** — 本会话内按 executing-plans 批量执行 + 检查点。

**Which approach?**
