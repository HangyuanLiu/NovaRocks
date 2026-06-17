# A2 — RBO on OptExpr（大翻转）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 一个 PR 内把整个 RBO rewrite 层从 `LogicalPlanNode` 翻成 `OptExpr`/`Operator`/`ScalarId`：trait/result/traversal/pipeline/registry 全部 re-type，~56 条规则 + `required_columns.rs` 列裁剪引擎全迁，Bridge 1 移到 `optimize()` 入口（rewrite 之前），`Rc<RefCell<ScalarArena>>` 跨 rewrite。终点:优化器 rewrite 只认 `Operator`,`LogicalPlanNode` 只在入口 Bridge 1 处被消费一次。

**Architecture（大翻转 = in-place re-type，编译 RED 到全部迁完）:** 这不是行为保持的"绿到绿"重构那种可逐步验证;它是一次性翻转。**执行法 = 编译器驱动**:先翻机器层(Phase 3),`cargo build` 会在每个未迁站点报错,把编译器当 worklist,按 §迁移 Recipe 逐站点修,直到 GREEN。**全程编译 RED,直到 Phase 3 末尾**。逐字节等价由末端 golden + TPC-DS 验证(行为不变是硬目标:rewrite 逻辑不变,只换标量/节点表示)。

**Tech Stack:** Rust;`cargo build`(dev);`cargo test --lib`;sql-test runner(optimizer golden + tpc-ds)。

**规模(已实测,含测试代码):** 56 规则 impl(column_pruning 17 + imv 18 + subquery 7 + 其余少量)+ `required_columns.rs` 2236 行;`required_output_columns` 读 55 处、`required_columns` 15 处;accessor ~466 处(`.unary_input()` 281/`.right()` 89/`.left()` 70/`.child()` 26);`LogicalPlanNode::new` 639 处、`LogicalPlanNodeKind::` 1858 处。

**关键事实(post-A1 main):**
- `OptExpr { op: Operator, children: Vec<OptExpr> }`(`src/sql/optimizer/opt_expr.rs`,A1 落地)。Bridge 1 = `convert::logical_plan_to_opt_expr(&LogicalPlanNode, &mut ScalarArena) -> OptExpr`;copy-in = `convert::opt_expr_to_memo(&OptExpr, &mut Memo) -> GroupId`。
- `LogicalRewriteRule`(`rewrite/rule.rs`):`matches(&LogicalPlanNode, &RewriteContext)->bool` + `apply(LogicalPlanNode, &mut RewriteContext)->Result<RewriteResult,String>`;便捷 trait `PlanRewriteRule`(`matches(&LogicalPlanNode)` + `apply(LogicalPlanNode)->Option<LogicalPlanNode>`)。
- `RewriteResult`(`rewrite/result.rs`):`Unchanged | Changed(LogicalPlanNode) | Rejected(RewriteDiagnostic)`。
- 遍历(`rewrite/tree.rs`)kind-agnostic:靠 `plan.children` 递归 + `rule.matches/apply`。
- pipeline(`rewrite/pipeline.rs`):`rewrite(LogicalPlanNode)->LogicalPlanNode`,stages×fixpoint×rules×`rewrite_with_rule`。
- `optimize_with_root_property`(`optimizer/mod.rs:91`):factory 包 `Rc<RefCell<>>`(:105)→ `rewrite_ctx.set_column_ref_factory`(:121)→ rewrite(:122)→ convert 时 `Rc::try_unwrap` 进 `memo.factory`(:148-157)。**arena 完全照搬此生命周期。**
- `ScalarArena`(`optimizer/scalar/mod.rs`):`intern(node,ty,nullable)->ScalarId`、`node(id)->&ScalarNode`、`data_type(id)`、`nullable(id)`;`scalar::intern_typed(&mut arena,&TypedExpr)->ScalarId`、`scalar::materialize(&arena,id)->TypedExpr`;`scalar_bridge::{intern_exprs, intern_project_items, ...}`。

---

## File Structure

- Modify: `src/sql/optimizer/opt_expr.rs` — 加 `required_output_columns` 注解字段 + accessor(`unary_input/left/right/child`)。
- Modify: `src/sql/optimizer/rewrite/{result.rs, rule.rs, tree.rs, pipeline.rs, registry.rs, context.rs}` — re-type 到 OptExpr + ctx 加 arena。
- Modify: `src/sql/optimizer/rewrite/required_columns.rs` — 列裁剪引擎 re-type(最大单块)。
- Modify: `src/sql/optimizer/rewrite/rules/**`、`rewrite/imv/**` — 56 规则迁移。
- Modify: `src/sql/optimizer/mod.rs` — 入口 Bridge 1 前移 + Rc<RefCell> arena + copy-in。
- 不删 `LogicalPlanNode`(planner 仍产出,入口 Bridge 1 消费);不删 `convert::logical_plan_to_memo` wrapper(非 rewrite 调用方仍用)。

---

## §迁移 Recipe（每个规则/站点的机械替换）

把一条规则从 `LogicalPlanNode` 迁到 `OptExpr`:

1. **匹配**:`matches!(&plan.kind, LogicalPlanNodeKind::X(node))` → `matches!(&expr.op, Operator::LogicalX(op))`。`plan` → `expr`(`&OptExpr`)。
2. **子节点**:`plan.unary_input()`/`.left()`/`.right()`/`.child(i)` → `OptExpr` 上的同名 accessor(Phase 1 加)。`plan.children` → `expr.children`。
3. **per-node 注解**:`plan.required_output_columns` → `expr.required_output_columns`(Phase 1 加的字段)。
4. **读标量**:`node.predicate`(`TypedExpr`)/`ExprKind::...` 深度匹配 → `op.predicate`(`ScalarId`)+ 经 arena 检视:`arena.node(id)` 返回 `&ScalarNode`(镜像 `ExprKind`),按 `ScalarNode::BinaryOp{..}` 等匹配。arena 经 `ctx.scalar_arena()`(Phase 2 加)拿。
5. **建标量**:规则新造表达式(谓词拆 AND、常量、去关联谓词)→ `ctx.scalar_arena().borrow_mut().intern(...)` 或先建 `TypedExpr` 再 `scalar::intern_typed`。
6. **建结果**:`LogicalPlanNode::new(LogicalPlanNodeKind::X(node), children, req)` → `OptExpr { op: Operator::LogicalX(op), children, required_output_columns: req }`(或 `OptExpr::new`/`leaf` + 设注解)。
7. **返回**:`RewriteResult::Changed(LogicalPlanNode)` → `Changed(OptExpr)`(result.rs re-type 后类型自动对)。

`PlanRewriteRule` 便捷 trait 同样 re-type(`matches(&OptExpr)`/`apply(OptExpr)->Option<OptExpr>`)。

**注意**:rewrite 逻辑本身**一行不改**(下推还是下推、裁剪还是裁剪)——只换载体(节点 kind→Operator、TypedExpr→ScalarId、LogicalPlanNode→OptExpr)。任何逻辑改动都是 bug。

---

## Task 1: 建分支 + OptExpr 富化(annotations + accessors)— 可绿

**Files:** `src/sql/optimizer/opt_expr.rs`

- [ ] **Step 1: 分支 + 基线**

```bash
git fetch origin && git switch -c claude/a2-rbo-optexpr origin/main
cargo build --lib 2>&1 | tail -3   # 基线绿
```

- [ ] **Step 2: 给 `OptExpr` 加 `required_output_columns` + accessors**

```rust
use crate::sql::analysis::OutputColumn;   // 确认 LogicalPlanNode.required_output_columns 的类型,对齐之

#[derive(Clone, Debug)]
pub(crate) struct OptExpr {
    pub op: Operator,
    pub children: Vec<OptExpr>,
    /// Mirrors LogicalPlanNode.required_output_columns — the column-pruning
    /// annotation rules read/propagate. None until column pruning sets it.
    pub required_output_columns: Option<Vec<OutputColumn>>,
}

impl OptExpr {
    pub(crate) fn new(op: Operator, children: Vec<OptExpr>) -> Self {
        Self { op, children, required_output_columns: None }
    }
    pub(crate) fn leaf(op: Operator) -> Self {
        Self { op, children: Vec::new(), required_output_columns: None }
    }
    pub(crate) fn unary_input(&self) -> &OptExpr { &self.children[0] }
    pub(crate) fn left(&self) -> &OptExpr { &self.children[0] }
    pub(crate) fn right(&self) -> &OptExpr { &self.children[1] }
    pub(crate) fn child(&self, i: usize) -> &OptExpr { &self.children[i] }
}
```

> 先核对 `LogicalPlanNode.required_output_columns` 的确切类型(读 `planner/plan.rs` 的 struct 定义),让 `OptExpr` 字段类型与之**完全一致**,否则 Bridge 1/copy-in 搬运会失配。同时检查 `tree.rs` 测试里 `LogicalPlanNode::new(.., required_output_columns)` 第三参类型。

- [ ] **Step 3: Bridge 1 / copy-in 搬运注解**

`convert::logical_plan_to_opt_expr` 每个 arm 末尾把 `plan.required_output_columns.clone()` 塞进 OptExpr(目前 A1 丢了它——之前没人用,现在 rules 要读)。最省力:在 `OptExpr` 构造后统一 `expr.required_output_columns = plan.required_output_columns.clone();`。copy-in `opt_expr_to_memo` 若 memo 侧需要该注解则一并搬(核对旧 `logical_plan_to_memo` 是否把它写进 MExpr/group;大概率不写——它是 rewrite 期注解,memo 不用,确认后 copy-in 可忽略它)。

- [ ] **Step 4: 编译(应绿,additive)**

Run: `cargo build --lib 2>&1 | tail -3` → PASS。

- [ ] **Step 5: 提交**

```bash
git add -A && git commit -m "feat(optimizer): enrich OptExpr with required_output_columns + child accessors"
```

---

## Task 2: RewriteContext 暴露 `Rc<RefCell<ScalarArena>>`(照搬 factory）— 可绿

**Files:** `src/sql/optimizer/rewrite/context.rs`

- [ ] **Step 1: 加 arena 字段 + setter + accessor,完全镜像 `column_ref_factory`**

读 `context.rs` 里 `column_ref_factory: Option<Rc<RefCell<ColumnRefFactory>>>`(或类似)及其 `set_column_ref_factory` / `column_ref_factory()`。照葫芦画瓢加:

```rust
scalar_arena: Option<Rc<RefCell<ScalarArena>>>,
// + pub(crate) fn set_scalar_arena(&mut self, arena: Rc<RefCell<ScalarArena>>) { self.scalar_arena = Some(arena); }
// + pub(crate) fn scalar_arena(&self) -> Rc<RefCell<ScalarArena>> { self.scalar_arena.clone().expect("scalar arena must be set before rewrite") }
```

- [ ] **Step 2: 编译(绿,additive,暂未用)+ 提交**

Run: `cargo build --lib 2>&1 | tail -3` → PASS。
```bash
git add -A && git commit -m "feat(optimizer): thread Rc<RefCell<ScalarArena>> through RewriteContext"
```

---

## Task 3: 大翻转(编译 RED → 全迁完 GREEN)

> 本任务全程编译 RED。每个 sub-step 后**不要求**绿;只在 Step Z(全迁完 + 入口 rewire)后要求绿。建议 RED 期间也按 sub-step 提交(`git commit --no-verify`)以便 checkpoint/bisect。

- [ ] **Step A: re-type 机器层(result/rule/tree/pipeline)**

  - `result.rs`:`Changed(LogicalPlanNode)` → `Changed(OptExpr)`;`use ...opt_expr::OptExpr;` 替换 `use ...plan::LogicalPlanNode;`。
  - `rule.rs`:`LogicalRewriteRule` 与 `PlanRewriteRule` 的 `matches`/`apply` 把 `LogicalPlanNode` → `OptExpr`。
  - `tree.rs`:`rewrite_with_rule`/`rewrite_top_down`/`rewrite_bottom_up`/`apply_rule_to_node`/`rewrite_children`/`rewrite_plan_list` 全部 `LogicalPlanNode` → `OptExpr`(逻辑不变,`plan.children` → `expr.children`)。
  - `pipeline.rs`:`RewriteStage.rules: Vec<Box<dyn LogicalRewriteRule>>`(类型名不变,trait 已是 OptExpr 版)、`rewrite(plan: OptExpr) -> Result<OptExpr,String>`。

- [ ] **Step B: re-type registry.rs**

  `query_rewrite_pipeline` 等组装不变(stage 顺序、phase 不动);仅因 rule trait 变为 OptExpr 版而自动对齐。`rule_names`/`stage_names` 不变。

- [ ] **Step C: `cargo build` → 得到 worklist**

Run: `cargo build --lib 2>&1 | tee /tmp/a2-errors.txt | tail -5`
现在编译器在**每个未迁规则/列裁剪/站点**报错。`grep -c '^error' /tmp/a2-errors.txt` 看剩余量。下面按 dir 逐块清零。

- [ ] **Step D: 迁 predicate_pushdown**(`rewrite/rules/predicate_pushdown/`)

按 §迁移 Recipe。**worked example(下推穿过 Project,纯结构 + 读标量):**
```rust
// before: matches!(&plan.kind, LogicalPlanNodeKind::Filter(_))
//         && matches!(&plan.unary_input().kind, LogicalPlanNodeKind::Project(_))
// after:
matches!(&expr.op, Operator::LogicalFilter(_))
    && matches!(&expr.unary_input().op, Operator::LogicalProject(_))
```
谓词拆 AND(读标量):旧 `match &predicate.kind { ExprKind::BinaryOp{op: BinaryOperator::And, left, right} => ...}` → `let arena = ctx.scalar_arena(); match arena.borrow().node(predicate_id) { ScalarNode::BinaryOp{op: BinaryOperator::And, left, right} => /* left/right 已是 ScalarId */ ...}`;新造的拆分谓词若需要新 ScalarId,直接复用已有子 id(拆 AND 不造新标量,只取 children id)。
build 后 `grep -c '^error'` 应下降。

- [ ] **Step E: 迁 variant_path_pushdown / ranking_window_predicate_pushdown / low_cardinality_dict / standalone(derive_join_not_null, ukfk)**

各按 Recipe。low_cardinality_dict 会造新列/新标量 → 经 `ctx.scalar_arena().borrow_mut()` intern。

- [ ] **Step F: 迁 aggregate_pushdown**(`rewrite/rules/aggregate_pushdown/`)

OPT-1 规则。注意 `AggregateNode::already_pushed` 幂等位:迁移后对应 `LogicalAggregateOp` 的等价字段(`is_split`/`stage`?核对 operator.rs)——**保持幂等语义不变**。

- [ ] **Step G: 迁 subquery(7 条,去关联)**

最易出逻辑漂移的一块:子查询去关联会**构造相关谓词/Apply 改写**。新造的标量必须 intern 进 arena。逐条对照旧逻辑,确保去关联结果等价(golden 守)。

- [ ] **Step H: 迁 column_pruning(17 条 + `required_columns.rs` 2236 行)— 最大单块**

`required_columns.rs` 自顶向下计算每节点 `required_output_columns`。re-type:遍历 `OptExpr`(读 `expr.op` 判类型、`expr.children`、写 `expr.required_output_columns`);读列引用从 `TypedExpr`/`ExprKind::ColumnRef` 改为经 arena 检视 `ScalarNode::ColumnRef`(或直接用算子already-carried 的 ColumnId 集合,优先后者避免 materialize)。17 条裁剪规则按 Recipe。**这块单独 build 清零再进下一步。**

- [ ] **Step I: 迁 imv(18 条,`rewrite/imv/`)**

IMV rewrite 规则。同 Recipe。注意 imv 专属算子(ImvDelta/ImvVersion)的 Operator 变体。

- [ ] **Step J: 入口 rewire(`optimizer/mod.rs`)+ arena 跨 rewrite + copy-in**

把 `optimize_with_root_property` 改成(照搬 factory 的 Rc 生命周期):
```rust
let factory = Rc::new(RefCell::new(factory));
let arena = Rc::new(RefCell::new(ScalarArena::new()));
// Bridge 1 在 rewrite 之前:LogicalPlanNode -> OptExpr,intern 进共享 arena
let opt_expr = convert::logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut());
let mut rewrite_ctx = RewriteContext::for_query(...);
rewrite_ctx.set_column_ref_factory(Rc::clone(&factory));
rewrite_ctx.set_scalar_arena(Rc::clone(&arena));               // 新
let rewritten: OptExpr = query_rewrite_pipeline(table_stats).rewrite(opt_expr, &mut rewrite_ctx)?;
// residual-apply / cte_rewrite:这些目前吃 LogicalPlanNode —— 评估:要么迁到 OptExpr,要么在此处对 OptExpr 操作(见下)。
// unwrap factory + arena 进 memo:
drop(rewrite_ctx);
let factory = Rc::try_unwrap(factory)...into_inner();
let arena = Rc::try_unwrap(arena)...into_inner();
let mut memo = Memo::new();
memo.factory = factory;
memo.scalars = arena;
let root_group = convert::opt_expr_to_memo(&rewritten, &mut memo);
```
**子任务**:`find_residual_apply`(mod.rs:129)与 `cte_rewrite::{collect_cte_counts, inline_single_use_ctes}`(:135-136)目前吃 `LogicalPlanNode`。它们是 pre-Memo 结构步骤,也要迁到 `OptExpr`(或确认可在 OptExpr 上等价实现)。一并在本步处理。

- [ ] **Step Z: build GREEN**

Run: `cargo build --lib 2>&1 | tail -3` → 0 errors(可能有 warning)。
Run: `grep -c '^error' <(cargo build --lib 2>&1)` → 0。
若仍有 error,回到对应 dir step 清零。提交:
```bash
git add -A && git commit -m "refactor(optimizer): A2 big-flip — RBO rewrite operates on OptExpr/Operator"
```

---

## Task 4: 验收门

- [ ] **Step 1: 全 lib 单测**(很多 rewrite 规则有 #[cfg(test)],一并迁了)
Run: `cargo test --lib 2>&1 | tail -15` → 全 PASS。优先确认 `cargo test --lib sql::optimizer::rewrite` 绿。
- [ ] **Step 2: fmt + clippy**
Run: `cargo fmt && cargo clippy --lib 2>&1 | tail -5` → 无 error。
- [ ] **Step 3: optimizer golden(逐字节等价 —— 行为不变的权威证据)**
起 standalone-server(`source docker/iceberg-rest/runtime/current/env.sh`,等 `NOVAROCKS_READY`),`sql-tests --suite optimizer --mode verify` → 全 PASS。
- [ ] **Step 4: TPC-DS SF1 全量(dev-opt,串行)**
`cargo build --profile dev-opt` 后 `sql-tests --suite tpc-ds --mode verify -j 1` → 99/99。**这是大翻转必须全跑的门**(抽样不够)。
- [ ] **Step 5: 确认 rewrite 层不再出现 LogicalPlanNode**
Run: `grep -rn 'LogicalPlanNode' src/sql/optimizer/rewrite/ | grep -v '#\[cfg(test)\]\|mod tests' | head`
Expected: rewrite 规则/机器层无 `LogicalPlanNode`(仅入口 convert 的 Bridge 1 签名 + 必要 import;测试除外)。
- [ ] **Step 6: push + PR**
```bash
git push fork claude/a2-rbo-optexpr
gh pr create --repo NovaRocks/NovaRocks --base main --head HangyuanLiu:claude/a2-rbo-optexpr \
  --title "refactor(optimizer): A2 — RBO rewrite on OptExpr (big-flip off LogicalPlanNode)" \
  --body "Arc A step 2 (spec §6). Big-flip: the entire RBO rewrite layer (trait/result/traversal/pipeline/registry/~56 rules + column-pruning engine + imv) now operates on OptExpr/Operator/ScalarId. Bridge 1 moved to optimize() entry; Rc<RefCell<ScalarArena>> spans rewrite (mirrors ColumnRefFactory). Behavior-preserving: optimizer golden byte-identical, TPC-DS 99/99. RBO no longer touches LogicalPlanNode (only entry Bridge 1 consumes it)."
```

---

## Self-Review

**1. Spec coverage(对 §6 Arc A 之 A2):** Bridge 1 前移到 rewrite 前(Step J)、54+ 规则迁 OptExpr(Step D-I)、`Rc<RefCell>` arena 跨 rewrite(Task 2 + Step J)、convert 收敛成 copy-in(Step J,优化器入口 `…→OptExpr→rewrite→copy-in→memo`)。✓ A3 的"入口签名收口/删 LogicalPlanNode 遍历"大部分在此完成;残留(彻底删旧 trait 死代码、入口类型最终收口)留 A3 收尾。

**2. Placeholder 扫描:** 机器层 re-type、入口 rewire、OptExpr 富化给了完整代码;1858 个站点用「编译器驱动 + §迁移 Recipe + 每 dir worked example」覆盖(非 placeholder——是精确机械过程,编译器枚举站点)。column_pruning/subquery/imv 三大块单独成 step 并标注风险点。

**3. 类型一致性:** `OptExpr.required_output_columns` 类型须与 `LogicalPlanNode.required_output_columns` 一致(Task 1 Step 2 显式核对);`ctx.scalar_arena()` 返回 `Rc<RefCell<ScalarArena>>` 镜像 `column_ref_factory()`;`Changed(OptExpr)` 贯穿 result/rule/tree/pipeline。

**4. 行为不变守门:** rewrite 逻辑一行不改(只换载体);末端 golden 逐字节 + TPC-DS 99/99 是硬门(Task 4 Step 3/4)。RED 期无法增量验,故末端门是唯一安全网——必须全绿才算完。

**5. 风险点:** ① subquery 去关联 / column_pruning 易逻辑漂移(造新标量、列集合计算)——逐条对旧逻辑 + golden 兜;② `find_residual_apply`/`cte_rewrite` 这两个 pre-Memo 步骤也吃 LogicalPlanNode,易漏(Step J 子任务显式处理);③ RED 窗口长、无中间绿点——这是大翻转的固有代价,你已知情选择。

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-17-a2-rbo-on-optexpr-big-flip.md`. Two execution options:**

**1. Subagent-Driven** — 但注意:大翻转 RED 窗口长,per-task 绿验证不适用;subagent 之间难以"绿到绿"交接。若用,建议单个大 implementer 完成整个 Task 3(编译器驱动清零),再 review。

**2. Inline Execution(更适合大翻转)** — 本会话内编译器驱动逐 dir 清零,末端统一验收。

**Which approach?**
