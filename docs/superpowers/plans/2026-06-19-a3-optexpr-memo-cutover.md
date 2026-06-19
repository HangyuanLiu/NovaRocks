# A3 — OptExpr→Memo 直通 + 清除残留 LogicalPlanNode 往返 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development 或 executing-plans。**执行教训(来自 A2)**:大机械迁移**只用串行、受控、on-branch 的 subagent**(一次一个,逐 dir 提交,控制者每轮验证错误计数)——**绝不并行放 isolated-worktree swarm**(A2 时这么干造成大量白工+混乱)。

**Goal:** 去掉 A2 留下的 `convert::opt_expr_to_logical_plan` 在**优化器生产路径**上的所有往返,让 rewrite 之后的 cte_rewrite + memo 构建**直接吃 `OptExpr`**(`OptExpr → memo` via `opt_expr_to_memo`),收干净优化器内最后的 `LogicalPlanNode` 依赖。完成后 Arc A(优化器封装)真正闭环。

**Architecture:** 行为保持。当前主路径是**双重往返**:`rewrite(OptExpr) → opt_expr_to_logical_plan → LogicalPlanNode → cte_rewrite(LogicalPlanNode) → logical_plan_to_memo`(= Bridge1 再 intern 一遍 + copy-in)。A3 改成单条:`rewrite(OptExpr) → cte_rewrite(OptExpr) → opt_expr_to_memo(OptExpr→memo)`。同样,`low_cardinality_dict` 规则与 imv entrypoint 内部仍 `materialize→旧 LogicalPlanNode 逻辑→re-intern` 往返,A3 把它们的内部逻辑也迁到 OptExpr,从而 `opt_expr_to_logical_plan` 退化为**仅测试用**(或删除)。逐字节等价由 optimizer golden 守门。

**Tech Stack:** Rust;`cargo build --lib`;`cargo test --lib`;sql-test runner(optimizer golden + tpc-ds)。

**迁移 recipe(同 A2,已验证):** `LogicalPlanNodeKind::X(n)` 匹配 → `Operator::LogicalX(op)`;`plan: &/owned LogicalPlanNode` → `expr: &/owned OptExpr`;子节点 `.unary_input()/.left()/.right()/.child(i)/.children` 同名;`plan.required_output_columns` → `expr.required_output_columns`;标量 `ScalarId`,经 `ctx.scalar_arena()` 或传入的 `&ScalarArena`:`arena.node(id)` 检视、`scalar::intern_typed`/`materialize` 进出;构造 `OptExpr { op: Operator::LogicalX(op), children, required_output_columns }`。**逻辑一行不改,只换载体。**

**关键事实(post-#339 main):**
- 主路径往返:`optimizer/mod.rs:140` `let rewritten = convert::opt_expr_to_logical_plan(rewritten_expr, &arena.borrow());` 之后 `cte_rewrite::{collect_cte_counts, inline_single_use_ctes}`(`cte_rewrite.rs`,吃 `LogicalPlanNode`)再 `convert::logical_plan_to_memo`。
- `convert::opt_expr_to_memo(&OptExpr, &mut Memo) -> GroupId` 已存在(A1),`stats.rs:726` 已在用 → 主路径可直接改用它。
- `cte_rewrite.rs`:`collect_cte_counts(&LogicalPlanNode)`、`inline_single_use_ctes(LogicalPlanNode)`、`replace_cte_consume(LogicalPlanNode)`,匹配 `CTEAnchor/CTEConsume/CTEProduce/ImvDelta/ImvVersion`。
- `low_cardinality_dict/rule.rs:40`:`apply` 内 `opt_expr_to_logical_plan(expr,&arena)` → 调旧 `collector::collect`/`rewriter::rewrite`(仍 `LogicalPlanNode`)→ 结果再 intern 回。`collector.rs`/`rewriter.rs` 未迁。
- `imv/entrypoint.rs:71` + `imv/mod.rs:16`:imv 入口/helper 仍往返。其余 imv 文件里的 `opt_expr_to_logical_plan` 多在 `#[cfg(test)]`(测试 fixture 往返断言)。
- `logical_plan_to_memo` wrapper(Bridge1+copy-in)**保留**——非 rewrite 路径仍用(`stats.rs`、`cascades_rules/mv_rewrite/{descriptor,rule}.rs`)。

---

## File Structure

- Modify: `src/sql/optimizer/cte_rewrite.rs` — 3 函数迁 OptExpr。
- Modify: `src/sql/optimizer/mod.rs` — 主路径去往返,直走 `cte_rewrite(OptExpr) → opt_expr_to_memo`。
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/{collector.rs, rewriter.rs, rule.rs}` — 内部迁 OptExpr,去 rule 的往返。
- Modify: `src/sql/optimizer/rewrite/imv/{entrypoint.rs, mod.rs, ...}` — 生产 helper 迁 OptExpr。
- Modify: `src/sql/optimizer/convert.rs` — A3 末尾:`opt_expr_to_logical_plan` 改 `#[cfg(test)]` 或删(若生产无调用)。

---

## Task 1: 分支 + 基线

- [ ] **Step 1:** `git fetch origin && git switch -c claude/a3-optexpr-memo-cutover origin/main`
- [ ] **Step 2:** `cargo build --lib 2>&1 | tail -3`(绿)+ `cargo test --lib sql::optimizer 2>&1 | tail -3`(记基线通过数)。

---

## Task 2（核心):cte_rewrite 迁 OptExpr + 主路径去往返

**Files:** `cte_rewrite.rs`、`mod.rs`

- [ ] **Step 1:** 迁 `cte_rewrite.rs` 的 `collect_cte_counts`/`inline_single_use_ctes`/`replace_cte_consume` 到 `OptExpr`(按 recipe;匹配 `Operator::LogicalCTEAnchor/CTEConsume/CTEProduce`;`ImvDelta/ImvVersion` arm 保留同样处理)。签名:`collect_cte_counts(&OptExpr)`、`inline_single_use_ctes(OptExpr) -> Result<OptExpr,String>`、`replace_cte_consume(OptExpr,...,&OptExpr)`。迁其 `#[cfg(test)]`(用 `OptExpr`/`Operator` builder)。
- [ ] **Step 2:** `mod.rs` `optimize_with_root_property`:删 `let rewritten = convert::opt_expr_to_logical_plan(...)`;改为对 `rewritten_expr: OptExpr` 直接调 `cte_rewrite::collect_cte_counts(&rewritten_expr)` + `cte_rewrite::inline_single_use_ctes(rewritten_expr, &cte_ctx)?` → `rewritten_expr: OptExpr`;然后 `let root_group = convert::opt_expr_to_memo(&rewritten_expr, &mut memo);`(替代 `logical_plan_to_memo(&rewritten, ...)`)。`find_residual_apply` 已吃 `&OptExpr`(A2),不变。
- [ ] **Step 3:** `cargo build --lib 2>&1 | tail`(绿)+ `cargo test --lib sql::optimizer 2>&1 | tail`(同基线通过数)。
- [ ] **Step 4:** commit `refactor(optimizer): A3 — cte_rewrite on OptExpr + main path OptExpr→memo (drop round-trip)`。

---

## Task 3：low_cardinality_dict 内部迁 OptExpr(去 rule 往返)

**Files:** `low_cardinality_dict/{collector.rs, rewriter.rs, rule.rs}`

- [ ] **Step 1:** 迁 `collector.rs`(`collect`)+ `rewriter.rs`(`rewrite`)到 `OptExpr`(按 recipe;它们遍历计划/重写 scan dict 列;标量经 arena)。
- [ ] **Step 2:** `rule.rs` `apply`:删 `opt_expr_to_logical_plan(expr,&arena)` 往返,直接对 `OptExpr` 调迁好的 `collector::collect`/`rewriter::rewrite`。
- [ ] **Step 3:** build 绿 + dict 相关单测通过(`cargo test --lib low_cardinality_dict`)。恢复 A2 时 `#[cfg(lcd_tests_todo)]` 门住的 dict 测试(若有),迁到 OptExpr 并解除门。
- [ ] **Step 4:** commit `refactor(optimizer): A3 — low_cardinality_dict collector/rewriter on OptExpr (drop round-trip)`。

---

## Task 4：imv 生产路径迁 OptExpr(去 imv 往返)

**Files:** `imv/entrypoint.rs`、`imv/mod.rs`,及其调用的生产 helper（非 `#[cfg(test)]`）。

- [ ] **Step 1:** 审 `imv/mod.rs:16` 的 helper 与 `imv/entrypoint.rs:71` 的 `opt_expr_to_logical_plan`——它们把 rewrite 后的 OptExpr 转回 LogicalPlanNode 交给下游(IMV refresh 边界)。判断下游是否必须 LogicalPlanNode(IMV refresh 可能确实需要 LogicalPlanNode 交接给 engine)。**若下游是 engine 的 LogicalPlanNode 边界(优化器之外),则这个往返是正当的"出口物化",保留**(类似 codegen 的 materialize),只是确认它不在优化器内部逻辑里。
- [ ] **Step 2:** 把 imv 内部仍依赖 LogicalPlanNode 的**生产**逻辑(若有)迁 OptExpr;纯出口物化(优化器→engine 边界)保留并加注释说明。
- [ ] **Step 3:** build 绿 + imv 单测通过。commit `refactor(optimizer): A3 — imv production path on OptExpr (keep only optimizer-exit materialization)`。

---

## Task 5：收口 opt_expr_to_logical_plan + 全量验收

**Files:** `convert.rs`,纯验证。

- [ ] **Step 1:** `grep -rn 'opt_expr_to_logical_plan' src/ | grep -v '#\[cfg(test)\]\|mod tests'` —— 确认生产调用只剩"正当出口物化"(imv→engine 边界,若 Task 4 判定保留)或为空。若为空:把 `opt_expr_to_logical_plan` 标 `#[cfg(test)]`(仅测试 fixture 用)或删除。
- [ ] **Step 2:** `cargo fmt && cargo clippy --lib 2>&1 | tail`(无 error)。
- [ ] **Step 3:** `cargo test --lib 2>&1 | tail`(全绿,≥ A2 的 5071)。
- [ ] **Step 4:** optimizer golden:起 server(`source docker/iceberg-rest/runtime/current/env.sh`,等 `NOVAROCKS_READY`)→ `sql-tests --suite optimizer --mode verify`。**重点**:确认主路径去双重往返后 plan 逐字节不变(理论上等价:去掉了 materialize→re-intern,cte_rewrite 行为保持)。有漂移则定位(应无)。
- [ ] **Step 5:** TPC-DS SF1 `--suite tpc-ds --mode verify -j 1` → 99/99。
- [ ] **Step 6:** push fork + PR:
```bash
git push fork claude/a3-optexpr-memo-cutover
gh pr create --repo NovaRocks/NovaRocks --base main --head HangyuanLiu:claude/a3-optexpr-memo-cutover \
  --title "refactor(optimizer): A3 — OptExpr→memo direct + drop LogicalPlanNode round-trips" \
  --body "Arc A finish (spec §6). Removes the post-rewrite OptExpr→LogicalPlanNode round-trip from the optimizer's production paths: cte_rewrite now runs on OptExpr, the main path goes OptExpr→memo via opt_expr_to_memo, low_cardinality_dict collector/rewriter migrated, imv production path on OptExpr (only optimizer-exit materialization to the engine boundary kept). opt_expr_to_logical_plan is now test-only/removed. Behavior-preserving: optimizer golden byte-identical, TPC-DS 99/99, cargo test --lib green."
```

---

## Self-Review

**1. Spec coverage（对 §6 Arc A 之 A3 "convert 收敛成 memo copy-in"）:** Task 2 主路径 OptExpr→memo direct(`opt_expr_to_memo`)、cte_rewrite 迁 OptExpr;Task 3/4 清掉 rule/imv 内部往返;Task 5 收口 reverse materializer。✓
**2. Placeholder:** recipe 复用 A2 已验证模式 + 具体文件/函数/行号;无 TBD。imv(Task 4)需先判定"出口物化是否正当保留"——已显式作为 Step 1 的判断,非含糊。
**3. 类型一致性:** `cte_rewrite` 三函数签名统一 `OptExpr`;主路径用现成 `opt_expr_to_memo`(stats.rs 已用);`logical_plan_to_memo` wrapper 保留给非 rewrite 调用方。
**4. 行为保持:** 去双重往返理论等价(materialize→re-intern 是冗余);golden 逐字节 + TPC-DS 99/99 守门(Task 5)。
**5. 风险:** ① imv 出口物化可能确实正当(优化器→engine 边界),Task 4 不强删,判断后保留+注释——避免破坏 IMV refresh;② cte_rewrite 的 ImvDelta/ImvVersion arm 行为须原样保留。

---

## Execution Handoff

**串行受控执行(A2 教训)**:一次一个 on-branch subagent(或 inline),逐 Task 提交,每步验证。Task 2 是核心(主路径直通);3/4 是 rule/imv 内部去往返;5 收口+门。Arc A 完成后,**Arc B(planner 侧统一 LogicalPlanNodeKind+DistributedPlanNodeKind)** 另出 plan。
