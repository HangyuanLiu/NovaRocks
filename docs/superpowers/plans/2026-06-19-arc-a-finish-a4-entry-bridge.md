# Arc A 收尾 — A4(估计器去 TypedExpr)+ 入口 Bridge 1 归位 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development 或 executing-plans。**执行纪律(A2 教训)**:大机械迁移**只用串行、受控、on-branch 的 subagent**,逐 dir/文件提交,控制者每步验证;**绝不并行放 isolated-worktree swarm**。

**Goal:** 让 **`TypedExpr` 彻底退出优化器**——这是 spec G1/G4 的目标,分两块:① **A4**:优化器内部估计器(`stats`/`logical_props`/`runtime_filter_pass` + 任何 `derive` 内 materialize)改 `ScalarId`-native;② **入口签收**:`optimize()` 直接收 `OptExpr`(+ 共享 arena),Bridge 1(`logical_plan_to_opt_expr`)移到调用方(engine/mv),`LogicalPlanNode` 不再从入口进优化器。

**依赖:** **A3([#340](https://github.com/NovaRocks/NovaRocks/pull/340))先合入**(它让 cte_rewrite 走 OptExpr、主路径 `OptExpr→opt_expr_to_memo` 直通、去掉 `opt_expr_to_logical_plan` 主路径往返)。本 plan 从 A3 之后起。

**Architecture:** 行为保持。A4 是把"读 TypedExpr 结构"换成"读 `ScalarNode`(`arena.node(id)`)",估计值不变;入口签收是把 Bridge 1 + arena 创建从 `optimize()` 内部上移到调用方,签名从 `optimize(LogicalPlanNode,…)` 变为 `optimize(OptExpr, arena,…)`。逐字节由 optimizer golden + TPC-DS 99/99 守门。

**Tech Stack:** Rust;`cargo build --lib`/`test --lib`;sql-test(optimizer golden + tpc-ds)。

**非目标:** 不碰 codegen/Bridge 2 的 `ScalarId→TypedExpr`(planner 侧、正当永久,G4);不做 Arc B;不做 CSE/gap2。

**关键事实(需在分支上现查):** 优化器内 `materialize` 站点(A2 后)约:`stats.rs` ~20、`logical_props.rs` ~6、`runtime_filter_pass.rs` ~2(+ 可能 `derive/*`);`scalar::materialize(&arena, id) -> TypedExpr`、`arena.node(id) -> &ScalarNode`(镜像 `ExprKind`)。`optimize_with_root_property`(`optimizer/mod.rs`)目前收 `plan: LogicalPlanNode`、内部 `let arena = Rc::new(RefCell::new(ScalarArena::new())); logical_plan_to_opt_expr(&plan, …)`。`optimize()` 调用方:`grep -rn 'optimizer::optimize\|optimize_with_root' src/engine src/sql` 找全。

---

## Task 1: 分支 + 基线
- [ ] `git fetch origin && git switch -c claude/arc-a-finish origin/main`(确认 main 已含 A3 #340;若未合,等)。
- [ ] `cargo build --lib`(绿)+ `cargo test --lib sql::optimizer`(记基线)。
- [ ] 现查 materialize 站点:`grep -rno 'materialize' src/sql/optimizer/{stats.rs,logical_props.rs,runtime_filter_pass.rs,derive} | grep -v scalar_bridge | wc -l`,登记清单。

## Task 2(A4):估计器去 TypedExpr — stats.rs
**Files:** `src/sql/optimizer/stats.rs`
- [ ] **Step 1:** 逐个 `materialize(...)` 站点改 `ScalarId`-native。两种模式:(a) 只读列引用集合 → 直接用算子已带的 ColumnId,或 `arena.node(id)` 递归收集 `ScalarNode::ColumnRef`(写一个 `collect_scalar_column_ids(arena, id)` helper,可能 A2 column_pruning 已有 `collect_scalar_column_ids`——复用);(b) 需要表达式结构(selectivity 判 BinaryOp/比较等)→ 把"读 `TypedExpr.kind`/`ExprKind`"改成"读 `arena.node(id)`/`ScalarNode`"(同构枚举)。**估计逻辑/数值一行不改。**
- [ ] **Step 2:** `cargo build --lib`(绿)+ `cargo test --lib sql::optimizer::stats`(同基线)。`grep -c 'materialize' src/sql/optimizer/stats.rs`(降到 0,或仅剩注释)。
- [ ] **Step 3:** commit `refactor(optimizer): A4 — stats.rs estimators ScalarId-native (drop materialize)`。

## Task 3(A4):logical_props.rs + runtime_filter_pass.rs(+ derive 残留)
**Files:** `logical_props.rs`、`runtime_filter_pass.rs`、`derive/*`(若有 materialize)
- [ ] 同 Task 2 recipe,逐文件迁。`runtime_filter_pass.rs` 的 `materialize(eq.left/right)` → 直接用 equi-key 的 ScalarId/ColumnId 构造 runtime filter desc(看下游要什么;若下游(codegen/exec)要 TypedExpr,则这是"优化器→exec 出口物化",**保留并注释**,同 codegen)。
- [ ] 每文件 build + 相关单测绿 + commit。

## Task 4(入口签收):optimize() 收 OptExpr,Bridge 1 上移调用方
**Files:** `optimizer/mod.rs`、optimize() 的调用方(engine/mv)
- [ ] **Step 1:** 把 `optimize_with_root_property`/`optimize`/`optimize_with_root_distribution` 的入参从 `plan: LogicalPlanNode` 改为 `plan_expr: OptExpr` + `arena: ScalarArena`(或 `Rc<RefCell<ScalarArena>>`)。删掉函数内的 `ScalarArena::new()` + `logical_plan_to_opt_expr(&plan,…)`——改用传入的 `plan_expr`/`arena`。其余(rewrite ctx 装 arena、unwrap 进 memo)不变。
- [ ] **Step 2:** 每个调用方(`grep` 出的 engine/mv 入口)在调 `optimize` 前做:`let mut arena = ScalarArena::new(); let opt_expr = convert::logical_plan_to_opt_expr(&plan, &mut arena);` 再 `optimize(opt_expr, arena, …)`。**Bridge 1 现在在 optimizer 之外**(= "归位 planner/caller")。
- [ ] **Step 3:** `cargo build --lib`(修调用方编译)+ `cargo test --lib`(全绿)+ commit `refactor(optimizer): A4 — optimize() takes OptExpr; Bridge 1 moved to callers (TypedExpr out of optimizer entry)`。

## Task 5:验收 + 收口校验 + PR
- [ ] `cargo fmt && cargo clippy --lib`(无 error);`cargo test --lib`(≥ 基线)。
- [ ] **"TypedExpr 退出优化器"校验**:`grep -rn 'TypedExpr\|materialize\|LogicalPlanNode' src/sql/optimizer/ | grep -vE '#\[cfg\(test\)\]|mod tests|scalar_bridge|opt_expr_to_logical_plan|//' ` —— 生产代码里应只剩"正当出口物化"(runtime_filter/codegen 边界,Task 3 判定保留者)+ Bridge 1/copy-in 的 convert(它们桥接 planner 类型,正当)。理想:优化器内部逻辑/估计器零 `TypedExpr`/`materialize`。
- [ ] optimizer golden(起 server)+ TPC-DS SF1 99/99(dev-opt -j1)→ 逐字节/全绿(A4+入口签收均行为保持)。
- [ ] push fork + PR(`--repo NovaRocks/NovaRocks --base main --head HangyuanLiu:claude/arc-a-finish`),title `refactor(optimizer): Arc A finish — A4 estimators ScalarId-native + optimize() takes OptExpr`,body 说明两块 + golden/TPC-DS 结果 + "Arc A(优化器封装)闭环"。

---

## Self-Review
**Spec coverage:** §6 A4(估计器去 TypedExpr)= Task 2/3;入口签收(§6 A3 行所述、2026-06-19 A3 plan 未含)= Task 4;两块合起来达成 G1/G4「TypedExpr 退出优化器」。✓
**Placeholder:** recipe 沿用 A2 已验证模式;materialize 站点需现查(Task 1)——非含糊,是"先列清单"。runtime_filter 的"出口物化是否保留"作 Task 3 显式判断。
**类型一致性:** `optimize(OptExpr, arena,…)` 新签名贯穿三入口 + 所有调用方;`collect_scalar_column_ids` 复用 A2 column_pruning 的(若存在)。
**行为保持:** A4 只换表达式检视载体、估计值不变;入口签收只移 Bridge 1 位置(逐字节)。golden+TPC-DS 守门。
**风险:** ① 入口签收的调用方 fan-out(engine/mv 多入口)——逐个改,编译器引导;② runtime_filter 的出口物化别误删(判定后保留+注释);③ stats selectivity 的 ScalarNode 检视要和旧 ExprKind 路径逐一对应,避免估计漂移(golden 兜)。

## Execution Handoff
串行受控:Task 2(stats,最大)→ Task 3(logical_props/runtime_filter)→ Task 4(入口签收,改调用方)→ Task 5(门+PR)。完成后 **Arc A 真正闭环**;Arc B 见另一份 plan,可并行。
