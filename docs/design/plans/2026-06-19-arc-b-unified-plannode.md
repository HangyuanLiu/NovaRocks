# Arc B — planner 侧统一 PlanNode 体系 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development 或 executing-plans。**执行纪律(A2 教训)**:串行、受控、on-branch,逐 stage 提交,控制者每步验证;**绝不并行放 isolated-worktree swarm**。

**Goal:** 把 planner 侧的两套 plan-node 表示合并成**一套** `PlanNodeKind`:`LogicalPlanNodeKind` 与 `DistributedPlanNodeKind` 的直通节点(逐字段重复)去重为**共享 struct**,优化器决策节点(`Join`↔`HashJoin`、`Aggregate`↔`HashAggregate`、`Union/Intersect/Except`↔`SetOp`、`Exchange`)保留为各自变体;**两个 wrapper 都留**(薄 `LogicalPlanNode` / 厚 `DistributedPlanNode`),`DistributedPlanNode` + `build_distributed_plan` 从 codegen 迁入 planner、形式化为 **Bridge 2**;explain 同源渲染。完成后 planner 持单一节点 taxonomy(spec 的核心)。

**依赖:** A2([#339](https://github.com/NovaRocks/NovaRocks/pull/339))已合。**与 Arc A 收尾正交,可并行**(Arc B 是 planner 侧,不碰优化器内部 Operator)。

**Architecture:** 行为保持的结构重构。**关键事实(已实测,A2 前;需在分支上复核)**:直通节点逐字段相同且**两边都用 `TypedExpr`**(`LogicalFilterNode`/`DistributedFilterNode` 都是 `{predicate: TypedExpr}`;Project 完全相同;Scan 近乎相同,distributed +`mv_rewritten_from`)——合并成共享 struct 干净、无标量错配。分歧只在决策节点。分布式元数据(`node_id`/`fragment_id`/`tuple_ids`/runtime_filter)全在 `DistributedPlanNode` **wrapper** 上,per-kind struct 不含——所以"两 wrapper + 共享 kind"成立。**stage 合法性靠 wrapper 约定不靠类型**(Calcite RelNode 同款):`LogicalPlanNode` 实际只持 logical 子集、`DistributedPlanNode` 只持 distributed 子集。逐字节由 plan 等价 harness + golden + TPC-DS 99/99 守门。

**Tech Stack:** Rust;`cargo build`/`test --lib`;sql-test(optimizer golden + tpc-ds);DistributedPlan IR 等价 harness(#318-#330 引入,逐字节比对 thrift/EXPLAIN)。

**非目标:** 不删 `Operator`/`PlanFragment`;不动优化器内部(Operator/memo/rewrite);`build_distributed_plan` 的分片/Exchange 展开/runtime filter 装配**逻辑不变**(只迁位置 + 输出 kind 换统一枚举)。

**关键文件:** `src/sql/planner/plan.rs`(`LogicalPlanNode`/`LogicalPlanNodeKind` + `Logical*Node` 结构);`src/sql/codegen/ir/node.rs`(`DistributedPlanNode`/`DistributedPlanNodeKind`)+ `codegen/ir/kind.rs`(`Distributed*Node` 结构)+ `codegen/ir/build.rs`(`build_distributed_plan`);消费方 analyzer/engine/explain/codegen/优化器 Bridge1(`convert::logical_plan_to_opt_expr`)。

---

## 设计:统一 `PlanNodeKind`(放 planner,~30 变体)

- **共享直通(去重为单一 struct,~13)**:Scan、Filter、Project、Sort、Limit、TopN、Window、Values、Decode、Repeat、GenerateSeries、TableFunction、AssertOneRow。字段并集(如 Scan 取 `mv_rewritten_from`、Aggregate 暂不在此列)。
- **logical-only 变体**:Join、Aggregate、Union、Intersect、Except、CTEAnchor、CTEProduce、CTEConsume、Apply、AggregateStateMerge、ImvDelta、ImvVersion。
- **distributed-only 变体**:HashJoin、NestLoopJoin、HashAggregate、Exchange、SetOp、TopN(若分布式 TopN 与逻辑不同则独立)。
- **两 wrapper**:`LogicalPlanNode { kind: PlanNodeKind, children: Vec<LogicalPlanNode>, required_output_columns }`(薄);`DistributedPlanNode { kind: PlanNodeKind, children: Vec<DistributedPlanNode>, node_id, fragment_id, tuple_ids, nullable_tuple_ids, limit, execution_join_distribution, build/probe_runtime_filters, stats }`(厚)。标量两边都 `TypedExpr`。

---

## Task 1: 分支 + 基线 + 现场复核
- [ ] `git fetch origin && git switch -c claude/arc-b-unified-plannode origin/main`;`cargo build && cargo test --lib`(记基线)。
- [ ] **现场复核字段对照**(A2 后可能微变):逐对比 `plan.rs` 的 `Logical*Node` 与 `kind.rs` 的 `Distributed*Node`,确认直通节点字段一致(脚本:`for op in Scan Filter Project Sort Limit TopN Window Values Decode Repeat GenerateSeries TableFunction AssertOneRow; do echo "== $op =="; done` 后人工 diff)。登记"完全相同/近乎相同(差集字段)/分歧"三类。

## Task 2(B1):定义统一 `PlanNodeKind` + 共享直通 struct(planner)
**Files:** `src/sql/planner/plan.rs`(新增 `PlanNodeKind` + 共享 `*Node` struct)
- [ ] **Step 1:** 在 planner 定义 `enum PlanNodeKind`,含上面三类变体;直通节点用单一共享 struct(沿用现有 `Logical*Node` 的字段,补差集如 `mv_rewritten_from`);决策/logical-only/distributed-only 各自 struct(logical 用现有 `Logical*Node`;distributed 用现有 `Distributed*Node` 的字段)。**先只定义,不接线**(additive,可绿)。
- [ ] **Step 2:** build 绿(新枚举暂未用,`#[allow(dead_code)]` 临时)+ commit `feat(planner): define unified PlanNodeKind (shared pass-through + logical/distributed variants)`。

## Task 3(B2):两 wrapper 改用统一 `PlanNodeKind`
**Files:** `plan.rs`(`LogicalPlanNode.kind: PlanNodeKind`)、`codegen/ir/node.rs`(`DistributedPlanNode.kind: PlanNodeKind`)+ 全部构造/match 站点
- [ ] **Step 1:** `LogicalPlanNode.kind` 从 `LogicalPlanNodeKind` 改 `PlanNodeKind`。编译器驱动:analyzer/planner 的构造站点 `LogicalPlanNodeKind::X` → `PlanNodeKind::X`(logical 变体名保持);match 站点同改。直通节点的构造改用共享 struct。
- [ ] **Step 2:** `DistributedPlanNode.kind` 从 `DistributedPlanNodeKind` 改 `PlanNodeKind`。`build_distributed_plan` + codegen lowering 的构造/ match 站点改 `PlanNodeKind::`(distributed 变体名;直通用共享 struct)。
- [ ] **Step 3:** 删旧 `LogicalPlanNodeKind`/`DistributedPlanNodeKind`(已无引用)。build 绿(编译器逐站点引导)+ 全 lib 单测 + commit `refactor(planner): wrappers use unified PlanNodeKind; drop the two old kind enums`。
- [ ] **注:** 优化器 Bridge 1(`convert::logical_plan_to_opt_expr`)match `LogicalPlanNodeKind::X` → `PlanNodeKind::X`(logical 子集);Operator 侧不变。

## Task 4(B3):`DistributedPlanNode` + `build_distributed_plan` 迁入 planner = Bridge 2
**Files:** 把 `codegen/ir/node.rs`(`DistributedPlanNode`)+ `codegen/ir/build.rs`(`build_distributed_plan`)迁到 `src/sql/planner/`(或共享 plan 模块);更新 codegen 对它们的 import
- [ ] **Step 1:** 物理迁移文件 + 改 `use` 路径(`build_distributed_plan` 逻辑**一行不改**,只换位置 + 它已用统一 `PlanNodeKind`)。形式化命名/注释为 "Bridge 2(`PhysicalPlanNode`/PhysicalOperator → `DistributedPlanNode`,materialize + 分片)"。
- [ ] **Step 2:** build 绿 + 单测 + commit `refactor(planner): move DistributedPlanNode + build_distributed_plan into planner as Bridge 2`。

## Task 5(B4):explain 同源渲染
**Files:** `src/sql/explain.rs`
- [ ] explain 渲染逻辑改为消费统一 `PlanNode`(pre-opt `LogicalPlanNode` 与 post-opt `DistributedPlanNode` 用同一 `PlanNodeKind` 渲染路径,减少两套渲染分支)。行为保持(EXPLAIN 文本不变)。build + golden(explain golden 不变)+ commit。

## Task 6:验收 + PR
- [ ] `cargo fmt && cargo clippy`(无 error);`cargo test --lib`(≥ 基线)。
- [ ] **plan 逐字节等价 harness**(#318-#330 的)+ optimizer golden + TPC-DS 99/99(起 server)→ EXPLAIN/thrift 逐字节不变(B 是纯结构重命名 + 去重,行为零变)。
- [ ] 校验:`grep -rn 'LogicalPlanNodeKind\|DistributedPlanNodeKind' src/`(应为空——两旧枚举已删,统一为 `PlanNodeKind`)。
- [ ] push fork + PR(`--base main --head HangyuanLiu:claude/arc-b-unified-plannode`),title `refactor(planner): Arc B — unify LogicalPlanNodeKind + DistributedPlanNodeKind into one PlanNodeKind`,body 说明:统一 kind + 两 wrapper + DistributedPlanNode 迁入 planner(Bridge 2)+ explain 同源;byte-identical(harness+golden+TPC-DS)。

---

## Self-Review
**Spec coverage(§6 Arc B B1-B4 + §4.1/4.2/4.4):** B1=Task 2(统一 kind + 直通去重)、B2=Task 3(两 wrapper 用之 + 删旧枚举)、B3=Task 4(DistributedPlanNode+build 迁 planner=Bridge 2)、B4=Task 5(explain 同源)。两 wrapper 保留(§4.2)、PlanFragment 不动(非目标)、决策节点保留双变体(§4.1)。✓
**Placeholder:** 字段对照需现场复核(Task 1,A2 后可能微变)——非含糊,是"先 diff 登记";其余编译器驱动 + 变体清单具体。
**类型一致性:** 统一 `PlanNodeKind` 变体名 = logical 用 `LogicalX`-去前缀或保留(实施时定一套命名,全程一致);两 wrapper 同 kind 不同 wrapper 字段;Bridge 1(优化器)match 跟着改 logical 子集。
**行为保持:** 纯结构重命名 + struct 去重 + 文件迁移,无逻辑变更;EXPLAIN/thrift 逐字节(harness+golden+TPC-DS)守门。
**风险:** ① 统一 enum ~30 变体 + stage 合法性靠约定(非类型)——wrapper 边界 + debug 校验兜;② B2/B3 触及 analyzer/engine/explain/codegen 多处构造/match(编译器驱动,但量大,串行受控);③ 直通 struct 的字段并集(Scan `mv_rewritten_from`、Aggregate `mode`/`is_merge` vs `already_pushed`)——逐字段核对,别丢字段;④ 命名一套到底(避免 logical/distributed 变体名冲突)。

## Execution Handoff
串行受控,逐 Task 提交。Task 2(定义,additive 绿)→ Task 3(切 wrapper + 删旧枚举,最大、编译器驱动)→ Task 4(迁 DistributedPlanNode/build = Bridge 2)→ Task 5(explain)→ Task 6(门+PR)。与 Arc A 收尾可并行(不同分支)。
