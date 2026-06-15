# 设计：In-Memo 多候选 Join Reorder（对齐 StarRocks）

日期：2026-06-15
状态：设计（用于驱动实现；不做短期止血）
范围：standalone optimizer `src/sql/optimizer/**`
参照：StarRocks `fe/fe-core/.../sql/optimizer/rule/join/**`、`Memo.java`、`QueryOptimizer.java`
起因：`docs/design/specs/2026-06-13-starrocks-fe-benchmark-plan-gap.md`（P0 基数问题）及下文两个延伸发现

本设计基于对两套代码库的彻底通读，并经两路对抗式评审（NovaRocks 可行性 + StarRocks 忠实度）加固。带 **【经评审修正】** 标记的小节记录了初版设计错在哪里、为什么。

---

## 0. 背景：我们怎么走到这一步

1. P0 基数爆炸（tpc-ds q4/q11/q31/q74，约 1e14）根因定位为 `CTEConsume` 丢弃了 producer 的列统计，已在 memo 估计器（`stats.rs`）修复，并通过真实 TPC-DS SF1 的 EXPLAIN A/B 验证（1e14 → ~2.6M，与 StarRocks FE 量级精确吻合）。PR #315。

2. 审计同类问题时发现存在 **两套独立的基数估计器**：
   - `src/sql/optimizer/stats.rs` —— memo 估计器（遍历 `MExpr`，通过 `memo.cte_produce_groups` 解析 CTE producer）。修复落在这里。
   - `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs` —— 第二套 `LogicalPlan` 估计器，仅供 RBO join-reorder pass 使用。它仍有 CTEConsume 盲点（硬编码 `1000.0`、无 NDV），外加 AggregateStateMerge/Values/GenerateSeries/TableFunction 等盲点。

3. 追究 *为什么有两套*：NovaRocks 把 join reorder 做成了 **单结果 RBO 预处理**（`JoinReorderRule` → `reorder_joins_cbo` 返回单棵 `LogicalPlan`），用它作为 memo 的种子；Cascades 之后只做有限的 `JoinCommutativity`（build/probe 交换）和受限的 `JoinAssociativity`（memo 超过 200 个 group 就跳过）。由于 reorder 在 memo 建立之前运行，它无法使用 memo 估计器 —— 于是有了第二套估计器。又因为它一次性 commit 单个顺序，join 顺序质量完全取决于那套（更糙的）估计器，而 Cascades 无法挽回。

4. 对照 StarRocks 核实：**StarRocks 并不挑单个顺序。** 它的 `ReorderJoinRule` 枚举 **多种算法 × Top-K 候选**，并把它们 **全部注入 memo**（`Memo.copyIn`）作为备选，由唯一的成本搜索择优。这就是我们要对齐的模型。

**把两件事串起来的结论：** 把 join reorder *搬进 memo* 做成多候选生产者，可以同时 (a) 删掉第二套估计器（候选由唯一的 memo 估计器计算成本），(b) 让 join 顺序成为多候选成本择优、而非早期单点 commit。两个问题，同一根因。

---

## 1. 目标与非目标

### 目标

把单结果的 RBO join-reorder 预处理替换为 **忠实于 StarRocks 的 in-memo 多候选** 生产者：枚举多个候选顺序（LeftDeep 总跑；DP 和 Greedy-TopK 受 cap 限制），把它们 **全部** 作为逻辑等价备选注入 join 的 memo group，由现有的 memo 成本搜索（`search::optimize_group`）在完整的分布/exchange 感知下择优。删除第二套估计器。

### 非目标（明确**不做止血**——遵用户指示）

- **不**走「让 RBO 预处理 reorder 改用 `derive_logical_plan_statistics`」这种保留单点 commit 的去重式止血。我们把 reorder 搬进 memo。
- **不**保留那套独立的 `LogicalPlan` DP 引擎并行存在。其枚举 *核心* 会被复用，但重新表达到 `GroupId`/`JoinTree` 上。
- **不**加 FE 侧开关/guard（项目原则：在 BE 修能力）。仅改 standalone。
- **不**写双格式/兼容 shim（NovaRocks 无历史用户）。cutover 后旧 RBO reorder 路径直接删除，不做长期共存的 feature flag。
- **范围 = 仅 inner/cross 链**，对齐 StarRocks（`MultiJoinNode.flattenJoinNode` 遇到非 inner/cross 即停）。outer/semi/anti join 作为不透明的 atom 边界。

---

## 2. 关键架构决策 【经评审修正】

### 初版设计（已否决）：注册进 `explore()` 的变换规则

初版把 `MultiJoinReorder` 做成 `all_transformation_rules()` 里的 `Rule`，在 `explore()` fixpoint 循环里「和 JoinAssociativity 同位置」触发。两路对抗评审指出它既 **不忠实** 又 **有危害**：

- **不忠实（D1）：** StarRocks `ReorderJoinRule` **不是** 被调度的规则。`TF_MULTI_JOIN_ORDER` 从未加入任何 `RuleSet`；它在 `QueryOptimizer.java:971` 被命令式调用一次（`new ReorderJoinRule().transform(tree, context)`），时点在 memo 建立之后、成本搜索调度器之前。它一次性把候选预填进 memo。
- **不收敛（P2）：** NovaRocks `explore()` 每轮重扫所有 group（`mod.rs:230-289`）。注册式 reorder 规则会每轮对自己注入的 join 再次触发、不断造新 group、永远到不了 explore 去重，直到 `EXPLORE_MAX_GROUPS=5000` 静默截断（`mod.rs:282-284`）—— 得到一个非确定、只探索了一部分的 memo。为避免这点需要 `already_reordered` 标记 + 去重索引，纯粹是为了驯服一个自找的 fixpoint。

### 采纳设计：一次性 in-memo 预填充 pass（忠实于 StarRocks）

`MultiJoinReorder` 是一个 **命令式调用一次的 pass**，在 `optimize()`（`src/sql/optimizer/mod.rs`）里、紧接 `derive_group_statistics`（`mod.rs:130`，保证 atom group 已有统计）之后、`explore()`（`mod.rs:143`）之前调用。它遍历刚转换出的 memo，找到 inner/cross join 链的根，把每条链按 `GroupId` 拍平，枚举候选顺序，对每个候选调 `copy_in` 注入链根 group 作为备选。之后 `explore()`/`implement()`/`search()` 在全部候选上正常推进。

这个单一决策按构造消除：
- P2（重复触发/不收敛）—— pass 只跑一次，无 fixpoint。
- P3（双重去重一致性）—— 只需 pass 内去重索引。
- P5（与 JoinAssociativity 的 fixpoint 互动）—— 见下文 D2 硬分支。
- D1（忠实度）—— 这 *就是* StarRocks 的结构。
- G8（`already_reordered` 标记）—— 不需要；删除。

它仍需要 P1（建组即 stamp 统计 —— 见 §3 G3），这点无论 pass 还是 rule 都是强制的。

---

## 3. Gap 分析（要建什么）

每条：现状 / 缺什么 / 建什么 / file:line。带 **【经评审修正】** 的反映对抗评审结论。

### G1 — `copy_in` 原语（递归物化子树）
- 现状：`memo.new_group(MExpr)`（`memo.rs:51`）、`memo.add_expr_to_group`（`memo.rs:70`）；`JoinAssociativity` 开手写了单层中间组注入（`join_associativity.rs:136-145`）。
- 缺：递归、自底向上、带去重的物化器 ≈ `Memo.copyIn`（`Memo.java:134-161`）。
- 建：`memo.copy_in_join_tree(tree: &JoinTree) -> GroupId`。叶子 → 已有 GroupId。join → 先递归子节点（自底向上；G5 不变量），经 pass 内索引（G2）去重，否则 `new_group` + **立即** `derive_group_statistics_for`（G3，强制）。**【经评审修正 P6】** 它物化候选根的 **严格后代**，返回根的两个子 GroupId + 根 `LogicalJoinOp`；*根本身* 由调用方作为备选 `NewExpr` 加入链根 group（对齐 `JoinAssociativity`：`new_group` 内层、返回外层）。它**不**把顶层物化进自己的 group（否则会重复创建）。

### G2 — pass 内去重索引
- 现状：explore 循环用 `existing.children == new.children && op_equal` 去重，其中 `op_equal` 是 `format!("{:?}")`（`mod.rs:264-267`、`:353-355`）；`find_existing_logical_group`（`split_aggregate.rs:174-182`）。
- 缺：让不同算法产生的、共享中间子 join 的候选复用同一 GroupId 的哈希索引（≈ StarRocks `groupExpressions`，`Memo.java:99-121`）。
- 建：`HashMap<(OpKey, Vec<GroupId>), GroupId>`，供 `copy_in_join_tree` 查询。`OpKey` = 结构化键（join 类型 + 规范化的等值键列集合），**不是** `Debug` 串。**【经评审修正：定位】** 因为 pass 只跑一次，这是用于候选集间共享中间组的 pass 内索引 —— *不是* 为终止性服务的正确性装置（那个顾虑随 fixpoint 规则一起消失了）。仍值得做以节省 memo 体积。

### G3 — 每个新组的统计 stamp —— **强制【经评审修正 P1】**
- 现状：`derive_group_statistics` 只在 `mod.rs:130` 和 `:152` 跑；假定子组 index < 父组 index（`stats.rs:668-673`）；子组 `logical_props` 为 `None` 时回落到 `Fallback` 的 10k 行默认值（`stats.rs:733`）。
- 缺：建组时即 stamp 统计。
- **为什么强制（不是可选）：** `implement()` 在 `mod.rs:149` 运行，*早于* `mod.rs:152` 的重导出。`JoinToHashJoin::apply` 读 `get_group_column_ids(memo, child)`（`implement.rs:576`），子组 `logical_props` 为 `None` 时返回 **空集**（`implement.rs:18-30`）→ `orient_eq_pair` 在每个等值键上失败 → `JoinToHashJoin` 返回 `vec![]`，由 `JoinToNestLoop` 接管。**不做每组 stamp，所有多层（bushy）候选都会被静默实现成 NestLoop join —— 整个规则失去意义。**
- 建：`stats::derive_group_statistics_for(memo, group_id, table_stats)`，在 `copy_in_join_tree` 里 `new_group` 之后**立即**对**每个**新中间组调用。这是 StarRocks `Memo.java:154-158` 的对应物。

### G4 — Greedy Top-K
- 现状：`greedy_join_reorder` 返回单个最优计划（`reorder.rs:1033`）。DP/LeftDeep 同。
- 缺：StarRocks Greedy 用有界 `MinMaxPriorityQueue` 维护成本最低的 10 个全连接表达式并按最低成本顺序取出（`JoinReorderGreedy.java:36-79`、`:174-190`）。这是多候选的核心。
- 建：greedy 核心的 full-mask cell 累积有界 Top-K（`cbo_max_reorder_topk`，默认 10）；返回 `Vec<JoinTree>`。LeftDeep/DP 返回 `vec![best]`。

### G5 — 把枚举核重新表达到 `GroupId`/`JoinTree`
- 现状：`dp_join_reorder`（`reorder.rs:640`）、`greedy_join_reorder`（`:868`）、`left_deep_join_reorder`（`:1049`）基于 `u32` mask + `DpEntry { plan: LogicalPlan, ... }`；纯助手 `find_connecting_predicates`（`:813`）、`has_equijoin_predicate`（`:53`）、OR 因式分解（`:1262`）、`SubsetIter`（`:1157`）—— 可原样复用。
- 缺：memo 版本（叶子是 `GroupId`；统计来自 `logical_props`）。
- 建：`enum JoinTree { Leaf(GroupId), Join { left, right, op: LogicalJoinOp } }`。`DpEntry`/greedy cell 携带 `JoinTree` + `Statistics`。每候选 join 统计通过 `estimate::cardinality::estimate_join_cardinality(JoinCardInput{..})` 基于两个子树缓存的 `Statistics` 计算 —— 与 `stats.rs` 同一内核。叶子统计 = `memo.groups[gid].logical_props`。mask cap ≤62（DP），与 StarRocks long-mask 一致。**【经评审修正 D7】** 端口里保留 LeftDeep 的同表自连接规避 + 等值优先启发式（`JoinReorderLeftDeep.java:50-86`）。**【经评审修正 D6】** 端口饱和加/乘（针对 `MAX_REORDER_COST` 上限，≈ `JoinOrder.saturatingAdd/Mul`），使 DP 分支限界在 cross 链上不会溢出到 inf/NaN 而破坏比较器。

### G6 — 在子组上拍平 join 链
- 现状：`flatten_inner_joins` / `extract_join_graph`（`reorder.rs:463-635`）作用于 `LogicalPlan`；吸收顶层 Filter；下推单关系谓词；按 popcount 分类谓词。
- 缺：作用于 `MExpr.children` 的 memo 侧拍平器（窥视子组 `logical_exprs[0]`，如 `JoinAssociativity` 在 `join_associativity.rs:62-83` 所做）。
- 建：`flatten_join_chain(memo, root_expr) -> MultiJoinGraph { atoms: Vec<GroupId>, predicates: Vec<(TypedExpr, u64 mask)> }`。atom = 不是 inner/cross join 的子组。每关系列集合用 `get_group_column_ids(memo, gid)`（`implement.rs:18`）。单边谓词 → 用 `new_group(LogicalFilter)` 包裹 atom。**【经评审修正 D3/D4】** NovaRocks 的 join **不带 projection**，拍平器把任何 `LogicalProject` 当不透明 atom（`reorder.rs:631-633`），所以 reorder 不可能丢任何派生列。因此：**不 port `expr_map`/`expressionMap`（删掉），也不要 `checkDependsPredicate` guard（G7 删除）** —— 这两者在 StarRocks 里只为处理 projection-flatten 的 hazard，而 NovaRocks 结构上不可能有。这也解决了初版「既 port `expr_map` 又说不需要列恢复」的自相矛盾。加一条单元不变量测试：拍平器永不下穿 `LogicalProject`。

### G7 — *（删除【经评审修正 D4】）* `checkDependsPredicate` guard
NovaRocks 无 `expressionMap` 且不穿过 projection，链式派生列 hazard 不可能出现。无需 guard。

### G8 — *（删除【经评审修正 D1】）* `already_reordered` 标记
仅 fixpoint 规则才需要。一次性 pass 不重复触发。删除。`LogicalJoinOp` 不变；`join_reorder_global_applied` RewriteContext 标记随 RBO 路径一起删除。

### G9 — 爆炸上限
- 建：pass 每条链注入有界候选集（`1 LeftDeep + 1 DP + K Greedy`），受 StarRocks 对齐的 cap 限制（DP ≤ `cbo_max_reorder_node_use_dp`/62；Greedy ≤ `cbo_max_reorder_node_use_greedy`；主 cap `cbo_max_reorder_node`）。超主 cap 则跳过该链（退化为 LeftDeep-only）。**【经评审修正 P5】** Phase 7 必须把「12–16 表 join 不触发 `EXPLORE_MAX_GROUPS` 截断」作为门禁。

### G10 — 两套估计器（统一）—— 见 §4。

---

## 4. 估计器统一

### 4.1 为什么 in-memo reorder 能删第二套估计器
一旦 reorder 在 `derive_group_statistics`（`mod.rs:130`）之后运行，每个 atom group 已有 `logical_props.{row_count, column_statistics}`。每候选 join 基数就是对缓存的子统计直接调 `estimate_join_cardinality` —— 与 `stats.rs` 同一内核。不再有任何调用方需要为 reorder 从头遍历 `LogicalPlan`。

### 4.2 删什么 / 留什么
- **删：** `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs`（第二套估计器）及其 5 个调用点（`reorder.rs:654,785,889,986,1062`）。
- **留：** `derive_logical_plan_statistics`（`stats.rs:603`）—— 它建一个临时 memo 并走 *memo* 估计器；它是 *桥接到* 被保留估计器的桥，供 aggregate-pushdown（仍是 pre-memo）使用。它不是第二套估计器。
- **原样保留：** 共享内核 `estimate/{cardinality,selectivity,ndv,join_condition}.rs`。

### 4.3 在 `stats.rs` 里一次性补齐各算子盲点
删掉那套有损遍历会自动修复 reorder 时的偏差；但下列盲点在 memo 估计器里也有，必须在 `stats.rs` 补齐，使唯一的估计器对所有调用方都正确：

| 算子 | 当前 `stats.rs` | 修复 |
|---|---|---|
| CTEConsume | 已正确（`stats.rs:78-106/:501-527`） | 无（删第二套即修复） |
| AggregateStateMerge | 行数相加、列统计为空（`:240-249/:582-591`） | 按 output_columns 位置合并 old/delta 子列统计 |
| Values | 行数精确、列统计为空（`:65-69/:562-566`） | 从字面量行合成精确 NDV/min/max |
| GenerateSeries | 行数精确、列统计为空（`:70-74/:568-572`） | 合成精确 NDV(=行数) + min/max |
| TableFunction | 行数 ×3、列统计为空（`:741-753`） | 从子列统计起步透传；仅 TF 生成列 unknown |

> 注：本节即 **Phase 1**，已实现并合入本分支（见 §7 Phase 1）。

---

## 5. 退役 / 改动了什么

| 组件 | 处置 |
|---|---|
| `JoinReorderRule`（`join_reorder/rule.rs`）+ `join_reorder_global_applied` | 退役；从 `rewrite/registry.rs` 移除。 |
| `reorder_joins_cbo` 树驱动、`extract_join_graph`/`flatten_inner_joins`、`reorder_joins_heuristic`、`estimate_size` | 退役（memo 永远有统计；heuristic 过时）。 |
| `reorder.rs` mask 核 + 助手、`cost.rs` join 成本算术 | 移动并改造进 `cascades_rules/multi_join_reorder/`，作用于 `JoinTree`；Greedy → Top-K；饱和成本。 |
| `join_reorder/cardinality.rs` | 删除（§4）。 |
| `JoinCommutativity` | 保留，不变（memo 内 build/probe 交换；StarRocks 同样保留 `addJoinCommutativityWithoutInnerRule`）。 |
| `JoinAssociativity` | **【经评审修正 D2】** 硬分支、不共存：atom 数 > `cbo_max_reorder_node_use_exhaustive`(4) 的链由 `MultiJoinReorder` 处理且对它们 **禁用** inner-associativity；≤4 的链交给 `JoinAssociativity` 穷举、reorder pass 跳过。对齐 StarRocks `QueryOptimizer.java:967-981` / `RuleSet.java:481-494`（互斥，而非软性的 200-group 阈值）。 |
| 新 session 开关（`options.rs`） | `cbo_enable_dp_join_reorder`(t)、`cbo_enable_greedy_join_reorder`(t)、`cbo_max_reorder_node`(50)、`cbo_max_reorder_node_use_exhaustive`(4)、`cbo_max_reorder_node_use_dp`(10, cap 62)、`cbo_max_reorder_node_use_greedy`(16)、`cbo_max_reorder_topk`(10)。经 `SessionOptimizerSettings`→`OptimizerOptions::from_session` 穿线。可用 `SET disable_optimizer_rules='MultiJoinReorder'` 关闭。 |

---

## 6. 正确性硬点（来自对抗评审）

均为硬性要求，各带测试义务：
- **M1（P1）：** 每个新中间组在创建时即 `derive_group_statistics_for`，早于 `implement()`。测试：构造 ≥3 层 bushy `JoinTree`，过 implement，断言每层都活下来的是 HashJoin（不是 NestLoop）。
- **M2（G5 不变量 / D5）：** `copy_in_join_tree` 严格自底向上分配；`debug_assert` 子 GroupId < 父 GroupId，使 index 序重导出（`stats.rs:668-673`）仍有效、不漏 `Fallback` 10k 行（`stats.rs:733`）。
- **M3（D2）：** reorder 与 inner-associativity 按链互斥（按 atom 数 vs 穷举阈值硬分支）。测试：6 表 inner 链由 `MultiJoinReorder` 重排，`JoinAssociativity` 对它产出 0 个备选。
- **M4（D3）：** 拍平器永不下穿 `LogicalProject`；不透明 atom 测试。
- **M5（D6）：** 枚举成本饱和；cross 链测试断言剪枝比较器无 inf/NaN。
- **M6（搜索桥接，P4）：** 注入的逻辑备选经 `implement()` 变物理，由 `search::optimize_group` 比较（只对物理表达式计成本，`search.rs:111-125`）。由 M1 + Phase-5 A/B 传递性覆盖。

---

## 7. 分阶段实现计划（TDD；引擎全程绿）

pass 直到 Phase 5 才接进 `optimize()`。在此之前旧路径继续运行。

- **Phase 0 — 基线（无代码）。** 记录 q4/q11/q31/q74 + inner-join 密集集（ssb、tpc-h q5/q8、`sql-tests/optimizer/`）在当前路径下的 golden EXPLAIN。这是后续每次 A/B 的 A 侧。用 `-- @explain_contains` / `--mode record`。
- **Phase 1 — `stats.rs` 估计器盲点修复**（§4.3；AggregateStateMerge/Values/GenerateSeries/TableFunction）。逐算子单测。全套绿；对这些算子喂入 join 的计划记录 golden。**【已完成】** 见提交 `bc12c4f5`：四算子全部 TDD（RED→GREEN），全 crate lib 单测 4938 passed / 0 failed。
- **Phase 2 — memo 原语**（G1+G2+G3）：`JoinTree`、`copy_in_join_tree`、pass 内索引、`derive_group_statistics_for`。测试：3 叶树创建预期组、复用叶子 GroupId、自底向上不变量（M2）、每组统计已 stamp（M1 单元）。原语暂不投产。
- **Phase 3 — 移植枚举核**（G4+G5+G6）：`multi_join_reorder/{algo,flatten}.rs` 作用于 `JoinTree`；Greedy Top-K；饱和成本（M5）；LeftDeep 启发式（D7）；带「不穿过 projection」不变量的拍平器（M4）。纯函数 + golden-tree 单测。
- **Phase 4 — 一次性 pass**（组装）：`run_multi_join_reorder(memo, opts, table_stats)` —— 遍历 memo、找 > 穷举阈值的 inner/cross 链根、拍平、unknown-stats→LeftDeep-only 退化、选算法、枚举、`copy_in` 每个、加根备选。session 开关进 `OptimizerOptions`。已建但 `optimize()` 里**不调用**。测试：手构 memo → pass 加 N 个备选；bushy implement→HashJoin（M1 集成）；cap/退化/关开关行为。
- **Phase 5 — cutover + A/B。** 在 `optimize()` 中 `mod.rs:130` 之后调用 pass；与 `JoinAssociativity` 套用 D2 硬分支（M3）。本阶段旧 RBO 路径仍注册（结果正确性双覆盖；`SET disable_optimizer_rules='MultiJoinReorder'` 可即时回退）。完整 A/B：q4/q11/q31/q74 + ssb/tpc-h/tpc-ds plan golden + `sql-tests/optimizer/`。预期 match-or-improve；记录前一路径看不到的分布感知改进。
- **Phase 6 — 退役 RBO + 删第二套估计器。** 移除 `JoinReorderRule`、删 `cardinality.rs`、删退役的 `reorder.rs`/`cost.rs` 的 LogicalPlan 派发、删 `join_reorder_global_applied`。确认 aggregate-pushdown 桥接完好。全套绿且只剩 in-memo 路径；`cargo clippy` 死代码干净。
- **Phase 7 — 性能加固。** 校验 in-memo 枚举成本；调 Top-K/cap vs `EXPLORE_MAX_GROUPS`/timeout；**门禁：12–16 表 join 不触发截断（M3/P5）**；ssb/tpc-h release-build 规划时延基准。

---

## 8. 剩余风险与未决问题

- **R1（D5 分歧，有意为之）：** StarRocks `Memo.copyIn` seed-and-trust reorder 时的统计（`Memo.java:158`）；我们用规范内核重导出（`derive_group_statistics_for`）。这更安全（proxy/规范无分歧）但是对 StarRocks 的有意分歧 —— 已记录，靠 M2 自底向上不变量保证。
- **R2（输出列，评审 §7.3 / D3）：** 确认无需单独 prune pass（join 无 projection；每组 `output_columns` 重导出）。Phase-5 宽表 A/B（q11/q74）验证不同顺序下中间 join 被投影到所需列。
- **R3（分布感知导致的计划变化）：** 搬进 memo 意味着搜索按 exchange 感知给每个顺序计成本，这是 pre-memo 路径没有的 —— Phase-5 A/B 会出现 *有意* 的变化；按改进而非回归来 review。确认 `JoinToHashJoin`/`JoinToNestLoop` + property enforcement 对每个注入顺序产出两种 build 侧朝向（`JoinCommutativity` 提供交换）。
- **R4（迭代预算）：** 新组在 `explore()` 之前创建（一次性），所以从第一轮 explore 起、全部 `EXPLORE_MAX_ITERATIONS=16` 轮都在场 —— 严格优于被否决的 fixpoint 设计。Phase 7 测实际轮数。

---

## 附录：权威 file:line 索引

NovaRocks：`rule.rs:20-40`；`memo.rs:51-86`；`mod.rs:127/130/143/149/152/224/255/282`；
`cascades_rules/mod.rs:45-61`；`join_associativity.rs:43/62-83/136`；`split_aggregate.rs:174-182`；
`operator.rs:162-165`；`options.rs:60-146`；`join_reorder/rule.rs:46-57`；
`join_reorder/reorder.rs:307/463/640/868/1049（+stats 调用 654,785,889,986,1062）`；
`join_reorder/cardinality.rs（删除）`；`stats.rs:57/603/668-673/674/733/1336`；
`implement.rs:18-30/576-577/606-608/653-666`；`search.rs:87-268（成本 111-125）`；`cost.rs`。

StarRocks：`ReorderJoinRule.java:103-180/243-281（+OutputColumnsPrune 288-409、退化 260-263）`；
`Memo.java:99-121/134-161`；`JoinReorderGreedy.java:36-79/174-190`；
`JoinOrder.java:243-262/285-311/488-502/546-563`；`JoinReorderLeftDeep.java:35-108`；
`JoinReorderFactory.java:46-65`；`MultiJoinNode.java:63-132`；`SessionVariable.java:1840-1856`；
`QueryOptimizer.java:967-981/971/1006`；`RuleSet.java:481-494`；`StatisticsEstimateCoefficient.java:59/62`。
