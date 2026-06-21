# 设计：优化器公共子表达式消除（CSE v1，基于 ScalarArena 的 Project 物化模型）

日期：2026-06-21
状态：设计（已评审通过，待写实现计划）
范围：standalone optimizer `src/sql/optimizer/**`、optimizer→codegen 边界、session 选项
参照：本仓 `docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md`（§8 CSE 设计附录、M3）、`src/sql/optimizer/runtime_filter_pass.rs`（同形态 post-CBO 物理树 pass 先例）；StarRocks `rule/tree/exprreuse/ScalarOperatorsReuse.java`、BE `CommonExprEvalScopeGuard`（**仅作 exec 契约与相位/gating 的参照，不作实现模板**）
前置：ScalarArena/ScalarId hash-consing（M0/M1 已落地，`src/sql/optimizer/scalar/mod.rs`）、算子已全面切到 `ScalarId`（`operator.rs`）、project working-chunk 物化（`src/exec/operators/project_processor.rs`）

---

## 0. 指导原则（承重）

**StarRocks 在本块仅作参照，NovaRocks 架构已超越它，检测与改写一律按 ScalarArena 原生表达。**
StarRocks 要自建结构摘要判等（`ScalarOperatorsReuse` 的 OperatorId）、逐算子 shuttle 改写；NovaRocks 因 ScalarArena hash-consing 而「结构相同 ⟺ 同一 `ScalarId`」是天生的——CSE 检测退化成一次 id 频次遍历，nested common 与交换律规范化全部免费。本设计吃这个红利，**不**照搬 StarRocks 的检测/改写算法；只在「公共列如何在执行期被算一次」这一 exec 契约层面，借鉴 StarRocks 已被验证的形态。

---

## 1. 背景

`2026-06-16-optimizer-scalar-expr-ir.md` §8 把 CSE 列为设计附录、显式不实现（M3 design-only），因为当时表示层（ScalarArena）尚未落地。如今前置已成熟：

- **检测基础就绪**：`ScalarArena::intern`（`scalar/mod.rs:208`）保证「结构相同 ⟺ 同一 `ScalarId`」，并对可交换算子做了规范化（`a AND b` 与 `b AND a` intern 成同一 id）。
- **算子已 id 化**：`operator.rs` 全部字段为 `ScalarId`/`Vec<ScalarId>`/`Option<ScalarId>`。
- **物化路径就绪**：`project_processor.rs:250-386` 的 working-chunk 机制——按顺序求值每个投影表达式、把结果作为新 slot 追加进 working chunk、后续表达式按 slot id 读已物化列——本身就是「compute-once」。

本设计实现 §8 预言的 CSE 改写。

---

## 2. 目标与非目标

### 目标
- **G1（计划质量）**：消除一个算子表达式集合内重复的、非平凡的子表达式计算，使其只算一次。
- **G2（架构最小侵入）**：复用唯一已验证的 compute-once 机制（Project working-chunk）；**不新增 exec 机制、不改 thrift、不新增算子**。
- **G3（可观测/可验收）**：CSE 结果以真实 Project 节点 / Project 列出现在物理计划里，`sql-tests/optimizer` plan-golden 可直接断言。

### 非目标（v1）
- **不做 join 跨两侧条件的 CSE**（见 §4 的唯一例外）。罕见，且需把物化逻辑接进 join probe 的 joined-chunk 求值路径——独立后续（§9 v2）。
- **不动 exec 层**（`src/exec/**`）、**不改 codegen 节点构造**、**不改 thrift**。
- **不做 lambda body 内部 / 跨 lambda 的复用**；不向 SubqueryPlaceholder 内部 factor。
- **不引入代价模型**：v1 用「非平凡 + count≥2」的结构性判据，不做基于代价的 factor 门限。

---

## 3. 核心洞察：唯一的运行时物化点是 Project

调研确证（见 §10 证据）：

1. `ExprArena::eval`（`src/exec/expr/mod.rs:202`）是纯递归求值，**无按 ExprId 记忆化**——共享 ExprId 只省内存、不省 CPU。
2. `src/exec/operators/` 下**只有 Project**（`project_processor.rs`）在运行时把中间结果物化成 chunk 列；`common.heavy_exprs` 与非 project 节点的 `common_slot_map` 在 lowering 时只**内联**、运行时不物化。
3. standalone codegen **不发 `SELECT_NODE`**；filter conjunct 被融进「子计划第一个节点」的通用 `conjuncts` 字段（`lower_filter_node`，`lowering.rs:1161-1208`），该节点可为 scan/sort/agg/exchange 等 15+ 种。

**推论**：要让公共子表达式 compute-once，最稳妥且零新增机制的做法，是把它**物化成一个 Project 节点的输出列**，消费方按 `ColumnId`/slot id 引用。这正是「在上游 Project 里算一次、下游按 slot 取数」——对**所有单输入算子**成立。

### 唯一例外：join 跨两侧条件
多输入算子里唯一有「跨输入表达式」的是 **join**。若一个跨左右两侧的子表达式（如 `o.price * l.qty`）在某 join 的非等值条件里出现 ≥2 次，它**无法**由任何子节点的 Project 算出（左孩子缺右列、右孩子缺左列），只能在 join 把左右行拼成联合行后、在 joined chunk 上算。该条件又用于决定哪些联合行通过 join，故也不能放到 join 之上。**因此它是 Project 物化模型唯一覆盖不到的格子，v1 顺延。**

---

## 4. 架构

### 4.1 相位
新增 post-CBO 物理树 pass `cse_pass`，形态完全对齐 `runtime_filter_pass`：

```
optimize() (src/sql/optimizer/mod.rs):
  physical = extract::extract_best(...)                          // :228
  runtime_filter_pass::annotate(&mut physical, &memo.scalars, &options)  // :231
+ cse_pass::rewrite(&mut physical, &mut memo.scalars, &options)  // 新增
  physical_plan::attach_scalar_arena(&mut physical, Arc::new(memo.scalars.clone()))  // :232
```

- **必须在 `attach_scalar_arena` 之前**：CSE 要 `&mut ScalarArena`（mint 新 `ColumnRef`、re-intern 改写后的根）。arena 在此之后才被 `Arc` 冻结交给 codegen。
- 在最终物理树上做，**不进 memo、不参与搜索、不扰动代价**（理由：CSE 是「最终计划如何高效求值」的物理后处理，不是「选哪个计划」的关系决策；放进搜索只会用零关系收益吹大搜索空间、并使候选在不一致的表达式基准上被打分）。

### 4.2 检测（ScalarArena 原生）
对每个算子的**根表达式集合** `roots: &[ScalarId]`：

```text
count: HashMap<ScalarId, usize>
对每个 root 自底向上 visit（复用 runtime_filter_pass::column_ids 同款遍历）：
    每遇到一个 sub-id 就 count[id] += 1
candidates = { id | count[id] ≥ 2 且 eligible(id) }
```

- **nested common 自动正确**：内层公共子表达式的 count 天然 ≥ 外层，二者都会入选并各自 mint 列，引用关系靠 id 结构成立——无需 StarRocks 的 common-inside-common 特判。
- **交换律免费**：intern 时已规范化。
- **`eligible(id)` 排除**：
  - 叶子 `ColumnRef`、`Literal`（复用无意义）；
  - 裸 `Cast(ColumnRef)`（廉价，物化反增列）；
  - **volatile / 非确定性函数**（`rand` 族）——一次复用 vs 两次独立求值是语义差异，**必须排除**；v1 保守地排除全部非确定性函数；
  - 不跨 lambda body factor（lambda 参数为局部作用域）；`SubqueryPlaceholder` 视为不透明。

### 4.3 改写：统一「插/复用 Project」

对入选的每个公共 `ScalarId c`：mint 新 `ColumnId k`（类型/nullable 取 `arena.data_type(c)`/`arena.nullable(c)`）；令某 Project 输出 `k := c`；`rebuild` 消费方根表达式，把出现的 `c` 替换为 `arena.intern(ColumnRef(k))`，未改子树返回原 id（O(改动路径)，对齐 scalar-expr-ir spec §5 的 `rebuild` 辅助）。

公共列的承载 Project 按算子类型确定：

| 算子 | 承载 Project | 说明 |
|---|---|---|
| 投影列表 | **本 Project** | 公共项作为靠前 item 算出，后续 item 改引用其 `ColumnId`；working-chunk 现成支持顺序求值+引用 |
| Filter / Aggregate / Sort / Window | **下方 Project** | 孩子已是 Project 则复用（追加输出列），否则在算子与孩子之间插入一个新 Project；本算子表达式改引用 |
| join 条件的**单侧**子表达式 | 推到对应那一侧孩子，按上一行处理 | |
| join 条件的**跨两侧**子表达式 | —（v1 不做） | 见 §3 例外、§9 v2 |

- **插的 Project 紧贴消费方**，无需跨层透传；输出列 = 孩子输出列 + 新公共列。
- **插入的 Project 属性安全**：Project 分布保持、序保持，故 `output_property` = 孩子的，`stats` = 孩子的，`output_columns` = 孩子 + 新列。
- **优先复用相邻已有 Project**，避免 Project 增殖。

### 4.4 pass 性质
本 pass 不是纯注解：对投影列表是**就地改写本 Project**，对 Filter/Agg/Sort/Window/join 单侧是**插入或复用 Project**（拓扑改动，但 Project 属性安全）。提供辅助 `insert_or_reuse_project_below(parent_edge, commons) -> ()` 收口插入/复用逻辑。

---

## 5. exec / codegen 影响：≈零

CSE 产出的是**标准 Project 节点 / Project 列**：

- codegen 早已 lower Project 节点（`build_project_node`，`nodes.rs:304`）→ **零 codegen 改动**。
- exec 早已跑 project working-chunk（`project_processor.rs:250-386`）→ **零 exec 改动、无新算子、无新 thrift 字段**。
- 投影列表内 CSE 依赖「working-chunk 顺序求值 + 后项按 slot 读前项」——该行为现成（`project_processor.rs:268-386`）。

这是「Project 物化模型 + v1 不做跨侧 join」的直接红利：v1 基本是**纯优化器改动**，实现风险锁在优化器内部。

---

## 6. Gating

- 注册稳定规则名 `CommonSubexpressionReuse`：加入 `is_known_rule_name`（`mod.rs:258`，与 `runtime_filter_pass::RUNTIME_FILTER_RULE`、`mv_rewrite::RULE_NAME` 并列），使 `SET disable_optimizer_rules='CommonSubexpressionReuse'`（别名 `cbo_disabled_rules`）可关；pass 入口 `options.is_enabled("CommonSubexpressionReuse")` 短路。
- 加 session 变量 `enable_common_subexpr_reuse`（默认 `true`）到 `OptimizerSettings`（`options.rs`），并在 `server/mod.rs` 的 `SET` 处理里接线（对齐 `enable_materialized_view_rewrite` 的现成路径，`server/mod.rs:1025`）。
- 两道开关任一关闭即跳过 pass。

---

## 7. EXPLAIN 与验收

- **可观测**：公共列以真实 Project 节点 / Project item 出现在计划中，Verbose/Costs 现成渲染即可断言；plan-golden 比 side-map 更直观。可选：在 EXPLAIN 给 CSE 产生的 Project/列加一个稳定标记，便于 golden 审阅（非必需）。
- **验收标准**：
  1. 全 lib 单测绿；
  2. `sql-tests/optimizer` golden：会新增 Project 节点 / 列——**有意重录并人工审**（diff 应只表现为「多了 compute-once 列 / Project」）；
  3. TPC-DS SF1 99 query、SSB、TPC-H verify **结果不变**；
  4. 定向用例：含重复昂贵子表达式的 query 结果与关闭 CSE 时逐字节一致；**volatile 函数（rand）不被 factor** 的正确性用例；
  5. 用 `-- @explain_contains=<substr>` 在 sql-test case 里断言 CSE 计划形状。

---

## 8. 风险与唯一须验证项

- **【须验证，低风险】插的 Project 上 fuse 的 conjunct 必须对 Project 的输出 working-chunk 求值（而非输入）**。该语义由现有「Filter-over-Project」计划形状的正确性背书——该形状今天就走「filter conjunct 融进下方 Project 节点」这条路且测试通过，故新插的 Project 与现状同路。实现首步显式验证并加回归用例。
- **Project 增殖**：靠「优先复用相邻已有 Project」缓解；仅在 CSE 实际触发时插入。
- **plan golden 漂移**：批量重录，人工审「只多了 compute-once 列/Project」。
- **canonical 不变式承重**：检测依赖「结构相同 ⟺ 同一 id」；改写产物（`ColumnRef(k)`、rebuild 后的根）必须全部经 `intern`，否则 id 判等失效。沿用 scalar-expr-ir spec §9 的不变式纪律。
- **类型/nullable 一致性**：mint 的 `k` 类型/nullable 必须取自 `c`，rebuild 后整树类型不变。

---

## 9. 分阶段

- **v1（本设计）**：Project 物化模型，零新增 exec。覆盖投影列表 / Filter / Aggregate / Sort / Window / join 单侧条件。
- **v2（独立后续，非本范围）**：join 跨两侧条件 CSE。把 `project_processor` 的物化逻辑抽成共享 helper `materialize_common_slots(arena, &[(slot,expr)], &mut chunk)`（map 显示需处理 append + chunk schema 重建，非平凡但有界），接进 hash/nestloop join probe 的 joined-chunk 求值路径；公共列经现成的 `hash_join_node.common_slot_map` / `nestloop_join_node.common_slot_map`（`nodes.rs:397` 等，目前传 `None`）承载。需 join 算子级验证。

---

## 10. 证据（调研锚点）

- 唯一运行时物化点：`src/exec/operators/project_processor.rs:250-386`；`ExprArena::eval` 无记忆化：`src/exec/expr/mod.rs:202`。
- filter conjunct 融进首节点通用 `conjuncts`、不发 SELECT_NODE：`src/sql/codegen/ir/lowering.rs:1161-1208`。
- 全局公共槽收集仅在 lowering 期：`src/lower/node/mod.rs:154-198`；`heavy_exprs` 为 FE-only、运行时不物化。
- 相位先例：`src/sql/optimizer/mod.rs:228/231/232`、`src/sql/optimizer/runtime_filter_pass.rs`（`annotate`、`column_ids` 遍历）。
- 检测基础：`src/sql/optimizer/scalar/mod.rs:208`（`intern` + 交换律规范化）、`:244/248/252`（`node`/`data_type`/`nullable`）。
- gating 先例：`src/sql/optimizer/options.rs`（`is_enabled`/`disable`）、`src/sql/optimizer/mod.rs:258`（`is_known_rule_name`）、`src/server/mod.rs:1025`。

---

## 11. 已定决策（2026-06-21）

1. **首要目标 = 通用计划质量 + 闭环 §8 M3**（不绑特定 workload，plan-golden + 全回归验收）。
2. **机制 = Project 物化模型**（用户洞察）：「在 Project 里算一次、下游按 slot 取」是统一机制，覆盖除 join 跨两侧外的全部。
3. **v1 不做 join 跨两侧条件 CSE**（罕见、需 exec helper），顺延 v2。
4. **StarRocks 仅作 exec 契约 + 相位/gating 参照**，检测/改写按 ScalarArena 原生表达。
5. **eligible 排除**：叶子 / 裸 Cast(col) / volatile 函数（必须）/ 不跨 lambda / SubqueryPlaceholder 不透明。
6. **零新增 exec / codegen / thrift**（v1）。

---

## 12. 执行交接

本 spec 经评审通过后，用 writing-plans 写 bite-sized 实现计划（含 TDD 步骤）。建议首步切片：检测器（id 频次 + eligible）+ 投影列表内 CSE（不插节点，最小端到端，验证 working-chunk 顺序语义）→ 再扩 Filter/Agg/Sort/Window 的 Project 插入/复用 → join 单侧 → gating + EXPLAIN 断言。
