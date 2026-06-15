# OQ-13 子项 · Ranking-window 谓词下推（top/rank-per-group → partition-TopN）设计

- 关联 roadmap：Optimizer Plan Quality Roadmap · OQ-13（Subquery decorrelation and analytic rewrite parity）
- 关联设计：`docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md`
- 状态：设计待评审 → 实现计划（writing-plans）
- 日期：2026-06-15

## 1. 背景与目标

OQ-13 的 analytic/window parity 含两个子项：

1. **correlated scalar aggregate → window**：已由 Apply/correlated-subquery 框架（M0–M4，PR #282/#294/#297/#301/#305）交付并默认开启（`ApplyToWindow`/WinMagic）。
2. **top/rank per group → window 化 rewrite**：**本设计要做的子项**。Apply 框架的设计文档 §8.5 已把它单列为「OQ-13 另一子项，不依赖 Apply 入口，但复用 SubqueryRewrite stage、plan golden 与 rule disable 基建」。

本设计的目标：当查询用 `ROW_NUMBER()/RANK()/DENSE_RANK() OVER (PARTITION BY p ORDER BY o)` 配外层 `rank_col <= k`（top-per-group）表达时，让优化器把每分区的输入提前裁剪到「rank ≤ K 的超集」，从而减少进入 analytic 算子的行数、提升 plan 紧凑度与执行效率，**且查询结果逐行不变**。

对标 StarRocks 的 `PushDownPredicateRankingWindowRule`（ranking-window 谓词下推）。注意 StarRocks FE 自身只对 ROW_NUMBER/RANK 下推、未覆盖 DENSE_RANK；NovaRocks 的 exec 底座（`SortTopNType::DenseRank`）已支持三种，本设计三种都覆盖。

## 2. 范围与非目标

**范围**
- 新增逻辑重写规则 `RankingWindowPredicatePushdown`。
- 给 `LogicalSortOp` / `PhysicalSortOp` 增加 `partition_limit: Option<usize>` 与 `topn_type: Option<SortTopNType>` 字段（`None` = 非 partition-topn 的普通 Sort，与 `TSortNode` 的 Option 字段一致），并贯通 convert/implement/codegen/explain。
- 覆盖 ROW_NUMBER / RANK / DENSE_RANK，PARTITION BY 非空（per-group）。
- 补齐 OQ-13 完整收尾所需测试产物（见 §8）。

**非目标**
- 不改变任何查询语义：本规则是纯 plan 形态 + 行裁剪优化，结果逐行一致（保留 Window 与外层 Filter）。
- 不做「彻底消除 Window、由 TopN 直接产出 rank 列」的形态（正确性风险高，与「结果不变」验收相悖）。
- 不覆盖全局无 PARTITION BY 的 ranking（与既有 `sort_limit_to_top_n` 路径重叠，列为 future）。
- 不依赖 seed tpc-ds 数据或 FE plan-diff 基线作为硬验收（见 §8 验证基准）。

## 3. 当前状态调研（2026-06-15）

- **exec 底座已存在**：`src/exec/operators/sort/chunks_sorter_topn.rs` 实现 RANK/DENSE_RANK/ROW_NUMBER 的 per-partition 截断边界（含 tie 处理）；`src/exec/node/sort.rs` 定义 `SortTopNType{RowNumber,Rank,DenseRank}`；`TSortNode` 已有 `partition_exprs`/`partition_limit`/`topn_type` 字段。
- **标准优化器从不生成它**：`PhysicalSortOp`（`src/sql/optimizer/operator.rs:370`）目前只携带 `analytic_partition_exprs`，**没有** `partition_limit`/`topn_type`；`src/sql/codegen/fragment_builder.rs:2580` 把 `TSortNode.partition_exprs/partition_limit/topn_type` 写死为 `None`；优化器无任何规则消费 ranking 谓词。故该能力今天只能从 FE 下发的 plan 触达，standalone 路径完全缺失。
- **挂载点已就位**：planner 在 Window 下面已放一个带分区键的 Sort——`analytic_partition_exprs` 在 `src/sql/optimizer/convert.rs:100` 由 `node.analytic_partition_by` 填充，这正是要标注 `partition_limit` 的节点。`cascades_rules/implement.rs:735` 已把 `analytic_partition_exprs` 从 logical 透传到 physical。

## 4. 架构与挂载点

**规则**：`RankingWindowPredicatePushdown`，逻辑树重写，置于 `src/sql/optimizer/rewrite/rules/`，注册进 `src/sql/optimizer/rewrite/registry.rs` 的 known-rules 列表；kill switch：`SET disable_optimizer_rules='RankingWindowPredicatePushdown'`。

**匹配形态**（自顶向下）：

```
LogicalFilter(pred 引用 rank_col)
  └─[可选 LogicalProject]
      └─ LogicalWindow(window_exprs 含 rank_col = ranking_fn OVER (PARTITION BY p ORDER BY o))
          └─ LogicalSort(analytic_partition_exprs = p, order = o)   ← planner 已产生
```

**改写动作**：不动 Filter / Project / Window，只在底部 `LogicalSort` 设三字段：
- `partition_exprs = p`（= window 的 PARTITION BY 键）
- `partition_limit = K`（从谓词推出的每分区 rank 上界，见 §5）
- `topn_type = RowNumber | Rank | DenseRank`（从 ranking_fn 推出）

**到 exec 的链路**（仅差填值）：
1. `LogicalSortOp` + `PhysicalSortOp` 新增 `partition_limit: Option<usize>` 与 `topn_type: Option<SortTopNType>`（`None` = 非 partition-topn）。`partition_limit.is_some()` 为是否 partition-topn 的判定门。
2. `convert.rs`：logical→optimizer 转换透传新字段（默认 `None` / 既有缺省）。
3. `cascades_rules/implement.rs`：把新字段从 logical 透传到 physical（仿现有 `analytic_partition_exprs` 透传）。
4. `codegen/fragment_builder.rs`：把 `TSortNode.partition_exprs/partition_limit/topn_type` 改为读 `PhysicalSortOp` 的值。
5. `src/sql/explain.rs`：Sort 节点渲染追加 `partition_limit=K topn_type=Rank` token（golden 断言所需）。
6. exec 侧 `chunks_sorter_topn` + `TSortNode.partition_limit` 零改动。

**为何逻辑重写而非 cascades 规则**：匹配需跨 Filter→[Project]→Window→Sort 多层识别 ranking 函数与谓词，逻辑树重写匹配多层形态更稳；`partition_limit` 虽是物理细节，但挂在 Logical/Physical 两侧都已存在的 Sort 上，透传路径已通。

## 5. 匹配器、谓词提取与 tie 语义

**覆盖的窗口函数**：`ROW_NUMBER` / `RANK` / `DENSE_RANK`。

**谓词提取 → `partition_limit = K`**（核心不变量）：从 Filter 的**合取**谓词里，对 `rank_col` 求一个**有限上界 K = 能通过 filter 的最大可能 rank 值**：

| 谓词 | K |
|---|---|
| `rk <= k` | k |
| `rk < k` | k − 1 |
| `rk = c` | c |
| `rk BETWEEN a AND b` | b |
| `rk IN (..)` | max(..) |

- 只有下界（如 `rk >= 5`）、k 非常量、K ≤ 0 → **不改写**（无有限上界）。
- **Filter 原样保留**：Sort 只把每分区裁到「rank ≤ K 的超集」，精确比较仍由 Filter 完成。

**topn_type 与 tie**：`row_number → RowNumber`（每分区精确留 K 行）；`rank → Rank`（留 rank ≤ K，边界 tie 全留）；`dense_rank → DenseRank`。三者均满足：裁剪保留的是按同一 ORDER BY 的前缀超集，Window 在其上重算的 rank 与原值一致；被丢的行 rank > K，必然过不了上界为 K 的 Filter。**∴ 逐行结果一致。**

## 6. 正确性护栏

1. **Window 节点内只能含 ranking 函数**：若同一 Window 还含 `avg/sum/count(...) OVER`（全分区聚合窗口），裁剪分区会让聚合算错 → **放弃改写**。后果：命中面是「纯 ranking + 外层 filter」的 top-per-group（如 tpc-ds q49 的纯 `rank()`）；q47/q57 的 `avg(...) over` 与 `rank()` 混合形态**不触发，但结果不变**（仍为设计文档所述「回归保险」）。
2. **PARTITION BY 非空**：per-group 为焦点；全局无分区 ranking 不碰。
3. **避让 `AssertOneRow` 之下**：Apply 框架设计文档风险 #7——`AssertOneRow` 不可与 limit 类交换；本规则在其下方不触发。
4. **幂等**：若 Sort 已带 `partition_limit` 则跳过（仿 `aggregate_pushdown::already_pushed` 的 flag 守卫，其它规则克隆 Sort 时须保留该字段）。
5. **Project 穿透**：Filter 与 Window 之间允许一层 Project（rank_col 经投影传递）；超出形态保守放弃。
6. **保守放弃**：多 ranking 列、partition/order 与待裁剪 Sort 不一致、谓词含 OR 跨列混合等无法证明安全的形态，一律返回 Unchanged。

## 7. Stats / 属性

- `partition_limit` 把每分区行数压到 ~K：Sort 输出基数 ≈ `min(N, P·K)`，P = 分区键 NDV，N = 输入行数。RANK/DENSE_RANK 有 tie，可能略超 K，用保守系数。把降后的行数反映到 Window 输入基数，下游 cost 才看得到收益。
- ordering / distribution **不变**（仍按 partition+order 出；分区分布来自既有 `analytic_partition_exprs`），Window 的 ordering 需求仍满足。
- **不要双重折扣**：截断是「输入行数下降」，Filter 仍保留自身 selectivity，二者不叠乘。

## 8. 测试与交付物（完整收尾）

1. **explain 渲染**：`PhysicalSortOp` explain 追加 `partition_limit=K topn_type=…` token。
2. **optimizer plan golden**（`sql-tests/optimizer/`）：
   - `ranking_window_topn.sql`：三种 ranking × `rk<=k`/`rk=1`，`@explain_contains=partition_limit`（及 topn_type token）。
   - `ranking_window_topn_rejected.sql`：聚合窗口存在 / 无上界谓词 / 无 PARTITION BY → `@explain_not_contains` 对应 token。
3. **correctness sql-test**（rule on/off 结果须一致）：rank=1 per group；rank≤k 边界 tie；dense_rank≤k；row_number≤k；ORDER BY 含 NULL；单行/空组。
4. **补 Apply 半边缺失产物**：
   - tpc-h q2/q17 plan golden / `@explain_contains=WINDOW`（锁 WinMagic 收益）。
   - scalar 子查询多行 `@expect_error`（Apply 设计文档 §8.3 已规定但当前缺失）。
5. **单测**：谓词→K 提取、topn_type 映射、各护栏拒绝路径（聚合窗口 / 无上界 / 无分区 / AssertOneRow / 幂等）。
6. **kill switch + 注册**：`RankingWindowPredicatePushdown` 进 `registry.rs` known-rules。
7. **验证基准**（已选定）：新 golden 在 rule on/off 下结果一致 + `optimizer`/`join`/`filter`/`sort`/`cte` 套件无新增失败。不硬依赖 FE/tpc-ds seeding；如数据可用则 best-effort 跑 q47/q49/q57、q2/q17 观察形态，不阻塞交付。

## 9. 风险与未决

1. **命中面有限**：聚合窗口混合护栏会让 q47/q57 不触发。这是可接受的正确性优先取舍；纯 ranking（q49 形态、合成 golden）是确定性收益。不放宽护栏去凑覆盖率。
2. **tie 行数估计**：RANK/DENSE_RANK 截断后行数可能略超 K，stats 取保守值，避免下游 cost 低估。
3. **与 `topn_compactness` / `sort_limit_to_top_n` 协调**：这些 cascades 规则也读/写 Sort 字段且检查 `analytic_partition_exprs`（`topn_compactness.rs:235`）；需确认它们不会清掉或误处理新设的 `partition_limit`，并补一条「带 analytic 分区的 Sort 已携 partition_limit 时」的回归。
4. **planner 是否总在 Window 下产出 analytic Sort**：实现期确认；若存在无 feeding-sort 的 Window 形态，规则需自行插入或保守放弃。

## 10. 验收标准（对接 OQ-13）

- 新增 optimizer plan golden 覆盖 top-per-group（本设计）与 min/max per group（已由 `subquery_scalar_to_window.sql` 覆盖）。
- 命中 query 的 plan 出现 `partition_limit` + `topn_type`，且 rule on/off 结果逐行一致。
- tpc-h q2/q17 plan golden 锁住 `WINDOW`（补齐 Apply 半边）。
- scalar 子查询多行 correctness 用例补齐。
- `optimizer`/`join`/`filter`/`sort`/`cte` 套件无新增失败。
- q47/q49/q57 结果与 plan 不回退（回归保险）。
