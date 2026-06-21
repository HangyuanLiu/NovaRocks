# 通用 Logical Rewrite Framework 设计

## 背景

NovaRocks 当前已经有 `src/sql/optimizer/**`，包含直接作用于
`LogicalPlan` 的 RBO driver、Cascades memo、物理实现规则、统计与
EXPLAIN 输出。Iceberg v3 增量 MV 后续需要 delta / version marker、
column lineage、internal column tracking、rule ordering、failure
isolation 和 rule observability。如果这些能力只做成 MV refresh path
里的 shape-specific helper，后续 aggregate、join、UNION ALL、schema
evolution、partition pruning 仍会继续分叉。

本设计把前置工作定义为一套查询和 MV 都可复用的 logical rewrite
substrate，而不是 IMV 专用 optimizer。MV rewrite 是第一批重要消费者，
普通 SELECT 优化规则也应逐步迁移到同一套框架中。

## 目标

- 提供通用 `LogicalPlan -> LogicalPlan` rewrite framework。
- 支持按 phase 注册和执行规则，保持 rule ordering 可审计。
- 支持 top-down / bottom-up tree traversal 和稳定的 children rebuild。
- 支持 rule trace、rule disable、命中统计、失败诊断和耗时记录。
- 支持 failure isolation：规则失败时不能污染原始 plan。
- 为 column lineage、internal column、branch identity、source identity
  和版本窗口等 metadata 预留中性承载接口。
- 让普通查询和 MV refresh 都能作为消费者接入。

## 非目标

- 不复制 StarRocks 的完整 memo / CBO 架构。
- 第一阶段不迁移现有 RBO 主路径。
- 第一阶段不实现 Delta / Version / Action column 语义。
- 第一阶段不改变 Iceberg MV refresh 语义。
- 不把 Iceberg commit、target apply、refresh scheduler 放进 optimizer。
- 不引入 silent fallback；无法安全改写时必须给出明确诊断。

## StarRocks 参考取舍

StarRocks 可借鉴的部分是 rule pipeline 和工程边界：

- `OptExpression + Operator` 提供统一逻辑树承载。
- `Rule + Pattern + RuleSet` 提供规则注册、匹配和调度。
- IVM rewrite 先在 trial plan 上执行，失败时恢复原始 plan。
- Delta / Version marker 必须在进入物理优化前清理完毕。
- rule disable、optimizer trace 和 MV rewrite trace 让问题可二分。

NovaRocks 第一阶段只吸收这些通用机制，不复制 StarRocks 的完整 optimizer
层级，也不把框架命名为 IVM 专用模块。

## 架构

新增模块建议放在：

```text
src/sql/optimizer/rewrite/
  mod.rs
  context.rs
  phase.rs
  rule.rs
  result.rs
  trace.rs
  tree.rs
  pipeline.rs
  registry.rs
```

### RewriteContext

`RewriteContext` 是单次 rewrite 调用的上下文，承载：

- disabled rule set。
- rewrite policy。
- trace collector。
- 可选 consumer metadata，例如 query context 或 MV refresh context。

consumer metadata 必须保持中性扩展接口。MV 可以把 refresh context 作为扩展输入，
但 framework 本身不依赖 MV 类型。

### RewritePhase

`RewritePhase` 描述规则运行阶段。第一阶段建议保留少量通用阶段：

- `LogicalNormalize`
- `StructuralRewrite`
- `SemanticRewrite`
- `Validation`

MV 后续可以在 registry 层选择启用额外规则，但不需要新增 IMV 专用 driver。

### LogicalRewriteRule

通用规则 trait 应覆盖：

- stable rule name。
- 所属 phase。
- traversal strategy。
- cheap `matches`。
- local `apply`。

规则只负责当前 node 的局部改写，不递归遍历 children。遍历由 framework 统一处理。

### RewriteResult

规则返回值应显式区分：

- `Unchanged`
- `Changed(LogicalPlan)`
- `Rejected(RewriteDiagnostic)`

`Rejected` 用于表达当前 shape 或 metadata 不满足规则前置条件。pipeline 根据 policy
决定 fail fast、跳过，还是把诊断交给上层 fallback 策略。

### RewriteTrace

trace 至少记录：

- phase start / end。
- rule skipped by disable。
- rule matched。
- rule changed plan。
- rule rejected with diagnostic。
- rule failed with error。
- elapsed time。

普通 EXPLAIN 和 MV refresh debug 后续都应读取同一种 trace 数据结构。

### TreeRewriter

`TreeRewriter` 提供统一 traversal 和 children rebuild helper。第一阶段覆盖：

- `Scan`
- `Project`
- `Filter`
- `Aggregate`
- `Join`
- `Union`
- `Sort`
- `Limit`
- `Values`
- 现有 CTE / Window / TableFunction / Repeat 等节点保持 no-op traversal 支持。

该层的核心验收是：no-op traversal 不改变结构，子节点改写能稳定回装。

### RewritePipeline

`RewritePipeline` 按 phase 执行 rule registry。每个 phase 内可以采用固定点循环，
但第一阶段应保守：

- 空 registry 必须 no-op。
- rule disable 必须在调用 `matches` 前生效。
- 每次 rule apply 先作用于 cloned node / cloned plan。
- rule failure 不能污染输入 plan。
- phase 顺序必须进入 trace。

## 第一阶段范围

第一阶段做框架和低风险接入：

1. 新增 `src/sql/optimizer/rewrite/` 通用框架。
2. 保留现有 `rbo::driver` 主路径，不迁移 predicate pushdown、aggregate pushdown
   或 column pruning。
3. 在普通 query optimize path 中接入一个空的 `QueryRewritePipeline`，证明查询侧
   可消费该框架且不会改变现有 plan。
4. 预留 MV adapter 类型，让 Iceberg MV refresh context 后续可以进入同一套
   `RewriteContext`，但第一阶段不启用 MV 改写规则。
5. 把新框架的 rule name 纳入统一 rule disable / known-rule 检查，避免未来出现
   两套互相不可见的 rule 开关。

## 测试与验收

### Tree rewrite 单测

- no-op traversal 不改变 `LogicalPlan` debug structure。
- 子节点改写能正确回装到 `Project / Filter / Aggregate / Join / Union`。
- leaf node 不触发 children rebuild。

### Pipeline 单测

- 空 pipeline 不改变 plan。
- phase 顺序进入 trace。
- disabled rule 不调用 `matches` / `apply`。
- `Changed` 能进入下一轮。
- `Rejected` 产生稳定 diagnostic。
- rule error 不污染原始 plan。

### Query-side 集成单测

- `optimize()` 接入空 query pipeline 后，已有简单 query 的 physical / explain
  输出保持不变。
- 禁用一个新框架注册的 test rule 时，trace 能显示 skipped。

### MV adapter 单测

- MV refresh context 可以作为扩展输入放入 `RewriteContext`。
- 空 MV pipeline 不改变 logical plan。
- 不产生 delta / version / action marker。

## 后续迁移路线

1. 迁移一个低风险普通查询规则作为示范，例如结构简单且 plan shape 稳定的规则。
2. 逐步把 RBO predicate pushdown、column pruning、aggregate pushdown 接入新
   framework，保留现有 phase 顺序。
3. 在 MV 侧新增 marker / metadata 承载结构，但保持 marker cleanup validation
   位于 logical rewrite 和 physical planning 边界。
4. 实现 Iceberg scan snapshot binding、action column propagation、aggregate
   state rewrite、join delta rewrite、UNION ALL rewrite。
5. 将 rewrite trace 接入 EXPLAIN / debug 输出和 SQL golden。

## 自检

- 设计没有把框架绑定到 IVM 或 Iceberg。
- 第一阶段不改变现有 SELECT 优化语义。
- 第一阶段不改变 Iceberg MV refresh 语义。
- 查询和 MV 都有明确接入路径。
- 现有 RBO 迁移被拆到后续阶段，避免一次性扰动主路径。
- 所有不支持的 rewrite shape 都通过 diagnostic 或 fail-fast 表达。
