---
id: ADR-0072
title: "SQL compilation freezes statistics after analysis"
domain: [sql-compiler, distributed-query-lifecycle, provider-spi]
status: active
supersedes: [ADR-0025]
superseded-by: null
date: 2026-08-14
provenance:
  - "discussion: 2026-08-14 two-phase SQL analysis and statistics sealing boundary"
code-anchors:
  - "novarocks/sql/src/compiler/mod.rs (SqlCompiler::analyze and SqlCompiler::optimize)"
  - "novarocks/frontend/src/query/compiler.rs (FrontendQueryCompiler)"
  - "novarocks/core/src/query_execution/planning/statistics.rs (QueryStatisticsContext)"
---

## 问题

当 SQL 分析本身才会为主查询及普通 MV candidate 物化 exact table binding token 时，compiler 应在什么时点冻结 statistics，才能保证 optimizer 使用完整、同 scope 且不可回退到 latest 的统计事实？

## 背景与执行事实

SQL analyzer 通过 application 提供的 catalog materializer 解析表，并把 exact Connector handle、owner、incarnation、data version、statistics pin 与 planning lease 写入 request-local `QueryTableBindingStore`。optimizer 不按表名读取 statistics，而是用 logical scan 上的不透明 `SqlTableBindingId` 从 `DmlStatisticsSnapshot` 取证据。普通 MV rewrite candidate 的 definition SQL 与 target table 也会在 candidate eligibility 和 logical planning 期间产生 binding token。

原先的单次 `SqlCompiler::compile` request 同时携带 catalog 与一个已经冻结的 statistics snapshot。application 因而只能在调用 compiler 前冻结 snapshot；但此时 analyzer 尚未产生上述 token。后续 optimizer 对新 token 查询 snapshot 时得到 `BindingMissing`。这不是统计不可用，而是 snapshot 根本没有覆盖已分析的 binding authority；将它当作 missing estimate、重新读取 latest 或跳过 candidate 都会掩盖 scope/owner/version 矛盾。

statistics freeze 仍由 application owner 执行，因为只有它持有 exact Connector admission、lease 与统一 statistics resolver。SQL owner 只应接收冻结后的值；它不能持有 provider callback，也不能在 optimizer 阶段重新进入 catalog。

## 考虑过的选项

1. 保留单阶段 API，在 binding 缺失时按统计 Missing、默认行数或无 MV candidate 继续。改动小，但把 authority 缺口伪装成成本估计缺失，允许 schema、statistics 与 scan preparation 来自不同 binding，违反 fail-closed 契约。
2. 在分析前预扫描 SQL 文本和 MV repository，猜出所有表并提前物化。这样仍无法可靠覆盖 CTE、view、function resolution、rewrite target 与后续 analyzer 语义；它还会形成第二套 name resolution owner。
3. 让 SQL compiler 在内部分析后回调 application statistics resolver。时序正确，但 compiler 会重新持有 application/provider capability，phase 2 无法证明是纯值计算，也使 retry、generation 与 cancellation owner 模糊。
4. 将编译拆为两个 typed 阶段：phase 1 接收 catalog/functions 并完成所有可能产生 token 的分析与物化，返回不可 Clone、不可序列化、按值消费的不透明 handle；application 随后从同一 request-local store 冻结 statistics；phase 2 只接收 handle 与 snapshot，附着统计、优化并 seal distributed plan。

## 裁决

采用选项 4。公共 kernel 是 `SqlCompiler::analyze(SqlAnalyzeRequest) -> SqlAnalyzeOutput` 与 `SqlCompiler::optimize(SqlOptimizeRequest) -> SqlCompileOutput`。

`SqlAnalyzeRequest` 不含 statistics。phase 1 完成 parse、name/function resolution、logical planning、IMV rewrite，以及普通 MV candidate 的 definition analysis、base/target materialization 和 SQL-private descriptor 构造。`AnalyzeOnly` 与 `LogicalOnly` 在这里返回 terminal `Complete`；所有需要 optimizer 或 distributed seal 的 intent 返回 move-only `SqlAnalyzedQuery`。MV first refresh、join refresh 与 change-stream 等需要 SQL-private logical transform 的路径使用同样不可重放的 terminal-specific opaque analyzed handle，不把 logical graph 或 column factory 暴露给 application。

application 必须从 phase 1 使用的同一个 `QueryTableBindingStore` 构造 `DmlStatisticsSnapshot`，然后创建 `SqlOptimizeRequest`。phase 2 的类型中没有 catalog、function catalog、MV repository、materializer 或 raw SQL capability；它只能读取 frozen snapshot、分配 `StatsRef`、优化并 seal。handle 按值消费且不实现 `Clone`、serde serialization 或 deserialization，不能跨 request、retry、recovery 或进程重放。

snapshot 中显式的 typed `Missing` 是合法的保守统计，允许正常优化。snapshot 遗漏 token、cross-scope token、owner/incarnation/data-version mismatch 与 corrupt evidence 都是 compilation fatal。普通 MV candidate 的 parse、freshness、unsupported shape 或 materialization eligibility failure 可按原候选诊断跳过；一旦进入 statistics attachment，authority error 不得降级为 candidate warn-and-skip。

Frontend SELECT、物理 EXPLAIN、EXPLAIN ANALYZE、DML、CTAS 与 MV refresh 都遵循 `analyze -> same-store statistics freeze -> optimize/seal -> post-compile preparation`。Logical EXPLAIN 是 phase-1 terminal，不冻结 statistics。scan preparation 与 native encoding 继续使用同一个 binding store，且不成为 SQL compiler 的职责。

## 接受的妥协（诚实记录）

公共 API 从一个 request/terminal 增加为 typestate 风格的两个 request 和多个 specialized opaque handle，调用方迁移与测试面显著扩大。这个复杂度不是为了提供更灵活的扩展点，而是为了让不可能的时序与能力组合在类型上不可表达；真实分布式统计正确性优先于 API 表面简洁。

phase 1 必须在知道统计质量和最终 optimizer 选择之前物化所有合格普通 MV candidate 的 base/target binding，最多保留既有的 16 个成功 candidate 上限。这可能产生随后未被选中 candidate 的 catalog 工作。选择它是因为完整 binding coverage 与稳定 repository order/factory identity 比推迟物化更重要，并非因为提前物化更省资源。

opaque handle 不能 durable 化或跨进程传输，因此 crash/retry 必须从新的 request-local analysis 和新的 exact binding store 重新开始。我们接受重复分析成本，以避免把 provider capability、logical graph 或失效 token 变成可重放状态。

## 何时重新评估

- SQL 优化完全不再消费 table statistics，或 statistics 不再按 analysis 产生的 exact binding token 索引；
- catalog materialization 能在不复制 analyzer/name-resolution 语义的前提下，原子返回完整 logical analysis 与同版本 statistics；
- compiler 需要跨进程 durable continuation，且已有可证明 owner/incarnation/data-version 有效性的版本化 handle 与恢复协议；
- 普通 MV candidate 的提前物化成本在生产指标中成为主要规划瓶颈，并且有新的 typed eligibility proof 能在不产生 target binding 的情况下安全裁剪；
- application 与 SQL 的 ownership 边界改变，使 statistics resolver 成为 SQL-owned、request-local 且无 latest/retry capability 的纯值服务。
