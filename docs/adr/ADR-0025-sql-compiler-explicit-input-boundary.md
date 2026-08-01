---
id: ADR-0025
title: "Explicit SQL compiler input boundary"
domain: [sql-compiler, distributed-query-lifecycle, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-02
provenance:
  - "discussion: 2026-08-01 explicit SQL compiler input boundary"
code-anchors:
  - "novarocks/core/src/sql/compiler/mod.rs (SqlCompileRequest and SqlCompiler::compile)"
  - "novarocks/core/src/sql/catalog/provider.rs (QueryTableBindingStore)"
  - "novarocks/core/src/query_execution/preparation/mod.rs (prepare_fragments)"
---

## 问题

独立 SQL 入口如何在不泄漏 Frontend、Connector registry、native wire 与 query lifecycle 的前提下，使用同一次 admission 冻结的 catalog、statistics、function、MV 与 topology 事实完成编译？

## 背景与执行事实

独立 SQL 的 SELECT、EXPLAIN、DML source 和 MV refresh 都需要 parse、analyze、logical planning、statistics、optimizer 与分布式 physical planning。此前这些阶段分散在 session、engine helper 和 refresh flow 中，部分路径会在 metadata resolve 后再次取得当前 Connector generation。这样会让一次 query 的 schema、statistics 与 scan split 观察到不同 incarnation，也让解释、profile 与写入 source 的 plan facts 漂移。

Frontend 仍是 statement admission、session projection、view rewrite、deadline/cancellation 与 live backend topology snapshot 的 owner（ADR-0011、ADR-0012）。Connector metadata、statistics 与 planning lease 仍是 FE control capability，且必须按 generation 围栏（ADR-0015、ADR-0022）。native encoding、fragment preparation 和 lifecycle request 是 post-compile application work，不是 SQL compiler output。

## 考虑过的选项

1. 保留各 application helper 的 analyzer/optimizer 调用，并用 session/state 参数约定其一致性。改动最小，但无法机械证明没有二次 latest acquire，也会继续产生 EXPLAIN、write 和 refresh 的双 pipeline。
2. 让 compiler 接收完整 RequestContext、StandaloneState、repository 或 raw application callback。调用方便，但把 application owner 和 mutable runtime 重新藏进 compiler，无法形成可迁移边界。
3. 以 `SqlCompileRequest` 传入 statement、intent、session SQL projection、non-zero topology facts、query-scoped catalog/statistics/function/MV snapshots 与 control；以 paired `PostCompilePlanningContext` 传入同一 binding store 供 preparation/native request assembly 使用。

## 裁决

采用选项 3。`SqlCompiler::compile` 是 parse/analyze/logical/optimizer/physical/distributed SQL facts 的唯一 production kernel。它不接收 session/state、repository、registry、Frontend service、native DTO/bytes、QLC entry 或 result buffer。

application 在 view/virtual rewrite 后构造 `QueryPlanningInputs`。catalog 的首次 resolution 将 exact table handle、incarnation、statistics pin 与 planning lease 写入 `QueryTableBindingStore`；statistics 与 scan preparation 都只能读该 binding。缺少 binding 或 lease 立即失败，不能 re-acquire current/latest。MV candidate 输入在 admission 一次性冻结，保持 repository 顺序、warn-and-skip 与 16 个成功 candidate 上限。function classification、signature 与 volatility 由同一 immutable function catalog 提供，optimizer 不维护第二份名称列表。

`SqlCompileOutput` 仅包含 analysis/logical/optimized/immediate explain/distributed SQL plan facts。application 再将 distributed plan 用相同 binding store 做 scan preparation、native encoding 与 lifecycle request assembly。写入 root distribution 和 IMV validation 使用 typed SQL values，不能再传 raw closure。

## 接受的妥协（诚实记录）

本决策不立即创建独立 `novarocks-sql` crate，也不移动 native encoder 或 fragment preparation；这些工作在当前 Core 内仍有物理邻接。选择这个过渡形态是为了先消除同一 query 的事实漂移和 caller 双路径，因变更风险与验证成本较低，并非认为长期 Core 聚合优于独立 ownership。

为保留现有函数行为，function snapshot 暂时组合 signature registry 与 legacy fallback。它消除了 analyzer/optimizer volatility 分叉，但不代表所有函数实现都已迁移到一个新 registry。

## 何时重新评估

- SQL kernel 已无 Core runtime type 依赖且独立 crate 的 Cargo owner 可以稳定承担；
- native encoder 或 preparation 需要由 Frontend/Execution adapter 独立演进；
- 新 statement family 不能用 typed intent 表达而要求万能 context 或 callback；
- 多 FE admission 使 catalog/MV snapshot 需要跨进程 durable fencing；
- 新 Connector 无法在一次 exact binding 中同时提供 metadata、statistics 与 scan planning。
