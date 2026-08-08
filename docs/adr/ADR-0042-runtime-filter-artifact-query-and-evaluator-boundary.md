---
id: ADR-0042
title: "Runtime filter artifact query and evaluator boundary"
domain: [runtime-filter]
status: active
supersedes: [ADR-0041]
superseded-by: null
date: 2026-08-08
provenance:
  - "PR: pending"
  - "discussion: 2026-08-08 runtime-filter execution ownership closeout"
code-anchors:
  - "novarocks/execution/src/runtime_filter/evaluator.rs (RuntimeFilterArtifactQuery, evaluate_rows)"
  - "novarocks/backend/src/runtime_filter/service/native_execution.rs (NativeRuntimeFilterArtifactQuery)"
  - "novarocks/core/src/exec/operators/scan/runner.rs (scan-unit evaluation call)"
---

## 问题

行级过滤与 reader-open 前 scan-unit 过滤应如何共享同一份 runtime-filter artifact，同时让 Execution 独占类型检查、降级、outcome 和 effect，而不让 Backend 或 Core 再实现第二套求值器？

## 背景与执行事实

runtime filter 是保守的性能优化。artifact 是否可用、输入 Arrow 列与契约类型是否精确一致、null 与排序语义如何组合，以及资源不足时是否保留输入行，都必须得到同一结论；把 row loop 留在 Core、把 scan-domain loop 留在另一个层会使这些语义随时间漂移。

Backend 仍拥有 participant service、artifact、reducer、materializer 与订阅状态，因为它是本地 fragment participant owner；这些职责不需要 Arrow batch 或 Connector scan facts。Core 仍拥有 `ExprId`、key ordinal、operator node 与 scan reader 生命周期，但不拥有 artifact 的可匹配判断。

## 考虑过的选项

1. 让 Core 保留 Arrow row predicate，Backend 另给 scan evaluator 提供 capability。短期迁移量较小，但 row/scan 会有两条类型、null、fail-open 路径，且 Core 会持续依赖 concrete artifact。
2. 让 Backend adapter 接收 Arrow batch 或 scan facts 后直接返回 mask、prune 或 effect。这样少一次调用，却会把 Execution policy 与 Connector facts 混入 participant service，无法独立测试或迁移。
3. Backend 仅实现 immutable `RuntimeFilterArtifactQuery` 原语，Execution 消费 Arrow 值或 sealed scan facts 并产生 row/scan outcome；Core 只调用 Execution 并执行已有 operator/reader 生命周期。

## 裁决

选择选项 3，并 supersede ADR-0041。`RuntimeFilterArtifactQuery` 只报告冻结数据类型、null/non-null 命中、单一 Execution scalar 是否可能命中，以及 Connector closed range 是否可能相交。它的错误仅是 `Unsupported`、`ResourceUnavailable`、`ContractViolation`；它不得接收 Arrow batch、SPI scan facts、provider handle 或返回 outcome/effect。

Execution 的 `evaluate_rows` 与 scan-domain evaluator 使用同一 query，负责类型与事实校验、null/order 组合、fail-open 分类、logical version 和只能由 evaluated 结果导出的 effect。FinalDomain 与 contribution 均以 canonical typed bytes、schema metadata 和 digest 传递，不携带 `Any` 或 backend/core concrete value。Backend 对这些 bytes 作严格 decode、artifact profile 与 authority 校验；Core 只保存 Execution contract 和 kernel 坐标。

## 接受的妥协（诚实记录）

participant artifact 和 materializer 暂时仍位于 Backend，且其低层 resident layout 可能复用 Core 的过渡物理 helper。这是为了保持现有 transport、reducer 和内存记账不变，而不是因为 Core 仍应拥有 runtime-filter 语义。该物理迁移将在后续独立变更中处理；在此之前 helper 只能暴露中立查询原语，不能重新提供 Arrow evaluator 或 scan outcome。

Execution 对 Arrow array 的遍历会比直接让 Backend/ Core 手写专用 loop 多一层抽象。选择它是为了让 fail-open 与 profile/effect 的唯一 owner 可审计；若性能成为问题，优化必须保留 artifact-query 形状与 Execution outcome 边界。

## 何时重新评估

- artifact query 需要 compound key、collation、timezone 或新的 Connector domain 原语时，先扩展 Execution 的 typed request/result，并补齐 row 与 scan 共用的语义矩阵。
- Backend 需要持久化或聚合 scan outcome 时，只在 adapter 后添加 observation store，不让 Execution 依赖 Backend。
- 当 participant physical artifact 脱离 Core 时，将临时 primitive 一并迁移；若仍需 Core Arrow predicate，说明本 ADR 的边界被破坏，应重新设计而非添加 facade。
