---
id: ADR-0020
title: "Frontend owns the DELETE application flow"
domain: [frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-07-31
provenance:
  - "PR: frontend INSERT application ownership #792"
  - "discussion: 2026-07-31 frontend DELETE and equality-delete ownership"
code-anchors:
  - "novarocks/core/src/engine/delete_engine.rs (DeleteEngine)"
  - "novarocks/frontend/src/dml/delete/mod.rs (DmlService::try_execute_delete)"
---

## 问题

在 frontend 已拥有 statement admission、INSERT application routing 和 StateStore DML journal 后，标准
DELETE 与 equality-delete 的 statement conversion、operation authority 和 distributed write orchestration 应由哪个
crate 拥有，才能既保持 Iceberg connector 的 external commit truth，又不重新捕获 request execution identity？

## 背景与执行事实

ADR-0011 要求每个 statement 仅在 admission 时冻结一次 topology、deadline、cancellation 与 optimizer settings；
ADR-0012 已将 session admission/router 置于 frontend。INSERT 已由 frontend 通过窄 `InsertEngine` port 调用 core 的
query 和 connector truth。旧 DELETE flow 留在 core command dispatch，且 equality-delete 曾以缺失 execution context 的
方式启动 distributed sink，可能退化为 maintenance execution capture。

Iceberg predicate、position delete/deletion vector、equality-delete file、catalog/table、distributed execution 与 external
commit 都仍是 core/connector 的实现能力；它们不能泄漏给 frontend 或提升为通用 provider SPI。frontend host 已提供
StateStore-backed operation journal，缺少该 journal 时可明确拒绝 Iceberg DML，而不是提供内存或 metadata fallback。

## 考虑过的选项

**frontend 拥有 DELETE application route，core 暴露窄 `DeleteEngine`。** frontend 识别目标 statement、在 writer side
effect 前检查 durable journal，并传递 admitted execution；core 保留 SQL validation、target resolution、distributed sink
与 Iceberg commit truth。

**继续让 core command dispatch 直接执行 DELETE。** 改动最小，但 frontend router 不是唯一 application owner，且
equality-delete 的 execution identity 容易与 admission 脱节。

**将 DELETE contract 置入系统 SPI 或向 core 注入 frontend callback。** 前者把单一 consumer 的 statement orchestration
错误固化为产品 provider 契约，后者反转 crate dependency 并形成 service locator/cycle；两者均与 ADR-0006 不符。

## 裁决

frontend `DmlService` 是标准 DELETE 和 `ALTER TABLE ... ADD EQUALITY DELETE` 的 production application owner。
它在现有 router 中位于 core fallback 之前，目标 statement 一旦识别即在 frontend route 中 fail-fast，绝不再回退到 core
command dispatch。每次调用将同一个 admitted `QueryExecutionContext` 传给 `DeleteEngine`；core adapter 必须用它启动
distributed write，不能重新采集 topology、deadline 或 cancellation。

core 的 `DeleteEngine` 是仅供 frontend 的 statement-specific reverse port。它保留 parser primitive、Iceberg target/ref
validation、position/deletion-vector/equality-delete sink 和 external commit/finalize implementation；frontend 不读取
connector/catalog/table 或 write payload。journal v1 继续由 frontend host 拥有，operation 类型为 `RowDelta`；无
StateStore 时目标 Iceberg DML 在调用 adapter 前失败。

## 接受的妥协（诚实记录）

这项裁决增加了一个与 INSERT 相似但不能泛化的 port，并要求 frontend route 与 core adapter 同时演进。它不是因为多一层
plumbing 更优，而是为了在不泄漏 Iceberg private payload、不创建 generic DML SPI、也不保留双 production route 的条件下
完成 owner hard cut 的最低成本。

在 aggregate core 仍未完全物理拆分期间，DeleteEngine 仍位于 core，部分旧 DELETE execution helper 也会与 MV/mutation
callers 共存。接受这个过渡成本是为了先保持 connector read/delete visibility 与 external commit 的真实 owner，而不是把
它们错误搬到 frontend。

## 何时重新评估

- SQL application/kernel/provider 物理拆分完成，DeleteEngine 不再是合理的边界；
- 出现多个真正可替换的 DELETE provider，并需要稳定的跨实现 conformance，此时按 ADR-0006 重新判断 SPI；
- StateStore 部署契约或多 frontend fencing/takeover 改变，需要重审 operation journal authority；
- ADR-0011 的 immutable execution contract 被 supersede，必须复核 DELETE distributed write 的 identity 透传；
- UPDATE/MERGE 迁入 frontend，发现其与 DELETE 存在可证明的、稳定且不泄漏内部 payload 的共同 contract。
