---
id: ADR-0044
title: "Backend-owned runtime-filter participant domain"
domain: [runtime-filter]
status: active
supersedes: []
superseded-by: null
date: 2026-08-09
provenance:
  - "PR: pending"
  - "discussion: 2026-08-09 runtime-filter participant ownership migration"
code-anchors:
  - "novarocks/backend/src/runtime_filter/participant.rs (BackendRuntimeFilterParticipantFactory)"
  - "novarocks/backend/src/runtime_filter/domain/session.rs (BackendRuntimeFilterSession)"
---

## 问题

当 runtime filter 已由 Execution 定义语义值与 evaluator 后，谁应拥有分布式 participant 的安装、replay、coverage、物化、订阅与路由状态？

## 背景与执行事实

runtime filter 的 contribution、membership schema、ordered contract、logical version 和 evaluator outcome 是跨 fragment 的语义值。它们既不属于 Core kernel，也不属于 native protobuf；其中的 canonical bytes 必须在 producer、Backend ingress 和 artifact decode 之间逐字节一致。另一方面，query attempt、部署 epoch、route edge、coverage witness、重试/去重、retained-artifact budget 及订阅唤醒均是 Backend 本地 participant 的物理生命周期。

ADR-0043 已把 row/scan 的类型检查、fail-open 分类及 Effect 固定在 Execution，并限制 Backend 只能提供 immutable artifact query。本决策继续收口其余的 participant 生命周期，避免 Core 继续作为 reducer、router 或 service 状态的隐式 owner。

## 考虑过的选项

1. 保留 Core runtime-filter service，Backend 仅调用 transition adapter。短期 diff 小，但 Core 会继续保存 deployment、reducer 和 transport 镜像类型，所有权无法验证，也会迫使 Backend 启用 test-support feature。
2. 将所有 participant 状态搬入 Execution。能减少 crate 数量，但 Execution 将获得 query attempt、route、budget 和 transport 的物理知识，破坏它作为 evaluator/semantic value owner 的中立性。
3. Backend 建立私有 participant domain，Execution 只提供 sealed contract、contribution、snapshot 与 handle。Backend 严格 decode 后维护物理状态，并把 immutable artifact query 注入 Execution subscription。

## 裁决

选择选项 3。`novarocks-execution` 独占 Runtime Filter semantic contract、canonical contribution、snapshot/version、producer/consumer handle 和 row/scan evaluator。`novarocks-backend::runtime_filter::domain` 独占 participant identity、install policy、coverage、reduction/replay、artifact materialization/admission、subscription、routing、transport 与 Backend-local event observer。Core production caller 仅保留 operator coordinate 与 Execution API 调用。

Backend native install decoder 直接从 protocol DTO 构造 Backend install 与 Execution contract；contribution ingress 先按 installed Execution contract 严格解码 NRFC，再进入 Backend reducer。artifact delivery 严格解码 Backend NRFA/NRPU frame，构造 immutable Backend artifact query，最后将 Execution snapshot/unavailable outcome 发布给 subscription。Backend query 只能回答 null、value 与 closed range 原语，不能接收 Arrow batch、scan facts，或产生 evaluator outcome/Effect。

## 接受的妥协（诚实记录）

这次迁移暂时保留 Core 对通用 query lifecycle、kernel operator 和 native plan carrier 的依赖；它不是一次完全移除 Core crate 的工作。也没有在 Backend 建立 observation store，participant event observer 默认丢弃，仅保留 test injection。这是为了先完成可审计的 owner cut，而不是因为当前事件可观测性已经足够；持久化/聚合会在独立 observation 设计中处理。

artifact NRFA/NRPU 继续是 Backend 私有物理 codec，而 contribution NRFC 留在 Execution。两种 frame 并存增加了边界数量，但避免让 Execution 知道 resident index、admission lease 或 transport envelope，也避免 Backend 重复实现 canonical contribution。

## 何时重新评估

- 如果 artifact materialization 需要跨 Backend 共享 resident memory 或全局 admission，应定义新的 Backend resource owner，而不是把 budget 状态放回 Execution。
- 如果多种 connector 需要同一 immutable artifact query，需要在不携带 Arrow/scan facts 的前提下扩展 Execution query capability；否则保持 Backend adapter 私有。
- 如果需要诊断、SLO 或跨 attempt 聚合，应在 Backend participant 之外建立 observation store，不向 Core 恢复全量 service event collection。
- 如果 Core 的旧 runtime-filter test-support 已无测试价值，应在后续删除任务中移除该历史代码和 feature，而不是重新让 Backend 依赖它。
