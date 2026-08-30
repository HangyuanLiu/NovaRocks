---
id: ADR-0127
title: "Participant attempt identity fences immutable stage admission"
domain: [distributed-query-lifecycle, runtime-filter, crate-boundary]
status: superseded
supersedes: [ADR-0126]
superseded-by: ADR-0128
date: 2026-08-30
provenance:
  - "mechanism: participant attempt and immutable Stage fence convergence"
  - "discussion: 2026-08-30 native lifecycle identity direction"
code-anchors:
  - "idl/novarocks/service.proto (StageFragmentsRequest, QueryControlAttach, AbortQueryRequest, FragmentLiveObservation, RuntimeFilterFeedbackEvent)"
  - "novarocks/proto-codec/src/lifecycle/stage.rs (StageDigest, QueryStageRequest)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/barrier.rs (QueryLifecycleCoordinator)"
  - "novarocks/backend/src/query_lifecycle/registry.rs (QueryLifecycleRegistry::stage_fragments)"
---

## 问题

一个已分配 Backend participant 在 Init 后接收 Attach、Abort、Stage、observation 和 runtime-filter feedback 时，怎样既用同一最小进程 incarnation identity 拒绝 foreign/retired message，又保留 Init fingerprint 的内容围栏与全局 Stage/Start 启动 fence？

## 背景与执行事实

ADR-0126 已把 `ParticipantAttemptRef = (QueryExecutionId, BackendProcessId)` 定为 terminal delivery 的唯一 participant identity，并明确把非 terminal carrier 的迁移留给后续裁决。一个 query attempt 可同时包含多个 Backend process；只比较 execution 会使旧进程或错误参与者有机会影响另一个 participant 的 local entry、telemetry 或 runtime-filter winner。

Init manifest 的 digest 仍是一次内容 admission 的精确 fingerprint：它区分同一 participant ref 下的不同 manifest，并且是 unary Abort 在 Init/Abort 重排时保留的窄 cleanup 围栏。它不是 Stage、Attach、observation、feedback 或 terminal delivery 的 participant identity。此前 Attach 的 frontend owner epoch 和 Stage/Start 的 version 也不能说明消息是否属于当前 Backend incarnation。

Stage 是 participant-local 的不可变 bundle。它在 Backend 的 CatalogReady 后一次性保留资源，再由 Start 释放 gate；Coordinator 只有全部 participant Stage ACK 后才发送任何 Start。该内容需要稳定 digest 用于重复投递与 Start fence，但 digest 不应重新承担 participant identity。

## 考虑过的选项

1. **在所有 post-Init carrier 继续并列发送 execution、backend、Init digest 与 owner/version。** 兼容旧调用方，但每条消息重复不同层的 identity，容易让 receiver 只验证其中一部分；owner epoch 也不标识 Backend process incarnation，拒绝。
2. **只以 `QueryExecutionId` 关联 post-Init message。** wire 更小，但同一 attempt 的 foreign process 可以写 observation、feedback 或释放本地状态，拒绝。
3. **所有 post-Init carrier 使用 `ParticipantAttemptRef`，仅 Init/unary Abort 另保留 manifest digest；StageDigest 以 ref 与完整 immutable bundle 派生。** 每个状态机在 mutation 前都能比较 exact participant，内容与身份职责分离，选择。
4. **让同一 ref 接受第二份 Stage 并以 revision、append 或 partial Start 协调。** 这会增加第二个启动语义和恢复协议，破坏“一 participant 一 immutable Stage”的资源与全局 barrier 证明，拒绝。

## 裁决

ADR-0126 被本 ADR supersede。`ParticipantAttemptRef` 成为所有 post-Init 非 terminal carrier 的唯一 participant identity：Attach、Stage、unary Abort、fragment observation 与 runtime-filter feedback 都只携带 ref，旧 tag/name 永久 reserve，不保留 dual decoder 或 mixed-version negotiation。Backend 在 entry、pre-Init tombstone、control attach、Stage、observation 与 feedback 副作用前验证 ref 的 process 等于 local process；Frontend 在 session/manifest/slot/channel 可变状态前验证 exact ref。

Init 保留 manifest digest 用于重复 Init 与 unary Abort 的 cleanup 矩阵：同 ref/同 fingerprint 是 duplicate，same ref/different fingerprint 是 Conflict，foreign process 不能创建或覆盖 tombstone。该 digest 不再进入普通 post-Init carrier。

StageDigest 以 `(ParticipantAttemptRef, canonical ordered fragments)` 计算。相同 ref 的 exact Stage 可重试；同 ref 的不同 Stage 冲突；空 Stage 也包含 process identity。Start 仅携带 execution 与 retained StageDigest；Backend 只释放同一已 Stage digest 的 gate，Frontend 仍维持 all-Stage-before-any-Start barrier。compatibility epoch 随这个 hard cut 演进，拒绝混合 island。

## 接受的妥协（诚实记录）

这次同时迁移多个 carrier、测试夹具和 system runner fault vocabulary，改动面明显大于保留旧字段。选择它是为了消除已经存在的平行 identity authority，而不是因为一次 hard cut 成本更低。该决定仍不提供 rolling upgrade：所有 FE/BE 必须使用同一个 compatibility island；reserved tags 只是防止未来误复用，不是 legacy fallback。

Init fingerprint 没有被完全删除。它在 Init/Abort 重排中仍需精确区分 content，因而 unary Abort 比流式控制面多保留一个 digest。这个例外是明确的 cleanup contract；若今后把它扩散回 telemetry 或 Stage，只会重新混淆内容围栏和 participant identity。

## 何时重新评估

1. 若引入 Backend process takeover、attempt migration 或 rolling upgrade，先裁决新的 lifecycle ownership 与 compatibility 协商；不得增加隐式 legacy field 或 fallback。
2. 若 Stage bundle 的 canonical digest 成为可测量的 admission 瓶颈，优化 canonical encoding 或缓存，但仍必须覆盖 ref、empty Stage 与完整 fragment 内容。
3. 若一个 execution attempt 不再是一组独立 Backend participant，重新验证 ref 的最小字段；不得退回 endpoint、ordinal 或 process-global registry。
4. 若需要 partial/append Stage 或 global atomic stage/start，建立独立启动协议 ADR；不得把 revision 偷渡进本 contract。
