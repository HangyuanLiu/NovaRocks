---
id: ADR-0126
title: "Terminal delivery uses participant attempt identity, not payload content identity"
domain: [distributed-query-lifecycle, crate-boundary, runtime-filter, provider-spi]
status: active
supersedes: [ADR-0113]
superseded-by: null
date: 2026-08-30
provenance:
  - "mechanism: terminal first-wins delivery identity convergence"
  - "discussion: 2026-08-30 participant-attempt identity direction"
code-anchors:
  - "idl/novarocks/service.proto (ParticipantAttemptRef, QueryControlTerminalAck, QueryTerminalSnapshot, TerminalizationProof, NegativeAttestation)"
  - "novarocks/proto-codec/src/lifecycle/manifest.rs (ParticipantAttemptRef)"
  - "novarocks/proto-codec/src/lifecycle/terminal/mod.rs (ParticipantTerminalOutcome)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/lease.rs (FrontendTerminalReceipt)"
  - "novarocks/backend/src/query_lifecycle/registry.rs (terminal_ack_from_control, schedule_terminal_fallback)"
---

## 问题

终止证据在 control stream 与 unary fallback 两条传输路径上重试时，应该用什么事实证明它属于同一个已分配 participant，并安全地释放 Backend 的有界 retained record？

## 背景与执行事实

`QueryExecutionId` 只标识一次查询尝试，不能区分该尝试中的不同 Backend 进程；endpoint 和 backend ordinal 又会随路由或拓扑变化，不能标识进程 incarnation。`BackendProcessId` 是 BE 启动时分配的稳定 process identity。因此 `(QueryExecutionId, BackendProcessId)` 是终止交付所需的最小 allocated participant identity。

此前的 terminal wire 把 execution、完整 backend identity、Init digest、snapshot version 与 canonical payload content digest 分散在 snapshot、proof、negative attestation 和 ACK 中。它让 payload fingerprint 同时承担了 delivery identity 的职责，也要求 stream ACK 与 unary accepted 走两种不同的释放表达。Frontend/Backend 各自缓存 content ID 还造成两端重复的 identity authority。

## 考虑过的选项

1. **保留 Init digest + content ID 作为 terminal fence**。它可精确区分 payload，但 payload 已由 first-wins store 以完整 typed outcome equality 保护；把它放进 ACK 会把“这是哪个 participant”与“首次 payload 是否相同”混为一层，并迫使 fallback 伪造 ACK。
2. **仅用 QueryExecutionId**。字段最小，但一个 attempt 可以有多个 Backend participant，foreign process 可以错误释放别人的 retained evidence，拒绝。
3. **使用 `ParticipantAttemptRef = (QueryExecutionId, BackendProcessId)`，并让 first-wins store 比较完整 outcome**。选择。terminal carriers 与 ACK 只携带该 ref；同 ref 的 exact outcome 才是 duplicate，异 payload 是 conflict。
4. **提前把 Attach/Stage/Start 也迁到 ref**。这会把 terminal 工作扩大成启动协议迁移并增加与并行 identity 工作的冲突面，拒绝；这些 carrier 保留各自已存在的 Init digest 合同，直到单独裁决。

## 裁决

新增 `ParticipantAttemptRef`，只在 terminal carriers、Frontend terminal receipt/store、Backend retained delivery、control ACK 与 unary fallback 使用。`QueryTerminalSnapshot`、`TerminalizationProof`、`NegativeAttestation` 和 `QueryControlTerminalAck` reserve 旧 identity 字段的 tags 与 names，wire 不保留 dual decode 或 fallback。

Frontend 在同一个 first-wins store 中处理 stream 和 unary outcome：验证 process/session 一致后，以完整 typed outcome equality 判 duplicate 或 conflict，**先写 receipt 再发送**携带 ref 的 ACK。Backend 仅接受 local process 的 matching ref；unary accepted/already-accepted 直接完成 matching retained delivery，而非伪造旧 ACK。conflict、gone、retry exhaustion 与 retention expiry 都是不同的可观察 delivery 结局；终止执行资源在 immutable record retained 后、交付前释放。

## 接受的妥协（诚实记录）

这次不再把 payload hash 当作 ACK key，因此 ACK 不能独立表达“我确认的是哪一份字节内容”。代价由 Frontend first-wins store 承担：它必须保留完整 typed outcome 并对重传做 equality/conflict 判定。这样增加了 FE 本地 retention 代码，却避免了两端各自哈希同一 payload、以及 fallback 再构造一个不真实 ACK 的第二协议。

选择 terminal-only 切片是为了降低与启动协议工作的冲突，不是因为 Init digest 永久适合 terminal 之外的所有 identity 需求。混合版本 FE/BE 不受本裁决支持；reserved tags 不是 rolling-upgrade compatibility contract。

## 何时重新评估

1. NID-2 迁移 Attach/Stage/Start 时必须复用这一 wire type，不得创建同义第二 ref；其字段扩展须单独裁决。
2. 若 proof/attestation outcome 的完整 equality 成为可测量的 retention 或 CPU 瓶颈，优化 role-local representation；不得重新把 content hash 放回 ACK。
3. 若需支持 mixed-version 或 rolling upgrade，先定义显式 compatibility/version negotiation；不得以 dual wire 或 digest fallback 偷渡。
4. 若 Backend process identity 的生成、替换或认证语义改变，重新审查 ref 是否仍覆盖 incarnation，而不是退回 endpoint 或 ordinal。
