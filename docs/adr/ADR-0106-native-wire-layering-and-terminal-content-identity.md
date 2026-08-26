---
id: ADR-0106
title: "Native wire layering and terminal content identity"
domain: [crate-boundary, distributed-query-lifecycle, runtime-filter, provider-spi]
status: superseded
supersedes: [ADR-0078, ADR-0079, ADR-0098, ADR-0105]
superseded-by: ADR-0113
date: 2026-08-25
provenance:
  - "mechanism: native wire generated-model split, shared codecs, and retained terminal content identity"
  - "discussion: 2026-08-25 native wire crate layering and terminal correctness ownership"
code-anchors:
  - "novarocks/proto-models/build.rs (generated DTO and descriptor owner)"
  - "novarocks/proto/src/connector.rs (shared connector codec and DTO digest)"
  - "novarocks/backend/src/runtime_filter/observation.rs (Backend runtime-filter correctness owner)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/lease.rs (Frontend retained terminal content identity)"
  - "novarocks/backend/src/query_lifecycle/entry.rs (Backend retained terminal content identity)"
---

## 问题

当 native FE/BE wire 同时需要 generated DTO、纯 codec、SPI domain conversion、字段化错误、terminal identity 与
runtime-filter terminal facts 时，怎样让每种事实只有一个 owner，而不让 SPI/低层 crate 反向依赖 wire、让 Proto
承担角色状态机，或用 self-carried digest 制造第二份 identity？

## 背景与执行事实

IDL 仍是 FE/BE 之间的唯一 schema authority，但 code generation、descriptor/ledger 与手写 codec 的变更频率和
依赖约束不同。把两者都塞进一个 `novarocks-proto` 会迫使所有 DTO consumer 带上 codec 的领域依赖；反过来让
SPI 持有 generated declaration 或让 FE/BE 各自重做 connector codec，会让传输和领域的边界重新变成双权威。

terminal message 曾携带自身 digest。该 field 并不能防止同一角色在 retained record、fallback、conflict 和 ACK
路径上反复 hash；更糟的是 digest 本身成为需要由 payload 再证明的字段。runtime-filter observation 也曾只作为
无否决 telemetry，这掩盖了 Backend 已观察到的 identity、sequence、state 或计数矛盾。

## 考虑过的选项

1. 保持单一 Protocol crate 既生成 DTO 又承载所有 codec。路径改动最少，但 generated consumer 被迫获得 codec
   依赖，无法用依赖闭包证明低层 crate 不碰 wire。
2. 让 SPI/Types 接受 Protocol 依赖，或新增共享 domain adapter crate。前者反转 leaf 依赖；后者形成额外长期
   conversion/validation owner。
3. 把 generated DTO 与纯 codec 分层，角色只保留本地状态；terminal identity 在角色 admission 时计算一次并缓存；
   Backend 对自己生产的 runtime-filter facts负责 correctness。选择此方案。
4. 继续在 terminal message 携带 self digest，并把 runtime-filter domain checks 保留在 Protocol。前者增加
   payload 自证和重复 hash，后者使输出格式 owner 重复实现 Backend state machine。

## 裁决

`novarocks-proto-models` 是 repository-level generated DTO、descriptor set 与 schema ledger 的唯一 owner；它只依赖
Prost 层。`novarocks-proto` 只依赖 Models、Types、SPI，拥有 canonicalization、canonical digest、`ProtocolError +
FieldPath`、validated wire values，以及 shared connector encode/decode。它不 re-export generated modules，也不依赖
Tonic、Execution 或角色 crate。FE/BE 直接依赖 Models 与 Proto；Tonic stub 仍分别在角色本地生成。

SPI 继续是 transport leaf，拥有 Connector domain declaration、binding key 与 instance-id grammar；Protocol 的 shared
connector codec 在 FE/BE application boundary 完成 DTO/domain conversion，并从原始 DTO 计算 canonical digest。Types
拥有 `AttemptId` 和 `QueryExecutionId`；Protocol 只拥有该 identity 的 wire conversion。错误一律以 Protocol 的
`ProtocolError` 和精确 `FieldPath` 表达，角色负责把它映射为自己的 Tonic status 或 typed outcome，不能抹平成
string-only error。

删除 `QueryTerminalSnapshot.digest`、`TerminalizationProof.digest` 与 `NegativeAttestation.digest` 并 reserve 它们的
tag/name。`ParticipantTerminalOutcome` 的 canonical content ID 不在 message 内：FE/BE 各自在首次 admission/retention
时计算一次，和 immutable outcome 一起缓存；retransmit、fallback、conflict 与 ACK release 只读取该缓存值。Init/
Stage digest 及 `QueryControlTerminalAck.snapshot_digest` 继续是各自已有的 fence。

Backend runtime-filter observation 是 domain state 的唯一 owner。unknown identity、sequence regression、terminal
conflict、apply-before-delivery、invalid row effect、acknowledgement exceeding delivery、partition conflict 和 counter
overflow均 first-wins sticky correctness evidence；terminal capture 将其收敛为 negative-attestation path。Protocol 只
保留 generated message 的 format、enum、identity、version、cardinality 和 budget validation，不重复检查 Backend
已经折叠的 state、cross-reference、ordering 或 derived counters。

## 接受的妥协（诚实记录）

Models/Proto split 增加 manifest、extern path 和两条 FE/BE direct dependency，canonical codec 也不再是极小的
codegen facade。我们接受它不是因为 crate 数量更少，而是因为需要机械证明 SPI、Types、SQL、Execution、StateStore
与 connector implementations 不会被 wire 依赖污染。

FE/BE 分别缓存同一 content ID，仍会有两次跨进程的计算；这不是全局 identity service，也不保证跨版本互通。
我们选择本地一次计算而非把 digest 塞回 wire，是因为 retained record 的 lifecycle 和资源释放是角色私有状态。

runtime-filter 不影响 SQL 的行集语义，但 Backend 已生产出互相矛盾的 terminal correctness facts 时不能把它伪装成
optional P2 telemetry。negative attestation 会使 terminal verdict 失败并降低可用性；这是为了避免将错误的 Backend
状态机输出当作可信证明，而不是把 runtime-filter 优化提升为 SQL 语义 owner。

## 何时重新评估

1. 若需要独立发布 Models、mixed-version FE/BE 或 rolling upgrade，先定义 schema/digest version negotiation 与
   compatibility window；不能从 crate split 推断兼容性。
2. 若新的 carrier 需要超出 SPI domain constructor 和 generated DTO 的真实执行事实，先证明不会引入第二 validation
   或 digest authority，再裁决是否需要新的 owner crate。
3. 若 terminal content-ID hashing 成为已测瓶颈，可在同一角色 retained boundary 优化 canonicalization；不得恢复
   self-carried digest 或跨角色共享 mutable cache。
4. 若 Runtime Filter observation 需要事件级 durable audit，新增独立 telemetry retention owner；不得把原始事件
   回流给 Protocol 或 Frontend registry。
