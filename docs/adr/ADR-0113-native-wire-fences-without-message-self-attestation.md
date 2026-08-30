---
id: ADR-0113
title: "Native wire fences without message self-attestation"
domain: [distributed-query-lifecycle, crate-boundary, runtime-filter, provider-spi]
status: superseded
supersedes: [ADR-0106]
superseded-by: ADR-0126
date: 2026-08-26
provenance:
  - "mechanism: native lifecycle self-attesting digest removal with retained cross-message echo fences"
  - "discussion: 2026-08-26 native wire digest classification and self-attestation removal"
code-anchors:
  - "idl/novarocks/service.proto (InitQueryRequest, StageFragmentsRequest, RuntimeFilterContribution, ParticipantManifest)"
  - "novarocks/proto/src/lifecycle/control.rs (QueryInitRequest::parse)"
  - "novarocks/proto/src/lifecycle/stage.rs (QueryStageRequest::parse)"
  - "novarocks/proto/src/lifecycle/manifest.rs (ParticipantManifest::digest)"
  - "novarocks/proto/src/error.rs (ProtocolErrorKind)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/barrier.rs (init_one, validate_stage_ack)"
  - "novarocks/frontend/src/native/transport.rs (LifecycleTransport ACK echo validation)"
  - "novarocks/frontend/src/runtime_filter/install_encoder.rs (encode_participant)"
  - "novarocks/backend/src/runtime_filter/install_decode.rs (decode_runtime_filter_contribution)"
  - "novarocks/backend/src/query_lifecycle/entry.rs (retained manifest digest and stage digest)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/manifest.rs (MaterializedParticipant)"
  - "novarocks/frontend/src/coordinator/query_lifecycle/lease.rs (Frontend retained terminal content identity)"
  - "novarocks/backend/src/runtime_filter/observation.rs (Backend runtime-filter correctness owner)"
  - "novarocks/proto-models/build.rs (generated DTO and descriptor owner)"
  - "novarocks/proto/src/connector.rs (shared connector codec and DTO digest)"
---

## 问题

同一个 `digest` 名词在 native lifecycle wire 上混着三类完全不同的事实：**消息自证**、**跨消息内容引用**、**格式与资源边界**。
其中「消息 M 携带字段 d、d 的全部派生输入都在 M 内、接收方重算 d 再与携带值比对」这一类，究竟提供了什么单靠接收方
自身派生得不到的检测能力？如果答案是「没有」，把它从 wire 上删掉是削弱了 fence，还是删掉了一份伪装成 fence 的冗余？

## 背景与执行事实

### 判据

**消息自证（self-attestation）**：消息 M 携带字段 d，d 的全部派生输入都包含在 M 内部，接收方重算 d 并与携带值比对。
这与「M 携带的 d 派生自**另一条**消息的内容」（跨消息引用）在结构上不同——后者的输入不在 M 内，接收方无法独立重建。

### 本裁决前 native lifecycle wire 上的三处自证

1. **`InitQueryRequest.init_digest`**（`idl/novarocks/service.proto`）。它曾是同一条消息里 `manifest` 字段的 digest。
   `novarocks/proto/src/lifecycle/control.rs` 的 `QueryInitRequest::parse` 调用 `manifest.digest()` 重算后与携带值比对。
2. **`StageFragmentsRequest.stage_digest`**。它曾是同一条消息里 `execution_id`、`init_digest` 与 `fragments` 的
   canonical 投影。`novarocks/proto/src/lifecycle/stage.rs` 的 `QueryStageRequest::parse` 调用 `StageDigest::compute_v1`
   重算后比对。
3. **`RuntimeFilterContribution.contribution_digest`**。FE 侧 `novarocks/frontend/src/runtime_filter/install_encoder.rs`
   的 `encode_participant` 用 `participant_id / lifecycle / install` 组装 install envelope 并哈希；BE 侧
   `novarocks/backend/src/runtime_filter/install_decode.rs` 的 `decode_runtime_filter_contribution` 用**同一条消息里的
   同样三个字段**加上外层 execution identity 重建同一 envelope、重算哈希、再比对。

### 为什么这三处可以删除

- **无认证价值**：这是 unkeyed SHA-256。能够修改消息内容的一方同样能重算并替换 digest，比对必然通过。它不构成
  tamper evidence。
- **接收方无信息增益**：d 的全部输入都在 M 内，接收方能自行派生 d，携带值不提供任何它得不到的事实。
- **接收方本来就在派生**：上述三处的重算调用在删除前都已存在于生产路径。删除携带值后这些派生**照常发生**，只是不再与
  一份冗余副本比对，因此不增加计算成本；第 3 处甚至净减少一次全量哈希（FE 生产端那次）。

### 为什么删除不降低检测能力

BE 从消息内容派生出的值会写进 ack 回显，FE 用自己保留的派生值比对：

- Init：`novarocks/frontend/src/coordinator/query_lifecycle/barrier.rs` 的 `init_one` 把 `InitQueryResponse.init_digest`
  与 `MaterializedParticipant.digest` 比对；`novarocks/frontend/src/native/transport.rs` 的 `LifecycleTransport` 在
  transport 层做同样的 identity/digest 回显校验。
- Stage：`barrier.rs` 的 `validate_stage_ack` 把 `StageFragmentsResponse` 的 `(stage_digest_version, stage_digest)`
  与请求侧值比对。

若内容在途损坏，BE 从损坏内容派生出 D′ ≠ FE 保留的 D，回显比对在 **FE 侧**失败。检测点因此从「BE 进程内自洽」
**升级**为「FE↔BE 端到端、覆盖完整消息内容」——自证只能证明消息与自己一致，回显能证明两端看到的是同一份内容。

`contribution_digest` 不需要替代检测：`RuntimeFilterContribution` 是 `ParticipantManifest` 的 field 8，而
`novarocks/proto/src/lifecycle/manifest.rs` 的 `ParticipantManifest::digest()` 已改为 descriptor 全遍历
（`canonical::digest_message` 覆盖完整生成消息，新 schema 字段自动进入 fence，不再依赖手写投影）。contribution 的内容
因此已被外层 `init_digest` 的端到端回显链路完整覆盖——`contribution_digest` 是**嵌套在已覆盖内容内部的双重冗余**。

### 与自证共存的另外两类事实

- **跨消息内容引用**：`StageFragmentsRequest.init_digest`、`AbortQueryRequest.init_digest`、
  `QueryControlAttach.init_digest`、`FragmentLiveObservation.init_digest`、terminal 各消息的 `init_digest`、
  `QueryControlTerminalAck.snapshot_digest`、`StartPreparedQueryRequest.stage_digest`（Start fence）以及 Init/Stage/Start
  response 的回显。这些字段的派生输入**不在**携带它的消息内，接收方无法独立重建，删掉就真的失去 fence。
- **格式与资源边界**：required presence、enum 合法性、version、fixed-width identity、字符串/集合/payload 上界。
  这些与 digest 无关，也不因本裁决改变。

### 承接的执行事实（来自 ADR-0106，未被本裁决改变）

IDL 仍是 FE/BE 之间的唯一 schema authority。`novarocks-proto-models` 拥有 generated DTO、descriptor set 与 schema
ledger；`novarocks-proto` 拥有 canonicalization、canonical digest、`ProtocolError + FieldPath`、validated wire values 与
shared connector codec。`ParticipantTerminalOutcome` 的 canonical content ID 不在 message 内，由 FE/BE 各自在首次
admission/retention 时计算一次并与 immutable outcome 一起缓存。Backend runtime-filter observation 是 domain state 的
唯一 owner。

## 考虑过的选项

1. **保留自证不动**。改动成本为零，且「有 digest」在阅读上显得更安全。但它不能抵抗任何有能力改内容的一方，也不给
   接收方任何新事实；保留它的真实代价是让后来者把「消息自带 digest」当成一种可复制的 fence 模式向新 message 扩散，
   并让 `digest` 这个名词继续覆盖三类语义不同的事实、无法在 review 中被区分。
2. **用 keyed MAC（HMAC）替代 unkeyed digest**。这确实能把自证变成真正的 tamper evidence。拒绝的理由是**职责归属**
   而非成本：证明 caller 属于本 deployment 是 transport trust 的职责，已由 ADR-0110 裁决的 mandatory JWT caller
   authentication 承担。在 lifecycle message 内再做一套 keyed integrity，等于在 Protocol 层新建第二套密钥分发、轮换与
   失败语义，与 transport 层形成双权威——这正是 ADR-0106 划分 wire owner 时要避免的形态。
3. **删除自证，依赖跨消息回显比对 + Protocol 的格式/资源校验**。选择此方案。派生照常发生，检测点从进程内自洽升级为
   跨进程端到端，`digest` 名词收敛为「跨消息内容引用」单一含义。
4. **把所有 digest 字段一并删除**。诱人之处是名词彻底消失。但跨消息引用的派生输入不在携带消息内，删除后 Stage 无法
   证明自己属于哪个 Init、Start 无法证明自己属于哪个 Stage、terminal ack 无法证明释放的是哪份 outcome——这会把真实的
   fence 一起删掉。拒绝。

## 裁决

**删除三处消息自证字段**：`InitQueryRequest.init_digest`、`StageFragmentsRequest.stage_digest`、
`RuntimeFilterContribution.contribution_digest`。在 IDL 中 reserve 它们的 tag 编号与字段名，永不复用。相应地删除
`QueryInitRequest::parse` 与 `QueryStageRequest::parse` 中的重算比对分支，以及 `encode_participant` 的 FE 生产端哈希与
`decode_runtime_filter_contribution` 的 BE 比对分支。

**保留全部跨消息内容引用**：`InitQueryResponse.init_digest`、`StageFragmentsRequest.init_digest`、
`AbortQueryRequest.init_digest`、`QueryControlAttach.init_digest`、`FragmentLiveObservation.init_digest`、terminal 各消息的
`init_digest`、`QueryControlTerminalAck.snapshot_digest`、`StartPreparedQueryRequest.stage_digest` 及 Stage/Start response
的回显字段。它们继续是各自阶段的 fence，语义不变。

**`StageFragmentsRequest.stage_digest_version` 保留**。它指示派生投影的版本而非内容自证；且 Start 与 Ack 本来就以
`(version, digest)` 成对携带，删掉 version 会让 Start fence 失去版本协商入口。

**派生值只 retain 在 role-local 记录**：BE 侧 `novarocks/backend/src/query_lifecycle/entry.rs` 的 `QueryLifecycleEntry`
已有的 manifest `digest` 与 `stage_digest: Option<StageDigest>`；FE 侧
`novarocks/frontend/src/coordinator/query_lifecycle/manifest.rs` 的 `MaterializedParticipant.digest`。**不得**把派生值
缓存进 validated Protocol wrapper——这继承 ADR-0106 对「retained record 的 lifecycle 与资源释放是角色私有状态」的约束。

**删除 `ProtocolErrorKind::DigestMismatch`**（`novarocks/proto/src/error.rs`）。`QueryStageRequest::parse` 是它在生产代码
中的唯一生产者，自证删除后它成为无生产者变体。`novarocks/frontend/src/native/report_server.rs` 与
`novarocks/backend/src/query_lifecycle/rpc.rs` 中的 `Conflict | DigestMismatch` 映射折叠为 `Conflict`。
`BackendInstallPolicyError::ContributionDigestMismatch` 是 Backend 领域内部的另一件事，**不在本裁决范围内，保留**。

**Protocol 的其余校验面不变**：required presence、enum、version、fixed-width identity、字符串/集合/payload 上界全部保留。
`ParticipantManifestDigest` 与 `TerminalOutcomeContentId` 的身份语义不变。

**承接 ADR-0106 中未被改变的全部裁决**：`novarocks-proto-models` 是 repository-level generated DTO、descriptor set 与
schema ledger 的唯一 owner，只依赖 Prost 层；`novarocks-proto` 只依赖 Models、Types、SPI，拥有 canonicalization、
canonical digest、`ProtocolError + FieldPath`、validated wire values 与 shared connector encode/decode，不 re-export
generated modules，也不依赖 Tonic、Execution 或角色 crate；FE/BE 直接依赖 Models 与 Proto，Tonic stub 仍分别在角色本地
生成。SPI 继续是 transport leaf，拥有 Connector domain declaration、binding key 与 instance-id grammar。Types 拥有
`AttemptId` 与 `QueryExecutionId`，Protocol 只拥有该 identity 的 wire conversion。错误一律以 `ProtocolError` 加精确
`FieldPath` 表达，角色负责映射为自己的 Tonic status 或 typed outcome，不能抹平成 string-only error。
`ParticipantTerminalOutcome` 的 canonical content ID 不进 message，由角色本地 compute-once 并与 immutable outcome 一起
缓存，retransmit、fallback、conflict 与 ACK release 只读缓存值。Backend runtime-filter observation 是 domain state 的唯一
owner：unknown identity、sequence regression、terminal conflict、apply-before-delivery、invalid row effect、
acknowledgement exceeding delivery、partition conflict 与 counter overflow 均为 first-wins sticky correctness evidence，
terminal capture 将其收敛为 negative-attestation path；Protocol 不重复检查 Backend 已折叠的 state、cross-reference、
ordering 或 derived counters。

## 接受的妥协（诚实记录）

**失去了针对畸形构造的即时本地拒绝点。** 自证比对是接收方在解析当场就能给出的一次拒绝；删除后，一个内容在途损坏的
Init/Stage 请求会被 BE 正常受理，直到 FE 拿到 ack 回显并比对才失败。检测**没有变弱**（覆盖面反而更完整），但检测**变晚**
了一个跨进程 round-trip，失败路径也从「BE 立即返回 InvalidValue」变成「FE 判定 ack 回显不匹配」。诊断信息的现场性因此
下降：BE 日志不再指出是哪个字段对不上。

**`contribution_digest` 删除后，某些畸形输入的拒绝点发生了位移。** 原先该比对会先于
`decode_participant_install` 失败并给出「digest 不匹配」；删除后这类输入改由 `decode_participant_install` 自身的结构与
取值校验拒绝。这是拒绝**理由**的变化而非拒绝**能力**的丢失，但错误文本会不同，依赖旧文本的诊断习惯需要重建。

**这是一次收窄，不是一次否定。** ADR-0106 的裁决段写有「Init/Stage digest 及 `QueryControlTerminalAck.snapshot_digest`
继续是各自已有的 fence」。本 ADR 保留它们作为跨消息 fence，只删除 Init/Stage 请求中那份覆盖自身内容的副本。之所以走
supersede 而不是「补充说明」，是因为 ADR 一旦合入不改实质内容；把这条留在原文里而在别处澄清，会让两份文档对同一句话
给出不同读法。

**承接 ADR-0106 已记录的妥协。** Models/Proto split 增加 manifest、extern path 与两条 FE/BE direct dependency，canonical
codec 不再是极小的 codegen facade——接受它是为了机械证明 SPI、Types、SQL、Execution、StateStore 与 connector
implementations 不被 wire 依赖污染，而不是因为 crate 更少。FE/BE 分别缓存同一 content ID 仍有两次跨进程计算，这不是全局
identity service，也不保证跨版本互通。runtime-filter negative attestation 会使 terminal verdict 失败并降低可用性，这是为
避免把错误的 Backend 状态机输出当作可信证明，而不是把 runtime-filter 优化提升为 SQL 语义 owner。

## 何时重新评估

1. **不得恢复 self-carried digest。** 若 terminal content-ID 或 manifest digest 的 hashing 成为已测瓶颈，可在同一角色的
   retained boundary 优化 canonicalization；不得把 digest 塞回 message，也不得跨角色共享 mutable cache。本条是 ADR-0106
   第 3 条的等价约束，删除自证后它同时覆盖 Init、Stage 与 runtime-filter contribution。
2. **若出现没有 ack 回显路径的新 carrier**，其自证删除需要单独论证。本裁决的检测能力结论依赖「接收方派生值会回显、
   发送方保留派生值并比对」这一闭环；单向、fire-and-forget 或无对端保留态的 carrier 不满足该前提，不能直接套用结论。
3. **若需要真正的 tamper evidence 进入 lifecycle message 层**（例如出现 transport trust 无法覆盖的中间转发角色），
   先裁决它与 ADR-0110 的 caller authentication 是同一套密钥权威还是新增权威，再决定字段形态；不得以「补回 digest」的
   形式绕过这次裁决。
4. **若需要独立发布 Models、mixed-version FE/BE 或 rolling upgrade**，先定义 schema/digest version negotiation 与
   compatibility window；不能从 crate split 或 reserved tag 推断兼容性。
5. **若新的 carrier 需要超出 SPI domain constructor 与 generated DTO 的真实执行事实**，先证明不会引入第二 validation 或
   digest authority，再裁决是否需要新的 owner crate。
6. **若 runtime-filter observation 需要事件级 durable audit**，新增独立 telemetry retention owner；不得把原始事件回流给
   Protocol 或 Frontend registry。
