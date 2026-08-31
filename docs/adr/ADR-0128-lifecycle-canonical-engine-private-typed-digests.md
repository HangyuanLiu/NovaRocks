---
id: ADR-0128
title: "Lifecycle canonical engine is private behind typed digest APIs"
domain: [distributed-query-lifecycle, runtime-filter, crate-boundary, provider-spi]
status: active
supersedes: [ADR-0127]
superseded-by: null
date: 2026-08-30
provenance:
  - "mechanism: lifecycle descriptor canonical engine governance closeout"
  - "discussion: 2026-08-30 lifecycle canonical governance"
code-anchors:
  - "novarocks/proto-codec/src/lifecycle/canonical.rs (digest_message, hash_message, CanonicalError)"
  - "novarocks/proto-codec/src/lifecycle/manifest.rs (ParticipantAttemptRef, ParticipantManifest::digest)"
  - "novarocks/proto-codec/src/lifecycle/stage.rs (StageDigest::compute)"
  - "novarocks/backend/src/query_lifecycle/registry.rs (QueryLifecycleRegistry::stage_fragments)"
---

## 问题

当 `ParticipantManifestDigest` 与 `StageDigest` 仍是 Init retry/Abort cleanup 与 immutable Stage/Start 的真实内容围栏时，如何让
descriptor-driven canonical engine 的 Rust 可见性准确反映其仅属于 query lifecycle 的实现所有权，而不再把任意 protobuf message 的
generic hasher 暴露为新的公共 contract？

## 背景与执行事实

ADR-0127 已裁决 `ParticipantAttemptRef` 是 post-Init participant identity，Init manifest digest 仍用于 same-ref exact retry、
different-manifest conflict 与 unary Abort cleanup，StageDigest 则以 participant ref 与完整稳定排序 Stage bundle 围栏 exact retry、
conflict 和 Start gate。这些 lifecycle fence 的输入、字节、错误时序和 retained owner 仍然有效。

`novarocks-proto-codec` 曾从 crate root 导出 `canonical` module，其中 `digest_message` 接受 caller-provided domain 和 generated
message name。实际 production caller 只有 `ParticipantManifest::digest` 与 `StageDigest::compute`；Frontend、Backend 与其它 codec
消费的是这两个 validated typed value，而不是 generic helper。descriptor reflection 仍是必要实现：它覆盖完整 generated message、按
field number 投影、按 typed map key 排序、保留 ordinary repeated order、区分 optional/oneof presence，并拒绝 non-finite float。

`CatalogVersion`、`NativeCompatibilityId`、runtime-filter artifact schema/order/hash、provider-signed row-mutation identity 与 durable
canonical formats 都有独立 owner 和生命周期。它们不应通过 lifecycle helper 统一，也不因本裁决而改变。`provider-spi` 被保留为
README 检索域，只表示 provider admission/lifecycle boundary 必须继续消费 typed lifecycle facts；它不赋予 SPI generic canonical
engine 或新的 provider digest authority。

## 考虑过的选项

1. **保留 crate-root generic canonical API。** 调用方便，但任何 sibling 或未来 caller 都能绕过 carrier identity、retry、conflict、
   cross-message reference 和 retention 设计，自行创建看似合法的 digest contract，拒绝。
2. **删除 canonical engine 或改用普通 protobuf encoding。** 会失去 map/repeated/presence/finite-value 的已验证语义，破坏 Manifest
   和 Stage 的 existing fence，拒绝。
3. **为 Manifest 和 Stage 各复制一份手写 projection。** 可隐藏 generic module，但会丢掉 generated-field coverage 或制造两份排序/
   framing authority，拒绝。
4. **把一个共享 descriptor engine 放在 private lifecycle child module，只有 typed lifecycle API 调用它。** Rust privacy 机械阻止
   crate root 与 sibling codec 命名 helper，同时保留单一算法和既有 bytes，选择。

## 裁决

ADR-0127 被本 ADR supersede。`novarocks-proto-codec::lifecycle` 私有拥有 descriptor-driven canonical engine；它不是 crate-root
export，也不是其它 codec、Frontend、Backend、SPI 或 Connector 的 reusable extension point。low-level `CanonicalError`、
`digest_message` 与 `hash_message` 只对 lifecycle parent/descendants 可见。

稳定消费面只有 `ParticipantManifest::digest() -> ParticipantManifestDigest` 和
`StageDigest::compute(ParticipantAttemptRef, &[StageFragment])`。两者继续使用同一 engine，保持 domain separator、outer framing、
field/map/repeated/presence 规则、error mapping 与 digest bytes。Backend 继续在 lifecycle entry 副作用前使用 typed values 判定
exact retry 或 conflict；Frontend 继续 freeze/retain/compare typed values。此裁决不修改 IDL、descriptor、compatibility epoch/revision，
也不引入 mixed-version、fallback、dual decoder 或 all-in-one path。

## 接受的妥协（诚实记录）

engine 不会消失，且仍依赖 descriptor reflection、protobuf encode/decode 和 map sorting；这是为了让新增 generated field 自动进入
Manifest/Stage fence，并保持既有 semantic ordering，而不是为了获得 CPU、内存或 wire 性能收益。公开 Rust surface 会收窄：假设中的
第三方 direct caller 将无法编译，但 NovaRocks 当前没有将该 generic helper 承诺为 third-party extension contract。

private module 是 crate 内所有权治理，不是 crate graph 隔离的替代品；其它领域的 identity 仍靠其 own crate/domain contract 维护。
我们接受 review 时仍需理解少量 shared internal algorithm，换取不把“任意 message + 任意 domain”的工具误示为默认安全机制。

## 何时重新评估

1. 若出现第三个 lifecycle carrier，先裁决其 allocated identity、retry、semantic conflict、cross-message reference、retention 与
   process replacement；只有满足这些条件才可在 lifecycle owner 下增加新的 typed API。
2. 若 canonicalization 成为可测量的 lifecycle admission bottleneck，可优化 internal representation 或 cache，但必须维持 full-field
   coverage、Manifest/Stage exact bytes 和 error contract。
3. 若 NovaRocks 要把 `novarocks-proto-codec` 作为第三方 public library 发布，单独定义 semver/public extension policy；不得隐式恢复
   unrestricted generic hasher。
4. 若 IDL、descriptor、compatibility island、CatalogVersion、RF artifact、provider-frozen 或 durable canonical contract 需要变化，建立
   其 owner 的独立 ADR，不能在本 implementation-visibility 决策中偷渡。
