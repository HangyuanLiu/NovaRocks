---
id: ADR-0119
title: "Connector read SPI runtime and wire codec separation"
domain: [provider-spi, distributed-query-lifecycle]
status: superseded
supersedes: [ADR-0114]
superseded-by: ADR-0123
date: 2026-08-28
provenance:
  - "discussion: 2026-08-27 connector read SPI/runtime and wire codec separation"
  - "PR: <backfill after merge>"
code-anchors:
  - "novarocks/spi/src/connector/read_stack/runtime/mod.rs (ConnectorReadMetadata and execution SPI)"
  - "novarocks/spi/src/connector/read_stack/adapter/mod.rs (private concrete-type erasure adapter)"
  - "novarocks/proto-codec/src/connector_read/runtime_codec.rs (closed carrier codec)"
  - "novarocks/connector/iceberg/src/typed_read/codec/mod.rs (Iceberg concrete codec)"
  - "novarocks/frontend/src/connector/typed_control_registry.rs (exact-generation read control lease)"
  - "novarocks/backend/src/connector/execution_host.rs (exact-generation execution bundle)"
---

## 问题

在 FE/BE 不直接依赖具体 Connector、SPI 又不能依赖网络 DTO 的前提下，Connector read 的真实 handle、split 与 transaction 应在哪里完成具体化和 wire 编解码，才能避免 provider 判断侵入核心角色？

## 背景与执行事实

ADR-0114 正确地采用 closed IDL carrier、runtime split delivery、exact replay 与 page-source 模型，但把 metadata、split manager、page-source factory 等内部业务接口放入了 `novarocks-proto`。这使 proto crate 同时是网络 carrier owner 和运行时业务 SPI owner，名称与依赖方向都不再反映职责：provider 的真实类型被迫先转换为 wire handle 才能参与正常 metadata、pushdown 与 split planning。

生产约束没有变化：IDL 仍是 closed `oneof`，不存在 opaque provider bytes；FE/BE 不链接 provider；Server 是唯一同时链接角色与 Iceberg 的组合根；exact-generation admission、TaskUpdate replay、canonical order、ControlReady 和 BE lifecycle 语义必须保持。普通 metadata、pushdown、split planning 与 page-source 调用不是网络边界，不应做 codec round trip。

## 考虑过的选项

1. 保留 proto 的 role-facing business traits。改动最小，但内部 SPI 继续依赖网络 carrier，职责混合没有消除。
2. FE/BE 直接持有 Iceberg 的具体类型或在角色中按 provider downcast。类型表面最直接，但 provider 分支会进入核心逻辑。
3. 在 SPI 公开 `Any` 或 provider payload。能避免 codec round trip，但角色获得 downcast 通道，公共 API 失去可审计性。
4. SPI 定义内部 read vocabulary，私有 generic adapter 封装 concrete 类型；proto-codec 只定义 carrier codec；每个 provider 实现自己的 codec；role 按 exact binding 解析一组 services+codec。选择此方案。

## 裁决

`novarocks-spi` 拥有 transport-neutral read runtime：opaque read table/column/transaction/split handles、relation、constraint、dynamic filter、metadata、split source、page-source provider 和 provider factory。其 public 语义不依赖 `proto-models` 或 `proto-codec`。只有 SPI 内部的 generic adapter 使用 private `Any` 存储，并在同一 provider adapter 中按 exact binding 恢复关联类型；role 既不能取 payload，也没有公开 downcast。

`novarocks-proto-codec` 只拥有 IDL carrier 的结构校验、canonical encoding、received scheduled-split replay evidence 与 `ConnectorReadCodec`/execution-bundle contract。codec 只在真正 FE/BE wire ingress、egress 处工作。FE 的 metadata、predicate/projection/limit pushdown 和 split planning，BE 的 page-source 创建，都传递 SPI values，不调用 codec。Iceberg 同时拥有 concrete runtime adapter 与 concrete codec，负责一次把自己的真实类型同 closed carrier 对应。

FE 的 `ConnectorReadControlRegistry` 以完整 metadata/split-manager/codec bundle 和弱注册 lease 按 exact generation 保存；connector generation 保留强 lease，旧新 generation 可以并存，旧 lease drop 只能删除自己的 ticket。BE execution host 以同一 exact key 安装 factory+codec bundle。Server 只把具体 Iceberg factory 组合进角色公开的抽象接口，FE/BE 不取得 provider dependency。

原有 wire contract 保持：wire `TupleDomain` 按 canonical validated-column protobuf bytes 排序，Iceberg 内部 domain 使用其真实 column handle 的 `Ord`；assignment 维持业务顺序；TaskUpdate exact replay 使用收到的 scheduled split canonical evidence；queue 先作 Closed/SequenceConflict/AfterNoMore preflight，再 decode 新 split 并原子投递。BE 的 provisional context 只在已有 lifecycle admission 成功后发布，live runtime filter 仍使用保守 oracle 与 sticky error/close 规则。

## 接受的妥协（诚实记录）

- 一个 provider 现在需要同时维护 adapter 和 codec。adapter 做本进程真实类型转换，codec 做受限网络 carrier 转换；合并会让普通业务调用重新依赖 wire。
- SPI opaque handle 的比较通过私有 erased comparator 委托给 provider concrete `Ord`。这比公开 payload 更复杂，但不用 wire bytes、指针或 debug 文本伪造业务排序。
- 只支持静态链接、同仓原子发布；没有 plugin ABI、mixed-version negotiation 或 fallback decoder。需要这些能力时必须另行设计兼容协议。
- ADR-0114 关于 closed carrier、runtime split delivery、page-source lifecycle 和 canonical replay 的结论保留；被替代的是 proto 承担内部业务 SPI 的 crate ownership 决定。

## 何时重新评估

- 第二个 connector 需要 read runtime 时，以其真实 adapter/codec 检查当前 SPI vocabulary 是否仍中立。
- 若要求独立动态加载 connector 或滚动 mixed-version，重新设计 provider ABI 与 wire negotiation，不能复用 private `Any` adapter。
- 若 codec 在非 wire 业务路径出现，先补 SPI vocabulary；不得用额外 codec round trip 作为快捷修复。
- 若 exact-generation bundle 的 lease 或 BE lifecycle 被要求跨进程持久化，重新评估 owner，而不是把 registry 变成 durable provider authority。
