---
id: ADR-0131
title: "Server composes provider role bindings from role-local resources"
domain: [provider-spi, runtime-role]
status: superseded
supersedes: []
superseded-by: ADR-0132
date: 2026-09-01
provenance:
  - "discussion: 2026-09-01 StarRocks connector role-binding convergence"
  - "implementation: PR #1016"
code-anchors:
  - "novarocks-server/src/composition.rs (compose_frontend_control_role_factories)"
  - "novarocks-server/src/connector_role_binding.rs (StarRocksControlRoleBindingFactory)"
  - "novarocks-server/src/app_config/starrocks_binding_registry.rs (StarRocksLocalBindingRegistry)"
  - "novarocks/frontend/src/catalog_application/frontend_port.rs (FrontendCatalogApplicationPort::reconcile_snapshot_with_page_size)"
---

## 问题

当 provider 的 control runtime 需要 endpoint、credential 与远端 client，而 catalog desired state 必须可持久化、可投递且不能泄露 process-local secret 时，生产 Server 应如何把每个角色的完整 binding 组装进同一 lifecycle，避免 provider 反向依赖 binding host 或借助默认 metadata source？

## 背景与执行事实

ADR-0130 已要求每个角色只发布一个 complete connector generation，但 Server 原先只为 Iceberg 装配 FE control factory；StarRocks 的 factory 仍只能由测试注入单一 metadata source 和 local binding。这样不能从真实 catalog definition materialize，且会诱使实现把 endpoint 或 credential 放进 durable catalog property，或添加 process-global registry。

`StarRocksLocalBindingRegistry` 在 FE startup 由 closed Server configuration 构造 immutable remote metadata source；它只以 `StarRocksLocalBindingRef` 解析资源。`StarRocksControlRoleBindingFactory` 从 catalog 的 execution property 读取唯一 `local_binding`，解析失败、缺失、重复或未配置时均返回 `InvalidDefinition` 和 `UntilDefinitionChanges`。构造 registry 与 materialize binding 都不访问远端 metadata；后者仅把已解析的 source 放入 complete control generation。Backend 仍只装配 capability-free StarRocks execution factory，typed read/write 继续显式为 `None`。

Catalog bootstrap 的 serving barrier 也必须与这一 owner 边界一致：先完成一次 authoritative snapshot enumeration 和缺失 projection retirement，再让 snapshot 的每个 exact key 被 scheduler 接收，随后 bootstrap 即可完成。provider materialization、remote metadata I/O、backoff 和首次完成结果都是该 barrier 之后的 role-local background work；早先要求 bootstrap 等待首次 provider attempt 的做法不再成立，因为一个挂起 catalog 会阻塞无关 catalog 与整个 FE serving lifecycle。

## 考虑过的选项

1. **让 StarRocks provider crate 直接依赖 connector-binding 并在其中导出工厂。** 调用方便，但 provider 会反向依赖 host 的 lifecycle 与其 transitive wire/model 依赖，破坏 provider boundary；拒绝。
2. **把 endpoint、username、password 放进 catalog desired state 或 fragment carrier。** catalog 可自包含，但 secret 会进入 durable state、日志和跨进程边界，且不能表达角色本地差异；拒绝。
3. **用全局 registry、未命名的默认 binding，或找不到引用时降级为 `None`。** 看似减配置，但会让 catalog definition 的实际 owner 不可追踪，并把错误伪装成 unsupported；拒绝。
4. **由 Server 在每个角色构造其本地资源；catalog 只保存 exact local-binding reference；factory 将其解析为 complete binding。** 采纳。

## 裁决

Server 是 provider role-factory 的唯一 production composition root。FE 从 `[connector.starrocks.local_bindings]` 建立有界、不可变的 local-binding registry；配置含 endpoint、credential、timeout 和 retry，BE 明确拒绝这类 FE-local 配置。每个 StarRocks catalog definition 必须恰有一个 `local_binding` execution property，且引用当前 FE registry 中一个精确条目。catalog state、SPI identity、native wire 与 Backend 都不携带 endpoint 或 credential。

factory normalize 不做 I/O；control materialize 只解析已启动时封存的资源并发布完整 binding。远端 metadata I/O 仍由 control generation 的 request context 执行。StarRocks execution factory 只声明当前明确支持的 capability；在独立 typed-contract 被接受之前，其 read、write 与 codec capability 均是 `None`，不得有 legacy fallback。

Frontend snapshot bootstrap 只等待有界 submission fanout：`worker_count` 限制提交 scheduler 的轻量任务数，但不能成为 provider I/O 的并发或等待边界。完整 snapshot 的所有 key 都成功交给 scheduler，才是 bootstrap completion 的含义；projection Ready/Unavailable counts 随后异步收敛。

## 接受的妥协（诚实记录）

每个 FE 都必须显式配置它可服务的 local-binding 名称；同一个 durable catalog 在某个 FE 缺失该配置时会 fail closed，而不是自动选 endpoint 或从其他 FE 借 client。运维上这增加了配置发布和多 FE 一致性的要求，但这是为了让 credential 与 network reachability 保持 process-local owner，而不是因为它比集中式 registry 更省配置。

当前 StarRocks control generation 仍只有 metadata control capability，Backend 也暂不提供 typed execution capability。我们接受 connector 的角色 lifecycle 已统一、但能力尚未对称；把未设计的 typed carrier 硬塞进 binding 会制造 codec 与 execution correctness 的假实现。

## 何时重新评估

1. 多 FE 需要对同一 catalog 使用不同 endpoint/credential 或需要热更新 local-binding resources 时；设计 versioned FE-local resource replacement 和 catalog admission fencing，不能引入 global client registry。
2. `local_binding` 配置数量、client 连接池或 remote metadata latency 达到可观测资源瓶颈时；可调整有界配置和 client policy，但 secret 不得进入 catalog state/wire。
3. StarRocks 的 typed read 或 write contract 经单独设计批准时；将 capability 加入对应 complete binding，并同时提供 provider conformance 与 native distributed evidence。
4. Server composition 需要加载动态 provider 插件时；保持 composition root 和每角色一个 complete binding 的裁决，另行定义受控 discovery 与 dependency boundary。
