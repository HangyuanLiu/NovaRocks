---
id: ADR-0130
title: "Connector role bindings publish one complete generation per process role"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-08-31
provenance:
  - "discussion: 2026-08-31 connector role-binding convergence"
code-anchors:
  - "novarocks/connector-binding/src/binding.rs (ConnectorControlRoleBinding and ConnectorExecutionRoleBinding)"
  - "novarocks/frontend/src/connector/control_host.rs (ConnectorControlHost)"
  - "novarocks/backend/src/connector/catalog_manager.rs (CatalogManager)"
---

## 问题

当同一 catalog generation 同时包含通用 control/execution lease、typed read services、directional codec、request-scoped credential factory 与 writer capability 时，Frontend 和 Backend 如何避免分别注册平行 map，进而让一次 query 混用不同 generation 或在失败后错误地把 unsupported 当作可用？

## 背景与执行事实

Connector 的 control runtime 是 FE-local，execution runtime 是 BE-local；两者不能共享 registry、生命周期或失败状态。此前 control read registry、codec installer 和 BE 的 read/write/runtime factory list 分别保存 generation 的不同 facet，任何一个局部注册或退休先后不一致都可能制造第二个 authority。

`ConnectorControlRoleBinding` 与 `ConnectorExecutionRoleBinding` 现在都以 exact `CatalogHandle` 为 identity，只有 complete immutable binding 可以发布 Ready。FE `ConnectorControlHost` 的一次 planning acquire 同时取得 SPI lease 与 role snapshot；typed read accessor 只从该 snapshot 读取。BE `CatalogManager` 只在本地 bind 成功后把 complete execution binding 放入 query-reachable Ready，并把 typed failure class/disposition 保留在有界 suppression state。Server 是唯一把每个 provider/role factory 装配进生产路径的位置。

## 考虑过的选项

1. **继续保留通用 registry 与 typed registry/factory list，并要求调用方手工比对 generation。** 改动小，但每个调用点都变成新的一致性与退休竞争面；拒绝。
2. **让 FE 和 BE 共享一个跨进程 connector runtime 或在 all-in-one 直接调用。** 看似减少构造次数，但破坏 native 角色故障域和 production topology；拒绝。
3. **把任一构建失败压缩为 `None` capability。** 会把 transient、permission 或 bug 伪装成明确 unsupported，抑制恢复并误导调用方；拒绝。
4. **每个 provider/role factory 一次性构造 complete role binding，Host/Manager 只发布完整 binding。** 采纳。

## 裁决

FE control 与 BE execution 分别拥有 role-local factory set、registry 和失败策略。factory normalize 必须无 I/O；FE materialize 可有远端 I/O，但受 application-owned deadline、cancellation、keyed singleflight、bounded concurrency 和 typed retry disposition 约束；BE bind 只使用 startup-sealed local resources，不能远端 I/O。

`None` 仅表示 provider 对该 role 的明确 unsupported capability；任何 error 保持 error。planning operation 只持有一个 Host generation lease，不能再查询 independent typed registry。Server 以每 provider/role 一个 factory 完成生产 composition；all-in-one 复用正常 FE/BE composition，不获得旁路。

## 接受的妥协（诚实记录）

我们接受 control/execution binding 内会暂时携带少量仍由旧 SPI effect contract 消费的通用 capability，且 FE 的 CREATE 要通过 scheduler completion ticket 等待首次 attempt。这不是更简单的实现；代价是显式的 binding 类型、更多失败状态测试和较长的启动构造链。它换来的是 generation 只有一个可发布答案，且 retry、suppression 与 lease 的 owner 不会因调用入口不同而漂移。

## 何时重新评估

1. 新 provider 需要的 capability 无法作为 complete typed role binding 表示，且会迫使引入 `Any`、动态 facet map 或第二个 registry 时。
2. FE materialization 的有界并发或 backoff 在真实 profile 中成为可量化瓶颈时；可调整配置或调度实现，但不得拆出第二条 side-effect path。
3. StarRocks 获得经单独设计批准的 typed read/write contract 时；可以把明确 `None` 替换为该 contract 的 binding，不能复活 legacy fallback。
4. Native wire 或 catalog identity 需要变化时；该 ADR 不授权改变 IDL、`CatalogVersion`、attachment schema 或 query lifecycle barrier。
