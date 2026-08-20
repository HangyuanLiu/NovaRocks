---
id: ADR-0087
title: "Server owns process data-runtime composition and role-local access adapters"
domain: [configuration]
status: active
supersedes: []
superseded-by: null
date: 2026-08-18
provenance:
  - "discussion: 2026-08-18 process data-runtime composition and native FE/BE transport ownership"
  - "PR: pending — server-owned data-runtime composition owner cut"
code-anchors:
  - "novarocks-server/src/main.rs (build_data_runtime)"
  - "novarocks-server/src/composition.rs (run_all_in_one_until)"
  - "novarocks/frontend/src/native/data_runtime.rs (FrontendDataRuntime)"
  - "novarocks/backend/src/rpc/runtime.rs (BackendDataRuntime)"
---

## 问题

进程级 data runtime 应由谁创建、持有和销毁；FE/BE 的同步 native transport 又应如何取得它，而不恢复 Core 全局单例？

## 背景与执行事实

早期的配置注入决策为降低迁移成本，保留了 Core 内惰性 `OnceLock` data runtime：Server 仅先安装线程尺寸，深层
调用方再从全局取得 runtime 或 Handle。这条例外曾避免大范围改造，但它使 runtime 的创建时机、生命周期与 channel cache
归属都不在类型和构造图中：all-in-one 可以看似共享资源，单角色重启与 role generation 却无法由 owner 明确释放。

Server 已是完整应用配置和 production role dispatch 的唯一 composition root（ADR-0072）。实际 native consumer 只属于
Frontend 的协调/transport 和 Backend 的 fragment/lifecycle/runtime-filter transport；FS 与 Connector 已经接收 application
runtime 的显式注入，不需要也不应增加第二条 data-runtime 依赖边。

## 考虑过的选项

1. **保留 Core `OnceLock`，继续由 Server 安装 sizing。** 改动小，但启动顺序仍是运行时约定，cache 继续跨 role
   generation 残留，无法证明谁销毁资源。
2. **为每一个 native RPC 调用显式传递裸 `Handle`。** 可以删除全局，但会让 channel cache 和同步调用语义分散到大量
   consumer，role host 仍没有一个可关闭的资源 owner。
3. **把 data runtime 交给 FS、Connector 或 Execution service。** 这些 owner 的真实工作已经使用各自的 application
   runtime；把 native transport 生命周期塞入其中会制造不真实的跨 owner 依赖。
4. **Server 创建并持有唯一 data runtime，FE/BE 接收 role-local adapter。**（采纳）

## 裁决

`novarocks-server` 在 role dispatch 前按已解析 `[runtime]` 线程预算创建唯一命名 data runtime，并在所有 role host
完成 shutdown 后、同步返回路径上销毁它。`role=fe` 与 `role=be` 各取得该 runtime 的 clone Handle；`role=all-in-one`
将同一个 Handle 同时传给 Frontend 和 Backend，绝不各建一个 runtime。

Frontend 和 Backend 分别拥有只含 clone `Handle` 与 role-local native gRPC channel cache 的薄 adapter。同步 RPC 保持原有
上下文内 `block_in_place + Handle::block_on` 与上下文外 `Handle::block_on` 的两条语义路径；adapter 不读取配置、不拥有
`Runtime`、不提供默认值或全局查找。Backend runtime-filter worker 保留其 `JoinHandle` 并在 role shutdown 时停止；Frontend
control bridge 继续由其 session owner abort。

Core 不再导出 data runtime、sizing 安装 API 或文件执行回接 runtime。worker stack size 是进程无关的共享类型常量，归
`novarocks-types` 单一定义。该裁决撤销 ADR-0059 中“data runtime 可作为组合根只安装 sizing 的全局例外”，但不改变
ADR-0059 的历史状态或 ADR-0072 的 configuration wire owner 结论，因此不以 `supersedes` 重写既有谱系。

## 接受的妥协（诚实记录）

**Server 会显式持有另一个 Tokio runtime。** 这增加线程和关闭顺序管理成本，且 all-in-one 必须清楚地区分 application
runtime 与 data runtime。我们接受它，因为 native transport 的生命周期确实是进程 composition 事实；把它藏回 Core
全局只会以较低的局部改动成本换取不可证明的 owner。

**adapter 仍保留同步 `block_on`。** 原生 transport trait 目前是同步边界，完全 async 化会扩大 Core/FE/BE contract
变更。我们保留双路径行为以维持语义，却要求它只在 role-local adapter 集中实现，并以测试覆盖上下文内外调用；这不是
对任意 async task 中阻塞的许可。

**FS 和 Connector 不迁移到 data runtime。** 一种看似统一的运行时注入会减少名词数量，但会虚构它们与 native
transport 的共同 owner。维持现有 application-runtime 注入意味着短期内有两种显式 runtime capability；这是基于真实
资源边界，而非追求表面统一。

## 何时重新评估

1. 若 native transport contract 完成端到端 async 化，应评估是否能删除 adapter 的同步 `block_on` 面，但必须先证明
   所有 caller 的背压、取消和错误语义不变。
2. 若一个进程需要并存多个隔离的 FE 或 BE role generation，应重新设计 data runtime 的实例数、预算和可观测性；在此
   之前一个 production composition root 只创建一个。
3. 若 FS、Connector 或 Execution 的真实资源生命周期需要与 native transport 同步停止，应先定义新的 owner contract
   和关闭顺序；不得把它们接到 data runtime 作为临时捷径。
4. 若 Tokio 或部署平台提供可验证的进程级线程预算/优雅 shutdown primitive，应重新评估当前由 Server 同步持有
   `Runtime` 的实现成本。
