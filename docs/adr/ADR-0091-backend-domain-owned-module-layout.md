---
id: ADR-0091
title: "Backend modules follow domain ownership with narrow RPC infrastructure"
domain: [backend-architecture]
status: active
supersedes: []
superseded-by: null
date: 2026-08-20
provenance:
  - "discussion: 2026-08-20 Backend domain-first module ownership"
  - "PR: #945 — Backend module ownership migration"
code-anchors:
  - "novarocks/backend/src/lib.rs (module ownership boundary)"
---

## 问题

当 Backend 只剩一套生产 FE/BE wire 边界时，模块应继续按历史协议族组织，还是应按实际领域 owner 组织；其中真正跨领域的 RPC 基础设施又应放在哪里？

## 背景与执行事实

`novarocks/backend/src/native/` 最初用于区分 NovaRocks 的 native FE/BE 协议与已经退役的 StarRocks-compatible
inbound runtime。ADR-0026 已将后者退役，StarRocks 仅作为 read-only external Connector；生产 Backend 因而只承载一套
FE/BE RPC 边界。

这一历史目录现在同时容纳 generated Tonic surface、codec、client/server、channel cache 和 data-plane handler，也容纳
fragment protobuf decode、query lifecycle RPC adapter、runtime-filter wire/install/发送 adapter，以及 Connector execution
binding decode。后四类工作分别已有 `fragment`、`query_lifecycle`、`runtime_filter` 与 `connector` 领域 owner；把它们放在
同一个协议名目录下，使目录无法表达新增代码应由谁拥有，也把同一领域拆成两个物理入口。

`BackendDataRuntime` 只是 Server 注入的 Tokio `Handle` 与 Backend-local Tonic channel cache，不创建进程 runtime、也不拥有
query execution；其 composition 结论由 ADR-0087 固定。native DTO 的 schema、字段路径与验证错误由 Protocol 拥有（ADR-0085）。
因此目录调整不能借机转移 Protocol、Server、lifecycle、runtime-filter 或 Connector 的既有 authority，更不能改变 wire、
取消、重试、去重、背压或资源关闭语义。

## 考虑过的选项

1. **保留 `native/` 作为统一目录。** 改动最小，并能继续使用既有 import 路径；但它已不对应并列的生产协议，也不能区分
   transport 与领域 adapter。每次新增代码仍要由维护者猜测应放入历史总目录还是领域目录，所有权漂移会继续累积。
2. **将整个 `native/` 整体改名为 `rpc/` 或 `transport/`。** 这能删除过时名称，且能集中 Tonic 相关代码；但 fragment decoder、
   lifecycle state-machine adapter、runtime-filter participant adapter 与 Connector binding 都不是通用 RPC 基础设施。整体改名会把
   领域行为伪装成 transport，延续错误的聚合边界。
3. **按领域归位，并保留窄的 `rpc/` 基础设施。** fragment ingress/decode 归 `fragment`，lifecycle stream adapter 归
   `query_lifecycle`，runtime-filter envelope/install/outbound transport 归 `runtime_filter`，Connector binding decode 归
   `connector`；`rpc/` 只保留 generated surface、codec、client、server composition、data-plane handler 与
   `BackendDataRuntime`。这是采纳的选项。

## 裁决

Backend 模块以真实领域 owner 为一级组织原则，不再保留 `native` 这一历史聚合 root。每个领域的 wire adapter 与其状态、
验证规则和 execution host 保持相邻；RPC server 作为唯一跨领域 composition root，直接装配各领域提供的最窄 crate-private
handler surface，但不吸收其状态机或业务语义。

`rpc/` 是窄基础设施边界：generated Tonic declarations、codec、outbound client、listener/server composition、低层
data-plane handler，以及 role-local `BackendDataRuntime` 可以放入其中。它不得拥有 fragment plan/expression decode、
query lifecycle registry、runtime-filter participant 或 Connector execution state。

此次迁移只改变物理路径、module declarations、imports 和在 Backend-local 上下文中冗余的基础设施名称。每个 production
能力必须只有一条模块路径；不添加 compatibility re-export、双入口 decoder 或临时 legacy facade。确实表达 FE/BE wire
语义的 `Native` 名称仍保留，不能因目录清理被机械改名。

## 接受的妥协（诚实记录）

**迁移会造成一次性的大范围 path churn。** Rust module 声明、绝对 import、测试路径和 Server composition 都需要同步修改，
短期 diff 比保留旧目录明显更大。我们接受这项成本，是因为目录路径本身已经误导所有权；用 alias 或 re-export 降低当前
迁移成本，只会让未来维护者继续面对两个入口和两个名义 owner。

**RPC server 仍是跨领域 composition root。** 它必须认识多个 handler 并把生成的服务绑定到 listener，因此不能做到每个
文件只依赖一个业务领域。我们接受这种集中装配，但 server 只负责 transport 生命周期与调用分发；它不保存 lifecycle、
runtime-filter 或 Connector 的业务状态，以免 composition root 演化成第二 authority。

**并非所有 `Native` 词汇都会消失。** wire DTO、跨角色 contract 和协议错误仍需要准确描述 native FE/BE 语义；一概改名
会制造不必要的 public API churn，也会模糊协议含义。选择清理历史目录和 Backend-local 冗余基础设施名称，而不是进行全仓库
词汇替换，是基于改动范围与语义精确性，不是认为旧术语本身都错误。

## 何时重新评估

1. 若 NovaRocks 重新引入第二种正式的、长期支持的 inbound Backend protocol，且它与 FE/BE RPC 共享可证明的 transport
   基础设施，应重新评估目录是否需要按协议分层；不能因测试 shim、短期兼容或外部 Connector 而恢复历史聚合目录。
2. 若某个 adapter 真实地服务多个领域，且其输入、验证、状态和失败语义都不能由任一领域 owner 解释，应先定义独立契约与
   owner，再评估是否提升到 `rpc/` 或新 crate；不得以 `common`、`legacy` 或第二 decoder 作为临时收容处。
3. 若 RPC server 开始持有领域状态、执行重试策略或业务关闭 authority，应先拆分 composition 与领域 ingress，避免基础设施
   变成隐式 application owner。
4. 若跨 crate 的依赖关系需要更强的编译期隔离，应以 crate 边界表达该事实并遵循 ADR-0058；不能把本 ADR 的目录结构扫描
   固化成永久 correctness gate。
