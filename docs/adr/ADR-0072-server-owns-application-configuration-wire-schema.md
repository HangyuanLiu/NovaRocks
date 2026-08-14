---
id: ADR-0072
title: "Server owns the application configuration wire schema and projects resolved domain inputs"
domain: [configuration]
status: active
supersedes: [ADR-0059]
superseded-by: null
date: 2026-08-14
provenance:
  - "discussion: 2026-08-14 application configuration wire and resolved-value ownership after Core engine retirement"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/901"
code-anchors:
  - "novarocks-server/src/main.rs (load_config_and_resolve_role)"
  - "novarocks-server/src/composition.rs (run_all_in_one_until)"
  - "novarocks/frontend/src/server.rs (FrontendServerConfig)"
  - "novarocks/backend/src/application.rs (BackendServerConfig)"
---

## 问题

完整应用 TOML 的 wire schema、默认值、加载和跨 section 校验应该由谁拥有？各 domain crate 应该拥有并解析自己的
TOML section，还是只接收由唯一 composition root 投影出的 resolved typed input？

## 背景与执行事实

进程级配置单例退役后，配置已经由启动路径显式加载并按值传递；但完整根结构及所有 section 仍定义在聚合 Core 中，
Frontend 和 Backend 的 application config 也仍直接包含这份根值。这使 Core 反向认识 StateStore、Runtime、Frontend、
Connector 与进程网络配置，也允许 role crate 在自己的 owner 边界之外读取任意 section。

当前代码同时提供了正确方向的证据：Server 已经是唯一生产加载入口，并已在 composition 中把 object-store、Connector
factory 和 StateStore host input 投影出来；Execution、Frontend 与 StateStore 也已有窄 typed config。缺少的不是新的共享
配置 crate，而是把这个投影边界做完整。

另一个关键事实是 `[runtime]` 并不等于 Execution 配置。它同时包含 Execution、Backend query lifecycle、Frontend query
control、memory、cache 等多个 owner 的用户 wire 字段。把整个 section 搬进 Execution 只是把聚合从 Core 换一个位置。

## 考虑过的选项

1. **继续把完整根 schema 留在 Core，只删除剩余调用点。** 改动最小，但 Core 仍是 application composition hub，
   Frontend/Backend 仍能接收并读取整个根值，无法由 Cargo 图表达 owner cut。
2. **每个 domain crate 各自定义和解析自己的 TOML section。** 表面上 section 与 owner 对齐，但会产生多个 parser、
   重复 defaults、无法集中表达跨 section 校验，并让 all-in-one 可能对同一文件进行多次读取。
3. **新建共享 configuration/common crate。** 能把文件移出 Core，却会让一个被所有人依赖的 crate 反向认识所有 domain，
   形成新的 hub，并没有解决依赖方向。
4. **Server 唯一拥有完整 wire schema，并一次性投影为各 owner 的 resolved typed input。**（采纳）

## 裁决

`novarocks-server` package 是完整应用配置 wire 的唯一 owner。它拥有根和嵌套 section 的 `serde` 表达、默认值、文件与环境
搜索顺序、TOML parse context、release 环境拒绝以及全部跨 section 启动校验。

Server 在启动时只解析并校验一次，然后显式投影：Frontend、Backend、Execution、FS、Connector、StateStore 等 crate
只定义和接收自己真正消费的 resolved typed value/config。Frontend 与 Backend 的 application input 不得包含完整根值，
domain crate 不反向依赖 Server，也不重新解析 TOML或重新补默认值。

`[runtime]` 保持为 Server-private wire section，并按消费者分别投影；不得整体迁入 Execution。部署角色是跨 owner 的中立值，
其 canonical `ClusterRole` 定义归 `novarocks-types`，而 `[cluster]` wire 仍归 Server，Frontend 只接收 resolved membership
input。

Core 删除应用根 schema、re-export、根配置参数与解析型 test fixture。迁移不保留 Core parser、type alias、deprecated
facade、feature fallback 或任何双 authority。repository-owned 工具若需要现有应用配置，只能通过 Server package 的窄加载+
投影 API 获取 resolved value，不能复制完整 parser。

本裁决保持字段、默认值、未知键策略、错误文本/类别、配置查找顺序、CLI role override 和 FE/BE/all-in-one 行为；它只改变
物理 owner 与跨 crate 输入形状。

## 接受的妥协（诚实记录）

**Server 将显式依赖并认识所有配置域。** 这会让 Server schema 和 projection 代码较大，也意味着新增 section 必须修改
composition root。我们接受这项 fan-in，因为“完整应用如何启动”本来就是 composition root 的职责；把 fan-in 藏进 shared
crate 只会制造反向依赖。

**投影会产生机械字段映射。** 直接传根值代码更少，但会永久扩大每个 consumer 的权限与耦合。我们接受映射成本，以换取
domain 输入可独立构造、校验和测试，以及 Cargo DAG 能表达 owner 边界。

**repository tool 复用 Server config 可能增加构建依赖。** 这是避免第二完整 parser 的真实成本。若工具构建成本后来成为问题，
需要重新设计一个稳定且窄的配置读取接口；不能以复制 schema 作为优化。

**StateStore provider typed config 的最终物理拆分留给后续变更。** 当前 owner cut 会先把 wire 固定在 Server，并投影为现有
typed host input；它不会顺带拆 SQLite、FoundationDB 与 MySQL provider。这个中间态保留了单一 wire authority，但暂时保留
provider typed config 的聚合实现成本。

## 何时重新评估

1. 若出现第二个需要完整应用 TOML 的独立产品 composition root，应重新评估 Server package 是否仍是合适的唯一 wire
   owner；在此之前不能先复制 parser。
2. 若引入运行时可变配置、远程配置或版本化配置发布，启动时一次解析和冻结的前提失效，需要为可变字段单独定义 authority、
   observation 与更新失败语义。
3. 若 Server projection 长期无法在不依赖某个 domain implementation 的情况下构造输入，应先检查该输入是否仍混入 wire
   或跨域事实，而不是建立 common config hub。
4. 当 StateStore provider crate 完成物理拆分时，重新核对 provider-private typed constructor 已下沉、Server wire owner 未被
   复制、neutral host input 未重新枚举 concrete provider。
