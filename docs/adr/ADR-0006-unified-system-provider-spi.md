---
id: ADR-0006
title: "Unify replaceable provider contracts in one system SPI"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-07-27
provenance:
  - "discussion: 2026-07-27 NovaRocks system provider SPI architecture"
code-anchors: []
---

## 问题

NovaRocks 应如何组织可替换 provider 的长期契约：建立一个统一系统 SPI、让契约继续分散在领域/实现 crate，还是为每种
能力建立独立 contract crate？哪些跨 crate 接口应被视为 SPI？

## 背景与执行事实

NovaRocks 是同仓、静态链接、统一发布的 Rust workspace，当前不承诺仓库外插件 ABI。系统同时存在两类不同的
解耦需求：

第一类是产品架构明确支持的可替换 provider。StateStore 已有 SQLite、MySQL、FoundationDB 三种 production 实现；
Connector 已有 JDBC、HDFS、Iceberg、StarRocks 等实现。Consumer 不应知道选中了哪个实现，新 provider 应能在不修改
consumer 业务逻辑的前提下加入。

第二类是单一 owner/provider 的跨 crate 协作，例如 frontend 向 core 提供 topology，core 向 frontend 提供 query
cancellation/drain。它们同样需要清晰依赖方向，但并不存在产品级 provider 替换语义。

当前 StateStore contract、provider config、open 分派、provider runtime、具体实现和上层 coordination 同处一个 crate。
Connector contract 则散落为 scan connector、catalog backend、table source/sink、MV backend、scan planner 等接口，
并暴露 core SQL/planner、具体 connector config、`Any`/downcast 与默认 `unsupported` 行为。只移动现有 trait 无法形成
稳定边界。

对照 Trino 后确认：统一 SPI module 的价值是给 engine、provider 和 plugin/host 一个单一、明确的契约面；内部仍按
provider 类别组织。NovaRocks 不需要复制 Trino 的 Java `ServiceLoader`、外部插件 classloader 或兼容模型。

## 考虑过的选项

### 选项一：一个统一系统 SPI crate，内部按 provider 类别分模块

SPI 自己拥有 provider trait、必要的 factory/descriptor、边界值类型、typed error、capability、生命周期语义与
conformance contract。Provider 和 consumer 都依赖 SPI；host 认识具体 provider 并负责选择、构造、注册和关闭。

优势是依赖面、类型所有权和 provider 治理统一，未来新增 provider 类别有唯一准入位置。代价是中央 crate 的 review 和
编译影响面较大，而且同一 crate 内的模块纪律不能完全依靠 Cargo 强制。

### 选项二：契约继续归各领域 crate，只统一治理规则

StateStore contract 留在 state-store crate，Connector contract 留在 connector/core 领域。优势是物理隔离和依赖更窄；
代价是 contract 与实现容易继续混放，各类别会发展出不同的 factory、capability、error 和 lifecycle 模型，系统没有
统一 SPI surface。

### 选项三：每种能力建立独立叶子 contract crate

Cargo 可以更强地隔离单项依赖，但 capability/trait 很容易被机械提升为 crate，造成命名、发现和依赖碎片化。它把
“解耦”错误等同于“增加 crate 数量”，不适合 NovaRocks 希望形成的统一 provider 模型。

### 选项四：保留领域 contract，再建立统一 SPI facade/re-export

迁移最小，但 facade 不拥有类型和语义，仍携带所有 transitive dependency，并长期保留两套 canonical path。它没有
真正改变依赖方向。

## 裁决

NovaRocks 采用一个统一的系统 SPI crate，最终 package 名称由实现 spec 确定。该 crate 内部以“产品级可替换 provider
类别”为一级模块，不以 trait、调用方向或任意能力为拆分单位。

SPI 的准入条件是：

- provider 可替换性是产品架构的明确维度，而不是 test fake、迁移 seam 或内部策略；
- host 可以选择 provider，consumer 不依赖 provider 身份或实现类型；
- 存在可定义、可验证的共同语义和真实 production provider；
- provider 只依赖 SPI 与允许的基础层；
- contract 可以建立共享 conformance、typed error、capability 与生命周期语义。

当前事实只证明 StateStore 和 Connector 两个 provider 类别应进入 SPI。Catalog 需要区分领域模型、host/runtime 与
connector provider contract，只有最后一类属于 SPI。

SPI 是完整 provider 边界模型，不是 trait-only crate。`novarocks-types` 作为更低层的 provider-neutral 类型基础；
只为 provider contract 存在的 request/response/handle/error 由 SPI 对应模块拥有。SPI 不依赖 core、frontend、
server、具体 provider、application config、transport client 或全局 registry。

Provider 实现和 consumer 依赖 SPI。Host 负责发现可用 provider、选择 typed factory、创建/注册实例和编排
startup/drain/shutdown；SPI 只定义 factory/lifecycle 的可观察契约。允许 provider 类别专用的 typed registry/context，
禁止 `Services`、`get<T>()`、`dyn Any`、万能 `SpiContext`、production global install 和 service locator。

SPI 与 plugin loading、跨进程 transport、wire compatibility 相互独立。NovaRocks 当前按 workspace 原子演进 SPI，
不为内部 Rust API 保留旧方法、fallback、双写或 compatibility adapter；稳定性由语义治理、共享 conformance、真实
provider integration、host composition 和必要的 1FE+3BE e2e 保证。

Frontend/core 的一对一协作使用 domain API 或 consumer-owned port，不因跨 crate 或双向运行时调用进入 SPI。

## 接受的妥协（诚实记录）

统一 SPI crate 会扩大部分改动的重新编译和 review 范围，也无法像多个叶子 crate 那样由 Cargo 完全阻止 provider 模块
互相引用。选择统一 crate 不是因为这些成本不存在，而是因为 NovaRocks 更需要一个易发现、语义一致的 provider
contract surface；多个叶子 crate 的结构碎片化和治理漂移风险更高。

本裁决不会立即让现有 StateStore/Connector 边界变干净。迁移需要重新设计边界模型并原子切换所有 provider/consumer，
工作量显著大于移动 trait 或建立 facade。为保持长期结构清晰，明确接受这项成本，不采用 bridge、双 canonical path 或
短期 fallback 降低迁移量。

当前不提供仓库外插件 ABI、独立 SPI semver 或通用 `Plugin` 入口。这限制了第三方 provider 的独立发布，但避免在没有
真实产品需求时提前承担 Rust ABI、loader 隔离和跨版本兼容成本。

## 何时重新评估

- NovaRocks 正式支持仓库外、独立发布的 provider，需要 SPI version、兼容区间、loader 与安全隔离时；
- 某 SPI provider 模块需要与其他模块明显不同且互斥的基础依赖，导致统一 crate 的 dependency ceiling 无法维持时；
- 统一 SPI 的编译影响或变更冲突被持续测量为主要开发瓶颈，且按模块拆 crate 能保持单一逻辑 SPI surface 时；
- 新的产品级 provider 类别满足本 ADR 的全部准入条件时，应新增模块，但不需要推翻统一 SPI；
- StateStore 或 Connector 不再是可替换 provider，系统取消其 provider 选择承诺时，应重新判断是否仍属于 SPI。
