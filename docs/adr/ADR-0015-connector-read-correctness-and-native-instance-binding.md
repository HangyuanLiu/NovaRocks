---
id: ADR-0015
title: "Connector-owned read correctness with pre-dispatch native instance binding"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-07-29
provenance:
  - "discussion: 2026-07-29 SPI-3R2 connector read correctness and native instance cutover"
code-anchors:
  - "novarocks/spi/src/connector/distribution.rs (distributed instance contract)"
  - "novarocks/core/src/connector/host.rs (process-scoped instance host)"
  - "novarocks/core/src/connector/iceberg/provider.rs (Iceberg reader boundary)"
  - "novarocks/core/src/protocol/native/decode/scan/generic.rs (generic lookup boundary)"
---

## 问题

table-format Connector 的 read correctness 应在什么边界完成，以及动态 catalog 的真实 Connector instance 如何安全地跨 FE/BE 进程使用？

## 背景与执行事实

Iceberg 的 snapshot、position/equality delete、deletion vector、field-ID schema evolution 和 lineage columns 共同决定可见行。把这些事实作为 core-private sidecar、auxiliary 或 scan runner hook，会让 `ConnectorRead::open_reader` 不能独立保证正确性，并且无法由通用native carrier恢复。

历史native路径将Iceberg scan降为 `hdfs` execution identity，再由generic decoder按provider ID临时materialize query-local reader。这既破坏catalog-specific instance identity，也要求fragment payload承载不应跨进程传递的配置或凭证。

## 考虑过的选项

1. decoder从fragment payload选择provider并临时materialize instance。动态catalog接入直接，但generic decoder依赖provider业务，凭证边界和instance identity都不可靠。
2. 所有catalog降为固定execution instance。省去install控制面，但会丢失metadata/read同一instance、drop/recreate generation和访问策略边界。
3. Connector在自身reader内完成table-format correctness；coordinator在fragment dispatch前把同一逻辑instance的read-only binding安装到目标BE。carrier只带instance identity和opaque handles。

## 裁决

采用选项3。`ConnectorRead::open_reader` 返回的每个batch已满足该provider的table-format correctness。Core只执行provider-neutral Arrow→engine适配、SQL residual/runtime filter和limit，不解释delete、DV、schema evolution或lineage。

instance declaration由planning instance导出，只带provider-owned非秘密binding reference和opaque事实。BE startup composition注册provider installer并以本地credential/access handle创建read-only instance。coordinator先完成所有目标BE install ACK，再发送fragment。generic decoder只lookup injected process-scoped host，绝不按provider ID分支或fallback materialize。

incarnation和declaration digest保证install幂等、冲突显式、generation replace安全；retiring instance拒绝新reader，已resolve reader通过Arc完成drain。fragment split内部校验incarnation，避免旧fragment在新binding上误读。

## 接受的妥协（诚实记录）

R2在core crate内保留Iceberg provider的物理位置，先固定正确的read contract；独立crate迁移属于后续Cargo owner工作。compat缺少NovaRocks catalog identity，因此可使用startup-composed、binding派生的Iceberg read-only instance，但仍不得引入HDFS identity或query-local execution identity。

R2不引入runtime-filter/predicate pushdown SPI。没有pushdown会影响性能，但core在correctness-complete batch上执行残余谓词可保持结果正确。

## 何时重新评估

- Paimon/Hudi等provider需要不同的read lifecycle或跨split state；
- 多named startup access binding成为产品需求；
- runtime filter pushdown需要accepted/residual provider-neutral contract；
- Connector独立crate迁移暴露出SPI仍含core类型；
- instance install需要跨BE重启持久化或租约化管理。
