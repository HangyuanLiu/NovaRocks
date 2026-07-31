---
id: ADR-0018
title: "Static connector predicate disposition is capability negotiation"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-07-31
provenance:
  - "discussion: 2026-07-31 static connector predicate pushdown"
code-anchors:
  - "novarocks/spi/src/connector/predicate.rs (provider-neutral DTO and response validation)"
  - "novarocks/core/src/query_execution/preparation/scan_preparation (lowering and residual reconstruction)"
  - "novarocks/core/src/connector/iceberg/provider.rs (Iceberg PruningOnly planning)"
---

## 问题

静态 scan conjunct 如何由 Core 提供给 Connector 做性能裁剪，同时不把 Connector 的统计能力误当成 SQL 语义执行？

## 背景与执行事实

SQL residual 是结果正确性的 owner。Connector 可以利用文件、partition 或 row-group 元数据减少读取，但统计缺失、截断、schema rename 和不同格式能力都要求其保守 Keep。native fragment carrier 只能携带已选定的 opaque connector handle/split，不能成为 provider predicate 或动态 runtime-filter 的通用载体。

## 考虑过的选项

1. 让 Connector 只返回一个“已下推”布尔值。实现简单，但无法区分只裁剪候选单元与可以移除 residual 的精确执行。
2. 把 provider predicate 扩展进通用 native wire，或用 runtime callback 传递动态 filter。能力看似完整，但使 provider 语义、wire 兼容和 query lifecycle 耦合，并扩大了故障面。
3. 在 begin-scan 使用 provider-neutral 静态 DTO，以每个原始 conjunct ID 返回 `Exact`、`PruningOnly` 或 `Unsupported`；Core 只依 `Exact` 重建 residual。

## 裁决

采用选项 3。请求与响应必须 total one-to-one；缺失、重复或未知 ID 作为 FE planning 的 `CorruptData` 失败，合法 `Unsupported` 则保留 residual。会话设置 `enable_connector_static_predicate_pushdown` 默认开启；关闭时发送空列表并保留全部 residual，形成结果等价的回滚/A-B 路径。

Iceberg V1 只对受支持的 Boolean/Int32/Int64/Date32 比较和非空 IN 返回 `PruningOnly`，把私有 physical field ID predicate 放入 opaque V2 split，用于文件与 Parquet row-group 裁剪。ORC 不接收该物理 predicate；page-index predicate pruning 留给后续独立能力。动态 runtime filter 的顺序和通用 native wire 都不改变。

## 接受的妥协（诚实记录）

`Exact` 已被合同保留且 conformance fake 可验证，但当前 Iceberg 不声明它，因此 SQL residual 继续在 Core 执行，性能收益仅来自少读文件/row-group。Core 首版不发送不能证明 timezone/collation 语义的 timestamp/string predicate。DTO 与 disposition 会增加 planning 状态和测试矩阵，但这是为了把错误的精确声明暴露为正确性问题，而不是静默 fallback。

## 何时重新评估

- Connector 能以可证明的语义精确执行一个静态 predicate；
- 引入新的 logical type、collation 或 timestamp 合同；
- 需要 predicate-driven page-index pruning；
- 发现必须改变通用 native wire 或引入动态 callback；
- 分布式验收不能同时证明三 BE reader 活动、A/B 结果一致和取消关闭 reader。
