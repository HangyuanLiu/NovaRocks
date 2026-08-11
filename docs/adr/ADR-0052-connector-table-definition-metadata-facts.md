---
id: ADR-0052
title: "Connector table-definition metadata facts"
domain: [provider-spi, sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-08-11
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/861"
  - "discussion: 2026-08-11 exact-generation SHOW CREATE metadata during the Iceberg provider owner cut"
code-anchors:
  - "novarocks/spi/src/connector/metadata.rs (ConnectorTableMetadata)"
  - "novarocks/core/src/engine/mod.rs (handle_show_create_table)"
---

## 问题

`SHOW CREATE TABLE` 如何在不读取 concrete Provider table、解码 opaque handle 或增加专用 Connector capability 的前提下，准确显示一个 exact Connector generation 的表定义？

## 背景与执行事实

Connector metadata 已由 retained exact lease 下的 `ConnectorMetadata::load_table` 返回 `ConnectorTableMetadata`。其中 Arrow schema 是 scan/SQL projection 的物理载体，`ConnectorTablePlanningFacts` 补充 visibility、logical kind 与系统列角色；它们不能表达 Iceberg `FIXED(n)`、嵌套 SQL type tree、top-level column doc 或 table comment。

Iceberg 现有 `SHOW CREATE TABLE` 仍在 Core 从 concrete loaded table 读取 Iceberg schema 和 property map。provider implementation 移入独立 crate 后，这会使 Core 继续拥有 concrete catalog/runtime 依赖，或迫使 Core 解码 provider payload，二者都破坏 Connector owner boundary。直接根据 Arrow schema 猜测 SQL type 又会丢失 fixed binary length 与嵌套定义；让 Server inspector 以 Iceberg-only API 回填则会创建第二条 capability 和绕过 exact metadata lease 的路径。

## 考虑过的选项

第一种是保留 Core 读取 concrete Iceberg table。改动最小，且可继续复用当前 renderer；但 Core 必须保留 catalog registry、runtime 或 provider table type，owner cut 不可能完成。

第二种是让 Core 从 Arrow schema 或 opaque table handle 重建 DDL。前者会对 fixed binary、nested type 和 comments 静默降级，后者把 provider codec 变成通用应用接口，并可在错误 generation 上重新解析 payload。

第三种是增加 Iceberg-only Server inspector 或新 `ConnectorShowCreate` capability。它能保留 provider renderer，却平行于已有 `load_table` 建立第二 metadata 生命周期，令每个 provider 为一个 display command 扩张 capability surface。

第四种是在现有 `ConnectorTableMetadata` 上附加有界、provider-neutral table-definition facts，并由 exact metadata lease 一次返回。Core 只渲染该事实；provider 从自己的真实 table 转换；没有事实的 provider 明确不支持 SHOW CREATE。

## 裁决

采用第四种方案。

`ConnectorTableMetadata` 增加 `ConnectorTableDefinitionFacts`。facts 只是一项 request-local、immutable metadata value，不新增 `ConnectorMetadata` method、Connector capability、native wire field、durable attachment property、runtime/client handle 或 provider payload。它的列以严格递增 Arrow schema ordinal 对齐，精确覆盖 SQL-visible top-level fields；planning facts 为空时全部 schema fields 视为 SQL-visible。每列携带 nullable、optional comment 及专用于 DDL 的 `ConnectorTableDefinitionType`。该 type tree 表达当前渲染的 primitive、decimal、`BINARY`/`BINARY(n)`、array、map 与 named struct child 类型；它不复用 mutation 的 `ConnectorDataType`，以免把 DML default/aggregation vocabulary 带入 display metadata，且该旧类型不能表示 fixed binary length。

facts 另携带 optional table comment。嵌套 field doc、partition spec、sort order、property map、snapshot、file、credential 和 table-format raw type 均不进入 SPI：当前 SHOW CREATE 没有渲染前两者，后续若需要它们必须以独立设计裁决其跨 Provider 语义。constructor 必须验证 ordinal 覆盖、次序、重复、type tree depth、字符串/注释长度和 `ConnectorRequestContext` total-payload budget。

Core 在原有 statement admission 与 exact metadata lease 中，以 table identity、schema、planning visibility 及 definition facts 做通用 DDL rendering、identifier/comment escaping 和 QueryResult encoding。definition facts 缺失、不完整或与 schema 不一致时确定失败；不回退到 latest lookup、concrete registry、provider handle decode、Arrow type guess 或 Iceberg-only inspector。

## 接受的妥协（诚实记录）

这会为一个展示型 SQL 命令增加小型 public SPI DTO，且 Iceberg provider 必须在每次 metadata load 时从其 schema 生成它。选择它不是因为 DTO 比 provider renderer 更优雅，而是现有 `load_table` 已是唯一正确的 exact-generation metadata seam；保持该 seam 的成本低于保留 concrete Core owner 或引入第二 capability。

facts 只复现当前 SHOW CREATE 的语义，不试图成为通用 catalog DDL export format。它故意不包含 unbounded property map、partition/sort details、nested docs 或 raw table-format syntax，因而未来命令可能需要再次 load 或设计新的 bounded facts。对于尚未提供 facts 的 provider，SHOW CREATE 从历史上的可能猜测变为显式未支持；这牺牲了表面兼容性，换取不产生错误或不完整 DDL。

## 何时重新评估

- 产品要求 SHOW CREATE 保留 partition transform、sort order、view text、provider-specific table property 或 nested field doc，并且这些语义已证明跨多个 Provider 可替换。
- definition facts 的 schema-ordinal validation 或 request payload size 成为高频 metadata load 的可观测瓶颈，需要独立 cache/codec；任何 cache 仍必须受 exact generation lease 约束。
- 新 Provider 无法把自身公开表定义降到当前 bounded SQL vocabulary，且用户需要无损 provider-native DDL；应设计显式 provider command 而不是扩张通用 facts 为 raw text。
- `ConnectorTableMetadata` 变为 native wire 或 durable state 的一部分；届时必须重新裁决 versioning、compatibility 与 credential/provenance exclusion。
