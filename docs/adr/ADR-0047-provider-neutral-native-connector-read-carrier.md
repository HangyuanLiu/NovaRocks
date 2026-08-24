---
id: ADR-0047
title: "Provider-neutral native Connector read carrier"
domain: [provider-spi, distributed-query-lifecycle]
status: superseded
supersedes: []
superseded-by: ADR-0103
date: 2026-08-08
provenance:
  - "mechanism: exact-generation catalog and read admission; PR number to be backfilled after merge"
  - "discussion: 2026-08-08 provider-neutral native Connector read carrier"
code-anchors:
  - "novarocks/spi/src/connector/metadata.rs (ConnectorMetadata)"
  - "novarocks/core/src/query_execution/preparation/scan_preparation/iceberg.rs (plan_iceberg_connector_read)"
---

## 问题

动态 catalog 的 namespace、time-travel reference、view 与 metadata-table read，如何在不把 provider 表格式事实固化进 Core binding 或 native wire 的前提下，和普通表读取使用同一个 exact Connector generation？

## 背景与执行事实

一次 statement admission 中，catalog 解析、statistics、scan preparation 与 fragment submission 必须引用同一 Connector control generation；重新获取 current binding 会使 drop/recreate、权限变更或 snapshot 变化在同一个 query 中混入。`ConnectorControlPlanningLease` 已能封存该 generation，但若 namespace 枚举、reference 解析或 view rewrite 直接访问 concrete registry，就会绕过这一边界。

Iceberg 的 metadata alias、冻结 snapshot 文件和 reader-specific layout 是 provider-owned read correctness 的一部分。将它们拆成 `IcebergTableInfo`、`IcebergDataFileInfo`、metadata-table proto variant 或 Core 拼装的 JSON handle，会迫使 native encoder、decoder 和 query-local binding 知道 Iceberg 的私有语义。这样既阻止新 provider 使用相同读路径，也使 BE 端从通用 carrier 回退到 provider 分支。

现有 `ConnectorReadSource` 已能承载 instance identity、opaque table/split payload 和 Arrow schema；BE 通过已安装的同 incarnation execution binding 打开 correctness-complete reader。它应成为所有 Connector read 的唯一 native carrier，而不是只作为普通表的可选路径。

## 考虑过的选项

1. 保留 registry 直读和 Iceberg 专用 read carrier。改动最小，也容易沿用 metadata-table reader，但 exact generation 不能在所有 catalog 操作中证明，且 wire 会持续累积 provider variant。
2. 将 snapshot、files、metadata-table 类型、view 定义等扩展为通用 protobuf 字段。FE/BE 可直接检查更多事实，但会把 table-format 演进固化为 native protocol，并要求每个 provider 迁就 Iceberg 的数据模型。
3. 在 Connector control 上增加 namespace、read-reference facts 和可选 view metadata capability；将一次 read admission 封存为 opaque table handle、SPI Arrow schema、read selector、可选 statistics pin 与 exact planning lease；所有 read 经 `ConnectorReadSource` 提交。选择此方案。
4. 让 Core 保留 provider-private adapter，但在 binding 或 native wire 中同时维护中立和专用两条路径。过渡期风险较低，但 dual path 会让调用方继续选择 latest fallback，无法验证旧 carrier 已真正退役。

## 裁决

采用选项 3。`ConnectorMetadata` 负责 namespace 枚举与 bounded、可验证的 read-reference facts；view 以独立、可选的 `ConnectorViewMetadata` capability 表达。每个调用先取得 `ConnectorControlPlanningLease`，再只经该 lease 的 binding 调用 capability。未声明 view capability 是 typed `Unsupported`，不是“view 不存在”；provider 的不存在、冲突和损坏错误仍保持 typed 分类。

query-local read materialization 只存 `ConnectorTableHandle`、`SchemaRef`、`ConnectorReadSelector`、可选 `ResolvedTableStatisticsPin` 和 exact lease。provider 在 admission 时把 snapshot 文件、metadata alias、partition allow-list、hidden-column filtering 与 table-format layout 封入 opaque handle 或 provider reader binding；Core 根据 SPI schema 计算 projection ordinal，并在缺列、重复列或 returned-schema 不一致时 fail fast。SQL/DML/MV 所需的 identity、target contract 和用户可见事实留在各自 SQL/application owner，不从 opaque handle 反序列化。

native plan 只以 `ConnectorReadSource` 表示 Connector read。旧 Iceberg data-files、metadata-table 与 version-table oneof 字段及对应 encoder/decoder 退役并 reserve 编号/名称；仍独立拥有 MV 生命周期语义的专用事实不借此裁决改变。BE 继续只 lookup query-scoped、已安装的 execution binding，绝不根据 provider ID materialize、读取 latest metadata 或走 all-in-one 直接调用。

## 接受的妥协（诚实记录）

这会使 Iceberg provider 需要承担过去由 Core 代管的 metadata alias、冻结文件与可见 schema 适配，并在 opaque payload 中保存少量重复的 provider facts。选择它是为了把 correctness owner 和 protocol 边界一次对齐，而不是因为 provider 实现更少或调试更简单。排查问题时，通用 wire 也不再直接展示文件和 metadata-table kind；必须依赖 provider 的受限诊断和 admission 测试。

Core 仍暂时包含 Iceberg application adapter，且 DML/MV 的 durable lifecycle 仍可能保存 provider-specific evidence。这里没有立即删除 concrete runtime registry、没有把 provider 拆为独立 Cargo crate，也没有把 MV target state/locator 重写成新 wire。这些边界保留是为了将 catalog/read carrier 迁移控制在可验证的原子改动内，并非认为当前物理 crate 或 MV wire 已经是最终形态。

可选 view capability 会让不支持 view 的 provider 在 SQL 入口返回显式 Unsupported，而不是模拟空列表或隐式退回 legacy REST 判断。短期会暴露过去被分支掩盖的产品差异；这是为了防止“缺 capability”被错误解释为“对象不存在”。

## 何时重新评估

- 新 provider 无法以 opaque handle、SPI schema 和 selector 表达正确读取，且需要把稳定、跨 provider 的 read facts 升格为 SPI value contract 时；
- 生产 workload 证明 opaque admission payload 的资源上限不足，或 provider-private diagnostics 无法支撑故障定位时；
- MV target state/locator、durable recovery 或跨进程 compiler output 需要共享新的版本化 read evidence 时；
- 多 provider 都实现 view metadata 后，view 定义、security definer 或 dependency graph 需要超出当前 optional capability 的统一语义时；
- concrete registry 删除、Connector 独立 crate 迁移或 native protocol 版本协商改变时；这些变化必须通过新的 ADR 重新裁决。
