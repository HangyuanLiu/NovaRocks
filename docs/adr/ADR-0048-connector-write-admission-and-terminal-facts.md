---
id: ADR-0048
title: "Connector write admission and durable terminal facts"
domain: [provider-spi, frontend-dml]
status: superseded
supersedes: []
superseded-by: ADR-0051
date: 2026-08-09
provenance:
  - "discussion: 2026-08-09 Connector write admission and terminal-write neutralization"
  - "PR: pending local implementation"
code-anchors:
  - "novarocks/spi/src/connector/write.rs (ConnectorWriteControl)"
---

## 问题

如何让一次已冻结的 Connector table admission 签发完整的 distributed-write 输入 authority，并让 Frontend 持久化终态事实而不重新解释 Provider 的 commit 语义？

## 背景与执行事实

既有 `ConnectorWriteControl` 已由 FE 聚合 operation、cohort、attempt 与单一 terminal latch，并由 Provider 执行外部 commit、abort 与 reconcile。此前 planning request 仍允许 application caller 同时提交 table handle、Arrow schema 和任意 provider payload，且 terminal path 会把 receipt/evidence 再投影为 Iceberg application DTO。这样无法证明这些事实来自同一 control generation，也让 durable DML journal 依赖 table-format 语义。

## 考虑过的选项

第一种是保留 caller-assembled payload，只在 Provider plan 时做更多校验。它改动较小，但 caller 仍可形成来自不同 admission 的组合，无法建立 exact-generation authority。

第二种是为 row DML 新建独立 `ConnectorDmlPlanning` capability。它可局部表达策略，但会制造第二个 write control family、重复 lease/terminal ownership，并在 Provider crate 迁移前固化错误边界。

第三种是扩展现有 `ConnectorWriteControl`，让 Provider 签发 sealed preparation，并让通用层只处理 typed opaque facts。

## 裁决

采用第三种。Provider 在 retained exact write lease 上接收 SQL-owned intent 与 tagged input request，返回 owner/table/base-version/input-shape/opaque-handle 共同摘要的 `ConnectorWritePreparation` 或 typed deny。field token 仅在该 preparation 内稳定，不暴露 table-format field/source ID。later planning 接收 sealed preparation、execution ID 与 frozen writer manifest，不能再接收 caller payload。

Frontend 持久化 tagged terminal lifecycle：committed 保存 SPI-owned bounded receipt wire，unknown 保存既有 evidence wire；Frontend 不解码 provider payload。普通 DML journal 作为无历史数据的内部格式原子切换，不维护旧 schema 读取、迁移或双写。cohort/attempt/aggregate commit ownership继续属于通用 operation session；row-mutation physical strategy另行演进。

## 接受的妥协（诚实记录）

该裁决迫使所有真实 provider、fake 与 consumer 在同一契约切换中更新，短期改动面明显大于增加验证函数。选择它是为了阻止 authority 拼装和 durable provider DTO 泄漏，不是因为 preparation 类型本身更小。SPI 公开的 tagged shapes 也只能表达跨 Provider 的 Arrow layout/role；具体 partition、delete、storage 与 compression policy 留在 opaque handle，短期仍需要 Iceberg adapter 承接已有 row-DML 策略。

## 何时重新评估

- 新 Provider 需要无法由当前五种 tagged input shape 表达、且确实属于跨 Provider contract 的写入角色。
- 产品要求 durable journal 由外部工具直接读取，因而需要版本化迁移与公开兼容承诺。
- 多 incarnation takeover 改变了 retained lease 不能覆盖整个 terminal 决策的前提。
- row-DML strategy owner 完成独立迁移后，临时 Iceberg adapter 不再是唯一实现。
