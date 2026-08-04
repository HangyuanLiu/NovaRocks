---
id: ADR-0035
title: "Frontend ADD FILES source ownership uses provider scopes"
domain: [frontend-dml, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "implementation: frontend ADD FILES durable source ownership"
  - "discussion: 2026-08-03 frontend data-mutation lifecycle"
code-anchors:
  - "novarocks/spi/src/connector/data_mutation.rs (ConnectorDataMutationSourceScope)"
---

## 问题

在不把完整文件清单持久化到 frontend 的前提下，`ADD FILES` 如何在提交结果未知时阻止同一物理来源被再次注册？

## 背景与执行事实

`ConnectorDataMutation` 的 provider 在 planning 时拥有目录枚举、对象存储规范化、Parquet 校验和完整 frozen manifest；frontend 只拥有 admitted statement 的 durable operation、StateStore journal 和用户可见 lifecycle。完整 manifest 最多可含 4096 个文件，不适合作为 frontend durable DTO，也不能由 frontend 从原始 URI 猜测对象存储、HDFS 或 endpoint 的物理同一性。

外部提交的三态结果意味着 `CommitUnknown` 不能释放来源：提交可能已经写入 Iceberg snapshot，而再次注册、修改或删除该来源会破坏正确性。exact connector incarnation 又是短生命周期资源，因此进程重启后不能用 current incarnation 猜测执行旧 plan 或 reconcile 旧 evidence。

## 考虑过的选项

1. frontend 持久化逐文件 manifest 并逐文件加锁。它可以表达精细 overlap，但复制 provider 的文件事实，突破 SPI/StateStore 边界，并要求新的 manifest GC、保留和隐私策略。
2. frontend 以原始 SQL URI 或字符串规范化计算目录锁。实现简单，但无法正确处理 credential、endpoint、storage family、HDFS authority 和 provider 特有的 canonicalization。
3. provider 在 public plan 中返回 secret-free、operation-independent 的 canonical source-scope digest，frontend 以该 scope 做 durable ownership transition。它保持 manifest 私有，同时可在 operation 间保护同一物理来源。

## 裁决

采用 provider-calculated `ConnectorDataMutationSourceScope`。首版 scope kind 固定为 directory，包含版本、kind 与 32-byte digest；digest 必须由 provider 基于物理来源计算，且不得绑定 operation、target table、connector instance 或 manifest。

`RegisterExistingFiles` plan 必须携带 scope，`Truncate` 必须不携带 scope，scope 纳入 plan digest 和 canonical durable wire。frontend 以 scope digest 建立 `ReservedImmutable`、`Frozen` 或 `TableOwned` 的 durable ownership record：known-uncommitted 才释放；unknown/possibly-dispatched 冻结；known-committed 永久转为 table-owned。恢复只做无需 provider 副作用的本地分类；不在新 incarnation 执行或 reconcile 旧 operation。

## 接受的妥协（诚实记录）

目录级 scope 比逐文件 overlap 更保守：一个已成功注册或被冻结的目录不能靠替换目录内容再次使用，用户必须改用新的 source location。我们选择它是因为当前不拥有 durable manifest service、也不能让 frontend 复制 Connector 的完整文件事实，并非因为目录级锁在所有产品场景都更灵活。

same-incarnation 的 unresolved 状态会在 FE restart 后停留为 manual inspection，不能自动恢复。这牺牲了可用性，换取 exact-generation contract 不被 current-binding fallback 破坏。

## 何时重新评估

- 产品明确要求复用来源目录、部分 overlap 检测或安全释放 `TableOwned` scope；
- 需要跨 FE / 跨 connector incarnation 的 authoritative marker inspection 或自动 reconcile；
- 系统引入 provider-owned durable manifest ledger，并具备明确 publication、retention、GC 与访问控制语义；
- StateStore 的原子事务无法容纳 operation、artifact 与 source ownership 的完整 bounded 写入。
