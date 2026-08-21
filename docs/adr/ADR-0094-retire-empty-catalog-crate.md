---
id: ADR-0094
title: "Retire an empty catalog crate after owner convergence"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-20
provenance:
  - "discussion: 2026-08-20 catalog vocabulary and runtime owner convergence"
  - "PR: pending — catalog crate retirement"
code-anchors:
  - "Cargo.toml (workspace members)"
  - "novarocks/types/src/naming.rs (catalog naming vocabulary)"
  - "novarocks/frontend/src/catalog_application/query_catalog.rs (Frontend query catalog runtime)"
---

## 问题

当一个 crate 的共享词汇、运行时容器和死路径都已有明确归属时，应继续保留它作为空 facade，还是在同一次 owner cut 中删除该 crate？

## 背景与执行事实

旧 catalog crate 曾同时承载三类不同性质的内容：名称规范化和 schema 等纯值词汇、SQL 本地内存表目录、以及 Frontend 的 registry、schema cache 与 query catalog service。它不再是一个独立领域：纯值词汇只依赖 Arrow 与标准库，SQL 本地目录只保存 SQL 私有的 `TableDef`，而 registry、cache、connector admission 与读写锁快照只能由 Frontend application runtime 正确拥有。

本次收敛后，`novarocks-types` 拥有 naming/schema value vocabulary，`novarocks-sql` 拥有 `PlannerMemoryCatalog` 与其 provider materialization，`novarocks-frontend` 拥有具体 `QueryCatalogService`、`CatalogRegistry` 与 `SchemaCache`。遗留 range partition 存储和单实例 `CatalogProvider` 已被证明无生产调用并一并删除。没有 consumer 需要 catalog crate 的独立 API，也没有新 crate 依赖边。

这补充 ADR-0058：Cargo 依赖图应表达真实的架构边界，而不是用源码扫描维护历史形状。这里的结论不是“叶子 crate 越少越好”，而是空边界不应伪装成领域隔离。

## 考虑过的选项

1. **保留 catalog crate 作为 re-export facade。** 迁移调用点较少，但保留两条可见路径和错误 owner，未来修改者仍会把无状态词汇或 Frontend runtime 塞回这个名字。
2. **把所有内容集中到一个新的通用 catalog runtime crate。** 表面统一，却会让 SQL 私有 `TableDef`、Frontend connector lifecycle 与无状态 naming/schema 重新共居，并扩大依赖图。
3. **按唯一真实 owner 收敛后删除空 crate。** 词汇进入已有中立 owner，运行时能力留在唯一 application owner，SQL 私有目录留在 SQL；Cargo member 与所有依赖行同时移除。（采纳）
4. **保留 crate 并新增 source-shape guard 防止回流。** 这重复 ADR-0058 已否决的脆弱机制，而且无法解释什么才是该 crate 的独立领域。

## 裁决

仅当以下条件同时成立时，删除空 crate：共享词汇已有中立 owner；每项运行时能力恰有一个生产 owner；剩余路径已死亡；迁移不引入依赖边；并且原 crate 不再提供独立领域或 API。

catalog crate 满足这些条件，因此删除它而不保留 facade、alias、re-export、双路径或迁移开关。`Catalog` trait 仍作为 Frontend 内部动态派发契约保留，但其元数据类型固定为 Frontend 的 `CatalogRuntimeMetadata`；这不是把 trait 提升为新的共享基础。SQL 与 Frontend 的公开调用路径保持在各自真实 owner 下。

## 接受的妥协（诚实记录）

`SqlType` 与 `types::LogicalType` 的语义重复在本次之后仍然存在，尽管两者现在都位于 `novarocks-types`。这是为了先消除错误 crate owner 和反向依赖，而不是因为重复已经得到设计解决；强行在同一 owner cut 中统一它们会扩大语义与 wire 风险。选择分两步是为控制改动成本和验证面，并非认定两种类型模型天然更优。

删除 crate 也减少了一个显式 Cargo 边界；未来若某项能力重新出现多个独立 production consumer、需要稳定的独立 API 或必须与 SQL/Frontend 生命周期隔离，就不能以“曾经删过”拒绝重新建立有真实领域语义的 crate。

## 何时重新评估

- 命名/schema 词汇需要依赖 runtime、SPI、SQL 或 Frontend 任一上层 crate 时；
- SQL 本地目录需要被 Frontend 以外的 production crate 构造、检查或持久化时；
- query catalog registry/cache 需要跨 Frontend process 共享独立生命周期或稳定 API 时；
- 新 catalog 能力不能在现有 owner 内保持单一 authority，且能证明独立 crate 比 owner-local module 更能表达依赖和故障边界时；
- 对 `SqlType` 与 `LogicalType` 的统一已有明确语义、wire 与兼容性裁决时。
