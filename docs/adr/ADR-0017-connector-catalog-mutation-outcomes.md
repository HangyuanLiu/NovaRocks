---
id: ADR-0017
title: "Connector catalog mutation outcomes"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-07-30
provenance:
  - "discussion: 2026-07-30 SPI-4B connector catalog mutation outcomes"
code-anchors:
  - "novarocks/spi/src/connector/mutation.rs (FE-only mutation contract and external outcome)"
  - "novarocks/frontend/src/connector/control_host.rs (generation-fenced mutation lease)"
  - "novarocks/core/src/connector/iceberg/provider.rs (Iceberg semantic commit and reconcile adapter)"
---

## 问题

外部 catalog mutation 如何在不泄漏 Iceberg 类型或让 BE 获得控制能力的前提下，可靠区分已提交、未提交和未知提交状态？

## 背景与执行事实

SPI-4A 已分开 FE control binding 与 BE execution binding，但 namespace、table、view、schema、partition、properties 与 ref 的 mutation 仍走 core 的 `CatalogBackend`，并以 `Result<(), String>` 表达外部失败。该路径无法原子承载存在性策略，也无法在 response loss 或 post-commit cleanup failure 时安全决定重试、返回或 reconcile。

## 考虑过的选项

1. 保留 core backend 并补充错误字符串分类。改动较小，但仍存在 TOCTOU、provider 类型泄漏和不可验证的 commit 语义。
2. 让 frontend 用 metadata precheck 后直接重试。可复用现有 read SPI，但外部状态会在 read/write 间变化，且网络错误不能证明未提交。
3. 在 FE control binding 增加 provider-neutral mutation capability，使用 generation-fenced lease 和三态 external outcome；provider 对未知结果做 authoritative reconcile。

## 裁决

采用选项 3。mutation 是 optional FE-only `ConnectorControlBinding` capability；application 只能通过 narrow resolver 获得精确 `{instance_id, incarnation}` lease。BE execution binding 不声明、安装或解析 mutation capability。

SPI 的 typed request 包含 namespace/table/view/schema/partition/properties/ref operation 与存在性策略，但不携带 SQL AST、Iceberg metadata/client 或 runtime object。每次 execute 返回 `KnownCommitted`、`KnownUncommitted` 或 `CommitUnknown`；unknown 携带 bounded、versioned、redacted evidence，调用方仅可用同一 lease 做一次 authoritative reconcile，不能 blind replay。semantic commit 与 cleanup/finalization 分离：cleanup failure 仍为 committed。

## 接受的妥协（诚实记录）

同步 DDL 不建立 durable journal 或后台 reconciler；deadline 内仍无法证明状态时将错误显式暴露为 `CommitUnknown`。evidence 设计为可持久化，以便后续 write lifecycle 引入 frontend-owned journal 时复用。每 generation 的 lease fencing 保护 NovaRocks 内部 lifecycle；provider 仍须为外部并发实现自身的 conditional/CAS 语义。

## 何时重新评估

- 多 FE failover 或跨进程 DDL takeover 需要 durable evidence journal；
- 新 provider 不能以 typed operation 表达其外部原子动作；
- DML staging/commit 需要同一 outcome vocabulary 的持久化恢复；
- provider 的外部 conditional create/CAS 能力不足以实现产品声明的存在性语义。
