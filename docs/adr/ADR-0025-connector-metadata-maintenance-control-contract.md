---
id: ADR-0025
title: "Connector metadata maintenance control contract"
domain: [provider-spi, table-maintenance]
status: active
supersedes: []
superseded-by: null
date: 2026-08-02
provenance:
  - "discussion: 2026-08-01 connector metadata maintenance control"
code-anchors:
  - "novarocks/spi/src/connector/metadata_maintenance.rs (ConnectorMetadataMaintenance)"
---

## 问题

不需要后端 Arrow staging 的 Iceberg metadata maintenance，如何在 frontend 持久化其精确 Connector generation、冻结
plan，并在 catalog 响应丢失后安全恢复？

## 背景与执行事实

`REWRITE MANIFESTS` 通过 replace snapshot 改写 metadata layout；`EXPIRE SNAPSHOTS` 则以 metadata commit 删除历史
snapshot，随后才 best-effort 删除物理对象。两者都不需要 BE fragment 或 writer，但当前 concrete core caller 会在每次
retry 时重新读取并解释表状态。

catalog commit 的响应可能在提交后丢失。此时 current binding 不能替代创建 operation 时的 generation，且“找不到
marker”不能证明没有提交。StateStore 单值限制也不能承载大型 manifest group 或 cleanup 路径集合。

## 考虑过的选项

1. 复用 distributed writer。可以复用部分 commit vocabulary，但会制造零 writer 的伪数据面，并污染 native/BE carrier。
2. 继续由 core concrete maintenance enum 直接调 Iceberg。短期改动较小，但 frontend 无法拥有 durable operation 和
   exact-generation recovery。
3. 建立 FE-only metadata maintenance capability。planning、execute、reconcile 绑定 exact lease，provider 独占
   Iceberg metadata/artifact truth，并复用三态 external outcome。

## 裁决

采用独立的 `ConnectorMetadataMaintenance` capability；首版仅有 `RewriteMetadataLayout` 与
`ExpireTableVersions`。每次 SQL 先持久化 operation 和 immutable plan，再执行一次；unknown 只 reconcile，不重发。

- `REWRITE MANIFESTS` 在同一 replace snapshot summary 写 marker。
- `EXPIRE SNAPSHOTS` 在同一 `TableCommit` 中提交 `RemoveSnapshots` 与 reserved property marker。
- exact generation 缺失时 durable operation 进入 `Unresolved`，不得让 current incarnation 接管。
- provider-owned artifact 位于 table root 的 `_novarocks/maintenance/v1/`，以有界、不可变、带 digest 的 part 保存；SPI 与
  StateStore 只保存 bounded handle。
- capability 不提供 public abort；BE execution binding、native proto、compat 和 writer contract 不增加 carrier。

## 接受的妥协（诚实记录）

首版把 control artifact 限为 64 个 1 MiB parts、64 MiB 总量和 262,144 records。这个限制不是 Iceberg 的理论限制，
而是为了让 recovery 不依赖无界内存、StateStore 或 process-local map 所做的工程折衷；超限必须失败。

terminal artifact 先 best-effort 删除，残留由后续 cleanup capability 在七天后回收。我们接受短暂甚至因
`Unresolved` 而长期存在的私有对象，以换取不在 unknown 时误删仍可能被提交引用的文件。

## 何时重新评估

- 真实 workload 超过 artifact 固定预算。
- catalog 无法原子提交 expire marker 与 `RemoveSnapshots`。
- 产品需要 BE data rewrite 或 clean-up batch；它们分别进入 SPI-4E2/E3，而非扩大本 capability。
- 多 FE takeover 已有 durable generation fence，能够安全接管 `Unresolved` operation。
