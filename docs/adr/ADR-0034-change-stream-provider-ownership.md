---
id: ADR-0034
title: "Change-stream layout and provider ownership"
domain: [frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "PR: pending"
  - "discussion: 2026-08-03 change-stream owner closeout"
code-anchors:
  - "novarocks/core/src/sql/planner/distributed/write/change_stream.rs (bind_change_stream_write_layout)"
  - "novarocks/core/src/connector/iceberg/change_stream_write.rs (bind_iceberg_change_stream_provider)"
---

## 问题

当 UPDATE、MERGE 与 MV 都需要 Iceberg change-stream writer 时，怎样分配 layout、provider commit 与 application lifecycle，才能保持一个 aggregate commit 且不制造第二个 DML owner？

## 背景与执行事实

ADR-0033 已将 UPDATE/MERGE 的 statement admission、durable intent 与 terminal lifecycle 固定在 frontend。change-stream 本身不是新的 statement application use case：它由 SQL 产出 immutable writer topology，由 Iceberg Connector 解读 sink、writer handle 与 staged reports。ADR-0023 的 operation/cohort/execution/writer identity 仍是 exact session 的唯一提交依据。

过去的 engine helper 同时绑定 output ordinal、DV frozen facts、commit routing 和 lazy control service，导致 mutation 与 MV 都依赖同一 application-private helper，并可能让 planner topology 与 provider terminal handles 分别派生。

## 考虑过的选项

1. 让 frontend 为 change-stream 建立独立 service、DTO 与 journal kind。这样会让 frontend 了解 MOR branch、Iceberg reports 和 provider payload，并把内部执行形态误升格为 application use case。
2. 保留 engine 的通用 helper，由 DML 与 MV 继续调用。改动最小，但 SQL layout、Connector external truth 与 application composition 持续混在一个 owner 内。
3. 让 SQL 绑定 layout，Connector 从同一 topology 创建 provider binding，DML/MV 仅消费该 binding。这样保留两个独立 application lifecycle，同时让 provider commit 保持单一事实源。

## 裁决

采用选项 3。

SQL planner 的 canonical layout API 负责 internal/user-visible output binding、partition/data-route validation、ordered branch id 与 immutable DAG/topology。mutation kernel 只提供有序逻辑 branch intent；UPDATE MOR 固定为 DeleteDv 后 ReuseData，MERGE 只提供其 statement action 的合法子集。

Iceberg Connector 从一个 frozen topology 同时派生 terminal handle map、provider payload、activation digest、change-stream committer 与 aggregate commit plan。ready registration 与 exact-lease lazy activation 使用同一 binding。Connector 拥有 commit、abort、report conversion、cleanup 与 recovery evidence；application owner 保留自身 cache invalidation 和 durable lifecycle。

exact session 在已证明零 loaded rows、零 staged report frames 时可本地终结为 known-empty NoOp；其他 session 后错误必须带同一 exact abort capability。DML 与 MV 共享 provider binding，但不共享 attempt ledger、publication、scheduler 或 frontend mutation lifecycle。

## 接受的妥协（诚实记录）

这没有把 change-stream 抽成独立 crate 或新的 SPI：当前 Iceberg payload、control service 与 commit collector 仍在同一 core crate，先以 crate-private seam 固定真实 owner。这样是为了避免在未需要跨 provider 复用时扩大稳定接口，不是因为现有 Iceberg 形状天然是通用 SPI。

MV 的 test short-circuit 仍从 immutable topology 调用 Connector canonical commit-plan derivation；它保留测试可观察性，但生产 physical planner 不再提前保存或路由该 plan。

## 何时重新评估

- 第二个 table-format Connector 需要同一 change-stream provider binding，且其 payload/receipt 无法复用当前 crate-private contract 时。
- SQL planner 无法在 external side effect 前完整验证一个新 branch kind 或需要 provider 反向分配 writer fragment 时。
- MV 与 DML 需要共享 durable lifecycle、publication 或 recovery，而不仅是 provider binding 时。
- exact session 无法证明空聚合，或 aggregate commit 不再能保证单 operation、单 terminal decision、单 snapshot 时。
