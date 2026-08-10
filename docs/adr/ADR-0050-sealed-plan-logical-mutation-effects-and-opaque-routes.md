---
id: ADR-0050
title: "Sealed plans carry logical mutation effects and opaque provider routes"
domain: [sql-compiler, provider-spi]
status: active
supersedes: [ADR-0042]
superseded-by: null
date: 2026-08-10
provenance:
  - "discussion: 2026-08-09 logical row-mutation carrier"
  - "implementation: sealed plan route carrier neutralization"
code-anchors:
  - "novarocks/core/src/sql/plan_read.rs (DistributedPlanRead)"
  - "idl/novarocks/plan.proto (RowMutationEffect)"
---

## 问题

sealed `DistributedPlan` 如何继续作为跨 owner 的只读门面，同时携带行级 DML 的逻辑语义而不把 provider 的物理 branch 策略固化为 execution-neutral value？

## 背景与执行事实

`DistributedPlanRead` 使 encoder、execution assembly 与应用 owner 读取同一 immutable SQL 结果，而不暴露 compiler-private mutable graph。旧 carrier 将 `DeleteDv`、`ReuseData`、`FreshData` 及 `change_op/data_route` 放入 topology 和 native wire；这些值实际编码了 Iceberg strategy，不能被其他 provider 或通用 execution 正确解释。

Provider-signed route preparation 现已能绑定 route id、accepted logical effects 与 token-to-ordinal input shape。native plan 是单次 attempt 的 FE 到 BE 临时 contract，而不是跨版本持久化格式。

## 考虑过的选项

1. 将旧 branch enum 留在 sealed plan，并约定新 provider 忽略它。这样保留两套语义和错误的 execution-neutral 断言。
2. 让 encoder 从 provider payload 重新推导 effect 与 route。encoder 会变成 provider-aware planner，并破坏 sealed plan 的只读性。
3. sealed plan 只携带 logical effect slot、opaque 32-byte route id、accepted-effect set 和 token-bound input ordinals；provider payload 仍只由 provider 解读。

## 裁决

采用选项 3。SQL planner 表达 `Delete`、`Replace`、`Insert`，route topology 表达 opaque route id 和可接受 effect 集合。execution expand 只 materialize logical effect，split sink 依 effect 集合独立过滤，允许一个 `Replace` fanout 到 delete 与 replacement routes。`change_op/data_route` 与物理 branch enum 从 active carrier 移除，旧 proto field number/name/enum value 保留为 reserved。

`DistributedPlanRead` 仍是只读 facade：native encoder、Core decoder、Backend decoder 和 assembly 只消费同一 immutable carrier。FE 与 BE 以原子版本部署；旧/新组合 fail closed，不引入 compatibility decoder 或 top-level plan version。

## 接受的妥协（诚实记录）

logical effect 与 opaque route 增加了 router validation 和 negative wire tests，不能依赖旧 enum 的紧凑数值映射。原子部署限制了滚动升级自由度，但避免 attempt 内对同一 route 使用两种不等价解释。

## 何时重新评估

当 native plan 需要脱离 attempt 长期持久化、必须支持 mixed-version FE/BE，或新的 mutation 类型不能由逻辑 effect 和 provider route 表达时，重新裁决兼容和 carrier 演进机制。
