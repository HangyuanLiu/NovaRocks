---
id: ADR-0041
title: "Sealed distributed plan read contract"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-08-08
provenance:
  - "implementation: sealed distributed plan read contract"
  - "discussion: 2026-08-08 SQL compiler crate extraction boundary"
code-anchors:
  - "novarocks/core/src/sql/plan_read.rs (public read facade)"
  - "novarocks/types/src/change_stream.rs (neutral change-stream value contract)"
---

# ADR-0041：为何以同形状只读契约公开 sealed DistributedPlan

## 问题

当 native encoder 在 SQL owner 之外读取 sealed `DistributedPlan` 时，如何冻结其所需的公共读取契约，同时不泄漏 draft、seal、validation 或可变构造能力？

## 背景与执行事实

`DistributedPlan` 是 SQL compiler 在完成结构、拓扑、输出和写入契约校验后产生的不可变结果。native encoder 只把其中已经冻结的事实映射为 wire DTO；它不重建 plan，也不决定 SQL 语义。此前 encoder 通过许多 SQL 内部模块路径读取这些事实，Backend 对 change-stream branch kind 也直接依赖 SQL 类型。

ADR-0040 已裁决 compiler 应先完成依赖倒置闭包再物理迁移。迁移前若让消费者逐个穿透 SQL 内部模块，公开 API 会由迁移时的临时编译错误决定，且无法表达「可读取、不可构造」这一 owner 边界。

## 考虑过的选项

1. 为 encoder 建立一份镜像 plan DTO。它可把 SQL 内部类型完全隐藏，但要复制全部 sealed 形状、转换和一致性验证；每次 plan 演进都需要两份模型同步，容易重新引入 encoder 侧的语义重建。
2. 立即把 native encoder 迁入 Frontend。这样可让 encoder 与消费 owner 同处，但会把物理移动、调用方调整和本次契约冻结混为一个行为风险更高的改动。
3. 公开 SQL 全部 planner 模块。实现成本最低，却把 draft、builder、validation 与偶然内部细节一起冻成长期 API，无法阻止其他 owner 越过边界。
4. 通过单一 `sql::plan_read` 门面公开 sealed plan 的同一数据形状，并把构造路径保持 SQL 私有。

## 裁决

选择选项 4。`sql::plan_read` 是跨 owner 读取 sealed plan 的唯一公共入口。它平铺导出 encoder 实际读取的 distributed、physical、analysis、common 和 write 数据形状，并以 `table` 与 `runtime_filter` 子投影承接仍需模块分组的只读事实。

`DistributedPlan` 保持私有 data carrier；只有借用式或 copy-value 的读取访问器公开。draft、builder、seal、validation error、seal 入口和任何可变 API 都不进入门面。encoder 保持原位置、签名和 wire 映射，只把 SQL import 收敛到该门面。

change-stream branch kind 属于 execution-neutral 值语义，置于 `novarocks-types`。SQL planning 与 proto decode 都以穷举转换取得该中立值；未知或 unspecified wire 值继续失败，不以默认值修复。

## 接受的妥协（诚实记录）

这里提前冻结了一小块 public surface，代价是以后改变 sealed plan 读取形状要维护兼容性或显式升级契约。选择它是为了降低即将到来的 crate 迁移风险、让 owner 边界可编译验证，并不是因为长期公开 SQL 类型天然优于专用 DTO。

本次没有迁移 encoder。它仍在聚合 core 中，因而 crate 层物理依赖尚未完全体现最终架构；这是为了把结构性公共契约变更与大规模移动、测试和调用方调整分开，避免把行为问题归因不清。

## 何时重新评估

- encoder 被迁入 Frontend，且可以在不复制 SQL 语义的前提下消费独立的冻结 DTO 时；
- 新 consumer 需要的事实无法通过借用式读取表达，或者公开形状反复迫使 SQL 增加不自然的兼容层时；
- sealed plan 需要跨进程、跨版本持久化，届时应评估版本化 wire DTO 而非 Rust public type surface；
- public read contract 出现新增可变 API、draft/seal 泄漏，或 owner guard 无法以语义规则维护时。
