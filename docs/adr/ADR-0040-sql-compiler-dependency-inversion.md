---
id: ADR-0040
title: "SQL compiler dependency inversion closure"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "discussion: 2026-08-02 SQL compiler dependency inversion"
code-anchors:
  - "novarocks/core/src/sql/compiler/mod.rs (SqlCompiler::compile)"
  - "novarocks/core/src/engine/query_planning/mod.rs (QueryPlanningInputs)"
---

## 问题

在独立 SQL crate 物理迁移之前，如何让聚合 Core 中的 SQL compiler 只依赖 SQL-owned vocabulary、共享类型和窄 SPI 契约，同时不改变 Connector、native wire 或应用生命周期的 owner？

## 背景与执行事实

`SqlCompiler::compile` 是 statement admission 后唯一的 canonical compiler kernel。它需要 catalog、statistics、函数、MV rewrite、deadline 和 cancellation 等冻结事实，但不应知道 Frontend session、repository、Connector registry、native encoder、query lifecycle 或执行 profile 的 concrete type。

一次 query 对外部表的解析、统计和 scan preparation 必须使用同一个 exact Connector generation。把 provider handle、lease 或 files 放入 SQL plan，会让 compiler 反向依赖 concrete Connector；每个 consumer 重新获取 current binding，则会在 metadata 解析与 fragment submission 之间破坏 generation fence。

EXPLAIN profile、native DTO、prepared write、MV operation/cohort 和 refresh publication 都属于 application 或 execution 的生命周期事实。它们可以消费 SQL 产物，但不是 compiler input 或 output。Variant、Bitmap 和 HLL 的值格式同时被 SQL 和 runtime 使用，因此也不能继续由其中任一方的执行实现隐式拥有。

## 考虑过的选项

1. 保留 SQL 对 Core execution、MV application 和 concrete Connector 的直接依赖。局部改动最少，但无法形成可迁移的依赖闭包，也允许 exact binding 在不同阶段被替换。
2. 向 compiler 传递完整 request context、service locator 或带回调的 provider facade。短期可减少参数数量，但把 owner 和失败语义藏在万能 context 中，无法审计或独立测试。
3. 将 SQL 输入、scan facts、statistics、function semantics、控制观察和解释 profile 定义为 SQL-owned value/trait；application 在 `QueryPlanningInputs` 中组装 request-local binding store 与 post-compile context。选择此方案。
4. 立即创建独立 crate。它能由 Cargo 图强制边界，但会把未闭合的 native/application 依赖机械搬运，增加本次变更风险，延后到依赖已闭合后进行。

## 裁决

SQL compiler 只消费 SQL-owned compile request、immutable snapshot/value 和允许的窄 SPI fact，并只产出 analysis、logical、optimized、explain 或 distributed SQL facts。SQL scan 以不可序列化、request-local binding token 标识外部表；application-owned `QueryTableBindingStore` 以该 token 保留 exact lease、handle、incarnation、版本和 provider authority。catalog、statistics 和 preparation 必须通过同一 token 读取，缺失、跨 scope、owner/incarnation/version 不匹配或损坏 evidence 均在 submission 前以 typed error 失败。

application 在 `QueryPlanningInputs` 中拥有 compile request 以外的 `PostCompilePlanningContext`，负责 Connector control、profile/cancellation 投影、scan preparation、native encoding 和 execution request 组装。SQL 拥有 TopN、change route/action、row-lineage、partition 和 MV planning vocabulary；native encoder 和 execution 只做显式转换。共享值格式由 `novarocks-types` 拥有。MV first-refresh artifact 只包含 SQL plan、shape、target contract、root distribution 和 binding token；operation、cohort、prepared write、staging、publish、recovery 与用户结果继续留在 application/provider owner。

## 接受的妥协（诚实记录）

SQL 仍暂存于聚合 Core，因此 Cargo 还不能独立阻止新的反向 import；本裁决依赖 source audit、测试 owner 和 code review 保持闭包。选择先完成依赖倒置而不立刻拆 crate，是为了避免把 native/application 细节连同实现一起复制，并非因为当前 package 边界已经足够强。

token 只在单个进程、单次 request 的 binding store 中有效，不能序列化、缓存或用于重启恢复。这会增加 application adapter 和显式转换的样板代码，也意味着 durable MV/DML recovery 必须继续保存自己的 provider evidence，而不能试图恢复 compiler token。该限制是为了让 stale generation fail closed，而不是为了简化调用方。

## 何时重新评估

- `sql/**` 已通过完整 source/Cargo audit，且需要将闭合 compiler 物理迁入独立 crate 时；
- 新 Connector 不能以 token-bound、request-local facts 支持 catalog、statistics 与 preparation 的同 generation 读取时；
- 产品需要 compiler output 直接跨进程传输，必须定义独立版本化 wire fact 而非序列化 binding token 时；
- 新的 shared value format 需要超出 `novarocks-types` 的稳定公开契约时；
- native wire、DML/MV lifecycle 或 Connector external failure contract 需要改变时；这些改变必须由各自 owner 的新 ADR 裁决。
