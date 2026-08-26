---
id: ADR-0116
title: "DROP CATALOG materialized-view reference check is a best-effort operational guard"
domain: [catalog-attachment]
status: active
supersedes: []
superseded-by: null
date: 2026-08-26
provenance:
  - "PR: pending; mechanism: closed frontend state family manifest and catalog desired-state source modes"
  - "discussion: 2026-08-25, catalog projection versus rebuildable MV accelerator"
code-anchors:
  - "novarocks/frontend/src/catalog_attachment/repository.rs (observe_materialized_view_references)"
  - "novarocks/frontend/src/catalog_application/frontend_port.rs (drop_catalog)"
---

## 问题

`DROP CATALOG` 应不应该在删除 attachment 的同一个 StateStore 事务里扫描 MV 的 target / dependency 索引，
以此保证「被 MV 引用的 catalog 不可删除」？

## 背景与执行事实

- 原实现确实这么做：attachment 的 exact-version delete 与 MV 前缀扫描在同一个写事务内，因此被引用的 catalog
  无法删除。它读起来像一个跨 family 的串行化 fence。
- 但 MV 的 definition / target / dependency 投影已经变成 **lake-source `Accelerator`**：它可以从湖上的 MV
  descriptor 与 publication facts 确定性重建，也因此**允许被整体 wipe**。用一个允许为空的缓存去为
  `ExternalProjection` 的删除提供事务级保证，意味着这个保证在缓存被清空的那一刻正好消失。
- 跨系统串行化本来也不成立：真正会与 `DROP CATALOG` 竞争的是另一个 FE 上的 MV DDL、以及外部 catalog 期望态
  controller，它们从来不是这个事务的参与者。
- 因此这个 fence 的强度是一种表象：它在缓存健在时有效，在缓存被清空或读不出来时静默失效，而调用方无从区分。

## 考虑过的选项

1. **保留同事务 fence**。零改动，但把一个可清除的缓存钉成正确性权威，并让后续设计误以为「跨 family 事务」是
   可用工具。它还会让「accelerator 可以被 wipe」这条不变量与「catalog 不可删」互相矛盾。
2. **把 MV 引用关系提升为持久真源**（不再是缓存）。能让 fence 名副其实，但直接违反湖单一真源：MV 的定义真源
   在湖上，FE 再持有一份权威副本就是第二真源。
3. **在湖上做引用校验**。语义正确，但 catalog 与 MV 的引用关系目前没有湖侧等价物，需要新的湖上元数据。
4. **降级为读路径 best-effort 检查 + 单 family 删除事务**（采纳）。

## 裁决

1. 引用检查移到删除之前的**读路径**，删除事务只触及 catalog attachment 一个 family。
2. 观察到引用仍然拒绝，且错误文本显式声明这是**运维保护，不是跨系统串行化保证**。
3. **accelerator 读不出来不再阻塞删除**：读不出来的缓存什么都没有证明。读失败记录 warning 后放行。
4. 保留原有的 `CommitUnknown` / `resolve_commit` 收敛逻辑不变——外部删除结果未知时的三态处理是真正的正确性，
   与被移除的 fence 无关。fence 去掉后两条 delete 路径变得完全相同，因此合并为一条，而不是留两份等待漂移。
5. 完成态的判据不是「wipe 后能 drop 成功」，而是「删除事务里对 MV 前缀的扫描次数为 0」。空前缀对事务内
   fence 与对根本不在事务里的检查，回答是完全一样的，所以只断言成功会把两种实现混为一谈。

## 接受的妥协（诚实记录）

- **这是本次改动中唯一主动降低的既有保证**，必须原样记下来：wipe 过或读不出来的 accelerator 会让一个真实存在
  的引用漏过，于是 catalog 会从一个活着的 MV 引用下面被删掉。
- 这个后果是**有界**的：MV 会指向一个已经消失的 catalog，由 MV 侧既有的 `Unavailable` / fail-closed 路径承接，
  不会产生错误的湖上发布。测试里把这条代价写成了断言，而不是藏起来。
- 检查与删除之间存在 TOCTOU 窗口：即使缓存健在，也可能在检查通过后才出现新的引用。原实现在缓存健在时没有这个
  窗口，所以在「缓存健在」这个子情形里，本裁决确实比原来弱。
- 检查放在 attachment repository 上，而不是一个独立的引用服务。理由是它已经持有 store、并且原本就在调用
  MV repository，所以耦合比原来更弱且不需要新接线；代价是 catalog 模块仍然知道 MV 模块的存在。
- 我们**没有**顺手把引用关系搬到湖上。那是更正确的方向，但需要新的湖侧元数据，本次不做。

## 何时重新评估

- 当 catalog 与 MV 的引用关系在湖上有了等价表达时：应当把检查移到湖侧，恢复一个名副其实的保证。
- 当出现「必须阻止悬空 MV 引用」的硬需求（而不是运维便利）时：best-effort 就不够了。
- 当有人再次提出跨 family 同事务约束时：应先回到本 ADR，确认被读的那一侧不是可清除的加速态。
