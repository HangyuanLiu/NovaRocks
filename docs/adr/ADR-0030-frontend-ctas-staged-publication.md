---
id: ADR-0030
title: "Frontend CTAS uses provider-owned staged publication"
domain: [frontend-dml, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-02
provenance:
  - "discussion: 2026-08-02 CTAS staged publication and destructive compensation"
code-anchors:
  - "novarocks/frontend/src/dml/service.rs (DmlService application owner)"
  - "novarocks/spi/src/connector/mod.rs (connector capability exports)"
  - "novarocks/core/src/engine/iceberg_ctas.rs (CTAS migration source)"
---

## 问题

CTAS 如何在分布式写失败、响应丢失和外部并发改名空间下发布新表，同时保证自动恢复不会删除一个已经提交或由其他创建者拥有的可见表？

## 背景与执行事实

旧 CTAS 先创建可见表，再执行分布式写；任意写错误都会按 catalog 名称执行 `DROP TABLE ... PURGE`。这种补偿无法从错误字符串区分已提交、未提交和提交状态未知。

Iceberg table UUID 的预读取也不能使补偿安全：在“读取 UUID 相同”和“按名称删除”之间，外部进程仍可删除并重新创建同名表。当前 Connector catalog mutation 与 Iceberg catalog drop 只按表名删除，没有 expected identity 的原子条件；标准 Iceberg REST Drop Table 也没有 `If-Match` 或 expected UUID 参数。

Iceberg REST Catalog 的 create route 支持 staged create：服务返回初始化 metadata，但目标表尚不可见；客户端随后把 create 初始化与后续 snapshot changes 一起提交到 table commit route，并用 `assert-create` 防止并发创建。这为 CTAS 提供了“写入未发布对象，最后原子发布”的 catalog authority。

## 考虑过的选项

1. 保留 visible create，写失败后按名称 drop。改动最小，但在 commit unknown、known-committed finalization failure 和并发 drop/recreate 下可能删除正确数据，拒绝采用。
2. 在 drop 前读取 table UUID，或给 frontend/core 加本地锁。UUID check 与 drop 不是一个原子动作；本地锁也不能约束 Spark、其他 NovaRocks frontend 或 catalog client，拒绝采用。
3. visible create 后永不自动清理，失败时保留空表或部分表。该方案是当前 contract 下的安全保底，但把失败残留永久变成用户可见资源，不作为支持 atomic staged publication 的 provider 的完成态。
4. 由 provider 准备不可见的 staged table，分布式 writer 只向该 staging identity 写入；完成后由 provider 原子 publish，已知未提交时仅按 opaque staged handle abort。选择此方案。

## 裁决

Frontend 继续拥有 CTAS application saga、StateStore lifecycle、用户错误和恢复策略；Connector provider 拥有 staged object、catalog transaction 与原子 publication truth。跨边界只传有界、版本化、provider-neutral 的 opaque handle、digest、typed outcome、receipt 和 evidence，不传 metadata location、snapshot update、credential 或 catalog client。

CTAS 顺序固定为 pure source prepare、durable staged-prepare intent、provider staged prepare、一次 distributed write、durable publish intent、atomic publish。任何 external dispatch 前必须先写 durable identity。`CommitUnknown` 必须先持久化 evidence，再由同一 exact-incarnation capability authoritative reconcile；不能把 unknown 当作 uncommitted，也不能在 unknown 后 abort。

自动清理只允许 provider 按自己签发的 opaque staged handle 丢弃尚未发布对象。Frontend/core 永远不得把该操作实现为按可见表名 `DROP/PURGE`。Known committed 后的 cache、response 或 journal finalization failure不改变外部提交事实。

REST provider 使用标准 staged create 与带 `assert-create` 的 table commit，把 create 初始化与 CTAS snapshot changes 原子发布。没有等价 atomic staged-publication capability 的 provider 必须在 source execution和任何外部 side effect 前返回 typed `Unsupported`。Hadoop catalog 只有在独立的 catalog authority/fencing 能力完成后才能广告支持；process-local lock、check-then-write 或对象存储 rename 不能充当该能力。

TRUNCATE 不依赖 staged publication，继续使用 FE-only data mutation lifecycle，不能因 CTAS capability 缺失而阻塞。

## 接受的妥协（诚实记录）

该裁决扩大了 CTAS cutover 的上游工作：需要新的 provider-neutral capability、REST staged transaction adapter、writer 与 publish 的组合，以及额外的 failure/reconcile 测试。它不是改动最少的方案；选择它是因为 visible-table destructive compensation 无法证明安全，而不是因为 staged transaction 实现更便宜。

在 Hadoop catalog 获得原子 publication/fencing 之前，frontend CTAS 对该 provider 将显式不支持。这是有意识接受的短期能力缺口；系统宁可在 side effect 前失败，也不继续提供一个可能删除并发创建表或已提交数据的表面成功路径。

## 何时重新评估

- Iceberg REST 标准移除 staged create，或目标 catalog 实现无法原子提交 create 初始化与 snapshot changes；
- Hadoop catalog 获得跨进程、跨 frontend、对象存储语义下可验证的 atomic publication/fencing；
- 新 provider 无法产生 bounded opaque staged handle，但能提供另一种同等级别的原子 CTAS transaction；
- 生产 evidence 表明 staged handle、write report 或 reconcile evidence 超出现有 StateStore/SPI 大小预算；
- 产品明确接受失败后保留可见表，并要求以该语义替代 atomic publication。
