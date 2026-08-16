---
id: ADR-0077
title: "Hadoop catalog table creation uses storage conditional publication"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-13
provenance:
  - "discussion: 2026-07-30 / 2026-08-13, Hadoop catalog atomic table creation"
code-anchors:
  - "novarocks/fs/src/access.rs (filesystem conditional-create primitive)"
  - "novarocks/connector/iceberg/src/hadoop_catalog.rs (HadoopFileSystemCatalog table publication)"
  - "novarocks/connector/iceberg/src/catalog_control/catalog_mutation.rs (catalog mutation outcome and reconciliation)"
---

## 问题

没有独立 catalog 服务的 Hadoop catalog，如何在本地文件系统和对象存储上为并发 `CREATE TABLE` 提供跨进程、
跨 frontend、可被外部 Iceberg client 共同观察的 create-if-absent 语义？

## 背景与执行事实

Hadoop catalog 使用固定的 `<table>/metadata/v1.metadata.json` 作为第一版 table metadata，并用
`version-hint.text` 帮助 client 定位当前版本。旧实现先查询 table 是否存在，再普通写入这两个文件；普通写允许覆盖，
因此两个进程可以同时通过预检并分别返回成功，后写者还会替换先写者的 table UUID。

SQL 层锁、进程内 mutex 与 frontend generation lease 都只能约束一个 NovaRocks 进程或一个 runtime generation，不能约束
另一个 frontend、进程重启或 Spark 等外部 client。`exists + write` 在共享 storage 上同样不是原子操作。Connector
catalog mutation 已经区分 `KnownCommitted`、`KnownUncommitted` 与 `CommitUnknown`，但 provider 只有在外部系统中存在
可归因的线性化点时，才能诚实地产生这些结果。

当前 filesystem 基础使用 OpenDAL；local FS、S3 与 OSS backend 都声明原生 `write_with_if_not_exists` capability。
Iceberg table metadata 自带随机 UUID，也可以对一次性序列化的 canonical metadata bytes 计算稳定 digest。两者共同提供
response loss 后的 owner attribution，而不需要在公共 SPI 中暴露 storage header、ETag 或 credential。

## 考虑过的选项

1. **保留 metadata precheck，再普通覆盖写。** 改动最少，但检查与写入之间存在 TOCTOU，不能提供 create-if-absent，
   因此拒绝。
2. **在 frontend 或 provider 进程内加锁。** 可以让单进程测试稳定，却不能约束另一个 frontend、进程重启或外部 client，
   因此拒绝。
3. **增加 NovaRocks 私有 reservation/lock marker。** marker 可以协调升级后的 NovaRocks client，但标准 Hadoop catalog
   client 不认识它；这会把“跨 client 原子”退化为 NovaRocks 私有协议，因此拒绝作为完成态。
4. **用 object-store rename 或目录创建作为提交点。** 本地 rename 可能原子，但 S3/OSS rename通常是copy+delete，目录在
   object store中也不是一致的排他对象；不同storage会产生不同正确性语义，因此拒绝。
5. **条件创建 canonical `v1.metadata.json`。** 所有遵守Hadoop metadata layout的client观察同一目标；第一次成功创建
   获得table incarnation，竞争者得到typed conflict。选择此方案。

## 裁决

Hadoop catalog 创建表时，必须在任何外部 I/O 前冻结一次 create attempt：table UUID、canonical v1 metadata bytes、
metadata digest、metadata location 与上层 operation identity。`v1.metadata.json` 的原生 create-if-absent 是唯一 semantic
commit point。

filesystem 边界提供 connector-neutral、authorized 的 conditional-create primitive。它在调用前检查具体 storage 的
native capability；不支持时在任何 metadata side effect 前返回 typed `Unsupported`，绝不退化为 `exists + write`。
OpenDAL 与 conditional request 属于 filesystem/provider 实现细节，不进入 Connector SPI、Core、Frontend 或 Backend。

条件创建首次成功后，该 table incarnation 已知提交。请求响应丢失或内部安全重试遇到 existing target时，provider必须
authoritative reread v1：只有 UUID 与 canonical digest 都匹配，才能归因给同一 attempt；不同 UUID 是另一个 owner，
读取失败保持 `CommitUnknown`。不得盲目重放 create，也不得按 table prefix cleanup。

`version-hint.text` 是 owner-only finalization，不是第二个 commit point。只有在 authoritative v1 与目标 UUID/digest匹配
后才能写入或修复 hint。hint失败不能把已经提交的v1降级为uncommitted或unknown，而是
`KnownCommitted` + finalization failure。table discovery在hint缺失或损坏时必须probe canonical v1并验证metadata，不能让
一个已提交table仅因hint finalization失败永久不可见。

严格创建在发现其他有效 UUID 时返回 typed AlreadyExists/Conflict；`IF NOT EXISTS` 返回 NoOp并引用同一个authoritative
table。旧 evidence 遇到同名 drop/recreate 的新 UUID 时不得把新 incarnation 归因给旧 operation。

## 接受的妥协（诚实记录）

这个裁决只原子化第一版 metadata 的创建，不顺带解决后续 `update_table` 的多 writer CAS、按identity删除、namespace
marker、CTAS staged publication或durable DDL takeover。选择这个窄边界是因为 canonical v1已经是所有Hadoop client
共享的最小线性化对象；把其余问题合并进来会扩大外部协议与失败状态空间，而不是因为那些问题不重要。

`version-hint.text` 与 v1不能作为一个跨local/S3的原子多对象事务提交，因此接受“table已提交但hint finalization失败”
这一可观察状态，并要求读取fallback和typed finalization错误收敛它。每次create还会多一次capability检查，冲突或response
loss时会多一次authoritative read；这是获得可证明归因所支付的成本。

该方案只能约束同样尊重canonical v1 create-if-absent语义的外部Hadoop client。如果一个外部实现会强制覆盖已有v1，
NovaRocks不能用私有marker修补其行为；产品必须显式判为不兼容，而不能继续宣称跨client原子。

## 何时重新评估

- OpenDAL或目标local/S3/OSS backend不再提供可靠的native create-if-absent，或生产证据显示capability声明与实际语义不符；
- Iceberg Hadoop catalog标准不再以canonical v1 metadata作为共同创建目标，或引入了更强的标准catalog transaction；
- 产品要求跨FE接管长期未决DDL，此时需要durable evidence journal与后台reconciler；
- `update_table`多writer、identity-fenced drop或CTAS staged publication需要与create共享一个更完整的catalog authority；
- 外部Spark/Trino/Hadoop client实测会覆盖已有v1，导致该storage上的跨client兼容声明无法成立；
- hint fallback或冲突reread的额外I/O在真实catalog规模下成为可测量瓶颈，并且有同等级正确性的索引机制可替代。
