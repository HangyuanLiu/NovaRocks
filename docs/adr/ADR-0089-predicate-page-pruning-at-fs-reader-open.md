---
id: ADR-0089
title: "Predicate-driven Parquet page pruning belongs at FS reader-open"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-19
provenance:
  - "discussion: 2026-08-19 predicate-driven Parquet page pruning"
  - "implementation: PR number pending"
code-anchors:
  - "novarocks/fs/src/physical_reader/parquet.rs (ParquetPhysicalReader::try_new)"
  - "novarocks/connector/iceberg/src/batch_reader.rs (IcebergBatchReader::try_new)"
---

## 问题

对于已经以 `PruningOnly` 传入 Connector reader 的 static predicate，Parquet ColumnIndex 与 OffsetIndex 应由哪个
owner 消费，怎样既避免跨列 page ordinal 错配，又不让物理优化改变 SQL correctness？

## 背景与执行事实

Iceberg 负责把 table field 正确映射为有界的 physical predicate，并维持 delete、DV、schema evolution 与最终 batch
correctness；它不拥有 Parquet physical metadata。Backend 只把已冻结的 query options 投影为 reader request，Execution
在 reader 返回 batch 后继续执行 residual conjunct。`novarocks-fs` 已拥有 Parquet metadata、row-group pruning、物理解码
与 `RowSelection`。

ColumnIndex 的 page bounds 与 OffsetIndex 的 row boundary 都按 physical leaf 排列。同一 row group 的不同列可以有不同的
page 数和 `first_row_index` 边界，因此把某列 predicate 的 page ordinal 填入 column-agnostic selection，再按另一列的
offset index 解码，可能错误排除本应保留的行。

## 考虑过的选项

1. **由 Iceberg 预先生成 page ordinals。** Connector 已知 table field，但不拥有 Parquet metadata 的 leaf layout 或
   reader-open cache capability；跨列 AND 仍会把 ordinal 与别的列 boundary 混用。
2. **扩展 generic native wire 或 scan unit 传输 page identity。** 可以让 Backend 接收预计算选择，但 page 是 reader-local
   优化状态，不是 split、prepared unit 或 retry identity；此方案会把 provider-private file facts 固化为长期协议。
3. **在 FS reader-open 按实际 leaf 转为 row ranges。** 每个 predicate 读取同一 leaf 的 ColumnIndex 与 OffsetIndex，
   先在 row-range 域求交，再构造 `RowSelection`；缺少安全证据保守 Keep，结构矛盾返回 typed error。（采纳）

## 裁决

`novarocks-fs` 的 Parquet reader-open 是 predicate-driven automatic page pruning 的唯一 owner。它以 field ID 优先、
安全 name binding 为后备，针对每个 predicate 在同一个 physical leaf 上消费 ColumnIndex 和 OffsetIndex，把 surviving
pages 转换为 row-group-local row ranges，再对 top-level AND 做 range intersection。

自动选择不复用 column-agnostic `PhysicalPageSelection`，也不回写 page ordinals 到 Connector payload、native wire、
prepared scan unit 或 Execution morsel。`PruningOnly` 与 Execution residual 不变：page pruning 只能排除已证明不可能
产生 TRUE 的物理范围，不能宣称 surviving rows 已满足 SQL predicate。

缓存 capability 与 row effect 同属 reader-open boundary：footer-only metadata 不能满足需要 page-index capability 的请求；
attempt、fallback、considered rows 与 pruned rows由 FS 产生，经 SPI/Backend/Frontend 作中立聚合和展示。

## 接受的妥协（诚实记录）

在 FS 内做 per-leaf range conversion、cross-column intersection 和 capability-aware cache 会增加实现与测试复杂度，也可能让
同一文件短期同时存在 footer-only 与 page-index-capable metadata entry。选择它不是因为代码路径更短，而是因为把 page
ordinal 留在上层会把列布局假设藏进 API，最坏结果是 false negative；多一份可明确验证的 metadata entry 比错误过滤数据
更可接受。

本裁决也保留了显式 `PhysicalPageSelection` 的历史第一列 contract，而没有顺手泛化它。两条 path 并存会增加局部维护成本，
但避免在这次自动优化中无证据地改变已有 caller 的语义。

## 何时重新评估

1. 显式 page selection 需要支持任意 physical leaf 且已有两个独立 caller 需要稳定公共 contract 时，可单独设计新的
   column-aware selection DTO；不得复用 automatic path 的内部 representation。
2. 新文件格式能够提供与 Parquet page index 同等的 per-range metadata 时，仍应由对应的 FS physical reader 评价；若
   需要共享 domain API，先证明该 API 不包含 provider、wire 或 scan-unit identity。
3. metadata cache 的真实容量或 I/O 数据表明双 capability entry 不可接受时，可以改变实现为单调 upgrade/value tag，
   但必须保持弱请求不能满足强请求、并发不降级和同样的 fallback semantics。
