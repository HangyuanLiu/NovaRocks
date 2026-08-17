---
id: ADR-0081
title: "Statistics are versioned, may be stale, and the reader decides usability"
domain: [connector-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-17
provenance:
  - "PR: pending (ancestor statistics reads and measured-snapshot publication)"
  - "discussion: 2026-08-15 统计新鲜度语义与 FE durable 记录边界"
code-anchors:
  - "novarocks/connector/iceberg/src/statistics_ancestry.rs (resolve_ancestor_ndv)"
  - "novarocks/connector/iceberg/src/catalog_control/statistics.rs (read_statistics, publish_statistics)"
---

## 问题

统计与表的当前版本不一致时，应该报错、丢弃，还是照用？读侧要求「统计必须与被查询快照精确同版」是不是一条
可以维持的不变量？

## 背景与执行事实

改动前 NovaRocks 在四条路径上强制 currentness（`read_statistics`、`prepare_publish`、
`publish_statistics`、`reconcile_statistics` 各有一次 `ensure_current_version`）。后果是可观测的：

1. **ANALYZE 在写热表上必然失败**。采集扫完全表之后才提交，期间只要落一次写入，
   提交就以「table changed while statistics evidence was being processed」告终——工作全部作废。
2. **表一推进，已发布统计立即不可读**。读侧只查被查询快照自己的统计文件，没有祖先回溯。
3. **manifest 可派生指标被 Puffin 支配**。有 Puffin 时行数等不再从 manifest 取，Puffin 里没有就是 `Missing`；
   于是「从没 ANALYZE 过的表」等于「没有任何统计」。

业界无一家做精确同版（2026-08-15 源码核实）：Trino 沿 `parentId()` 回溯取祖先统计并直接使用；
StarRocks 同样回溯，另加 `min(ndv, record_count)` 的 cap；Iceberg 官方
`ComputeTableStatsSparkAction` 允许把统计提交到非 current 快照且提交前不校验 currentness。

## 考虑过的选项

**A. 维持精确同版。**
语义最简单：拿到的统计一定描述你正在查的那批行。但它把「统计」当成事务性产物，
而统计本质是估计事实；代价是写热表上 ANALYZE 永远失败、统计永远不可用——严格性买不到任何正确性。

**B. 允许陈旧但不标注（Trino 姿态）。**
祖先统计直接使用，不告诉消费者它测于何时。实现最省，但消费者失去了做保守处理的依据，
而 NovaRocks 已经在 `ADR-0080` 里建立了 per-metric 基准事实——不标注等于浪费掉已有表达力。

**C. 允许陈旧并逐 metric 如实标注（本裁决）。**
读侧沿祖先链回溯，每个 metric 带上自己的基准版本与集合关系；写侧挂到实际测量的快照。

**D. 按陈旧程度做数值修正（NDV scaling / 衰减）。**
表达力最强，但没有可信的修正模型：Trino 连 scaling 都不做。只采纳
StarRocks 式的保守 cap，不引入衰减。

## 裁决

采用 **C**。四条具体规则：

1. **manifest 可派生指标恒从被查询快照的 manifest 取**，与统计文件是否存在无关。它们的基准就是被查询版本。
   Puffin 唯一真正拥有的是 NDV。
2. **NDV 允许来自祖先快照，且按 metric 各自回溯**。取「第一个含有该 metric 对应 blob 的祖先」，
   而不是 Trino/StarRocks 的「第一个含有任何统计文件的祖先」——后者会让一次单列 ANALYZE
   遮挡住上一个快照里的完整统计，其余列全部报 `Missing`。
3. **读侧不再要求被查询快照仍是 current**。查询在快照 S 上规划，就读 S；表随后推进不使统计读失败。
4. **写侧挂到实际测量的快照**，以 metadata-only 乐观提交，不要求该快照仍是 current。

跨快照匹配一律用稳定 field ID：一旦统计可以跨版本读，按列名匹配会在重命名后张冠李戴。

保守 cap：表被证明为空（行数精确且为 0）时 NDV 为 0；否则 `min(ndv, row_count)` 并以 1 为下界。

## 接受的妥协（诚实记录）

1. **NDV 会被明确地允许陈旧。** 写热表上的基数估计质量会下降。这是业界一致做法，且本设计的 cap 比 Trino 更保守，
   但必须承认：换来的是「估计始终存在」，不是「估计更准」。

2. **移除 currentness 前置削弱了一层并发探测。** 该探测原本也不保证正确性（只覆盖从加载到提交的窄窗口），
   却会在正常写入下造成失败。外部提交的正确性仍由 `ADR-0017` 的三态与 reconcile 承担。
   但要如实说：这次改动确实拿掉了一个会响的警报，理由是它响错了地方，不是它没用。

3. **祖先回溯增加对象存储读取。** 每个命中统计文件的祖先要读一次 Puffin。遍历上界设为 64，
   深链表上仍有成本。这里没有做缓存或索引——如果实测证明成本不可接受，那是一次独立的设计。

4. **`min(ndv, row_count)` 的 cap 在有 delete 文件时是拿上界去 cap。** 行数本身可能高于真实值，
   于是 cap 偏松。这是保守方向上的放宽，不是精确约束。

5. **D1 让有 delete 文件的表丢掉了 ANALYZE 的精确行数。** ANALYZE 全扫描能得到扣减删除后的精确行数，
   而 manifest 求和只是上界。恒从 manifest 取意味着这份精确值不再被读到。
   接受它是因为「用祖先快照的精确行数描述当前快照」比「用当前快照的上界」错得更远，
   但这是一次信息质量的实际回退，不是纯收益。

## 何时重新评估

- **祖先回溯的读取成本在生产负载上变得可观时**：需要 per-column 统计索引或缓存层，而不是继续调大遍历上界。
- **出现可信的 NDV 陈旧修正模型时**：当前一律不修正只 cap；若有证据表明按行数比例外推更准，选项 D 值得重开。
- **`Incomparable` 在真实工作负载中占比过高时**：说明保守回退过于频繁，祖先统计等于不可用，
  那时要重新评估集合关系的判定强度。
- **delete 文件下的精确行数被证明重要时**：可以考虑让 ANALYZE 的行数在「基准就是被查询快照」时优先于 manifest，
  但那会重新引入两个来源的优先级问题，需要显式裁决而不是顺手加回。
- **若某个 provider 的快照模型没有单亲祖先链**：本裁决的回溯假设线性 parent 链，分支模型需要重新设计遍历。
