---
id: ADR-0080
title: "Statistics evidence separates coverage, basis, numeric nature and basis relation"
domain: [connector-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-17
provenance:
  - "PR: pending (per-metric statistics evidence in novarocks/spi/src/connector/statistics.rs)"
  - "discussion: 2026-08-15 统计新鲜度语义与 FE durable 记录边界"
code-anchors:
  - "novarocks/spi/src/connector/statistics.rs (StatisticsEvidence, StatisticsMetricObservation)"
---

## 问题

一份统计证据里的不同 metric，其可信程度天然不同：便宜指标来自被查询快照的 manifest，NDV 来自
sketch，某些值还可能测于更早的快照。如果用整份证据一组 coverage / accuracy / provenance 描述
全部 metric，任一维度的降级会污染其余全部 metric。

那么：证据应该用几个维度描述？这些维度之间是什么关系？ANALYZE 允许发布的前置条件到底在断言什么？
消费者应该以什么粒度决定采用？

## 背景与执行事实

改动前 `StatisticsEvidence` 在证据级持有 `coverage`（Full/Subset/Superset）、`accuracy`
（Exact/Approximate）与 `provenance`，`metrics` 只承载值。由此产生三个具体后果：

1. **Iceberg manifest 读路径用一个布尔同时决定覆盖度与精确性**：
   `record_count.is_some() && delete_files.is_empty()`。只要出现一个 delete 文件，整份证据降为
   `Subset + Approximate`，连仍然有效的 min/max 边界一起作废。

2. **Theta NDV 被当作精确值发布**。ANALYZE 的发布门禁要求 `Full + Exact + VisibleRows`，而这份
   证据里就含 Theta sketch。它能通过门禁，是因为"扫描覆盖了全部可见行"这个事实被错误地表达成了
   "数值精确"。断言本身从来不成立，只是借壳通过。

3. **消费者以整份证据为单位判断可用性**：`coverage == Full && accuracy == Exact` 才进 optimizer。
   于是"有 delete 文件的表"对 optimizer 而言等于"完全没有统计"。

此外，一旦读侧开始沿快照祖先链回溯（本 ADR 为其预留表达形状），还会出现第四个独立事实：
某个 metric 的基准行集合与被查询行集合是什么关系。

## 考虑过的选项

**A. 保持证据级单一 accuracy，用最保守值聚合。**
最小改动。但它正是当前问题：聚合意味着最弱的 metric 决定所有 metric，且无法表达"行数是上界而
minimum 是下界"这种方向相反的事实。

**B. 三维模型：把 accuracy 与 provenance 下沉到 metric 级，删除 coverage。**
曾经的初版方向。它错在两处：覆盖度是发布门禁的承重字段，删掉之后门禁无从表达；而且它把"陈旧带来
的集合差异"塞进数值精确性，等于让一个字段编码两件事。

**C. 四维模型：collection 级覆盖度 + per-metric 基准版本 / 来源 / 数值性质 / 集合关系。**
表达力足够，且每一维都有独立的判定依据与独立的消费方式。代价是证据结构明显变重。

**D. 引入置信区间或衰减模型统一表达不确定性。**
表达力最强，但需要为每个 metric 定义可信的区间语义，而当前没有任何产出方能诚实地给出区间。
业界（Trino）连 NDV scaling 都不做。

## 裁决

采用 **C**。

**collection 级**只保留 `data_version`、`evidence_revision` 和 `row_coverage`
（是否覆盖了其 basis 的全部可见行）。

**per-metric** 携带四个事实：基准版本、来源类别、数值性质（相对**其自身 basis 上的真值**是
精确 / 上界 / 下界 / 双向近似）、基准集合关系（其 basis 行集合与**被查询版本行集合**的关系：
相同 / 子集 / 超集 / 不可比）。

四条承重规则：

1. **数值性质与集合关系不得互相编码。** 前者只回答"在它自己的 basis 上准不准"，后者只回答
   "它的 basis 和你问的版本差多少"。把其中一个塞进另一个，就会重新制造本 ADR 要消除的污染。

2. **Theta NDV 永远不是精确值**，即使 basis 就是被查询版本。这条由类型强制：`StatisticsEvidence`
   字段私有、`try_new` 是唯一构造入口并在其中校验，因此任何 provider 都无法构造出把 sketch 标为
   精确的证据。

3. **发布门禁表达覆盖度，不表达数值精确性。** ANALYZE 的前置条件是"目标版本匹配 + 覆盖全部可见行
   + 来源是可见行扫描"。删除对 `accuracy == Exact` 的要求——它在含 Theta NDV 的证据上本就不成立。

4. **消费者逐 metric 采纳。** 准入只问"这个值是否描述被查询版本的行集合"（基准版本相同且集合关系
   为"相同"）；数值性质不参与准入，只映射为置信度。一个 metric 降级不得导致整份证据被丢弃。

证据级如果需要摘要用于展示，只能由 per-metric 事实**派生**，不得作为可独立写入的字段与
per-metric 事实并存——两份可能不一致的真相正是本问题的根源。

## 接受的妥协（诚实记录）

1. **证据结构明显变重**，构造一个 metric 从"一个值"变成"一个值加四个事实"。这是描述既有事实所需的
   最小表达力：删掉任一维度都会导致一类降级污染其他 metric，或让发布门禁失去承重字段。

2. **放宽发布门禁（去掉 `Exact`）表面上降低了严格性。** 实际是把一条本来就不成立的断言换成一条
   成立的断言，严格性由覆盖度承担。但必须承认：门禁字面上确实变松了，评审时不能只看 diff 就断定
   安全。

3. **消费者规则的选择偏向"行为等价 + 诚实标注"，而不是"最优利用"。** 数值性质只映射成
   `Exact`/`Estimated` 两档置信度，方向信息（上界还是下界）对 optimizer 目前完全不可见。这是有意
   的：如何利用方向是独立的代价收益判断，不应与表达形状变更捆在一起。信息可得不等于已被使用。

4. **这次改动确实改变了 optimizer 的输入。** 有 delete 文件的表此前拿不到任何统计，现在拿得到
   （标为 Estimated）；Puffin NDV 的置信度从 Exact 降为 Estimated。两者都是本 ADR 的直接后果，不是
   副作用，但它们会改变计划形状，这一点必须在 review 中被当作行为变更对待。

5. **集合关系的推导库已落地，但读路径尚未使用它。** 当前 reader 只读发布在被查询快照上的统计，
   因此基准恒等于被查询版本。祖先回溯读是后续工作；在它落地之前，集合关系这一维只有推导函数的单元
   测试证据，没有端到端证据。

6. **`StatisticsInterval` 保留但恒为 `None`。** 明确不做置信区间（选项 D），字段只是为将来的估计器
   预留形状。一个永远不填的字段是已知的味道，接受它以避免后续重塑观测结构。

## 何时重新评估

- **祖先回溯读落地后**：集合关系将首次产生非 `Identical` 的值。届时需要重新检查消费者规则——
  "非相同即跳过"是当前的保守选择，如果证据表明子集/超集关系可以安全地按方向使用，这条规则应当放宽。
- **optimizer 开始利用方向信息时**：如果上界/下界能驱动更好的基数估计，`Exact`/`Estimated` 两档
  置信度就不够了，需要重新设计数值性质到代价模型的映射。
- **出现第二个 statistics provider 时**：四维模型的判定责任目前全部由 Iceberg provider 承担。
  第二个实现会检验这些维度是不是真的 provider-neutral，还是悄悄编码了 Iceberg 的快照模型。
- **如果 `Incomparable` 在真实工作负载中占比过高**：说明保守回退过于频繁，统计等于不可用。那时要
  重新评估是否需要更强的谱系证据，而不是继续放宽判定。
- **如果证据级摘要被反复要求**：说明消费者其实不想逐 metric 决策。届时应重新审视是消费者接口设计
  有问题，还是四维模型对某类消费者过于底层——但不得把摘要退回成可写字段。
