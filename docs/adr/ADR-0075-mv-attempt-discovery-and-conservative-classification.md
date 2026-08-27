---
id: ADR-0075
title: "Lake-first MV attempt discovery classifies conservatively or not at all"
domain: [frontend-mv, provider-spi]
status: superseded
supersedes: []
superseded-by: ADR-0112
date: 2026-08-13
provenance:
  - "discussion: 2026-08-10 MV refresh active-active recalibration"
code-anchors:
  - "novarocks/spi/src/connector/mv_attempt_discovery.rs (ConnectorMvAttemptPage)"
  - "novarocks/connector/iceberg/src/commit/mv_attempt_scan.rs (scan_attempt_page)"
  - "novarocks/frontend/src/mv/attempt_classification.rs (classify_attempt)"
---

## 问题

在 StateStore refresh ledger 丢失之后，凭什么可以断定某个 MV staging artifact 已被放弃从而可以删除？

## 背景与执行事实

recovery 已经能 inspect 一个「拿到 descriptor」的 attempt，但**枚举**入口仍然是 StateStore ledger
（`mv/recovery.rs` 的 candidate 列表 + legacy unfinished 扫描）。`MvLakePackageObservation` 只携带 target、
descriptor 与 current publication，无法分页发现该 target 的 staging attempts。

于是 ledger 丢失（灾备、或重建后重新分配 numeric `mv_id`）之后，Frontend 无法证明自己找到了某个 target 的**全部**
attempt。而「已放弃」是一个需要完整枚举才能成立的判断：漏看一个 attempt 就可能删掉仍然有用的数据。

## 考虑过的选项

1. **周期性全 catalog 扫描。** 概念最简单，但负载不可控，且仍然没有回答「这一次扫描是否完整」。
2. **按 ref 命名约定枚举并认领。** 便宜，但 ref 名是 provider 私有且会复用，用名字认领等于用命名巧合决定数据归属。
3. **按时间戳或「最大 refresh ID」选 winner。** 看起来能自动收敛，但 refresh ID 是各 Frontend ledger 分配的，
   最大的那个只是最吵的那个；时间戳记录的是某个 Frontend 何时**观察**到，而不是 lake 何时提交。
4. **provider 拥有的、target-scoped、bounded/paginated discovery，配合保守分类。** 需要新契约与新 provider 能力。

## 裁决

采用选项 4，并把三条不变量做进类型而不是留给调用方。

**其一，页面有界，且完整性只有一个来源。** `complete` 不可与 continuation 或 stop-reason 共存；incomplete 页面
必须说明停止原因；只有 page-budget 停止可以携带可续 cursor——storage 故障、无法解码的条目、失效 continuation 都
不得把自己表现成「一个可以继续的位置」。这条排除的危险情形是：**一个空的 incomplete 页面被当成空 target**。

**其二，无法解码的 ref 上报而非丢弃。** dangling ref、pre-V2 marker、比本版本更新的 schema、外来 target、畸形
attempt identity，各有独立的上报原因。静默过滤会让调用方得出「这里没有 attempt」并删除它从未看过的数据。

**其三，分页 cursor 是 attempt ID，不是位移。** ref 名 provider 私有且复用，且 ref 会在两页之间增删；用 UUIDv7
attempt ID 排序与续扫，意味着无论期间发生什么，续扫返回的都恰好是排在 cursor 之后的 attempt。

分类只使用 lake 事实：`main` 指向哪里、target ancestry 是否包含、哪个 generation 拥有 fence、provider 能否解析该
attempt 自己的 operation。**明确排除** StateStore 时间戳、local queue 顺序、numeric `mv_id` 与「最大 refresh ID」。
generation 之间的先后交给 ADR-0064 的 fencing 契约裁决，所以「superseded」在这里与外部提交点是同一个含义；契约
拒绝比较的组合（跨 cluster、一个 epoch 两个 token）判为 `Ambiguous` 而非按偏好取一个。

publication 需要**正面**的 lake 证据：target 就是这个结果，或这个结果在其 ancestry 中，且 staged identity 匹配。
target 声称是本 attempt 的结果却携带不同 identity，判为矛盾而非近似命中。

cleanup 的授权与分类**分离**：分类说「这个 attempt 是什么」，授权说「这个 Frontend 此刻是否有权处置它」。授权要求
live ownership、在已建立的 fence 下行动、以及 observation digest 匹配，三者独立必需——因为分类是在更早某个时刻算出
的，届时可能已经丢了 lease 或 lake 已经前进。

## 接受的妥协（诚实记录）

- **保守方向优先于自动化程度。** 缺失的 staging artifact **不**被视为「从未提交」的证明，因此判 `Ambiguous`
  而非可回收。代价是这类 attempt 需要后续 inspect 或人工介入才能收敛，运维会看到残留物。这是为了避免错误删除
  而刻意付的代价。
- **`Published` 不授权 reclaim。** 已发布 attempt 的 staging ref 可能仍是某个并发 recovery 正在读的证据，
  所以释放它是另一个在当前所有权下做的决定。代价是已发布 attempt 的 artifact 会多存活一段时间。
- **legacy V1 在无 ledger 时不承诺可完整枚举。** V1 缺稳定 resource/fence identity，无法证明归属，因此只记录
  unresolved artifact 且禁止自动 publish/delete。这是能力边界，不是缺陷——但确实意味着升级前的历史 attempt
  在灾备场景下需要人工判断。
- **本决策落地时，Frontend 侧的 startup/rebuild 编排尚未接线。** discovery 契约、provider 扫描与分类/授权规则
  已落地并各自有测试，但把它们串成「ledger 清空后从 lake 重建」的生产路径还没有做。因此本 ADR 记录的是**规则**
  已被裁决，不是 ledgerless recovery 已经可用。
- **分类与授权当前是纯函数，未被生产路径调用。** 选择先落规则再接线，是因为规则是错误代价最高的部分（一次错误的
  删除不可逆），先让它可测、可评审比先让它可运行更重要。

## 何时重新评估

- Frontend startup/rebuild 编排接线后：应重新评估 `Ambiguous` 的比例是否高到运维不可接受；若是，则重新评估判定
  规则，但保守方向仍优先于自动化程度。
- 若 target-scoped ref 枚举在真实规模下产生可观的 catalog metadata 负载：应引入 bounded index 或 backlog/age
  指标，但不得退回无界周期扫描。
- provider 侧出现原生的「按 target 列出 MV attempt」原语时：本契约的分页语义可能可以简化。
- 若出现第二个拥有 MV target 的 provider：`ObservedAttemptRef` 中的 Iceberg 特有假设（branch/tag 区分、
  ancestry 概念）需要重新审视是否仍属 provider 私有。
