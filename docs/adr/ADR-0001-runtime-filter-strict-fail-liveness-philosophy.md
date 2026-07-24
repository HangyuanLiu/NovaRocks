---
id: ADR-0001
title: "Runtime-filter wait cycles fail statically, runtime timeout is only a floor"
domain: [runtime-filter]
status: active
supersedes: []
superseded-by: null
date: 2026-07-24
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/726"
  - "vault: 2026-07-23-rfd-6f-join-runtime-filter-progress-proof-discussion-design"
  - "discussion: 2026-07-24 CTE/E4/multicast 对话"
code-anchors:
  - "novarocks/core/src/runtime_filter/deployment/wait_for.rs (validate_wait_for)"
---

## 问题

Join runtime filter 的 `BlockingSnapshot` 等待可能与数据流/反压组成循环等待。为什么 NovaRocks 选择在 fragment submission 前**静态拒绝整个 query**（strict-fail），而不是像业界常见做法那样靠运行时 timeout + fail-open 兜底放行？

## 背景与执行事实

- `BlockingSnapshot` consumer 在产出首个 chunk 前阻塞等待 RF（scan source 与 exchange source 的 `acquire_configured`）；等待有界，超时/不可达一律降级为 `PassThrough`（放行、RF 不生效、结果正确）。所以真环的运行时后果不是死锁，而是「白等 ≤timeout 后照常出结果」——一笔隐藏的停顿税。
- RFD-6B 把 Join RF 切到 query-global Graph/Service 数据面后，串行 CI 出现 17 个确定性 `BlockingFeedbackCycle` 失败，暴露了进度证明的结构覆盖缺口。
- RFD-6F（PR #726）落地当前机制：planner 密封 build-frontier proof（producer fragment 入边的精确二分划分），deployment 逐字段重验，再把所有等待关系放进最小细化图（E1 数据流边 / E2 frontier 边 / E3 等待边 / E4 多分支反压边）做一次全局 Kahn 验环；有环即 `BlockingFeedbackCycle` 拒绝，错误携带完整环路径。入口：`wait_for.rs` 的 `validate_wait_for`。
- 被拒绝或缺失的 proof 保持 fail-closed：等待边退化为粗粒度 fragment 边参与验环，而不是被豁免。

## 考虑过的选项

1. **运行时 timeout + fail-open 作为语义权威**（StarRocks 路线：global RF 约 20ms 短等，超时机会性放行）。优点：永不因等待环拒 query。代价：真环被静默吞成延迟税，开发期完全失去「这里的 RF pattern 是错的」信号——雷达失效；且「blocking acquire 恒有界」成为无人守护的隐式前提。
2. **逐 channel 证书豁免**（6F 之前的形态）：对每条等待边独立回答「build 与 consumer 是否独立」。已被证伪：CTE multicast 使 plan 成为 DAG，两个各自有效的证书可以联合数据流边组成环——逐边豁免组合不封闭。
3. **静态全局验环 + 运行时兜底分层**（选定）：静态层证明不了安全就 fail-fast；运行时 timeout+PassThrough 保留为生产可用性下限，但不反向作为静态放宽的依据。

## 裁决

采用分层：**静态 strict-fail 是开发期 bug 雷达和长期语义断言；运行时 fail-open 是生产兜底，不是语义权威**。判据方向是「宁严勿宽」：proof 的 false positive（安全形状证明不了 → query fail）是开发期噪音，可接受；false negative（危险形状被误证明 → 运行时静默白等后放行）是雷达失效，不可接受。拿不准一律不签。

## 接受的妥协（诚实记录）

- 开发期会持续出现「安全形状因证明覆盖不足被拒」的响亮失败，需要不断补证明覆盖（6F 本身就是一次补课；其 E4 边随后又暴露出需要 RFD-6G 的 activation 降级）。这是选定方向的固有成本，不是缺陷。
- 惩罚不对称是有意的：检出环罚 query fail，漏检只罚 ≤timeout 延迟——它逼着覆盖缺口被补齐、真 bug 被修掉，而不是被兜底默默容忍。
- 每一类新的等待/反压形态都必须先能被细化图表达，才能被证明安全——扩展这套图模型是后续所有 RF 演进的前置税。

## 何时重新评估

- guard 触发率在证明覆盖补齐后仍居高不下（误报持续消耗开发效率）；
- proof 目录 + 细化图的维护成本明显超过其作为雷达的价值；
- 出现无法用静态结构表达、只能运行时观测的新等待形态（那将需要升级图模型或重开分层讨论）。
