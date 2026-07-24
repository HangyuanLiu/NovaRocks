---
id: ADR-0002
title: "Multicast backpressure stays consumer-coupled"
domain: [runtime-filter, exchange]
status: active
supersedes: []
superseded-by: null
date: 2026-07-24
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/726"
  - "vault: 2026-07-23-rfd-6g-join-rf-cycle-forced-activation-design"
  - "discussion: 2026-07-24 CTE/E4/multicast 对话（含 StarRocks 对照考据）"
code-anchors:
  - "novarocks/core/src/exec/operators/multi_cast_data_stream_sink.rs (need_input)"
---

## 问题

NovaRocks 的 multicast sink（CTE 扇出）采用「任一分支满则整体停」的耦合反压。这个语义与 `BlockingSnapshot` RF 等待组合会形成真实的循环等待（q23 等 CTE 形状被 6F 的 E4 边拒绝）。为什么保持耦合模型，而不是像 StarRocks 那样把消费者解耦？

## 背景与执行事实

- `MultiCastDataStreamSinkOperator::need_input` 遍历全部 inner sink，任一不可写即整体返回 false → 上游对**所有**分支停产。这就是细化图 E4 反压边的执行层事实。
- CTE 复用使 plan 成为 DAG：同一 producer fragment 的输出可以同时喂一个 join 的 build 侧和 probe 侧。与「probe consumer 阻塞等该 join 的 RF」组合，即：consumer 等 build 的 RF → consumer 停排空 → 其分支缓冲满 → multicast 全停 → build 分支断供 → RF 永不发布。RFD-6F 的 E4 边在 tpc-ds q1/q2/q14/q23/q59/q95、cte_in_where_subquery、cte_recursive、join_reorder_no_op_invocations 上抓到该真环。
- StarRocks 对照（源码考据结论）：其 `InMemoryMultiCastLocalExchanger` 按**最快**消费者节流（`can_push_chunk` 只看最快者与尾部的落后量），每个消费者持独立进度指针，cell 待全部消费者读过才释放，可选 `SpillableMultiCastLocalExchanger` 把积压落盘。被阻塞的分支**不会**饿死 build 分支，环根本不成立。其 FE 无 RF 环检测，仅有「不向 MultiCastPlanFragment 推 RF」的安全阀（`PlanNode.canPushDownRuntimeFilter`）；local RF 等待甚至无超时——扛住死锁的是解耦 multicast，不是 timeout。

## 考虑过的选项

1. **StarRocks 式解耦**（最快消费者节流 + 每消费者独立缓冲 + spill）：在执行层消除这一整类死锁与队头阻塞，multicast 变成无 liveness 附加条件的可组合原语；快消费者不被慢消费者拖累。代价：worst-case 积压为「最慢消费者到尾部」的整段（极端=整份 CTE 输出），需 spill/记账控住内存；改动波及全部 CTE 查询的内存模型；正确性依赖 spill 基础设施。
2. **保持耦合小缓冲 + 静态验环 + 环内 activation 降级**（选定）：multicast 内存 ≈ 固定 buffer，与 CTE 体量无关，不依赖 spill 即正确；等待环交给 6F 全局验环显式拒绝、由 RFD-6G 在 planner seal 期把纯 proof-backed 环内的 consumer 静态降级为 `NonBlockingLive{Batch}` 使环不再成立。
3. **无限缓冲不落盘**：直接 OOM 风险，否。

## 裁决

保持耦合反压模型（选项 2）。必须诚实记录：**这不是「耦合更优」的裁决**——就 multicast 内存模型这一层而言，解耦+spill 在架构本质上更可组合（消除死锁类、消除队头阻塞、成本花在可 spill 可记账的内存上）。选择耦合的真实理由是：①改动成本——不为一个窄死锁类重写 exchange 层与全部 CTE 查询的内存行为；②spill 独立性——有界小 buffer 在无本地盘/ spill 未成熟的部署上依然正确。

## 接受的妥协（诚实记录）

- 耦合并没有消掉成本，只是把它**挪了位置**：从「内存（可 spill、可 admission-control 的廉价资源）」挪到「调度耦合（CTE 扇出被最慢消费者限速的队头阻塞）+ 一套永久的 liveness 分析子系统（6F proof/细化图/E4 + 6G cycle-forced 降级）+ 环内等待的延迟税」。
- 此后任何触碰 RF / multicast / 多分支 sink 的改动都必须尊重并维护这套机制（新的多分支 sink 家族默认按耦合语义保守建 E4 边）。

## 何时重新评估

- spill 基础设施成熟并成为默认（解耦的内存风险由基础设施兜底）；
- CTE 重负载出现可测量的队头阻塞损失（慢分支拖垮整体扇出的真实案例）；
- 6F/6G 机制的维护成本（新形状补证明、误报处理、降级路径演进）持续超过一次 exchange 层重构的预估成本；
- 出现第二类需要 E4 式建模的多分支 sink 死锁形态（说明该类问题在扩散而非收敛）。

届时「切换到解耦 multicast」应作为**独立的大决策**按自身收益评估，而不是作为某个死锁补丁顺手带出。
