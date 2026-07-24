---
id: ADR-0003
title: "RF consumers default to BlockingSnapshot; NonBlockingLive is a targeted downgrade"
domain: [runtime-filter]
status: active
supersedes: []
superseded-by: null
date: 2026-07-24
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/726 (the strict cycle guard that motivates the targeted downgrade)"
  - "discussion: 2026-07-24 CTE/E4/multicast 对话"
code-anchors:
  - "novarocks/core/src/sql/planner/distributed/build/runtime_filter_binding.rs (ConsumerActivation materialization)"
---

## 问题

Join RF consumer 有两种 activation：`BlockingSnapshot`（拿到 RF 才产出第一行）和 `NonBlockingLive`（立即消费，RF 到达后对后续数据晚应用）。既然 blocking 会参与死锁环（见 ADR-0002），为什么不全部改成 NonBlockingLive，而是默认 BlockingSnapshot、只做定点降级？

## 背景与执行事实

- **两种 activation 的结果语义完全相同**：RF 是保守预过滤（只删必不匹配行），晚到/不到只是多放行，join 本体兜底。选择纯粹是性能取舍。
- 差别在 **RF 作用到多少输入上**：BlockingSnapshot 在首输出前 acquire，RF 一到即对**整个** probe 输入生效；NonBlockingLive 的 RF 只对到达后的 batch 生效，**之前已流过的数据永久漏滤**——那部分收益不可追回。
- 更大的头在 **reader 级剪枝**：RF 已知时可整段跳过 file / row-group / split / partition，对存算分离引擎意味着不去对象存储拉那份数据（省 I/O 省钱）。这种剪枝**只有开读前已知 RF 才可能**——晚到的 RF 无法取消已发出的读。当前 native Join RF 仅做 chunk 级 mask，reader 级剪枝是既定演进方向，blocking 语义为它留门。
- Blocking 的等待有界（timeout → PassThrough，见 ADR-0001），且效果**确定**；Live 的过滤率是时序赌博——取决于 build 与 probe 的相对速度，同一条 SQL 的剪枝率随调度抖动。
- StarRocks 对照：global RF 约 20ms 短等 + 机会性晚应用——在「为剪枝红利等多久」这条轴上取了很短的点。NovaRocks 默认等待更长（默认 1s），并把「等或不等」做成 plan 的确定部分而非运行时赌博。

## 考虑过的选项

1. **全部 BlockingSnapshot**（定点降级落地前的现状）：planner 无条件密封 blocking。在 CTE multicast 拓扑下与耦合反压组成真环（ADR-0002），被全局验环（PR #726）拒绝或在运行时吞成 timeout 延迟税。
2. **全部 NonBlockingLive**：无死锁，但系统性放弃「RF 对全量输入生效」与 reader 级剪枝的可能性。对 RF 的典型盈利场景——小维表 build 喂大事实表 probe、RF 高选择性、build 快——等于花了 build+传输 RF 的全部成本，却只把过滤用在输入的尾巴上。净亏。
3. **默认 Blocking + 定点 Live 降级**（选定）：结构必然的降级（纯 proof-backed 环内 consumer，等待可证明只能以超时收场）在 planner seal 期静态执行（实现进行中，合入后回填 PR 号）；收益权衡型的选择（build 大/慢、RF 低选择性、深下推等「等不划算」的点位）留给后续 cost-based 决策（尚未实现）。

## 裁决

`BlockingSnapshot` 是默认——对绝大多数规划良好的 RF，「有界地等一下」拿到几乎全部剪枝红利；`NonBlockingLive{Batch}` 是定点手术刀，用在**等待不划算或不安全**的点位。降级决策静态密封进 plan（planner seal 期），不做运行时时序赌博。顺序：correctness/liveness-forced（环内必须不等）先行，cost-based（按收益选等或不等）后置。

## 接受的妥协（诚实记录）

- 在 cost-based 决策落地前，非环点位上「等不划算」的场景（build 大/慢、RF 低选择性）仍会白等 ≤timeout——这是把安全子集先做、收益子集后置的排期选择。
- Join 的 NonBlockingLive 执行路径（CompleteOnce live poll、batch 晚应用）是为定点降级新增的执行面，需要与 blocking 路径共用同一 predicate 编译以保证语义零漂移。
- 默认 blocking 意味着每个新的等待形态都要先通过静态 liveness 分析（见 ADR-0001 的前置税）。

## 何时重新评估

- cost-based activation 落地时（默认策略从「一律 blocking」演进为「按收益密封」，本 ADR 的默认值部分将被 supersede）；
- reader 级剪枝（file/row-group/split）接入 native Join RF 后，等待收益结构变化，需重新校准「值得等多久」；
- `runtime_filter_wait_timeout` 默认值调整，或出现证据表明大量查询的实际瓶颈在 blocking 等待本身。
