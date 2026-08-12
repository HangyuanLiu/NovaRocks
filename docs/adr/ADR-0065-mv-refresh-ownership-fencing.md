---
id: ADR-0065
title: "MV refresh ownership is fenced per target and inside each transaction"
domain: [frontend-mv, cluster-membership]
status: active
supersedes: []
superseded-by: null
date: 2026-08-13
provenance:
  - "discussion: 2026-08-10 MV refresh active-active recalibration"
code-anchors:
  - "novarocks/frontend/src/mv/coordination.rs (MvRefreshOwnershipRegistry)"
  - "novarocks/frontend/src/mv/repository/mod.rs (MvRefreshFenceSource)"
---

## 问题

当多个 Frontend 共享同一个 StateStore 时，什么东西阻止它们同时为同一个 MV target 开始 refresh，以及在哪一层
验证「我仍然是这个 target 的 owner」才算真正的 fencing？

## 背景与执行事实

Frontend 已经拥有 manual refresh、background scheduler 与 startup recovery（见 ADR-0036、ADR-0038），但用来仲裁
并发的三样东西都是**单进程语义**：

- `mv/activity.rs` 的 activity gate 是 process-local FIFO；
- `mv/scheduler.rs` 的 queue 与 running set 只存在于内存中；
- MV definition 上的 `refresh_in_progress` / `active_refresh_id` 只是记录，不是租约。

两个 Frontend 各自扫描 due definitions，都会发现同一个 target 到期，都会开始 attempt。更关键的是：MV repository
的 begin / action / finalize 事务当时**完全没有**验证任何 lease fence，所以一个已经失去所有权的 Frontend 依然可以
写入 durable refresh state。

同仓已有可直接复用的先例：statistics worker 取得 CP 系列 coordination lease 后，把一个 fence validator 闭包传给
repository，由 repository 在**自己的 write transaction 内**调用它。

## 考虑过的选项

1. **加强 process-local gate**（更严格的 FIFO、更长的 running set 保留）。零协调成本，但两个进程之间不存在共享
   状态，无论怎么加强都不能仲裁跨进程并发。
2. **在 service 层取 lease，然后调用 repository**。直觉上足够，但 lease 检查与 commit 之间存在窗口：期间另一个
   Frontend 可以接管，而那次 stale 写入依然落盘。这是「礼貌」而不是 fencing。
3. **给 repository 的每个 mutating 方法加一个 `fence` 参数**。语义正确，但要改约 26 个方法签名、它们的同步封装
   以及全部调用点。
4. **注入一个 fence source，由 repository 在事务内取用**。语义与选项 3 相同，改动面小得多。

## 裁决

采用选项 4，并固定三件事。

**其一，一个 target 一把 lease，所有入口共享。** resource key 是 ADR-0064 冻结的稳定 target identity
（provider ID + immutable target table UUID），**不含** numeric `mv_id`。理由是 StateStore rebuild 会重新分配
`mv_id`，用它做 key 会让同一个 target 在 rebuild 前后落入两个不同的所有权域。manual SQL、scheduler 与 recovery
竞争**同一个** key，因此「按入口拆分域」不可能让两个并发 attempt 看起来是正确的。不同 target 持有不同 lease，
仍然并行刷新。

**其二，每一次 durable refresh 转换都在自己的事务内验证 exact fence。** 覆盖 24 个 refresh-lifecycle 转换：
intent 创建、external action 记录、staging 与 publish observation、commit-unknown、四种 finalize、progress 清理、
五个 recovery 转换、watermark 与 metadata 更新、partition state 写入。**幂等重写也要验证**——takeover 之后，被取代
的 owner 即使写入相同的值也不允许刷新 observed state，否则新 owner 关于「谁最后观察过这个 target」的视图就是错的。

**其三，fence source 无法产出 validator 是错误，不是静默跳过。** 「我丢了 lease」与「这里没有配置 fencing」必须
是两个可区分的结果。丢 lease 因此**撤销写权限**，而不只是停止调度新工作。

definition DDL 明确留在这个 fence 之外：`CREATE MATERIALIZED VIEW` 不可能要求一个尚不存在的 target 的 lease，
它继续由 catalog attachment observation 守护（durable catalog attachment control plane，PR #881）。两个 fence 由**同一个**
Frontend transaction owner 组合进同一次 commit，而不是各建一套协调器。

## 接受的妥协（诚实记录）

- **每个 lifecycle 事务多一次 fence 读取与校验。** 这是 stale writer safety 的必要成本，不接受用「只在 service
  层检查一次」换取。
- **注入式 fence source 而非显式参数，意味着编译器不会强制调用方提供 fence。** 选它是为了避免 26 个签名 + 全部
  调用点的改动面，属于**因改动成本而非因更优**的取舍。缓解手段是「已安装 source 无法产出 validator 即失败」，
  以及 registry 未注册 target 一律 fail closed；但一个忘记安装 source 的 composition 仍然会得到未 fenced 的
  refresh 路径。真正的补强需要架构 guard 断言生产 composition 已安装，那尚未落地。
- **registry 按 `mv_id` 键。** repository 调用只带 `mv_id`，所以这是唯一可用的键；每个条目同时记录稳定 resource
  以防 rebuild 后条目存活于错误的 target incarnation，但这是补偿而非根治。
- **本决策落地时，三个 refresh 入口尚未切到 acquisition service。** 因此「两个 Frontend 竞争同一 target 只有一个
  越过 barrier」端到端还不成立；已经成立的是「任何 durable refresh 转换若不能在其事务内证明 fence 就会失败」。
  这一半先落地是因为 repository 强制点是另一半的前提，不是因为它本身构成完整保证。

## 何时重新评估

- 三入口切换完成、架构 guard 落地后：应重新评估注入式 source 是否仍需保留，或可否收敛为编译期强制。
- 出现「同一 target 需要多个并发 refresh」的产品需求（例如按 partition 并行刷新）：本决策的「一 target 一 lease」
  需要重新裁决为更细的 resource 粒度，而不是放宽 fencing。
- 若 fence 校验在高频 lifecycle 事务上成为可测量的开销：可重新评估是否合并同一事务内的多次校验，但不得退回到
  事务外预检。
- StateStore 提供原生的 per-key 条件写原语时：per-resource lease 可能可以简化，但「同事务验证」这一性质必须保留。
