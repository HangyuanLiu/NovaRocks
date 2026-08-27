---
id: ADR-0065
title: "A per-table lease attempt is the single dispatch authority for frontend table maintenance"
domain: [table-maintenance]
status: superseded
supersedes: []
superseded-by: ADR-0111
date: 2026-08-13
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/862"
  - "discussion: 2026-08-10 CP-4A table-maintenance frontend fencing"
code-anchors:
  - "novarocks/frontend/src/table_maintenance/coordination.rs (MaintenanceCoordination)"
  - "novarocks/frontend/src/table_maintenance/repository.rs (validate_bound_fenced_authority)"
  - "novarocks/frontend/src/table_maintenance/worker.rs (recover_claimed_jobs)"
---

## 问题

多个 frontend 共享同一个 StateStore 时，谁有权对某张表发起维护动作、谁有权把维护结果写回
durable 记录？V1 optimize、V2 metadata maintenance、V3 distributed rewrite、V4 orphan cleanup 是四套
独立的 typed lifecycle，它们如何在不合并状态机、不设置全局 scheduler leader 的前提下，避免两个 FE
同时对同一张表派发外部操作，或让一个已经失去所有权的旧 worker 覆盖新 owner 的状态？

## 背景与执行事实

ADR-0009 已经把表维护的 application/lifecycle 交给 frontend，ADR-0035 已经为 orphan cleanup 建立
immutable manifest + 逐 batch receipt + reconcile-only unknown 契约。但在此之前，维护记录的并发保护只有
两样东西：V1 用 record CAS 做 claim，V2～V4 用「同一张表是否已有 active record」做互斥。两者都只在
单进程假设下成立。

- record CAS 只能防止同一条记录被并发改写，不能防止另一个 FE 在旧 worker 仍在向 connector 派发时
  取得同一张表的执行权。
- V2 记录里保存的 `instance_id + incarnation_id` 是 connector generation fence。它能阻止同一进程误用
  另一个 provider generation，但对「另一个进程写同一条 durable 记录」完全无效。
- 启动恢复把所有 `Running` 且没有 outcome 的 V1 job 直接判 Failed。这是单 FE 重启策略：它把「外部结果
  未知」当成「外部操作失败」，在多 FE 下会把另一台机器正在跑的作业标成失败。

同期已经落地的两块能力决定了本决策的形状：control-plane coordination primitives 提供
`IncarnationGate`、`LeaseManager` 与 `LeaseFence::validate_in`（后者在调用方自己的写事务里重读 control
incarnation 与 exact held lease）；frontend DML coordination kernel（PR #863）已经把进程级
`FrontendCoordinationRuntime` 装进 application host，DML 与 statistics 都消费同一个实例。

## 考虑过的选项

1. **全局 scheduler leader**：选一个 FE 独占所有表的维护调度。实现最简单，但把维护吞吐绑死在单机上，
   且 leader 切换期间所有表都停摆；与「任意健康 FE 都能提交并执行维护」的目标直接冲突。
2. **每个 action family 各自加锁**：V1/V2/V3/V4 分别持有自己的 lease。并发度最高，但同一张表会同时存在
   四个「自认为拥有该表事实」的执行者——rewrite 正在改文件、cleanup 正在删文件、metadata 正在过期快照，
   彼此的 base state 互相失效。
3. **继续用 record CAS，只在写回时比较 revision**：改动最小，但它只保护 StateStore 记录，不保护外部副作用。
   旧 worker 仍会在失去所有权后继续调用 connector。
4. **per-table lease + 同事务 fence 校验**（采用）。

## 裁决

同一个 canonical logical table target（connector instance identity + normalized namespace + normalized table）
对应**一个** coordination resource；V1～V4 全部 family 共用它。每次取得该 resource 生成一个新的
`AttemptId` 与递增 fencing token，这个 attempt 是该表在该时刻**唯一**的 dispatch authority。

三条强制规则：

1. **新 intent 先过 incarnation gate**：创建维护记录是短事务，只做 `WriteAdmission` 校验，不取 lease；
   restore/reconciling 模式拒绝新 intent，gate 不可用时 fail closed，不落内存态作业。
2. **每一次 authority-bearing transition 都在同一个 StateStore 事务里同时断言**：记录当前状态允许该转换、
   记录上绑定的 attempt 与调用方 attempt 相同、调用方的 `LeaseFence` 仍然有效。三者缺一即拒绝。
3. **外部派发分三段**：先提交足以恢复的 immutable intent/plan/prepared evidence（fenced），再在事务之外调用
   provider，最后持久化 typed receipt 并 fenced 推进状态。每个 cohort、每个 destructive cleanup batch 之前
   重新检查 authority；lease 丢失、incarnation 变更、clock unsafe 或 renew 结果未知之后，不再开始任何新的
   外部调用。

派生的两个结构决定：

- **V1 → V3 继承而非二次 acquire**：optimize job 派生的 distributed rewrite 复用父 attempt，并在创建子操作的
  同一事务里断言两者 authority 相同。同一张表被同一个执行链取两次锁只会自我阻塞或制造第二个权威。
- **恢复是显式 takeover，不是继承**：接管方证明自己持有当前 lease 后，把记录上的 attempt **替换**为自己的
  （`adopt_authority_fenced`），之后走普通的同 attempt 校验。旧 attempt 的 provenance 从不被信任。
  V1 启动恢复据此改判：已有 outcome 则终结；已经派发过子操作（durable `dispatched_child`）则 fail closed 并
  指向该子操作；两者都没有则退回 PENDING，任意 FE 可以重跑——因为它证明了没有产生任何外部副作用。

coordination resource key 按 canonical lake 名编码，不施加 SQL 标识符规则：`orders-2026` 是合法的 Iceberg
表名，若按标识符校验会让这张表的所有维护动作在 acquire 处永久失败。

## 接受的妥协（诚实记录）

1. **同一张表的所有维护 family 串行**。吞吐低于 per-family lease。换来的是 rewrite/cleanup/metadata 不会
   各自认为自己拥有同一张表的事实。这是明确的正确性优先取舍，不是「暂时这样做」。
2. **resource key 以名字为粒度**。表被 drop 再以同名重建后，若旧的 unresolved 维护记录还在，新表的维护会被
   挡住。我们接受这个 ABA 防护带来的可用性代价：宁可要求人工收敛旧记录，也不在缺少 table UUID 时靠猜测放宽
   并发。
3. **CP-4A 只能证明 active-active claim 与 stale-write safety，不能自动恢复旧 provider generation**。
   进程重启后旧的 exact `ConnectorInstanceIncarnation` 不可再取得，此时状态停在 `Unresolved`，需要
   provider-owned historical inspection 才能收敛。这是已知缺口，不是设计终点。
4. **多 holder / takeover 的证据来自 SQLite 上的确定性单元与故障测试，不是真实双 FE 运行**。当前 live
   验收固定为 1FE+3BE + SQLite StateStore。我们不因为抽象语义已被证明就宣称多 FE 生产可用。
5. **恢复扫描每轮都跑一次 RUNNING 索引**。正常情况下该索引为空，代价是一次范围读；换来的是一个仍在
   takeover observation 窗口内的目标能在下一轮被收敛，而不是等到下次进程重启。

## 何时重新评估

- 维护 target 能够取得稳定的 table UUID 时：resource key 应改为 UUID 粒度，妥协 2 随之消失。
- provider-owned historical inspection 落地后：妥协 3 的 `Unresolved` 出口应改为可证明的收敛路径，本 ADR
  关于「exact generation 不可得即 fail closed」的表述需要重写。
- 如果实测显示同表串行成为真实瓶颈（例如大表的 metadata maintenance 长期阻塞 optimize），需要重新审视
  选项 2，但前提是先给出「四个 family 如何共享同一份 base state 真相」的答案，而不是简单拆锁。
- 如果 coordination primitives 不再支持在业务事务内校验 fence（`LeaseFence::validate_in`），本决策的第 2 条
  规则失去实现基础，必须回到设计讨论。
