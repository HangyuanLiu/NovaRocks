---
id: ADR-0121
title: "Frontend serving lifecycle uses one-way admission drain instead of connection shutdown or remote management mutation"
domain: [runtime-role, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-08-27
provenance:
  - "discussion: 2026-08-27 frontend serving lifecycle and disposable FE"
  - "mechanism: frontend serving lifecycle implementation"
code-anchors:
  - "novarocks/frontend/src/server.rs (run_frontend_server_until_shutdown)"
  - "novarocks/frontend/src/mysql/mod.rs (run_mysql_server_until_shutdown)"
---

## 问题

可被随时替换的 Frontend（FE）如何停止接收新工作、继续完成已获准入的工作，并在有界时间内退出，而不把连接、外部路由或本地临时状态误当成正确性 authority？

## 背景与执行事实

生产拓扑是独立的 `role=fe` 与一个或多个 `role=be`；all-in-one 只复用这两个角色的启动路径作为测试便利。FE 拥有 MySQL session admission、全局 query coordination、catalog runtime projection 与 FE-owned background work；BE 拥有本地 fragment execution。因而终止 FE 时，已被 FE 准入的 attempt 仍需要 Native lifecycle/report ingress、connector/catalog runtime 与 result delivery 才能收敛。

已有 MySQL connection registry 是协议 owner 的连接清理工具（ADR-0102），不是 workload ownership 的替代。一个 idle connection 不应阻塞退出；反过来，仅关闭 listener 或终止连接又无法区分已进入 planning/commit window 的 statement。catalog logical desired state 由启动时选定的 source authority 提供（ADR-0115），而不是由 FE 进程、StateStore cache 或外部路由保存。

当前 management listener 没有 authenticated administrative mutation authority。把 Trino worker 的 graceful-shutdown HTTP mutation 直接复制过来，会向 management network 增加未经授权的远程进程关停能力。

## 考虑过的选项

**A. 收到 signal 后立即取消所有 connection 与 query。** 实现短，但把 protocol connection 误当 attempt。它会中断已经安全准入、且仍可在当前 FE 完成的 query/job，也无法表达 deadline 到达时的 typed cancellation source。

**B. 只停止 MySQL listener，等待所有 socket 自然断开。** idle session 会无限阻塞终止，且 signal 前 accept、signal 后认证的 race 仍可创建新 session。它不能限制 background effect-capable dispatcher。

**C. 把 serving 状态保存在 StateStore 或外部路由层。** 这会把 process-local lifecycle 变成持久化或跨 FE authority，与可丢弃 FE 及 ADR-0114 的 ProcessRuntime 分类冲突。外部路由只拥有是否把新连接导向 deployment 的可逆决策，不能封住已经存在的连接上的下一条 statement。

**D. 提供未认证的 management HTTP drain endpoint。** 操作便利，但越过当前 authorization owner，且形成一条与 process termination 不同的状态机。否决。

**E（选中）. FE-local、单向 serving lifecycle 与 RAII admission lease。** 一个 transport-neutral owner 以同一同步域线性化 `try_admit` 与 `begin_drain`。`SIGTERM`/`SIGINT` 是 v1 唯一 mutation authority；management 只读。所有 statement 与 effect-capable background attempt 在产生 side effect 前持有 lease，连接和 protocol task 仅在最终 teardown 才由 ADR-0102 owner 终止。

## 裁决

- FE serving state 单调为 `Starting -> Ready -> Draining -> Stopping`。只有 startup bootstrap 可进入 Ready；首次 `SIGTERM` 或 `SIGINT` 使 Ready 单向进入 Draining，重复 signal 不恢复 Ready。
- `try_admit(kind)` 和 `begin_drain()` 在一个同步域内线性化。先取得 lease 的 statement/background attempt 可在内部 drain deadline 前继续；先观察到 Draining 的请求在 parse、session mutation、planning、catalog materialization 或外部 effect 前收到 typed、可重试的 `FRONTEND_DRAINING` 拒绝。
- statement lease 覆盖每条 SQL statement，而不是整个 MySQL connection 或 multi-statement batch。idle session 不计 active attempt；drain 在线性化后拒绝它的下一条 statement。每条 batch statement 保持既有独立 autocommit 语义。
- FE background dispatcher 在创建 effect-capable attempt 前取得同类 lease。drain 后 timer、queue 与 reconciliation control work 可以存在，但不得转化为新的 attempt；已经有 lease 的 attempt 仍按既有 cancellation/publication contract 收敛。
- drain 立即使 base readiness 为 false，停止 MySQL accept 与 session registration；liveness、只读 management、Native lifecycle/report ingress、connector/catalog runtime 和 topology 保持到 active leases 清零或 deadline 到达。
- deadline 到达后，FE 以 typed drain-deadline source 取消仍存活的 attempts，再按有界 cleanup 完成协议连接终止和依赖逆序 release。该取消不改变 lake publication 的 KnownUncommitted / CommitUnknown / KnownCommitted 语义，也不自动重试、迁移或接管旧 attempt。
- all-in-one 收到终止 signal 时先 drain FE，FE 完成后才停止本地 BE；不得为此增加 direct-call execution 或单节点例外。外部 LB/Gateway 的 deactivate 与本地 drain 分别由其自身 owner 管理。
- management HTTP 只暴露 live、base readiness、serving state、sanitized bootstrap/catalog 计数、active lease 和 drain metric；v1 不提供 mutating drain API。

## 接受的妥协（诚实记录）

- **drain 并不保证已准入工作成功。** deadline 是进程存活上界，而非 transaction success guarantee。我们保留 publication unknown，代价是客户端在结果返回前断连时必须按 provider truth reconcile。
- **所有 SQL command 都需走统一 statement gate。** `SET`、`USE`、`KILL` 等不产生 lake effect 的命令也会被 drain 拒绝。这扩大了接入改动面，但避免旧连接通过 session mutation 建立旁路。
- **background worker 需要显式接线。** 没有全局 task hook；每个 effect-capable owner 都要在创建 attempt 前取 lease。代价是实现分散，换来 authority 不被隐藏在 scheduler/runtime global 中。
- **v1 只支持 OS signal 发起本地 drain。** 这降低了自动化便利性。未来若确有远程 drain 需求，必须先设计 authenticated admin authority、audit 与 exact target identity，再调用同一个 transition。
- **management 在 drain 期间持续监听。** 这延后了一部分 resource release，但让平台能区分 live、not-ready、graceful completion 与 forced cancellation，而不会把观测盲区伪装成成功退出。

## 何时重新评估

- 出现经过认证、审计和精确 target identity 约束的 management administration domain；
- LNP-9 定义 multi-active FE、attempt migration 或 cross-FE resume，需重新定义 lease 与 ownership 边界；
- 生产 drain 指标显示默认 timeout 持续导致高比例 deadline cancellation；
- MySQL protocol 或 background service 引入新的 effect-capable admission point，现有统一 gate 无法覆盖；
- 外部 orchestrator 无法提供大于 internal drain timeout 加 cleanup margin 的 termination grace period；
- FE lifecycle 被证明需要跨 process durable authority，而非当前的 ProcessRuntime；这会要求先修订 ADR-0114。
