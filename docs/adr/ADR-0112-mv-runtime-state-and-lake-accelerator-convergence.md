---
id: ADR-0112
title: "MV runtime state is process-local and StateStore is a lake-source accelerator"
domain: [frontend-mv, provider-spi]
status: active
supersedes: [ADR-0038, ADR-0061, ADR-0075, ADR-0096, ADR-0109]
superseded-by: null
date: 2026-08-26
provenance:
  - "discussion: 2026-08-26 MV runtime-state and lake accelerator convergence"
  - "implementation: local branch; PR number to backfill after merge"
code-anchors:
  - "novarocks/frontend/src/mv/service.rs (FrontendMvService)"
  - "novarocks/frontend/src/mv/domain/lake_rebuild.rs (rebuild_mv_definition_from_lake)"
  - "novarocks/spi/src/connector/write.rs (ConnectorManagedPublicationIntent)"
  - "novarocks/connector/iceberg/src/commit/write_control.rs (IcebergWriteControl)"
---

## 问题

在 lake descriptor 与 publication facts 已能完整表达 MV desired semantics 和已发布水位后，如何避免
StateStore 中的旧 attempt、lease、scheduler 或 recovery 重新成为跨进程 mutation authority，同时让可重建
projection 在并发 finalization、同名对象重建和部分 catalog 失败时仍不倒退或误指向新对象？

## 背景与执行事实

MV 的完整 desired semantics 位于 target 的 versioned descriptor；当前 snapshot、publication provenance、base
waterline 与目标物理 identity 由 provider 的 exact lake observation 提供。它们是能在 StateStore wipe 或 FE restart
后重新取得的外部事实。与之相反，active refresh、FIFO waiter、scheduler queue/error/backoff、worker permit、
statement deadline 和 recent terminal 只描述当前 FE 如何驱动一次 attempt。把后者持久化会让新进程根据旧记录 claim、
takeover、inspect 或 reconcile 已经可能派发的 publication，违反 ADR-0110 的 crash-only outcome。

可重建并不意味着可以采用任意 stale lake observation。一个 `KnownCommitted` 的 S1 finalizer 若把自己携带的
receipt 直接写回 StateStore，就可能在 S2 已提交后将 published waterline 回退。逻辑 catalog/namespace/table
名字也不足以代表同一 target：DROP/recreate 后同名表具有新的 provider-observed immutable object identity。因而
accelerator 的 source revision 必须同时绑定 descriptor digest、target snapshot 与 opaque object identity。

Iceberg 可在一个 `TableCommit` 中组合 partition-spec transition、data snapshot、main ref、provenance 与
application descriptor properties；将 descriptor 留给后置 property mutation会重新引入第二个 external frontier。
Catalog enumeration 还可能无法证明完整性，单个 package 也可能损坏；把这些都压成成功的空列表会使旧 projection
伪装成可信真相。

## 考虑过的选项

1. **保留 ledger、lease、recovery 与 durable scheduler。** 它保留跨 restart 的进度和自动处理，但旧 StateStore
   record 继续决定新进程能否解释并推进 effect-capable attempt；即使加 fence 也不能把本地控制面变成 lake truth。
2. **不保留任何 StateStore MV projection，每次都直接读取 lake。** 它避免 projection 竞态，但使 catalog、dependency
   和 SHOW 的每次消费都依赖完整远端 I/O，且没有可验证的 source-index/rebuild 边界。
3. **lake facts 为唯一 authority，StateStore 为 source-aware Accelerator，运行态只属于当前进程。** 每次 projector
   从 latest exact package 构造完整 projection，并用 CAS 防止旧 payload 覆盖新版本；catalog/readiness 以完整性和
   package outcome 局部隔离。（采用）
4. **把历史 attempt/recovery 移到外部 scheduler 或 serverless service。** 更换存储位置不会消除它作为第二条
   publication authority 的问题；只读审计可另行设计，但不能反向授权 mutation。

## 裁决

1. `MvDescriptorV3`、exact current snapshot/provenance、base waterline 和 provider-observed immutable target
   object ID 是 MV 的 lake authority。StateStore 只保存 versioned Accelerator projection：完整 definition、target/
   dependency indexes、aggregate published waterline、source revision 与可重分配的内部 numeric ID；不保存 refresh
   attempt、lease/fence、recovery、per-partition runtime、scheduler wall-clock 或 cleanup backlog。
2. startup、explicit resync、wipe rebuild 与 `KnownCommitted` finalization 必须共用 source-aware projector。每一轮先
   读取 current CAS version，再重新读取 latest exact lake package，构造完整 payload 后 CAS；CAS conflict 必须丢弃
   payload并从 lake 重读，不能重用 S1 receipt 或 stale observation。object identity 改变时旧 projection、ticket 和
   finalizer 必须失效。
3. catalog discovery 明确区分 `Complete` 与 `Incomplete(reason)`；catalog incomplete 只让该 catalog 的 MV domain
   在当前进程 `Unavailable`，单 package corrupt 只隔离该 target。StateStore/global source 无法打开仍是 FE startup
   failure。consumer 只读取 ready projection 与 current-process runtime，不能绕过 projector 读取 raw store。
4. active publication、FIFO admission、scheduler queue/error/backoff/next-run、readiness、SHOW runtime observation
   与 bounded terminal observation 都是 current FE `ProcessRuntime`。CREATE、ALTER、DROP、manual/scheduled refresh、
   repartition 和 automatic maintenance 在 descriptor read、target observation 和 preparation 前取得同一 canonical-target
   FIFO gate；gate 只做本进程排序，不是跨 writer fence，外部 correctness 仍来自 immutable identity、target OCC 与
   catalog commit。
5. 每次可能产生 lake mutation 的 MV operation 在首次副作用前冻结 UUIDv7 `LakePublicationId`。它是 staging ref、
   provenance、typed terminal、日志与用户可见结果的唯一 outward identity；numeric refresh/materialization ID、marker
   token 与独立 action ID 不得参与 external contract。`KnownUncommitted` 终止；`CommitUnknown` 以同一 ID 立即终止，
   后续禁止 inspect、reconcile、abort、cleanup、projector 或 re-dispatch；`KnownCommitted` 只允许仍存活 attempt 在原
   deadline 内进行纯 projector finalization，失败仅把 exact target 标为当前进程 `Unavailable`。
6. repartition 在同一个 Iceberg `TableCommit`、同一个 `LakePublicationId` 下原子提交 partition spec、data snapshot、
   main ref、publication provenance 和完整 canonical next descriptor properties；不存在 post-commit descriptor patch。
   success 与 crash staging remnants 一样只由 generic exact-head、age-gated GC 处理，成功路径不主动 drop staging ref。

## 接受的妥协（诚实记录）

重启会遗忘旧 MV 的 RUNNING、queue、error、backoff、next-run、history 与 cancel handle；崩溃后的 unknown publication
也可能留下需要人工核对的残留。我们选择这些可用性和运维成本，不是因为遗忘状态更高效，而是因为保留它会让新进程获得
解释和推进旧 effect-capable attempt 的权力。

source-aware projection 会额外读取 lake，CAS 冲突还会重复读取；catalog incomplete 期间该域的 MV 不可用而非以旧
projection 服务。我们接受 I/O 与局部不可用，而不是把无法证明完整性的枚举或 S1 payload 当成真相。

process-local gate 无法阻止多个 FE 或外部 writer 重复做准备工作，且 generic GC 的安全年龄会延迟回收 staging
objects。我们接受重复成本和短期存储开销，因为把 StateStore lease、scheduler designation 或即时 cleanup 重新包装
成 correctness authority会推翻 ADR-0110 的 crash-only 边界。

## 何时重新评估

- Catalog/provider 提供可证明的跨进程 idempotency、operation status 与 proof-bound cleanup，使 old attempt 的
  mutation authority 可以被严格限制；
- 产品要求跨 FE scheduler designation、持久 job history 或 cancel/progress，且先定义了不能反向驱动 publication 的
  只读 observation contract；
- lake observation 的 I/O 或 catalog completeness 故障成为可量化的可用性瓶颈，并存在不引入第二 authority 的
  source manifest 或 cache proof；
- 支持的 provider 无法把完整 descriptor properties 与 repartition publication 放进一个 atomic external commit；
- 需要保留 per-partition published facts，且能由 exact lake package 无损且有界地重建这些 facts。
