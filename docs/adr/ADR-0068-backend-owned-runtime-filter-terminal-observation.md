---
id: ADR-0068
title: "Backend-owned runtime-filter terminal observation"
domain: [runtime-filter, distributed-query-lifecycle]
status: superseded
supersedes: []
superseded-by: ADR-0073
date: 2026-08-13
provenance:
  - "mechanism: service-owned runtime-filter terminal observation; PR #741"
  - "discussion: 2026-08-13 Backend attempt-local observation and typed terminal transfer"
code-anchors:
  - "novarocks/backend/src/runtime_filter/observation.rs (RuntimeFilterObservationStore)"
  - "novarocks/backend/src/query_lifecycle/registry.rs (capture_terminal_profile_contribution)"
---

## 问题

Runtime filter 的 attempt-local 运行事实应由谁聚合、何时冻结，以及如何跨 FE/BE 生命周期边界进入最终 profile，才能既避免进程全局 registry，又不在资源释放后猜测或丢失事实？

## 背景与执行事实

ADR-0043 已把 row/scan evaluator 与 Effect 固定在 Execution，ADR-0044 又把 participant 的安装、replay、coverage、物化、订阅和路由状态固定在 Backend。运行过程中产生的 channel、producer stream、transport route、consumer row/scan outcome 同样依附于一个 Backend participant 和一个 query attempt；它们不是 Core 的进程全局事实，也不能依靠 fragment profile 的零散计数器重建。

Backend participant 的安装清单已经封存 channel、producer instance、route 和 consumer identity，可用作 observation 的有界授权集合。producer partition stream 只有在对应 instance 冻结非零 `partition_count` 后才能进一步授权。这样 store 的 cardinality 由安装事实与已认证 partition 数界定，无需保留原始事件、scan-unit id 或无界日志。

终止时，Query Lifecycle Control（QLC）必须先让本地执行静止，再捕获 immutable observation contribution，构造并保留 terminal record，最后才释放 query resources 和 participant。若先 cleanup，事实来源会消失；若捕获失败却制造 empty contribution，则会把“观测损坏”伪装成“该 query 没有 runtime filter”。

Frontend 只应消费版本化、具名 section 的 terminal value。它不应读取 Backend store、Execution observer 或 Core global state，也不应从 fragment counters 猜测 runtime-filter profile。

## 考虑过的选项

1. 继续使用 Core 进程全局 registry，并由各执行点直接写入。调用简单，但它跨越 Backend participant、Execution evaluator 与 QLC terminal owner，attempt 隔离和清理顺序只能靠约定，分布式部署还会把每个 BE 的局部事实误装成全局事实。
2. 只保留 fragment profile counters，Frontend 在 terminal 汇聚时拼接。复用现有 profile tree，但 channel/route/consumer identity、失败原因和未评估分类会丢失；多 fragment 重复计数与 partial failure 也无法可靠区分。
3. Backend participant 拥有有界折叠 store；Execution 只通过 fragment event sink 上报 typed Effect；QLC 在 capture-before-release 窗口把 immutable snapshot 转成版本化 terminal contribution；Frontend exactly-once 消费该 contribution。选择此方案。
4. 将每个原始事件写入 durable log，再由 FE 离线聚合。可提供更细审计，但明显超出 query profile 的需求，并引入持久化、retention、重放与隐私成本；当前没有足够收益证明它合理。

## 裁决

采用选项 3。每个 `RuntimeFilterParticipant` 持有一个 Backend-private `RuntimeFilterObservationStore`。store 仅接受安装清单授权的 identity，使用确定性有序 map 折叠固定宽度 counters 和最新版本/terminal 状态；所有累计使用 checked arithmetic。未知 identity、版本回退、非法 row effect、冲突 partition count 或溢出均记录 first-wins sticky error，之后 capture fail closed。可选 observer 只是隔离的旁路通知，panic 或同线程 reentrancy 不得影响 store。

Execution 保留 runtime-filter row/scan 语义，并把 typed outcome 发送到既有 fragment event sink；Backend event adapter 用 exact fragment identity 找到 QLC participant 后写入 store。Execution 和 Core 不再直接写 runtime-filter profile counters，也不保留 global observation registry。

成功与失败终止都遵循 `quiesce -> capture -> immutable terminal record retained -> release resources -> close participant`。无 participant 时显式构造 versioned empty contribution；存在 participant 而 capture、校验、编码、容量预留任一步失败时，不得伪造 empty terminal snapshot。并发终止通过 entry-local freeze latch 保证最多一次 capture/retain。

跨角色唯一传输面是 Core lifecycle 中立值 `QueryTerminalProfileContributionV1` 及其 concrete protobuf message。四个 section 按 canonical identity 排序、拒绝重复，并验证 identity、version、counter 与 row/scan/transport invariants。Frontend 在已保留的 participant snapshot 中 exactly-once 消费 contribution，使用 checked sum 生成 synthetic runtime-filter profile；empty contribution 不生成虚假节点。

## 接受的妥协（诚实记录）

这项设计只保存 query terminal profile 所需的聚合结果，不保存原始事件或 scan-unit identity。因此它不能回答精确事件时序、单个 scan unit 的历史或跨进程因果链；这是为了让内存上界可由安装契约证明，而不是因为原始事件没有诊断价值。若需要事件级审计，应由独立的采样或 durable telemetry 方案承担，不能放宽 attempt-local store。

typed contribution 会增加 lifecycle protobuf、canonical codec、版本校验和 FE synthetic profile 的维护成本，并使 capture 失败成为 terminal delivery failure，而不是降级为缺少 profile。这里选择 fail closed，是因为静默伪造 empty 会永久破坏 profile 可信度；代价是极端溢出或内部 identity bug 会让终止记录无法交付，需要通过日志和 tombstone 诊断。

Frontend 当前把 contribution 投影成 synthetic profile tree，而不是引入独立用户可见 telemetry API。这复用了现有 profile 消费面，但层级是稳定的逻辑投影，不等同于 Execution operator tree，也不能据此推断算子级时序。

## 何时重新评估

- 需要按单个 scan unit 或事件时间线做生产取证，且 sampling 无法满足时，应设计独立、有 retention 和容量契约的 durable telemetry；不得让本 store 保存无界原始事件。
- 一个 query attempt 的已认证 producer partition 或 consumer cardinality 大到 terminal contribution 接近 QLC encoded-size 上限时，应评估分层摘要或显式 telemetry attachment，而不是截断 counters。
- Frontend 需要把 runtime-filter observation 暴露为独立 API 或长期指标，而非 query profile 时，应新增版本化消费契约，并保持 terminal snapshot 为 immutable source。
- 如果未来 QLC 支持 durable takeover 或 terminal record 跨 BE 重建，必须定义 observation capture 的 ownership fence 和重放语义；不能让两个 Backend incarnation 同时冻结同一 attempt。
