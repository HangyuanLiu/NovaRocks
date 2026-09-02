---
id: ADR-0051
title: "Exact-generation connector write activation"
domain: [provider-spi, frontend-dml, frontend-mv]
status: superseded
supersedes: [ADR-0048]
superseded-by: ADR-0133
date: 2026-08-10
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/861"
  - "discussion: 2026-08-10 exact-generation provider writer activation during the Iceberg provider owner cut"
code-anchors:
  - "novarocks/spi/src/connector/write.rs (ConnectorWriteControl)"
  - "novarocks/spi/src/connector/control.rs (ConnectorControlPlanningLease)"
  - "novarocks/core/src/query_execution/contract.rs (ConnectorWritePlanningTemplate)"
---

## 问题

一次 Provider 签发的 write preparation 如何在不依赖 Core concrete registry、runtime downcast 或隐式 planning 副作用的前提下，绑定为同一 control generation 内可执行、可提交的 writer service？

## 背景与执行事实

`ConnectorWriteControl` 已把 admission、placement planning、aggregate commit、abort 与 reconcile 绑定到 retained exact write lease。Provider 签发的 `ConnectorWritePreparation` 封存 table、base version、input shape 与 opaque payload，阻止 application caller 拼装 provider authority。

但 preparation 与 planning 之间缺少正式 activation 阶段。现有 Iceberg 普通 data write、row-level write、copy-on-write 与 materialized-view refresh 都由 Core adapter 在 preparation 之后直接读取 concrete catalog registry，构造 catalog/collector/commit executor，再向 process-global write-service registry 注册 operation。`plan_write` 随后假设该隐藏注册已经发生。这个顺序产生三项不可表达的约束：注册必须属于 preparation 的 exact generation；同一 operation 的重试必须幂等而冲突注册必须失败；managed publication 的 typed intent 必须由 Provider 编码，不能由 Core 转成 snapshot property map。

把 implementation 与 generation runtime 移入独立 Provider crate 后，Core 既不能取得 Provider runtime，也不能继续构造 commit executor。现有 preparation 直接进入 planning 的契约因此不完整，ADR-0048 被本 ADR supersede；其 Provider-signed admission 与 durable terminal facts 裁决继续保留，只增加一个强制的 post-admission activation 阶段。

## 考虑过的选项

第一种是保留 Core activation adapter，以 concrete registry、service locator 或 runtime downcast取得 Provider generation。改动最小，但会永久保留第二 runtime owner，破坏 Cargo dependency ceiling，也使 later-current generation 有机会替代 preparation 所属 generation。

第二种是让 `plan_write` 同时完成 service reservation。它不增加公开方法，但把一次性的 semantic activation 混入 placement-dependent planning。planning 可因 writer placement 或 execution attempt 重跑；在其中隐式创建 mutable service，会让幂等、冲突检测、取消和资源释放都依赖调用顺序，而类型无法区分未 activation 与已 activation 的 write。

第三种是在现有 `ConnectorWriteControl` 上增加 generic exact-generation activation。它接收 Provider-signed preparation 或 row-mutation execution plan、operation identity、bounded typed semantic intent 与 request context，返回 Provider-signed activation proof；后续 planning 必须消费该 proof。

## 裁决

采用第三种方案。

Write lifecycle 固定为 `prepare -> activate -> plan -> execute -> commit/abort/reconcile`。`prepare` 保持纯 admission，不注册 writer、不创建 staging artifact、不执行 catalog mutation。application owner 在需要时先持久化 operation intent，再通过由原 planning lease 派生的 exact write lease调用 activation。

Provider-signed admission 与 durable terminal facts继续作为该 lifecycle 的两端约束：preparation封存 exact owner、table、base version、tagged input shape与 opaque provider authority；application caller不能提交任意 provider planning payload。commit success仍以 bounded provider-neutral receipt表示，commit unknown仍保存 opaque evidence并只在 retained exact capability上 reconcile；Frontend持久化并原样转交这些事实，不解码 table-format payload或从错误文本猜测 external truth。

Activation request 使用 tagged source，完整携带一个 ordinary `ConnectorWritePreparation` 或一个已由同 generation 签发的 row-mutation execution plan；不能用互斥 optional 字段拼装。它同时携带 operation identity、request context 与 tagged semantic intent。ordinary write 不附加 provider payload；managed publication 使用有界、provider-neutral typed facts表达 refresh/materialization identity、marker、technique、base watermark、definition fingerprint 与 empty-input disposition。Provider 独占这些 facts 到 table-format provenance/snapshot properties 的编码。

Provider 校验 owner、incarnation、operation、preparation/route digest、target ref 与 semantic intent 后，在该 generation-local runtime 内预留 writer/committer service，并返回 sealed `ConnectorWriteActivation`。该值绑定 owner、operation、source digest 与 activation digest，但不暴露 catalog client、credential、table-format payload或 runtime object。`ConnectorWritePlanningRequest` 必须消费 activation，而不是裸 preparation；类型上不能表达未 activation 的 planning。

Activation 可以创建 process-local、尚未对外可见的 service reservation，但不能写 staging data、提交 external metadata 或改变 durable application state。相同 activation digest 的重复请求返回同一逻辑结果；同 operation 的不同 digest确定失败。取消、deadline 或 validation failure 在 external action 前终止。terminal completion、abort/reconcile、generation retirement 与 shutdown负责释放 reservation；retained lease保证 runtime 在 terminal decision 前不被销毁。

Activation proof 只存在于 FE control/application 内，不进入 native wire或 durable journal。BE 仍只接收 provider-planned opaque writer handle；Frontend 仍拥有 operation lifecycle、query orchestration、terminal outcome 与 durable evidence，Provider仍拥有 external commit truth。

## 接受的妥协（诚实记录）

该裁决为所有 Provider、fake、普通 DML、row mutation、MV 与 planning template 增加一个 breaking SPI 阶段，改动面大于把注册塞进 `plan_write`。同时，generation runtime 必须维护有界的 process-local activation reservation，并明确处理 terminal cleanup 与 retirement drain。选择这一方案不是因为多一个阶段更简洁，而是因为现有隐藏注册已经具有独立语义；把它建模为 sealed state transition，才能在删除 concrete Core registry 后继续证明 exact-generation authority、幂等与资源所有权。

Managed publication facts进入系统 SPI 也扩大了公开 DTO 面。接受这一点是因为这些值描述跨 Provider 都必须理解的 application publication semantics，而不是 Iceberg snapshot-summary key 或 codec；若未来只有单一 Provider需要某字段，该字段不得继续泛化进入 SPI。

## 何时重新评估

- 所有受支持 Provider 都能仅凭 immutable preparation 与 planning request无状态地产生 writer handle，且不再需要 operation-local committer reservation。
- durable cross-generation takeover 要求在新 process恢复 activation，此时需要单独裁决 activation evidence 的持久化与 fencing，不能复用当前 process-local proof。
- 新 Provider 证明 managed publication 的某项 typed fact并非跨 Provider语义，或需要当前 tagged intent无法表达的新 application publication model。
- activation reservation 的数量、内存或清理时延成为可观测生产瓶颈，需要 durable indirection或不同生命周期分层。
