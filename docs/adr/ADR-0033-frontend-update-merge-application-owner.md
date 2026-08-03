---
id: ADR-0031
title: "Frontend owns UPDATE and MERGE application lifecycle"
domain: [frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "implementation: frontend mutation lifecycle cutover"
  - "discussion: 2026-08-03 frontend UPDATE/MERGE ownership"
code-anchors:
  - "novarocks/core/src/engine/mutation_engine.rs (MutationEngine)"
  - "novarocks/frontend/src/dml/mutation/mod.rs (DmlService::try_execute_update)"
---

## 问题

UPDATE 与 MERGE 的 SQL admission、durable lifecycle、connector staging 和外部提交应由哪个角色拥有，如何在不泄漏 Iceberg 私有 AST 或 write session 的前提下保证恢复证据正确？

## 背景与执行事实

frontend 已拥有 INSERT、DELETE、CTAS 与 TRUNCATE 的 production application route，并持久化 StateStore operation journal。UPDATE 与 MERGE 仍需要 core 的 SQL 私有 AST、Iceberg metadata/assignment 校验、COW/MOR execution kernel、以及 exact connector write session；这些事实不能复制到 frontend，也不能通过公共 SPI 或 MetaStore transaction runner 反向暴露。

外部 mutation 的 abort 不是简单的 cleanup：connector 可以明确返回未提交、明确返回已提交，或因 RPC/证据不足返回未知。因此 durable intent 必须早于 match、cohort registration 与 staging；MERGE 的 matched UPDATE/DELETE 与 not-matched INSERT 必须继续封在一个 aggregate connector operation 中，以产生至多一个 snapshot。

## 考虑过的选项

1. 保留 core command dispatcher 与 MetaStore transaction runner，frontend 只作 SQL router。改动较小，但会产生两个 application owner，journal intent 不能成为 staging 的先决条件，恢复真相会分裂。
2. 将整个 mutation flow、Iceberg AST 和 connector contract 移到 frontend。owner 表面单一，但 frontend 会取得 provider 私有模型，破坏 core/frontend 边界，并使 MV/change-stream kernel 被复制。
3. frontend 拥有两个 UPDATE/MERGE use case 和 journal runner；core 提供一对一 `MutationEngine` reverse port，返回 opaque prepared/commit/abort handle。frontend 只了解 statement kind、durable target 与 RowDelta lifecycle；core 保留 metadata、COW/MOR kernel 和 exact connector session。

## 裁决

选择选项 3。frontend 按 INSERT → DELETE → UPDATE → MERGE → CTAS → TRUNCATE 的顺序识别 statement；一旦 UPDATE 或 MERGE 被识别，错误终止该 route，不回退 core command dispatcher。inert prepare 可解析和校验但不得启动 match/source query、写 lease、cohort 或 staging。frontend 在 stage 前创建 `RowDelta` Preparing intent，subkind 固定为 `UPDATE` 或 `MERGE`。

`MutationEngine` 的 prepared/commit/abort handle 是 opaque、带 attempt/kind 一次性门的能力。stage 在拥有 exact session 后的失败必须返回 typed abort；frontend 将 KnownUncommitted、KnownCommitted、CommitUnknown 如实写入 journal。MERGE 不按 action 拆 transaction：所有分支使用一个 sealed aggregate operation、一次 commit、最多一个 snapshot。journal 的 commit family 固定为 `RowDelta`，具体 COW/MOR provider action 保留在 opaque handle 与 recovery evidence。

## 接受的妥协（诚实记录）

这不是通用 DML SPI。新增的是 frontend 唯一 consumer 的窄 reverse port，因而仍有一层 handle/state 转换代码，也必须同时维护 core kernel 与 frontend runner 的测试。选择它是为了在不大规模搬运 mutation kernel 的改动成本下完成真正的 owner cut，而不是因为两个 crate 间的 opaque handle 比直接调用更容易理解。

single-FE journal 的恢复语义也没有借机扩展为多 FE takeover/fencing；未知提交保留 unresolved evidence，不能猜测重试或删除文件。DELETE 的独立 reverse port、MV flow 与其它 DML family 不由本决定重写。

## 何时重新评估

- 若新增第二个 frontend consumer，或 mutation capability 需要被独立 connector crate 消费，应重新评估是否形成稳定 SPI。
- 若多 FE active-active、takeover 或 fencing 进入生产，需要为 operation journal 和 exact write lease 新建独立决策。
- 若 connector external outcome contract 发生变化，或 MERGE 无法保持单 aggregate commit/snapshot，必须先重新裁决，不能在 frontend 加 catalog 特例或 fallback。
