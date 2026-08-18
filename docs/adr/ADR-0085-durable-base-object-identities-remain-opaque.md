---
id: ADR-0085
title: "Durable base object identities remain opaque"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-18
provenance:
  - "discussion: 2026-08-18 durable MV and maintenance physical identity acceptance"
code-anchors:
  - "novarocks/spi/src/connector/metadata.rs (ConnectorTableObjectId)"
  - "novarocks/connector/iceberg/src/commit/write_control.rs (iceberg_uuid_from_object_id)"
---

## 问题

当 MV refresh/publication 与 table maintenance 等 durable caller 需要跨重启证明基表仍是同一个物理对象时，如何保存并传播物理 identity，既防止同名 drop/recreate 的 ABA，又不让 Iceberg UUID 成为 Core、SQL 或 Frontend 的协议？

## 背景与执行事实

逻辑 `ConnectorTableIdentity` 只能解析当前 catalog 名称，不能证明同名表仍是原对象。ADR-0075 已将 physical object capture/rebind 定义为 exact metadata lease 下的 Connector 契约，并规定 `Missing`、`Replaced` 与 `Unsupported` 必须是 typed outcome。

MV durable definition、refresh ledger、schema contract、publication/recovery descriptor 以及 maintenance operation 都会跨 Frontend 重启保存基表事实；它们不能保存 provider runtime handle，也不能在重启后以 latest logical lookup 替换 submission-time 对象。另一方面，Iceberg UUID 只是在 Iceberg provider 内表示这项 identity；另一个 provider 可以使用非 UTF-8 或非 UUID 的有界字节。

`ConnectorTableObjectId` 因而是有界、可比较、可序列化且日志脱敏的 raw-byte value。Core、SQL 与 Frontend 可保留和比较它，但不解释字节。Iceberg 的 lake provenance 仍需要 canonical UUID JSON，因此 provider 在写入或比对该 provider-local 事实的最后边界严格转换 object ID；非 UTF-8、非 UUID 或非 canonical UUID 均失败。

## 考虑过的选项

**A：让 Core durable schema 继续保存 Iceberg UUID 字符串。** 这使当前 Iceberg 调用点较少改动，但把一种 provider 的编码泄漏为通用 durable 协议，迫使未来 provider 模拟 UUID，并诱导 Frontend/SQL 在边界外解析 identity。

**B：只保存 logical table name，并在每个恢复 attempt 重新解析 latest。** 记录最小，却无法区分同名 drop/recreate；replacement 会被静默当成原任务继续执行。

**C：保存 opaque `ConnectorTableObjectId`，只在 provider-local provenance 边界转换。** durable caller 可通过 rebind 证明连续性；Core/SQL/Frontend 不依赖 Iceberg 语义；provider 保留自身 canonical metadata 的编码规则。

**D：保存 provider metadata handle 或增加 Frontend 到 Iceberg 的 UUID 转换器。** 前者不能跨进程安全持久化，后者把 provider 专有知识与第二条 authority 放进 application 层，并破坏 Connector 可替换性。

## 裁决

选择 **C**。所有 durable MV 基表物理 identity 和所有 durable maintenance target identity 均使用 `ConnectorTableObjectId`；Core durable schema、SQL frozen facts、Frontend repository/Avro carrier、staged-publication/recovery descriptor 与 maintenance model 一律保存 raw bounded bytes，而非 Iceberg UUID。

submit/create 时通过 ADR-0075 的 capture 获取 object ID；attempt 在 external side effect 前通过同一契约 rebind。只有 `Bound` 可以继续；`Replaced` 在可证明零派发的 maintenance record 上进入 fenced `TargetReplaced`，`Missing` 与 `Unsupported` 终止而不派发。已派发或无法证明未派发的历史记录保留原有 reconciliation，不伪造 `TargetReplaced`。

Iceberg 是唯一可将 opaque object ID 转为 UUID 的 owner，且只用于 Iceberg provenance 的读写、比对与恢复；转换严格要求 canonical UUID。target table UUID、snapshot 与 Iceberg provenance 保持 provider-local 事实，不反向定义 Core durable base identity。

## 接受的妥协（诚实记录）

这是一项不兼容 durable schema 迁移：旧 MV/maintenance record、Avro schema 与旧 publication descriptor 都不再被读取；现阶段没有历史用户，测试数据和运行时 fixture 必须重建。选择 hard cutover 是为了避免 long-lived dual-read、字符串 fallback 和两套 identity authority，而不是因为迁移成本更低。

每个 attempt 增加 capture/rebind metadata I/O，并要求 provider 在有界 record 预算内提供 stable ID；这扩大了失败面。我们接受它以换取 drop/recreate 的 fail-closed 语义，不能以 latest lookup、UTF-8 猜测或 UUID parse failure 的 fallback 继续执行。

## 何时重新评估

- 产品开始承诺升级前 durable MV/maintenance record、publication descriptor 或外部恢复数据可继续读取时；必须另立 versioned migration/retention 决策，不能在此契约中悄悄 dual-read。
- 第二个 provider 无法在 exact metadata observation 中给出稳定、有界的 object ID，或需要比 byte equality 更强的 identity proof 时；应扩展 Connector 契约而非将 Iceberg UUID 上提。
- lake provenance 需要容纳非 UUID provider identity 时；应由 provider-own provenance 的 versioned schema 解决，而非改变 Core durable carrier。
- 多 FE takeover 需要把 capture/rebind 与新的 catalog fence 组合为更强线性化证明时；需要单独裁决 authority 与 recovery evidence。
