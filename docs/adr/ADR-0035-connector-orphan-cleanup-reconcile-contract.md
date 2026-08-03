---
id: ADR-0035
title: "Connector orphan cleanup uses immutable receipts and reconcile-only unknowns"
domain: [provider-spi, table-maintenance]
status: active
supersedes: []
superseded-by: null
date: 2026-08-03
provenance:
  - "discussion: 2026-08-03 connector orphan cleanup recovery semantics"
code-anchors:
  - "novarocks/spi/src/connector/cleanup_maintenance.rs (ConnectorCleanupMaintenance)"
---

## 问题

对不可原子化的外部对象删除，如何在 frontend 重启、删除响应丢失和 connector incarnation 变化时，既不重复删除也不重新枚举候选对象？

## 背景与执行事实

对象存储、HDFS 和本地文件系统的删除不能提供跨对象、跨进程的原子事务。删除请求可能已到达 provider，而调用方在收到响应前超时；以 current connector incarnation 重放旧操作又可能把旧 table state 的结论作用到新的 provider generation。

Iceberg provider 才能判断 snapshot、metadata、statistics、Puffin 和时间阈值所定义的候选集合，也才知道某一路径可用的 version、ETag、size 与 mtime identity。frontend 则是 StateStore operation、同表 active fence、用户 SQL 结果与 restart recovery 的 owner。两边都不能安全地复制对方的事实。

## 考虑过的选项

1. frontend 持久化候选路径和 identity，并在恢复后逐项重发 delete。它看似直接，但把 provider 的对象存储语义、敏感 location 以及大 manifest 复制进 StateStore，也无法区分首次删除是否已经生效。
2. provider 在恢复时重新 list、重新 plan 后继续删除。它能降低 artifact 存储，但时间阈值和 live-reference 集合已变化，会把一次 operation 扩展为另一次破坏性 operation。
3. provider 写 immutable manifest 与 per-batch receipt；frontend 只持久化 bounded handle、digest、batch ordinal 和四类计数。首次 dispatch 后所有不确定性都只允许对同一 prepared batch reconcile。

## 裁决

采用第三个选项，定义 FE-only `ConnectorCleanupMaintenance` capability，并由 exact-generation lease 保持 provider generation 直到 terminal state。

- planning 冻结 base state、canonical candidate 顺序、manifest 与 batch descriptor；public SPI 和 StateStore 只携带 bounded opaque payload/handle/digest。
- `prepare_batch` 只验证 immutable batch，`execute_batch` 对每个 prepared batch 最多调用一次；执行调用后的错误、receipt/checkpoint 写失败均进入 reconcile。
- `reconcile_batch` 只能读取同一 manifest、prepared evidence 和 object identity，不得重新 list、plan 或 delete。`Unknown` 不得 terminal，必须保持 active fence；找不到 exact generation 时 operation 保持 `Unresolved`。
- provider 使用 strongest available identity 删除：versioned delete 优先，其次 ETag+size+mtime、再其次 size+mtime；没有可靠 identity 的对象不得进入 destructive batch。`NotFound` 为 `AlreadyAbsent`，明确条件失败为 `Failed`。
- terminal result 从 immutable candidates/receipts 投影后才 best-effort 清理 provider artifact；清理失败不改变已持久化的 operation 或 SQL 结果。

## 接受的妥协（诚实记录）

首版固定为 frontend 串行 batch，且对 manifest、receipt、对象数和 payload 施加硬上限。大表或不可可靠识别的对象会在首次删除前失败，吞吐和覆盖面都不如并行或 best-effort delete。选择它是为了让 response-loss、恢复与 identity change 的破坏性边界可验证，而不是因为串行删除更高效。

per-object `Failed` 可以结束 operation，因而 cleanup 的最终结果不是“所有候选都已删除”的承诺；它是冻结候选上可审计的执行结果。`Unknown` 则牺牲自动可用性以避免重复 dispatch。

## 何时重新评估

- 外部存储提供可证明的批量幂等删除 receipt，且能够跨 frontend restart 安全读取。
- 产品需要超过当前 hard budget 的 cleanup，并已有 provider-owned manifest ledger、保留、GC 与访问控制方案。
- multi-FE takeover 具备 exact-generation handoff 或 authoritative historical inspection，能安全处理 `Unresolved`。
- 需要公开 abort、并行 destructive batch 或新的 BE/native carrier；这些变化必须先重新裁决 ownership 与 failure contract。
