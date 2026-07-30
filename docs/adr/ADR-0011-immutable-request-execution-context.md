---
id: ADR-0011
title: "Immutable request execution context"
domain: [distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-07-29
provenance:
  - "discussion: 2026-07-29 request execution boundary"
code-anchors:
  - "novarocks/core/src/query_execution/request_context.rs (RequestContext)"
  - "novarocks/core/src/query_execution/backend.rs (BackendTopologySnapshot)"
  - "novarocks/core/src/server/mod.rs (execute_sql_in_worker)"
---

## 问题

一个 statement 如何在 optimizer、DML/MV 入口和 frontend coordinator 之间使用同一份 topology、deadline 与取消身份，而不依赖 TLS、全局 membership 或重算默认值？

## 背景与执行事实

frontend 是 live backend membership/generation 的唯一 owner；它的 revision 与 `FeDeploymentView.topology_revision` 的 wire 语义不同。statement admission 先取得 QLC statement lease，随后才有可以被 KILL、disconnect、deadline 与 shutdown 共同观察的取消 view。若 planning 或 coordinator 在之后重新发现 topology，会让同一 query 在不同阶段看到不同 backend 集合。

## 考虑过的选项

1. 在 optimizer、scheduler 和 DML owner 各自读取 TLS 或全局配置：调用方便，但 worker 复用、并发请求与 topology 变化会串扰。
2. 在 coordinator submit 前重新读取 live backend：能使用最新成员，但会让 planning fanout、schedule 和 fragment ownership 不再对应同一事实。
3. 在 statement admission 创建不可变 request context，并把窄投影传入每个消费者：参数会增加，但生命周期和失败边界可验证。

## 裁决

选择第三项。`RequestContext` 由 core admission adapter 唯一构造，包含 session 投影与 `QueryExecutionContext`。后者固定携带 role、按 backend id 排序且拒绝重复的 `BackendTopologySnapshot`、可选 monotonic deadline 与 QLC cancellation view。

topology owner 必须对 membership 或 generation 的每次变化推进 revision；coordinator 在 register 与首次 submit 前验证 captured snapshot。任何 revision 变化（包括 join）都使已捕获 query fail closed；网络失败只走既有 registry/QLC cleanup，绝不刷新、重规划或选取新 backend。空 snapshot 对本地语句合法，但需要 distributed execution 时明确失败。

## 接受的妥协（诚实记录）

每个 SQL/DML/MV 调用链都要显式携带 context 或它的窄投影，短期会增加函数参数和测试 fixture。我们拒绝为旧调用方保留 TLS、默认 backend count 或未取消 fallback；这会让迁移初期更严格，但避免两条语义路径长期并存。

## 何时重新评估

- 若跨进程 domain request 需要携带 snapshot，则单独裁决 wire 版本与 serialization；本 ADR 的 `Instant` 仅限同进程。
- 若多 frontend owner/lease 被引入，则扩展 revision authority 和 fencing，不允许 consumer 自行合并 topology。
- 若 QLC 增加新的 lifecycle state，则保持同一 cancellation identity，不把状态机嵌入 request context。
