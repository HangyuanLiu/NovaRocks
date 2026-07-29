---
id: ADR-0012
title: "Frontend owns query session admission and routing"
domain: [distributed-query-lifecycle, frontend]
status: active
supersedes: []
superseded-by: null
date: 2026-07-29
provenance:
  - "PR: #763"
code-anchors:
  - "novarocks/frontend/src/query.rs (FrontendQueryService)"
  - "novarocks/core/src/query_execution/session.rs (QuerySessionFactory)"
---

## 问题

MySQL wire adapter、连接级 session admission 与 SQL application routing 应由哪个边界拥有？

## 背景与执行事实

frontend 已拥有 query-global coordinator、cancellation control、role 与 topology；而 core 的 MySQL shim 曾同时保存这些 application state，并在协议回调中构造 request context。这样会使 frontend 无法成为 query/session 的真实 owner，也会让 protocol adapter 知道 QLC session lifecycle。

SQL compiler、connector-specific table preparation、fragment artifact encoding 与 BE-local execution 仍依赖 core，不适合随 session 一起迁入 frontend。`RequestContext` 已规定 admission 只能捕获一次 topology、deadline 和 cancellation identity。

## 考虑过的选项

1. 保持 core MySQL shim 的 session/router：改动最小，但 query application owner 仍然错误，且 request admission 不能被 frontend 组合。
2. 将 MySQL wire DTO 和 protocol server 迁入 frontend：frontend 会拥有协议实现，扩大其职责且破坏 core 的 protocol boundary。
3. 以 core 定义、frontend 实现的 `QuerySessionFactory`/`QuerySession` 端口切开 wire 与 application session：wire 只做认证、回调、编码和 typed error mapping；frontend 拥有 session、admission、router 与 QLC 调用。

## 裁决

采用选项 3。每个认证成功的连接由 core wire 通过 `QuerySessionFactory` 打开 frontend session。frontend session 对每条 statement 先冻结 session inputs、开始 QLC statement、计算 deadline、捕获 topology，再构造 immutable `RequestContext`。取消、KILL、disconnect 与 shutdown 都向同一 QLC cancellation source 请求终止。

core 保留 SQL compiler/command kernel 与 connector truth；frontend 通过明确 API 调用它们，不把 `StandaloneState`、connector registry 或 execution plan 内部类型暴露到 frontend。query-global coordinator 继续物理位于 frontend。

## 接受的妥协（诚实记录）

在 SQL compiler 的 concrete connector、execution artifact 与 MV/DML 依赖尚未清理前，frontend 不抽取完整 SQL crate。它通过 core kernel 调用编译与未迁移 command，这保留了一个临时的物理依赖，但该依赖是窄 API 而非 core-owned session/router 或 dual execution path。

## 何时重新评估

- SQL parser/analyzer/planner 对 connector、exec、MV/coordinator 的 concrete 依赖已替换为稳定 artifact/port 时；
- QLC 后续 protocol 阶段稳定且 coordinator 出现可独立复用的真实 crate dependency 时；
- frontend 需要支持除 MySQL 外的第二个协议入口，且 session factory contract 已被两者复用时。
