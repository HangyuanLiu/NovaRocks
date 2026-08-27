---
id: ADR-0112
title: "Native role launch with ephemeral backend membership"
domain: [runtime-role, configuration, cluster-membership, crate-boundary]
status: active
supersedes: [ADR-0108]
superseded-by: null
date: 2026-08-26
provenance:
  - "implementation: native FE/BE dual-config launch with backend self-registration"
code-anchors:
  - "novarocks-server/src/launch.rs (resolve_server_launch)"
  - "novarocks-server/src/composition.rs (compose_backend_server_config)"
  - "novarocks/frontend/src/topology.rs (ClusterBackendService)"
  - "novarocks/backend/src/application.rs (BackendAnnounceTask)"
---

## 问题

native NovaRocks 如何保持 FE/BE 双配置和 role-local listener 的启动边界，同时不让 all-in-one、StateStore 或启动配置重新成为 Backend membership authority？

## 背景与执行事实

ADR-0108 正确地确立了只有 `role=fe` 与 `role=be`，以及 all-in-one 只是读取两份正常 role 配置的 supervisor。但是它仍把 StateStore 和被配置 endpoint 描述为 durable backend membership；该描述与 Backend self-registration 的现行裁决冲突。

外部 orchestrator 拥有 Backend 的 desired lifecycle。每个 BE 以自己配置的 logical FE Native endpoint 启动、生成新的 process identity，并通过既有、受 Native trust 保护的 listener announce。FE 的 registry 是可丢失的内存观察，必须再以 FE-pull exact heartbeat 验证，才形成 eligible topology；语义边界由 ADR-0111 定义。

## 考虑过的选项

1. 保留两份 role config，但允许 FE `backends` seed 或 StateStore 恢复 membership。它会令启动配置和 orchestrator 双重决定同一 Backend，拒绝。
2. 为 announce 新开 management HTTP 或未认证 h2c listener。它绕开 Native trust 与 role-local RPC contract，拒绝。
3. 保持 FE/BE 双配置、同一 Native listener、all-in-one 同路径，并将 membership 仅作为可重建 runtime observation。采纳。

## 裁决

`[cluster].role` 只允许 `fe` 或 `be`；`standalone --role all-in-one` 仍只监督一份 FE config 和一份 BE config。FE 的 Native gRPC、management HTTP、MySQL 与 BE Native gRPC、management HTTP 仍是独立 surface；announce 扩展 FE 已有 Native listener，复用同一 NWT-3 admission、JWT/TLS 与 build/deployment proof，不增加端口或 fallback。

FE config 不接受 `frontend_endpoint` 或 BE announce cadence；BE config 必须提供 `frontend_endpoint`，不接受 FE heartbeat/lease settings。`[cluster].backends`、SQL `ADD BACKEND` 与 `DROP BACKEND` 均被 hard cut。StateStore 继续保存其余 FE control-plane state，但绝不读写 Backend membership。

all-in-one 不注入 loopback Backend、临时 registry 或动态 endpoint。它按与独立部署相同的 listener/channel 路径启动两个 role；BE announce、FE heartbeat 和 Init 都必须经过正常的网络 adapter。

## 接受的妥协（诚实记录）

FE restart 后需要等待 BE renew announce 与 heartbeat，短时间内没有 eligible Backend。我们接受这项可见延迟，因为任何立即恢复旧 endpoint 的捷径都不能证明 endpoint 仍属于同一进程。

双配置增加本地开发步骤，h2c 默认模式也不提供 body confidentiality。我们接受前者以换取生产等价性；后者是 NWT-3 明确的可信网络选择，而不是 membership 可以降低认证要求的理由。

## 何时重新评估

- 需要多 FE takeover 或 replicated membership observation 时，先定义 fencing 和 query ownership，不能让 StateStore 回到 desired membership 角色。
- 需要 orchestration-specific pull discovery、Managed capacity 或多 warehouse 时，先建立新的 desired-lifecycle contract。
- 若 Native trust/listener 发生新的 supersession，重新验证 announce 仍无新增 port、未认证 route 或 direct-call path。
