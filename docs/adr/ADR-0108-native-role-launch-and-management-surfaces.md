---
id: ADR-0108
title: "Native role launch and management surfaces"
domain: [runtime-role, configuration, cluster-membership, crate-boundary]
status: active
supersedes: [ADR-0013, ADR-0026, ADR-0072, ADR-0099]
superseded-by: null
date: 2026-08-26
provenance:
  - "implementation: native FE/BE dual-config launch and role-local management surfaces; PR number pending"
  - "discussion: 2026-08-25 native listener separation and all-in-one role equivalence"
code-anchors:
  - "novarocks-server/src/launch.rs (resolve_server_launch)"
  - "novarocks-server/src/supervisor.rs (supervise_all_in_one)"
  - "novarocks/frontend/src/server.rs (run_frontend_server_until_shutdown)"
  - "novarocks/backend/src/application.rs (run_backend_server_until_shutdown)"
---

## 问题

一个 native NovaRocks 进程应如何以严格的 FE/BE 配置、listener 和生命周期启动，同时让 all-in-one 保持为真实双角色组合而不是另一套运行时？

## 背景与执行事实

生产边界只有 `role=fe` 与 `role=be`。FE 拥有 MySQL、协调、StateStore durable backend membership，以及 native report gRPC；BE 拥有 native fragment gRPC 和本地执行。两者的 management HTTP（含 metrics）也是独立 listener 和独立 registry，native gRPC 不再兼任 metrics HTTP。

`[cluster].role` 是单一配置文件描述的 application role，不能表达进程内组合。此前单文件 `all-in-one` role、动态分配 report 端口、临时 loopback membership 与手工 host 启动会绕过正常 StateStore、listener 和 startup/shutdown 路径；它们既不能证明 native 进程等价，也使 metrics owner 混淆。

## 考虑过的选项

1. 保留单文件 `all-in-one` domain role，并在 Server 内注入 loopback backend、动态 report endpoint 与聚合 metrics：本地启动较短，但形成第二个 application lifecycle，绕过 durable membership，且生产和测试的 listener 事实不同。
2. 允许 `all-in-one` 读取一个文件两次、从 CLI 或默认值覆盖其 role/ports：文件数量少，但同一 wire 配置具有两种解释，配置 resolve、secret lookup、端口冲突和启动副作用无法定义唯一顺序。
3. 只接受 FE/BE 单角色配置；all-in-one 是 Server 的双配置 supervisor，启动两个不变的 role application，并使用各自 management listener：配置和跨进程部署等价，代价是测试/本地运维需要维护一对配置文件。（采纳）

## 裁决

`[cluster].role` 只允许 `fe` 或 `be`；Server 的精确命令形状为 `standalone --role fe|be --config <path>`，或 `standalone --role all-in-one --fe-config <path> --be-config <path>`。`all-in-one` 只是 `ServerLaunchMode`，不是 domain role、TOML role 或 topology mode。

Server 在 logging、Tokio runtime、StateStore、目录或 listener 的任何副作用之前，加载每个显式配置一次，完成 role 精确匹配、同进程 shared process settings 和全部 listener bind address 的冲突检查。FE 与 BE 的 native gRPC、management HTTP，以及 FE MySQL 都是独立 surface；native endpoint 必须拒绝 `/metrics`，metrics 只能从对应角色 management endpoint 取得。

all-in-one 使用同一套 `run_frontend_server_until_shutdown` 和 `run_backend_server_until_shutdown` 正常启动路径。FE 仍以 StateStore 恢复及持久化 membership，BE 仍经被配置的 native endpoint 被发现；禁止动态 loopback injection、临时 membership、共享 metrics registry、动态 report port 或直接调用 role host。supervisor 的第一个 role 失败或进程 shutdown 会通知 sibling 停止并等待其清理；role-local error 保持 primary，清理错误只作附加诊断。

## 接受的妥协（诚实记录）

all-in-one 不再是“拿一份默认配置即可运行”的最短开发命令：它需要一对端口不重叠、process 设置一致的配置，并且 FE 必须有可用 StateStore。这是为了让它成为 deployment-equivalent 验证，而不是因为双配置在手工调试时更方便。

两份配置中 logging 与 data runtime 设置必须一致，因而不能在同一进程对角色做独立 runtime 调优。选择此限制是因为一个进程只有一个 logging/runtime 生命周期；若假装可独立生效，就会产生无法观察的优先级或双 runtime。

role-local metrics 使单进程抓取者必须抓取两个 management endpoint，失去“一个 all-in-one 指标页”的便利。我们接受这一成本，因为合并 registry 会把跨进程时不存在的所有权关系重新带回产品模型。

## 何时重新评估

- 产品需要一个进程承载多个可独立配置、隔离故障域的 native role 时，应先定义新的 process composition 与 resource authority，不能扩展 `ClusterRole` 或复活动态注入。
- 多 FE fencing/takeover 改变 StateStore membership startup 协议时，重审 all-in-one 是否仍能使用完全相同的 FE durable path。
- 需要统一观测入口时，可以由独立 observability gateway 或采集系统聚合 role-local metrics；不得复用 native gRPC 或令 FE/BE 共享 registry。
- 配置 schema 需要版本化、远程发布或运行时变更时，重新定义 preflight 的冻结时点与失败语义，但保留单文件单 role 的解释权。
