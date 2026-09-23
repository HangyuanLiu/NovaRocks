---
id: ADR-0153
title: "A completed PhysicalPlan is the static execution authority"
domain: [sql-compiler, distributed-query-lifecycle]
status: active
supersedes: [ADR-0050]
superseded-by: null
date: 2026-09-20
provenance:
  - "discussion: 2026-09-09 final physical-plan contract design"
  - "implementation: M1 completed-plan producer and activation convergence"
code-anchors:
  - "novarocks/physical-plan/src/plan.rs (PhysicalPlan)"
  - "novarocks/physical-plan/src/builder.rs (PlanBuilder::finish)"
  - "novarocks/sql/src/compiler/completion_driver.rs (final physical-plan lowering)"
  - "novarocks/query-application/src/preparation/description.rs (FrozenExecutionDescription)"
  - "novarocks/query-application/src/coordination/plan_activation.rs (ActiveLogicalPlan)"
  - "novarocks/frontend-application/src/query_execution/physical_encoding.rs (encode_completed_plan)"
---

## 问题

一条查询的静态执行语义由谁宣布完成，执行尝试又从何时起不得换用另一份计划？

## 背景与执行事实

SQL 先分析、优化并陈述精确的外部事实需求；provider 与 application 冻结这些事实后，SQL 才能把物理树降低为带版本、DOP 域、relation、节点、边、读取 occurrence、写入目标和结果端口的 `PhysicalPlan`。`PlanBuilder::finish` 与 `validate_plan` 拒绝不完整或矛盾的静态语义。计划本身不持有 provider handle、凭据、运行中 reader/writer、BE placement 或 cancellation。

| 实体 | 权限与寿命 |
|---|---|
| SQL completion | 消费一次分析结果与准确事实；产出一个完成的物理计划，不在 seal 后补写拓扑或 schema。 |
| `CompletedPhysicalPlanCandidate` | 持有已验证的 immutable 计划；可在第一次 Task 提交前被另一候选替换。 |
| `CompletedPlanWithAccess` | 将候选和精确冻结的 provider access 成对校验；access 仍由 application 生命周期 owner 释放。 |
| FE encoder / attempt template | 从同一对计划与 access 编码，并另行绑定本次尝试的 placement、端点和会话。 |
| `ActiveLogicalPlan` / `DispatchSeal` | 一条逻辑执行只发一次提交权；交给 dispatcher 时即消耗，未知提交结果也不得重新规划。 |

旧 SQL `DistributedPlan` 同时充当 seal 产物、跨 owner 只读门面和 native 编码来源，另有 node output、fragment edge output、write contract 三张 seal 后目录。它们使“完成”有第二个位置：静态事实可以在 SQL seal 后才由 FE 补齐。M1 的产品 producer 已改读完成的物理计划，旧图、三张目录与 `plan_read` 门面因此退役。native v1 字节仍是执行尝试的传输形式，不反向定义计划语义。

## 考虑过的选项

**A. 保留 SQL sealed 图，逐项映射到新契约。** 迁移成本低，但两份对象都可宣称计划完成；schema、边和 writer 条件会在映射处再判断一次。**设计否决**：同一静态语义不能有两个最终权威。

**B. SQL 只产出完成的 `PhysicalPlan`；application 持有运行事实并配对 access。** SQL 的构造与验证是唯一静态完成点，provider 权限与 FE 资源寿命仍在拥有者手中。接受编译迁移和现有 native v1 编码的成本。**采纳**。

**C. 将 provider session、placement 与编码字节放进 `PhysicalPlan`。** 表面上得到一个可直接发送的对象，却把查询静态语义与尝试、进程和秘密寿命绑在一起。**设计否决**。

## 裁决

采用 B，并固化四条审查规则。

1. **一次静态完成**：producer 在所有准确事实齐备后调用 `finish`；`validate_plan` 通过之前不得发布 candidate，之后不得补写静态 schema、读集合、写集合或拓扑。
2. **计划与 access 成对**：每个 provider read occurrence 必须与冻结 access 精确覆盖；编码只能消费这个配对，不得从 session input shape、latest catalog 或已编码 protobuf 反推缺失事实。
3. **提交关闭替换窗口**：逻辑执行可以在首次提交前换候选并交还旧 access；`DispatchSeal` 在向 dispatcher 交付意图时消耗。任何未知结果和后续 replacement attempt 都复用已提交的计划版本。
4. **运行事实留在 owner**：FE 保留 provider session、私有编码 sidecar、placement、资源和取消；BE 只接受经 native 边界投影的冻结 Task 内容。静态计划不得持有这些活动对象。

逻辑行变更的 `Delete`、`Replace`、`Insert` 与 provider 签署的 opaque route 继续成立；它们现在由完成的计划表示，不再依赖 ADR-0050 的 SQL sealed 图。M1 只完成纯计划定义和产品 owner 切换。跨进程完整 creation metadata 与 sink 的后续合同必须在各自里程碑验收，不能从本 ADR 推出它们已经可运行。

## 接受的妥协（诚实记录）

现有 native v1 编码会复制部分计划事实，FE 在 M1 仍需私有 attempt template 和 provider access 映射；这不是第二份语义权威，但会占内存并增加转换成本。长链 SQL 的语法、分析、逻辑规划及 optimizer bridge 需有界遍历；结构限额保护的是单计划形状，不能替代每个编译线程的栈和进程级内存预算。

## M1 结构门限与 FE 生命周期交接

`PlanLimits::FROZEN` 限制每 fragment 4,096 个节点、65,536 个值、262,144 个表达式节点和 256 层表达式语义深度；整计划限制 16,384 个 fragment、65,536 条边。资源校验另限制单 fragment 动态内容 4 Mi 项/64 MiB、整计划 16 Mi 项/256 MiB，provider 私有 payload 单项 16 MiB。现有 native v1 树编码另限制 64 层。这些是单对象的结构拒绝界，不是多查询的进程内存承诺；M2/M3 仍需按各自的载体和并发范围补界。

| 载体 | 创建与共享 owner | 取消、恢复与真实退出 | MEM 后续接入点 |
|---|---|---|---|
| `PhysicalPlan` / `CompletedPhysicalPlanCandidate` | SQL `finish` 产出唯一静态计划；Query Application 的 `FrozenExecutionDescription` 与 `ActiveLogicalPlan` 共享同一候选引用。派发前替换会把旧候选交还原 owner。 | `DispatchSeal` 在首次 Establish/Create 意图交给 dispatcher 前消耗；未知结果和替换 attempt 仍引用活跃版本。逻辑执行收敛且所有 description、active、attempt 引用退出后，最后引用才释放计划。 | completion、validate、候选引用及最后 backing release。 |
| `CompletedPlanWithAccess` / `PreparedDistributedAttemptTemplate` | FE 在读冻结后按 occurrence 成对校验；Native facts、fragment 编码和 access factory 由逻辑模板分别以共享引用持有，attempt 只实例化本次绑定。 | 取消阻止新的 attempt 与 split 工作；已打开的 source 仍由受监督的关闭路径释放。逻辑模板保留到恢复窗口结束，最后 attempt、模板和 provider access 引用退出后释放。 | provider freeze、编码、Native backing、打开的 source 与 access 的准确 owner 转移。 |

D1 的 `MetadataRelation` 与 `SealedArtifact` 在 M1 只具备纯定义和校验。它们尚无 M3 wire、Worker 静态校验及运行能力，因此不能据此宣称可执行。M1 没有建立 query/process 动态 charge、等待或回收账本。

## 何时重新评估

- 若出现必须在首次 Task 提交后改变静态语义的真实需求，先重新定义逻辑执行身份和可见性围栏，不能仅重铸 `DispatchSeal`。
- 若 native wire 的共享片段、完整 creation metadata 或 provider 合同改变静态与运行事实的界线，重新审查配对与生命周期；单纯换编码版本不触发此裁决回退。
- 若编译形状或并发使已冻结的结构限额、线程栈或进程内存预算无法覆盖真实语料，重新校准它们并保留对称的接受与拒绝证据。
