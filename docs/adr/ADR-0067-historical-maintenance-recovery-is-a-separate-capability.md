---
id: ADR-0067
title: "Recovering a dead generation's maintenance is a separate provider capability, not a relaxed exact-generation reconcile"
domain: [table-maintenance]
status: superseded
supersedes: []
superseded-by: ADR-0111
date: 2026-08-13
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/888"
  - "discussion: 2026-08-10 CP-4B table-maintenance historical recovery"
code-anchors:
  - "novarocks/spi/src/connector/historical_maintenance_recovery.rs (ConnectorHistoricalMaintenanceRecovery)"
  - "novarocks/core/src/engine/table_maintenance.rs (inspect_historical_maintenance)"
  - "novarocks/connector/iceberg/src/catalog_control/historical_maintenance_recovery.rs (IcebergHistoricalMaintenanceRecovery)"
---

## 问题

一个 durable maintenance operation 绑定在创建它的 `ConnectorExecutionBindingKey` 上，而
`ConnectorInstanceIncarnation` 是 process-local 的——进程一死，那个 exact generation 就永远回不来了。
接管这张表的新 frontend 该如何收敛这些历史工作？直接放宽 exact-generation 要求，让当前 generation
去执行旧 operation 的 reconcile，行不行？

## 背景与执行事实

- ADR-0065 让同一张表的维护由单个 per-table lease attempt 独占派发权。它解决了「谁能写」，但没有解决
  「新 owner 能不能读懂旧 owner 干了什么」。
- 在此之前，三条恢复路径的结局都是 `Unresolved`：distributed rewrite 无条件标记
  「requires its original exact connector generation」，metadata 与 cleanup 在 exact generation 不可得时
  同样落到这里。这个结论是诚实的，但操作会永久卡住。
- 已落地的破坏性护栏不能在接管时倒退：ADR-0035 规定每个 prepared cleanup batch 只 dispatch 一次，
  response 丢失后只能 `reconcile_batch`，reconcile 不得重新 list/plan/delete。
- 相邻先例 ADR-0037 让当前 generation 对历史 MV publication 做只读 lake inspection，证明了
  「current generation 解释历史证据」这条路可行，但它解释的是 MV publication artifact，
  不能被 maintenance 的 marker/artifact/receipt 复用。
- Iceberg 的 metadata maintenance marker 内嵌写入它的 incarnation：identity digest 由
  `(provider, instance, incarnation, operation id, kind, request digest, plan digest)` 构成。
  这意味着「用旧 identity 去查当前表」在技术上是可行的，而「用当前 generation 冒充旧 generation」
  在技术上会算出不同的 identity。

## 考虑过的选项

1. **放宽 ordinary reconcile 的 exact-generation 要求**：让当前 generation 直接接受旧 operation 的
   reconcile 请求。改动最小，但它把 generation fence 变成建议——同一个入口既服务「我还持有原
   generation」又服务「原 generation 已死」，调用方无法在类型上区分，一次误用就是重放已经发生过的
   破坏性操作。
2. **持久化 / 复活旧 generation**：把 incarnation、client、credential 存下来，重启后重建。这要求持久化
   进程内运行时和凭据，且两个 generation 会同时认为自己拥有同一张表的外部事实。
3. **Frontend 自己解析 provider 证据**：让 frontend 读 Iceberg metadata、manifest、snapshot summary
   来判断旧操作是否提交。这把 provider-private 的语义搬进 frontend，违背 connector 拥有 external truth
   的边界，且每加一个 provider 就要复制一遍。
4. **独立的 provider-owned historical recovery capability**（采用）。

## 裁决

新增一个**与 ordinary maintenance capability 并列但分开解析**的 provider capability。当前 generation
只解释证据，永远不继承旧 generation 的权限。

三条强制规则：

1. **旧 binding 只是输入**。它作为 descriptor 字段参与 identity 重建，绝不注册成 live binding，
   也不存在 exact-generation 的 historical resolver——exact 解析正是恢复已经失去的东西。
2. **已 dispatch 的动作只能被分类，不能被重放**。descriptor 携带 `dispatch_started`；
   continuation（唯一让未 dispatch 工作继续的机制）对任何已 dispatch 的 operation 一律拒发，
   并且必须绑定到发问的 operation 与 CP-4A attempt。cleanup 的历史恢复不 prepare、不 plan、不 execute，
   只对已记录的那个不可变 batch 做分类。
3. **不确定就是一个答案**。`Ambiguous` 与 provider 未实现（`Unsupported`）都保持 `Unresolved`，
   不回落到 ordinary reconcile。证据缺失不得被解释成「操作没发生」。

按 family 分开的 typed outcome 而不是统一 payload：四套状态机的并集没有任何消费者能验证。

## 接受的妥协（诚实记录）

1. **多一套 SPI 与一份 provider 实现成本**。ordinary 与 historical 两条 capability 并存，provider 要实现
   两遍、测试要覆盖两遍。换来的是「exact generation 还在」与「exact generation 已死」在类型上不可混淆。
2. **Iceberg 侧只能证明「已提交」，证明不了「未提交」**。marker 属性只存一份，承载它的 snapshot 可能已被
   expire，所以 marker 不在**不构成**未提交的证据，只能报 `Ambiguous`。这意味着一部分 operation 仍会
   停在 `Unresolved` 等人工介入。我们接受这个可用性代价，而不是把「没找到」当成「没发生」。
3. **首版只覆盖 metadata maintenance**。distributed rewrite 与 orphan cleanup 的证据在 provider artifact 里，
   当前实现返回 `Unsupported` 而不是猜一个分类。这是有意的：这个 capability 存在的理由就是不猜。
4. **只从 StateStore 的 candidate 恢复**，不从 lake 反向发现 StateStore 里不存在的 operation。
   一个 record 都没留下的 operation 不在本决策的救援范围内。
5. **provider artifact retention 成为隐性契约**。artifact 被 GC 之后证据消失，operation 永久 `Unresolved`。
   本决策没有引入 retention 保证机制，只是让这个后果显式化。

## 何时重新评估

- 如果 Iceberg（或其它 provider）提供了能证伪的证据——例如一份不随 snapshot expire 消失的 operation 日志
  ——妥协 2 应当消失，`Ambiguous` 的比例会显著下降，本 ADR 关于「只能证明已提交」的表述需要重写。
- 如果 `ConnectorInstanceIncarnation` 变成可持久化、可跨进程重建的身份，选项 2 需要重新评估，
  但重新评估的前提是先回答「两个 generation 如何不同时认为自己拥有同一张表」。
- 如果实际运行中 `Unresolved` 的堆积成为运维负担，正确的方向是补齐 rewrite/cleanup 的证据读取
  （妥协 3），而不是放宽规则 3 去猜。
- 如果 provider artifact retention 被证明无法覆盖恢复窗口，需要一个显式的 retention 契约，
  而不是让 frontend 在证据缺失时做推断。
