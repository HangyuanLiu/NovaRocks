---
id: ADR-0117
title: "Local view registry is process runtime state, not durable frontend truth"
domain: [frontend-state-families]
status: active
supersedes: []
superseded-by: null
date: 2026-08-26
provenance:
  - "PR: pending; mechanism: closed frontend state family manifest and catalog desired-state source modes"
  - "discussion: 2026-08-25, which frontend-local state may remain durable"
code-anchors:
  - "novarocks/frontend/src/view/mod.rs (FrontendViewService)"
---

## 问题

非外部 catalog（例如 `default_catalog`）里的 `CREATE VIEW`，其定义应该由 frontend 持久化吗？

## 背景与执行事实

- 原实现由一个 StateStore 上的 view repository 持有 `novarocks/frontend/views/v2/` 前缀，FE 打开时全量加载。
  它是**最后一个**「真源只在 frontend durable store 里」的用户可见 DDL。
- 它既不是外部期望态的投影（没有配置或 controller 提供它），也不是可重建的缓存（没有任何外部权威可以重建它）。
  按闭合三分类（ADR-0114），它属于那个不被允许存在的第四类。
- 外部 catalog 里的 view 走的是完全不同的路径：目标先被解析为外部 catalog，然后交给 connector 的 view
  capability 持久化到 catalog 自身。这条路径已经可用。
- ADR-0090 规定 MV/View 定义持久化用户有效 SQL 原文与解析上下文。该裁决不受影响：它约束的是**存什么**，
  而本 ADR 决定的是**谁存**。外部 catalog view 仍然按 ADR-0090 存原文。

## 考虑过的选项

1. **保留 durable 本地 view registry**。用户体验最好，但要么承认 frontend durable store 是真源（与湖单一真源
   冲突），要么给它编一个假的「重建来源」。
2. **把本地 view 也写进湖**。需要为「没有 catalog 的 view」发明一个存放位置与命名空间，等于在湖上另建一套
   frontend 私有元数据。
3. **对本地 `CREATE VIEW` 直接 typed reject**，只允许外部 catalog view。语义最干净，但会打断本地开发与测试的
   常用写法，且对一个进程内就能满足的需求过度严格。
4. **降级为进程内 registry，能力上限如实说明**（采纳）。

## 裁决

1. 本地 view registry 归 `ProcessRuntime`：只存在于当前 FE 进程，无持久 key、无启动加载、无恢复入口。
2. **不**对本地 `CREATE VIEW` 增加 typed reject。它在进程生命周期内行为不变；改变的只是它不再跨进程存活。
   这是能力上限，不是禁令。
3. 需要 durable view 的部署使用支持 view 的外部 catalog——这条路径已实现，且本次未做任何改动。
4. 「view 不跨 FE 存活」的断言方向被**反转而不是删除**：原来断言「重开后 view 仍在」的测试改为断言「新实例上
   不存在」。那条反转后的断言就是这个分类的证据。

## 接受的妥协（诚实记录）

- **这是本次改动里唯一用户可直接感知的行为变化**：`default_catalog` 里创建的 view 在 FE 重启后消失，且没有任何
  警告。影响面主要是本地开发与测试写法。
- 我们选择「静默降级」而不是「typed reject」，因此用户可能在不知情的情况下依赖一个不会持久的 view。反过来，
  typed reject 会立刻打断一批本来能用的用法。两害相权取了前者，但这确实是把发现问题的时机推迟到了重启之后。
- 随之删除的还有 durable record 的 60 KiB 预算校验与持久化失败后的缓存回滚路径：进程内 registry 没有这两种
  失败模式。这意味着一个超大 view 定义现在只受内存限制约束。
- `FrontendApplicationErrorKind::ViewServiceOpen` 变体在生产路径上不再可达，暫时保留（还有一个无关测试在用它
  当任意 host-open 错误）。这是一处已知的残留，不是设计。
- 我们**没有**为本地 view 提供任何导出/导入手段。用户要保留定义只能自己保存 SQL。

## 何时重新评估

- 出现「本地 view 必须跨重启存活」的真实用户需求时：那需要一个真正的存放位置，而不是把 frontend store 重新
  当成真源。
- 外部 catalog 的 view capability 覆盖面足够广、可以成为唯一入口时：届时对本地 `CREATE VIEW` 做 typed reject
  反而更诚实。
- 若将来 frontend 获得一个合法的、可重建的本地元数据载体：本地 view 有可能重新分类为 `Accelerator`。
