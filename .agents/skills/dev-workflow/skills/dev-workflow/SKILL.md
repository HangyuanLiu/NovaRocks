---
name: dev-workflow
description: "Route feature, refactor, roadmap, and architecture work through a durable development workflow: discuss the problem, write an accepted spec, plan in Codex Plan mode, execute under a persistent goal, verify, and optionally publish and archive. Use when the user asks to start, continue, or drive a development task end to end, or when the current phase is unclear."
---

# 开发工作流

把开发任务路由到正确阶段，并守住阶段门。不要在这个 skill 中重新实现各阶段的具体方法。

## 加载契约与解析文档根

在任何阶段动作前：

1. 完整读取 bundle 内的 `references/workflow-contract.md`；它是唯一流程源。
2. 读取仓库根目录 `AGENTS.md`。
3. 按 contract 的优先级解析 `DOC_ROOT`；memory 没有可用候选时，使用仓库根目录下的 `docs/workflow/`。
4. 读取 `DOC_ROOT` 及目标子目录中任何更具体的 `AGENTS.md`。
5. 检查当前代码、活跃文档和归档文档；不要仅凭对话记忆判断当前阶段。

不要把解析出的机器相关路径写回 skill。Memory 只负责定位；bundled contract、适用的 `AGENTS.md` 和当前代码
共同定义实时规则。

## 阶段路由

| 当前状态或用户意图 | 调用 |
|---|---|
| 问题、边界或方案尚未讨论清楚 | `$dev-workflow-discuss-design` |
| 设计已明确接受，但尚无 spec | `$dev-workflow-write-spec` |
| 已有 accepted spec，但尚无 approved plan | `$dev-workflow-plan` |
| 已有 accepted spec + approved plan，用户要求实现 | `$dev-workflow-execute` |
| 实现已验证，用户明确要求提交、开 PR 或归档 | `$dev-workflow-finish` |

如果用户显式指定某个阶段，直接进入该阶段；不要强迫完整重走前序流程，但必须检查该阶段所需输入是否存在。
“完成了吗”“当前状态如何”等状态询问属于只读状态报告，不进入 finish，也不构成 commit、push、PR 或归档授权。

## 状态机与阶段门

```text
discussion --设计被明确接受--> spec --spec 已确认--> Plan mode
Plan mode --plan 被明确批准并已落盘--> goal execution
goal execution --验收证据充分--> verified
verified --用户另行明确授权发布--> PR + archive
```

只设置两个常规人工门：

1. **设计接受门**：用户明确接受问题定义、目标、非目标和关键设计决策。
2. **计划批准门**：用户在 Codex Plan mode 中批准实现计划。

进入执行后，不因普通实现困难、测试失败、耗时超预期或局部代码选择而停下。仅在 `$dev-workflow-execute`
定义的重大决策点暂停。

## 连续执行

当用户明确要求端到端完成：

1. 在每个阶段完成后自动进入下一阶段。
2. 需要 Plan mode 时，说明原因并请用户切换；同时提示将计划组织为依赖清晰、尽可能可并行调度的 task graph，
   以便 execute 阶段使用 sub-agent。
3. Plan mode 获得批准后，先把批准版本落盘到 `DOC_ROOT`，再开始实现。
4. 实现开始时创建 goal；在 goal 的终态条件真正满足前持续工作。
5. 允许在任务本地开发分支创建检查点 commit；execute 阶段始终禁止 push 和 PR。
6. 只有用户另行明确授权发布后，才进入 finish 阶段执行 push / PR。

## Sub-agent 使用

全流程均可在确有必要时使用 sub-agent，例如并行代码调研、独立证据核验、互不重叠的实现切片或高风险改动审查。

- 主 agent 保持阶段所有权，亲自读取适用的 skill、项目契约和最终证据。
- 只委派边界清晰、可独立完成的子任务；不要为形式而拆分。
- 合并 sub-agent 结论前对照当前代码核实。
- sub-agent 不得绕过设计接受门、计划批准门、Goal 终态或发布权限。

## 范围纪律

- 讨论阶段保持只读。
- spec 阶段只写设计文档，不写 plan 或代码。
- plan 阶段只规划，不实现。
- execute 阶段实现与验证，可创建本地检查点 commit，但明确禁止 push 和 PR。
- finish 阶段只在用户明确授权后 push、开 PR 和归档。
- 设计变更回退到讨论阶段；不要在实现中静默修改 accepted spec。
