---
name: dev-workflow-execute
description: "Execute an approved implementation plan continuously under a persistent Codex goal, dispatching parallel-ready tasks to sub-agents, integrating changes, and gathering verification evidence without stopping for routine difficulties. Use when the user asks to implement, execute, or continue an approved spec and plan until genuinely complete."
---

# 执行计划

以 approved spec 和 plan 为边界，先创建 goal，再持续实现和验证。

## 启动检查

1. 完整读取 `../dev-workflow/references/workflow-contract.md`。
2. 读取 `AGENTS.md`；按 contract 从当前请求 / memory 解析 `DOC_ROOT`，无可用记录时回退到仓库
   `docs/workflow/`。
3. 读取 `DOC_ROOT` 下适用的 `AGENTS.md`、目标 spec、approved plan 和相关 ADR。
4. 检查工作树、分支、HEAD 和现有改动，保护用户的无关修改。
5. 确保位于本任务的本地开发分支；不要在 detached HEAD、主分支或无关分支创建检查点 commit。
6. 确认 plan 的前置条件仍成立；对易漂移的代码锚点重新核实。
7. 若缺少 accepted spec 或 approved plan，回到对应阶段，不边实现边补设计。

## 创建与维护 Goal

执行请求使用本工作流时，goal 是必需的：

1. 先检查是否已有未完成 goal。
2. 没有 goal 时，按用户授权的终态创建一个具体 goal，包含：
   - 要实现的 spec / plan；
   - 必须满足的行为结果；
   - 必须完成的验证；
   - 本地实现与验证终态。execute goal 永远不包含 push 或 PR。
3. 已有同一任务 goal 时继续使用，不重建、不缩小终态。
4. 用执行计划跟踪当前步骤，但不要把“计划步骤完成”误当成 goal 完成。
5. 仅在目标实际达成且无必需工作剩余时标记 `complete`。
6. 仅在同一阻塞条件连续出现至少三个 goal turn、且安全替代路径耗尽后标记 `blocked`。

不要因 token、上下文压缩、测试时间长、第一次失败或计划步骤较多而结束 goal。

## 执行循环

对每个行为增量循环：

1. 核实当前代码状态。
2. 实现最小的完整语义切片。
3. 运行最贴近改动的测试或探针。
4. 修复失败并重跑。
5. 更新计划状态和必要的 plan 验证记录。
6. 继续下一个未完成项。

根据风险选择测试先行、同步测试或实现后补测试；不机械强制一种形式。任何行为变化都必须有能失败的验证，且生产拓扑
语义不能只靠测试便利形态证明。

按 approved plan 的 DAG 调度 sub-agent：

1. 默认优先调度：只要存在两个或以上依赖已满足、标为 `sub-agent-safe`、文件范围不冲突且能产生独立证据的 task，就使用可用
   sub-agent 并行处理；不得仅因协调成本而把它们全部留给主 agent。
2. 仅当串行明显更有优势时才不调度 sub-agent：改动极小且派发成本高于实现成本、任务共享高风险语义或同一文件、需要连续交互式
   调试、可用并发槽会使关键构建/测试明显变慢，或主 agent 已在该精确文件范围内进行不可安全切分的整合。作出例外时，在计划或
   commentary 中简短记录原因。
3. 给每个 sub-agent 传递 spec、单个 plan task、精确写入范围、禁止项和验证命令。
4. 主 agent 保留 goal、共享文件、集成、冲突解决和最终验证所有权。
5. 共享工作树中的 sub-agent 无论串行或并行都不得 commit；主 agent 在 wave 集成验证后创建检查点 commit。
6. 只有使用独立 worktree 或独立 clone、并配有任务专用 branch 的 sub-agent 才可以本地 commit；仍禁止 push 和
   PR。
7. `main-agent`、`serial`、共享文件和最终收敛任务由主 agent 按依赖顺序执行。

## 本地检查点 Commit

在任务本地开发分支上，以下时机允许创建 commit：

- 一个完整章节、行为切片或可独立验证的 plan 部分完成后；
- 进入高风险、跨模块或难以回滚的改动前，为当前稳定状态建立恢复点。

创建 commit 前：

1. 只暂存本任务文件，不混入用户的无关修改。
2. 优先保证该检查点语义自洽，并运行适合该切片的定向验证。
3. 使用当前请求和适用 `AGENTS.md` 规定的 commit message 语言，说明完成的行为边界。
4. 记录 commit 与 plan 章节的对应关系。

检查点 commit 是可选的安全机制，不代表 plan 或 goal 已完成。execute 阶段**明确禁止任何 push 和 PR 创建**；
无论是否存在总体发布意图，都必须交给 `$dev-workflow-finish` 并重新检查用户授权。

## 不应停下的情况

以下属于正常执行，由 agent 自主处理：

- 编译或测试失败；
- 发现局部调用点遗漏；
- 需要小范围重构以满足已接受契约；
- 工具命令需要调整；
- 实现比预计更久；
- 有多个等价、可逆、不会改变契约的代码写法。

## 重大决策点

只有下列情况可以暂停并询问用户：

- 需要改变 spec 的目标、非目标、外部协议、持久化格式、所有权边界或失败语义；
- 当前证据证明 accepted design 的核心前提错误；
- 存在多个会产生明显不同用户行为或长期架构成本的方案，plan 未裁决；
- 需要未授权的破坏性操作或生产操作；
- 缺少只有用户能提供的权限、凭据或业务决定。

暂停时提供证据、受影响的 spec 条款、2–3 个选择及推荐。获得决定后更新 spec / plan，再恢复同一 goal。

## 完成证据

在声称实现完成前：

- plan 必需项全部完成；
- 运行与风险相称的格式、单元、集成、端到端和生产形态验证；
- 检查 diff 与 accepted spec 一致；
- 记录未运行的测试及原因；
- 工作树中不存在本任务留下的临时文件或残留进程；
- goal 到“本地实现已验证”即完成；允许合规的本地检查点 commit，但不得 push 或开 PR。
