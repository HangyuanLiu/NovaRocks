---
name: dev-workflow-plan
description: "Turn an accepted spec into an approved, executable implementation plan by using Codex Plan mode, then persist the approved plan in the resolved project documentation root before coding. Use when a design spec is accepted and the user asks to plan implementation, review implementation steps, or prepare parallel execution."
---

# 实现计划

使用 Codex 内建 Plan mode 完成调研、决策和计划审阅；不要复制一套独立的 planning 流程。

## 前置条件

1. 完整读取 `../dev-workflow/references/workflow-contract.md`。
2. 读取 `AGENTS.md`；按 contract 从当前请求 / memory 解析 `DOC_ROOT`，无可用记录时回退到仓库
   `docs/workflow/`。
3. 读取 `DOC_ROOT` 下适用的 `AGENTS.md` 和目标 spec。
4. 确认 spec 已被接受，且粒度适合一个 PR。
5. 检查当前代码、测试入口、依赖关系和相关 ADR；不要只把 spec 改写成任务列表。

若当前不在 Codex Plan mode：

- 不开始实现；
- 明确请用户切换到 Plan mode；
- 给出将要规划的 spec 路径和需要解决的计划问题；
- 明确提示 Plan mode：尽量把计划产出为依赖清晰、文件范围不冲突、可供多个 sub-agent 并行调度的 task
  graph；无法安全并行的部分必须明确标成串行。

## 在 Plan mode 中

遵循 Codex Plan mode 的交互与审批机制：

1. 研究 spec 点名的代码路径和实际调用链。
2. 识别需要修改、创建和删除的文件。
3. 建立 task DAG，标出硬依赖、关键路径、并行 waves 和最终收敛点。
4. 把任务切成行为完整、可独立验证、文件所有权尽量不重叠的增量；不要机械按文件拆任务，也不要为了并行制造
   错误边界。
5. 为每个任务写明：
   - 稳定 task ID；
   - `depends_on` 与调度 wave；
   - `sub-agent-safe` / `main-agent` / `serial` 标签；
   - 精确文件 / 模块所有权；
   - 目标与对应 spec 契约；
   - 输入、输出与交接契约；
   - 实现步骤和关键约束；
   - 测试 / 验证方式；
   - 完成证据。
6. 写明每个 wave 的集成顺序、组合验证、冲突处理和回滚点。
7. 标出适合创建本地检查点 commit 的边界：完整章节 / 行为切片完成后，以及进入高风险改动前。
8. 明确 execute 阶段可以本地 commit，但禁止 push 和 PR。
9. 写明哪些局部选择可由执行者自行决定，哪些变化会使 plan 失效并必须回到设计讨论。
10. 让用户通过 Plan mode 明确批准最终计划。

Plan mode 中可使用 sub-agent 并行梳理独立子系统、测试面、文件所有权或风险点。主 agent 负责验证调研结果、
构造 DAG、消除伪并行和批准最终计划。

## 批准后落盘

Plan mode 本身不做文件修改。用户批准并回到允许编辑的模式后，在执行任何代码改动前：

1. 使用 `assets/plan-template.md` 把批准版本写入 contract 或适用 `AGENTS.md` 规定的 plan 目录。
2. 在 plan frontmatter 和正文链接目标 spec。
3. 按项目文档约定在 spec 中增加指向 plan 的反向链接；没有既有约定时添加 `## 实现计划` 与 plan wikilink。
4. 项目使用 umbrella 时，将 plan 链接补入对应子任务面板。
5. 标记 `status: approved`；未批准草案不得用于执行。

计划必须足够完整，让执行阶段无需重新做架构决策；但不要规定无关紧要的逐行实现。
将模板落盘时，把标题、字段显示名和占位文字转换为当前请求与适用 `AGENTS.md` 要求的文档语言。

## 完成门

只有以下条件同时满足才进入 `$dev-workflow-execute`：

- 用户已批准 Plan mode 的最终计划；
- approved plan 已落盘；
- spec 和 plan 相互链接；
- task DAG、并行 waves、文件所有权、调度标签、验收命令、生产形态验证和完成标准明确；
- 没有未决重大决策。
