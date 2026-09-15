---
name: dev-workflow-plan
description: "Turn an accepted spec into a persisted, reviewable, and executable implementation plan in the resolved project documentation root without requiring Codex Plan mode. Use when a design spec is accepted and the user asks to plan implementation, review or revise implementation steps, or prepare parallel execution."
---

# 实现计划

在当前可编辑模式中完成调研并直接把实现计划固化到项目文档。不要要求切换 Codex Plan mode，也不要只在对话中输出
计划。

## 前置条件

1. 完整读取 `../workbench/references/workflow-contract.md`。
2. 读取 `AGENTS.md`；按 contract 从当前请求 / memory 解析 `DOC_ROOT`，无可用记录时回退到仓库
   `docs/workbench/`。
3. 读取 `DOC_ROOT` 下适用的 `AGENTS.md` 和目标 spec。
4. 确认 spec 已被接受，且粒度适合一个 PR。
5. 检查当前代码、测试入口、依赖关系和相关 ADR；不要只把 spec 改写成任务列表。
6. 摸清测试构成：有哪些测试目标 / 套件，各自覆盖什么区域，判断相关性的依据在哪里（适用 `AGENTS.md`、
   测试目录说明、测试与模块的实际依赖）。依据不足时见下方「测试面选择」一节。

## 调研与落盘

不要把模式切换作为 plan 阶段的前置条件。完成前置检查后，在同一阶段执行：

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
   - 验证方式：写明这次改动影响哪些具体测试目标 / 套件 / 用例，以及其余测试为什么不受影响；
   - 完成证据。
6. 写明每个 wave 的集成顺序、组合验证、冲突处理和回滚点；全量验证放在 wave / 里程碑收敛和最终验证，
   不放进每个任务。
7. 标出适合创建本地检查点 commit 的边界：完整章节 / 行为切片完成后，以及进入高风险改动前。
8. 明确 execute 阶段可以本地 commit，但禁止 push 和 PR。
9. 写明哪些局部选择可由执行者自行决定，哪些变化会使 plan 失效并必须回到设计讨论。
10. 使用 `assets/plan-template.md` 将计划以 `status: draft` 写入 contract 或适用 `AGENTS.md` 规定的 plan 目录。
11. 在 plan frontmatter 和正文链接目标 spec，并按项目约定在 spec 中增加反向链接；没有既有约定时添加
    `## 实现计划` 与 plan wikilink。
12. 项目使用 umbrella 时，将 plan 链接补入对应子任务面板。
13. 向用户报告 plan 路径、关键任务图和仍需确认的内容，请用户明确批准已落盘版本。

plan 阶段可使用 sub-agent 并行梳理独立子系统、测试面、文件所有权或风险点。主 agent 负责验证调研结果、
构造 DAG、消除伪并行并维护待用户批准的落盘版本。

plan 阶段只修改计划文档及其必要的 spec / umbrella 链接，不修改产品代码。计划必须足够完整，让执行阶段无需重新做
架构决策；但不要规定无关紧要的逐行实现。将模板落盘时，把标题、字段显示名和占位文字转换为当前请求与适用
`AGENTS.md` 要求的文档语言。

## 测试面选择

验证一栏写的是**这个任务的改动会影响哪些测试**，不是「跑测试」。plan 阶段的职责就是把改动面翻译成测试面：
逐个任务写出要运行的具体测试目标 / 套件 / 用例，并说明其余测试为什么不受影响。说不清第二点的，说明任务边界
还没切干净，先改边界。

全量验证只写在 contract 第 8.1 节允许的点上：wave / 里程碑收敛、最终验证，以及影响面本来就是全仓库的任务
（共享类型词汇表、wire 或持久化格式、跨模块公共契约、全局构建配置）。属于后者的任务必须在明细里写明理由；
没有写明理由的全量要求视为未完成的计划。

调研时若发现现有测试构成不足以判断相关性——测试目录没有覆盖面说明、模块与测试的对应关系无处可查——把补齐
这份依据作为计划中的一个任务落盘（给测试目录补映射说明，或把判据写进适用 `AGENTS.md`），不要用「保险起见跑
全量」代替判断。

## 批准与修订

用户明确批准当前落盘版本后：

1. 最后核对用户批准的内容与磁盘版本一致。
2. 将 plan frontmatter 更新为 `status: approved`。
3. 再次检查 spec / plan 双向链接和 umbrella 面板。
4. 只有完成以上步骤后才进入 `$dev-workflow-execute`。

用户要求修订已经 approved 的计划时，若改动影响任务 DAG、文件所有权、验收边界、关键依赖或风险裁决，先将状态退回
`draft`，完成落盘修订后重新取得明确批准。纯文字澄清且不改变执行契约时可保留 `approved`。

## 完成门

只有以下条件同时满足才进入 `$dev-workflow-execute`：

- 用户已明确批准当前落盘版本；
- plan 已落盘且为 `status: approved`；
- spec 和 plan 相互链接；
- task DAG、并行 waves、文件所有权、调度标签、验收命令、生产形态验证和完成标准明确；
- 每个任务的验证指向具体测试面，要求全量验证的任务写明理由；
- 没有未决重大决策。
