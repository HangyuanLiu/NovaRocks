---
name: dev-workflow-write-spec
description: "Write an explicitly accepted design into the resolved project documentation root, with current-code evidence, scope, contracts, acceptance criteria, roadmap metadata, and links. Use after design discussion is accepted, when the user asks to create or update a spec, umbrella, roadmap design, or PR-sized design artifact."
---

# 写 Spec

把已接受的设计固化为项目文档。不要在这个阶段继续发明关键设计，也不要写实现 plan 或代码。

## 前置检查

1. 完整读取 `../dev-workflow/references/workflow-contract.md`。
2. 读取 `AGENTS.md`；按 contract 从当前请求 / memory 解析 `DOC_ROOT`，无可用记录时回退到仓库
   `docs/workflow/`。
3. 读取 `DOC_ROOT` 和目标子目录下适用的 `AGENTS.md`。
4. 确认存在明确接受的设计摘要。
5. 对摘要中的当前行为、缺口和代码锚点重新核实。
6. 搜索活跃文档和归档文档，避免重复立项或覆盖已有决策。
7. 若仍有会改变目标、外部契约、所有权或失败语义的未决项，停止写作并回到
   `$dev-workflow-discuss-design`。

## 选择文档类型

- 一个可独立实现、一个 PR 粒度的执行单元：写入 contract 或适用 `AGENTS.md` 规定的 spec 目录。
- 会派生多个独立 spec 的长期工作线：写入 contract 或适用 `AGENTS.md` 规定的 umbrella 目录。
- 文件名遵循 `YYYY-MM-DD-<kebab-slug>-design.md`。

使用 `assets/spec-template.md` 或 `assets/umbrella-template.md` 作为起点，并按 contract 调整 frontmatter；项目启用
roadmap / umbrella 时，再维护 Roadmap、子任务面板和阶段依赖图。
将模板落盘时，把标题、字段显示名和占位文字转换为当前请求与适用 `AGENTS.md` 要求的文档语言。

必要时使用 sub-agent 独立核查代码证据、文档重复项或 wikilink 完整性；最终文档由主 agent 汇总并校验。

## 内容要求

使用当前请求和适用 `AGENTS.md` 规定的文档语言写清：

1. 问题与代码证据；
2. 目标和非目标；
3. 当前行为；
4. 设计裁决与关键语义；
5. 组件 / 所有权边界；
6. 错误、取消、并发、恢复和兼容语义中与本任务相关的部分；
7. 适用的生产部署形态和跨组件验收边界；
8. 可观察、可验证的验收标准；
9. 风险、取舍与不在本 PR 处理的后续工作；
10. 相关 umbrella、spec、archive 文档和 ADR。

不要把尚未决定的接口名、文件拆分或实现步骤写成既定设计。明确区分“设计契约”和“计划阶段可决定的实现细节”。

## 完成检查

- 文档位于解析出的 `DOC_ROOT`；
- frontmatter 可解析；项目启用 Roadmap 时，对应字段完整；
- spec 粒度可由一个 PR 独立验收，或 umbrella 明确拆出子任务；
- 所有关键现状结论都能追溯到当前代码；
- 验收标准描述行为，不用旧符号缺失、精确文件数或迁移编号作永久 guard；
- 项目启用 umbrella 时，更新必要的面板、反链和阶段图；
- 没有同时开始 plan 或实现。

完成后向用户报告文档路径、核心裁决和下一阶段是 Codex Plan mode。
