# NovaRocks Docs Agent Guide

本文件适用于 `docs/` 目录及其子目录。后续 agent 在新增、移动或整理文档时必须遵守。

## 目录规则

1. `docs/design/` 一级目录只能包含两个子目录：
   - `docs/design/specs/`
   - `docs/design/plans/`

2. 不要在 `docs/design/` 根目录直接新增 Markdown 文件。

3. 不要创建或恢复以下目录：
   - `docs/superpowers/`
   - `docs/design/notes/`
   - `docs/design/spikes/`

4. 如果工具、skill、模板或旧计划要求写入 `docs/superpowers/...`，在本仓库中必须改写为：
   - spec/design 文档写入 `docs/design/specs/`
   - implementation plan、roadmap、status、summary、recon、handoff 文档写入 `docs/design/plans/`

## 文档分类

- 设计/spec 文档放在 `docs/design/specs/`。
  - 推荐命名：`YYYY-MM-DD-<topic>-design.md`
  - 例外：roadmap、diagnosis 等若本质是设计输入，也可以放在 `specs/`，但不要放在 `docs/design/` 根目录。

- 实施计划、执行记录、状态、PR summary、recon findings、handoff 放在 `docs/design/plans/`。
  - 推荐命名：`YYYY-MM-DD-<topic>.md`

- 面向用户的技术博客放在 `docs/blog/`，教程和部署指南放在 `docs/guides/`，不要混入 `docs/design/`。

## 移动和引用修正

整理文档时，如果发现旧路径，必须同步修正引用：

- `docs/superpowers/specs/` -> `docs/design/specs/`
- `docs/superpowers/plans/` -> `docs/design/plans/`
- `docs/design/notes/` -> `docs/design/plans/`
- `docs/design/spikes/` -> `docs/design/specs/`
- `docs/design/<file>.md` 根目录散落文件 -> 按文档性质移动到 `specs/` 或 `plans/`

如果目标目录已经有同名文件，不要直接覆盖。先比较内容：

- 内容相同：删除旧路径副本。
- 只有路径引用差异：保留 `docs/design/...` 中引用已修正的版本，删除旧路径副本。
- 内容不同：合并有意义的新增内容后再删除旧路径副本。

## 语言和风格

- 设计文档、计划文档和 agent 指导文档使用中文。
- 代码注释、日志、错误信息、commit message 使用英文。
- 文档内容应直接描述约束、方案和验证方式，避免写成聊天记录。

## 完成前检查

涉及 `docs/` 结构的变更，在结束前至少运行：

```bash
git diff --check
test "$(find docs/design -mindepth 1 -maxdepth 1 -print | sort | tr '\n' ' ')" = "docs/design/plans docs/design/specs "
test ! -d docs/superpowers
rg -n "docs/superpowers|docs/design/(notes/|spikes/|[^/]+\\.md)" docs src tests -g '!docs/AGENTS.md'
```

最后一条扫描必须无命中。`docs/design/notes/...`、`docs/design/spikes/...`、
`docs/superpowers/...`、`docs/design/<file>.md` 根目录引用都必须修正。
