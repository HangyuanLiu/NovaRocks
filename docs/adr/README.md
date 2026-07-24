# 架构决策记录（ADR）

本目录是 NovaRocks 的架构决策知识库：每条 ADR 固化一个**已裁决的设计问题**——背景、考虑过的选项、裁决、**接受的妥协**与**何时重新评估**。检索单位是「架构师会问的问题」，不是「当年做过的任务」。改动某子系统的架构级行为前，先在下方领域索引里找到它。

**怎么写**：用仓库内置的 `/adr` skill（`skills/adr/`，经 `.claude/skills/adr` 软链暴露给 Claude Code）新建、supersede 或重编号——skill 内嵌模板与全部规则；人工手写请复制下方模板。正文中文；id/slug/tags/frontmatter 键用英文。

**核心规则**：

- **不可变，永不删除**：ADR 合入后不改实质内容。立场变化时写新 ADR 标 `supersedes`，旧条标 `superseded-by` 并移入领域节末尾「历史」小节。被完全覆盖的 ADR 也**不删除**——它记录的是「曾经那样做过、为什么、后来为什么改了」，防止未来（人或 AI）循环重提已否决方案，并保住 provenance 链。允许物理删除的仅有例外：误创建/重复、从未定稿的草稿、泄密与合规问题。允许的原地修改仅限：状态字段、锚点链接的机械修正、错别字、编号冲突重编号。
- **自包含**：结论、选项、妥协必须在文件内完整可读，不依赖外部文档才能理解；设计工坊（vault spec）、PR、讨论日期仅作 provenance 字段。
- **编号**：`ADR-NNNN` = 现存最大编号 + 1，四位零填充。并行 PR 撞号时，**后合入者在 rebase 后重新编号**，并同步修改四处：①文件名、②frontmatter `id`、③本 README 索引行、④代码中全部 `Design: ADR-NNNN` 锚点（含其它 ADR 的交叉引用）。
- **代码锚点**：承重代码处放一行英文注释 `// Design: ADR-NNNN (docs/adr/ADR-NNNN-<slug>.md)`，只放「改这里之前必须读」的位置，不铺开。
- **谱系**：业界标准 ADR（Architecture Decision Record，Nygard 2011 谱系）；本库模板为 MADR 风格的六节增强变体——「接受的妥协」与「何时重新评估」为必填节（标准模板中它们名义上属于 Consequences，实践中最常被敷衍，而它们恰是本库存在的意义）。

## 模板

```markdown
---
id: ADR-NNNN
title: "English one-line title"
domain: [domain-tag]
status: active            # active | superseded
supersedes: []
superseded-by: null
date: YYYY-MM-DD
provenance:
  - "PR: <link>"
  - "vault: <spec name>"
  - "discussion: <date + topic>"
code-anchors:
  - "<path> (<symbol>)"
---

## 问题
（未来架构师会问的那个问题，一句话。检索入口。）

## 背景与执行事实
（成立此决策的客观事实，带符号名锚点；不写会腐烂的行号。）

## 考虑过的选项
（每个选项一段：机制、优势、代价。对照过外部系统的写明对照结论。）

## 裁决
（选了什么。）

## 接受的妥协（诚实记录）
（为此放弃了什么、真实理由是什么——「因改动成本而非因更优」这类必须如实写。）

## 何时重新评估
（触发条件清单：负载形态、依赖成熟度、指标阈值。）
```

## 领域索引

### runtime-filter

领域哲学：runtime filter 是**纯性能优化**——任何 activation、等待、降级策略都不得改变 SQL 结果语义（RF 是保守预过滤，join 本体兜底）。数据面走 query-global `RuntimeFilterGraph + DeploymentCompiler + Service`；静态层**宁严勿宽**（证明不了安全就在 fragment submission 前 fail-fast），运行时 timeout + PassThrough 只是生产可用性兜底、不是语义权威。planner 与 deployment 双侧独立验证，二者之间不传裸布尔结论。

- ADR-0001 — runtime filter 等待环为何静态 strict-fail，而不是靠运行时 timeout 兜底（active）
- ADR-0002 — multicast 反压为何保持消费者耦合（active）
- ADR-0003 — RF consumer 为何默认 BlockingSnapshot、NonBlockingLive 只做定点降级（active）
