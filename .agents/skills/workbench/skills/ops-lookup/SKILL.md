---
name: ops-lookup
description: "Search the project's own first-hand engineering knowledge base — scenarios we pinned, problems we personally debugged, decisions we ruled on — to answer whether we have hit this before and what we did about it. Use when someone reports a symptom, when the user asks whether a problem is familiar or has been seen before, when a change might break a previously captured scenario, or when a design question may already have a recorded ruling. This searches OUR OWN records only; it is complementary to, not a replacement for, any skill that searches external sources such as GitHub, Jira, Confluence, or chat history."
---

# 检索工程知识

回答「我们是不是遇到过这个」。只查**我们自己的第一手记录**，不查外部来源。

## 加载契约

行动前：

1. 完整读取 `../workbench/references/ops-contract.md`；重点是 §8 检索流程与 §9 诚实性规则。
2. 按 contract §1.1 解析 `DOC_ROOT` → `OPS_ROOT`；涉及 ADR 时另按 §1.2 探测 `ADR_HOME`。
3. `OPS_ROOT` 或 `INDEX.md` 不存在时，说明知识库尚未建立，直接进入移交（见下），不要报错收场。

## 流程

1. **读 `OPS_ROOT/INDEX.md`**，按症状与组件粗筛候选。用户给的往往是口语描述，
   先与索引的「症状关键词」列匹配，再退回组件标签匹配。
2. **深读候选条目全文**。不要只看 frontmatter 就下判断——`symptoms` 是入口，
   判定依据在正文的机制与证据里。
3. **三档判定**，逐条给出结论：
   - **确定命中**：症状、机制、版本三者都对得上，且条目 `status: verified`；
   - **疑似**：部分吻合，或条目 `status: unverified`，或版本 / 环境有实质差异；
   - **不匹配**。

   **绝不把「疑似」表述为「命中」**，也不要为了给出答案而合并档位。
   说「疑似」时必须讲清**哪一部分对不上**。
4. **命中时**给出：根因、修复、修复版本，以及条目里的**复发检测**步骤——
   让对方能在自己环境里自行确认，而不是只听你的结论。
5. **全部不匹配时**，明确声明「内部知识库无记录」，然后移交：
   项目若有检索外部来源的 skill（GitHub / Jira / Confluence / 聊天记录）则移交它；
   没有则说明可在哪些外部源继续查。
   **不要自己去搜外部源**——那是另一个 skill 的职责，混做会让两边的结论无法区分来源。

## 边界

- **只读**。不写任何文件，不改条目。
- 发现条目信息过时或不完整时，提示用户可用 `$ops-capture` 补录，自己不动手。
- 不替代外部来源检索；也不要因为内部命中就断言外部无更新——
  命中的条目可能已被上游后续修复取代，必要时提示对方再查外部。
- 检索不到不是失败。如实说「没有记录」比给一个牵强的匹配有用得多。
