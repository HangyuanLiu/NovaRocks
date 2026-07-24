---
name: adr
description: Create, supersede, or renumber an Architecture Decision Record in docs/adr/. Use when the user wants to record a design decision or compromise, distill a design discussion into a durable record, mark an ADR as superseded, or fix an ADR id collision after a rebase. Also use at PR-finishing time when the change embodies a new design decision.
---

# ADR authoring

ADRs live in `docs/adr/`. `docs/adr/README.md` is the map: usage rules at the top, then a per-domain index. One decision per file, `ADR-NNNN-<english-kebab-slug>.md`.

Non-negotiable rules (mirror of README):

1. **Immutable, never deleted.** A merged ADR's substance is never edited. A change of position = a NEW ADR with `supersedes: [ADR-OLD]`, and the old one gets `status: superseded`, `superseded-by: ADR-NEW`, and its index line moves to the domain's「历史」subsection. Fully-covered old ADRs are still kept — they prevent re-litigating rejected options and preserve provenance. Physical deletion only for: accidental duplicates, never-accepted drafts, secrets/compliance. Allowed in-place edits: status fields, mechanical anchor fixes, typos, collision renumbering.
2. **Self-contained.** Verdict, options, and compromises must be fully readable inside the file. Vault specs / PRs / discussions are provenance strings only.
3. **Language.** Body in Chinese; id/slug/tags/frontmatter keys in English.
4. **The two signature sections** —「接受的妥协（诚实记录）」and「何时重新评估」— must never be empty or perfunctory. Record the REAL reason ("chosen for cost-of-change, not because it is better" must be written as such).

## Operation: new

1. Read `docs/adr/README.md` and list existing `docs/adr/ADR-*.md`; next id = max + 1, zero-padded to 4 digits.
2. Distill the source (discussion / decided spec / PR) into the template below. The `## 问题` section is the retrieval key: phrase it as the question a future architect would ask.
3. Add one index line under the matching domain section in README (`- ADR-NNNN — <一句话中文摘要>（active）`). If the domain is new, create the section with a ≤10-line domain philosophy paragraph first.
4. Suggest code anchors: a single English comment line `// Design: ADR-NNNN (docs/adr/ADR-NNNN-<slug>.md)` at load-bearing sites only ("must read before changing this"). Do not scatter them.
5. Run the self-check below.

Template:

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

## 背景与执行事实

## 考虑过的选项

## 裁决

## 接受的妥协（诚实记录）

## 何时重新评估
```

## Operation: supersede

1. Write the new ADR (operation "new") with `supersedes: [ADR-OLD]`.
2. Edit the OLD ADR's frontmatter only: `status: superseded`, `superseded-by: ADR-NEW`. Do not touch its body.
3. In README, move the old index line into the domain's「历史」subsection (create it at the end of the domain section if absent), rewriting it as `- ADR-OLD — <摘要>（superseded → ADR-NEW）`.

## Operation: renumber (id collision after rebase)

When a rebase reveals the id is already taken on main, the LATER-merging PR renumbers. Take the new max+1 and update **four surfaces in the same commit**:

1. filename (`git mv`);
2. frontmatter `id`;
3. the README index line;
4. every `Design: ADR-OLD` code anchor, plus any cross-references in other ADRs (`supersedes`/`superseded-by`/body text) — find them with `grep -rn "ADR-OLD" docs/ novarocks/ .claude/`.

If a duplicate id is only discovered after both PRs merged, the later-merged ADR does a follow-up renumber commit (this counts as an allowed mechanical edit).

## Self-check (run after every operation)

- frontmatter parses; `id/title/domain/status/date` present; id matches filename; id unique across `docs/adr/`;
- all six section headings present, the two signature sections non-empty;
- README index line exists, matches the ADR's status and domain; superseded ADRs sit in「历史」;
- `status: superseded` ⇔ `superseded-by` non-null; `supersedes`/`superseded-by` targets exist and agree both ways;
- every `code-anchors` path exists; every in-code `Design: ADR-\d{4}` reference resolves to an existing file.

There is deliberately no CI gate for any of this — consistency is maintained here (at authoring time) and by PR review.
