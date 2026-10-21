---
id: ADR-0188
title: "Optional PR validation and formal admission"
domain: [test-capabilities]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0189
    scope: "PR-only fast precheck cost decision; formal admission and evidence rules remain active"
date: 2026-10-20
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1237"
  - "discussion: 2026-10-20, visible non-blocking PR validation"
code-anchors:
  - ".github/workflows/admission.yml (admission, pr_validation, precheck)"
  - "tools/ci/verification/admission_entry.py (execution_context)"
---

## 问题

如何在 PR 合入区域展示完整验证的真实状态，同时保持非阻断反馈与正式准入的来源边界？

## 背景与执行事实

GitHub 工作流的检查由触发事件和检出提交关联到 PR；在某 SHA 上手动运行工作流，不足以让结果进入 PR Checks。检查是否阻塞合入由 required checks 配置决定，原生步骤退出与检查结论必须如实保留。ADR-0189 的指定正式来源、启用范围、当前尝试组合及分阶段上线规则继续有效。

| 检查 | 事件与检出 | 职责及证据类别 |
|---|---|---|
| `precheck` | `pull_request` 的测试合并提交 | 快速仓库预检，独立反馈 |
| `pr-validation` | `pull_request` 的测试合并提交 | 完整 cargo-only 链，`diagnostic`，非必需 |
| `admission` | `merge_group` 或手动运行 | 指定托管上下文的合并组/main 手动运行为正式来源；其他上下文仍为诊断 |

普通 `pull_request` 保持只读权限、无 secret；不使用 `pull_request_target`。准入入口自行判定正式性，不因步骤共用而升级来源。

## 考虑过的选项

1. **PR 只做快速预检。成本否决。** 省去完整构建，但合入前看不到启用范围的完整结果；当前接受额外执行成本以取得更早反馈。
2. **独立的非必需 PR 检查，共用执行步骤。采用。** GitHub 原生展示排队、运行、成功、失败和取消，正式性继续由事件判定；YAML anchors 保持工具、环境、摘要与上传一致。
3. **用 `continue-on-error` 或成功包装实现非阻断。设计否决。** 抹去真实失败状态；非阻断必须通过非 required 配置实现。
4. **跨运行 API 汇报或复用手动运行结果。成本否决。** 需要额外写权限、SHA/事件关联及过时结果处理，当前原生 PR 事件足以满足反馈要求。
5. **独立维护第二份执行脚本或步骤。成本否决。** 两条路径容易在工具版本、启用集合及失败上传上漂移，同一工作流内直接复用配置即可。

## 裁决

1. **反馈与准入独立具名。** 保留正式 `admission` 的名称和触发条件，新增只响应 `pull_request` 的 `pr-validation`，不加入 required checks；`precheck` 保持独立。
2. **执行共用，来源独立。** PR 与正式 job 通过 YAML anchors 共用环境和完整步骤，包括固定工具版本、入口、摘要及原始材料上传。PR 默认测试合并 SHA，材料恒为诊断。
3. **真实失败可见。** 不忽略退出码，不设置 job/step `continue-on-error`，不加成功包装。失败仍尝试发布摘要与上传；缺失材料如实显示。
4. **取消边界按 PR 与检查隔离。** PR 检查各自使用按 PR 编号的并发组，只取消对应检查的过时运行；不添加会取消正式任务的工作流级并发策略。

## 接受的妥协（诚实记录）

每次 PR 更新都可能增加一次完整 cargo-only 构建与测试，后续正式准入再次支付相同执行成本；本决策优先提供合入前的完整反馈。非必需检查失败时维护者仍可合入，不能据此宣称正式门禁已经启用或产品测试全绿。YAML anchors 把复用限制在同一工作流文件，但省去了第二套执行路径和额外汇报权限。

## 何时重新评估

- PR 量或完整链耗时使早期反馈收益不再覆盖重复运行成本时，重评仅快速预检的方案。
- 需要跨工作流、多执行平台或持久共享证据时，重评 API 汇报、复用与可复用工作流的权限、身份和保留责任。
- 两个事件需要不同执行集合或准备步骤时，重评步骤复用方式；先由原生入口定义差异，避免手工复制两套执行逻辑。
