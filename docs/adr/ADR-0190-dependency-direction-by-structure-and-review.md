---
id: ADR-0190
title: "Dependency direction is kept by crate structure and review; CI keeps generic checks"
domain: [crate-boundary, test-capabilities]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0140
    scope: "Dependency guard form and enforcement; the StateStore contract/testkit crate split remains active"
  - id: ADR-0150
    scope: "Permanent graph/lockfile guard and structural benchmark gate as an acceptance gate (not an acceptance gate under the current-stage policy in AGENTS.md); exact release pinning and minimal features kept by review, format ownership and upgrade gates 1-4 remain active"
  - id: ADR-0168
    scope: "Guard identity and test-authority rules; versions stay owned by the root manifest and Cargo.lock"
  - id: ADR-0179
    scope: "Permanent dependency-graph boundary guard; publication lifecycle decisions remain active"
partially-superseded-by:
  - id: ADR-0194
    scope: "Only the statement that global Clippy remains warning-only and strictness is deferred; dependency governance, resource estimation, and all other conclusions remain active"
date: 2026-10-21
provenance:
  - "discussion: 2026-10-20 dependency governance through Rust generic mechanisms"
  - "discussion: 2026-10-21 resource grants as admission estimates without replacement prerequisite checks"
code-anchors:
  - "Cargo.toml ([workspace.dependencies])"
  - "deny.toml (advisories, bans, licenses, sources)"
  - "tools/ci/lib/build_capabilities.sh (unused dependency integration)"
  - "novarocks/native-trust/clippy.toml (disallowed-methods, disallowed-types)"
  - "novarocks/functions/build.rs (scalar Arrow request model)"
---

## 问题

crate 之间允许什么依赖由谁判断，CI 应保护哪些性质，依赖第三方私有实现的资源预留应承诺到什么程度？

## 背景与执行事实

Cargo 让 crate 无法命名未声明的依赖，但不会判断一条新声明的边是否符合产品角色和领域所有权。normal、dev、build、optional、target 与 feature 共同决定可达能力：根依赖看起来中立，不代表其传递闭包中立。契约与测试机械能力拆成不同 crate，仍需要 review 维持正确的依赖种类。

根 manifest 表达依赖要求，`Cargo.lock` 表达版本、来源与 registry checksum。cargo-deny 的版本、来源、license 与 advisory 政策处理供应链问题，不表达应用角色方向；其 sources 检查跳过本地 path 包，不能替代本地替换限制。cargo-machete 根据 manifest 与源码报告未使用依赖，不判断架构语义；宏展开、生成代码和只为启用 feature 的依赖需要所属 package 注明原因。

| 性质 | 维护位置与证据 |
|---|---|
| FE/BE、Connector、StateStore、Memory、测试叶子等依赖方向 | crate 结构、边界指南与 review |
| 通用供应链政策 | `deny.toml` 与 cargo-deny；版本事实只来自 Cargo |
| manifest 未使用依赖 | cargo-machete；分析不完整必须使接入阶段失败 |
| native-trust 不读取环境/文件、不绕过连接 owner | 所属 crate 的 Clippy `disallowed-methods` / `disallowed-types` 与 deny 属性 |
| DataSketches 持久格式与 consumer 行为 | 上游格式所有权、跨语言 TCK、fixture 校验及 consumer/分配上界测试 |
| JSON 函数与标量 Arrow 请求模型的执行前预留 | 审查时依赖配置下的准入估计；实际分配归属与结算属于内存子系统 |

JSON 的 `json_object`、`parse_json` 与标量 Arrow 请求模型依赖第三方布局与配置。预留、执行和现有结算路径照常运行，但不能仅从已审查的布局计算推导所有依赖配置下的实际分配硬上界。内存设计已把第三方步骤的额度定位为准入与结算额度。

## 考虑过的选项

1. **依赖方向由 crate 结构与 review 维持，CI 保留通用机制（方案 C，采用）。** 与 Rust 同类项目的治理方式一致，不维护另一份包名、源码形状与产品前提策略。代价是架构违规只能在 review 中发现。
2. **全量分层策略引擎（方案 A，成本否决）。** 每个 crate 声明分组，以分组 DAG 约束依赖，可让架构变化集中可见；但多了一套分类、例外和策略维护成本。当前选择简单机制；同类违规反复造成实际架构问题时重评。
3. **cargo-deny `wrappers` 表达局部边界（设计否决）。** 它只检查直接父节点，不能表达分组与通配，目标 crate 改名后规则可能静默通过。零散恢复方向检查违背统一的职责划分。
4. **保留少数原有守卫（设计否决）。** StateStore testkit 的 dev-only 规则等可在 manifest review 中判断。保留个别脚本会重新引入按领域各自维护依赖检查的例外。
5. **用 feature 黑名单、编译期布局断言与构建期来源检查承接资源前提（设计否决）。** 这些机制能够维持原来“预留一定覆盖实际分配”的承诺，但承诺建立在第三方私有实现上，只把耦合与证明维护成本搬到别处。采用准入估计的资源定位后，不再为这个承诺保留替代检查。
6. **`buf breaking`（待评估）。** 它保护跨版本 wire 兼容；当前产品没有这项承诺，会阻断合理的协议演进。承诺兼容时再评估。

## 裁决

**依赖方向规则。** 由 crate 结构和 review 维持架构边界，不新增依赖边界检查、源码形状守卫或产品前提检查。审查 manifest 时核对正常依赖、禁用的 optional/target 入口、相关 feature 与传递能力；参考 SPI、StateStore、Memory 边界指南。crate 拆分和类型所有权不因删除检查而改变。

**通用机制规则。** 供应链使用 cargo-deny；未使用依赖使用固定版本 cargo-machete 0.9.2 的 `--with-metadata`，只扫描 root-workspace 成员所在顶层目录。任何 manifest 分析失败都必须令阶段失败，不能仅信工具退出码。误报在所属 package metadata 记录原因，不关闭检查。API 级约束使用所属 crate 的 Clippy `disallowed-*`；native-trust 通过 crate 级 deny 生效。全局 Clippy 当前仍报告警告，严格模式另行收敛。

**来源前提规则。** 放宽的检查逐项交代：

| 前提 | 当前维护方式 |
|---|---|
| JSON/标量 Arrow 模型的依赖 feature 集合、64 位/小端/指定架构 | 不再检查，不新增 feature 黑名单、布局断言或来源检查 |
| JSON 模型中 serde_json、serde_core、hex、chrono 的版本唯一及 registry/git 来源 | cargo-deny 的既有版本唯一与来源政策；本地 path 替换不检查 |
| 模型已有的精确编译器与 Arrow 等锁定记录断言 | 现有 build.rs 保留；是否退出属于表达式与内存模型 owner 的后续裁决 |
| DataSketches 唯一版本、registry/git 来源 | cargo-deny 的既有政策 |
| DataSketches 禁止本地路径替换、根要求为 `=X.Y.Z` 正式版、逐 consumer 最小 feature、vendor 独立 lock 的 checksum 一致 | review 维持，不能声称 cargo-deny 等价覆盖；格式由 TCK 与 fixture 校验兜底 |
| physical-plan 外部依赖的唯一身份 | cargo-deny 的版本唯一政策；本地路径替换由 review 维持 |

**资源额度规则。** JSON 函数与标量 Arrow 请求模型的预留是审查时配置下的准入估计。feature、来源或目标平台变化可能使预留少于实际分配，实际分配由内存子系统归属与结算。需要操作前硬上界时，让分配路径本身受控，例如预算 buffer 和可失败扩容；不通过恢复第三方依赖前提检查来宣称硬保证。

**部分替代规则。** 四份旧 ADR 保持 active，正文保留为历史，双向 frontmatter 与索引限定变化范围：

- ADR-0140 的守卫形态与检查承诺退出，StateStore 契约/testkit 拆分与所有权保留。
- ADR-0150 的永久图/lockfile 守卫退出；精确正式版、上游格式所有权、最小 feature，以及第 1–4 道升级门继续有效：上游包身份、跨语言 TCK、fixture 字节等价、consumer 与分配上界测试。第 5 道结构性 benchmark 按 `AGENTS.md` 当前阶段仅维护功能与数据正确性的政策，不作为验收门；恢复性能工作时重评。
- ADR-0168 的 DataSketches 身份规则、物理计划身份规则与守卫测试权威规则退出；版本只由根 manifest 和 `Cargo.lock` 表达，CI 不保存版本字面值的规则保留。
- ADR-0179 的“永久依赖图边界：采用”退出，其余 Iceberg 发布生命周期裁决继续有效。

## 接受的妥协（诚实记录）

依赖方向只受 review 保护。一次 manifest 改动可以引入错误角色依赖或让 testkit 进入 normal closure，通用 lint 仍可能通过。这是为了降低专用治理成本而接受的窗口，不能把 cargo-deny 或编译成功称为架构合规证明。

DataSketches、arrow-ord、arrow-select 等的同版本本地替换不再被静态拒绝。review 负责来源规则，行为 oracle 负责其覆盖的格式和行为；两者都不能伪称为完整来源证明。

JSON 与标量 Arrow 请求模型在依赖配置变化时可能低估实际分配。生产查询尚未接入内存子系统分配时归属之前，这类偏差没有专门报警。估计的定位不改变结果正确性要求，也不以测试通过恢复“预留必然覆盖”的承诺。

现有模型 build.rs 的编译器与锁定记录断言暂时保留，因此本次并未消除所有第三方私有实现耦合。表达式与内存模型 owner 负责其长期形态。

## 何时重新评估

- 同类依赖违规反复发生并造成实际架构问题时，重新比较完整分层策略引擎的维护成本与收益；不零散恢复脚本。
- 产品明确承诺跨版本 wire 兼容时，评估 `buf breaking` 与兼容策略。
- 要求某段操作具有分配前硬上界时，重新设计可控分配路径和失败语义，不恢复第三方布局/feature 前提证明。
- 恢复性能工作时，重新评估 DataSketches 的第 5 道结构性 benchmark 及其验收定位。
- 内存分配时归属接入生产查询、或表达式/资源模型改为基于公开契约后，重审预留估计、结算和报警的责任边界。
