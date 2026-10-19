---
id: ADR-0184
title: "Compiled packages are the sole static plan path"
domain: [sql-compiler, pipeline-execution, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0124
    scope: "Compatibility material additionally includes the sole compiled-package interpreter and its exact sealed pure catalogue digest; exact-island admission, ingress ownership and no negotiation remain active"
    link: "ADR-0124-native-compatibility-islands-and-ingress-admission.md"
date: 2026-10-19
provenance:
  - "discussion: 2026-10-19 sole compiled-package composition, phased capability restoration and complete final verification"
code-anchors:
  - "novarocks-server/library/src/static_plan.rs (ServerStaticPlan)"
  - "novarocks-server/library/src/compiled_composition.rs (seal_pure_catalog)"
  - "novarocks-server/library/src/command.rs (build_information_value)"
  - "novarocks/frontend-application/src/query_execution/physical_encoding.rs (encode_completed_packages)"
  - "novarocks/plan-codec/src/physical_package_v2/frozen_limits.rs (PackageEncodeLimits::FROZEN, PackageDecodeLimits::FROZEN)"
  - "novarocks/native-adapter/src/backend_task_execution/compiled_package.rs (CompiledPackageInterpreter)"
  - "novarocks/local-compiler/src/lowering.rs (FragmentCompileError::is_unsupported)"
  - "novarocks/execution/src/exec/pipeline/builder/compiled.rs (build_compiled_pipeline_graph_typed)"
  - "novarocks/version/src/lib.rs (StaticPlanInterpreter, derive_native_compatibility_material)"
  - "idl/novarocks/service.proto (FrozenFragment, TaskFailureCategory)"
---

## 问题

静态物理计划如何只有一种生产解释方式，同时准确表达尚未实现的能力、保持 FE/BE 兼容边界，并让阶段性迁移与最终产品正确性分别可验证？

## 背景与执行事实

完成的物理计划是静态执行权威（ADR-0153），FE 验全图、BE 验局部；Task 接管、安装与真实退出分别拥有事实和责任（ADR-0161）。传输形式必须落实这两个边界，不能让另一份计划树或另一套解释器重新决定类型、函数绑定、布局和运行拓扑。

| Owner | 输入与产出 | 边界 |
|---|---|---|
| Server `ServerStaticPlan` | 一份 pure function/provider 组合，FE carrier 与 BE interpreter | 启动前封存；没有逐 statement、Task、CLI 或 feature 选择 |
| FE `encode_completed_packages` | 完成的 physical IR → compiled package | 精确结果声明、准入快照、类型和 frozen binding 保留原始身份 |
| package codec | 有界 package bytes 与冻结值事实 | `frozen_limits.rs` 拥有生产上限和常量策略；不是部署配额 |
| Native ingress / Worker | opaque package 与 Task identity、资源和 lifecycle | 接管前资源准入；Accepted 不证明 package 语义或本地安装成功 |
| BE `CompiledPackageInterpreter` / LocalCompiler | package、pure function/provider facts → `LocalProgram` | 纯编译不获取 live scan、writer 或 topology |
| Native execution host / compiled pipeline | program 加本 Task 的 scan/writer/exchange/sink/RF 能力 | 实例绑定与生命周期归各 runtime owner |

`FrozenFragment` 仅保留 package 字节。原计划树字段的编号和名称永久 reserved；旧计划树 IDL、codec、BE decoder、`ExecPlan` 构造桥与 legacy pipeline 构造入口均退出生产链。`execution_shared.proto` 仍使用 `novarocks.plan` namespace，保留 ExecutionTypeTable、RF 和类型共享消息；namespace 名称不代表保留计划树解释能力。共享 writer ledger、RF canonical facts、Arrow 类型和值描述保持各自 owner。

Server 的 pure 目录按整份函数定义封存：每个重载都必须具有完整的 installed pure owner 与 effects，否则整份定义进入确定排序的排除清单。`--build-info` 输出摘要、封存定义数、排除定义的 name/kind/reason。这是当前 metadata 与其实现 owner 的一致性证据；尚不是独立执行侧安装清单，不能冒称完整能力证明。

旧 `ExprArena`、名字注册表、函数外壳和 legacy aggregate adapter 尚有真实调用方：FE 常量折叠、FE COW cast（`query_execution/row_mutation.rs`）与 compiled 的 `exec/expr/compiled_program/guarded.rs` 共享 IN helper。它们及私有 differential oracle 暂留，后续独立迁移；保留表达式求值不授予重新构造生产 fragment 树的权限。

## 考虑过的选项

1. **长期双解释器并按 feature/CLI 选择。设计否决。** 它保留运行对照便利，却让同一发行拥有两种静态语义权威，并把缺口恢复变成两套结构的同步维护；与单一生产解释路径冲突。
2. **遇到 compiled 缺口回退旧树或旧 decoder。设计否决。** 它表面保留能力，却隐藏实际支持边界，使结果、绑定、资源与故障来源取决于运行期分派，破坏冻结契约和 exact compatibility。
3. **先恢复全部能力再翻转。成本否决。** 它可缩短用户可见缺口阶段，但要求继续维护两条构造链，并延后在单一路径上的修复。接受先翻转删除、保留真实失败与后续责任的顺序；这不降低最终正确性要求。
4. **唯一 compiled 路径，具名拒绝与完整失败账分别表达中间态。选择。** 移植和修复只面对同一生产链。代价是阶段内仍有能力拒绝、缺陷与未覆盖项，需要明确后续 owner 和最终清账，不能用登记改写原始结果。

## 裁决

1. **唯一构造规则。** 正常 dev 和 release 都组合同一 compiled 静态路径。FE 仅冻结 package，BE 仅解释 package；不保留 v1 wire 变体、decoder、构造树、candidate 参数或 fallback。SQL 的发射不能按解释器改变精确类型或准入事实。
2. **兼容材料规则。** `StaticPlanInterpreter` 仅含 `CompiledPackage { pure_function_catalog_digest }`，仍将准确 pure 摘要加入 Native compatibility material。不同 pure 目录的 FE/BE 不能进入同一兼容岛；digest 不能因枚举只剩一个 variant 而删除。保留 compiled component 的 domain、tag 与字节算法。ADR-0124 的 exact identity、topology/ingress 拒绝、role-local 所有权及无协商规则继续有效。
3. **冻结上限规则。** package 编解码结构上限和常量策略由 codec owner 统一冻结，FE 作者与 BE 接收者共用；不得为某个失败临时放宽、增加配置或伪造缺失类型。结构上限与运行期资源覆盖分别验证。
4. **准确拒绝规则。** 能力缺口保持 typed：BE 通过 `TaskFailureCategory::Unsupported` → `AttemptFailureClass::Unsupported` → `[UnsupportedCompiledShape]` 运输；FE 不支持的编译形态使用同一 engine code。SQL 绑定缺 pure 实现保留 `[sql.analyze.unavailable_implementation]`。Protocol、真实 provider 错误、非法输入和控制失败不得重标能力拒绝，不按诊断文本猜类型；拒绝不触发重新规划或 fallback。DML 传递身份时仍须履行 abort，并保留 KnownCommitted/CommitUnknown 和清理失败的原有优先级。
5. **登记不替代正确性规则。** 过渡登记只接受准确、具名的能力拒绝，逐 case 绑定稳定 code、原因、后续 owner 与到期日。错误结果、崩溃、非预期超时和未运行覆盖保持原始失败或未完成；已有失败不获得旧基线豁免。当前翻转或删除引入的回归由当前变更修复。golden、冻结契约和独立 oracle 提供预期，不能重录错误值或削弱断言换取通过。
6. **最终清账规则。** 翻转删除的阶段交付保留全部拒绝、缺陷、未运行及未到达后缀与责任，不能宣称全产品通过。整个能力恢复周期的终验必须清零修复账、撤除有关临时登记、清空 pure 排除清单，并在唯一 compiled 构建上完成全部必需 CI；原生 `1FE+3BE`、workspace/tests/doctests/静态检查、稳定 SQL、system、兼容专项、约定显式数据集及 release 接线分别保留证据。VALUES 等 phase probe 按实际结果记录缺口，未支持不能记已关闭。一次 smoke、局部通过或登记匹配均不替代该终验。
7. **共享件与 oracle 保留规则。** 按实际依赖删除旧链，保留中立值类型、writer ledger、RF/type shared codec、compiled 复用 helper 及旧 evaluator 的私有 oracle。后续迁移旧 evaluator 必须先承接其 FE/compiled 生产调用方和独立正确性保护，不以目录名决定删除范围。

## 接受的妥协（诚实记录）

先统一解释链会暴露尚未恢复的能力，阶段内不承诺全量 CI 绿色。我们接受这段可追踪的缺口期，以减少双链维护与归因成本；代价由完整原始结果、逐项 owner 和最终全量清账承担，不能转嫁为静默结果变化或永久白名单。

当前 pure 封存从 metadata 自带的实现 owner 取得记录，能证明双方一致，尚不能独立证明安装完整。排除清单和 compatibility digest 使这一支持边界可见，独立安装清单仍需单独交付。旧 evaluator 的实际调用方也仍在，删除树构造链不等于完成表达式与聚合实现的全部迁移。

exact-island 继续保守分隔不同目录和 descriptor，即使部分 SQL 可能互通也不协商或降级。这保留了 ADR-0124 接受的升级窗口成本。这里不提供性能结论，也不以功能验证替代性能测量。

## 何时重新评估

- 如果产品选择必须在翻转时立即保持完整旧能力覆盖，重新评估“恢复全部能力后翻转”的时间与维护成本；在明确变更裁决前仍不恢复双生产链。
- 独立执行侧安装清单就绪时，重新评估 metadata 自证的封存输入，继续要求目录摘要与真实执行集合一致。
- FE 折叠、COW cast 与 compiled helper 迁到 pure 核心并有独立等价验证后，退役旧 evaluator/registry/adapter；不能提前删除其值类型或 oracle。
- 要支持 role-specialized binary、外部插件或混合版本窗口时，先定义新的兼容 authority 与测试矩阵，按 ADR-0124 重评估 exact-island；不能把它实现为本路径上的隐式 decoder fallback。
- 新类型、表达式、provider 或资源模型改变 frozen 结构预算时，在 codec owner 重新推导上限并验证两端；不得以单例失败作为调大上限的依据。
