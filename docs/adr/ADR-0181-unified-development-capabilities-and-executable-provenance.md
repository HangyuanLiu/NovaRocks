---
id: ADR-0181
title: "Unified development capabilities and thin executable provenance"
domain: [test-capabilities]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0175]
date: 2026-10-17
provenance:
  - "discussion: 2026-10-16 effective debug assertions, a joint development graph and thin executable source boundaries"
  - "partial replacement: ADR-0175-test-capabilities-and-finite-role-authorization.md compilation exclusion, separate ordinary/control artifacts and build-kind selection; finite authorization and actual exit rules remain effective"
code-anchors:
  - "build-support/development.rs (emit)"
  - "build-support/provenance.rs (emit)"
  - "tools/ci/dev-test.sh (prepare, test, check)"
  - "tools/ci/lib/build_capabilities.sh (ci_prepare_main, ci_collect_artifacts)"
  - "tests/cluster-harness/src/test_sessions.rs (ClusterTestSessionPlan)"
  - "novarocks-server/library/src/provider_manifest.rs (build_iceberg_control_factory, build_iceberg_execution_factory, build_paimon_execution_factory)"
---

## 问题

如何让日常开发、Rust 测试与真实 FE/BE 场景共用构建和产品装配，同时保持测试动作的准确授权，并限制来源身份变化造成的重编范围？

## 背景与执行事实

编译能力、运行授权、产品执行身份和源码诊断身份是不同事实。开发接缝可以存在于程序中，却不能自动取得动作权限；诊断身份变化也不能改变 Native compatibility identity。

| 实体 | 唯一职责 | 不承载的职责 |
|---|---|---|
| 实际 `debug_assertions` 与 `novarocks_dev_controls` | `development::emit` 从 Cargo cfg 导出开发控制别名，owner 校验二者一致 | profile 名和人为环境声明不授予能力或权限 |
| 根 workspace 的 dev 主图 | 联合准备已启用 tests 和实际 bins，使 dev-dependencies 从开始参与解析 | 不消除 Cargo 的单包、host 单元、doctest 或真实变体差异 |
| 薄 Server/SQL/System CLI package | 冻结来源、实际编译事实并注入无来源 application library | 不编入解析、领域模块、场景或替代产品装配 |
| 产品 builder 与 QueryControl owner | 普通启动和有限授权启动共用产品实例与生命周期 | 观察对象不创建第二份产品 owner |
| `ClusterTestSessionPlan` | 为每次实际角色启动冻结准确 binary、有限 grants 和原期限 | 无 grant 的角色不建立控制会话，重启不继承旧授权 |
| Cargo artifact receipt | 从实际输出绑定 executable 路径与内容 hash | 路径、源码 HEAD、自报都不是可复现发布或供应链认证 |

Server CLI 保留原 package 和 manifest；SQL CLI 保留原 manifest、package 与 default-run，fixture bin 属于其 library 子包；System 保留原 library manifest 的路径语义。既有运行入口应由明确的 CLI 身份解析，不能因剩余唯一 bin 变化而静默转交参数。

本 ADR 部分替代 ADR-0175 的编译传播、普通/控制分产物及相关取舍。准确角色授权、有限工作绑定、单向结束、原期限、领域接缝和实际退出规则继续有效。release 的标准配置关闭 debug assertions；任何显式开启 debug assertions 的构建均按实际 cfg 报告能力。

## 考虑过的选项

1. **只统一命令参数，保留两套普通/控制 feature。设计否决。** 能集中 CI 参数，但裸 Cargo 和 IDE 仍可选择另一套能力传播与依赖图，无法实现统一开发接缝。
2. **翻转默认 feature 并继续维护镜像传播。设计否决。** 能让默认开发看见接缝，却仍要求所有实际依赖边同步转发和冲突检查，不能消除同一开发用途的平行能力选择。
3. **实际 debug assertions 决定编译能力，运行权限继续独立。采用。** 默认开发共用一套能力和产品装配；标准 release 排除接缝。代价是带调试断言的产品程序本身含有控制代码，必须保留准确且有限的授权。
4. **共用产物并授予整个 suite 全部控制。设计否决。** 构建可以共用，权限仍只能属于本次准确角色与领域动作；减少产物不构成扩大授权的依据。
5. **在领域 library 的 build script 中冻结来源。设计否决。** 即使值只被 main 使用，package 级 build script 仍会使库、模块测试及其消费者一起失效，不满足来源边缘隔离。
6. **workspace-hack、nextest 筛选或共享缓存替代统一输入。待评估。** 它们可以解决特定合并、运行或存储成本，但不替代能力、来源和授权职责；不把尚未测量的工具收益当作当前依据。

## 裁决

1. **实际 cfg 规则。** 开发控制编入只取决于真实 debug assertions；具名别名必须与其一致。标准 dev 包含、标准 release 排除，构建名称和 `NOVAROCKS_BUILD_PROFILE` 不参与授权或编译选择。
2. **联合主图规则。** 日常正式准备选择 workspace 的 tests 与 bins，使用根 lock 和唯一 dev 配置。窄 `-p` 内循环可以采用，但承认其额外图组合；IDE check 使用同 worktree 内具名私有 target，成本独立记录。
3. **薄来源边缘规则。** 来源只在不含领域模块的 CLI package 冻结，library 消费纯输入。正常 Git 读取本 worktree HEAD，监听其 HEAD 和 HEAD reflog，不监听共享 refs/packed-refs；缺可用信号时要求明确 fallback，缺事实即失败，不伪造来源。
4. **身份分离规则。** 只变诊断身份时允许对应薄 CLI 和入口 smoke 重建，领域库、模块测试和工具保持 fresh。真实兼容材料变化允许必要重编。声明 profile 不进入编译输入，自报中的未知事实保持未知。
5. **单装配与逐启动授权规则。** 一份主程序可用于无控制与有限授权启动，仍由同一 provider builder、QueryControl 和维护提交 owner 创建产品。编入能力不等于建立会话；无 grants 不启动 RunnerSession，准确角色、工作绑定、原期限和重启新授权继续遵守 ADR-0175。
6. **最后产物绑定规则。** 所有相关 Cargo 结束后从原生 artifact 绑定主程序，SQL/System 使用前核对内容；消费者运行时不得改写程序。变体串行复用主缓存，只独立复制必要最终程序，随后恢复主图并重新绑定。
7. **兼容专项独立规则。** 完整 registry 保留兼容场景，默认功能集合排除它们。先解析选择及完整生命周期需求再准备变体；缺产物、未知选择和被要求却未运行都不能静默通过。专项按兼容合同变化或明确请求触发，独立入口不自动进入每次开发的默认矩阵。

## 接受的妥协（诚实记录）

开发程序包含控制代码，放弃了普通 debug 产品的物理排除边界，以换取同一开发构建和可见接缝；准确有限授权仍是不可省略的边界。仅关闭 debug assertions 才排除这些接缝，不能以 `release` 字样推断。

主 workspace 图比窄单包准备更大，首次优化、底层变更和许多测试目标的链接成本仍存在。依赖 opt 覆盖不能保证第一方泛型实例化已优化；profile 选择是有界观察下的工程取舍，不保证统计提速。IDE 私有检查缓存也接受真实重复成本。

薄入口仍有重建与链接成本；HEAD reflog 维护可能触发入口刷新，绕过该 worktree 日志的底层 ref 改写不在新鲜性保证内。正常诊断身份不是 Native 兼容算法，也不是发布认证。真实兼容变体可合法改变依赖图，恢复后的程序须重新绑定而非承诺字节复现。

## 何时重新评估

- 若项目要求所有开发产物物理排除控制代码，必须重新裁决编译能力与授权模型，不能仅恢复一组镜像 feature。
- 若原生 Cargo 观察证明固定主图仍存在显著合并成本，评估 workspace-hack 或新的统一方式；nextest 和共享缓存也只能在各自执行与保留职责明确后引入。
- 若工具获得独立发布或路径语义变化，重新评估 CLI/library package 边界，同时验证旧命令明确进入预期程序或明确失败。
- 若来源信号或供应链认证要求变化，重新评估通知、签名与实际构建事实采集；不以诊断 identity 代替可验证事实。
- 若兼容场景新增后续启动或外部 fixture，扩展其原生需求与维护责任，不重新并入默认功能矩阵。
