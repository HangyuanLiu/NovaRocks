---
id: ADR-0099
title: "Neutral tail owner cut"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-19
provenance:
  - "implementation: neutral-tail owner cut; PR number pending"
  - "discussion: 2026-08-18 neutral owner boundaries"
code-anchors:
  - "Cargo.toml (workspace members)"
  - "novarocks/failpoint/src/lib.rs (owner-neutral failpoint vocabulary)"
---

## 问题

当原聚合 Core 已不再拥有任何可执行行为时，怎样把剩余的值、故障注入、测试和指标归给真实 owner，且不以兼容 facade 重新引入隐式依赖？

## 背景与执行事实

Frontend 拥有 SQL admission、Connector control、查询协调、取消、结果表示、维护 application 与 FE 指标。Backend 拥有 Connector execution adapter、native decode、结果缓冲、backend identity、query lifecycle 本地状态和 BE 指标。Protocol、Types、Execution、FS 与 SQL 已各自拥有中立 wire/value/kernel/文件/编译契约。

先前的聚合 Core 同时暴露这些不相干的 owner 表面，令 crate 依赖图无法表达真实边界；测试也因目录便利而漂移到非 owner。所有生产 consumer 已在迁移后直接依赖其实际 owner，Core 不再承载实现。

## 考虑过的选项

1. 保留 Core 为稳定 facade，将新 owner 的符号重新导出。调用方改动最少，但会继续允许错误 owner 通过聚合依赖获得能力，Cargo 图无法约束隔离。
2. 将所有共享表面扩大为 SPI。可统一路径，但会把 FE/BE application 事实误包装成可替换 provider 契约，并扩大长期兼容面。
3. 按实际 owner 移动，并让 Core 成为无依赖的空兼容 shell。调用方必须显式选择 Frontend、Backend、Protocol、Types 或独立 failpoint crate，编译器可以拒绝越界依赖。

## 裁决

采用选项 3。Core 不再导出模块、features 或依赖，只保留空 crate 壳。值渲染与 `EngineErrorCode` 归 Types；故障注入归独立 failpoint crate，完整 wire 词汇、runner 子集、harness 路径和 cleanup directive 由同一 API 管理；Connector application 归 Frontend，Connector execution 归 Backend；测试随契约或执行 owner 迁移。

指标按角色拆分：FE 与每个 BE 在跨进程模式下只暴露自己的 family；all-in-one 同时暴露两者。cluster harness 在启动屏障后验证该隔离，避免仅靠单进程 smoke 宣称边界正确。

## 接受的妥协（诚实记录）

这次 cut 破坏了 `novarocks` 根路径，短期内需要大量 import 与测试路径改写，也要求下游同时完成依赖重连。选择它并非因为迁移成本更低，恰恰相反；原因是 facade 会把已完成的 owner 判定永久模糊化，并让未来的新功能再次落进无 owner 的桶。空 Core crate 暂时保留是为了让工作区和发布迁移可控，它本身不提供兼容 API。

角色指标的隔离也使抓取端必须明确选择 FE 或 BE endpoint；这是为了使生产拓扑事实可观测，而不是为了简化 all-in-one 测试。

## 何时重新评估

- 出现真正被多个角色共同消费、且不属于 Protocol、Types、Execution、FS、SQL 或 SPI 的稳定契约时，先为该契约建立明确的 owner crate，而不是复活 Core facade。
- failpoint DSL 必须覆盖 runner 10-kind 子集之外的生产场景且不能由当前 vocabulary 无损表达时，扩展独立 failpoint crate 并保持 token/cleanup 映射单一来源。
- 多 FE 或外部 observability 系统需要聚合角色指标时，可以在 Server 或专用观测层聚合；不得让 FE endpoint 混入 BE family 或反向混入。
