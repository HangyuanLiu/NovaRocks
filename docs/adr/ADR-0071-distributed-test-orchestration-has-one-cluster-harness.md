---
id: ADR-0071
title: "Distributed test orchestration has one cluster harness"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-13
provenance:
  - "discussion: 2026-08-13 layered test ownership and distributed scenario composition"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/897"
code-anchors:
  - "tests/cluster-harness/src/lib.rs (CrossProcessClusterOptions, CrossProcessServerHandle::launch)"
  - "tests/sql-test-runner/src/cluster.rs (launch_server)"
---

## 问题

当多个系统场景需要启动、验证、重启和诊断真实的 1FE+NBE NovaRocks 集群时，跨进程编排应由哪个测试层拥有，怎样防止
SQL runner、Server integration test 与后续 scenario frontend 各自形成一套 cluster lifecycle？

## 背景与执行事实

`novarocks-test-support` 已是零 NovaRocks 产品依赖的机械叶子：它只管理子进程、readiness marker、端口保留、日志与
cleanup。它不能命名 Frontend、Backend、拓扑、FE restart、BE start epoch、query-lifecycle fault 或资源指标。

SQL runner 曾同时拥有 SQL DSL/golden/CLI 和真实 cross-process cluster 的配置渲染、端口、process、topology barrier、
restart、fault 文件、metrics 与 artifact cleanup。这使该 runner 成为事实上的通用集群 harness，但其 package 的独立
workspace/profile 又使后续 system scenario 不能自然复用它。

真实部署以 1 FE + N BE 为基准；all-in-one 只是一种测试执行模式，不能成为分布式拓扑、restart 或 fault 行为的第二
语义。分布式 harness 的输入必须是已解析的 binary、base config、artifact root、BE count、deadline 与 capability，
不能反向依赖 suite/case、golden、`RunnerConfig` 或 SQL CLI。

## 考虑过的选项

1. **继续由每个 consumer 复制 cluster lifecycle。** 局部改动小，但 topology/readiness、restart 与 cleanup 的错误
   修复会分叉；任何新场景都可能成为第三套 owner。
2. **把所有测试放进万能 testkit。** 它可复用更多 API，但会让中立机械层依赖 NovaRocks role 与场景断言，并把所有
   owner 汇聚为 service locator。
3. **建立嵌套 `tests` workspace。** 目录看似整齐，但会让 root package membership、独立 SQL profile 与 Cargo
   workspace 解析互相干扰，也不能表达每个 crate 的真实 dependency edge。
4. **root-member named crates，单独建立 cluster harness。** `tests/cluster-harness` normal-depend on
   `tests/test-support`，scenario frontend 和 SQL adapter 依赖 harness；SQL runner 保持自己的 workspace/profile。
   （采纳）

## 裁决

建立内部、`publish = false` 的 root workspace member `novarocks-cluster-harness`，作为真实 1FE+NBE distributed test
orchestration 的唯一 owner。

1. harness 只接受显式 launch options，拥有 per-process config、reserved ports、children、marker-first readiness、
   protocol topology barrier、BE/FE restart、fault scope、日志/metrics diagnostics 与 explicit shutdown；它 normal-depend
   only on neutral `novarocks-test-support` 和必要的测试协议库。
2. SQL runner 保留 CLI、环境与 runner config precedence、suite/case/golden、all-in-one/no-op adapter 和 test doubles；
   cross-process mode 只把已解析输入交给 harness。它仍是独立 Cargo workspace，以保留专有 profile。
3. 根 workspace 直接列出 `tests/test-support` 与 `tests/cluster-harness` 两个 named crate；不建立 `tests/Cargo.toml`
   或把 SQL runner 吸入 root workspace。
4. 新 system scenario 只能以前端 consumer 身份复用 harness，不得复制 process/topology/restart/fault lifecycle。现有
   Server `cluster_mvp` harness 在完成既定迁移前保留，但不得新增第三套 distributed harness。
5. all-in-one 不进入 harness 的 cross-process contract；它仍由具体 frontend 的 no-op/local adapter 表达，防止测试便利
   改写 1FE+NBE 的生产拓扑语义。

## 接受的妥协（诚实记录）

- 新 crate 让一次迁移产生较大的移动 diff，也使 SQL runner 需要一个薄适配层。选择它是为了把已存在、易出错的
  topology/restart/cleanup 状态机收敛为单一 owner，**不是**因为新增 crate 或目录层级本身更少。
- harness 暂时保留与当前 SQL 生命周期故障注入兼容的 child environment 细节；这是一项受控过渡，尚未形成通用场景
  DSL。过早抽象 fault API 会把尚未稳定的断言语言固化为公共接口。
- `cluster_mvp` 不立即迁移，短期内仍有两套历史实现。这是按已确认迁移顺序降低风险的成本，不代表允许继续复制；任何
  新的跨进程场景必须使用此 harness。
- SQL runner 继续在 root workspace 外独立构建，因而其 lock/profile 仍要单独维护。接受这一点以避免为了目录整齐而
  改变现有 SQL 验证的构建和执行语义。

## 何时重新评估

1. 当 harness 需要接受 SQL case、golden、record mode 或 runner config type 时：说明 adapter 方向倒置，应把该逻辑
   留回 consumer。
2. 当 system scenarios 需要一种稳定的、非 SQL 的故障描述语言时：以 typed scenario API 扩展 harness，并以至少两个
   frontend consumer 的真实需求证明它，而非直接复用 SQL directive 文本。
3. 当 `cluster_mvp` 的既定迁移完成时：删除其重复 lifecycle owner，并确认没有第三套启动/重启实现。
4. 当 Cargo 的 root member 与 SQL 独立 workspace 无法同时表达所需 profile 或 target dependency 时：优先调整包边界或
   workspace metadata，不用 source-shape scanner 掩盖图上的问题。
5. 当生产拓扑增加 multi-FE、外部 scheduler 或远程 backend provisioner 时：重新审视 harness 的 launch contract；不能
   把当前本地 1FE+NBE assumptions 静默当作永久产品部署模型。
