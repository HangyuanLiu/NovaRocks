---
id: ADR-0041
title: "Runtime filter scan-domain evaluation boundary"
domain: [runtime-filter]
status: superseded
supersedes: []
superseded-by: ADR-0042
date: 2026-08-08
provenance:
  - "PR: #842"
  - "discussion: 2026-08-04 runtime-filter scan-domain evaluation boundary"
code-anchors:
  - "novarocks/execution/src/runtime_filter/scan_domain.rs (evaluate_scan_unit)"
  - "novarocks/core/src/exec/operators/scan/runner.rs (ScanSourceOperator::evaluate_scan_unit)"
---

## 问题

当一个已封存的 Connector scan unit 能提供物理列域事实时，runtime filter 的 scan-unit reader-open 前剪枝判断应由哪个层拥有，才能既不改变 SQL 结果，又不把 provider、artifact 或 Core 的过渡实现泄漏进跨角色契约？

## 背景与执行事实

Frontend 在同一次 pinned scan preparation 中把语义列映射为 provider 的稳定 field ordinal，并把精确类型与 nullable 一同封存到 native fragment。Backend 只能解码、校验并绑定这些事实；它不能查询 latest catalog 或按名称重新寻找列。

Connector prepared scan unit 已经以 immutable membership digest、unit ordinal 和 bounded domain facts 表示一个可调度物理单元。runtime-filter snapshot 同时包含行级 predicate 与 logical version。二者结合可在 reader open 前证明某个 unit 不会产生匹配行；证明不足时必须保持 reader open，因为 runtime filter 是性能优化而非 SQL 正确性来源。

## 考虑过的选项

1. 由 Core scan runner 直接识别 Membership、Ordered 等 concrete predicate，再读取 Connector facts 并返回 prune 结论。实现初期改动少，但 Core 会重新拥有 domain 语义，并依赖 concrete downcast，后续无法独立迁移执行层。
2. 由 Connector 或 provider reader 接收 runtime-filter callback，自行决定是否打开 reader。这样会把 query-local RF identity、等待和 artifact 语义传入 SPI，模糊 provider truth 与执行策略的边界。
3. 由 Execution 解释 sealed facts 并产出 outcome；Core concrete predicate 只提供中立的 artifact 查询原语。Backend 只构造 immutable snapshot，Core runner 仅在 reader open 前调用 Execution 并转发 outcome。

## 裁决

选择选项 3。`novarocks-execution` 依赖 `novarocks-spi` 的 sealed domain-facts DTO，但不依赖 Connector implementation、reader、payload、Core、Frontend、Backend 或 protocol。Execution 定义 target、unit identity、typed `Pruned/Kept/NotEvaluated` outcome 和只能从 evaluated outcome 构造的 effect。

Snapshot 显式携带可选 scan-domain capability，不再通过 `Any`/downcast 发现 concrete predicate。该 capability 只回答 retained artifact 的类型、null 命中、是否有非 null 值和闭区间是否可能命中；它不得读取 Connector facts、选择 fail-open 理由或构造 prune/keep 结论。Execution 负责验证 target 与 sealed facts、处理 Missing/unsupported/resource 分类、保留 logical version，并把 contract drift 作为 fail-fast。

## 接受的妥协（诚实记录）

v1 只支持 Connector scalar 与 runtime-filter artifact 可无损对应的布尔、整数、Date32、无时区微秒/纳秒 timestamp 和 UTF-8。Binary、浮点、LargeInt、Decimal、带时区或其他 timestamp unit 都返回 typed `NotEvaluated`。这是为了先把 ownership 和错误边界固定下来，而不是因为这些类型在原则上不能做剪枝；扩展它们需要同时证明 provider facts、artifact comparator 和 Arrow type 的比较没有隐式转换。

Capability 仍由 Core concrete predicate 实现，因为现有 retained artifact 与其内存记账暂时属于 Core。这个过渡依赖是为降低一次迁移的改动成本，不代表 Core 拥有 scan-domain evaluator；KRN-5 完成运行器迁移时应一起移动该 adapter。

## 何时重新评估

- 需要 compound/multi-column domain、prefix/collation 或 Decimal/timestamp-with-timezone 比较时，先新增 execution-owned typed capability 和完整的跨层类型证明。
- provider facts 需要额外 identity、统计版本或安全边界才能保持 sealed correctness 时，先扩展 SPI immutable facts，再评估 evaluator 输入。
- 建立 Backend observation store 时，保持 outcome 代数不变，只在 adapter 后聚合；若 store 反向要求 Execution 依赖 Backend，应拒绝该设计。
- 将 scan runner 移出 Core 时，将 Core predicate adapter 与 runner 一并迁移，避免保留第二个 evaluator owner。
