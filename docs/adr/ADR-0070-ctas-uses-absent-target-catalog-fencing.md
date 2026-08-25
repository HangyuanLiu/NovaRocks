---
id: ADR-0070
title: "CTAS takeover uses catalog-native absent-target fencing"
domain: [provider-spi, frontend-dml]
status: superseded
supersedes: []
superseded-by: ADR-0104
date: 2026-08-13
provenance:
  - "mechanism: catalog-native fenced CTAS staged publication and historical recovery (PR number to backfill after merge)"
  - "builds on: ADR-0032 (provider-owned staged CTAS publication)"
  - "builds on: ADR-0068 (catalog-linearized external write fencing)"
code-anchors:
  - "novarocks/spi/src/connector/ctas_staged_publication.rs (ConnectorCtasPublicationFence)"
  - "novarocks/frontend/src/dml/ctas/recovery.rs (CtasRecoveryProfile)"
---

## 问题

CTAS 的目标表尚不存在、旧 frontend 又可能已向 catalog 派发 stage、publish 或 abort 时，新 frontend 应如何先切断旧
owner 的外部写权限，再在没有旧 process-local handle 的情况下确定性恢复，而且不把同名表的缺失、重建或损坏记录猜成
`NotCreated`？

## 背景与执行事实

ADR-0032 已决定 CTAS 必须使用 provider-owned staged publication，不能先创建可见表再按名称做破坏性补偿；ADR-0068
又决定普通分布式写必须把 takeover fence 放在 catalog 原子条件更新的线性化点上。但是 CTAS 在 source 与 stage
开始前没有目标表，普通 Iceberg table ref fence 没有可附着的 table identity，不能充当 absent-target fence。

StateStore lease 只保护 frontend durable journal。旧 holder 可能在 lease 失效前已把 catalog 请求送出，并在新 holder
接管后才得到成功响应；因此仅拒绝旧 holder 的 journal writeback 不能阻止旧请求改变外部 catalog。反过来，新 holder
也不能取得旧 Connector generation 的普通 staged-publication lease：旧 handle 是 generation-local 的，跨 generation
复活它会把 exact-generation contract 变成表面约束。

CTAS 还同时存在几类不能从表名推断的事实：stage/create 响应丢失、publish 成功后响应丢失、`IF NOT EXISTS` 的
`NoOp`、catalog durable record 缺失或损坏，以及目标被 drop 后以新 UUID 重建。任何一个场景若被猜成
`NotCreated` 或 `Staged`，都可能导致重复发布或清理另一个创建者的对象。

## 考虑过的选项

1. **只使用 StateStore operation lease。** 实现成本最低，但无法阻止已派发的旧 catalog 请求，拒绝采用。
2. **等 staged table 出现后复用普通 external write fence。** fence 建立得太晚：旧 stage 本身仍可跨 takeover，且
   stage response loss 时新 owner 不知道应绑定哪张 staged table，拒绝采用。
3. **先创建可见空表，再使用表上的 fence。** 重新引入 ADR-0032 已排除的 destructive compensation 与可见残留，
   拒绝采用。
4. **由 frontend sidecar 或进程内锁按 catalog name 记录 owner。** 它不能约束其他 frontend、Spark 或 catalog
   service，也不是 external mutation 的线性化点，拒绝采用。
5. **由 catalog 原子维护 operation-scoped ordered generation、stage locator、terminal disposition 与 proof，并通过
   current Connector generation 的独立 historical facet 检查和清理。** 采纳。

## 裁决

CTAS 使用独立于普通 table write fence 的 **catalog-native absent-target fence**。fence 绑定 cluster identity、稳定
CTAS operation id、目标 identity、严格递增的 `(control-plane incarnation, resource epoch, attempt)` generation、稳定
action id 与 canonical input digest。catalog 必须先原子接受更高 generation，随后才允许该 generation 的 stage、publish
或 proof-bound abort；旧 generation 的 action 必须确定性失败，不能靠 frontend 本地锁近似。

catalog durable record 是 CTAS external truth，至少保存已确认 fence receipt、staged locator 与 proof、每个 mutation 的
exact action seal、typed terminal disposition，以及 cleanup authority。相同 generation 的 exact replay 是幂等的；相同
generation 的 action/digest 漂移是 typed conflict；更低 generation 是 typed stale。所有 opaque locator、proof、receipt
在 SPI 边界和 frontend journal 边界都必须有界并密封完整上下文。

takeover 顺序固定为：StateStore claim 与 strictly-higher generation → catalog advance fence → durable receipt →
visibility-first historical inspect → typed converge。historical recovery 是 current binding 上独立安装的 facet，不取得旧
ordinary lease，也不调用旧 stage/publish/abort session。`Published` 永远禁止 cleanup；`Staged` 或 retained `NoOp`
只有在 catalog proof 与 historical distributed-write 结论共同授权后才能 guarded cleanup；`Ambiguous`、`Unsupported`、
record missing/corrupt、drop/recreate identity drift 一律保留 recovery due，不从名称、对象列表或错误文本推断。

能力通过 catalog server 的 exact-version advertisement 安装。只有明确广告并实现上述原子语义的 REST catalog generation
才能启用；用户 catalog property 不能伪造能力。vanilla REST、Hadoop 和 Hive 在 source planning、stage dispatch 与文件写入
之前返回 typed `Unsupported`。reference fixture 只用于契约与故障验证，不构成通用第三方 REST catalog 认证。

## 接受的妥协（诚实记录）

该裁决引入了一个非标准 Iceberg REST extension 和额外 catalog durable state。选择它不是因为扩展协议比复用标准 REST
更优雅，而是因为标准 staged-create API 没有 absent-target generation fence，也没有足够的历史 operation 查询来阻止旧
action。短期结果是 CTAS takeover-safe 支持范围明显收窄：vanilla REST、Hadoop 与 Hive 全部显式不支持，即使它们仍能
执行普通读写或单进程 create。

catalog record 与 frontend side record 都有严格大小上限；超限时系统 fail closed 并需要人工处理，不会截断证据。history
也是有界的，长期反复 takeover 的极端 operation 可能进入 manual retention。reference fixture 使用 SQLite 证明 durability
与竞态，但它不是生产 catalog 的高可用实现，也不证明任意第三方 catalog 已正确实现扩展。

`NotCreated` 只表示 catalog 对该 operation 的明确、带 proof 结论，不等于系统拥有足够的 source execution context 来自动
重放 CTAS。当前实现宁可安全终结或保留人工恢复，也不从旧 journal 猜测一个 continuation。drop/recreate 只能依赖 catalog
持久化并比较 target UUID 或等价 incarnation；缺少该 identity 的 provider 必须返回 `Ambiguous`。

## 何时重新评估

- Iceberg REST 标准增加 absent-target operation fencing、幂等 staged-publication operation 查询与 proof-bound cleanup 时；
- vanilla REST、Hadoop 或 Hive 获得可跨进程、跨 frontend 证明的同等级 catalog authority，并能通过完整 fault/race matrix 时；
- 生产 catalog 的 operation history 或 bounded evidence 经常触达大小/retention 上限，导致可观测的人工恢复负担时；
- 产品需要在 takeover 后自动重放明确 `NotCreated` 的 CTAS，并已能持久化、重新验证完整 source execution context 与新
  provider-signed child continuation 时；
- 出现第二个非 Iceberg provider，证明 ordered generation + catalog operation record 不是合适的最小跨 provider 契约时。
