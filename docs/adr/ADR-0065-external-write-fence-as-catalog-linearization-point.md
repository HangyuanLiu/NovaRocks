---
id: ADR-0065
title: "External write fence as a catalog linearization point for distributed DML"
domain: [provider-spi, frontend-dml]
status: active
supersedes: []
superseded-by: null
date: 2026-08-13
provenance:
  - "mechanism: provider-owned external write fence + historical write recovery facet for INSERT/DELETE/UPDATE/MERGE (PR number to backfill after merge)"
  - "builds on: ADR-0054 (operation-scoped StateStore authority, PR #863)"
  - "builds on: ADR-0051 (exact-generation Connector write activation)"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/write_fence.rs (establish_fence, raise_fence)"
  - "novarocks/connector/iceberg/src/commit/helpers.rs (submit_fenced_action)"
---

## 问题

控制面 lease 可以阻止陈旧 frontend 修改 durable coordination 状态，但它**无法撤回一个已经发出的 Connector
commit**。一个在写入中途被暂停的 frontend，可能在另一个 frontend 完成 takeover 之后醒来，并把它那次提交送进
catalog。

那么：跨 frontend takeover 之后，如何确定性地让旧 generation 的 Connector commit 失败？以及新 owner 在拿不到旧
process-local session 的前提下，如何合法判断旧 operation 到底提交了没有？

## 背景与执行事实

- ADR-0054 已经明确记录：operation-scoped StateStore authority 只保护 durable DML transition，**不是** external
  commit fencing。它把这一层留给了后续决策，本 ADR 就是那个决策。
- ADR-0051 冻结了 exact-generation write contract：prepare 到 terminal 必须保持同一个 Connector generation。旧
  generation 消失后，新 owner 既取不到旧 session，也不能用 later-current generation 去重放普通
  `commit`/`reconcile`。
- frontend 只持有中立 identity、digest 与 bounded opaque evidence；它不允许读取 Iceberg snapshot summary、
  metadata location、manifest、数据/删除文件或 staging 目录。
- ADR-0037 已经为「历史 MV refresh 只能做 lake inspection 与 guarded cleanup」建立了同构形状：acquire current
  lease → binding 上的窄 facet → digest 密封 descriptor → typed disposition → proof-bound cleanup。本决策沿用
  该形状，并补上它没有的 fence 层。
- **iceberg-rust 0.9 的 `Transaction::commit` 结构上无法 fence 陈旧 writer。** `do_commit` 开头即
  `catalog.load_table`；只要 metadata 变化，它就以刷新后的 table 为 base **重新执行每个 action**，从而按即将断言
  的那个值重算 requirement，外层还有 backoff 重试。这样产生的 requirement 永远自我一致，因此永远不会拒绝一次陈旧
  提交。
- 仓库内的 iceberg-rust 是 vendored 分支，其 patch 已把 `TransactionAction` 与 `TableCommit::builder().build()`
  提升为 `pub`，自组装 `TableCommit` 并直接 `Catalog::update_table` 是受支持路径。
- 该 vendored 版本的 `TableRequirement` 只有 uuid、ref-snapshot-id、schema-id、field-id、partition-id 等变体，
  **没有任何 table property 断言变体**；Iceberg REST 规范同样没有，服务端不会校验一个自造的 property 断言。

## 考虑过的选项

1. **只用 StateStore fence。** 已被 ADR-0054 排除：它挡不住已经 dispatch 的外部提交。
2. **保留 `Transaction::commit`，事后检测冲突。** 事后检测不是线性化点：两个 owner 仍可能都提交成功，只是之后
   发现不一致。放弃安全性换取零改动，不可接受。
3. **让 frontend 解释 Iceberg snapshot/metadata 来判定旧 operation 的结果。** 直接违反 provider 拥有
   external-system truth 的边界，且会把 frontend 绑死在一个 provider 的物理布局上。
4. **用 table property 断言承载 fence generation。** vendored crate 与 REST 规范都没有 property 断言变体；对
   REST catalog 而言服务端不会校验，等于没有 fence。
5. **单个 per-(table,target-ref) fence ref。** 实现最省，但同表所有并发 DML 会争用同一个 marker：sibling
   operation B 建立 marker 后，完全不陈旧的 operation A 的 ref 断言立即失效。fence 由此退化成 table-global write
   lease，与「普通 table concurrency 由 provider base-state CAS 仲裁、不默认引入 table-global lease」的领域原则
   冲突。
6. **per-operation fence ref 上的 marker snapshot，与写提交在同一个原子条件更新中比较。**（采纳）

## 裁决

采纳选项 6。

- fence 是 SPI 拥有的 bounded value，绑定 cluster identity digest、control-plane incarnation、resource epoch、
  stable write operation id、resource identity（table + target ref）与 coordination attempt id，并提供**可全序
  比较的 generation**（incarnation 支配 epoch）。Connector 不依赖 state-store crate，provider 不持有 lease、
  不访问 StateStore。
- 每个分布式写 attempt 在任何可能产生不可逆外部效果的 writer/commit dispatch **之前**，先在
  `novarocks-write-fence-<write_operation_id>` 这个 provider-private ref 上发布一个 **marker snapshot**（无数据
  文件、空 manifest list，summary 携带 fence provenance），并把确认后的 receipt/digest 写入 fenced journal。
- 真正的写提交由 provider **自组装 `TableCommit`** 提交，requirements 里追加
  `RefSnapshotIdMatch{fence_ref, 本 attempt 的 marker}`，直接 `Catalog::update_table`，**不经过
  `Transaction::commit`**。fence 比较与写入因此落在同一个原子条件更新里：要么一起成功，要么一起失败。
- **fence ref 按 stable write operation id 派生**，不是按 table。fence 因此只是 takeover guard：只有*同一
  operation 的更晚 attempt* 能 fence 更早的 attempt，互不相关的并发 operation 继续由数据 ref 上的普通 Iceberg
  base-state CAS 仲裁。这也让「不同 operation 不得复用 marker」从运行时检查变成结构性保证。
- takeover 顺序固定为：fenced 记录 recovery request → 让 current provider generation 原子建立**更高** fence →
  事务外 historical inspect → fenced 持久化 typed result → 仅按 result finalize/cleanup/保持 unresolved。更高
  fence 无法确定建立时，不得把旧 operation 判为 safe-to-retry。
- fence 冲突是**终态 typed stale**，永不降级为 unknown、永不当作可重试冲突。分类不依赖 catalog 错误文本：
  precondition 失败后重新观测 fence ref——ref 已移动或消失即判定被 fence；ref 仍指向本 attempt 的 marker 则判定
  为数据 ref 竞争并重试。
- 已 dispatch 的 operation 在 historical recovery 期间**不重放**：对旧 operation 的普通 `commit`/`reconcile`
  调用数必须为 0。只有 provider 证明从未 dispatch 且更高 fence 已关闭旧 authority，才可签发绑定 current
  binding/attempt/fence 的 continuation。

## 接受的妥协（诚实记录）

- **绕过 `Transaction::commit` 意味着我们自己拥有 re-stage 与重试。** `do_commit` 的 refresh + re-apply +
  backoff 同时也是四类 DML **唯一**的并发写重试机制（仓库自己的 `commit_with_retry` 只服务 maintenance 路径）。
  如果只把提交换成一次性自组装 `update_table`，同表并发 INSERT 会从「透明重试」静默退化为「其中一个直接失败」。
  因此 `submit_fenced_action` 复刻了 re-stage 语义。代价是这段并发正确性从上游依赖变成了我们自己的维护负担。
- **重试策略与 iceberg-rust 默认值不同。** 统一收敛到 provider 自己的 3 次 / 10-100-500ms，而不是 iceberg-rust
  按 table property 计算的 backoff。这是为了让 provider 只有一套重试策略，但确实改变了高竞争表上的重试行为。
- **每次 fenced 写多出 catalog 往返。** 至少多一次 marker 提交，加上 staging 前一次 `load_table`（marker 提交已
  让调用方手上的 table 落后一个 commit，若不重新 load 会算出重复的 sequence number）。这是线性化点的必要成本。
- **table metadata 会增长。** 每个在飞 operation 一个 ref 加至少一个 marker snapshot。回收方式是 terminal 时
  `RemoveSnapshotRef`，在飞数量以并发 DML 数为界；但异常路径下残留的 fence ref 需要靠 retention 清理。
- **no-op 提交不携带 fence 断言。** staged action 产出零 update 时无外部效果可线性化，直接返回而不提交一个
  requirements-only commit（避免依赖各 catalog 对空 update 提交的实现差异）。后果是陈旧 holder 的一次 no-op 在
  外部层面"成功"了；挡住它的是 StateStore fence（ADR-0054），不是 external fence。这是分层的边界，不是漏洞，
  但它确实意味着 external fence 并非覆盖 100% 的提交路径。
- **staged output 默认 guarded cleanup + 重新 prepare，而不是跨 generation adopt。** 已完成的 writer 工作会被
  浪费。选这条是因为跨 generation 接管旧 cohort 需要额外证明 cohort/fence/lineage 不变量，而那套证明目前不存在。
- **fence 载体绑定在 Iceberg 的 ref/snapshot 模型上。** 换成另一个 provider 时，"原子条件更新 + 可比较
  generation"这个契约要重新落地一次；SPI 层是中立的，物理载体不是。
- **format-version-1 表的 INSERT 由此收窄为直接报错。** 放弃 iceberg-rust 内置 fast append、改走自组装路径
  后，V1 表落到"phase 1 不支持 V1"的错误上。仓库内没有创建或测试 V1 写入的地方，但这是真实的能力收窄，
  不是纯内部重构。
- **marker snapshot 会出现在表的 snapshot 列表里。** 任何统计或展示 snapshot 的地方（测试、snapshot 列表、
  过期策略）都必须能把它和用户写入的数据区分开；为此提供了 `is_fence_marker_snapshot`。
- **"表被 drop 后重建"只在单次 raise 的情况下可判别。** 当前实现通过"我们发布的 marker 是否有前驱"检测该情况；
  `raise → 崩溃 → 在重建后的表上再次 raise` 这一序列无法与正常序列区分，因而落到 `Ambiguous`。方向是安全的
  （绝不会误判成 `NotApplied`），但恢复不会自动收敛，需要人工介入。彻底闭合需要一个能按 digest 精确匹配历史
  marker 的 fence 血缘读取器。
- **`Staged` 无法枚举被孤立的 writer 输出。** 历史 recovery 拿到的 bounded opaque evidence 不携带已写文件路径，
  而 staging 目录就是表的数据位置、文件名把 operation id 作为**中缀**嵌在分区路径下，因此没有"有界且可证明归属"
  的枚举方式。guarded cleanup 因此只回收 fence ref，不删数据文件；孤立数据文件仍归 orphan cleanup 维护能力处理。

## 何时重新评估

- iceberg-rust 提供可注入的额外 requirement，或原生支持写提交 fencing 时——那时应把自组装提交与自己维护的
  re-stage 循环退回上游，只保留 fence 语义。
- Iceberg REST 规范增加 table property 或通用 precondition 断言时——marker snapshot 可能被更轻的载体取代，
  metadata 增长与 catalog 往返成本随之下降。
- 生产中观察到 fence marker 造成的 metadata 体积或 snapshot 数量成为实际问题时（例如大量长尾在飞 operation
  或 fence ref 泄漏）。
- 出现明确需求要在 takeover 后接管旧 writer cohort（跨 generation adopt）时——那需要一份新的 cohort/fence/
  lineage 不变量证明，并且会取代本 ADR 关于 staged output 的裁决。
- 若未来出现第二个支持分布式写的 provider，应在那时判断"可比较 generation + 原子条件更新"这个 SPI 契约是否
  仍然是正确的最小公约，而不是把 Iceberg 的 ref 模型泛化成所有 provider 的要求。
