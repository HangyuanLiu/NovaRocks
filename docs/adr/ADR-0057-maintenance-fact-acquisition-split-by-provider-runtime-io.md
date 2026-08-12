---
id: ADR-0057
title: "MV maintenance fact acquisition split by provider runtime IO"
domain: [table-maintenance, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-12
provenance:
  - "PR: MV maintenance fact neutralization — storage-observation projection for metadata facts plus a compactable-group observation capability (PR number pending merge)"
  - "discussion: 2026-08-12 MV 自动维护事实中立化的通道判据"
code-anchors:
  - "novarocks/core/src/mv/storage_observation.rs (MvStorageObservationPort::observe_maintenance_metadata)"
  - "novarocks/spi/src/connector/metadata_maintenance.rs (ConnectorMetadataMaintenance::read_max_compactable_data_files)"
  - "novarocks/connector/iceberg/src/storage_inspector.rs (IcebergStorageInspector::observe_maintenance_metadata)"
  - "novarocks/frontend/src/mv/maintenance.rs (TablePolicy::resolve)"
---

## 问题

一个应用侧策略需要多项 provider 表事实时，哪些事实走 Core 自己拥有的观测 port、哪些必须走 Connector SPI
capability？分界判据是什么？

## 背景与执行事实

MV 自动维护策略消费七类表事实。消费点是 frontend 的 `TablePolicy::resolve` 与 `evaluate_facts`
（`novarocks/frontend/src/mv/maintenance.rs`），载体是 `MvMaintenanceFacts`
（`novarocks/core/src/mv/background.rs`）。取得这些事实的路径此前直连具体 Iceberg 表：
`engine/iceberg_maintenance.rs` 的 `resolve_maintenance_catalog` 直读 `StandaloneState.iceberg_catalogs`，
`engine/mv_maintenance/stats.rs` 的 `collect_table_stats` 再在这个具体 catalog 上 `load_table`。这条直连让
Core 继续拥有 concrete Iceberg catalog 与 runtime 依赖，锁死了 Iceberg provider 的 owner cut。

中立化时暴露出四个客观事实：

1. **已有一条零成本的中立通道。** `MvStorageObservationPort`（`novarocks/core/src/mv/storage_observation.rs`）
   已在生产装机，composition root 装的是 `IcebergMvStorageObservationAdapter`
   （`novarocks-server/src/composition.rs`）。它是 **Core 拥有的 application port，不是 Connector SPI
   capability**：Core 在 retained exact lease 下载入 `ConnectorTableMetadata`，再把这个封存值交给
   inspector，inspector 是唯一允许解释其 opaque handle 的一方。

2. **该通道能提供的东西有硬上界。** provider 侧 `IcebergStorageInspector` 的 `decoded_table` 从 opaque
   handle 解出的是**纯 `TableMetadata`**——没有 FileIO，没有 catalog client。这不是风格选择，是这条通道的
   物理边界：它换来的正是「不触发 provider runtime IO」。

3. **七项里六项落在这个上界内，第七项不落。** current snapshot id、全部保留快照的 `(id, timestamp)`、
   非默认引用计数、current snapshot summary 的三个计数、以及四项 typed maintenance policy 值，都是纯
   `TableMetadata` 投影。`max_compactable_data_files` 不是：`current_live_data_file_compaction_stats`
   （`novarocks/connector/iceberg/src/commit/rewrite_data_files.rs`）的分组键是「(partition spec id,
   partition 值, 保留 row lineage 时还有 data sequence number)」，**逐 live manifest entry** 统计；
   snapshot summary 只有 `total-data-files` 这类总数。fixture 实测反例：总数 5 / 7 / 9 的表，答案分别是
   3 / 4 / 5。取得它必须枚举 live manifest，也就必须使用 provider runtime 的 FileIO。

4. **快照清单不能用既有 read facts 代替。** 下游 `downstream_floor`（`engine/mv_maintenance/stats.rs`）对
   任何解析不出时间戳的 consumer 快照置 `unknown = true`，而 unknown **直接阻断 expire**。既有的
   `ConnectorReadReferenceFacts`（`novarocks/spi/src/connector/metadata.rs`）两个字段都不够：`snapshot_ids`
   来自 `snapshots()` 但不带时间戳，`snapshot_log` 带 `(id, timestamp)` 却由 `history()` 填充，而 snapshot
   log 只记录默认分支上的提交。只在非默认引用上可达的快照因此有时间戳、没有 log entry，复用它会是一次
   静默的行为变更。

此外，ADR-0052 已把 unbounded property map 明确排除出中立面，并写明「后续若需要它们必须以独立设计裁决其跨
Provider 语义」。`TablePolicy::resolve` 实际只读四个键，这里就是那次裁决的落点。

## 考虑过的选项

**A：一个自包含的 `ConnectorMetadata` method 承载全部七项。** 单次调用、语义集中，读者不必知道分界线。
代价是要向 SPI 公共面加一个会做 manifest IO 的 metadata 方法——这既偏离 ADR-0052「不新增 `ConnectorMetadata`
method」的先例，其成本性质也与 `load_table` 这条热路径完全不符（把一次昂贵的 manifest 枚举挂在通用 metadata
面上，等于邀请未来的 planning 路径去调它）。此外 legacy Core 侧要写一份完整 DTO 实现，而这份实现在 legacy
Iceberg 子树被整体退役时会一并删掉；同时它还要与刚装好的观测通道并行维护两套机制。

**B：全部七项走 `ConnectorMetadataMaintenance`。** 语义归口一致（都是「维护事实」），也是单次调用。但它主动
放弃了一条零成本的既有投影通道：六项本来可以搭在维护 pass 已经付过的那次 metadata load 上，改走 capability
后要为它们单独设计 DTO，并且同样要在 legacy 侧写一份注定被删的完整实现。

**C：按「取得该事实是否需要 provider runtime 的 FileIO」切成 6 + 1，两侧走不同通道。** 六项纯投影搭观测口，
第七项走 capability。代价是同一个策略的七项事实被切到两条通道上，读者必须知道这条判据才不会觉得放置随意。

**D（另一条轴）：把 `novarocks.maintenance.enabled` 从表属性搬到 application 侧存储。** 这样 policy 事实
就不必跨 provider 边界。但它是 MV 自动维护落地时特意留给用户的**可设逃生口**，两侧属性 denylist
（`novarocks/connector/iceberg/src/catalog_control/catalog_mutation.rs` 与 legacy 的
`connector/iceberg/catalog/schema_update.rs`）都已显式放行该键；搬走会改变用户可见行为。

## 裁决

采用 **C**。分界判据是**取得该事实是否需要 provider runtime 的 FileIO**，不是「该事实在语义上属不属于维护」。

**六项纯 `TableMetadata` 投影走 Core 拥有的观测口。** `MvStorageObservationPort` 增加
`observe_maintenance_metadata`，返回 `MvMaintenanceMetadataObservation`（私有字段 + `try_new` 自校验）。它
**不带 default method**：未安装即 fail-closed，与该 port 既有约定一致。快照清单必须覆盖**全部保留快照**
而非 `history()` 子集（事实 4）。`try_new` 拒绝一个不在保留清单里的 current snapshot——保留清单是消费者唯一
能解析快照时间戳的地方，缺失即 corrupt metadata，不是值得转发的事实。observation 报告有多少引用不是 provider
的默认引用，**从不报告那是哪个 ref**：provider-specific 命名不进入中立面。

**第七项走 SPI capability。** `ConnectorMetadataMaintenance` 增加 `read_max_compactable_data_files`，带
default `Unsupported`。请求 `ConnectorMaxCompactableDataFilesRequest` **刻意不携带 operation id**：只读观测
不是一次 mutation attempt，因此永远不进入 durable maintenance lifecycle，不产生 plan、receipt 或 marker
（对照 ADR-0028 的 plan/execute/reconcile 三段式）。契约文档显式声明该调用**昂贵**、保留给后台维护策略，
**禁止放到任何 SQL planning 路径上**。分组规则本身保持 provider-private，只有结果标量跨界；`None` 表示该
provider 不提供此观测，不是零。

**policy 以四个 typed 字段承载，没有 property map。** `maintenance_enabled`、
`expire_max_snapshot_age_ms`、`expire_min_snapshots_to_keep`、`target_file_size_bytes` 四项，**刻意不留 map
fallback**，使第二个数据源无法长回来。事实层只报告表**声明**了什么：键缺失与键存在但解析不出，两者都是
absent。默认值与三处 `.max(1)` 钳制留在 frontend 的 `TablePolicy::resolve`，因为那是策略——表维护的
application/lifecycle 归 frontend（ADR-0009），provider 只负责陈述外部系统事实。

**新鲜度是 provider 的内部义务。** 此前是 Core 亲手调 `entry.invalidate_table_cache` 再让 provider 加载，
那是所有权错位：Core 不该知道 provider 有没有缓存、缓存以什么为键。现在两侧实现各自在加载前失效自己的缓存。

**两次调用锚在同一身份上，并由调用顺序对齐状态。** 观测口在 retained exact
`ConnectorControlPlanningLease` 上执行，capability 调用在同一 exact generation 的 maintenance lease 上执行，
generation 与表身份被显式钉住并校验。

状态一致性则由**顺序**保证，而不是由冻结保证：capability 调用**必须先发生**。它失效 provider 的表缓存、
重读 catalog，并把读到的版本回填缓存；随后的 `ConnectorMetadata::load_table` 命中该缓存，因而观察到同一个
表版本。两侧的 `load_table` 都在 miss 后 insert（`control_runtime.rs` 与 `catalog/registry.rs`），所以这
对 provider 与 legacy 实现同样成立。

这两个性质无法同时用冻结取得：capability 若改读 handle 里的 frozen metadata，就放弃了它存在的理由——
维护决策必须看当前状态。因此顺序是行为契约的一部分，在调用点用注释锁住并说明理由，不是可自由重排的实现细节。

## 接受的妥协（诚实记录）

- **判据是「取得成本」而不是「语义归属」，所以同一个策略的事实被切到两条通道上。** 选 C 的真实理由是复用
  已装机的零成本投影通道、并且避免在 legacy 侧写第二份注定被删的完整 DTO，**不是**因为「六项和第七项在语义
  上属于不同范畴」——它们全都是维护事实。任何按语义分类去找这七项的人都会找错地方；这条判据必须靠本 ADR
  和契约注释显式传达，代码结构本身不自明。

- **状态一致性从「一次加载」降级为「顺序 + 缓存回填」，是一个真实的强度下降。** 改动前
  `collect_table_stats` 只做一次 `load_table`，两类事实复用同一个 loaded 表，状态一致是**类型层面**的必然。
  现在它由两件事共同成立：capability 先调用，以及 provider 的 `load_table` 在 miss 后回填缓存。前者靠调用点
  注释约束，后者靠 provider 的实现行为——都不是编译器能守住的。残留窗口是「两次调用之间另有提交者失效缓存」，
  此时投影事实会比计数更新。
  接受它是因为：manifest 计数只是与 `compaction_min_data_files` 比较的阈值输入，跨提交的偏差最坏让某个
  optimize 动作早一轮或晚一轮被规划，下一次维护 pass 会重新求值；而两种更强的替代都要付更大代价——
  让 capability 改读 frozen metadata 会放弃 D4 的强制刷新（对一个维护决策不可接受），把七项收回同一个
  capability 则推翻本 ADR 的核心裁决。
  **若未来有消费者要求这七项严格同快照，本裁决不成立。**

- **`MvStorageObservationPort` 从 4 个方法变 5 个，它会逐渐像一个杂物袋。** 约束是它的边界仍为「MV 存储表的
  中立观测」：维护目标全部由 MV 定义派生（`novarocks/frontend/src/mv/maintenance_worker.rs` 的
  `canonical_target` 只从 MV 定义的 target 三元组构造），语义未越界。但「方法数增长」本身不构成越界证明，
  只是暂时还没越界。

- **legacy 侧的 `read_max_compactable_data_files` 是一次性投入。** legacy Core Iceberg 子树被整体退役时它会
  一并删除。它存在的唯一理由是「每个 PR 只有一条 production path」——生产 frontend control 当前仍由 legacy
  adapter 服务，只发 provider 侧会在合入当天直接打断维护。用它衡量中立化进度会得到虚高的数字。

- **typed facts 有滑坡风险。** 「只开四个字段」不是一条能自我维持的边界：下一个策略需求会自然地提议第五个。
  约束是任何新增字段必须是 frontend 策略**已经实际消费**的（而不是「将来可能有用」），并且需要重新裁决——
  ADR-0052 拒绝 property map 的理由对第五个 typed 字段同样成立。

## 何时重新评估

- **出现非 MV 表的维护需求时**：观测口的边界（「MV 存储表的中立观测」）不再成立，这六项事实的归属必须重新
  裁决，很可能应整体移到一个不以 MV 为前缀的维护观测面上。
- **维护策略需要第五个表属性时**：说明「四个 typed 字段」不是稳态。届时应正面回答 ADR-0052 悬置的那个问题
  ——bounded typed facts 与 provider property 语义映射，哪个才是跨 Provider 可替换的表达——而不是继续逐个加
  字段。
- **manifest 枚举成为可观测瓶颈时**：`read_max_compactable_data_files` 的成本若在生产维护 pass 上变得显著，
  需要 provider 侧的增量统计或 summary 级近似；任何近似都必须显式区分「精确」与「估计」，因为它直接决定
  optimize 是否触发。
- **观测口方法数继续增长到需要拆分时**：若它开始承载与「MV 存储表观测」无关的调用，应按消费者拆成多个
  port，而不是让它变成 MV 域的通用 service locator。
- **有消费者要求七项事实严格同快照时**：上面记录的一致性妥协失效。届时应让 capability 接受一个显式的
  snapshot 锚点，而不是继续依赖调用顺序——顺序保证挡不住并发提交者，也挡不住一次无意的调用重排。
- **provider 的 `load_table` 不再在 miss 后回填缓存时**：状态一致性的另一半支柱消失，两次调用会退化为
  两个版本。这是一个 provider 内部实现变化就能悄悄破坏契约的点，属于本裁决最脆弱的地方。
