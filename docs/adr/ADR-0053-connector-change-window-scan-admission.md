---
id: ADR-0053
title: "Connector change-window scan admission"
domain: [provider-spi, frontend-mv]
status: superseded
supersedes: []
superseded-by: ADR-0114
date: 2026-08-11
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/861"
  - "discussion: 2026-08-11 provider-neutral MV snapshot-window admission during the Iceberg owner cut"
code-anchors:
  - "novarocks/spi/src/connector/read.rs (ConnectorBeginScanRequest)"
  - "novarocks/spi/src/connector/control.rs (ConnectorScanPlanning)"
  - "novarocks/connector/iceberg/src/control_provider.rs (IcebergControlProvider::begin_scan)"
---

## 问题

Frontend 如何在 retained exact Connector generation 上请求一个 snapshot change window，并在不解释 table-format
lineage、delete visibility 或 provider payload 的前提下，区分 metadata-only、可增量执行、可安全 full rebuild 与
必须失败？

## 背景与执行事实

ADR-0047 已把普通 Connector read 收敛为 exact-generation table handle、FE-only scan planning、opaque split 与中立
native carrier。从 provider 生成 opaque split之后，Frontend placement、native transport、Backend exact binding 与
provider reader已经保持 owner-correct。

MV incremental read还多一个 admission阶段：application持有 previous/current snapshot identity，provider必须沿
table-format lineage判断这两个端点之间是否存在 insert、row delete、deleted data file或不安全的 replace，并为
delete-side scan生成 prior visibility与row membership。现有 `ConnectorReadSelector` 只能选择 current、一个 snapshot
或一个 timestamp；`ConnectorScan` 也没有表达 change policy的返回值。因此 Core仍直接调用 concrete Iceberg change
planner、保存 provider delta DTO并构造 split，令 Core无法删除对 Iceberg provider crate的依赖。

Snapshot ID是 opaque identity，不保证数值顺序。`from`与`to`的祖先关系只能由 exact provider table判断。两个端点
相同是合法的空 window，而不是 invalid range。

## 考虑过的选项

第一种是把第二个 snapshot ID编码进 static predicate、projection、limit、Arrow metadata、provider alias或 opaque
handle。这样表面上不改变 SPI，却把 snapshot lineage伪装成已有 SQL/read语义，并要求 Core或Server理解隐藏 codec；
它是不可验证的 facade。

第二种是新增 `ConnectorDeltaPlanning` capability或 MV-specific application port。它可以显式建模 delta，却与普通
scan复制 exact generation、table handle、scan handle、split planning与lifecycle，扩大 resolver/host surface并形成
平行 read path。

第三种是保留 Core concrete delta admission，等后续再迁。它能减少当前 diff，但 Core必须继续依赖 provider crate，
使 provider owner与Cargo DAG无法在同一原子边界完成。

第四种是扩展既有 `ConnectorScanPlanning`：begin-scan request使用 tagged snapshot/change-window selection，returned
scan携带sealed、provider-neutral admission；provider在同一 exact generation内规划lineage与opaque delta splits。

## 裁决

采用第四种方案。

`ConnectorBeginScanRequest` 使用 `ConnectorScanSelection`。`Snapshot(ConnectorReadSelector)`保持普通 current、
snapshot-id、timestamp read；`ChangeWindow(ConnectorChangeWindow)`封存 `from_exclusive` 与 `to_inclusive`。两者是
互斥tagged shape，普通 provider不能把window误作一个snapshot selector。window端点不做数值排序；provider验证
snapshot存在性与祖先关系，同端点返回合法empty admission。

Change-window scan返回sealed `ConnectorChangeWindowAdmission`：

- `MetadataOnly`：window没有logical row change；
- `Incremental { has_inserts, has_deletes, partition_impact }`：application可据此选择branch和write mode；
  `partition_impact` 只携带 bounded、canonical、provider-neutral 的 added/removed partition tuple，每个field仅包含
  exact metadata下的source column、typed transform与null/canonical string value；具体position/equality delete、
  deleted data file identity、prior visibility与row-id facts只进入provider-private split payload；
- `FullRebuild`：只允许bounded typed `LineageBroken`或`UnprovenReplace` evidence。replace failure进一步区分
  missing parent、record-count change、missing/invalid summary、invalid data-file counts与schema change。

Schema evolution、unsupported operation/format、corrupt metadata、I/O、cancellation、deadline与internal invariant
仍是typed `ConnectorError`，不得降级为full rebuild，也不得由consumer解析错误文本选择policy。

Partition impact固定为 `Unavailable`、`Unpartitioned` 或
`Exact { has_row_deletes, added, removed }`。added/removed合计最多16,384个unique partitions，单partition最多
256 fields，总计最多65,536 fields，且全部结构和字符串成本受request total-payload budget约束。fields按
ASCII-normalized source column与transform排序并保证pair唯一，partitions分别排序去重。unsupported transform/value
或缺失partition metadata产生`Unavailable`，只让affected-partition policy保持现有`NotDerived`，不改变scan admission；
row delete由`has_row_deletes`显式保留row-evaluation fallback。Core使用同一exact lease的neutral schema observation
解析source column，不猜测名称、不读取provider field ID、不解码handle。

Admission绑定exact owner/incarnation、selection digest与opaque scan-handle digest。scan与admission必须由constructor
一起seal，Core不能跨generation/window重新组合。`plan_splits`从同一opaque handle生成delta splits。selection、
admission与scan handle只存在于FE control/application内，不新增native wire或durable state；现有opaque split carrier、
Backend installer与provider reader保持不变。

Core继续拥有MV policy和多base合并：hard error直接失败，任一full-rebuild evidence触发现有full refresh，全部
metadata-only只推进metadata，其余按insert/delete flags形成join branches及FastAppend/RowDelta，并只消费neutral
partition impact推导affected partitions。Core不拥有table-format lineage、change batch或delete DTO。

## 接受的妥协（诚实记录）

该裁决让所有scan provider与fake都必须处理新的tagged selection，并把原本只为普通scan设计的`ConnectorScan`
收紧为sealed admission value；这是一项breaking public SPI change。选择它不是因为一个trait承载更多语义更简洁，
而是snapshot read与change-window read实际共享同一exact generation、table authority、opaque scan/split生命周期；
另建capability只会复制这些安全边界。

Provider-neutral full-rebuild reason与partition impact也把一小部分application policy vocabulary放进SPI。接受这一点是因为consumer
必须可靠区分“允许full rebuild”与“必须失败”，通用error kind或字符串不能证明该差别。该vocabulary故意限制为
lineage broken与unproven replace，不泛化为provider error dump；新的fallback reason必须重新证明跨Provider语义。

## 何时重新评估

- 第二个非Iceberg Provider需要change-window scan，但无法把安全fallback归约到当前bounded reason，且其语义确实
  可以被同一application policy替换。
- 第二个provider无法用bounded source-column/transform/value tuple表达partition impact，且consumer确实需要比
  `Unavailable`更强的中立partition policy；不得用provider reason string或raw field ID扩展当前值。
- Change-window selection必须跨native process或持久化恢复；届时需要单独裁决versioned wire/durable evidence，
  不能直接序列化当前FE-local value。
- 普通snapshot scan与change-window scan的lifecycle不再共享table authority、split planning或execution binding，
  新capability能显著减少而不是复制安全边界。
- Application需要比insert/delete存在性更丰富的中立change summary；新增字段前必须证明不会泄漏table-format
  manifest、delete或row-membership事实。
