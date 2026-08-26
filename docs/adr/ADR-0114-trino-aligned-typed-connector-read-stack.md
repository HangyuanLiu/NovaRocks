---
id: ADR-0114
title: "Trino-aligned typed connector read stack with runtime split assignment"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: [ADR-0034, ADR-0039, ADR-0053]
superseded-by: null
date: 2026-08-27
provenance:
  - "discussion: 2026-08-26/27 Trino-aligned typed Iceberg read stack, B1-B23 裁决"
  - "PR: <backfill after merge>"
code-anchors:
  - "novarocks/spi/src/connector/read_stack/mod.rs (通用 Trino 读词汇)"
  - "idl/novarocks/connector_read.proto (每个 handle/split category 的 closed oneof)"
  - "novarocks/proto/src/connector_read/mod.rs (structural validation 与 canonical bytes)"
  - "novarocks/proto/src/connector_read/execution.rs (role-facing typed connector 边界)"
  - "novarocks/execution/src/connector/scan_queue.rs (per-attempt/per-plan-node split queue)"
---

## 问题

Connector read 的 snapshot、schema、projection、predicate、file、delete、partition 与特殊读语义要以什么形式跨
FE/BE 传输，才能既让 Engine 与 Protocol 用稳定类型审计真实 scan contract，又不把 Provider 的领域正确性搬进
Protocol？在没有 opaque payload 的前提下，splits 应该在什么时刻、以什么身份到达 worker？

## 背景与执行事实

切换前，FE 冻结 provider-private JSON scan handle，把完整 split 集合 eager 嵌进 fragment plan；BE decode 后读
Parquet footer，把 row group 提升为 prepared scan unit 与 membership digest，再在 reader-open 时二次 decode。
`ConnectorTableHandle`、`ConnectorScanHandle` 与 split 都是 `owner + Bytes`；`RuntimeFilterScanDomainTarget` 用
provider 私有的 `field_ordinal` 作跨边界 identity；IMV、MV target、metadata 与 DML rewrite 靠 read purpose、lane
与 payload 分支挤在同一条普通 scan 路径上。

后果是可审计性与分层同时缺失：中央 wire 看不到 file/delete/partition 语义；prepared-unit identity 依赖私有
serializer 字节，把 row-group 错误提升成 public membership；调度并行度由 eager split count 决定，因此一个查询的
DOP 在 Init 之前就被 planning 结果钉死。

三条既有裁决同时到期。ADR-0034 的 composite split + Backend local scan unit 两级生命周期，和 ADR-0039 的
immutable prepared scan-unit domain facts，都建立在「准备期就必须冻结 row-group 级 membership」这个前提上；一旦
split 在运行时投递、footer 只在 page source 内部读取，这个前提不再成立。ADR-0053 让 MV change window 复用
exact-generation scan planning 并返回 sealed neutral admission，也预设 change 语义可以塞进普通 opaque scan。

上游同期两条变化直接约束本设计：删除 native lifecycle 消息自证 digest（ADR-0113）否定了给 scan/split 再造一套
自证身份；Backend self-registration 与 ControlReady 前一次性 topology replan（ADR-0111）要求任何 split source 必须
是 execution round 拥有的资源，而不是跨 round 存活的状态。

crate DAG 是硬约束：`novarocks-spi` 是零依赖 leaf，`novarocks-frontend` / `novarocks-backend` 都不依赖 provider
crate，`novarocks-proto` 依赖 `{proto-models, spi, types}`。任何表示方案都不能反转这些边。

## 考虑过的选项

1. 保留 opaque payload，只在外面加 typed header。迁移量最小，但真实语义仍然是 bytes：Protocol 无法验证 bounds，
   FE 无法按真实代价调度，RF 只能继续用 provider ordinal，问题原样保留。
2. 让 FE/BE 直接依赖 provider crate，在 role adapter 里做 concrete 转换。类型最直白，但会把之前专门拆掉的
   frontend→provider 耦合重新装回去，并让每个新 provider 都改动引擎 crate。
3. 让 SPI 直接声明 provider-named 的具体 handle/split 结构（沿用 `ConnectorExecutionDeclaration` 的形状）。对
   一两个字符串的 declaration 合适，但把上千行 Iceberg frozen facts 放进通用 SPI 会让 SPI 变成 Iceberg 的影子。
4. 中央 IDL 为每个 handle/split category 定义独立 closed `oneof`；SPI 只放 transport-neutral 的 Trino 通用词汇；
   `novarocks-proto` 拥有 structural validation、bounds、canonical bytes 与 role-facing 边界 trait；provider crate
   依赖 Protocol 并独占 concrete 转换；FE/BE 只消费 validated carrier 与边界 trait。选择此方案。
5. 继续在准备期冻结 row-group membership，只把 split 列表改成分批下发。仍然要求 FE 读 footer，并保留
   prepared-unit digest 这套身份，与 ADR-0113 的方向相反。

## 裁决

Connector read 采用 Trino 的 `handle → split source → scheduled split → page source` 模型，并按下列 owner 切分。

**表示与 owner。** 每个 handle/split category 在中央 IDL 拥有自己的 closed `oneof`，variant 由结构选出，不靠 class
id、message name 或私有 bytes。`novarocks-spi` 只定义通用语义（`TupleDomain/Domain/ValueSet/Range`、`Constraint`、
`SplitWeight`、`SourcePage`、`ConnectorPageSource`、`DynamicFilter`、`SystemTable` distribution），不出现 provider 名，
也不依赖 Models。`novarocks-proto` 独占 structural validation、bounds、cross-field consistency 与 canonical bytes，
并拥有 FE/BE 持有的 role-facing 边界 trait；它不解释 connector 语义，也不依赖 provider crate。provider crate 依赖
Protocol 并独占 concrete 转换，因此没有任何引擎 crate link provider，也没有一处 downcast。generic scheduler 只读
split 上被显式提升的中立 envelope（weight、affinity key、retained size、remote-accessible），不解释 variant。

**调度身份。** split 在运行时经 `ScheduledSplit → SplitAssignment → TaskUpdate` 投递给已 admitted 的 task；
fragment plan 不再携带 split 集合。唯一的调度身份是 (task attempt, plan node) 内单调的 sequence：exact replay 幂等，
同 sequence 不同 canonical bytes 是 conflict，`noMoreSplits` 按 plan node terminal 且幂等。handle 与 split 不产生
content digest、freeze digest 或自证身份；canonical bytes 只用于结构校验与同 sequence 的重放比较，因此
connector-read 的 map 生成为 `BTreeMap`，让同一条 assignment 每次编码出相同字节。零 split 走正常的 no-more/EOS，
不构造 synthetic empty split；队列空且未 terminal 是 blocked，不是 EOS。

**准备与执行分层。** FE 从 pinned snapshot 与 immutable metadata reference lazy 产生 typed split，不读 Parquet
footer、不查 latest snapshot。一个 split 创建一个 page source；footer、row-group 选择、field binding、delete 应用与
row-group 级 domain facts 全部是 BE-local 且 split-local 的，不再被提升为准备期 membership。这正式取代 ADR-0034 的
两级 composite-split/scan-unit 生命周期与 ADR-0039 的 immutable prepared scan-unit domain facts：两者要求的准备期
row-group 冻结在本模型中不再存在，其 exact-generation 与 bounded-facts 意图由 typed split、Protocol bounds 和
page-source 内部的保守求值继承。

**特殊读语义 typed 化。** Trino `table_changes`、NovaRocks IMV change window、Iceberg system relations、table
execute 与 COW merge source 各自使用 typed specialization，不再由 read purpose、lane 或 opaque bytes 分流。IMV change
window 的正确性定义是两个 endpoint 的 visible-row set difference，不是 manifest-entry replay；`__change_op` 由 split
variant 派生。这取代 ADR-0053：change window 不再复用普通 opaque scan admission，而是自己的 closed handle 与 closed
split variants，无法证明 endpoint visibility 或 disjointness 时 fail closed。

**Runtime filter。** BE 现有的 row-group pruning 能力保留，但输入改为 `TableScanNode` 的 ordered assignments 与
dynamic-filter binding，通过 Iceberg field ID 的 column handle 解析，不再有 provider `field_ordinal` 上 wire。FE 侧
今天没有实时反馈，因此如实报告 complete、non-awaitable、`TupleDomain::all()`，不伪造等待。

剪枝的判据是 **oracle 而不是 domain**。NovaRocks 的 runtime-filter artifact 按 ADR-0043 只暴露谓词 oracle
（能回答「这段范围可能匹配吗」），不能枚举值、也不能给出边界。若强行把 artifact 转成 `TupleDomain`，在「无法精确
表达就必须放宽」的规则下每一列都会变成 all，得到一个自称 live、行为却与无反馈过滤器完全相同的东西。因此
`DynamicFilter` 保持 Trino 形状（`columnsCovered` / `isComplete` / `isAwaitable`）并如实报告不约束，另外提供
`boundsMayMatch(column, bounds) -> Possible | Impossible | Unknown` 作为剪枝提问；`Unknown` 是默认值且永不剪枝。
现有的 prepared-unit 剪枝本来就是这样问 oracle 的，本裁决只把提问粒度从 prepared unit 换成 row group。
ADR-0043 因此保持完整，不需要给 codec 增加值枚举 API。

page source 持有的是 dynamic filter 的**共享句柄**而非借用，并随 split 收到 task-attempt-scoped 的
`scheduledSplitSequenceId`。前者让「每个未读 row group 前重新取」成为可能，后者让一次 row-group 判定能在不引入
membership digest 的前提下被归属。

**边界与非目标。** Parquet 是唯一完整 reader；ORC、Avro、Parquet modular encryption 与加密 manifest 保留同名扩展点并
在 producer/consumer 两端稳定 `NOT_SUPPORTED`，绝不从路径后缀推断格式。StarRocks 只保持编译并在 read 入口稳定
`NOT_SUPPORTED`，不为其发明 typed wire variant。ADR-0111 的 ControlReady 前一次性 replan 保持有效：split source、
assignment、sequence 与 dynamic-filter view 都是 round 拥有的资源，旧 round 先 close/discard，replacement 以同
query ID、新 attempt ID 重建。ADR-0113 保持有效，本裁决不引入任何消息自证。ADR-0015（Connector 拥有 read
correctness 且 fragment 只绑定已安装真实 instance）、ADR-0043/0044（RF evaluator 与 Backend participant owner）、
ADR-0089（reader-open 的 page pruning）继续成立。ADR-0050 只做 read-side / merge handle 的机械迁移，其 logical
mutation effect owner 与 publication 语义不因表示变化而改变，因此不被 supersede。

## 接受的妥协（诚实记录）

- **SPI 与 Protocol 之间多了一层边界 trait。** provider 的 concrete 类型无法出现在 SPI 的方法签名里（会成环），
  所以 FE/BE 实际持有的是 `novarocks-proto` 定义的 `TypedConnectorMetadata` / `TypedConnectorSplitManager` /
  `TypedConnectorPageSourceProvider`。这比 Trino 的单层 SPI 多一跳，是为了保住「引擎 crate 不 link provider」这条
  更重要的边界而付的代价，不是因为两层本身更好。
- **delete descriptor 随每个 split 重复。** 一个 data file 切成多个 byte-range split 时，完整的 applicable delete
  closure 会被复制多份。接受这份有界重复，换取 split self-contained、没有远程 dictionary 的生命周期。
- **canonical bytes 需要 `BTreeMap`。** 为了让 exact replay 能逐字节比较而不引入 digest，connector-read 的 proto map
  必须生成为 `BTreeMap`。这让该 package 的生成类型与仓库其余部分不一致，是刻意的局部代价。
- **本 PR 只有 BE 侧动态剪枝。** FE 没有 coordinator-visible Domain 反馈，因此 split source 无法做 whole-file dynamic
  pruning。现有能力不回退，但新增能力明确留给后续 FE feedback 工作。
- **StarRocks 短期功能真空。** 其 read 入口在本裁决后返回 `NOT_SUPPORTED`，直到有独立 accepted spec 定义它的 typed
  语义。接受这个空缺，而不是让一个尚未成熟的 connector 反向塑造通用模型。
- **切换面积很大。** 一个 spec、一个 PR、一次原子切换，没有 feature flag、fallback decoder 或可合并的半状态。可合并性
  完全由内部 checkpoint 与文件 ownership 保证，而不是由中间兼容态保证。
- **`ConnectorSplitManager` 这个名字没有落在 SPI。** Trino 的对应接口在 NovaRocks 是 Protocol 的
  `TypedConnectorSplitManager`：SPI 是 transport leaf，方法签名里不能命名 validated carrier（会与 Protocol 成环），
  所以真正的入口只能在 Protocol。SPI 保留 `ConnectorSplitSource`（provider 真实实现它），不保留一个没有实现者的
  同名 manager trait。名字对齐让位于 crate DAG，这是刻意的取舍。
- **overloaded `ConnectorTableHandle` 的 non-read 迁移体量与 read 切换相当。** 它穿过 11 个 SPI capability 模块、
  约 230 处调用点。本裁决只规定它必须迁到各自 typed/provider-local domain 且不得改写 durable 语义，
  不假装那是 read 切换的附带清理。

## 何时重新评估

- 出现第二个真实需要 typed read 的 provider（例如 StarRocks 的 typed read spec 落地）时，重新检查通用 SPI 词汇是否
  仍然中立，以及 closed `oneof` 的 variant 增长是否仍可接受。
- 需要 mixed-version 或 rolling upgrade 承诺时：本裁决假设同仓、静态链接、原子发布，closed `oneof` 与「无 negotiation」
  会立刻成为约束。
- FE 获得 coordinator-visible dynamic filter 反馈、或需要 whole-file dynamic pruning 时，重新评估 `DynamicFilter`
  三层契约是否还够用，以及 split source 是否应当允许 awaitable。
- 需要真正支持 ORC/Avro 读或 Parquet modular encryption 时，重新评估「typed 扩展点 + 稳定 Unsupported」是否仍是正确
  的占位方式。
- 若 delete closure 的重复复制在实际负载中成为可观测的 wire 或内存成本，重新评估是否需要 per-task 的 delete
  descriptor 共享。
