---
id: ADR-0176
title: "NovaRocks owns the Iceberg REST request vocabulary, sends add-schema without last-column-id, and replaces views by always adding a version"
domain: [provider-spi, crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0174
    scope: "Ruling 3 retention row for core U2, P8 and View API (REST update vocabulary, authoritative stage initialization, view carrier) and its deletion condition: the patches are deleted and their vocabulary is owned by the Connector. Baseline reproducibility, consumer-owned deletion of the remaining core patches (P2, U3, P7a, U4, U8) and the other rulings stay effective"
  - id: ADR-0175
    scope: "Compromise that the REST-only core patches (staged-create initialization updates, AddSchema.last_column_id, view API) remain and that NovaRocks stage and view requests use their types. Session authority, request split, evidence mapping, attribution and redaction stay effective"
date: 2026-10-14
provenance:
  - "discussion: 2026-10-12 exit of the REST-only core patches (staged-create initialization, add-schema last-column-id, view API)"
  - "discussion: 2026-10-14 ruling that REST services based on Iceberg Java 1.8 and earlier are unsupported"
  - "discussion: 2026-10-14 review of view replacement: version-ID reuse after history expiry, and semantic validation before dispatch"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1220"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/staging/initialization.rs (staged_create_updates)"
  - "novarocks/connector/iceberg/src/commit/staging/engine.rs (validate_initialization)"
  - "novarocks/connector/iceberg/src/catalog_control/catalog_mutation.rs (schema_change_commit, scalar_integer_schema_updates)"
  - "novarocks/connector/iceberg/src/catalog/view_request.rs (ViewCreate, ValidatedViewReplace, existing_schema_id, validate_history_entries, OPERATION_ID_SUMMARY_KEY)"
  - "novarocks/connector/iceberg/src/rest_protocol/mutation.rs (ViewVersionWire, ViewUpdateWire, ViewRequirementWire, CreateViewRequest, CommitViewRequest::encode)"
  - "novarocks/connector/iceberg/src/catalog_control/views.rs (create_view, locally_refused, load_view)"
  - "novarocks/connector/iceberg/src/catalog/mod.rs (views_unsupported)"
  - "novarocks/connector/iceberg/src/catalog/hadoop.rs (NovaRocksHadoopCatalog view_exists, list_views, load_view)"
  - "novarocks/connector/iceberg/src/catalog/hive.rs (NovaRocksHiveCatalog view_exists, list_views, list_views_for_request, load_view)"
  - "vendor/iceberg-0.11.0/PATCH.md"
---

## 问题

NovaRocks 发给 Iceberg REST catalog 的请求内容（staged create 的初始化、schema 变更、view 的建与替换）由谁决定、按什么规则构造，最低支持什么样的服务端？

## 背景与执行事实

[ADR-0175](ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md) 规定全部 REST 变更请求由 NovaRocks 用会话绑定发出，并按证据映射归类结果；vendor 政策见 [ADR-0174](ADR-0174-vendored-iceberg-baseline-and-patch-policy.md)：vendor 只删不增，补丁由消费 owner 退出。本条回答「请求里写什么」。vendor core 现在只保留 P2、U3、P7a、U4、U8（以 `vendor/iceberg-0.11.0/PATCH.md` 为准），没有只为 REST 服务的补丁。

**词汇与 owner。**

| 请求内容 | 构造 owner | 使用的类型 | 不做什么 |
|---|---|---|---|
| CTAS staged create 的初始化 updates | `commit::staging::staged_create_updates`：staged metadata 的纯函数 | 上游 `TableUpdate` | 不从原始建表请求推导；不用上游 `TableMetadataBuilder` |
| 初始化 updates 的校验 | staging 引擎 `validate_initialization` | 上游 `TableUpdate` | 不读 `last-column-id` |
| ALTER 的 schema 变更 | `schema_change_commit` | 上游 `TableUpdate::AddSchema`（rc.2 原样，无 `last-column-id`）+ `LastAssignedFieldIdMatch` | 不另设旁路携带高水位 |
| view 的建与替换的语义校验 | view 控制层：`ViewCreate::validate`、`ValidatedViewReplace::validate` | 加载到的 `ViewMetadata` + 本次属性 | 不经上游 view builder |
| view 请求的 wire 形态 | REST 协议层 `rest_protocol/mutation.rs` | NovaRocks 的 `CreateViewRequest`、`CommitViewRequest`、`ViewVersionWire`、`ViewUpdateWire`、`ViewRequirementWire` | 不做语义判断，只编码、派发与证据分类 |
| Hadoop、HMS 的 view 读取 | 各 catalog owner，`views_unsupported` | `ConnectorError(Unsupported)` | 不调用 SDK，不经列举准入 |

上游类型在 view 路径上只用于 `Schema` 的 serde 与回应解码（`ViewMetadata`）。

**`last-column-id` 的外部事实。**

| 实现 | `add-schema` 的 `last-column-id` |
|---|---|
| REST 规范 | 弃用、待删除；缺省时由服务端计算 |
| Iceberg Java 1.9 及以上（服务端） | 解析时忽略；应用时取「旧高水位」与「新 schema 最大字段 ID」中的较大者 |
| Iceberg Java 客户端 | 只发送新 schema 的最大字段 ID |
| iceberg-rust | 0.4.0 起删除该字段；builder 同样取较大者 |
| 基于 Java 1.4–1.8 的服务端 | 缺省值当作新 schema 的最大字段 ID，并要求不低于表的高水位：删除最大字段的变更被拒绝，此后不新增更高字段的变更也被拒绝 |
| 基于 Java 1.3 及更早的服务端 | 拒绝任何缺少该字段的 `add-schema`，因而拒绝全部 schema 变更与 CTAS |

**staged create 的事实。**

- staged 回应还不是表：它的权威 metadata 要随首次 `assert-create` 提交一起发出。只有「空表的初始状态」能被 REST updates 精确表达：序列号为 0；没有快照与当前快照（元数据规范化只接受指向已有快照的 ref，没有当前快照时不允许 `main`，所以这也证明没有 ref，不需要 U3 的 `refs()`）；快照日志、metadata 日志、统计、分区统计与加密 key 为空；`next-row-id` 为初始值；恰好一个 schema、一个分区规格、一个排序；分区高水位等于默认分区规格的最大字段 ID，未分区时为 999。初始序列号 0 与 999 在上游是 crate 私有常量，由连接器定义。
- Java 服务端按 `upgrade-format-version` 选择新表的 format version，缺省时按服务端默认版本建表；Java 客户端总是发送它。
- 上游 `TableMetadataBuilder` 面向「从请求建表」：生成新 UUID、重新分配字段 ID、不发 `assign-uuid` 与 `upgrade-format-version`、用 `-1` 选择器，表达不了「照搬服务端给出的 metadata」。

**view 的事实（容易踩）。**

- **上游 rc.2 没有可用的 view 请求词汇。** 没有 view 的 requirement 与 commit 类型；`ViewRepresentations` 没有公开构造器；上游 `ViewUpdate::AddSchema` 仍带 `last-column-id`。上游 view metadata builder 在新 schema 与已有 schema 相同时仍记录一条 `add-schema`，随后的版本指向 `-1`；Java 服务端遇到已存在的 schema 不记「本次新增的 schema」，以 400 拒绝。NovaRocks 按位置分配 view 字段 ID，所以「只改 SQL、不改列」的替换必然遇到这种情形。
- **服务端复用 schema 的规则**：字段结构与标识字段都相同。view 没有删除 schema 的 update，Java 也从不清理 view 的 schema，所以 schema ID 与 schema 的对应不变；但已存在的 schema 不能用 `-1` 引用。
- **版本 ID 不是稳定的定义身份。** Java 服务端分配新版本 ID 时：与某个已有版本等价（比较内容与 summary）则改用它的 ID；否则取请求的 ID 与「当前保留版本最大 ID 加一」中的较大者。历史按 `version.history.num-entries` 只保留当前版本与 ID 最大的若干版本，当前版本 ID 较小时，较大的 ID 被清理，之后会被再次分配。`set-current-view-version` 只按数字查找。REST 规范为替换只定义了 `assert-view-uuid`，而 UUID 不随版本变化。因此「加载时看到的数字 ID」在派发时可能指向另一份定义。Java 自己的客户端在替换为等价历史版本时同样按旧数字 ID 设为当前，「与参考客户端一致」不证明这样安全。
- **折叠会让 `-1` 失效。** 新版本若与已有版本等价，服务端把它折叠进那个版本，「本次新增的版本」为空，`set-current-view-version -1` 以 400 被拒。
- **服务端在提交时复查语义**：方言丢失与无法解析的保留数，Java 参考 fixture 返回 500；不大于 0 的保留数返回 400。按 ADR-0175 的证据映射，派发之后的这些拒绝都是结果未知。
- **属性的解析规则以 Java 服务端为准，与 rc.2 不同。** `replace.drop-dialect.allowed` 只有不区分大小写的 `true` 为真（rc.2 另把 `t`、`1`、`on` 当作真）；`version.history.num-entries` 必须是大于 0 的 32 位十进制整数（rc.2 builder 只拒绝负数）。方言保留按「已有属性加本次设置的属性」判断。
- **Java REST 服务端建 view 时不采用请求中的版本 summary**，版本 1 的 summary 由服务端生成。所以 NovaRocks 建的 view 回读时没有 `novarocks.source-format`，第一次替换之后才有；读取与展开不依赖它。
- **Hadoop 与 HMS catalog 不支持 view。** 调用方把 Unsupported 读作「没有这个 view」（SELECT 的 view 展开、`external_view_exists`）；`DROP DATABASE FORCE` 需要权威列举，view 列举不受支持时在任何破坏性操作之前拒绝，不把它当作空列表。

**正例锚点。** `ValidatedViewReplace` 只能经 `validate` 构造：类型上必须带 UUID 与已完成的语义检查，REST owner 收不到别的东西；`CommitViewRequest::encode` 是只做编码的纯映射。改 view 替换前先读这两处。

## 考虑过的选项

**`add-schema` 的 `last-column-id`**

- **A（选中）：从不发送，支持下限为 Iceberg Java 1.9 的语义。** 与 iceberg-rust、Java 1.9 以上客户端和规范一致；提交模型（[ADR-0171](ADR-0171-commit-operation-model.md)）只携带上游 `TableUpdate`，没有弃用字段。代价是旧服务端拒绝请求（见妥协）。
- **B：在请求编码时注入真实高水位。成本否决。** 能兼容基于 Java 1.8 及更早的服务端，但要在冻结请求旁另设一条承载该值的旁路，并长期携带规范已弃用的字段，比参考 Java 客户端还多一层兼容；项目没有依赖旧服务端的用户。出现必须支持的旧服务端时回头。
- **C：保留 vendor 补丁 U2。设计否决。** 违反 vendor 只删不增与消费方退出（ADR-0174）；上游已删除该字段。

**staged create 的初始化**

- **D（选中）：连接器从 staged metadata 推导，只用上游公开 API。** 检查与输出顺序固定，各 format version 都发 `upgrade-format-version`。
- **E：用上游 `TableMetadataBuilder` 生成 changes。设计否决。** 它会生成新 UUID、重新编号字段，表达不了「照搬服务端 metadata」。
- **F：保留 vendor 补丁 P8。设计否决。** 只有 NovaRocks 一个调用方，属于只为 REST 服务的 core 补丁。

**view 请求词汇**

- **G（选中）：NovaRocks 拥有 view 请求的全部 wire 类型，按 REST 规范构造。** 代价是自己维护 view 的 update 词汇，但只有建与替换用到的几种。
- **H：沿用上游 view builder 生成 changes，只用 serde 构造 representations。设计否决。** builder 在 schema 不变时生成的 updates 被参考服务端拒绝；上游也没有 requirement 与 commit 类型。

**view 替换选择哪个版本**

- **I（选中）：每次新增一个携带完整目标定义与操作身份的版本，以 `-1` 选择。** 结果总是本次请求的定义，与历史 ID 无关。代价是相同定义的替换也增加历史版本。
- **J：与已有版本等价时按数字 ID 设为当前（参考 Java 客户端的做法）。设计否决。** 版本 ID 在历史清理后被复用，`assert-view-uuid` 发现不了，可能选中另一份定义。
- **K：只补发 `add-view-version`，不带操作身份。设计否决。** 服务端把它折叠进等价版本后 `-1` 为空，请求以 400 被拒。

**语义校验的位置**

- **L（选中）：view 控制层在派发之前校验，REST 协议层只编码。** 本地拒绝是确定的失败、无外部效果；服务端仍以自己的状态复查，本地检查不替代它。
- **M：交给服务端拒绝。设计否决。** 本可在本地判定的错误要等派发之后才出现，按证据映射成为结果未知。
- **N：保留上游 builder 只用来做检查。设计否决。** changes 已由 NovaRocks 构造，保留它等于维持第二套语义；它的属性解析规则又与 Java 服务端不同，会出现「本地放行、服务端拒绝」。
- **O：为满足方言保留，把旧方言的 SQL 复制进新版本。设计否决。** 定义改变之后，旧方言的 SQL 未必与新定义等价。

**不支持 view 的 catalog**

- **P（选中）：owner 直接回答 Unsupported，类别全程保持。**
- **Q：经 SDK `Catalog` trait 的默认回答。设计否决。** 那些 trait 方法本身就是被删除的 View API 补丁。
- **R：回答「不存在」或空列表。设计否决。** 与权威的「这里没有 view」无法区分，`DROP DATABASE FORCE` 会把它当作完整列举。

## 裁决

REST 请求的内容由连接器按 REST 规范与参考服务端的规则决定；表的请求使用上游公开的 `TableUpdate`、`TableRequirement`，view 的请求使用 NovaRocks 自己的 wire 类型；vendor core 不为 REST 保留或新增任何补丁。固化为以下规则。

### 开发规则

1. **请求词汇归连接器。** REST 请求的 updates、requirements 与请求体由连接器构造。表请求只用上游公开的 `TableUpdate`、`TableRequirement`，不为某个字段在冻结请求旁另设旁路；view 请求只用 `rest_protocol/mutation.rs` 中的 NovaRocks 类型。需要上游没有的词汇时扩展 NovaRocks 类型，不改 vendor。
2. **`add-schema` 从不带 `last-column-id`。** 包括 CTAS 的初始化、ALTER 的 schema 变更与 view 的 `add-schema`。新字段从表的 `last_column_id + 1` 编号；高水位由服务端取较大者；`assert-last-assigned-field-id` 以旧高水位防止并发的 schema 变更。
3. **支持下限：Iceberg Java 1.9 的语义。** 受支持的 REST 服务端必须像 Java 1.9 及以上那样自行计算 `last-column-id`。基于 Java 1.8 及更早版本的服务端不在支持范围内；不探测服务端版本，不写兼容分支。
4. **staged create 的初始化照搬服务端 metadata。** `staged_create_updates` 是 staged metadata 的纯函数：先检查空表的初始状态（见上文清单），任一不成立即拒绝，不近似；再按固定顺序发出 `assign-uuid`、`upgrade-format-version`、`set-location`、`add-schema`、`set-current-schema`、`add-spec`、`set-default-spec`、`add-sort-order`、`set-default-sort-order`、`set-properties`。`upgrade-format-version` 对每个 format version（包括 v1）都发出。stage 已派发，推导失败报告为结果未知。staging 的 `validate_initialization` 要求同样的形态。
5. **view 替换总是新增版本。** 新版本携带完整的目标定义；summary 带本次新铸的 `novarocks.operation-id`（UUIDv7），使它与任何已有版本都不等价；版本 ID 取加载到的最大 ID 加一（只是下限，服务端可取更大值）；以 `set-current-view-version -1` 选择它。**从不按数字 ID 引用已有版本**，即使新定义与某个历史版本相同。建 view 时版本 ID 为 1，不带操作身份。
6. **schema 引用按服务端的复用规则。** 新 schema 与某个已有 schema 的字段结构与标识字段都相同时，版本按那个 ID 引用，不发 `add-schema`；否则发 `add-schema`，版本的 schema ID 为 `-1`。规则偏离服务端会重新出现 400 拒绝。
7. **替换的 updates 形态固定。** 依次为：有属性变化时的 `set-properties`、需要时的 `add-schema`、`add-view-version`、`set-current-view-version -1`。requirement 只有取自加载结果的 `assert-view-uuid`。建 view 的请求体是名字、schema、版本与属性，不带 location（由服务端分配）。
8. **语义校验在派发之前，由 view 控制层完成。** `ViewCreate`、`ValidatedViewReplace` 只能经校验构造。检查项：
   - 方言保留：合并属性（已有加本次）中的 `replace.drop-dialect.allowed` 不为不区分大小写的 `true` 时，当前版本的每个方言（不区分大小写）都必须出现在新版本中；NovaRocks 只写 `starrocks`，所以当前版本含其他方言时拒绝，报错写明当前与新的方言集合以及该属性；
   - 属性约束：合并属性中的 `version.history.num-entries` 若存在，必须是大于 0 的 32 位十进制整数，只接受 ASCII 数字；建 view 同样检查；
   - 可表示性：view 有 UUID；新版本只有一条 `starrocks` SQL representation；schema 引用有效。
9. **本地规则跟随服务端，不跟随 rc.2；有歧义时取更严的一侧。** 本地放行而服务端拒绝会产生结果未知；本地更严只会让请求确定地失败。
10. **本地拒绝是确定的未提交。** 校验失败返回 `KnownUncommitted`，类别 `InvalidRequest`，不派发任何提交请求（之前的加载照常发生）。不要把它归入 `Unavailable`。
11. **REST 协议层只编码。** `CommitViewRequest::encode`、`CreateViewRequest::encode` 把已校验的操作写成 REST 词汇；派发与证据分类沿用 ADR-0175，不在这一层重做语义判断。
12. **不支持 view 的 catalog 由 owner 直接回答。** Hadoop、HMS 的 `view_exists`、`list_views`、`load_view` 返回 `Unsupported`，正文为「`<操作> is not supported by this catalog`」，不调用 SDK、不经列举准入。SPI 的 `load_view` 保留 owner 给出的类别；只有本模块自己产生的文本错误（view 已损坏、方言不支持等）才按文本分类。

### 调试规则

- **view 替换以 400 「Cannot set last added schema」被拒**：本地的 schema 相同判断与服务端规则发生了偏离（规则 6），先比较字段结构与标识字段。
- **`set-current-view-version -1` 被拒**：检查新版本的 summary 是否带 `novarocks.operation-id`；缺少它时服务端会把版本折叠进等价版本。
- **替换在本地以 InvalidRequest 失败**：读报错中的方言集合与属性值；按需在同一语句中设置 `replace.drop-dialect.allowed=true`，或修正 `version.history.num-entries`。没有请求被派发，无需核查 catalog。
- **删除最大字段之后 schema 变更或 CTAS 被拒、并报告为结果未知**：先确认服务端是否基于 Java 1.8 或更早；那是不受支持的服务端（规则 3），不是 NovaRocks 的提交缺陷。
- **新建 view 回读没有 `novarocks.source-format`**：参考服务端不采用建 view 请求中的 summary，第一次替换之后才出现；不要据此判断 view 损坏。
- **Hadoop/HMS 上的 view 相关报错**：类别应为 Unsupported；若是 Unavailable，说明某处又把 owner 的错误转成文本重新分类了。

### 对 ADR-0174 与 ADR-0175 的部分替代

| 被替代的内容 | 本 ADR 的处理 | 仍然有效 |
|---|---|---|
| ADR-0174 裁决 3 的保留项「core U2、P8、View API」及其删除条件 | 三项补丁已删除；初始化、`add-schema` 与 view 词汇归连接器（规则 1–8） | 基线可重建；其余 core 补丁（P2、U3、P7a、U4、U8）的消费方删除责任；其余裁决 |
| ADR-0175 妥协「core 中只为 REST 服务的补丁仍在，stage 与 view 请求暂时使用它们的类型」 | 已兑现：请求不再使用补丁类型 | 会话权威、请求分工、证据映射、归属与脱敏 |

## 接受的妥协（诚实记录）

- **旧 REST 服务端不受支持，而且失败不是干净的拒绝。** 基于 Java 1.4–1.8 的服务端拒绝删除最大字段的变更以及此后不新增更高字段的变更；基于 Java 1.3 及更早的服务端拒绝全部 schema 变更与 CTAS。它们以 400 拒绝，按 ADR-0175 的证据映射报告为结果未知、不授权清理，可能留下需要人工核查的产物。选择依据是与上游和规范一致、提交模型不设旁路，以及没有依赖旧服务端的用户，不是因为兼容方案做不到（选项 B）。
- **staged metadata 的高水位若大于 schema 的最大字段 ID，建出的表高水位会低于 staged 值。** 字段 ID 由 schema 决定，不会被复用；删除 `last-column-id` 之前，1.9 以上的服务端同样忽略发出的值。
- **相同定义的替换也增加历史版本。** 历史受 `version.history.num-entries` 限制（默认 10），较早的版本因此更快被清理。
- **`assert-view-uuid` 只防止目标被删除后重建**（409，确定未提交），不证明 metadata 在加载之后没有被并发修改。并发替换按后写覆盖前写；另一写入方刚加入相同 schema（`-1` 无效，400）或给当前版本增加了方言（500）时请求被拒，报告为结果未知。NovaRocks 不自动重试。
- **本地属性规则只接受 ASCII 数字。** Java 另接受其他 Unicode 数字；这类值在本地被拒绝，偏差落在更严的一侧。
- **报错正文不再带 SDK 前缀。** Hadoop、HMS 的 view 读取正文由 owner 给出，依赖旧正文的观察与断言须随之调整。
- **自维护 view 词汇。** 规范或参考服务端变化时由连接器跟进，不能自动获得上游的修复。

## 何时重新评估

- **某个必须支持的部署只能使用基于 Java 1.8 或更早的 REST 服务端**（或语义等同的实现）：重新评估选项 B，并设计该值在冻结请求中的承载方式。
- **REST 规范为 view 增加版本身份的前置条件**（例如断言当前版本），或服务端给出稳定的版本身份：重新评估「总是新增版本」，以及历史增长的代价。
- **上游 iceberg-rust 提供可用的 view requirement/commit 类型，且其 builder 不再为已有 schema 生成 `add-schema`**：评估改用上游类型（选项 H）；语义校验的归属不变。
- **参考服务端的 schema 复用、版本 ID 分配、等价判断或属性解析规则变化**：重跑 Java REST 互操作覆盖，同步规则 6、8、9。
- **相同定义替换带来的历史增长成为实际问题**：在不按数字 ID 引用已有版本的前提下重新评估替换策略。
- **某个受支持的服务端返回的 staged metadata 不是空表初始状态，或高水位与 schema 不一致**：重新评估初始化推导的检查与妥协 2。
