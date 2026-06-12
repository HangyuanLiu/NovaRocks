# 分布式执行长期目标架构

日期：2026-06-12
状态：Draft，等待评审
定位：**替代 PR #298 的 9 份并列 spec**，把它们重组为一棵以"减法"为脊柱的依赖树。

> 本文档的目标不是 CI 止血，而是长期系统架构合理。所有结论以"在源头消除分歧"
> 为准绳，而不是"在每个 operator 把分歧和解一遍"。

---

## 0. 为什么重组

PR #298 的 9 份 spec 是 9 个并列的"契约"。逐条核验代码（NovaRocks + StarRocks）
后，结论是：它们不是 9 个对等设计，而是 **1 个真正的地基改动 + 卫星**，并且地基本身
被过度建模了。更重要的是，原 9 份漏掉了两个真正属于长期架构的支柱（planning 类型
决定性、编码层与类型正交），并把若干已经合入或已经修好的代码当成未来工作来设计。

本文用 8 个支柱（pillar）替代，映射关系：

| 原 spec | 去向 |
|---|---|
| s1 execution-schema-contract | **P1**（单一类型权威）。但三类型/平行 `ExecutionSchema`/转换桥全部否决 |
| s2 decimal-semantic-payload | 拆解：decimal 规则 = P1 的一个标量叶子；"为什么会漂移"的根因 = **P2** |
| s3 typed-remote-root-result | **P3**。但新 RPC/`TypedResultBatch`/ChunkPB 全部否决，复用 exchange transport |
| s4 aggregate-state-layout | **P5** |
| s5 complex-type-compatibility | 并入 **P1** 的递归段，不独立成契约 |
| s6 iceberg-write-metadata-descriptor | **P6** |
| s7 unified-write-lifecycle | **P7**，重定为"真正剩余 + 分布式 DML 写" |
| s8 distributed-fragment-schema-property | 并入 **P8**。`FragmentBoundaryContract`/`BoundaryCapabilities`/`transport_schema` 全部否决 |
| s9 layered-distributed-ci-recording | 并入 **P8** |
| —（原 9 份缺失） | **P2** planning 类型决定性、**P4** 编码层与类型正交 |

---

## 1. 根因更正（先看这一节，它改变了哪条支柱最吃重）

### 1.1 decimal 漂移是 NovaRocks 自己制造的，不是 FE 供给不一致

上一轮 review 里我推测根因是"1FE+3BE 下 FE 给每 fragment 的 thrift 类型对同一逻辑
slot 不一致"。**这个推测是错的，现已在代码里证伪。**

真实根因（`src/runtime/exchange.rs:226`）：

```rust
fn exchange_transport_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
        DataType::Decimal256(_, scale) => DataType::Decimal256(76, *scale),
        _ => data_type.clone(),
    }
}
```

发送端**无条件**把 decimal precision 拓宽到 38/76，接收端的 `merge_exchange_field_type`
（exchange.rs:145-160）又 scale-agnostic 地接受任意 Decimal128/Decimal256 对。也就是说，
跨 fragment 的 decimal 类型分歧是 **NovaRocks 自己的 transport 制造的**，FE/standalone
analyzer 都没问题。

**含义（重要）**：真正修复 live distributed decimal 失败的是 **P3**（删掉发送端 widen +
接收端按 descriptor 物化），不是 P2。P2（planning 决定性 / DecimalV3 对齐）仍然要做——
它保证 cross-engine 正确性、保证 standalone analyzer 与 StarRocks 一致——但它**单独不修**
这个分布式 bug。这把"地基"分成了两半：P2 让类型在 plan 期就确定，P3 让运行期只服从。

### 1.2 q25 确实含 ordered `array_agg`（你的笔记是对的）

设计过程中一个对抗代理声称"q25 没有 array_agg"。**这是误判**：它读的是 `tpc-ds/q25.sql`
（TPC-DS 第 25 题，确实没有 array_agg），而你笔记里的 q25 指的是
`sql-tests/aggregate/sql/agg_test_count_distinct.sql` 的第 25 条查询，那里确有
`array_agg(v5 ORDER BY v5 ASC)` 与 `array_agg(DISTINCT ...)`（见 result 文件）。所以 P5
（ordered array_agg 分布式 state）动机成立。

但要注意：该 case 表面错误 `scalar output type mismatch for Decimal128` 的**确切出处**
仍需在实现期对照真实 plan 钉死——它可能在 `array_agg.rs` 的 `ArrayAggValue` 宽度分支，
也可能在 `common::build_scalar_array`（一个 P1 关心的、被保留的函数）。两者修法不同，
不要在没看真实 plan 前就断言 P5 修好了 q25。

---

## 2. 设计原则（减法脊柱）

1. **两类型，不是三类型**：逻辑类型（`TTypeDesc`，已在 `ChunkFieldSchema` 上，
   schema.rs:29）+ 物理类型（Arrow `Field`）。**永远不引入第三个 per-field
   `transport_type`**。
2. **单一权威，原地演进**：演进 `ChunkSchema` 使其成为唯一运行期类型权威；**不**新建
   平行 `ExecutionSchema` + "无损转换桥"（那是 dual-format 债，会漂移）。StarRocks 的教
   训是一个 `RowDescriptor`。
3. **类型决定性在 plan 期建立**：同一逻辑 slot 在每个 fragment 的 descriptor 由构造保证
   相同（StarRocks `DecimalV3FunctionAnalyzer` 在 FE planning 期预定型）。运行期**从不**
   重新推导/拓宽/和解。
4. **接收端权威**：exchange 接收端 decode 时把列物化到自己注册的 descriptor 类型；因此
   下游 sort/agg/concat **永远不会**看到不同 precision 的数组。widen-at-merge 机制是要
   **删除**的，不是 formalize 的。
5. **编码与类型正交**：dict / 低基数 / const / 压缩是 serde 层的"线上表示"，**绝不**表达
   成第三个 Arrow 类型。
6. **无 fallback / 无 dual-path**（CLAUDE.md rule 3，无历史用户）。旧路径**同 PR 删除**，
   不留 default-off flag。歧义处 fail fast（rule 2），不隐式降类型（rule 1）。
7. **一套 typed error enum** 贯穿引擎发出与 CI 分类（不 regex-on-text，不要两套词表）。
8. **拒绝防御性 flag / fingerprint-gate**：Cascades property enforcement 保证每个被发出的
   边界都可支撑，所以 capability bool 是死数据；fingerprint 顶多是 debug-assert，绝不做
   带 recovery 分支的运行期 gate。

> **贯穿全文的反模式警告**：本设计过程中，P1、P5、P4 三个支柱都出现了同一个错误——
> **"加一个新 carrier，再用 assert 和旧 carrier 调和"**。这恰恰是脊柱要消灭的 carry+reconcile。
> 每个支柱必须在**同一改动**里删掉旧 carrier，而不是新增后留旧的等以后清。

---

## 3. 八个支柱

每个支柱给出：目标态、关键 add/change/delete（file:line）、StarRocks 对照、必须先解决
的设计决策（来自对抗校验，标 ⚠️）。

### P1 — 单一运行期类型权威

**目标态**：引擎里只有一个运行期类型模型和**一个**递归类型关系。`ChunkFieldSchema`
（schema.rs:28，已携带 `type_desc: Option<TTypeDesc>` + `children`）演进为权威；新增一个
模块 `src/exec/chunk/type_relation.rs`，定义：

- `enum CompatibilityPolicy { ExactArrow, SameScaleWiden, AssignableLogical }`
  - `ExactArrow`：Arrow `==` + 递归 children（StarRocks `operator==`，type_descriptor.h:246）。
  - `SameScaleWiden`：**唯一**允许 precision 差异的策略——decimal 同 scale、同物理宽度内取
    较大 precision（128↔128、256↔256，**绝不** 128↔256）；timestamp 任意 unit/tz；
    utf8↔binary。
  - `AssignableLogical`：StarRocks `is_assignable`（type_descriptor.h:227）的类比，作用在
    逻辑 `TTypeDesc` 上，用于逻辑身份（而非 Arrow 身份）为契约的边界。
- 一个 `relate(expected, actual, policy) -> Result<TypeRelation, TypeMismatch>` 单函数体递归：
  标量叶子（decimal/timestamp/utf8-binary/primitive）、struct **按位置**（zip children，
  绝不比 field name——struct_column.h:194 不序列化 field_names）、list（List≠LargeList）、
  map（entries struct + `ordered` flag 相等）。累积 nested path 进错误。

**collapse 的 5（实为 6）份重复 helper**：`schema_compat.rs`、`schema.rs` 的
`reconcile_chunk_data_type`(~463)、`sort/mod.rs` 私有拷贝(~61)、exchange retag、
coordinator coerce、`array_agg.rs` 的 `reconcile_data_type`(~448，今天缺 Decimal arm 落
`_ => Err`)。

**StarRocks 对照**：`TypeDescriptor::is_assignable`/`operator==` 在一个函数体里递归 children；
一个 `TypeDescriptor` struct 当权威；struct 按位置序列化。

**⚠️ 必须先解决的设计决策**：

1. **`relate` 的签名不可行，必须先定**。`ChunkFieldSchema` 只存 `type_desc + children`，
   **没有 per-node 的 Arrow 类型**；物理 `Field` 只挂在顶层 `ChunkSlotSchema` 上。而被删的
   5 个函数全部作用在 Arrow `&DataType` 上（List/LargeList、Utf8/Binary、Decimal128/256 这些
   物理区分**只活在 Arrow 层**）。二选一并写死：
   - (a) `relate`/`merge` 作用在 Arrow `&DataType` 上，`TTypeDesc` 只用于 JSON-逻辑覆盖——
     并**放弃**"作用于 ChunkFieldSchema"的措辞；或
   - (b) 把 per-node 物理 Arrow 类型下沉进 `ChunkFieldSchema`，记为"单一权威的物理补全"
     （仍一个 struct，但 top-to-bottom 物理完整）。

   不要发布一个"签名收 ChunkFieldSchema、函数体却需要它不持有的嵌套 Arrow 事实"的关系。
2. **逐一清点现有 tolerance arm**，每条归类为 保留为策略 / 删除（死代码）/ 修源头：
   `List ↔ Struct[len==1]` 折叠（schema_compat.rs:54、exchange.rs:122/171）、PARTITION-TOP-N
   opaque-binary 接受 numeric（exchange.rs:1383）、CTE `wire_ids` 位置回退（exchange.rs:1349）、
   decimal scale-agnostic vs scale-strict 分歧（schema.rs:411 vs schema_compat.rs:31）。今天有
   三条无人处理；"单一关系"只有在可证覆盖或刻意丢弃每一条时才成立。
3. **decimal scale 收紧是一次显式行为变更，不是"折叠"**。`SameScaleWiden` 把 exchange 和
   schema 路径从 scale-agnostic 收紧成 scale-strict。列出可能因此变红的现有绿套件，并与 P2
   确认 plan 期已预定型，使收紧是对齐而非回归。
4. **nullability 规则只能有一个**。今天 `reconcile_chunk_field_to_field`(schema.rs:441)、
   `merge_exchange_field`(exchange.rs:201) 用 `nullable = expected||actual`(OR)，而 array_agg
   reconcile(array_agg.rs:419) 是 actual-wins。一个 `merge()` 体不能同时复现两者。选一条
   （建议 OR），并对 array_agg 验证。**这条直接关系 q94，见 §6**。
5. **`merge(SameScaleWiden)` 在终态删除，不留作 bridge**。P3 的 decode-物化契约保证算子内
   concat 只看到同-descriptor chunk，没有东西可 widen；终态只留 `relate()`（检查），不留
   `merge()`（widen 生产者）。若确有算子内 precision 分歧，点名那个算子。
6. **枚举所有 root 输出边界**（coordinator.rs:521、`src/server/encoding.rs` standalone 路径、
   FE result_buffer），统一施加 `RequiredToNullableAtRoot` fail-fast；不要一处严一处松（那是
   脊柱禁止的 dual-path）。
7. 把 `meta/avro/catalog.rs` 从 schema_compat 调用方列表移除（误报，那是 apache-avro crate）。
   真实调用方：aggregate/mod.rs、hashjoin×2、struct_expr.rs、struct_func.rs、exec/mod.rs。

**对 P8 暴露**：`TypeMismatch { slot_id, name, nested_path, expected, actual, policy, kind }`
作为引擎错误枚举的"类型不匹配"臂。

---

### P2 — Planning / lowering 类型决定性（地基，原 9 份缺失）

**目标态**：每个可跨 fragment 的逻辑 slot 在每个引用它的 fragment 里携带**同一个**
`TTypeDesc`（因而同一个 Arrow `DataType`），在 plan/lowering 期固定。运行期从不重新推导/
拓宽/和解。两条镜像但运行期互不和解的权威：

1. **FE-compatible 路径**：FE 的 `DecimalV3FunctionAnalyzer`（rectifyAggregationFunction /
   rectifySumDistinct）是权威；它发的 thrift 里 SUM/AVG/MULTI_DISTINCT_SUM 的 decimal 已预
   拓宽到 (38,s)/(76,s)、scale 已 clamp。NovaRocks lowering **逐字信任**，**不**从输入数组
   重新推 agg 结果类型。（已核实：`agg_type_signature_from_node`(lower/node/aggregate.rs:240)
   今天就直接读 thrift 的 `ret_type`/`intermediate_type`，不从数组推——FE 路径的决定性
   **大体已存在**。）
2. **Standalone 路径**：`canonical_agg_decimal_type` 作为单一权威，喂给 analyzer + codegen +
   runtime spec builder。

**关键 change**：`SUM`/`MULTI_DISTINCT_SUM` 算子 output_type 今天停在
`Decimal128(input_p,input_s)`（sum.rs:62），而 analyzer 已推到 `Decimal128(38,s)`
（functions.rs:1417）——对齐二者。审计 `src/sql/types.rs` 的 `decimal_arithmetic_result_type`
对 DecimalV3 规则的完整性。

**StarRocks 对照**：`DecimalV3FunctionAnalyzer.rectifyAggregationFunction`/`rectifySumDistinct`
在 FE planning 期预拓宽，使所有 fragment 的 descriptor 由构造一致。

**⚠️ 必须先解决的设计决策**：

1. **variance/stddev 返回类型先钉死**。设计稿声称"忠实移植 StarRocks → Float64"是**伪造的
   先例**：StarRocks 实际把 variance/stddev 的 decimal 参数强制为 `decimal128(38,9)` 且
   `returnType = argType`（即返回 DECIMAL，不是 DOUBLE，见 `DecimalV3FunctionAnalyzer.java`
   :280-281,491-492）。二选一并记为显式决策：(a) 忠实移植，返回 DECIMAL，更新 golden；或
   (b) NovaRocks 刻意发散保留 Float64，并**删掉**"忠实移植"措辞。**FE 路径上必须信 FE 的
   `ret_type`**（rule 1），不能照搬这个伪造先例去覆盖它。
2. **"本地推导再 assert 对齐 signature"本身就是 carry+reconcile**，要消除。`apply_type_signature`
   今天**已经**用 signature 覆盖 `output_type`/`intermediate_type`（spec.rs:~99）——所以运行期
   对最终类型已是 signature 权威。真正读原始输入 precision 的是 `sum_spec_from_type`(sum.rs:62)。
   终态应**停止本地推导 output/intermediate 类型**（只从 signature/descriptor 读），从而**没有
   东西可 assert**；不要"本地推导 + debug-assert 对齐"。
3. **timestamp / utf8-binary 的决定性来源要先指明**，再删 `is_compatible_signature_type` 等里
   对应的 catch-all arm。证明它们 plan 期确定（指出 canonicalization 点，如 `arrow_type_from_desc`
   固定 timestamp unit），或显式交给 P1/P6——不要凭信念删 guard。
4. **PARTITION-TOP-N opaque-binary 通道**（exchange.rs:1383-1421，numeric 列合法流入 avg/hll/
   percentile 的 VARBINARY 中间态 slot）是**优化器刻意发出的**类型 erasure，不是 planning bug。
   在删任何 exact-type assert 前，要么与 P5/优化器协同让该中间态 slot descriptor 在 plan 期匹配，
   要么把它显式排除在 P2 的删除范围外。否则"对任意类型分歧 fail fast"会硬失败一个今天能跑的路径。
5. **window/analytic 聚合的 decimal 决定性**：`TAnalyticNode` lowering 可能不走
   `infer_agg_function_types`，新 fail-fast 可能在窗口 decimal 聚合上误触。在 P2 范围内扩展
   `canonical_agg_decimal_type` 覆盖 analytic 路径，或记为显式 backlog。

---

### P3 — 接收端 descriptor 权威的 exchange + typed root result

**目标态**：exchange 接收端是列类型的唯一权威。每个 decode 出来的列被物化到接收端**注册的
descriptor**（`expected_chunk_schema`，P1 权威）推出的 Arrow 类型，**绝不**从 wire 推断。
因为 P2 保证同一逻辑 slot 在每个 fragment 的 `TTypeDesc` 相同，物化是 buffer-retag，不是
value cast。

- **wire 格式**：保留 Arrow IPC `StreamWriter`/`StreamReader`（exchange.rs:1230/1268），
  **不**引入 NovaRocks ChunkPB / 新 RPC / `TypedResultBatch` / fingerprint。改变的是语义：
  接收端不再信 `batch.schema()`，而是按注册 descriptor 把 buffer 物化到目标类型。
- **root result = 退化的单发送者 exchange**：root fragment 的输出 chunk 走现有
  `encode_chunks`/`decode_chunks_for_sender` 发到 coordinator 本地 receiver；**删除**
  coordinator.rs:625-693 的 MySQL-text 反解、`decode_result_batch_to_chunk`、
  `binary_like_to_decimal128_array`、`coerce_fetch_chunks_to_output_columns`。

**删除**：`exchange_transport_data_type`(226，发送端 widen)、`merge_exchange_field_type`(145)、
`merged_exchange_schema`(1084)、`sort_payload_data_type`(sort/mod.rs:73)。

**StarRocks 对照**：BE 间 `ChunkPB` 只发 slot_id_map/is_nulls/is_consts，接收端从自己的
`RowDescriptor` 推列类型（sender_queue.cpp、protobuf_serde.cpp）。**注意**：StarRocks 的
root result 本身**也是** remote-BE 上 `MysqlResultWriter` 产出的 MySQL-text、FE 逐字节转发——
所以 NovaRocks 走 typed 的理由是"它端到端 Arrow-native 且 coordinator 同构"，**不是**
"StarRocks 用 typed root"。

**⚠️ 必须先解决的设计决策**：

1. **`align_chunk_schema_to_batch`（schema.rs:646/683）是第三份、最深的一份 widen 容忍**，原
   设计两处都漏了它。今天 `Chunk::try_new_with_chunk_schema` → `align_chunk_schema_to_batch`
   会用 batch array 的 field **覆盖** slot 类型（schema.rs:683
   `expected.with_field_and_slot_id(slot_id, field.clone())`）。所以即便接收端物化到了
   descriptor，这一步又会从数组重新推回去。**它必须收紧成 exact-equality**（数组类型必须
   等于 descriptor），否则 P3 的"单一权威"不变量根本没关闭。明确这条归 P1 还是 P3，并记下
   P1→P3 依赖边。**这是 8 份设计里置信度最高的一处具体 bug。**
2. **原子性**：删发送端 widen + 翻转接收端物化 + 收紧 `align_chunk_schema_to_batch` 必须是
   **同一个 commit**，且 `materialize_array_to_descriptor_type` 跑在 `Chunk::try_new` 之前。
   只删 widen 不翻接收端 → 接收端仍等 precision-38 buffer（错结果窗口）。
3. **opaque-binary VARBINARY 通道**（exchange.rs:1383）在 P5+P2 把 avg/hll/percentile/decimal
   中间态 TTypeDesc 预定型前**不能删**；把它做成 P5 落地后联合删除的硬依赖边，不要做成"显式
   typed allowance"（那其实是 recovery 分支）。
4. **CTE `wire_ids` 位置回退**（exchange.rs:1349）在本支柱内解决：要么断言 P2 给 CTE 产/消
   共享 slot 命名空间（删回退），要么指明 descriptor 驱动的消费端 remap。
5. **FE-compat 边界**：root-result-as-exchange 只 scope 到 **standalone coordinator**；显式排除
   FE-relay BE 模式的 MySQL-text `TResultBatch`（rule 3/4）。删 `decode_result_batch_to_chunk`
   前先确认 FE-relay 路径不再需要它。

---

### P4 — 编码层与类型正交（原 9 份缺失；最 greenfield）

**目标态**：编码（全局字典低基数、const、未来 RLE/streamvbyte/lz4 压缩）是 exchange
column-serde 协商的 per-column **线上/表示**属性，**绝不**改变列的 (逻辑 `TTypeDesc`, 物理
Arrow `Field`) 对。正交的 `EncodingDesc` 描述"这块 slot 的字节在某个 chunk 上怎么排"。发送端
编码、接收端解码到 declared 类型（StarRocks `protobuf_serde` 教训），下游 sort/agg/concat
永不见到编码形态的数组。

今天低基数被**误建模成类型**：`src/sql/optimizer/rewrite/rules/low_cardinality_dict` 的
rewrite 直接把 `col.data_type = Int32`（rewriter.rs:218），并有一台 1378 行的 decode-boundary
机器插入 decode。目标是把它收敛到一个 serde seam。

**StarRocks 对照**：`data.proto` 的 `encode_level`、`column_array_serde`，且**没有**
`LowCardinalityColumn` 这种类型——编码是 serde 层的事。

**⚠️ 必须先解决的设计决策**：

1. **物理真相矛盾**：GlobalDict 下 slot 的 Arrow `field` 说 Utf8，而活数组和 IPC payload 是
   Int32 codes。二选一写死：(A) `EncodingDesc` **只活在 `ExchangeWireMeta`**（wire-only，
   不挂 `ChunkSlotSchema`），fragment 内编码数组用诚实的物理类型；或 (B) P1 正式把 `field`
   定义为 decoded 类型，并加一个派生的 `physical_array_type(encoding)` 作为真实线上布局的
   唯一来源。**不要两边都放**。
2. **`ChunkSlotSchema` derive 了 `PartialEq`（schema.rs:214）**。直接加 `encoding` 字段会让
   编码参与相等比较，与"相等忽略编码"契约相反，并可能误触 exchange.rs:1481 的"wire meta 变了"
   fail-fast。要么 encoding 不上这个 struct，要么手写 `PartialEq` 排除它。
3. **P4→P2 的范围越界**：原设计把整台 1378 行 decode-boundary 放置机器甩给"P2 的 Cascades
   property enforcement"，但 P2 只 scope 类型决定性，不是编码能力 property 推导。**先让 P2
   确认其框架能表达 order-preserving-sort 和 count-distinct-over-codes 的安全性**，否则别删那台
   机器；不能表达就在 P4 保留一个更薄的放置机制。
4. **FE 路径**：`TDecodeNode`/`TGlobalDict`（decode.rs:83/591）必须降到**同一个** `ColumnDecoder`
   seam，由 FE 提供的 dict id 驱动，零 standalone 能力协商（rule 1）。在证明能 round-trip 一个 FE
   decode node 前，不删 `lower_decode_node`。
5. **receiver-keeps-encoded 优化**（让接收端保留 codes 以加速 group-by/count）会把
   `register_expected_chunk_schema` 从"单向物化到 declared 类型"变成"declared 类型 + 期望编码"
   的双向协商。v1 先做 always-materialize-at-decode（正确，无低基数加速），把 keep-encoded 留作
   后续里程碑，别隐式塞进 P3 接口。

---

### P5 — 聚合 state layout 端到端

**目标态**：ordered-collection 聚合（array_agg/array_agg_distinct、group_concat/string_agg）的
order-by flags、distinct flag、separator/max_len 作为**结构化字段**从 lowering 一路带进 runtime
kernel。函数名只剩裸名（`"array_agg"`），**删除** `array_agg|a=...|n=...` 的 string↔struct
round-trip（编码 lower/node/aggregate.rs:259、解析 array_agg.rs:159 / group_concat.rs:111）。
merge 阶段按 layout/`intermediate_type` 决定的形状分派，不再 downcast 猜 List vs Struct。

**StarRocks 对照**：`array_agg2` 用 `struct<array<value>, array<order_key>...>` 中间态；order
flags 在 `TAggregateFunction`/function context，不在函数名里。

**⚠️ 必须先解决的设计决策**：

1. **只能有一个 order/distinct carrier**。结构化 flags **已经存在**于
   `AggKind::ArrayAgg{is_distinct,is_asc_order,nulls_first}` / `AggKind::GroupConcat{...}`
   （functions/mod.rs:82-95），且已是 runtime dispatch 权威。**不要**再新增 `AggStateShape` +
   `AggSpec.state_type` 叠在上面（那是三个 carrier 的 carry+reconcile）。要么让结构化 layout 喂
   `AggKind` 并删名字串，要么直接以 `AggKind` 为 carrier。**中间态形状从已有的
   `AggSpec.intermediate_type` 读，不要再存一份 `state_type`。**
2. **重新落地 q25 动机**：删除"本支柱让 q25 错误结构上不可能"的措辞，先看真实 q25 plan 确认
   错误在 `array_agg.rs` 的 `ArrayAggValue` 宽度分支还是 `common::build_scalar_array`（后者是 P1）。
3. **merge_batch 的形状检查做成 debug_assert，不是运行期硬错**（P3 的 descriptor 权威 + P2 决定性
   已保证形状）。
4. **嵌套 order-key 比较的契约要先定**：array_agg 的 ORDER BY 键能否是嵌套（Struct/List），决定
   P1 的 comparator 是否必须保证嵌套比较。这条不能留作 open question。
5. 复用 `common::key_fingerprint` 做 DISTINCT 去重，**显式删除** `encode_scalar_key`
   （array_agg.rs:58），保证只有一个 fingerprint 权威。group_concat 已用 `common::AggScalarValue`，
   它只剩名字串解析这一处债，不要把它当 value-enum 迁移。

---

### P6 — Iceberg 写元数据 descriptor 端到端

**目标态**：writer→coordinator 的 NovaRocks 内部分布式路径**无损**携带 iceberg-rust 的
partition `Struct`，commit collector **直接消费**。只有一个 forward encode 和一个 decode，二者
对 primitive `Datum` payload 互逆。collector **绝不**为自己反解 `partition_path`；
`parse_partition_path` 及其 transform-inversion 家族从内部 commit 路径**删除**。

**立即修复的 bug（partition evolution bucket mismatch）**：`collector.rs:342-347` 读了
`df.partition_spec_id` 却仍用 `&self.partition_spec`（当前表 spec）去 `parse_partition_path`。
evolution 下旧文件按旧 spec 编码，用新 spec 解会错。改为按 `partition_spec_by_id(df.partition_spec_id)`
解析。

**StarRocks 反例**：`TIcebergDataFile.partition_path`，FE 从 path 字符串重建 `PartitionData`
（`IcebergMetadata.java`:1853/1979 用 `nativeTbl.spec()` 当前 spec 误解）。

**⚠️ 必须先解决的设计决策**：

1. **去掉 `primitive_type_tag`**（wire 上的第二份类型）。decode 时类型只从
   `partition_spec_by_id(...).partition_type(schema)` 解析；type 校验降为 debug-assert。否则就是
   脊柱禁止的"冗余类型 carrier + 运行期 recovery gate"。
2. **`written_file_to_sink_commit_info_for_metadata`（data_writer.rs:444）已做 per-file spec 解析**，
   它是第二个 forward-encode 权威。把它折叠成**唯一** forward encode，或删除——否则"一个 forward
   encode"不成立。
3. **FE-compat 边界**：确认是否有部署跑真实 StarRocks FE 经 `partitionDataFromPath` 提交 Iceberg
   写。`partition_null_fingerprint` 是真实 FE 协议字段（`IcebergPartitionData.java`:119）。若 Iceberg
   写是 standalone 独占，直接删 thrift 的 `partition_path`/`partition_null_fingerprint`（真减法）；
   否则三个 partition carrier 只有在确认有外部 FE 消费者时才有理由共存。
4. delete/equality-delete 文件的 partition 也要走同一 encode/decode（content ∈ {Data,
   PositionDeletes, EqualityDeletes}），否则分区 DELETE/UPDATE 静默回归。
5. 确认 ADD FILES 路径产出 `Struct` 而非 path 字符串，再断言可删 `parse_literal_for_type`。

---

### P7 — 分布式 DML 写 + 写生命周期剩余

**事实校准**：~90% 已合入。`IcebergWriteTransactionRunner` 已 wired 进
iceberg_writer/mutation_flow/delete_flow/equality_delete_flow；12 态 `IcebergOperationState`
已存在（iceberg_operation.rs:48）；PR #270/#283 已 cut over append/overwrite/rowdelta。

**真正剩余（长期架构）**：

1. **DELETE/UPDATE/MERGE 的 writer 目前在 coordinator 本地跑**（`run_coordinated_write` 内，
   delete_flow.rs / mutation_flow.rs），**不是分布式**。长期应像 INSERT append 一样把
   data/position-delete/equality-delete writer 作为 BE 上的 distributed sink fragment 运行。
   这是这一支柱里**真正难、真正属于长期架构**的部分。
2. **MV refresh** 仍持有独立生命周期（mv_flow.rs:406），经 legacy 无类型 `run_iceberg_commit`
   本地提交（iceberg_refresh.rs:7905/10945）——接到 runner + `IcebergOperationState`。
3. **退役** legacy `run_iceberg_commit` wrapper，并**删除** `local_writer_commit_input` /
   `new_local_writer_write_id`（write_transaction.rs:204）这个伪造单写者的 coordinator-local shim。

与 D3/D4 fault-ops epic（PR #286，`docs/design/specs/2026-06-10-d3-d4-distributed-fault-ops-design.md`）
对齐：分布式后，恢复 worker 只需扫一张 `iceberg_operation` 表。

**⚠️ 必须先解决的设计决策**：

1. **`build_position_delete_output_schema`（sink.rs:760）硬编码 `[file_path Utf8, pos Int64]`**，
   是 delete 文件的第二个 schema 权威。必须改为 planning descriptor（P6 供给），否则违反 P1 单一权威。
2. **DELETE 分布式的最高正确性风险**：coordinator 今天对每行评估完整 sqlparser WHERE AST（比 bound
   `iceberg::Predicate` 表达力强）。分布式后，BE filter 表达不了的 WHERE 形状必须 **fail fast**
   （rule 2），不能静默漏删（=数据损坏）。删 `scan_for_position_deletes_at` 的逐行 AST 评估前，**先**
   产出"支持/不支持的 WHERE 形状清单 + fail-fast guard"。
3. **承诺强终态**：终态**没有任何** executor 在 coordinator 产文件；`local_writer_commit_input` 等
   **删除**，不是"如无人需要则删"。COW-UPDATE 能否让每个 BE sink 上报足够信息重建
   `CowUpdateRewriteSet`，要在 spec 里给出明确答案。
4. **不要投机加 `IcebergSinkMode::EqualityDeletes`**：`classify_sql_delete_strategy` 今天只返回
   PositionDeleteFiles | DeletionVectors，equality-delete 写是 equality_delete_flow / ADD FILES 的
   coordinator-local 事。先证明有 DML 路径需要它，否则 DELETE/UPDATE/MERGE 只走 position-delete。
5. **修正 StarRocks 先例引用**：原设计引的 `PlanFragmentBuilder.java:1750 getMORParams` 是**错的**
   （那是 scan/metadata 侧 API，不是 sink 构造）；`MergeJoinNode.java` 是 sort-merge join 算子，不是
   MERGE-INTO planner。正确先例是 `IcebergTableSink.java` + `IcebergDeleteSink.java` 协同 + BE
   `iceberg_delete_sink.cpp`。

---

### P8 — 一套 typed error 分类 + 分层 CI 观测

**目标态**：单一引擎错误枚举 `engine::error::EngineError`（新文件 `src/common/engine_error.rs`），
是减法模型创造的每个 fail-fast 站点的唯一词表。携带小而封闭的变体：`TypeDeterminismViolation`
（P2，仅 debug-assert 级 guard，绝无 recovery 分支）、`ExchangeDescriptorMismatch`（P3）、
`AggregateStateLayoutMismatch`（P5）、`IcebergWriteDescriptorMismatch`（P6）、`WriteCoordinatorGone`
（替代 grpc_server.rs:520 的字符串 contains）。CI **逐字读** error code（不 regex-on-text——runner
今天发的是 `value mismatch at row N col M` 这种与根因无关的通用文本）。EXPLAIN 输出 per-boundary
output schema。分层 CI：smoke/targeted/full tier；**已提交进仓库**的 known-failures baseline
（`logs/ci-full` 在 .gitignore 里，`reruns.jsonl --from` 跨 run 不成立）。

**否决**（原 s8）：`FragmentBoundaryContract`、`BoundaryCapabilities`、`transport_schema`——
distribution 已在 `FragmentEdge.output_partition`（codegen/mod.rs:93）、`output_columns`
（codegen/mod.rs:73）；capability 在 Cascades 下是死数据。

**⚠️ 必须先解决的设计决策**：

1. **错误词表必须坍缩成一个，不能叠第四层**。今天已有三套：`TStatusCode`（StarRocks-compat thrift，
   coordinator.rs:2296）、`REPORT_EXEC_STATUS_*` i32（grpc_server.rs:48）、`MetaErrorKind`
   （meta/error.rs:4）。要么 `EngineErrorCode` 成为 standalone 路径的权威 code 并**删掉** parallel
   空间，要么定义到 `TStatusCode` 的单一全映射（FE-compat 路径），不留第三空间。
2. **`Internal(String)` 逃生舱 + grep lint = 又一层 regex**。改成封闭 typed 逃生
   （`Internal { code }`，边界函数不返回 free-form String），用类型保证（如边界 trait 的关联错误类型
   排除 `Internal`）而非 grep。**删掉** `From<EngineError> for String` 这个有损桥（它丢 `code()`），
   否则旧 String 通道永久存在（rule 3）。
3. **EXPLAIN 层级错配**：`explain.rs:714` 在优化器层，只有 `analysis::OutputColumn.data_type`
   （Arrow），**没有** `TTypeDesc`/`ChunkFieldSchema`（在 exec/chunk，lowering 下游）。per-boundary
   类型工件必须从 **lowering/exec 层**输出，不能从 explain.rs；`FragmentEdge` 也不可被 explain.rs 消费。
4. 不要用伪造的 "269 个 Result<_,String>"。实测：exchange.rs=14、coordinator.rs=31、lower=156、
   sort=21。spec 里的数字要有据。
5. **internal vs FE report-status 边界**：`error_code` 只加到 `idl/proto/starust_grpc.proto`，绝不加到
   `FrontendService.thrift` 的 `BatchReportExecStatus`，并说明 `EngineErrorCode` 不泄漏进 FE thrift status。

---

## 4. 依赖树与关键路径

```text
            P8-spine (enum + CI baseline, 纯加, 先落)
                 │
P1-add (type_relation 纯加) ──┐
                              ▼
                        P2 决定性 (keystone)
                              │
                              ▼
                  P3 原子 decode-flip ──────┐
                       │                     │ (P4 编码在 P3 decode 之后)
        ┌──────────────┼───────────┐         │
        ▼              ▼           ▼         ▼
  P1-finalize     P5 agg-state    P3 删 opaque-binary   P4 编码 seam
  (删 widen/merge) (删名字串)      (与 P5 联合)         (gated on P2 property)
        │
        ▼
  P6 (partition descriptor) ──► P7 (分布式 DML + lifecycle 剩余)
        │
        ▼
  P8-population (各支柱 fail-fast 站点发 typed 变体) + EXPLAIN + CI 分类
```

- **关键路径**：`P8-spine + P1-add` → **P2** → **P3 原子翻转** → `P1-finalize + P5` → P4 → P6 → P7
  → P8-population。
- **8 个对抗校验全是 needs-revision，0 个 reject**——没有结构性阻塞。但**每个删除都 gated 在它的
  上游保证落地之后**；把任一 verdict 当成"可以开始删了"就会重新引入加法债。

### 落地阶段

- **Phase 0（纯加，无行为变更）**：P8 落 `EngineError` 枚举 + 提交 baseline + `local-full-ci.sh`
  `--tier`/`--from`；P1 落 `type_relation` + 单一 `AggScalarValue`（含 Decimal256）作为纯新增；
  P4 落 `EncodingDesc` + Plain-only 编解码（no-op 格式 bump）。
- **Phase 1（决定性）**：P2 `canonical_agg_decimal_type` 贯通 analyzer+codegen+spec builder；停止
  本地类型推导（不是"推导 + assert"）；决定性 debug_assert **先过全套**（FE-compat + standalone +
  1FE3BE）**再**进任何删除。
- **Phase 2（接收端权威，原子）**：删 `exchange_transport_data_type` + 翻转 `chunk_schema_for_wire_meta`
  物化到 descriptor + 收紧 `align_chunk_schema_to_batch` 成 exact-equality——**同一 commit**。root
  result 走 exchange，删 text 反解（仅 standalone）。
- **Phase 3（收尾减法）**：P1 删 `sort_payload_data_type`/`merged_exchange_schema`/`merge()` 及 5 份
  reconcile；P5 把 array_agg 收敛到一个 value enum + 结构化 layout（删旧 carrier）。
- **Phase 4（编码 + opaque-binary）**：删 opaque-binary 容忍（与 P5 联合）；P4 编码 wire-only。
- **Phase 5（写路径）**：P6 无损 partition + per-file spec 解码；P7 分布式 DML + 退役 legacy wrapper。
- **Phase 6（观测）**：P8-population + EXPLAIN（从 exec 层）+ CI 分类读 code。

---

## 5. 必须在动手前钉死的全局设计决策（汇总）

1. `relate` 作用层：Arrow `&DataType`（+ TTypeDesc 逻辑覆盖），**不是** ChunkFieldSchema。（P1）
2. `align_chunk_schema_to_batch`（schema.rs:683）收紧成 exact-equality——否则 P3 不变量是假的。（P1↔P3）
3. `merge()` 终态删除，不留 bridge。（P1）
4. nullability 单一规则（OR or actual-wins？），且 sort/TopN 与 root 边界一致。（P1，关系 q94）
5. P4 `EncodingDesc` wire-only（不上 PartialEq 的 ChunkSlotSchema）。（P4↔P1）
6. P2 的 Cascades 能否表达 order-preserving / count-distinct-over-codes 安全性——不能就别把 rewriter
   删了甩给 P2。（P4↔P2）
7. variance/stddev 返回类型：DECIMAL(38,9)（StarRocks）还是 Float64？FE 路径信 FE 的 ret_type。（P2）
8. 停止本地类型推导，而非"推导 + assert"。（P2）
9. `build_position_delete_output_schema`（sink.rs:760）改为 planning 供给。（P7↔P6↔P1）
10. `EngineErrorCode` 必须坍缩三套现有词表，并删 `From<EngineError> for String`。（P8）
11. EXPLAIN per-boundary schema 从 lowering/exec 层输出，不从 explain.rs。（P8↔P1）
12. FE-compat vs standalone 边界：P3 root-result / P4 TDecodeNode / P6 partition_path / P7 RESULT_SINK
    / P8 report-status——每个删除前画清线（rule 3/4）。
13. Iceberg 写是否 standalone 独占？决定 `partition_path`/`partition_null_fingerprint` 是否整体删除。（P6/P7）

---

## 6. 分布式正确性 backlog（不属于类型脊柱）

8 个支柱是一个**类型权威 + 写生命周期**的纲领。下面这些是**算子级/调度级/虚拟列级**的正确性问题，
没有任何类型支柱覆盖；它们应进入一个"分布式正确性 backlog"，按谁恰好碰到对应算子谁加回归用例，
**不**阻塞类型脊柱：

- **`join_full_outer_with_using` 行序**：build-side-unmatched 行在 `hash_join_probe_core.rs` 经
  `merge_join_outputs` 追加，跨 shuffle 相对顺序非确定。纯算子/exchange 排序问题，与类型无关。
- **tpc-ds q93 timeout**：left-outer + cross-join-via-where 膨胀，是调度/吞吐/基数估计问题，不是类型
  分歧。**风险**：若误归到 P2 会被"修复"但仍超时。
- **q94 TopN nullability**：**注意——这条与脊柱耦合**。今天它被 `merged_sort_schema_for_chunks` 的
  `nullable = expected||actual||null_count>0`（sort/mod.rs:133）掩盖，而那正是 P2/P3 要**删**的函数。
  删它会让 q94 由绿变红，**除非** P1 的 nullability 调整策略（§3 决策 4）在 TopN/sort 边界显式生效，
  不只在 root。**这是整个计划里"删除会留下一个本来能跑的 case"最具体的风险**，必须作为 P1+P3
  nullability 契约下的显式命名回归用例。
- **`__change_op`（IVM/CDC 控制列）**：`hdfs_scan.rs` 合成的虚拟 TINYINT，随 scan 输出跨 exchange/TopN。
  P3 翻成严格 descriptor 物化后，这类可能没有稳定 FE `TTypeDesc` 的合成虚拟列正是会误触
  `ExchangeDescriptorMismatch` 的。需要在 P2/P3 范围内**审计虚拟/控制列（`__change_op`、`__op`、
  `_file`、`_pos`、row-position）是否携带确定性 descriptor**，不要默认它们有。
- **列级 schema evolution**（增/删/改名列、跨 snapshot 类型 promotion）：P6 只解决 partition-spec
  evolution（按文件自己的 spec_id 解码）。读侧列级 schema evolution（`scan_planner.rs`/`read.rs`/
  `schema_update.rs`）无人拥有——独立 backlog 或 P6/connector 范围扩展。

---

## 7. 验证策略

- **行为变更会动 golden**：decimal scale 收紧、`RequiredToNullableAtRoot`、text-result 删除、variance
  类型决定——重录 golden **必须对照 StarRocks**，尤其 `iceberg-compatibility`（Spark 读 NovaRocks 写）
  是 silent-wrong 的 catch-net（P4 错字典、P7 未排序 position-delete 这类自测能过、Spark 读会挂）。
- **P2 决定性 assert 先过全套再删**：FE-compat + standalone + 1FE3BE。assert 漏掉某个非规范产出
  （window/analytic agg、grouping sets、avg-distinct rewrite）会把下游所有 fail-fast 从"静默能跑"翻成
  "硬失败"。
- **P3 原子性**（§3 决策 2）是 wrong-results 窗口的来源，必须单 commit。
- **新增 fail-fast 的超时影响**：把静默 widen 变硬错，可能让原本慢但绿的查询变快失败或反之；
  golden re-baseline 政策纳入 P8 的 committed known-failures。

---

## 8. 一句话总结

把 PR #298 的"加法契约群"换成"**减法**"：**两类型 + 一条 plan 期决定性规则 + 一个收敛的
比较器 + 一条 typed transport + 一套 typed error**。StarRocks 引用得对的那条（descriptor 权威 +
FE planning 期 decimal 预定型）是减法设计——它让坏状态不可能出现；原 9 份是加法——它让坏状态成为
一等公民再到处和解。长期合理 = 在源头消除分歧，而不是更多机制。
