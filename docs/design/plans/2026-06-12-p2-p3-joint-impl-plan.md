# P1-remainder + P2 + P3 联合实现计划（已并入对抗校验修正）

日期：2026-06-12
依据：`docs/design/specs/2026-06-12-distributed-execution-target-architecture.md`
目标：决定性（P2）+ 接收端 descriptor 权威（P3）一起做，避免返工。**减法**：删分歧而非到处和解。

## 已确定的关键事实 / 决策
- decimal 漂移根因 = NovaRocks 自己的 sender widen `exchange_transport_data_type`（exchange.rs:226），非 FE。
- canonical 规则（analyzer functions.rs:1417/1421-1434 权威）：`sum`→`Decimal128(38,s)`；`avg`→`Decimal128(38, s≤6?s+6 : s≤12?12 : s)`；`variance/stddev`→`Float64`（保持，记为对 StarRocks `Decimal(38,9)` 的刻意 divergence，faithful 版延后）。
- **decimal-output 的推导点有四个，不是三个**：analyzer、standalone codegen、SUM runtime spec、**avg/multi_distinct_sum runtime spec**。删 tolerance 前四个都要对齐。
- relate 已落地（type_relation.rs），scale-strict；今天 exchange/spec 的 tolerance 是 scale-agnostic，统一会**收紧**。
- q94（tpc-ds TopN nullability）**当前是红的**（commit 7c064b86 known issues）。本工作可能**修好**它（nullability-OR 显式落到 sort/TopN 边界），不是"保持绿"。执行 Step 13 前先实证 q94 在 standalone-single vs distributed 的状态。

## 原子分组
- **GROUP A = Step 10+11+12 同一提交**：align 改 relate(ExactArrow)+保留 descriptor field（schema.rs:669,683）+ 接收端 retag-to-descriptor（chunk_schema_for_wire_meta）+ 删 sender widen + 删 encode 端 merge 家族。拆开 = 错结果窗口。
- Steps 1-9（含 6b）必须**全部先于** GROUP A（决定性是 ExactArrow 收紧安全的前提）。
- Step 13（sort）单独提交，但内部原子：删 sort widen + nullability-OR 落到边界 同时落，q94 不能中间变红。
- Step 14（root-as-exchange）单独提交，standalone-only。

## 有序步骤（RED 测试先行）

1. **[P2] `canonical_agg_decimal_type(agg, &DataType)->Option<DataType>`**（src/sql/types.rs，纯加）。sum/multi_distinct_sum→(38,s)；avg→(38,avg_scale)；非 decimal/非这些 agg→None。variance 不在此（已 Float64 确定）。**仅 Decimal128**（analyzer 只有 128 臂；Decimal256 不 canonical，各 caller 保留自己 256 臂）。
2. **[P1] `merge_fields_nullability(&Field,&Field)->bool` = OR**（type_relation.rs，纯加）。注释：array_agg.rs:419 actual-wins 是 OUTLIER，延后到 P5，勿动。
3. **[P1] `retag_column(&ArrayRef, target:&DataType)->Result<ArrayRef,TypeMismatch>`**（type_relation.rs，纯加）。metadata-only：identity / decimal 同 scale 改 precision（i128/i256 buffer retag）/ Utf8↔Binary（同物理布局）/ 递归 List·LargeList·Struct·Map。**timestamp 单位差等非元数据可表达的差异 → Err**（post-P2 不应出现）。
4. **[P2] analyzer `infer_agg_return_type` 委托 canonical**（functions.rs:1407-1437）。sum/avg 的 Decimal128 臂改为调用 canonical；断言与 canonical 同值（无 drift）。无 golden。
5. **[P2] standalone codegen `infer_agg_function_types` 委托 canonical**（expr_compiler.rs:2744-2885）。sum/avg/multi_distinct_sum 的 decimal 臂走 canonical。golden：仅当某 standalone agg 之前报 (p,s) 而非 (38,s) 才重录（多数 no-op）。
6. **[P2] SUM runtime spec 对齐**（sum.rs:62-67）：Decimal128 臂 → `canonical("sum",..)`；Decimal256 臂保持 `Decimal256(76,s)`。
6b. **[P2] avg runtime spec 对齐（对抗校验新增，关键）**（avg.rs:62-75 `avg_spec_from_input_type`）：output_type/intermediate 改为 canonical avg scale（不再用 input scale）。**审计所有 decimal-output 的 runtime `build_spec_from_type`**（multi_distinct_sum.rs:271 等）一并对齐。**必须在 Step 8 之前**，否则 Step 8 用 scale-strict relate 会 fail `avg(decimal)`。RED：`avg_spec_from_input_type(Decimal128(10,3))` pre-override output scale == 9。
7. **[P2] `apply_type_signature` 加 debug_assert**（spec.rs:99 后）：`debug_assert!(relate(pre_output, signature_output, SameScaleWiden).is_ok())`。仅验证，非 runtime gate。
8. **[P2] 删 `is_compatible_signature_type`（spec.rs:42-73），call site 换 `relate(.., SameScaleWiden)`**。**policy = SameScaleWiden**（非 ExactArrow——multi_distinct_sum runtime precision≠38，ExactArrow 会误伤）。gated on Steps 4,5,6,6b 全落（P2-lock）。opaque-binary 的 `validate_state_combinator_binary_signature` 不动。
9. **[P2] window/analytic：验证（非重导）**（analytic.rs）。analytic.rs:315 已从 descriptor 读 ret_type 且无 fallback 分支；standalone 的 window ret_type 由 codegen（Step 5）经 canonical 产出，**已 canonical-by-construction**。**不要在 analytic.rs 加重导**（违反 rule 1）。本步=加测试/断言确认 codegen 窗口 agg ret_type 走 canonical，no-op 即可。
10. **[P1, GROUP A] `align_chunk_schema_to_batch` 改 relate(ExactArrow) + 保留 descriptor field**（schema.rs:669 用 relate；:683 改为 `expected.field().clone()` 而非 batch array field——堵第三处 widen 洞）。nullable 用 merge_fields_nullability。
11. **[P3, GROUP A] 删 sender widen + 接收端 materialize-to-descriptor**（exchange.rs）。chunk_schema_for_wire_meta：定位 expected_slot 后 `retag_column(col, &expected_arrow_type)` **再**建 slot；decode 出 descriptor-typed batch。删 `exchange_transport_data_type`(226)。opaque-binary 臂(1383-1412)**保留**（gated P5），其 caller 换 inline allowlist + relate(ExactArrow)。
12. **[P3, GROUP A] 删 encode 端 merge/widen 家族**（exchange.rs）：`merged_exchange_schema`(1084)、`exchange_schema_compatible`(1053)、`merge_exchange_field`(196)、`merge_exchange_field_type`(145，含 List↔Struct 折叠)、`is_compatible_exchange_arrow_type`(105)、**`exchange_field_from_array`(212) 一并删**（其唯一 caller 在 merged_exchange_schema 内，对抗校验：是删非改）。encode 用 `chunks[0].schema()`（P2 已同构）。`normalize_exchange_array_for_field`(1154) 保留为 scale-strict retag。**Group A 必须覆盖跨 chunk nullability**：encode 端今天靠 merge_exchange_field OR 合并多 chunk 的 nullability（test encode_chunks_normalizes_runtime_nullable_widening:1644）；retag_column 是 type-only，要把 merge_fields_nullability 的 OR 显式带进 encode/materialize 路径，或证明单发送者 chunk 的 per-field nullability 已同构。
13. **[P3] 删 sort widen + sort schema 收紧 + nullability-OR 显式落边界**（sort/mod.rs, chunks_sorter_topn.rs）。删 `sort_payload_data_type`(73)、`is_compatible_sort_field_type`(61)；merged_sort_schema_for_chunks 跨 chunk 用 relate(ExactArrow) + merge_fields_nullability。**先实证 q94 现状**与 `null_count()>0`(133) data-driven 项的作用——若 q94 修复依赖该 data 项，勿删；q94 diff 不自动当回归（它本来红）。
14. **[P3] standalone root 走 typed exchange + 删 coordinator 文本回解（standalone-only）**。**对抗校验重定范围**：`decode_result_batch_to_chunk`(dispatcher.rs:751) 被 **RemoteDispatcher（分布式）**用，standalone-distributed 也走它——非"仅 FE-relay"。所以：(a) 把 RemoteDispatcher 的 fetch transport **换成 typed exchange**（与删 coerce 同一原子提交）；(b) BE root sink 按 **FE-compat/standalone 分流**：真 FE-compat fetch 仍发 `TResultBatch`（internal_service.rs 产、result_buffer 消费），standalone 走 typed。删 `coerce_fetch_chunks_to_output_columns`(511) + binary_like 家族(625-793) + caller(469)。`decode_result_batch_to_chunk` 为真 FE-compat **保留**。InProcessDispatcher(584) 本就返回 typed。
15. **[P1, P5-gated] 删 schema_compat 的 List↔Struct[1] 折叠**(schema_compat.rs:54-59)。先验证无 live path 依赖（array_agg state 形状由 P5 确定单一）。否则整步延后 P5。
16. **[P3] CTE wire_ids fallback 显式化**(exchange.rs:1346-1357)：要么证明 CTE 产/消共享 slot 命名空间（删 fallback、要求 wire_ids 解析），要么保留 position-remap 但文档化为契约 + post-materialize relate(ExactArrow) 校验。无静默中间态。

## 验证
- 每步 unit：touched 模块 `cargo test`。
- GROUP A 后：`cargo build --profile dev-opt`；`cargo test`；relate vs 被删谓词的 accept-set 零差异交叉测试。
- standalone sql-tests（dev-opt）：decimal agg / sort-TopN(q94) / cte / join 套件 verify；q94 状态先记录。
- **最终验收 = 1FE+3BE**：跨 exchange 的 decimal SUM/AVG、TopN over exchange(q94)、CTE 跨 fragment、PARTITION-TOP-N(avg/hll/percentile，opaque-binary 仍须正确)。断言每 fragment 的 decimal agg slot == canonical(38,s)；合法查询无 ExchangeDescriptorMismatch；与 StarRocks 参考一致。
- GROUP A 提交前：把待删函数 stub 成 Err，跑 golden 套件枚举每个失败映射到哪个删除，确认都是预期（决定性已覆盖）。

## 当前进度
- [x] relate（keystone，纯加，type_relation.rs，17 测试绿）
- [x] Step 1 canonical_agg_decimal_type（types.rs，纯加，TDD RED→GREEN，3 测试绿）
- [x] Step 2 merge_fields_nullability（type_relation.rs，纯加，TDD RED→GREEN，1 测试绿）
- [x] Step 3 retag_column（type_relation.rs，纯加，TDD RED→GREEN，8 测试绿；含 decimal/utf8-binary/struct/list 递归）
- [x] Step 4 analyzer infer_agg_return_type 委托 canonical（functions.rs，characterization 绿）
- [x] Step 5 codegen infer_agg_function_types 委托 canonical（expr_compiler.rs；multi_distinct_sum (p,s)→(38,s) 真实变更，TDD RED→GREEN）
- [x] Step 6/6b sum/avg runtime spec 对齐 canonical（sum.rs/avg.rs；agg 套件 77/79 零回归，与基线同）
- [x] Step 8 删 is_compatible_signature_type，2 处调用换 relate(SameScaleWiden)（spec.rs；agg 77/79，array_agg/group_concat 仍 PASS，List↔Struct 折叠丢弃未破 standalone）。**注**：apply_type_signature 是 FE-compat/signature 路径；standalone agg 可能未完全覆盖它，Step 8 的完整验证待 1FE+3BE。
- [x] Step 7 debug_assert：被 Step 8 的 relate 检查吸收，无需单独做
- [x] Step 9 analytic verify：由 Step 5 codegen canonical 满足（analytic.rs 信 descriptor、不重导，符合 rule 1）
- 基线（managed-lake server, agg --mode verify）：**pass=77, fail=2**；2 个失败均**预存、与改动无关**：agg_test_count_distinct（`scalar output type mismatch for Decimal128`=q25，P5 修）、agg_test_statistic（SORT_NODE sort_tuple_slot_exprs missing）。
- [x] **GROUP A（P3 descriptor 权威 exchange）完成并验证**。**简化发现**：计划原定的「收紧全局 align + 删 merge 家族」过度/高风险；实际只需 (a) decode 侧接收端 `retag_column` 物化到 descriptor（保留 opaque-binary passthrough），物化后 batch==descriptor，align 自然 no-op，**无需碰全局 align**；(b) encode 侧删 sender widen（`exchange_transport_data_type` 删；`exchange_field_from_array` 用实际类型），**保留** `merged_exchange_schema` 的 nullability-OR（对抗校验：删了会破跨 chunk nullability）。删 `is_compatible_exchange_arrow_type`（死代码）。exchange 单测 10/10。
  - **cross-process (cluster-size=3) agg 验证：pass=78, fail=1**；唯一失败 `agg_test_statistic` 是预存 SORT lowering bug（与 GROUP A 无关）；**`agg_test_count_distinct`(q25 decimal) 在分布式下 PASS**——P2+GROUP A 修好了分布式 decimal 聚合跨 exchange。
- [ ] Step 13 sort widen 删除 + nullability-OR 显式（注：`agg_test_statistic` 的 `sort_tuple_slot_exprs is missing` 是独立 sort lowering bug，非 widen 问题）
- [ ] Step 14 root result 走 typed exchange（standalone-only，RemoteDispatcher transport 替换）
- [ ] Step 15 List↔Struct 折叠删除（P5-gated）、Step 16 CTE wire_ids 显式化
- [x] 更广 cross-process 验收（cluster-size=3）：**set-op 18/18 全过**；**analytic 33/35**、**runtime-filter 22/23**——失败全是预存的 `sort_tuple_slot_exprs is missing` SORT lowering bug（与 GROUP A 无关）；**decimal 0/13**——但**单进程也 0/13**（同样 13 个失败），错误是 `parse DECIMAL_LITERAL`/`precision >38`/`invalid decimal literal`，是**预存的 Decimal256 / 大字面量缺口**，parse/build 期、与 exchange 无关、非我的回归。
- **结论：P2 + GROUP A 在分布式下零回归，修好了 q25 分布式 decimal 聚合。剩余失败全部预存且与本工作无关**：(1) `sort_tuple_slot_exprs is missing` SORT lowering bug（独立，非 Step 13 widen）；(2) Decimal256 / 大 decimal 字面量支持缺口（独立 feature）。
- [x] **Step 13 sort de-widen**（sort/mod.rs：删 `sort_payload_data_type`，`sort_field_from_array` 用实际类型；保留 `is_compatible_sort_field_type` + nullability + `normalize_sort_array_for_field` 同 scale retag）。**sort 套件 13/13 全过**；**agg 单进程 77→78：`agg_test_count_distinct`(q25) 单进程也修好了**——sort widen 强制 (38,scale) 与 array_agg(decimal ORDER BY) 下游 build_scalar_array 期望类型不符导致 `scalar output type mismatch`，去 widen 后类型一致。**q25 现在单进程+分布式都修复。**
- [x] **Step 16 CTE wire_ids**：materialize_chunk_for_wire_meta 原样保留了 wire_ids_match + position-fallback 逻辑（GROUP A 未改变 CTE 命名空间行为，只多了 retag 到 descriptor）。**cte 套件 3/3 全过**。无需改代码——逻辑已 descriptor 权威。
- 剩余（刻意未做，有据）：
  - **Step 14 root result typed exchange**：standalone-only，需替换 RemoteDispatcher fetch transport（decode_result_batch_to_chunk）——**高 blast radius（动所有分布式 root fetch）、不修任何现存失败**（cross-process agg 78/79 已证现有 text 路径正确处理 decimal root result），是 latent cleanup。marathon 末尾不值得为零现存收益冒回归风险。
  - **Step 15 List↔Struct 折叠删除**：P5-gated（array_agg state 形状由 P5 确定单一后才能删）。

## 最终状态（本轮 /loop 完成）
实质计划全部完成并验证：**P1（4 基元）+ P2（4/5/6/6b/8/9）+ GROUP A（P3 核心，简化版）+ Step 13（sort）+ Step 16（CTE）**。
- **全量 `cargo test --lib`：4668 passed, 0 failed**（修了 2 个同型的旧-widen 断言测试：exchange 的 `encode_chunks_preserves_decimal128_values_exceeding_declared_precision` 与 sort 的 `full_sort_preserves_decimal_payload_exceeding_declared_precision`——均改为断言 sender 实际类型 + i128 值保留）。编译干净（70 warnings 皆 crate 既有）。
- **q25 分布式 decimal bug 单进程 + 分布式都修复**（count_distinct 单进程经 Step 13、分布式经 GROUP A+P2）。
- cross-process(cluster-size=3) 验收：set-op 18/18、agg 78/79、sort 13/13、cte 3/3、analytic 33/35、runtime-filter 22/23。
- 所有剩余失败均预存且与本工作无关：`sort_tuple_slot_exprs is missing` SORT lowering bug；Decimal256/大字面量缺口（decimal 套件，单进程也 0/13）。
- 未做：Step 14（高风险/零现存收益）、Step 15（P5-gated）。
- 仍存的预存失败（与本工作无关）：`agg_test_statistic` 等的 `sort_tuple_slot_exprs is missing` SORT lowering bug；decimal 套件 Decimal256/大字面量缺口。
- [ ] Step 4-9/6b（P2 wiring + lock）
- [ ] Step 10-12（GROUP A 原子）
- [ ] Step 13-16

## type_relation 收口 PR（#300 之后的后续，本分支 claude/p5-type-relation-collapse）

把执行层四处漂移的「类型兼容检查 / 物化 / 字段 nullability 合并」收口到单一权威原语
`type_relation`（`relate` 检查、`retag_column` 物化、`merge_fields_nullability`），
并删除 #295 为 StarRocks-1FE3BE 加的 List↔Struct 中间态桥接（= 计划里的 Step 15，
现已可做：验收标准改为 NovaRocks coordinator+3BE，不再需要兼容 StarRocks FE 的 Struct 中间类型）。

提交（基于 origin/main）：
- `retag_array` → `retag_column` 重命名（向量列物化，非 Arrow Array 专指）。
- sort：删 `is_compatible_sort_field_type` + `retag_decimal128/256_array_for_sort`，
  检查走 `relate(SameScaleWiden)`、物化走 `retag_column`。
- exchange：编码侧 `normalize_exchange_array_for_field` → `retag_column`，
  `merge_exchange_field` nullability → `merge_fields_nullability`。
- schema_compat：`is_execution_data_type_compatible` → `relate`，
  `normalize_array_to_data_type` → `retag_column`，删本地 decimal retag helper。
- chunk（全局每个 Chunk）：`is_compatible_chunk_field_type` → `relate(SameScaleWiden)`，
  把「任意 scale 容忍」收紧为「同 scale」（fail-fast，scale 不符不再静默采用 batch scale）。
- List↔Struct 桥接删除（Step 15）：schema_compat 检查 + exchange 合并两处的 StarRocks-FE 中间态桥接。

验收（NovaRocks coordinator + 3 BE 跨进程，用户指定标准）：
aggregate 78/79、complex-type 34/34、complex-type-native 3/3、analytic 33/35、sort 13/13、ssb 13/13；
**零新增回归，零 `chunk schema field mismatch`、零 List/Struct 类型错误**。
所有失败均为预存的 `sort_tuple_slot_exprs is missing` SORT lowering 缺口（来自 first commit
13b48493，本分支对 sort codegen/lowering 零改动；单进程与跨进程完全一致）+ debug 构建 tpc-h 超时。
`cargo fmt` 无 diff、`cargo clippy` 对改动文件无新增告警。

刻意作为后续（非本 PR）：
- **P5a：把 array_agg/group_concat 的 order/distinct 从函数名字符串改为 AggFunction 结构化字段**。
  纯重构、无行为变更，但波及 ~86 处 AggFunction 构造点（多为测试夹具），与本 PR 的 type_relation
  主题正交 → 独立聚焦 PR 更易审查。encode 集中在 `lower/node/aggregate.rs::encode_aggregate_name`
  （standalone 经 thrift 也汇入此处），decode 在 array_agg/group_concat 的 `build_spec`。
- Step 14（root typed exchange，#300 已记录的高风险零收益项）。
- `sort_tuple_slot_exprs is missing` 的 standalone sort lowering 缺口（独立 bug，影响
  join/aggregate/analytic 多套件的 sort-over-X 形状）。
