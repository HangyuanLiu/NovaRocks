# M07 P04 ScalarSchema 冻结接线只读审计

范围：approved v5 spec §5.6（spec 文件246–248）；只读当前代码，未运行 Cargo、未改 repository。当前主 agent 正独占纯类型/codec，因此以下为观察时刻接缝，不是产品已安装声明。

## 1. 现有权威与真正丢失点

- `sql/src/compiler/mod.rs:635` SqlAnalyzedQuery 私有拥有 logical_plan + ColumnRefFactory；ColumnMeta (`column_id.rs:82`) 有 data_type、nullable、logical_type: Option<SqlType>。factory create(:119) 初始化 None，set_logical_type(:184) 可保存事实。这是分析阶段事实，不是 Native 解码后推测。
- catalog ColumnDef 在 `types/src/schema.rs:32` 已有 logical_type。AnalyzerScope (:90/:110) 可查完整 catalog logical types；但 `analyzer/scope.rs:240–245` 写 factory 时只保留 Json，Bitmap/Hll/Variant/递归 Array/Map/Struct 已声明事实没有完整交到 factory。`types/schema.rs:148` SqlType 本身无 Object/Percentile，因此只扩大现有 Json whitelist 仍不形成完整 ScalarOpaqueType 域。
- projection 在 `analyzer/mod.rs:1671/:1707` 用 logical_output_type 写新或 passthrough ColumnId。`logical_output.rs:61` source identifier 优先直接返回 factory；:66 非源 ColumnRef 仍只过滤 Json。opaque 本表直接投影的后续最终 root 因而不能依靠这个 API 完整冻结。
- function bound identity 现权威 API 是 `functions/mod.rs:79` scalar_output_logical_type；只列 parse_json/json_object/json_array/to_json/json_query 五个 builtin identities，不按同名用户函数猜。`logical_output.rs:69` FunctionCall 只调用该 API，因此 coalesce/ifnull/if/nvl/nullif 不传播输入域。具体反例：coalesce(parse_json('null'), NULL) 结果物理 Utf8，但 factory 不保存 Json；不是新 codec 可以从 Utf8 猜回的问题。
- `logical_output.rs:71–101` CASE 当前只针对 Json，跳过 AST literal NULL，其余每分支必须 Json；nested/CTE 可保持这条 Json 路径。`analyzer/mod.rs:559–577` UNION/其它 set operation 同 logical_type 保留、Null 分支中性，否则 None。它只合并现有 factory 信息，不能补已丢的 opaque/递归身份。
- json_list_provenance 的明确 array literal/array_agg/array_sortby 和 CASE 路径在 `logical_output.rs:120–253`；:336 把 proven Json item metadata materialize 到真实 List cast target。不能把一个 bool 当完整任意深度 nested ScalarField。
- 嵌套已有真正可复用事实：`analyzer/helpers.rs:32–41` SQL nested Json Field 标记；List/Map/Struct 构造 :144/:151/:168。`types/coercion.rs:60–80` undecorated_nested_type 保留 NR_LOGICAL_TYPE_KEY，去掉 provider 装饰但保留语义。`plan-codec/physical_type.rs:160–180` 和 `native_type_encode.rs:29` 从实际 Field metadata 编出 Json/Hll/Bitmap/Object/Percentile；`native_type.rs:415–424` 再恢复 marker。
- **嵌套合并缺口**：`types/coercion.rs:119` wider_type 完全相等时 clone 保 metadata，但 :128 List、:144 Struct、:251–280 Map 在 nullable/type 变化时重建 Field::new 不携带 logical marker。同一 Json item 仅 nullable 不同就足以走合并而失去域；CASE/coalesce/UNION 都可能调用这条 physical coercion。必须递归合并原权威 semantic facts，不能把 wider 后的裸 Utf8/Binary 宣布为原逻辑类型。

## 2. 分析到 final plan 的事实接缝

- `sql/src/common/schema.rs:29` OutputColumn 只有 ColumnId/name/DataType/nullable/is_internal，没有根逻辑域。
- `compiler/mod.rs:1210–1264` 消费 SqlAnalyzedQuery，把 factory 交 optimizer。`optimizer/mod.rs:284–292` factory 进入 Memo，:359 CSE 仍使用它，但 :363 attach_scalar_arena 后只返回 optimized_tree，factory 未交出。`optimizer/optimized_tree.rs:80` tree 只有 OutputColumns。`optimizer/extract.rs:296–351` 新 project 输出按 actual scalar DataType/nullable 重建 OutputColumn，:289 outer join 可 widen nullability。
- `compiler/completion_driver.rs:780–796` final completion 亦只得到 optimized physical tree；:828 / :1105 进入 lower_final_physical_plan。仅在 SqlAnalyzedQuery 新 getter 取一次 schema，而不贯穿另一个 completion protocol，不能覆盖真实编译入口。
- `planner/distributed/build/contract_lowering.rs:207–222` final root 从 actual ValueDef 的 declared type 冻结 result_types；:8168 result_fields 固定 name/alias/value/ty；`physical-plan/plan.rs:102` ResultField 只有 ValueType。此时 Json 与 String 的顶层 Utf8、Binary 与 opaque 的顶层 Binary 无从区分。
- final nullability 必须服从 actual root value（:207 注释明确）；不能简单冻结 analysis nullable 后忽略 join/optimizer 变化。正确做法是语义身份跟 ColumnId/有序输出 occurrence 走，最终用 actual root carrier/unit/precision/nullability 精确校验并形成 ScalarField。

## 3. 最小完整实现顺序（建议接口，不是已实现）

1. 在 SQL 分析输出引入递归、明确的 resolved scalar semantic fact（可直接以 result-contract ScalarValueType/ScalarField 作为纯 vocabulary；不导出 ColumnRefFactory 或 Arrow/Native DTO 给应用）。源头从完整 ColumnDef logical_type + 实际 nested Field metadata、CAST 原 owner、精确 builtin/external FunctionBinding 声明产生；plain String/Binary 也必须是已解析结果事实，不能把 missing fact 当它们。
2. 实现同域值合并的一处递归 API，例如 `merge_scalar_output_facts(resolved_branches, actual_coercion_type)`：NULL 中性；CASE/if/coalesce 仅合并返回值分支，不把条件当值；nullif 返回首参数域；显式 cast-to-varchar 结果为 String；opaque/Json 同域保留，混域依照现 SQL 已选择的转换结果，不能改变 query 语义或靠拒绝全部原合法类型跳过。嵌套 List/Map/Struct 保准确名、顺序、nullability，map nullable key 不额外收紧。现 Json provenance 测试是基线。
3. 把这些 facts 与 OutputColumn/ColumnId 贯穿 optimizer extraction/CSE/CTE/setops/project 输出，或在 SQL completion 中携带一个等价的不可变 occurrence facts owner；每个 rewrite 输出必须按原 value/明确表达式声明转移，而不是 name lookup。最终 completion 取 actual root ValueType 做 `scalar_field_matches_storage` 精确验证并构造只有一个 ordinal0 的 ScalarSchema。若 facts 漏接，修事实 producer；不能以 Binary/Utf8 guessed mapping 作为捷径。现 bridge `type-contract/result_scalar_type.rs:26` **只验 physical storage**，不会检查 actual logical metadata，这一边界有意如此，BE 需另验实际 marker。
4. RootOutputContract 的显式 Scalar schema 应作为 InternalFacts(ScalarValueV1) 的 purpose-owned payload。当前 `result-contract/root.rs:119` InternalFacts 只 domain，:144 bind 只 ClientRows；`idl/novarocks/result.proto:116` 只有 client_schema；`proto-codec/root_result.rs:832–880` scalar domain 当前被无 schema 接受。增加 required-exact ScalarSchema wire（平坦节点、single native slot/ordinal0、闭类型、depth/nodes/name/wire/backing caps，与 ClientRows 分离），其它 domains 禁额外 scalar schema，ScalarValueV1 禁缺 schema。
5. 依次扩 `physical-plan/validation/graph.rs:958`、`physical-plan/resource.rs:1207`、`local-program/program.rs:919`、`native-adapter/fragment_sink.rs:68` slot/type 校验和费用 census。plan `with_root_output` (:1488) 仅允许未冻结 Result sink，再验证；Native wire projection必须把这一 schema 与根 layout sole slot 精确绑定，不猜 slot0。
6. BE `backend_task_execution/execution_host.rs:814` 和 `root_result_session.rs:148` 目前拒绝所有非 Statistics InternalFacts；在上述合同链完整后再开 ScalarValueV1 producer，使用 frozen schema + actual exact datum/nullability/metadata，不使用 result-render。`worker/root_result_channel.rs:321` schema bytes 目前只计 ClientRows；必须计 Scalar actual backings/Arc 元数据，而不仅 wire len。producer scratch/cursor/输入能力仍由同 Root 原预算留到真实退出。
7. FE 当前 `frontend-application/query.rs:1420` evaluate_governed_scalar_query 仍普通 prepare/Rows 采集；typed consumer须由 statement purpose 明确 ScalarValueV1，用 child128KiB/assignment128KiB/单值64KiB 和原 Session owner 收集，zero rows NULL、one typed value、second row拒绝，End严格结束，随后 session staged/live原能力提交。该步骤属于后续安装，不是本次纯 schema tests 完成。

## 4. 可击穿实际漏接的最少测试

- 复用 `sql/compiler/mod.rs:2064` array_agg_json_semantic_schema_survives_optimizer_and_exact_final_plan、`functions/mod.rs:2428` json_output_domain_uses_bound_identity_not_shadowed_function_spelling。新增 coalesce/ifnull/CASE/UNION Json+NULL、明确 string cast、外部/shadowed same-name producer；最终 scalar frozen field 必须不同于 String，不仅看 Arrow Utf8。
- catalog Hll/Bitmap（Binary）直接 SELECT、CTE alias、CASE同域/NULL、UNION同域；Object/Percentile 从真实已有 typed metadata producer接入（不要以 SqlType 尚无 variant 就冒称 SQL parser 已支持）。Variant 从明确 declared Variant 源/Cast/producer接线，LargeBinary alone不作证明。
- nested Json List/Map/Struct 两分支只 nullable 不同，强迫 wider_type 重建；检查 frozen node exact marker/名/顺序，并最终 BE实际输出 datum。map key nullable preserves existing事实。
- final plan→wire→LocalProgram→Host 的 schema missing/extra/mismatchedslot/type/timezone/decimalprecision 拒绝；正确 typed schema跨wire等价。移除 SQL semanticfact forwarding、scalar wire schema、BE logical marker验证各能 actual runtime failed。
- Scalar producer 0/1/2 rows（第二跨 batch）、Binary非UTF8全字节、Decimal128超过i64/Decimal25676位、µs/ns带原zone、negative TIME原值和 NULL nested。验证现已批准语义，不以 MySQL bytes作scalar oracle。

尚未闭合：SCV1完整 codec/tree carrier producer、frozen wire、Host安装、FE collector/typed session原授、实际源与serializer/backing alias graph。本审计不声明其完成。
