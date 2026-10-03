# M07 P04 下一 SQL builtin/domain 切片只读清单

审计时间：2026-10-04；HEAD `45a04a2724d42626ca245ef97d4af307e710c306`，包含当前冻结未提交 SQL semantic wave。本文仅记录现状和建议，未实施，未运行 Cargo。批准 v5/P04 继续适用；不新增公开 CAST、catalog/provider 类型、不开放 Scalar Host、不声称整个 Native V1 完成。

## 1. 最小词汇与事实来源

事实：`novarocks/types/src/schema.rs:148` 的 SqlType 有 Bitmap/Hll/Variant，没有 Object/Percentile；`types/src/logical.rs` 的 LogicalType 已有 Object/Percentile。`novarocks/physical-plan/src/plan.rs:103-136` 的 ResultValueDomain **已经**有 Object/Percentile，并且与 ScalarOpaqueType 映射完整。无需另造 neutral root enum。

建议只新增 SqlType::Object、SqlType::Percentile 两个内部事实叶子。它们代表可信生产者声明，不代表新增 SQL 类型拼写。未声明 Binary 仍是 Binary；名称包含 hll/bitmap/state 不是事实。源 ColumnDef.logical_type→scope→ColumnRefFactory 的入口已在 `sql/src/analyzer/scope.rs:233-247`、`sql/src/column_id.rs:175-205`；保留该按 ColumnId 的来源。

### 精确 builtin 输出合同

全部 ID 必须是实际 selected binding 的 function_id，且校验 kind、selected scalar result 和最终物理 carrier；scalar ID 构造在 `sql/src/functions/mod.rs:1100`，aggregate ID 构造在 `:2086`。Scalar catalog 只注册 InstalledScalar/WindowBoundary (`:2037-2084`)；不要把 Execution 中存在但未 admitted 的 kernel 变成新 SQL 支持。

| 精确 ID（表中名字加相应前缀和 `/v1`） | 输出事实 | 现有真实证据 |
|---|---|---|
| `builtin.scalar/`：to_bitmap、bitmap_empty、bitmap_from_string、bitmap_from_binary、bitmap_and、bitmap_or、bitmap_xor、bitmap_andnot、bitmap_intersect、sub_bitmap、bitmap_subset_limit、bitmap_subset_in_range | Bitmap；必须 selected Binary | SQL registry.rs:1535-1573；Execution object/to_bitmap.rs:27，bitmap_functions.rs:554/637/714/831，内部 bitmap 编码 |
| `builtin.scalar/hll_hash/v1` | Hll；selected Binary | registry.rs:1646；Execution object/hll_hash.rs:30 |
| `builtin.scalar/percentile_hash/v1`、`builtin.scalar/percentile_empty/v1` | Percentile；selected Binary | registry.rs:2080-2088；Execution object/percentile_functions.rs:32-61，encode_single_value/encode_empty_state |
| `builtin.scalar/variant_get/v1`、`builtin.scalar/try_variant_get/v1` | **仅 selected LargeBinary 时** Variant | functions/mod.rs:1763、1909-1921；literal target BIGINT 等绑定输出普通类型，不能统一标 Variant |
| `builtin.aggregate/bitmap_agg/v1`、`builtin.aggregate/bitmap_union/v1` | Bitmap；selected Binary | types/src/aggregate.rs:170；Execution agg/functions/mod.rs:483-484 |
| `builtin.aggregate/hll_union/v1`、`builtin.aggregate/hll_raw_agg/v1` | Hll；selected Binary | types/src/aggregate.rs:179；Execution agg/functions/mod.rs:494-495 |
| `builtin.aggregate/percentile_union/v1` | Percentile；selected Binary | types/src/aggregate.rs:207；Execution agg/functions/mod.rs:454 |
| parse_json/json_object/to_json/json_query | 保持已声明 Json | 现 helper functions/mod.rs:79-94；json_array 当前 manifest Unavailable，不能因 helper 列出而宣称 admitted |

排除：`bitmap_to_binary` 在 Execution `object/bitmap_functions.rs:719-743` 用 encode_external_bitmap 输出普通外部 Binary；不能按整个 bitmap 家族标记。`ds_hll_count_distinct_state` / `ds_hll_count_distinct_union` 当前 owner 只声明 Binary；HllHandle 实现名不构成 Object/Hll 合同。hll_empty/hll_serialize/hll_deserialize/array_to_bitmap/hll_hash1 的 runtime kernel 不等于目前 admitted SQL producer。count/merge_count/percentile_approx 等数值输出不应标 opaque。此次没有查到应新增 Object producer 的已声明确切 binding；Object 词汇只允许已有显式来源事实，不创造 producer。

最小实施接口：将现 JSON-only `scalar_output_logical_type` 扩成 closed binding facts helper（或保留名称并另加 aggregate helper），使用确切 ID + selected result carrier。`analyzer/logical_output.rs:77-149` 添加 AggregateCall resolved 分支和 WindowCall aggregate_binding 分支；WindowCall 不按 display name 判定。现 FunctionValueType 只有 DataType/nullable，不必先改整个 function contract；SQL owner 可先维护独立语义事实，binding 物理签名仍保持 exact。

## 2. Wrapper 的持久传播

已闭入口：SELECT mints ColumnId 后写 logical fact (`analyzer/mod.rs:1666-1674,1702-1710`)；CASE/coalesce/ifnull/nvl/if/nullif 和 implicit cast 以来源事实合并 (`logical_output.rs:30-175`)；UNION 在 `analyzer/mod.rs:552-573` 只保同域/物理 Null 中性。新增 SqlType 叶子后必须加入 logical_carrier_matches (`logical_output.rs:413`) 的 Binary/LargeBinary 兼容分支，不能改变显式 public CAST 清域语义或 Bitmap/Hll 已有 group/distinct 限制。

仍缺的具体入口：

- `functions/mod.rs:1531` list_type、`:1554` map_type 只接收 DataType；`:1521` struct_field_type 只提取子 data_type，丢子 Field 的 marker。`__array_literal` :1586、map :1685、map_from_arrays :1718、row/struct :1748、named_struct :1865 都重建无 marker Field。
- `types/src/aggregate.rs:118` array_agg、`:148` map_agg、`:275` approx_top_k 将顶层参数域包入新子 Field 时无语义输入。array_unique_agg/sum_map 直接复制已存在完整 nested type 的分支可以保留现 marker；不能统一重建。
- `analyzer/resolve_expr.rs:1062-1088` 实际 struct access 降成 `__struct_subfield`；结果 Field marker 必须升回返回值的可信逻辑事实。array element/min/max、map keys/values/element、array flatten/zip/repeat/intersect 等须按实际 selected closed wrapper ID 和精确输出位置处理，不能按相似名字批量赋域。
- JSON-only bridge `logical_output.rs:349-407` 已以内部 Cast 给 array_agg/__array_literal/array_sortby 加输出 child marker，三个调用入口在 resolve_expr.rs:1237、2442、2780、2901。它说明无需修改 selected binding 的物理签名；推广时须沿相同可信参数事实构造准确输出 target，保持 names/order/nullability/key/value/map flags，不把普通 provider decoration复制成逻辑授权。

建议拆成两个明确操作：① expression logical fact forwarding（完整返回值域/可信容器 SqlType）；② output child Field marker projection（只作用实际包装/选取节点的确定位置）。包装普通参数可保持普通 physical leaf，不能从 Utf8/Binary猜 opaque；同域合并与 NULL 中性复用现规则，mixed domain 清域。提取 child 时从输入**实际 Field marker/完整可信 SqlType路径**取事实，校验与物理 carrier一致。业务 wrapper signature 及 frontend/native binding 校验仍保持原 physical contract；若仅给 TypedExpr 偷改 DataType 会违反 resolve_expr.rs:2895 的 exact binding assert，因此应使用既有明确内部输出适配节点，或在真实 binding owner同时声明等价输出 projection，不能绕过 assert。

实际数组输出仍有独立 bridge风险：Execution `agg/functions/array_agg.rs:379-413` reconcile_field_to_field 在 nullable 变化时 Field::new(:391)丢 marker，reconcile_field_to_data_type(:404)同样。SQL root freeze完整并不证明该真实 carrier已保来源；后续 source/producer接线须沿 known FieldRef/provenance处理，不能 Root重新猜 marker。

## 3. SqlType consumers 的准确改动/拒绝

| 文件/行 | 必须保持的语义 |
|---|---|
| `sql/src/literal.rs:909-961` sql_type_to_arrow_type | 新内部 leaf→Binary 是 private carrier projection；Array/Map/Struct的新 Field若用它承接可信域，应显式写合法 marker。不要把它等同公开 CAST |
| `sql/src/literal.rs:241-290,799-839,1368` | 不增加 Object/Percentile type spelling，不加入 Binary literal/default cast的许可分支；保留当前 unsupported error |
| `sql/src/compiler/root_scalar_type.rs:147-181` sql_domain/sql_storage_matches | 加 Object/Percentile→对应 LogicalType，closed wire只支持 Binary；SQL wider compatibility仍可接受 Binary/LargeBinary，不能把 closed wire限制反施加SQLcoercion |
| `sql/src/compiler/root_output.rs:57-64` | 按 frozen ColumnId fact映射 Object/Percentile到已有 ResultValueDomain；不得 root根据函数名字推回 |
| `sql/src/planner/table.rs:737-773` add_sql_type | 新 leaf加入 exhaustive noheap tail；layout自动计 inline enum，不新增authority |
| `sql/src/compiler/completion.rs:3413-3436` | 已有 `_=>Ok(0)`；新 leaf无额外heap，无需创造fee |
| `frontend-application/src/catalog_application/statement.rs:349-393` connector_data_type | SPI ConnectorDataType (`spi/src/connector/mutation.rs:268`)没有 Object/Percentile；新增显式 Err，递归容器传播拒绝，不能转Binary/fabricate provider支持 |
| `frontend-application/src/query_execution/dml/iceberg_writer.rs:1007-1040` sql_type_name | 新私有域显式 Err，不生成未 admitted OBJECT/PERCENTILE DDL拼写 |
| `frontend-application/src/view/engine.rs:454-504` convert | 当前返回TypeName，需窄改Result以递归显式拒绝新私有域；不自动创建公开 view column CAST type |
| `query-application/src/sql.rs:785`、catalog statement.rs:1294-1336 | 现公开 parser保留，无新词汇 admission |
| `frontend-application/src/catalog_application/query_catalog.rs:263-273` | provider semantic enum只有 Bitmap/Hll显式来源；不从Binary推 Object/Percentile，也不扩 providermanifest |
| `sql/src/literal.rs:971-1035` CTAS inverse；mv_aggregate_layout.rs:279-330；iceberg_writer.rs:941-993 | physical-only conversion必须继续plain语义，不能由Binary猜域；如果来源已opaque，应由caller明确拒绝unsupported持久化，不能用此inverse清域后绕过provider拒绝 |
| `mysql-adapter/src/result_encoding.rs:34-65` | 仅Decimal检查logical_type，其余按Arrow类型，无新 exhaustive SqlType match；本切片无需改变MySQL metadata表现 |

新增叶子会自然触发的 exhaustive SqlType consumers主要是 schema enum、literal forward、planner table accounting、catalog connector projection、iceberg DDL type formatter、view formatter；logical/root helpers多有 wildcard，需要主动补，而不能指望编译器提醒。

## 4. QueryApplication 与 Native 输出两条独立缺口

**QueryApplication**：`query-application/src/preparation/description.rs:43-72` OutputContract::from_completed_plan 明确把每个 ResultField.logical_type传 None(:65)，丢现有 top Json/Hll/Bitmap/Variant。最窄修复是从 plan.result_port().fields[].domain 做 closed exhaustive投影 Plain→None，Json/Hll/Bitmap/Variant/Object/Percentile→准确SqlType；先利用/校验 domain.matches_storage，names/nullable按原result occurrence保持。容器的topdomain为Plain，其nested事实保留在精确DataType child Field；不得把Plain Binary推Opaque。`api/result.rs:122-158` 已有logical_type getter。SHOWPROCESSLIST/query.rs:516与immediate普通结果构造None仍合法，不应全局替换。

**Native output layout**：`plan-codec/src/physical_encode.rs:4104-4145` output_columns已经取得 exact result_field且匹配fragment/node/ordinal/value，但只用其name；`:4171-4193` output_column只根据 ValueType.data_type调用 encode_physical_type。`plan-codec/src/physical_type.rs:29-99` 将 Utf8/Binary编码成Varchar/Varbinary，无法恢复topdomain；嵌套 encode_nested_field(:160-185)已有 explicit marker→Json/Hll/Bitmap/Object/Percentile。`plan-codec/src/native_type.rs:415-426` 能准确decode distinct Bitmap/Object marker，`:38-77` 的decode_field_type[_owned]可形成真实来源origin。`native-adapter/src/root_scalar_leaf_codec.rs:124-131,218-240` 要求slot缓存+实际 Field canonical marker都与 ScalarSchema一致。因此只冻结ScalarSchema仍会让真实opaque/json carrier拒绝；目前Host未开放是正确门。

建议新增私有 `encode_output_type(data_type, proven_domain)`：先closedphysical验证及domain/storage一致，再将 exact root occurrence domain编码为已存在PrimitiveType；Plain沿旧路径。在output_columns传入exactresult_field.domain；复杂root的nestedScalarSchema只用已批准准确schema投影，不按名/URI/alias猜。最初可闭合法叶子root，不冒称任意intermediate opaque来源已闭。跨Exchange与同一片段透传节点必须有明确的 occurrence/value lineage传播：现 result_value_names(:3994-4100)只传播name，不能借name当语义。若需要上游layout domain，沿实际output ordinal/edge receive_mapping建独立fact映射并对冲突/物理错拒绝；复制输出 occurrence不能用字符串别名关联。Scalar source_slot应与最终WireLayout exactslot绑定。

## 5. 可独立实施顺序与能失败的oracle

1. **词汇+closed producer facts**：types/schema；SQL functions/logical_output/root_scalar_type/root_output；exhaustive consumers上述必要小修。测试实际catalog resolved binding的preciseIDs：percentile_hash/empty/union→Percentile；bitmap_from_binary→Bitmap、bitmap_to_binary→Plain；hll_hash/hll_union→Hll；DS保持Plain；variant_get target BIGINT→Plain、默认LargeBinary→Variant；shadowing/custom binding不能取得builtin事实。
2. **包装/提取事实**：独立wrapper helper与resolve_expr入口；真实array_agg/hash、arrayLiteral、map/struct/named_struct、child selection、CASE/COALESCE/UNION NULL与mixed-domain；字段顺序/nullable/marker/provider-decoration用实际TypedExpr/OutputColumn oracle。未覆盖的wrapper明确列为open，不宣称所有function forwarding。
3. **输出投影**：QueryApplication OutputContract top facts + exactNative root occurrence layout；真实SQL→completedplan→wire→ownedNativeField/LocalProgram→ChunkSchema→NativeScalarLeaf validate。用plainUtf8/Binary对照，typedJson/Bitmap/Object/Percentile distinct、slot重复occurrence、marker错配拒绝。删domain传递或恢复plain encode应runtimeFAIL，不只观察ScalarSchema roundtrip。

公开 `CAST(... AS OBJECT/PERCENTILE)`、CREATE/CATALOG provider支持以及新的Object producer不属于本切片。Scalar leaf producer/core/session/Host安装、nested完整codec、真实source增长前grant与C4另有门，不因这些SQL facts/roundtrip宣称完成。
