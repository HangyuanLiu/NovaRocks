# M07 P04 Scalar Native source / nested cursor 接线审计

范围：只读当前 source；没有运行 Cargo，没有修改产品或证据。Host 当前拒绝 ScalarValueV1，下面是可实施接点，不是 producer 已启用或 whole transport/C4 完成证明。

## 1. 累计行数与接受顺序

- `native-adapter/src/backend_task_execution/execution_host.rs:810–844` 是 root contract、LocalProgram、channel 和 session 汇合点。先借 root output layout 核验唯一准确 slot、一列及显式 ScalarSchema，再开 channel/session；不能根据 Arrow 自行推 Json/opaque。
- `native-adapter/src/root_result_session.rs:466–520` 是原 input 移交口。先 O(1) 读 batch.num_columns()/num_rows()、ChunkSchema.slots().len()、sole slot；超过一列或单 input 超过一行在 backing 遍历和 cursor/metadata clone 前拒绝。
- `ProducerState` 加累计 accepted rows 的独立事实。跨 input、DOP 的第二行检查和 `state.input = Some(input)` 必须在同一锁下裁决；若先做借用 backing proof，最后 commit 仍需复查计数。不能沿 statistics 在 input 编码完成后才更新 totals 的顺序（`root_result_session.rs:397–400`）认定 scalar 行数。
- 一行 NULL 占一行；累计不是 `Option<non_null_value>`。零行 input 不占行数，也不产生 NoRows 编码。仅所有 driver 完成、session sealed 且 accepted rows 为零时编码唯一 NoRows；否则 0-row→1-row 会错误输出两个 scalar records。
- empty input 仍核对 frozen source schema，不能以零行跳过类型/slot 身份。失败/abort 走现有 input/builder 清理和实际 producer exit；已输出第一行也不能在最终成功前发布 session assignment。

## 2. 保持原 carrier、permit 与 cursor

- `execution/src/runtime/fragment/io/result.rs:303–308` 在 last pull 前取得原 input 96MiB 与额外 hydrate 96MiB 能力。`execution/src/exec/pipeline/driver.rs:1622–1687` 保留原 permit 跨 materialization/Yield/edge；`1465–1474` 在 root takes_original_input 时跳过 hydrate。
- `execution/src/exec/pipeline/operator.rs:250–258` 默认 pull hook 只委托普通 pull，不能宣称任何普通 Project/Aggregate 因 root permit 存在而增长前受 96MiB 限制。Statistics 在 `builder/local.rs:1129–1147` 有真实 protected materializer，Scalar 未有对应 source 选择；现阶段只复用接受原 backing 的界，不声称所有 upstream source-growth 封闭。
- `root_result_session.rs:94–99` Input 顺序 encoder→Chunk→permit→scratch，让真实 original/cursor owners 先于 permit 退场。Scalar 保持这个顺序；空/error/cancel/panic 的所有路径同样成立。
- `root_result_session.rs:314–341` 先 full scratch grant，再核验 cursor Box 实际 size/alignment 与有限 RecordBatch columns Vec clone 后增长，是 domain cursor 可复用的模板。Scalar 一列可避免 batch clone：cursor 只存固定状态，step 显式借 `&input.chunk`。
- 当前纯 `ScalarLeafCursor<'a>` 保存 variable slice。它不能安全地和拥有该 slice 的 Chunk 一起存在同一个跨-turn Input 中。最小接线是拆开固定 emission state 与每 turn 借来的 atom，或给纯 cursor 增加精确 resumable position 的借用接口；Native cursor 只存 selection/offset。不要 unsafe 延长到 static，不要为了消除 lifetime 先复制大 bytes 再检查。

## 3. 源头/类型/metadata 的准确事实

- `execution/src/exec/chunk/root_chunk_storage.rs:35–99` 的实际 metadata owner、scaffold 和 array proof 必须先于入队，保留未知/自定义 owner fail-closed。`25–34` 明确它不授予增长能力；`52–56` 检查 actual schema Arc，不能等值替代 owner identity。
- `root_array_storage.rs:138–146` 计算 buffer 原 allocation capacity + 实际 owner metadata；`149–153` 禁止 logical_nulls 整体扫描/分配。one-row slice 仍可能持有 >96MiB 原 backing，不能按 visible len 返还。
- `ChunkSchema.slots()`（schema.rs:812）、`ChunkSlotSchema.data_type/nullable/field_schema`（358–370）是借用口。避免 `column_by_slot_id` 的缺失错误路径（chunk_impl.rs:143–155）构造 slot key Vec；形状已核验时直接借 batch.columns()[0] 与 sole slot，并核实准确 slot ID。
- 现 `ChunkSlotSchema::try_new_with_metadata_origins`（schema.rs:187–208）允许 caller 传入 Some(ChunkFieldSchema)，不证明缓存与实际 Field logical metadata 一致。metadata receipt 证明 owner，不自动证明语义。Scalar bridge 必须对 expected schema、actual immutable Field、cached ChunkFieldSchema 三者逐层核对。
- `ChunkFieldSchema.children()`（schema.rs:99）可直接借：Struct ordinal 对齐；List child0；Map child0 key / child1 value，Map entries Struct 是 physical wrapper 不是额外语义层（schema.rs:60–85）。缺/额外孩子或 nullability/type/struct name/order 不一致都显式拒绝，不能重建 FieldSchema 克隆补齐。
- `types/src/logical.rs:58–62` 调用的 parser 在 `from_metadata_value` 使用 trim+to_ascii_lowercase，会分配；producer 检查用固定 key 的 borrowed metadata value 与 canonical identity 比较，不在 row turn 再调用 from_field/normalization。String/Json 和 Binary/opaque 同 physical carrier，不许推断或互相降级。

## 4. Dictionary 与叶子读取

- `root_array_storage.rs:237–247` 当前标准 carrier 只支持 Int32→Utf8/LargeUtf8 Dictionary，完整 keys 和全部 values 都占原 input budget；其他 key/value 不 blanket 允许。
- runtime dictionary 不是静态 ScalarSchema 的隐式 canonical fallback：Native bridge 若显式支持，先验证 frozen semantic String/Json 与原 field logical identity，再借 exact DictionaryArray<Int32Type> 的 selected key→selected values。key 为 NULL 输出 NULL；key 非 NULL 还必须检查 selected value NULL。不要 logical_nulls、cast、hydrate、dictionary decode 整列，也不要先 `values().slice` 创建新 Array 物理对象。
- leaf 直接 exact downcast 后取 raw 数值/字节：IEEE bits、i128/i256 unscaled coefficient、precision/scale、Date32、Time64 micro、Timestamp raw ticks+unit+exact zone。`query-application/src/sql/user_variable.rs:94–108` 的 binary UTF8-lossy 与 `sql/src/literal.rs:1181–1240` 的 Decimal/string/秒级时间格式都不是 typed codec 权威。
- static type matching 严格沿 `type-contract/src/result_scalar_type.rs`。运行时允许的 dictionary carrier 需要独立显式桥；不要为了 carrier 把 schema canonical matcher 泛化。

## 5. Nested cursor 最小结构

建议固定 stack（最大 semantic depth64）存 `(expected schema path, actual array/field path, row, child index, count/emit phase)` 索引事实。每 turn 借当前 Chunk 与其 exact owners 重新定位，或固定 ArrayRef Arc clones（只 inline 计数，不建立 growable Vec）。不得存引用到可移动的 cursor 自身。

- Struct 借 columns()/column(i)，同 row；List 借 values() + value_offsets()[row..row+2]，Map 借 entries/key/value + row offsets。避免 ListArray.value(row)/MapArray.value(row) 产生独立 sliced Array 包装，避免 to_data、cast 或 RowConverter。
- null container 不访问其子行值；仍验证 frozen schema。Map 顺序不排序、不去重；Struct field name/order 和孩子 nullable 按 schema authority 保存。任何排序/normalization 是新 scratch 及语义，不属于此桥。
- 计数与 emit 共用同一 immutable selected carrier，checked wire bytes/children/work/depth 在进一步访问或写前核验。每 turn 至多 RootProfile 的 64KiB/1024 cells，不能一次 driver-side 全树收集到 Vec。大量 NULL children 也要有有限 work/长度退出，不可把 payload bytes 零当作遍历免费。
- 单值 64KiB 与 child/assignment 128KiB 是 ScalarProfile 的语义 ceiling，不是新 funding authority。container framing/header/name计入哪一个 ceiling必须精确；不能只算叶子 bytes，不能先整值 encode 然后切片。
- 现纯 leaf 明确 UnsupportedContainer；在 nested cursor 未实现前继续显式拒绝，Host 不能因为 leaf codec 已有而打开全 closed vocabulary。

## 6. 可失败验证切面

1. 2-column / >1-row Chunk 携 Unknown metadata、巨大 nested metadata，证明先 shape 拒绝，cursor constructor/clone/cell访问不可达。
2. 64 DOP two 1-row submissions；第一行为 NULL；第二必须拒绝，zero-row inputs 不消耗行数且只在全零 End 生成一条 NoRows。
3. one-row dictionary short selected value 持大 unused values capacity：>96MiB 拒绝；keyNULL/valueNULL分别准确，不扫描 unused keys/values。
4. expected Json 对 actual ordinary Utf8，或 caller缓存 Json 对 actual ordinary metadata，必须拒绝；nested List/Map/Struct 同类错位/缺孩子也拒绝。
5. Binary ff/00、Decimal beyond i64/nonzero scale、negative timestamp/nanosecond/zone raw roundtrip；不用 user-variable text 控制 oracle。
6. selected sliced List/Struct/Map offsets、nullable container、empty children；count/emit精确同值，失败在下一 stack growth/输出前。
7. 每 turn 很小 output、Yield、cancel、observer panic、context abort：原 Chunk/cursor alias 与 full input credit 留到 actual drop，不能用逻辑 InputComplete 代替物理退出。
8. 首行已 staged/编码后第二行失败，FE session 不提交；Host gate 与当前 unsupported domain oracle 保持到完整接线验证。
