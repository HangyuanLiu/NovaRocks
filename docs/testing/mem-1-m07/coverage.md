# P00 源头、接管与退出清单

P00 历史审查基线：`eb35251de575e071ad3657d0ce0fc1fc95d1a91a`。原始表保留当时事实与待安装保护；P06 新实现与未闭合项见“P06 逐源交接”；没有把 source review 当成容量或产品验收。路径相对仓库根。

## 应用结果

| source / 当前接点 | 目标用途 / 接收 owner | 增长前保护与最后释放 | 验证面 |
|---|---|---|---|
| 分布式 SELECT：`query-application/src/coordination/result_pump.rs:1353`；`execution/src/exec/operators/result_buffer_sink.rs:277`（均在 `novarocks/` 下） | ClientRows；BE encoder→context root→FE window→MySQL | 当前全批 Arrow decode / row Vec；新 input/hydrate/cursor/staging/segment/replay/send-alias 在接受 batch 前受界，context 拥有生产退出后的留存，最后实际 alias 退出才释放 | C2/C4/C5/C7；complex-type/function/decimal/low-cardinality/sort/limit/aggregate |
| EXPLAIN ANALYZE：`novarocks/frontend-application/src/coordinator/execution.rs:1027,1358`；`query_execution/completion.rs:488` | CountOnly；actor outcome | 当前先收集 batches 再取 row_count；新计数不得 hydrate/render/cell traversal，overflow 显式失败，仍等 root Finished+本地 End+seal | C4/C6/C7；optimizer / aggregate / sort |
| local SHOW / information_schema / EXPLAIN：`novarocks/query-application/src/system_catalog.rs`、`novarocks/frontend-application/src/catalog_application/system_catalog_facts.rs:97` | ClientRows；有限 local producer | 名称/list/schema/Arrow 在构造前限制，不从完整 QueryResult 后切片；local 能力独立于 distributed permit；writer/aliases 退出后归还 | C6/C7；session/iceberg/iceberg-ddl/optimizer |
| scalar：`novarocks/frontend-application/src/query.rs:2620,2653,2666` | InternalFacts(Scalar)；有限 typed collector | 现有一行一列拒绝仅界行数；batch clone/literal copy、staged/live session 同时驻留必须在放行前承诺；成功后才能提交 session | C5/C6/C7；session/function/decimal |
| user variables：`novarocks/query-application/src/sql/session.rs:121`；`frontend-application/src/query.rs:1321` | Scalar domain / local ClientRows | 当前 map 和单值无总字节界，staged state clone 造成共存；单值/条数/总量/replace overlap 在 clone/insert 前限制；generation 收尾后退休 staged owner | C6；session/user_variable |
| COW：`novarocks/frontend-application/src/query_execution/row_mutation.rs:147–191`、`dml/mutation_flow.rs:900,916` | InternalFacts(CowMatch)，准确 schema/cast/digest/uniqueness | 当前 collector 为后置界且 mutation flow 先全量 QueryResult 再 cast；source 批次、cast、RowConverter、去重和 retained 总量须在接受/转换前共存授权；commit/abort 沿原 writer effect owner 收尾 | C6/C7；iceberg-dml/iceberg-rest |
| statistics：`novarocks/frontend-application/src/query_execution/statistics.rs`、`query_execution/artifact.rs` | InternalFacts(StatisticsArtifact) | 可复用现有 artifact 条数/128MiB 类约束；新增分段 assembly、decode/workspace 前置界，大记录未完整时有限接管再 ACK，不允许等 record>S 永远装不下窗口 | C6；statistics |
| write commit：`novarocks/frontend-application/src/query_execution/write_result.rs`、`novarocks/execution/src/exec/operators/table_finish.rs` | InternalFacts(PreparedWriteSet) | prepared-set 现有限界保留；准确领域编码前置检查、assembly/collector 共存；End 不能代替 finisher/commit 事实 | C4/C6/C7；iceberg-dml/iceberg-rest/distributed-writer |
| MV / readiness：MV repository `list_projections`，`readiness.rs:809`；SHOW MV `flow.rs:326` | ClientRows / 内部领域 / CountOnly，按消费者明确选择 | 当前全量 projection list，再 sort/Arrow；repository/list/source、维护 command/job diagnostic 需增长前有限；业务锁/效果与通用输出 owner 分开 | C6/C7；materialized-view/iceberg-ivm/table-maintenance |
| Iceberg listing：`novarocks/connector/iceberg/src/catalog/delegate.rs:88,117`，`vendor/iceberg-catalog-rest-0.9.0/src/catalog.rs:1652` | FE local metadata source | SDK list_tables 跨页累加 Vec；虽存在 `catalog/rest.rs:135` 单页 API，当前 SQL consumer 仍用全量。P06 必须改变分页/响应/list 增长入口，末端限制无效 | C6；iceberg/iceberg-ddl；REST 页面/响应越界 |
| Paimon listing：`novarocks/connector/paimon/src/catalog.rs:136`、`vendor/paimon-0.3.0/src/catalog/filesystem.rs:159` | FE local metadata source | 当前 filesystem list_status 全量后复制 names；计数/名称总量/SDK 响应界先于分配。private provider codec 与 read-only 能力保持 | C6；paimon（显式外部 fixture） |

原 P00 记录的 SDK / vendor 调整范围属于历史约束。revision 6 的 P06 只使用 SDK 公开参数或 NovaRocks 调用前后检查，不新增或扩大第三方补丁；Paimon 既有 vendor 补丁限定在 ADR-0138 范围。源头尚未受界时保持现有 FE 保护。

## P06 逐源交接（revision 6）

依据 approved plan/spec revision 6 的 P06 与 §5.6，核对本地检查点
`603bf9678`、`eb3b008f3`、`60c52341f`、`5c8a9b64f`、`9eb733363`。
P07 起步 `51fce8fcc` 只新增 carrier 声明、actor 校验与 metadata 模块。
“已有”指代码与既有定向测试覆盖，不表示生产 caller 全部接线、P00b 测量完成、
P08 切换或 P09 的 1FE+3BE 产品验收通过。本次文档补充未重跑 Cargo。

`LocalResultBound::V1` 为 65,536 行、32 MiB、4,096 列。字符串 builder 在整行 append
前计算值字节与每 cell 5 B offset/validity 开销；Arrow amortized buffer 的额外容量归
profile workspace，逻辑字节不等于 backing。`ConnectorListingBound::V1` 为 65,536 项、
请求每页 256 项、最多 1,024 页、单名称 64 KiB、名称总量 16 MiB、token 4 KiB。
超界拒绝完整结果，不截断。最后释放指容器及实际别名退出，不能由 End、ACK、成功或超时代替。
下表路径均在 `novarocks/` 下，符号为检索锚点，行号为此次 review 定位。

| source / 接点 | output kind → consumer | 已有保护 / 最后释放 owner | 仍待闭合 / 验证面 |
|---|---|---|---|
| 分布式 SELECT：`query-application/src/coordination/root_relay.rs`；`api/result.rs` 的 `ResultRowCarrier` / `RootSegmentDelivery` | ClientRows → actor → MySQL framing | P05 有序 segment/窗口 alias/receipt 已有；delivery 完成时 body/alias 先退出再发消费 receipt。P07 起步拒绝与声明不符的 batch/segment | preparation→carrier、生产 relay writer 属 P07；旧 Arrow/decode 退出属 P08。closing writer 的独立 holder/drain 不能由 receipt 代替；C5/C7/P09 |
| `SET @v=(subquery)`：`query-application/src/sql/user_variable.rs:75` 的 `scalar_record_to_user_variable_literal` | InternalFacts(Scalar / ScalarValueV1) → typed collector → session literal | owned scalar decode 在分配前累计检查 child Vec 容量与 variable leaves ≤128 KiB；literal 借用两遍遍历，先计数再建单个 String，output 与固定 leaf bridge scratch 共用 128 KiB 界；NoRows=`null`、一行 NULL=`NULL`，原类型拒绝保持。decoded value 随转换退出，literal 移交 staged session | typed stream consumer 已接 ScalarValueV1、准确 schema、单记录与 receipt/success 门；真实 relay 的 sealed End 计数已校验 Value/NULL/NoRows。生产 preparation 尚未选择 Scalar sink，旧 carrier/scratch 仍留到 P08；当前测试不证明生产 typed sink 或 socket 行为；C6/session/function/decimal |
| user variables：`query-application/src/sql/session.rs:130` 的 `set_user_variable`；FE governed SET | Scalar session domain → staged/live state | 名称+表达式总量 ≤128 KiB、数量 ≤64；替换先扣旧值，拒绝不修改 map。staged/live 各同界；替换值随 insert 退出，staged 随提交/撤销退出，live 随 session 退出 | 字符串长度不是容器物理容量；candidate literal 已在调用前存在，staged/live/转换副本共存须 P07/P08 核对，单份 128 KiB 不是全部峰值；C6 |
| COW match：`frontend-application/src/query_execution/row_mutation.rs:270` 的 `RelayedCowSelectionCollector`；`native-adapter/src/root_cow_selection_codec.rs:978` | InternalFacts(CowMatch / CowSelectionArrowV1) → bounded collector → selection / match validator | assembly 长度界=min(collector budget, codec 256 MiB, Internal assembly 32 MiB)；完整 BATCH 立即 decode、cast signed layout、交给 collector，不保留第二份完整 stream。retain 前核对 rows/array bytes，End 拒绝半记录。临时 assembly/decoder 退出；selection batches 移交 mutation effect owner，到 commit/abort 和最后 alias 退出释放 | UPDATE/MERGE 已通过专用 CowMatch request/outcome 在 coordinator 中逐批接入 collector；旧 QueryResult 转换 helper 仅作 test reference。V1 body/End 已接线，End 行数、半记录、签名与目标唯一性在 success seal 前验证；生产 sink/window 切换仍待 P08。decode/cast/retained batch、RowConverter、唯一性/digest 共存与有限执行位置仍需核实，retain 检查不能写成全部转换事前授权；C6/C7/iceberg-dml |
| statistics：`frontend-application/src/query_execution/statistics.rs:622` 的 `apply_record` | InternalFacts(StatisticsArtifact) → decoder → publication owner | 显式 record view；artifact 身份/成员/重复/总 body 界在 copy body 前核验；finish 要求 EOF、all-success、完整成员。decoder draft 移交 consumer，最后 draft/body aliases 退出释放 | assembly/分段 receipt/生产 coordinator 属 P07；apply_record 测试不是生产 caller。记录可大于 S，不能等整记录留在 W×S 窗口；C6/statistics |
| write commit：`frontend-application/src/query_execution/write_result.rs:334` 的 `apply_record` | InternalFacts(PreparedWriteSet / PreparedWriteCommitV1) → decoder → original finisher/publication owner | 复用 summary/target/fragment/artifact 领域校验与条数/单值/总量限制；assembly/decoder 退出临时状态，prepared set/artifact body 随原效果 owner 持到 commit/abort 与最后 alias 退出 | record assembly/旧全量 coordinator 收敛属 P07；End 不证明 finisher/commit。collector+转换准入/drain 需集成证据；C6/C7/distributed-writer |
| EXPLAIN ANALYZE：FE `query_execution/completion.rs` / `coordinator/execution.rs` | CountOnly → checked count/outcome → local ClientRows 文本 | BE CountOnly producer/合同不要求客户端 renderer；local helper 可拒绝超界 profile 文本，文本结果随 writer 最后引用退出 | completion/profile outcome 已改为 u64 count + profiles；V1 coordinator CountOnly 不持有 batch，旧 Decoded transition 仍物化到 P08；不能称生产 SQL 已无 hydrate/无行物化。须 Finished+本地 End+seal 与晚失败证据；C6/C7/P09 |
| SHOW/EXPLAIN/管理：`query-application/src/api/local_result.rs:119` 的 `LocalTableBuilder`；`api/result.rs:231` 起 helpers | local ClientRows → immediate result → MySQL | 整行 append Arrow buffer 前核对行宽、必填列、总行数/逻辑字节；拒绝行不追加。finish 移交 arrays 到 QueryResult，最后 result/writer alias 退出释放 | helpers 参数仍为已形成 Vec，不能证明此前 list/text 构造事前受界。EXPLAIN formatter 文本与已驻留计划成正比，拒绝点在 helper；renderer/能力/协议尾部属 P07/P08；C6/C7 |
| 已形成 Arrow 的 local source：`query-application/src/api/result.rs:210` 的 `build_arrow_query_result`；`local_result.rs:80` 的 `check_arrays` | local ClientRows → immediate result | 发布前检查列数/行数/数组 memory size；拒绝 arrays 随参数退出，成功移交 result | 检查在数组构造之后，source 必须另有结构界；新动态 source 先计量或用 builder。schema/name 和原始副本需单独核实；C6 |
| SHOW [FULL] PROCESSLIST：`frontend-application/src/query.rs:460` | local ClientRows → immediate result | snapshot 共享 statement text，FULL 借用全文；从 snapshot 算总量后建数组。结果随 writer 退出，shared statement 依 session owner 退出 | snapshot/非 FULL 截断临时 Vec 在预检前存在，须连接/session 界覆盖；连接容量/closing 属 P07/P08；C6 |
| remove_orphan_files：`frontend-application/src/table_maintenance/worker.rs:359` 的 `cleanup_candidate_locations` | local ClientRows report → maintenance result；删除归 cleanup effect owner | 固定 manifest candidate_count 在首读页前核对；每页请求 1,024 candidate，location clone 前核对报告字节。在首批删除前生成报告，超界不删任何对象。locations 移交 report/result，最后引用退出释放 | 候选页自身、report→Arrow 共存与 writer/drain 需集成核对；C6/table-maintenance |
| information_schema schemata/tables：FE `catalog_application/system_catalog_facts.rs:53,76`；QA `system_catalog.rs:141,194` | bounded snapshot → virtual source（客户查询最后为 ClientRows） | local schema 复制前借用检查；一个 external catalog 的 namespaces+全部 tables 共用预算，table 连 schema 名计费，后续 listing 带 remaining bound；超界拒绝整快照。snapshot/行 Vec/Arrow 依 source/result 最后引用退出 | 预算是单 catalog 快照，不是进程总量；system_catalog 仍先复制 snapshot 成行 Vec 再进 helper，catalog 名重复/行对象/Arrow 共存需包络。SDK 限制见后表；C6 |
| SHOW MATERIALIZED VIEWS / information_schema MV：FE `mv/domain/analysis_adapter.rs:125,371`；`catalog_application/information_schema.rs:119` | local ClientRows / virtual source → builder/result | projection list 返回后先检查条数，再读 dependencies/建行；借用 row 写入 builder，Arrow append 有总字节界。projection/row Vec 退出临时副本，result aliases 退出释放 | repository 已先克隆全部 projection（SHOW 用 listable，information_schema 用 ready），与已准入 MV 定义成正比；LocalResultBound 不在此 clone 前生效。dependency/diagnostic row 峰值需原领域界和包络；C6 |
| SHOW VIEWS：`query-application/src/view_service.rs:188`；Iceberg `catalog_control/views.rs` | local ClientRows → SHOW rows/helper | local registry 在逐名称 clone 前计数/计字节；external request 携带 listing bound。registry 原事实归 view owner；names/rows/result 各到最后引用退出 | external SDK 峰值见后表；SHOW rows/helper 共存、protocol tail 需 P07/P08；C6 |

### Connector listing 的公开接口边界

| source → consumer | 已落实的自有 retained 界 / 最后退出 | 公开接口限制与未闭合项 |
|---|---|---|
| REST tables：`connector/iceberg/src/catalog/rest.rs:138,165` → SHOW/system facts/document discovery | 请求 page_entries，经 `ConnectorListingCollector::accept_page` 逐页检查后 retain，token/页数错误在下一页前拒绝；临时 SDK 页/names 退出，最终 Vec 跟 SQL consumer 到最后引用退出 | SDK 已先 receive/deserialize 一页，未证明响应字节上限。`5d54685ee` 已修复忽略 pageSize 的 server：无 continuation 的最终页按累计总条数/名称字节界接受；带 continuation 的超请求页在下次读取前拒绝。document discovery 仍在 table loads 前独立检查页/总界；HTTP mock 9 项通过，生产 SQL 验收待 P09 |
| REST namespace/view、Hive namespace/table/view：`connector/iceberg/src/catalog/delegate.rs:98,132` → metadata/SHOW/system facts | SDK 返回后在 push 自有 collector 前检查条数/名称量，拒绝完整列表；临时 SDK Vec 退出，保留 Vec 随 consumer 最后引用退出 | SDK 无对应公开分页/limit 时完整 list 已在内部形成；没有反序列化事前 bounded 证据。自有 retain 受界不证明 SDK 峰值闭合；需公开配置/准入证据或明确拒绝不可支持规模 |
| Hadoop：`connector/iceberg/src/hadoop_catalog.rs:242,272` 的 read listing；`catalog/hadoop.rs` → metadata/SHOW/system facts | list_directories 返回后先检查目录条数/名称量，再 child probe/retain table；普通 trait 路径再经 delegate。directory Vec/probe 输出退出，保留 names 随 consumer 退出 | 完整目录 Vec 先于检查形成；不能称 filesystem/object-store 枚举前授权。read binding 和普通 delegate 两条路径均须覆盖 |
| Paimon：`connector/paimon/src/catalog.rs:89,105` → role metadata → SHOW/system facts | plain list 返回后 retain_listing 整体检查，不截断，cancellation checkpoint 保持；SDK names/collector 退出，最终 entries/SQL names 到最后引用退出 | SDK filesystem 无分页参数，调用内完整枚举已物化；超界拒绝单测不证明枚举/反序列化分配前硬界。不扩大 ADR-0138 vendor 修改范围 |

### 后续收敛要求

1. P07 逐 consumer 安装准确 kind/carrier、assembly、receipt 与 success/effect 门，核对
   input/转换/collector 的最大共存与最后 owner 退出。helper publication 检查、SDK 返回后检查、
   领域 retain 检查分别留证据，不能统称 source 全链事前保护。
2. P00b 修订下方历史 Native holder 表及传输测量口径；本节不冻结传输系数，也不声称第三方
   内部逐字节授权。P08 只有在 producer/consumer/connection/session/closing 全部保护核对后才能撤旧保护。
3. SPI listing 接口已改变，收敛点需 workspace 全量验证；C6 定向结果与 P07/P09 原生
   1FE+3BE 功能/取消/业务效果收据分开，source review/all-in-one smoke 不替代产品验收。

## Native 与真实 holder

| 对象 | 当前事实 | 待安装与退出 oracle |
|---|---|---|
| root placement | `novarocks/frontend-application/src/coordinator/scheduler.rs:251,259–272` root count=1；preferred 按 query id 选择，但没有跨查询均衡合同 | 每 BE 可集中全部已放行 root；多 FE 聚合与 rpc tails 单独 checked，不使用 C/3 |
| FE fetch gate | `native/data_runtime.rs:20` 16；`fragment_transport.rs:457–485` 包住 channel/RPC/分类 | 原放行事务预承诺完整 transport；fetch/可选 ACK 单在途，普通窗持到短 transport/alias 实际退出 |
| FE Channel | `native/data_runtime.rs:40,143–188` endpoint-only cache；generation 防旧驱逐 | process+endpoint+lane+connection generation，single-flight；每 lane 连接/队列/stream/reconnect/closing 上界，body EOF/RST/实际退出后还 stream |
| BE listener | `novarocks/native-adapter/src/native_server.rs:331–393` accept 后直接 spawn；TLS accept 无 timeout | data/control 独立 accept/handshake/closing；认证前取位置，绝对期限不随滴流延长；真实 task/connection 退出后还位，保留 control FD/headroom |
| ingress | ordinary8+8/control4+4；status/Exchange 建立后普通 permit 释放 | result/submission/observation/lifecycle 准确 route；持续 stream 另持真实 stream 位置；新工作不借控制保底 |
| response aliases | `native_ingress.rs:605–663` `OwnedResponseBytes` 持所有权到最后 backing Drop | 复用真正正确的 unary ownership；new prost Bytes/body/replay/ACK-pop 不能脱离实际 input/scratch/backing 防护 |
| BE→BE | `native_client.rs:138–199,221–262` ExchangeUnary / filter 与 endpoint cache；旧 streaming Exchange 每流 4096 队列（`backend_rpc_service.rs:111–162`） | 数据 listener 聚合界包括准确 registry/task topology 的 BE→BE 连接/frame/alias/reconnect；旧 streaming route 必须有界或明确拒绝，不能漏记 |
| producer retirement | `novarocks/worker/src/task_registry.rs` 当前 task 退休删除结果 | 新 producer Finish=End 发布+编码退出+context 接管；task horizon 不删除通道。Release 先封 fetch/replay/drop/wake，再等 holder，不能自等尚未关闭的 long poll |

## 编码语义调查

客户端生产走 `mysql-adapter/result_writer.rs → build_mysql_row`；`types` 中 MysqlText helper 没有生产 caller，不能作为已验证 BE encoder。

| 类型 / 接点 | 当前事实 | P02 冻结纪律 |
|---|---|---|
| Decimal128 | completed read `preparation/description.rs:58–62` 的 logical_type=None，正常走准确128 formatter；declared Decimal 分支不接受Arrow decimal | 不能写成普通 Decimal SELECT 必坏；消除分支二义性，独立期望 bytes；不走 f64 降级 |
| Decimal256 / LargeList | Native/内部词汇支持，客户端顶层 metadata/row 缺完整分支 | 准备时按显式能力 fail fast；如果要新增对外支持，先记录并裁决，不借搬移 renderer 默改支持集合 |
| negative TIME | 当前 vendor text encode 明确拒绝，types formatter 支持 | 明确现有行为与目标支持集合；不得因选择 helper 偶然变更 |
| timestamp | 客户端 UTC/微秒，ns先除1000；types helper按unit输出并可能附tz，负ms/ns可能fallback epoch | profile/合同准确呈现；legacy helper 非 oracle，不静默改 time precision / zone |
| Variant / opaque | LargeBinary 客户端 raw bytes；types Variant→JSON；部分 opaque 仍按列名猜NULL | NativeType 取代猜名字；真实可达用例/独立期待先调查，发现语义变化按spec裁决 |
| nested | 当前 recursive String 全值物化；MySQL map 保持插入顺序 | 有限 depth/elements/count/emit cursor；保留顺序和NULL，不把 HTTP map排序规则搬到客户端 |

这张表没有接受新的客户端语义，也没有修改 golden；冲突必须在准确可达输入上重现后裁决。
