# P00 源头、接管与退出清单

P00 历史审查基线：`eb35251de575e071ad3657d0ce0fc1fc95d1a91a`。原始表保留当时事实与待安装保护；P06 新实现与未闭合项见“P06 逐源交接”；没有把 source review 当成容量或产品验收。路径相对仓库根。

## P08 当前切换增量（2026-10-08）

FE 已切到 count-only WorkloadControl；协议与 supervisor 不携带 LRA。ResultCredit/decoded delivery/unfrozen carrier/raw MySQL Arrow writer、行复制 API 及未消费的 result-credit description 字段已删除。ClientRows 中继与 Local 固定窗口 writer 为唯一行发送入口，未经原 owner 包装的 application-local QueryResult 在 metadata 之前被拒绝。日期 sentinel、opaque 名称、IPC domain 与 dictionary/null 回归已迁到 production bounded encoder；旧 binary-value/Arrow writer 接口专属测试随接口退役。

`2366cfc92` 的 clean C0 与原生 1FE+3BE 十项通过见 `evidence/p08-counted-carrier-convergence-20261008.json`；本次 raw writer 删除之后还须补 clean C0/native。P00b/P09/P10 的性能、集中 root、多 FE 与传输测量门仍未完成。下列原始表继续仅代表 P00 历史基线。

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
| MV / readiness：bounded projection inventory / per-item fresh lookup；SHOW MV | local ClientRows / 内部领域 / CountOnly，按消费者明确选择 | local snapshot/raw page/decode 在源头明确给界；planning/background full reads 与 local 结果路径分开。readiness 仍以 fresh exact version 为管理门，不使用 inventory 旧值 | C6/C7；materialized-view/iceberg-ivm/table-maintenance；bounded dependency snapshot 见下方证据 |
| Iceberg listing：`novarocks/connector/iceberg/src/catalog/delegate.rs:88,117`，`vendor/iceberg-catalog-rest-0.9.0/src/catalog.rs:1652` | FE local metadata source | SDK list_tables 跨页累加 Vec；虽存在 `catalog/rest.rs:135` 单页 API，当前 SQL consumer 仍用全量。P06 必须改变分页/响应/list 增长入口，末端限制无效 | C6；iceberg/iceberg-ddl；REST 页面/响应越界 |
| Paimon listing：`novarocks/connector/paimon/src/catalog.rs:136`、`vendor/paimon-0.3.0/src/catalog/filesystem.rs:159` | FE local metadata source | 当前 filesystem list_status 全量后复制 names；计数/名称总量/SDK 响应界先于分配。private provider codec 与 read-only 能力保持 | C6；paimon（显式外部 fixture） |

原 P00 记录的 SDK / vendor 调整范围属于历史约束。revision 6 的 P06 只使用 SDK 公开参数或 NovaRocks 调用前后检查，不新增或扩大第三方补丁；Paimon 既有 vendor 补丁限定在 ADR-0138 范围。源头尚未受界时保持现有 FE 保护。

## P06/P06s 逐源交接（revision 7）

依据 approved plan/spec revision 6 的 P06 与 §5.6，核对本地检查点
`603bf9678`、`eb3b008f3`、`60c52341f`、`5c8a9b64f`、`9eb733363`。
P07 起步 `51fce8fcc` 只新增 carrier 声明、actor 校验与 metadata 模块。
“已有”指代码与既有定向测试覆盖，不表示生产 caller 全部接线、P00b 测量完成、
P08 切换或 P09 的 1FE+3BE 产品验收通过。本次文档补充未重跑 Cargo。

`LocalResultBound::V1` 为 65,536 行、32 MiB、4,096 列。字符串 builder 在整行 append
前计算值字节与每 cell 5 B offset/validity 开销；新分段 buffer 的实际 capacities 单独受
collector 界，旧/新 tail 拷贝与 incoming row 共存先检查 workspace。逻辑字节不等于 backing。`ConnectorListingBound::V1` 为 65,536 项、
请求每页 256 项、最多 1,024 页、单名称 64 KiB、名称总量 16 MiB、token 4 KiB。
超界拒绝完整结果，不截断。最后释放指容器及实际别名退出，不能由 End、ACK、成功或超时代替。
下表路径均在 `novarocks/` 下，符号为检索锚点，行号为此次 review 定位。

| source / 接点 | output kind → consumer | 已有保护 / 最后释放 owner | 仍待闭合 / 验证面 |
|---|---|---|---|
| 分布式 SELECT：`query-application/src/coordination/root_relay.rs`；`api/result.rs` 的 `ResultRowCarrier` / `RootSegmentDelivery` | ClientRows → actor → MySQL framing | P05 有序 segment/窗口 alias/receipt 已有；delivery 完成时 body/alias 先退出再发消费 receipt。P07 起步拒绝与声明不符的 batch/segment | preparation→carrier、生产 relay writer 属 P07；旧 Arrow/decode 退出属 P08。closing writer 的独立 holder/drain 不能由 receipt 代替；C5/C7/P09 |
| `SET @v=(subquery)`：`query-application/src/sql/user_variable.rs:75` 的 `scalar_record_to_user_variable_literal` | InternalFacts(Scalar / ScalarValueV1) → typed collector → session literal | owned scalar decode 在分配前累计检查 child Vec 容量与 variable leaves ≤128 KiB；literal 借用两遍遍历，先计数再建单个 String，output 与固定 leaf bridge scratch 共用 128 KiB 界；NoRows=`null`、一行 NULL=`NULL`，原类型拒绝保持。decoded value 随转换退出，literal 移交 staged session | typed stream consumer 已接 ScalarValueV1、准确 schema、单记录与 receipt/success 门；真实 relay 的 sealed End 计数已校验 Value/NULL/NoRows。生产 preparation 尚未选择 Scalar sink，旧 carrier/scratch 仍留到 P08；当前测试不证明生产 typed sink 或 socket 行为；C6/session/function/decimal |
| user variables：`query-application/src/sql/session.rs:130` 的 `set_user_variable`；FE governed SET | Scalar session domain → staged/live state | 名称+表达式总量 ≤128 KiB、数量 ≤64；替换先扣旧值，拒绝不修改 map。staged/live 各同界；替换值随 insert 退出，staged 随提交/撤销退出，live 随 session 退出 | 字符串长度不是容器物理容量；candidate literal 已在调用前存在，staged/live/转换副本共存须 P07/P08 核对，单份 128 KiB 不是全部峰值；C6 |
| COW match：`frontend-application/src/query_execution/row_mutation.rs:270` 的 `RelayedCowSelectionCollector`；`native-adapter/src/root_cow_selection_codec.rs:978` | InternalFacts(CowMatch / CowSelectionArrowV1) → bounded collector → selection / match validator | assembly 长度界=min(collector budget, codec 256 MiB, Internal assembly 32 MiB)；完整 BATCH 立即 decode、cast signed layout、交给 collector，不保留第二份完整 stream。retain 前核对 rows/array bytes，collector ≤64 MiB、1,048,576 行、4,096 batches；cast/decode 前核验 row/batch 声明，空 batch 同样占名额。End 拒绝半记录。临时 assembly/decoder 退出；selection batches 移交 mutation effect owner，到 commit/abort 和最后 alias 退出释放 | UPDATE/MERGE 已通过专用 CowMatch request/outcome 在 coordinator 中逐批接入 collector；旧 QueryResult 转换 helper 仅作 test reference。V1 body/End 已接线，End 行数、半记录、签名与目标唯一性在 success seal 前验证；生产 sink/window 切换仍待 P08。RowConverter constructor/child Rows/parent buffers 与唯一性 table/keys/digest 已有借用事前共存检查；root/每batch独立 schema metadata 与actual container capacities记入source。冷match合同16 MiB前置借用检查、streaming Debug digest保留旧seal。relay owned decoder与signed输出通过SPI受限构造owner取得source receipt，复制切断foreign backing/private spare；整树schema/cast/workspace借用预检先于payload构造，未证明类型对提前拒绝。旧decoded transition仍不具备该receipt；domain有限执行位置和完整Internal窗口未生产取得；C6/C7/iceberg-dml |
| statistics：`frontend-application/src/query_execution/statistics.rs:622` 的 `apply_record` | InternalFacts(StatisticsArtifact) → decoder → publication owner | 显式 record view；artifact 身份/成员/重复/总 body 界在 copy body 前核验；finish 要求 EOF、all-success、完整成员。decoder draft 移交 consumer，最后 draft/body aliases 退出释放 | assembly/分段 receipt/生产 coordinator 属 P07；apply_record 测试不是生产 caller。记录可大于 S，不能等整记录留在 W×S 窗口；C6/statistics |
| write commit：`frontend-application/src/query_execution/write_result.rs:334` 的 `apply_record` | InternalFacts(PreparedWriteSet / PreparedWriteCommitV1) → decoder → original finisher/publication owner | 复用 summary/target/fragment/artifact 领域校验与条数/单值/总量限制；assembly/decoder 退出临时状态，prepared set/artifact body 随原效果 owner 持到 commit/abort 与最后 alias 退出 | record assembly/旧全量 coordinator 收敛属 P07；End 不证明 finisher/commit。collector+转换准入/drain 需集成证据；C6/C7/distributed-writer |
| EXPLAIN ANALYZE：FE `query_execution/completion.rs` / `coordinator/execution.rs` | CountOnly → checked count/outcome → local ClientRows 文本 | BE CountOnly producer/合同不要求客户端 renderer；local helper 可拒绝超界 profile 文本，文本结果随 writer 最后引用退出 | completion/profile outcome 已改为 u64 count + profiles；V1 coordinator CountOnly 不持有 batch，旧 Decoded transition 仍物化到 P08；不能称生产 SQL 已无 hydrate/无行物化。须 Finished+本地 End+seal 与晚失败证据；C6/C7/P09 |
| SHOW/EXPLAIN/管理：`query-application/src/api/local_result.rs:119` 的 `LocalTableBuilder`；`api/result.rs:231` 起 helpers | local ClientRows → immediate result → MySQL | 整行 append 前核对行宽、必填列、总行数/逻辑字节；mandatory/slack growth 按实际 buffer capacities≤32 MiB，最多64 KiB tail替换在拷贝前核对workspace，压力compact不复制累计column。String schema/name事前预检；source scratch退出后finish构造有限Arrow buffers，单block直接移交，最后result/writer alias释放 | helpers 参数仍为已形成 Vec，不能证明此前 list/text 构造事前受界；required/nullable helper 在column Vec前核对列数、required row在Option Vec前核对宽度。逻辑/普通物理树EXPLAIN借用单pass生成，整个输出65,536lines/8MiB，actual String及header增长有界，tree索引分配前≤4MiB，cycle/depth拒绝且golden不改；Contract annotation/edge/cut显示借用；PhysicalPlan owner 的 FragmentIoCutIndex 与原 provenance/writer kernels 共用，checked whole-index/union Vec→Arc 峰值在首个分配前核对4MiB；不复制provider payload/proof/artifact。renderer/实际window alias/能力/协议尾部属 P07/P08；证据 evidence/p06-local-buffer-capacity.md 与 evidence/p06-explain-source-bounds.md；C6/C7 |
| 已形成 Arrow 的 local source：`query-application/src/api/result.rs:210` 的 `build_arrow_query_result`；`local_result.rs:80` 的 `check_arrays` | local ClientRows → immediate result | 发布前检查列数/行数/数组 memory size；拒绝 arrays 随参数退出，成功移交 result | 检查在数组构造之后，source 必须另有结构界；新动态 source 先计量或用 builder。schema/name 和原始副本需单独核实；C6 |
| SHOW [FULL] PROCESSLIST：`frontend-application/src/query.rs:460` | local ClientRows → immediate result | snapshot 共享 statement text，FULL 借用全文；从 snapshot 算总量后建数组。结果随 writer 退出，shared statement 依 session owner 退出 | snapshot/非 FULL 截断临时 Vec 在预检前存在，须连接/session 界覆盖；连接容量/closing 属 P07/P08；C6 |
| remove_orphan_files：`frontend-application/src/table_maintenance/worker.rs:359` 的 `cleanup_candidate_locations` | local ClientRows report → maintenance result；删除归 cleanup effect owner | 固定 manifest candidate_count 在首读页前核对；每页请求 1,024 candidate，location clone 前核对报告字节。在首批删除前生成报告，超界不删任何对象。locations 移交 report/result，最后引用退出释放 | 候选页自身、report→Arrow 共存与 writer/drain 需集成核对；C6/table-maintenance |
| information_schema schemata/tables：FE `catalog_application/system_catalog_facts.rs:53,76`；QA `system_catalog.rs:141,194` | bounded snapshot → virtual source（客户查询最后为 ClientRows） | local schema 复制前借用检查；一个 external catalog 的 namespaces+全部 tables 共用预算，table 连 schema 名计费，后续 listing 带 remaining bound；超界拒绝整快照。snapshot依source owner退出；虚拟源借用固定row直接写入existing bounded builder，不先构造全量行Vec/String；下一次catalog/schema/name重复copy前核对整row与actual collector capacity≤32MiB，finish移交Arrow到最后result alias退出 | 预算是单catalog快照，不是进程总量；Client VALUES AST/TypedExpr/planner副本另属planning，未由该source小切片证明其整体包络或约100MiB/client高水位归因。SDK限制见后表；C6与clean efb C0（dev-opt 12298/0/7）、同HEAD dev真实REST native38phase PASS；AST/planner包络仍OPEN |
| SHOW MATERIALIZED VIEWS / information_schema MV：FE analysis_adapter / information_schema，MV bounded inventory/readiness | local ClientRows / virtual source → builder/result | 一个 read snapshot 单记录分页、16 MiB thin identity/4 MiB page；逐项 fresh lookup 保留 installed/reason；SHOW 排序 thin targets 后直接 append，无完整 domain row Vec；info_schema row Vec/workspace 先检查，compare/filter 借用，全部投影列合计后 exact-capacity Arrow build | SHOW dependency 单快照核对 exact downstream/CAS 与 canonical occurrences、逐记录接管 4 MiB collector，再用完整 bounded thin inventory 分类；16 MiB inventory/4 MiB collector/额外 decode/raw 归 workspace，borrowed sort/render；任何页/关闭失败拒绝，保留 mv:。生产 Local whole-window funding/实际 capacity 包络尚未验收。证据 evidence/p06-mv-bounded-source.md；C6 |
| SHOW VIEWS：`query-application/src/view_service.rs:188`；Iceberg `catalog_control/views.rs` | local ClientRows → SHOW rows/helper | local registry 在逐名称 clone 前计数/计字节；external request 携带 listing bound。registry 原事实归 view owner；names/rows/result 各到最后引用退出 | external SDK 峰值见后表；SHOW rows/helper 共存、protocol tail 需 P07/P08；C6 |

### Connector listing 的公开接口边界（revision 7 D15/D16）

当前SDK内部response/deserialization缓冲为D15受信端点例外，不要求所有第三方分配在构造前逐字节授权。
NovaRocks自有副本继续在增长前受V1条目/名字/collector界。Hive/Hadoop的view listing保持SDK
FeatureUnsupported，不制造空列表。先前裁决审查 `evidence/p06-sdk-listing-boundary.md` 保留为历史；
当前实现/定向收据见 [P06s](evidence/p06s-external-listing.md)，measurement/1FE+3BE尚未完成。

| source → consumer | 自有保护、调用准入与最后退出 | SDK例外与剩余验证 |
|---|---|---|
| REST tables/namespaces/views：`connector/iceberg/src/catalog/rest.rs` → SHOW/system facts/document discovery | 自有单页循环；每页借用V1检查后才copy，累计条目/名字/token/page界，重复token提前拒绝；一个generation共享8位置，覆盖完整loop及实际ctx绝对deadline/stop，SDK future退出后还位。temporary页退出，retained Vec到consumer最后引用退出 | REST公开connect5s/read30s、无整体clienttimeout；server忽略pageSize的terminal页仅整页在剩余界内接受，带continuation超请求页拒绝；SDK body/deserialize属D15，不能称事前硬字节界。组件反例已过，真实SQL/CL/P09待验收 |
| HMS namespaces/tables：`connector/iceberg/src/catalog/hive.rs` → metadata/SHOW/system facts | get_all_*调用经同一generation8位置与ctx deadline/stop；SDK返回项先借用V1核对再retain，失败整个结果拒绝，future先退出后还位 | 不新增HMS patch；保持framed选项及volo默认16MiB frame，pilota按声明长度预分配缺口仍为D15已知风险。view Unsupported；CL测量与P10上游跟踪待完成 |
| Hadoop：`connector/iceberg/src/{fs_io,hadoop_catalog}.rs` / `catalog/hadoop.rs` → metadata/SHOW/system facts | 原有borrowed过滤/去重、name/String/Vec old+new workspace界；new_with_binding用exact IO。新增generation8 gate/context期限、raw lister typed List overflow保留ResourceExhausted；能力到完整列表future退出 | 公开lister与远端List body16MiB cap已接；XML/SDK内部缓冲属D15。无binding的custom FileIO原Unsupported保持；历史source收据 evidence/p06-hadoop-stream-source.md，最新见P06s |
| Paimon：`connector/paimon/src/{catalog,catalog_listing,resources,role_binding}.rs` → role metadata → SHOW/system facts | 原有FileIO decorator在SDK Vec前核对32MiB source workspace；新共享generation8位置与实际ctx SDK run_until/deadline，SDK+bounded retain退出再还位；存在性判断列表失败准确返回错误，不推断false | FS/HMS内部SDK增长属D15；OpenDAL List body cap同下行。只读provider及ADR-0138 patch范围保持。组件drop/N+1/deadline/存在性反例通过；CL/真实外部fixture待验收 |
| 全endpoint OpenDAL `Operation::List`：`fs/src/{access,list_body_limit}.rs` | 公共HttpFetch/HttpClientLayer在Timeout/ConcurrentLimit/Retry之下数实际body≤16MiB，超界chunk在SDK read_all/XML前拒绝；typed cause为非temporary，Retry不重试。凭证client独立，非List不受此cap | cap是输入界，不证明transport chunk/XML/SDK事前allocator硬界。公开接缝与Operation扩展钉住反例已过；原生1FE+3BE ADD FILES exact16MiB到达empty-source检查，16MiB+1准确ResourceExhausted，两阶段各一次S3请求、零commit/delete、同连接复用及native资源收敛PASS（evidence/p09-opendal-native-pass-20261009.json）。进程采样不是SDK/XML硬字节界，真实provider规模CL仍OPEN |
| ADD FILES / 未锚定CTAS清理：`connector/iceberg/src/catalog_control/{add_files,unanchored_ctas_cleanup}.rs` | ADD FILES streaming lister，全部物理entry/name/workspace先计，合法file≤4096；整批超界不提交截断结果。CTAS streaming≤256删除batch，整体V1 discovery界，marker/root最后删除；首次删除前typed列表错误准确返回，删除后仍CommitUnknown并保留marker重试 | 枚举与删除效果语义反例已过；真实native业务效果待P09。SDK内部D15例外不免除NovaRocks自有增长前边界 |

### 后续收敛要求

1. P07 逐 consumer 安装准确 kind/carrier、assembly、receipt 与 success/effect 门，核对
   input/转换/collector 的最大共存与最后 owner 退出。helper publication 检查、SDK 返回后检查、
   领域 retain 检查分别留证据，不能统称 source 全链事前保护。
2. P00b 修订下方历史 Native holder 表及传输测量口径；本节不冻结传输系数，也不声称第三方
   内部逐字节授权。P08 只有在 producer/consumer/connection/session/closing 全部保护核对后才能撤旧保护。
3. SPI listing 接口已改变，收敛点需 workspace 全量验证；C6 定向结果与 P07/P09 原生
   1FE+3BE 功能/取消/业务效果收据分开，source review/all-in-one smoke 不替代产品验收。

## Native 与真实 holder（revision 6；Membership 补齐见 2026-10-08 收据）

自有 payload 以最后 backing alias 退出为准；第三方内部按公开配置、库外准入和公开退出事件。
配置结构算术见 [transport-envelope-v1.md](transport-envelope-v1.md)，系数与测量门尚未完成。
不沿用历史 v5 的逐连接2MiB断言或vendor task/allocator接缝。

| 对象 / owner | 当前接点与数量边界 | 退出与后续验收 |
|---|---|---|
| root placement | live registry冻结exact process；单BE可集中全部root。NativeResultSupportGeometry承载320 roots/FE/BE，不使用C/3 | P08 caller窗口/生产门与P09集中placement、真实退出证据仍需闭合 |
| FE fetch | `native/data_runtime.rs` / `fragment_transport.rs` 的旧16 gate保留至P08；R2 ResultData独立lane已有承载接口 | root只一个fetch/ACK在途；新窗口及body alias实际退出。旧gate退出只能在完整切换 |
| Channel / DNS | `frontend-application/src/native/transport.rs`、`native-adapter/src/native_channel_cache.rs`、`native-trust/src/adapter.rs`：exact process/endpoint/lane缓存、single-flight、连接/handshake/FD、有限DNS | IO wrapper Drop归还连接，DNS closure返回归还permit；body EOF/RST/Drop归还stream。GOAWAY不可见阶段继续算live |
| listener / ingress | `native_server.rs` / `native_transport_admission.rs`：Data/Control独立listener、认证前数量准入、绝对握手期限、身份封印与per-lane stream gate | 不借control reserve；持续response body持位到公开退出；半开、多FE重连风暴与真实控制进展属P00b/P09 |
| FE Membership incoming（OPEN） | `frontend-application/src/native/report_server.rs`调用未传admission的start；FE incoming_lane_stream_limit=0。上述BE listener准入不能外推到FE | 96 incoming连接目前只是目标算术，未接全进程连接/握手/stream/key gate；须补齐并复核handshake/FD/metrics后测量，evidence/p00b-membership-ingress-gap.md |
| response payload | 自有root payload通过Bytes owner维持实际backing与send aliases；ACK/EOS不等于最后alias退出 | 第三方内部copy不授自有容量；公开buffer/window常量进入结构式，析构差额由测量门验证 |
| BE→BE | exact peer process/endpoint/lane有限缓存；Exchange/RuntimeFilter数量按live registry与冻结geometry | 双方向dial/closing位置有限；不能以单进程测试推断生产退出或跳过真实peer数量 |
| producer / context | P04b统一root channel具有独立context ownership；task FINISHED与FE消费End/成功seal各自汇合 | release封fetch/replay并wake，等待实际holder；晚originating failure及生产SQL属后续原生验收 |

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

2026-10-08 FE Membership incoming已接同一个process admission：独立96 physical/32 bootstrap，
12288个stream到response body public exit；role/domain/class启动前核对，peer live quota等实际IO退出。
FE FD含accept拒绝瞬时socket共710；handshake总量160。定向与完整Native/Frontend库通过，
见 [Membership收据](evidence/p07-membership-ingress.md)。不构成Native cluster/测量或P08切换。

### P07 Local governed 接线（2026-10-08）

封闭Local source→原statement window→opaque graph→pure cursor→MySQL finite writer已接生产；旧decoded/protocol LRA随普通及closing实际writer保留。TCP半header驻留补尾/缺尾断连/初始取消/前序pending OK转交及下一generation反例通过；Query525、Frontend1446、MySQL71、opaque doctest1 PASS。见 `evidence/p07-local-delivery.md`。不将此结果扩展为ordinary terminal/全部Internal领域或原生1FE+3BE验收；P07/P08与后续门保持OPEN。


## P07 ordinary terminal 检查点（2026-10-08）

Ordinary governed OK、typed ERR 与 COM_INIT_DB 的实际 packet/flush/Closing owner 已接线；6项新增 TCP/partial/flush 反例与 MySQL77、Frontend1446、vendor lib150 PASS。范围/原始日志/失败与锁种子处理见 `evidence/p07-terminal-delivery.md`。整体 distributed/domain 矩阵与原生/性能门仍 OPEN；旧防护保留。

2026-10-09 request metadata cache：仅保留既有纯 IcebergAttemptTableAccess，FileIO/scope真实Drop与八并发single-flight回归通过；真实Hadoop读写/BE替换、REST vended读写/指定BE续租/定向deadline原生PASS。受控REST CL同冻结32×512 lake观察allocated peak694848456→76311360、after342812752→16339960 bytes；whole-process观察不替代SDK字节上界、实际job退出或跨provider验收。见 evidence/p09-request-cache-resource-exit-20261009.json 与 p09-request-cache-native-20261009.json。


2026-10-09 P09 correctness checkpoint：六份历史Decimal共同失败golden均由独立exact-literal/actual-DDL oracle推导，在old main及candidate完整原生核验后修正；原SQL与比较策略保留。完整Decimal20/20、617步PASS。另aggregate100/sort14/filter15/join64原生全部PASS（193case），见 `evidence/p09-decimal-complete-native-suite-20261009.json`、`evidence/p09-relational-native-sql-pass-20261009.json`。实际产品仍为63f16c67e，后续检查点只有测试/证据变化；不是最终同HEAD全CI。真实Paimon五项原生通过，snapshot因writer30s覆盖producer等待而失败；相关重大裁决待用户决定，大provider CL继续OPEN。

2026-10-09 statistics完整7/7原生1FE+3BE PASS，含真实Spark及Trino483 Puffin互通；actual63f16c67e，4PID退出，任务私有canonical shared fixture清理0。见 `evidence/p09-statistics-complete-native-pass-20261009.json`。不是大provider CL、release性能或最终同HEAD验收。

2026-10-09 additional native coverage：statistics7/63步、MV七suite33/537步、真实HMS1/6步已PASS；前台rewrite接线补齐后actual70dc18f8a完整Iceberg27/compatibility20/resilience13（60/536步）及membership/ingress11场景PASS，全部记录PID退出、私有fixture cleanup0。普通64承载/writer决定、real大provider CL、result-delivery新case、transport/CM/P00b/final仍OPEN；历史63 C0不能当作70dc全量。详见对应20261009 JSON收据。

2026-10-09 clean856e28f52前台rewrite C0全量PASS：12266/0fail/7ignored，524s，收据evidence/p09-foreground-rewrite-c0-pass-20261009.json。此次cargo-only不含native，不是final；MV rename六case原始输入已freeze v1，等待串行旧main/候选对照。

2026-10-09 MV rename对照v1被错误fixture凭据挡在INSERT，非rename证据；runner final fixture secret binding修复通过SQLlib269/harness7/focused2/fmt。输入freeze v2，原SQL/golden保留，server不变，native pending。

2026-10-09 MV rename six-case old-main native comparison: old9c7723bbe/candidate856e28f52相同步骤/相同guard五FAIL，unreferenced control两侧PASS；原SQL/golden不变，8PID退出cleanup0。见evidence/p09-mv-rename-old-main-native-comparison-20261009.json；确认已有失败，不说五PASS，非性能/final验收。

2026-10-09 P09 result-delivery两wire场景预冻结/实现/registry/build/fmt PASS，native尚未运行；200000小行跨S和16777474B单行跨U24，独立oracle见oracles/result_delivery_wire_oracle.py。仅wire与public owner oracle范围，其他P09/CM/CP门保持OPEN。

2026-10-09 result-delivery v2 native：200000小行准确wire与public owner barrier PASS（actual856）；大REPEAT超既有单字符串1MiB上限返回NULL，输入FAIL非编码结论，8PID退出，收据evidence/p09-result-wire-v2-diagnostics-20261009.json。v3在执行前冻结17合法1MiB列/17825860B大行，SQL功能目的及cap保持，native pending。

### 2026-10-09：C9 合法跨 U24 输入与探针缺口修正

- 干净 `54e850cfb`、实际产品 `856e28f52` 原生 1FE+3BE：200000 小行的行字节/行序/packet 与前后公开 owner 归零观察 PASS；17 个合法 1MiB 字符串组成的 17825860 B 单行在任何行发布前被 root original input backing 96MiB 检查拒绝，保持 FAIL。8 个启动 PID 均退出；收据 `p09-result-wire-v3-diagnostics-20261009.json` 保留原始观察与日志 hash。
- Arrow 58.2 IPC 将 offsets/values 切成共享 message body 的 Buffer；原 borrowed inspector 每个 alias 再计完整 capacity。修正在该纯借用检查内使用固定 64 项 `data_ptr()+capacity()` 缓存，同一个 payload backing 只收一次，所有 Buffer owner metadata/scaffold 仍逐项保守计入。缓存满后继续重复收，不按 len() 漏掉 sliced-away 容量，不修改 96MiB/节点/深度界、owner 责任或 driver 交付。custom declared region 仍需原 source owner 的完整 backing receipt，缓存不认证其任意私有 owner。
- 真正 IPC 17 列共享 backing、独立大 backing、缓存溢出三项反例与既有容量 introspection 共 23 PASS；检查区计数 allocator 均零分配。独立审查确认 fixed cache 有限结构工作及 custom source receipt 连续，尚未以此宣称原生修复 PASS。
- 历史 raw actor 只排除 ERR，畸形 ColumnDefinition41 `[3]` 可被当作列定义；因此历史 schema 正确性不宣称 PASS。强化探针检查六个 length-encoded 字符串和准确 fixed 12 B 区、filler/尾部；v4 在原 v3 SQL/行摘要/packet/全部容量和期限不变下新增独立冻结列名及 MySQL type。收敛 barrier 的第二次 idle 成功也须发生在既定 deadline 内。新原生复跑仍待执行。

### 2026-10-09：C9 两项强化 wire boundary 原生 PASS

- 干净源码与实际产品同为 `a4a6ce16bb3533839a81dbe841caa104669a46d0`，原生 1FE+3BE：200000 小行（1288895 B/200004 packets）和 17 列合法大行（17825860 B/22 packets）均 PASS。v4 仅新增独立列名/type oracle，原 SQL/行数据/全部 cap/deadline 不变；schema 结构、名字/type、sequence、精确字节摘要/行序及成功终态均验证。
- 每场景均实际创建 Native task；前后公开 FE governance/window 与 BE reservation/ingress 连续两次归零，8 个启动 PID 全部退出。收据 `p09-result-wire-v4-native-pass-20261009.json`。v2 输入错误、v3 backing 拒绝与探针审查历史保留，没有重写为 PASS。
- 这两项不证明全 C9、逐个晚 alias 的真实退出、RSS、transport envelope/CM/CP；P00b/P09/P10/final 与两个未收到答复的重大决定保持 OPEN。执行当前检查点的 cargo-only C0 收敛，再继续独立矩阵，无 push/PR/归档。

### 2026-10-09：共享 backing 检查点 C0 与协议/trust 原生收敛

- 干净 `7607dae64053ab853c47d827deb64eebddb29030` 的 dev cargo-only C0 全部 PASS，706s；component12091、Server owner178、binary smoke4，共12273 PASS/0 FAIL/7既有ignored。守卫/Cargo依赖政策/fmt/all-targets/System allocator/Clippy/build/错误清单通过；初次错误CLI组合在任何检查前被拒绝（2），原始诊断保留。收据 `p09-root-shared-backing-c0-pass-20261009.json`，没有把 Cargo-only 当作 runtime 验收。
- 同一 clean HEAD 与实际产品 `7607dae64` 的原生1FE+3BE：query-output 的 schema-once、non-negotiated-multi-statement、negotiated-multi-result、scalar-session；query-concurrency/terminal-releases-slot；native-trust 三项 plaintext/automatic/PEM 正常配置与三项 transport mismatch，11/11 PASS。44 个启动 PID 全部退出；无外部fixture更改。收据 `p09-c9-protocol-trust-native-pass-20261009.json`。名为 reject-jwt-domain-mismatch 的既有负例实际使用错误 automatic TLS reference，拒绝发生在 authenticated dispatch 前，不能据名称宣称另一种 JWT issuer 故障。
- P09其余直接 root 状态/晚 alias/ClosingRow/满池、真实 cross-provider CL、transport coefficients/CM/CP/P10/final 保持OPEN。ordinary支持承载与write deadline两个决定仍未得到答复；未改gate/cap/deadline/失败语义，goal active，无push/PR/归档。

### 2026-10-09：C9 authenticated root request refusal 切片

- 新注册 `result-delivery/root-read-profile-kind-refusal`；执行前冻结七个请求、精确 gRPC status 与 SUM1..100 健康查询字节摘要。结构合法的 V1 foreign-process baseline 必须到达准确身份拒绝(9)，其余六项 profile/kind/wanted 变体必须在结构 decode 返回3；全部共享同一合法foreign identity，避免不同身份掩盖判错阶段。
- probe限定 authenticated plaintext/IP；完整异步RPC包含absolute deadline，请求frame/响应DATA各≤4KiB，headers/第三方分配不在此界，Runtime teardown不冒充deadline内的物理退出。成功响应后的driver真实abort/join；授权不入观察。后续健康查询要求真实Native task、独立字节/schema oracle与公开owner连续归零。scope不覆盖malformed replies/installed-root ACK/retirement/closing。
- 首轮helper重复mutable借用编译失败已保留并修正；最后完整system runner96 PASS/0 FAIL/2既有ignored；独立只读审查无默认IP场景PASS漏洞。收据 `p09-root-refusal-focused-20261009.json`。原生尚待执行，未改产品code/cap/deadline。

### 2026-10-09：C9 root request refusal 原生 PASS

- 干净源码/runner `27899ebd98c008e80b06dd3bdcea0faf4e953d7a`，实际产品仍为已通过C0的 `7607dae64053ab853c47d827deb64eebddb29030`；两者间只有tests/docs/evidence，产品源码未变。原生1FE+3BE的七项请求精确status为9/3/3/3/3/3/3，随后真实Native SUM健康查询的schema/行字节/独立hash正确；前后公开owner连续两次归零，4 PID真实退出。
- 收据 `p09-root-refusal-native-pass-20261009.json` 保留原始probe/hash/实际二进制身份。只关闭 authenticated plaintext/IP 请求profile/kind/sequence/foreign-process拒绝，不冒充 installed-root ACK/replay/生产退休/context留存/ClosingRow、非法回复或全C9验收。其余门、两个待裁决决定与goal保持OPEN，无push/PR/归档。


### 2026-10-09：P09 context-owned root 直接观测与冻结场景

- Worker 新增只读固定 1024 个 context/root 扫描位置的 census，registry/root 均 try_lock，不 clone payload、不触发 callback；busy/覆盖不足/聚合溢出使整份观测 unavailable，poison 明确错误。Backend role-local 指标使用固定 resource labels，不可用时省略 ownership family，历史 Gauge 不冒充新快照。逻辑字段在各 root 锁下一致，物理计数为独立 atomic 样本。
- 观测 producer guard exit、task 退休记录、Data/End/ACK/seal 与原 segment/read/send/reservation/metadata holders。范围只到 context 当前持有的 root；移出 context 的固定 core backing、其它 Arc tail、线程/allocator dealloc 不在此 census，归零不证明全部 physical backing 最后退出。
- 新 `result-delivery/producer-exit-context-retention` 已在首次 native 执行前冻结：一个 1MiB 字符串形成 S+8 Native 行字节；校验 metadata 后停止读行，连续两次要求一个 context-held root 的 W=2、End 已发布但未ACK、producer guard exited、task 已退休，恢复后检查独立 frozen schema/row bytes/packet/digest。客户端 connect 前申请4KiB SO_RCVBUF并记录OS应用值，不外推真实TCP window/第三方容量，不修改server profile。
- Worker lib322、Backend metrics16、system runner98 PASS/0 FAIL/2既有ignored；前两次 alias counter 预期及第3次 test fixture scan coverage 错误均保留日志并修正。独立只读审查和 Python literal oracle 一致。收据 `p09-root-context-retention-focused-20261009.json`；原生尚未执行。P09其它门/P00b/P10/final及两个待裁决决定仍OPEN，goal active，无push/PR/归档。


### 2026-10-09：P09 producer-exit/context-retention 原生与 C0 PASS

- 干净 `9695b3965fed7996448e79fc4ac156762622b931`、实际相同 build identity，原生1FE+3BE新场景 PASS（4.939s）。三次census中最后两次连续命中：仅BE1持一个root，task退休记录1、producer guard exited1/running0、Data positions2/physical segments2、payload1048584B、End已发布但未ACK、sealed0。恢复后MySQL一行1048580B/5packets，严格schema与独立SHA256一致；前后公开owner barrier通过，4个精确PID均退出。
- 相同干净源码的dev cargo-only C0 PASS（563s），12280 PASS/0 FAIL/7既有ignored；守卫、fmt、all-targets、System allocator、Clippy、build、error manifest、component、Server owner、binary smoke通过。C0有一处新增test-only from_ref建议，拟作窄清理；无产品语义失败。原始目录 `logs/ci-full/20261009-065102`。
- 收据 `p09-root-context-retention-native-pass-20261009.json` 与 `p09-root-context-retention-c0-pass-20261009.json` 保留准确source/binary身份、采样与全部raw hashes。只证明context-held阶段与正常wire/公开owner收敛；归零不证明移出context的fixed backing/Arc tail/allocator最后退出。ACK-only、ClosingRow具体容量/partial矩阵、real跨provider CL、transport/P00b/P10/final及两个待裁决决定仍OPEN，goal active，无push/PR/归档。


### 2026-10-09：P09 paused-client 取消场景，native 待执行

- 首次 native 前冻结 `root-cancel-closing-freeze-v1.json`：原 S+8 单行暂停客户端读取，要求独立 Closing=1/Client=0、完整 row+ERR1317、同 socket 精确 SUM oracle 与实际 Native task 增长；原17×1MiB合法行冻结到W=2未完成producer，要求正 wire prefix/零完整行、物理EOF/reset、同 socket 后续零schema/packet/bytes且三BE task counter不增长。握手真实connection ID用于KILL，无猜测。
- KILL start/return/resume时间保存；返回后复查原2s观察截止，Closing观察2s从KILL返回计算，不作为生产5s deadline的起点证明。失败仍恢复并join客户端保存wire；不证明具体framing cursor/partial-tail/full-pool/allocator exit。生产配置、容量和deadline未改。
- runner all-target100 PASS/0 FAIL/2既有ignored，覆盖真实handshake身份拒绝与follow-up错误分类；最初缺少test import的E0425保留。C0 from_ref测试清理已纳入。两个主要语义裁决/P09其它门/P00b/P10/final仍OPEN；无push/PR/归档。

取消探针收紧：runner101 PASS/0 FAIL/2 ignored。真实短header后EOF反例记录3 wire bytes但0 packets，后续拒绝必须wire_bytes=0；resume后timing写失败延后到join/save wire后传播。独立只读复核已修正两项具体证据漏洞。native仍待执行。


### 2026-10-09：P09 paused-client 取消与留存复跑 native PASS

- 干净 `43463c6f678341e979629cc51ca7387fd813aa1f`、实际同build identity，3个独立native1FE+3BE场景全部PASS，总7.575s；原root-retention场景复跑验证actor改动，无server配置/容量/deadline修改。12个精确FE/BE PID已退出。收据 `p09-row-cancel-native-pass-20261009.json` 保存准确binary/input hashes与原始观测。
- resident S+8：连续两次W=2/producer guard exited/task退休/End未ACK；KILL仅0.789ms，101ms后观察Client=0/Closing=1；恢复得到完整1048580B行、5packets、独立digest/schema一致、ERR1317。同socket SUM精确5050/native task counter总增2。
- missing-tail17MiB：W=2/payload2S/producer仍running/End未发布，KILL后收到655360B行前缀、零完整行并真实EOF；后续同socket查询zero wire/schema/row/packet、EOF且三BE task counter完全不变。KILL仅0.755ms。该行为不证明FE parser未收到命令，不外推具体framing cursor/partial-tail/fullpool/allocator最后退出。
- 定向runner101 PASS/0 FAIL/2既有ignored；上一个产品切片C0为9695源码12280/0/7，当前test-only增量不冒充final同HEAD C0。ACK-only、small/large精确partial矩阵、满池、real CL、CM/CP/P00b/P10/final及两项待裁决语义继续OPEN，无push/PR/归档。


### 2026-10-09：P09 installed-root ACK/replay 冻结，native 待执行

- 新 `installed-root-protocol-freeze-v1.json` 依附准确原S+8 retention输入：解析三BE fresh TaskCreateApplied完整typed identities，仅允许同一execution、无重复、occupied BE最多8候选；零ACK路由必须定位恰一个真实installed root，其余仅exact UnknownRoot/status5/零DATA。不猜stage/task或假定单task，不加产品marker/registry。
- ZeroAck场景两次ACK0/no-retirement后恢复正常wire独立oracle。FinalAck独立协议干扰场景真实fetch Data1(S)/Data2(8B+End3/rows1)、replay1摘要/元数据等价、实际End3后ACK3两次、Retired1，逐步fresh census要求context持有/producer exited、最后Data0/payload0/EndACK1；随后KILL/resume/save事实wire，不宣称正常结果或physical-last-alias。
- Whole probe链5s，实际BE上游认证plaintext/IP；请求frame4KiB/累计响应DATA≤S+4096，逐块释放h2 flow-control。严格唯一application/grpc、有DATA成功仅final trailer0、重复status拒绝、单未压缩message，使用生产task-codec解码与实际proven watermark，不设无限上界。Busy/Preparing/其它状态直接FAIL不重试。失败仍resume/join/savewire。
- runner all-target111 PASS/0 FAIL/2既有ignored；10个helper负例覆盖实际身份/候选与response结构。独立只读复核重算native prefix与Data1/Data2摘要，并修正content-type/status位置宽松点。生产配置/容量/deadline未改；P09其它门/两个裁决/P00b/P10/final仍OPEN，无push/PR/归档。

P09 installed-root pre-native接入修正：此前request builder的wait=0违反生产RootResultRead正值契约，v1未执行native、原输入保留。v2明确冻结100ms request wait并附v1 SHA/correction；原S+8/全部操作/容量/5s链期限/20s actor不变，生产配置不变。新增实际冻结请求经过生产decode_read与zero拒绝反例，runner112 PASS/0 FAIL/2既有ignored。native待执行。


### 2026-10-09：P09 native空拒绝响应缺口与修正

- 首次clean63bb/v2 installed-root native失败（4.757s）：准确BE1/stage1/task1 typed零ACK定位成功，stage2/task2非root拒绝无法满足strict trailers-only；后者实为相同execution的第二个真实task，未猜root。失败仍resume/join，原1048580B row/独立schema/hash正常，4精确PID已退出；FinalAck场景未执行。收据 `p09-installed-root-native-fail-20261009.json`，不转换为PASS。
- 根因是 `native_ingress.rs` 的OwnedResponseBody只poll_frame，未转发inner.is_end_stream/size_hint；内层Tonic空status response被默认false掩盖，Hyper产生empty EOS DATA，违反gRPC拒绝应在status HEADERS上结束的结构。真实Hyper+h2 duplex回归旧码0PASS/1FAIL，修正后Native lib720PASS/0FAIL；收据 `p09-empty-grpc-refusal-focused-20261009.json`。
- 最小修法仅转发两个inner facts，ownership仍由实际Drop/last DATA alias退出，不主动释放permit、不改变status/容量/deadline/owner分类；strict probe保留。原生复跑待执行，跨共享Native响应包装器修正触发一次C0里程碑。两个待裁决语义与其它P09/P00b/P10/final仍OPEN，无push/PR/归档。


### 2026-10-09：P09 zero ACK native PASS / final ACK 探针假设修订

- 干净 c2b658f0778ddcdddc977be9a0ee34c89030c5ca 原生1FE+3BE v2矩阵整体FAIL（5.760s）；ZeroAck场景精确PASS，非root UnknownRoot/status5零DATA、两次ACK0不退休、原独立normal wire oracle均通过，空拒绝响应修正取得实际验证。FinalAck在Data1后因测试要求Data2附End而FAIL，实际生产契约支持独立End。剩余3场景未执行。8精确PID均退出；失败仍resume/join保存wire。v2 Data2在断言前未保存，不补造字段；收据 p09-installed-root-native-v2-fail-20261009.json。
- 保留v1/v2输入及失败收据，v3显式冻结fetch1/fetch2/fetch-end3，再replay1/ACK3/ACK3/Retired1；仅两Data严格匹配且实际End3/rows1后推进proof=3。逻辑退休按operation判断，每次解码先保存outcome/End观察再断言。原S+8与全部容量/5s链期限/100ms wait/20s actor不变；生产代码、profile和deadline未改。
- runner112 PASS/0 FAIL/2既有ignored；首次v3缺end变量编译错误保留，修复并补wanted3/consumed0实际生产decoder覆盖；独立只读复核无剩余阻止项。收据 p09-installed-root-v3-focused-20261009.json。v3 native和共享Native包装器C0待执行；两项语义裁决与其它P09/P00b/P10/final仍OPEN，无push/PR/归档。


### 2026-10-09：P09 installed-root End/replay/ACK 原生 PASS

- 干净80e28e5b18c3083e704c87b7005e93f47a737e94，实际相同build identity，五个独立native1FE+3BE场景全部PASS（9.711s），20精确PID均退出。ZeroAck两次不退休且normal wire独立oracle通过；FinalAck准确Data1=1048576B、Data2=8B均无End，实际单独End3/rows1后才proof3，replay1摘要相同、ACK3两次、Retired1，逐操作fresh census成立。非root exactUnknownRoot/status5为真正headers-only/零DATA。
- 相同矩阵复跑请求profile/kind拒绝、resident完整行+ERR1317/Closing独立位置/同socket健康，以及large partial EOF/后续zero response且无native task，均PASS。收据 p09-installed-root-native-v3-pass-20261009.json 保存准确source/binary/input/raw hashes与实际观测。FinalAck干扰不宣称normal结果；逻辑Data0/EndACK不证明物理last alias/allocator退出。
- 共享Native响应包装器修改已完成定向及native证据，按执行合同触发一次干净C0里程碑，待执行；不是final同HEAD全量。exact partial framing/full-pool/real CL/CM/CP/P00b/P10/final以及两项待裁决语义仍OPEN。无push/PR/归档，goal active。


### 2026-10-09：共享Native空拒绝包装器 C0 PASS

- 干净b6b3ad232（完整source见收据）dev cargo-only C0全部PASS，564s，12295 PASS/0 FAIL/7既有ignored；repository guards/fmt/all-targets/System allocator/Clippy/build/error manifest/component/Server owner/binary smoke全部通过。原始目录logs/ci-full/20261009-075644，收据p09-empty-grpc-refusal-c0-pass-20261009.json包含raw hashes及post-C0二进制hash。
- 该共享Native响应契约的里程碑收敛完成；cargo-only不含SQL/native/performance，前一五场景native准确绑定80e源码，不混用post-C0 feature-unified binary。不是M07完成或final同HEAD验收。下一独立test-only切片是S+1行的一字节合法续行，仍须执行；real CL/CM/CP/P00b/P10/final与两个待裁决语义OPEN，无push/PR/归档。


### 2026-10-09：P09 一字节合法续行独立冻结，native待执行

- 新单行S+1冻结：x×1048569，MySQL payload1048573B，Native Data1=1048576B/Data2=1B准确x、End3/rows1；独立Python literal摘要与新两份one-byte-continuation freeze一致，保留全部原输入/收据。复用1FE+3BE原 paused actor、freshTaskCreate identity、严格h2/protobuf/生产decoder；capacity/profile/5s链/20s actor不变。
- 实际Data1/2分别通过生产ClientRowStreamCursor，明确remaining1→0、completed_rows0→1，尾span starts_row=None、bytes=x、completes_row=true；不ACK、不改变原FE消费，恢复后要求完整normal wire独立schema/rowhash/5packets。可选Data2 End只接受准确End3/rows1，同时仍读独立End3。不是非法prefix-only或最小新行载荷证明。
- 真实NativeRootResultSession组件新例1PASS，runner112PASS/0FAIL/2既有ignored，新增test-only result-contract直接依赖；收据p09-one-byte-continuation-focused-20261009.json。独立review/native尚待；其它门与两个裁决仍OPEN，无push/PR/归档。


### 2026-10-09：P09 一字节合法续行 native PASS

- 干净f25aca4b4c1d9e9b92062bde1d7f117b87a682b6及实际同build identity，三场景native1FE+3BE全部PASS（7.435s）；12精确PID均退出。新场景实际Data1=S/cursor R1/rows0、Data2=1B准确x/cursor R0/rows1且唯一continuation span、单独End3/rows1；全部direct请求consumed0，fresh census不退休。
- 恢复后严格normal MySQL一行1048573B/5packets/schema/hash均符合独立oracle，无错误且正常End；旧ZeroAck与FinalAck/replay同矩阵复跑均PASS。收据p09-one-byte-continuation-native-pass-20261009.json保存准确source/binary/input、实际probe/wire/raw hashes。只闭合一字节合法续行，不冒充非法prefix-only、具体partial framing/fullpool或physical last alias退出。
- 产品代码未变；上一共享Native产品切片C0在b6b3ad232为12295/0/7 PASS，本次仅test-only增量，最终同HEAD门仍OPEN。real CL/CM/CP/P00b/P10/final与两个语义裁决仍OPEN，无push/PR/归档。


### 2026-10-09：真实REST private preflight首次输入核对拒绝

- 私有stock REST/MinIO owner启动成功；实际canonical镜像digest及运行容器image均严格匹配。helper在任何HTTP前拒绝owner image alias假设：render_isolated_stack先存alias，isolated_start:423之后将image_id替换为Docker实际digest，tag保持alias。不是服务Unsupported或views失败。
- 原helper/freeze/bound input/launcher与失败hash保留；exact project全部containers消失/private MinIO volume消失、canonical cleanup0，共享fixture未改。收据p09-real-rest-cl-preflight-v1-refusal-20261009.json。v2只修正test producer actualimage字段，全部32/512/512/数值期限不变；10纯本地parser测试PASS，真实preflight待重跑。未发布bulk READY/未运行native CL；其它门和两个裁决仍OPEN。


### 2026-10-09：真实REST private capability preflight PASS

- 干净d28682f9ca81b17d0368c0d6db5d5295d204ea43，无FE/native启动，实际task-private stock REST/MinIO实例；image/container digest精确匹配canonical f7d679d...。14实际HTTP完成fresh namespace、真table/view create、独立metadata load、pageSize1 exactlist、DELETE200+emptybody、最后namespace恢复empty；PREFLIGHT_PASS真实落盘。3.095s含启动；cleanup0，exact project全部container/private MinIO volume消失，共享fixture未改。
- 收据p09-real-rest-cl-preflight-v2-pass-20261009.json保留source/helper/freeze/actualowner/sourceimage、实际provider ledger与全部raw hashes。标准metadata位置按server warehouse事实，client warehouse独立保存；不假造metadata/空views。不发布bulk READY，不声称native CL、并发peak或真实跨provider完成。下一原32×512table+512view真实外部producer及透明observer接入尚待，其他门及两个裁决仍OPEN，无push/PR/归档。


### 2026-10-09 P09：真实 stock REST CL 独立场景接入（定向验证，native 待执行）

- 新显式场景 `catalog/mem-1-m07-real-rest-listing` 使用既有 `IsolatedIcebergRestFixture` 与 cluster-harness，外部 producer 在 FE 启动前完成实际 preflight、32×512 tables+512 views 和逐对象独立 GET，之后仅转发实际 REST 的 observer 绑定 FE catalog URI。原 controlled REST 场景及其边界输入保留；该场景不注入元数据、token、credentials 或错误。
- 原 CL v3 N/M/page256/clients1,8,16 不变；本次 native SQL client120s、场景1800s、observer30s请求/16MiB body/64KiB request/256 owners 等在执行前固定。FE admission 与 lake discovery、information_schema、每32 namespace的 SHOW VIEWS、FORCE DROP 分阶段记录实际 pages/names/name bytes/emitted bytes/concurrent HTTP requests 与 FE PID/start/build 的100ms allocator sampled peak。并发 HTTP 请求数不冒充 SDK positions；socket accepted bytes 不冒充 FE consumed bytes。
- 新 read-only `verify-after-drop` 仅发 GET，先核验同 bound freeze/source/owner 的 READY 与两份有限账本，再精确列出31 namespace，逐一比对31744存活对象的UUID/location/schema/metadata hash；target absence只依据正常 catalog namespace authority，不猜不存在对象的404类型，不单独声称 native DROP执行。该阶段在 FE 采样后运行，只发布 `VERIFICATION_PASS`。
- 定向验证：producer17纯本地 PASS、observer13纯本地/synthetic loopback PASS、runner112 PASS/0FAIL/2ignored、fmt/diff-check PASS。收据 `p09-real-rest-cl-integration-focused-20261009.json`；仅测试接入，不是 bulk READY/native CL/跨provider/CM/CP验收。此前两项裁决、P00b/P09/P10未完成门仍OPEN；无push/PR/归档。

- 执行前只读审查已修正验证层：native phase与resource convergence共用同一个固定1800s deadline，不在各phase重置；外部只读verification沿独立7200s producer时钟。SQL/observer/落盘错误分开保存，失败路径先kill/reap/join exactobserver再停止privatefixture，双cleanup失败聚合。修正后runner112 PASS/0FAIL/2ignored；真实执行仍未开始。


### 2026-10-09 P09：真实 stock REST CL 原始大输入原生 PASS（memory owner follow-up OPEN）

- clean `ce6b859efde841a32d964f50fb959e5ba669d11c`，实际 server `d37effa017f69979d189cbd3d33265a9647535685f56eeda744271d3ae0b5459` / runner `12fe4e3bb83ad4c7473391f2ff91a199a394a5371a63b06c79b0e4802a5336e9`，原生1FE+3BE；单场景 PASS/总409.495s（native context含外部后验核对155.886s），38个FE sampled phases全部PASS。没有结果后放宽输入、deadline、cap或重试。
- 实际私有REST镜像DockerId与冻结BOM digest一致，既有fixture拥有全部5个标准Compose服务。preflight14次HTTP通过；bulk65700次真实HTTP/241.066324s，实际创建32namespace×512tables+512views后完整分页、独立GET32768份metadata，READY同次source/binding/freeze/ledger严格一致，不以producer计数代替验证。
- catalog admission与独立cl_discovery各16384次实际metadataGET/129listing pages/16416names，sampled allocated peak82096240/82356008 B、after16400792/15963176 B。information_schema的1/8/16 clients分别65/520/1040实际listing pages、16416/131328/262656names，SQL每clientCOUNT16384且tablepage精确64×clients，actual HTTP listing concurrency peak1/8/8。每32namespace SHOW VIEWS精确512名字/2实际页；FORCE DROP精确1025实际HTTP200 mutations（512table+512view+namespace），此前各phase mutation0。
- 独立 GET-only after-drop：实际31871 HTTP/38.237511s、mutation0，31namespace×512table+512view=31744存活对象metadata逐一与READY完全相等；namespace0按正常catalog namespace authority精确缺席，未猜对象404类型。`VERIFICATION_PASS`与独立native DROP测量合并证明该SQL效果，不单凭外部receipt声称native执行。
- exact FE73291/BE73302,73303,73304/observer73290均已退出；observer lifetime failures0/active connections与requests0，exact private project容器与volume均消失。共享fixture未改。收据 `p09-real-rest-cl-native-v1-pass-20261009.json` 绑定全部源输入/实际镜像/进程身份/phase bytes/counters与140份raw hashes。
- 需继续审查：information_schema16clients sampled allocated peak1737269616 B（约1.62GiB）、after22278448 B（约21MiB）；1/8clients peak134418784/894877760 B。初步源码证实该SQL走Client路径，COUNT规划前把表集合展开为VALUES AST；observer metadata_loads0，不将峰值归因Local96MiB或SDK permit。该观测没有新增字节PASS门；内存owner/bound审查仍OPEN。真实HMS/Paimon大CL、P00b/CM/CP/P10、两个既有语义裁决及M07整体仍OPEN；无push/PR/归档。


### 2026-10-09 P07：information_schema source 全量行副本退出（定向 PASS，native/C0 待验证）

- 真实CL测量后只读诊断确认：tables查询为Client而非Local，builder四列有效UTF8内容仅720896B/逻辑计费1MiB；COUNT规划前的VALUES AST（65536inline expressions）、TypedExpr/scalar arena及完整query clones是高峰候选，进程100ms采样不能定量归因。planner/cache/catalog不在owner-envelopes结果小计内；不新增字节FAIL门，不以after一次读值冒充最后owner退出。
- 已闭合coverage中原有的system_catalog预复制缺口：schemata/tables不先collect全量owned row Vec/String，直接以固定Option<&str>数组写入原LocalTableBuilder，在任何cell copy之前核对整row与实际collector capacity。不可变fact snapshot仍由原source owner持有，builder finish输出Arrow不借用source；拒绝无部分table发布。没有改32MiB/65536rows/4096columns、NULL/required/列序/BASE TABLE/空batch或原错误prefix。
- 定向system_catalog+rewriter13PASS/0FAIL（473filtered）、fmt/diff-check PASS；Unicode/required黄金、schemata第五列NULL及重复catalog whole-source超32MiB准确拒绝。拒绝测试只证明结果/前缀，不单独称事前容量oracle；无预复制由源码结构与既有builder growth检查证明。只读审查无blocker，收据 `p07-system-catalog-borrowed-focused-20261009.json`。
- 此source切片未证明AST/optimizer包络、~1.62GiB峰值下降或M07整体完成。需要同候选真实REST native重跑与本里程碑C0；该生产source改动是本次C0触发点，不用旧ce6b/b6证据冒充新HEAD。其余门/两个既有语义裁决仍OPEN，无push/PR/归档。


### 2026-10-09 P07/P09：borrowed source 同 HEAD C0 与真实 REST v2 PASS

- clean `efb4d723211a6f9356cffbd0067a2ce687a615c7`，本次C0实际默认 **dev-opt**：全部guards/fmt/all-targets/System allocator/Clippy/build/error-manifest/workspace/server owner/binary smoke PASS，12298/0/7、5417s。组件12116/0/7、server owner178/0/0、binary smoke4/0/0；cargo-only不含SQL/native/性能。收据 `p07-system-catalog-borrowed-c0-pass-20261009.json` 保存实际命令、profile、每组计数、dev-opt binaries与raw hashes，不用旧dev结果代替。
- 随后同clean HEAD单独dev build（71s）并真实REST原始32×512 tables+512 views、clients1/8/16、全部38phase原生1FE+3BE PASS，总413.335s；65700实际外部准备HTTP/231.436537s后独立核对32768对象。独立after-drop31871 GET/39.541424s，mutation0，31744存活对象完全相等、目标namespace按正常catalog authority缺席。没有放宽cap/deadline/输入或自动重试。
- 5个精确进程41942/41945/41946/41947/observer41941均消失，observer lifetime failures0/全部active0，实际private project `nr-isolated-rest-40658-1791512946839762000-1` 全部container/volume消失，owner manifest已由owner退出删除；实际image/live binding与140份raw hashes保存于 `p09-real-rest-cl-native-v2-pass-20261009.json`。共享fixture未改。
- information_schema1/8/16 clients allocated peak134267200/911366120/1748091776 B，after17090024/21048080/22824576 B；16clients仍约1.63GiB。此dev同profile复跑只证明功能/provider观测与source-copy切片兼容，不证明因果峰值下降、AST/planner包络、physical last alias或正式release CL/CM/CP。P00b、真实HMS/Paimon大CL、两个待裁决语义问题、P10及M07最终同HEAD门仍OPEN；无push/PR/归档。


### 2026-10-09 P09：4B prefix-only 组件与5B最小新行接入（定向PASS）

- result_pump 新准确4B body及完整合法row后4B suffix均在完整body校验时以ContractViolation拒绝；consumer首个观察为failure，实际scripted port仅wanted1/consumed0，无Segment/ACK-only/positive ACK；drop测试held grant后window positions归零。新测试1PASS，相关relay_refuses三测试PASS；不称实际native FE reply或physical backing退出。
- 新ordered两行 S+5 scene：首row占满S、第二empty String新row准确Native `[1,0,0,0,0]`，cursor starts_row=1、remaining0→0、rows1→2，实际End3/rows2；独立MySQL两row1048573B/6packets/sequence1..6/hash冻结。保留旧zeroACK/finalACK/1B continuation、W2/caps/deadlines。runner113PASS/0FAIL/2既有ignored，收据 `p09-prefix-minimum-row-focused-20261009.json`，原生四scene矩阵待执行。
- 初始误选--lib与随后test-only &Bytes比较E0277均保留日志，已改正确bin target与借用slice比较；无生产行为或输入修改。前一生产source C0/真实REST已在cleanefb完成，本增量仅测试，最终同HEAD门仍OPEN。无push/PR/归档。


### 2026-10-09 P09：最小5B新行及旧Root协议四场景原生PASS

- clean `cae99549748f05a49c35e310bb3833b617cf6930`，同HEAD dev build 74s；原生1FE+3BE四scene全部PASS/8.348s。实际Data1准确S/完成row1；Data2准确5B `[1,0,0,0,0]`，cursor starts_row=1、remaining0→0、rows1→2，实际piggyEnd3/rows2及随后独立Read End3均成立。未改变caps、期限、输入或重试。
- 正常MySQL实际2rows/1048573B/6packets，actor逐包检查sequence1..6，literal row hash `a6884c6ae4c93d904c319ba5e87b9215745812b1b5be3edb1591762f820aea46`、normalEnd/error null。旧zeroACK、finalACK/replay与1B continuation同HEAD回归通过；四scene均两次owner idle，实际16个精确FE/BE PID全部消失。
- 收据 `p09-minimum-new-row-native-v1-pass-20261009.json` 保存actual source/binaries/input/protocol/effective config/身份与raw hashes。5B新行原生门闭合；4B非法prefix仍仅组件证据，实际FE reply负例及其他stress、P00b/CM/CP/HMS/Paimon大CL、P10/最终同HEAD门与两个人工语义裁决仍OPEN，无push/PR/归档。


### 2026-10-09 P09：私有HMS capability helper接入（host-only定向PASS）

- v5 helper推广到scripts/prepare_real_hms_capability.py，实际repo寻址parents[4]；三项真实Python child边界测试PASS/0.035s：正常capture无多余kill、128B超界保留partial及whole-failure sticky、Popen后KeyboardInterrupt保留第一cause及未知detached owner。BaseException/退出/清理语义和1200s+240s总时钟均未改。17个source pins核对PASS。
- tracked freeze保持draft/unfrozen/source_revision null，实际执行另建ignored immutable reviewed copy绑定clean已提交HEAD/hash/BOM/images。收据p09-hms-capability-host-focused-20261009.json。仅host capture与输入模板证据；真实HMS 1table+1view能力预检尚未执行，不冒充原32×512table+512view大CL/Native mixed分类或READY。无push/PR/归档。


### 2026-10-09 P09：真实HMS预检首跑失败及初始连接字段纠正

- clean5fe6f4dfd首跑实际stockSpark create exit1，唯一failure marker RuntimeMetaException/message hash与Failed to connect to Hive Metastore字面一致，无成功mutation记录。所有host children实际reap/原groupgone；实际HMS/writer IDs均消失，private HMS/catalog/object-store records消失，cleanup complete/errors空/shared未改。失败收据p09-hms-capability-preflight-v1-failed-20261009.json保留原freeze/hash/actualidentity，不冒充PASS。
- 无网络的锁定Spark镜像javap确认实际Hive2.3.9总连接轮数：初始attempt0>=retries0直接跳出，原helper误读成extra retries。当前helper/freezeinput改hms_connect_attempts1→Hive property1并readback1，failure retries0，允许首次connection而无额外retry；不改产品、scope/CL/caps/deadlines或旧freeze。需要新cleanHEAD、新UUID/newfreeze重新运行；并非重试unknowncreate。

- 纠正后host-only四测试PASS/0.037s（包括reviewed freeze通过及initial attempts0提前拒绝）；第一次schema key漏改所致定向ERROR日志保留，已在provider新运行前修正，未放宽exact schema。


### 2026-10-09 P09：HMS预检二次真实创建失败及显式S3 region接线

- clean50a932335第二次预检实际baseline/default与create_namespace applied；create_table attempt后SdkClientException，尚无table/view创建成功记录，不能把未执行view写成Unsupported。safe effect ledger/原freeze保留于p09-hms-capability-preflight-v2-failed-20261009.json。全部host groups/PIDs、实际HMS/writer IDs/privateownerrecords消失，cleanup成功，未改共享fixture。
- actual locked Spark javap确认AwsClientProperties只读取client.region；helper旧copy owner s3.region没有映射公开SDK region，one-shot容器亦没有标准Compose AWS_REGION环境。当前显式派生owner已发布region→public CLIENT_REGION并getter核对，无默认/猜测region。原exception cause仅hash，需新真实run确认因果；不以修正接线声称PASS，不重试旧unknowncreate。其余caps/clocks/input与zeroextra retries不变。


### 2026-10-09 P09：真实stock Java HMS table/view能力预检PASS

- clean6e052ec1ed78c9d40fad541baf734265514b4fe1，新的immutable freeze/privateUUID；四个独立stock Spark JVM create/oracle/drop/restored均exit0。实际namespace基线/default最终恢复，三项create与三项drop均actual applied，无unknown effects；零extra connection/failure retry，所有bounds/deadlines未放宽。
- required long id空table真实format2/spec0/unpartitioned/snapshots0，真实view format1/version1/defaultcatalog+namespace/唯一Spark SQL。独立fresh HiveCatalog filtered tables={cap_table}/views={cap_view}，第二public LIST_ALL_TABLES=true准确{cap_table,cap_view}；真实UUID/schema/location及table724B/view gzip442B metadata hashes在独立load完全一致。
- host-only修正后四测试PASS/0.037s；实际48条host commands leaderreaped/groupgone/PID absent，4个writer+HMS实际容器ID均消失，private HMS/catalog/objectstore records消失，cleanup complete/无errors/无stickybarriers。收据p09-hms-capability-preflight-v3-pass-20261009.json保存actual identities/freeze/effects/metadata/raw hashes，v1/v2失败完整保留，共享fixture未改。
- 此证据只闭合stock Java真实view capability/独立oracle和私有工具退出；没有启动NovaRocks、没有READY，不证明Native Rust mixed分类/list_views支持、原32×512+512大CL、性能或SDK/FE硬字节包络。其余P09/P00b/P10/final及既有两人工语义门OPEN，无push/PR/归档。


### 2026-10-09 P09：真实FE非法RootReply actor/runner整合（定向PASS）

- harness仅显式opt-in的3个Data H2 actor共享single-target slot/global credit/有限positions，原Control TCP默认与budget不变；three closed mutation仅profile2/kindfalse/准确4Bprefix。原真实reply先production decode+cursor校验，再共享creditedVec strictcanonical bytes；NotReady真实透传/no claim/no deadline refresh。typed peer CANCEL只对authenticated/decode完整/frozen fulltarget不同且非provisional ROOT单stream豁免，target/unknown/非ROOT/driver等wholefailure保留。特殊actor先实际stop/join再停FE/BE。
- 9actor组件PASS/0.01s，harness全149/0/3ignored/2.48s；runner全117/0/2ignored/5.90s。真实duplex同长度非canonical、共享cap/Bytes最后alias、H2typedCANCEL后同连接健康stream、NotReady透传、stop实际join及MySQL partialdeadline/oversize/严格ERR拒绝反例通过。old3test calls E0061及test observation E0063均准确补齐、保留初始失败日志；未改caps/clocks/input。
- 真实runner.rs dispatch已接Option/defaultNone，optional Data1 End无piggy合法/Some严格一致；observer同一absolute20s connect+read时钟在内返回有限partial证据，decoder-specific1105/HY000/sequence4原因必须完整。Cargo.lock仅9direct dependency edges、无版本/source/checksum升级。收据p09-native-root-reply-focused-20261009.json；实际FE三负例尚未执行，不冒充native拒绝/noACK/正常恢复与最终M07验收。无push/PR/归档。


### 2026-10-09 P09：真实RootReply首跑在注入前失败；阶段诊断与失败日志保留

- clean2d6f1960c实际原生1FE+3BE首跑5.367s/exit1，正常baseline成功；actor此前三条reset/broken-pipe导致begin_capture拒绝，claimed/emitted/target_attempts全部0。没有注入非法reply，不把它称FE负例PASS。actor所有positions/credit0、listeners3实际join、四个精确PID已独立确认不存在；whole actor join verdict仍失败。收据p09-native-root-reply-native-v1-failed-20261009.json绑定raw hashes与原cause。原runtime role logs被explicit shutdown提前删除，缺失不可重建。
- 本增量仅给downstream H2 handshake/accept、upstream TCP/H2加准确context，固定原position内一个AtomicU64记录实际读取bytes；任何reset/error判据均不放宽，bytes0也不当无输入proof。失败scene在explicit shutdown前retain，shutdown自身有failure亦保留runtime，供下一次实际定位。9actor/149harness/117runner全部PASS，既有ignored3/2不变；收据p09-root-reply-stage-diagnostic-focused-20261009.json。
- 将用新cleanHEAD与新immutable v2 artifact保持原三scene/caps/clocks/SQL运行。真实FE非法RootReply及M07最终验收仍OPEN，无push/PR/归档。


### 2026-10-09 P09：第二次RootReply首跑阶段定位与公开帧诊断

- clean dae0a3e74原生v2仍FAILED/5.335s，claimed/emitted/attempts0；三条downstream H2 accept分别read87/96/96B、children1 reset，另有独立root output capacity closed。四个精确PID已确认不存在，所有actor positions/credit0且实际join，但whole failure保留。失败runtime日志这次实际保留，收据p09-native-root-reply-native-v2-failed-20261009.json包含raw hashes。
- actual BE启动确有Data readiness connect_with_connector后drop channel、无application RPC的源码；仅byte数/children1不足以将这些错误认定正常。本增量增加固定public preface/header/parser，记录帧类别/完整边界/ever application frame、connection-local accepted RPC与typedIO kind，request/response copy分阶段；不保存AUTH/headerblock/body，不放宽reset、timeout、未知stream或独立capacity错误。
- 新逐cut/parser反例及相关actor10PASS/0.01s，harness150/0/3ignored/2.42s（最后context-only补充另做focused10PASS）。E0425初始counter误放listener scope已在运行前修正并保留失败日志。收据p09-root-reply-frame-diagnostic-focused-20261009.json；用下一cleanHEAD v3进一步测量，尚无negative/native PASS或新关闭豁免。


### 2026-10-09 P09：第三次公开帧诊断与GOAWAY/RPC精确事实

- clean d19ccfed6原生v3 FAILED/5.478s，仍claimed/emitted0。三个accept reset均正确完整preface、acceptedRPC0、完整87/96/96B边界，实际settings1/2/2、connection window1、GOAWAY1，noapplicationframe；typed IO ConnectionReset/resetfalse。GOAWAY payload此前尚unknown，不能称正常close。独立第四条是downstream response copy capacity closed，仍wholefailure。四PID独立不存在/所有actorowners实际退出，收据p09-native-root-reply-native-v3-failed-20261009.json。
- 本增量完整control parser仅一个8B scratch、标量frameheaders，校验known settings/WU/PING/GOAWAY公开值，保留GOAWAY laststream/error/debug长度而不留debug/Auth/HPACK/body；固定计数overflow sticky失去候选资格。加入RPC有限path/stream/authclass、capacityNone单次nonblocking poll_reset观察，不等待/不合成typedreset/不豁免。原caps/clock/三输入不变。
- 13actor/153harness PASS，既有ignored3；wholecontrol frames逐cut/bytewise、partial/badpreface/应用帧/unknownsetting/value/错误GOAWAY等反例通过。draft初始debug字面长度14纠正15在apply前完成，无执行失败。收据p09-root-reply-goaway-diagnostic-focused-20261009.json；下一cleanHEAD v4只进一步诊断，尚无native负例PASS或新close分类。


### 2026-10-09 P09：第四次GOAWAY内容确认与no-RPC关闭窄分类

- clean95dfb793a原生v4 FAILED/5.39s，实际唯一failure为downstreamaccept typedIO ConnectionReset；完整87B含合法settings(4entries)/window/GOAWAY，实际NO_ERROR/laststream0/debug0，acceptedRPC0/noapp/全部完整边界。claimed/emitted仍0，四PID消失/actorjoin与positions0，未注入，收据p09-native-root-reply-native-v4-failed-20261009.json。旧capacityclosed未在本次复现，旧v2/v3 cause不抹除。
- 新test-only分类仅上述完整no-RPC peerclose、capture未开始/未shutdown或disconnect、actualoriginal children全join/no cleanup failure；同Core锁结算两个sticky fence和实际listener inventory，每BE≤3关闭、第四次失败。合法控制帧不充分，必须恰一个NO_ERROR/last0/debug0 GOAWAY；rawIo/string/Reason不冒充typed h2acceptIO。任何existing failure、application/partial/protocol/timeout/upstream/stream/capacity原因仍wholefailure。没有产品代码/配置/SQL/cap/deadline改变。
- 三组新增真实h2 server accept typedIO与边界反例、原actor总16PASS；harness156/0/3ignored、runner117/0/2ignored（最终shutdown/disconnect fence另focused16PASS）。收据p09-root-reply-preapplication-close-focused-20261009.json如实限定scripted IO组件范围；需要新cleanHEAD v5实际stock readiness与三FE负例验证，不能先称native PASS。


### 2026-10-09 P09：v5实际双Task oracle失败与covered subscription取消独立识别

- clean8be8ddb2c原生v5 FAILED/5.677s：bootstrap no-RPC关闭窄分类通过，正常baseline成功，capture开始/attempt1；原marker inventory误假设每BE一个task，实际原SQL创建两个Task，尚未arm，claimed/emitted0。另一独立错误为SubscribeTaskStatus response容量关闭，实际poll_reset观测CANCEL仅作诊断；清理Root unknown错误仍不豁免。四精确PID已独立确认不存在/所有actor owners实际join并归零，wholefailure保留，收据p09-native-root-reply-native-v5-failed-20261009.json。
- 新debug-gated prepared ClientRoot marker只在实际成功prepare、真实profileV1 ClientRows root channel出现；不是Installed事实。oracle保持原SQL，核对全3BE真实UUID、exact2 task、唯一显式Root与实际occupied BE、每个参与BE的exact context/FE/execution，不能借candidate或maxstage推测Root。原2MiB日志/384B单行上限不变，observer固定8槽/BE。
- status仅exact authenticated normalFE RPC与原4096B请求production covered decode/签名FE一致/完整EOF后保留原RecvStream。只有response copy实际private CapacityClosed且同stream，单次nonblocking原inbound取得typed remote CANCEL才独立记账；Reason/GOAWAY/IO/Target/Provisional/未知身份及原failures都不能因此消失。额外固定一个digest槽，产品caps/clocks未改。
- 初始E0308修正helper Bytes签名；首跑18PASS/1FAIL为fixture提前drop上游导致driver broken pipe，修正为实际统一abort/join前保留owner。最终actor19PASS/0.02s、harness159/0/3ignored/2.45s、runner124/0/2ignored/6.14s；native task protocol43PASS、独立Root oracle9PASS。收据p09-root-reply-root-oracle-covered-status-focused-20261009.json保留全部失败/成功日志hash，duplex只是组件证据，下一cleanHEAD v6须实际三负例/native验证。无push/PR/归档，其余门仍OPEN。


### 2026-10-09 P09：v6真实profile拒绝PASS；无GOAWAY启动变体及合法行后缀负例接入

- cleanad553271f原生v6整体FAILED/7.829s：profile完整PASS（actualRoot是stage1，独立marker而非maxstage推测），实际profile2回复一次，完整metadata后1105/HY000/unsupported root-result profile，0row/0payload/0positiveACK；owner两次归零、健康native5050及actualjoin通过。kind尚未capture，bootstrap完整79B/SETTINGS2+WU1/noGOAWAY/acceptedRPC0 typedBrokenPipe仍按旧strict判据失败；prefix未执行。全部8精确role PID独立不存在。收据p09-native-root-reply-native-v6-partial-20261009.json完整保留部分成功与原failure，不能称三场景通过。
- backend_readiness只创建即drop channel、不发RPC，锁定h2不保证drop前GOAWAY flush。test-only分类仅把exact1 GOAWAY改为0或1（若有则仍NO_ERROR/last0/debug0），完整legalcontrol/noapplication、零accepted/unresolvedRPC、原children全join/无cleanupfailure、stickycapture/lifecycle、同锁结算/每BE≤3/fourthfail等全部不变。control-only不认证peer或独立证明readiness意图，不对目标/未知RPC/IO其他kind/protocol/partial/timeout放宽。真实typed脚本79B everycut与所有fence反例通过。
- 新第四独立freeze场景仅保留真实69B合法row后append01000000（73B/hash112eb22b8f4cb517da83112c0bcc142bfbb7c37e4d09dfe5b2e0b1f0df53fbd7），要求完整整段拒绝而不先publish合法前缀；原identity/profile/kind/seq/watermark/optionalEnd及原三freeze字节不变。真实OwnedBuffer共存计费、lastalias退出、cursor拒绝及独立End/hash oracle验证通过。
- 最终actor21PASS/0.03s、harness161/0/3ignored/2.43s、runner125/0/2ignored/6.38s（suffix初步20actor/10oracle亦PASS）；收据p09-root-reply-no-goaway-suffix-focused-20261009.json。下一cleanHEAD同binary v7运行原3+新suffix4场景；无native完整矩阵/M07终验或push/PR/归档结论。


### 2026-10-09 P09：真实FE四项非法RootReply原生PASS

- clean81f8cc970d44a62818805dfa130bbe6ac5b5ea9c，同HEAD dev binary+runner构建62s；actual native 1FE+3BE v7四项PASS/13.062s：profile2、kindfalse、准确4Bprefix-only、完整合法69Brow后追加4Bprefix-only（73B）。原三immutable freeze hashes未变，第四dff0d953f21c13d7d990d532ee0017c711d3a61c6d93edd62b25c0f5368ce19c；每场实际独立fullFE/BE/TaskRoot marker与single target一致，实际malformed reply emitted1/claimed1，完整metadata后准确1105/HY000/seq4及各自原因，0row/0payload/positiveACK0。suffix证明真实FE先整段校验，合法前缀未提前publish。
- 每项拒绝后与native healthy SUM=5050后均两次3BE×14项Root census全零；健康5packets/1row/hash准确。aftershutdown全部actor原handles实际join/listeners3、positions/credit0、failures/overflow空；每项独立status订阅remoteCANCEL记账2，不当目标ACK或掩盖failure。4FE+12BE共16个精确PID独立核对不存在。收据p09-native-root-reply-native-v7-pass-20261009.json绑定clean源码、binary/source identity、raw hashes、实际语义/过程与所有PID。v1–v6失败/部分成功保留。
- 仅此四场景contract refusal矩阵闭合；不存在独立RootReply domain codec-version字段，不虚构mutation。exact partial poll_write、完整client/compute/closing/short并发、全部CL/Paimon/HMS、transport系数/P00b、人工两语义门、P10与最终同HEAD C0/C10等仍OPEN；无push/PR/归档。


### 2026-10-09 P06s/P09：真实HMS view误归BASE TABLE的provider分类修正（定向PASS）

- r7准确分类审计发现旧vendor get_all_tables原样投影，把stock Java真实Iceberg view也送作BASE TABLE；不能将错误32768 count冻成成功oracle。固定1.11 JAR实际getAllTables→HMS实体parameters并equalsIgnoreCase(iceberg)，真实stock writer标志为ICEBERG/ICEBERG-VIEW（非下划线）；不采用未证明旧2.3.9大小写语义的uppercase serverfilter shortcut。
- 本routine domain correctness patch仅同private client保留get_all_tables、串行≤100 requested names公开get_table_objects_by_name。真实params借用ASCII-case分类，固定两个bool[100]整batch核对identity/db/name membership/duplicate/count后才投影；view/foreign/无type参数不作base table。合法concurrent missing不补默认/不重试，RPC error原样传播。没有Iceberg load_table/S3metadata、新client/owner、字节计量钩子或view/page能力；原8positions/absoluteDeadline和SDK受信端点增长例外保持，SDK entity反序列化未获得硬byte界。PATCH.md记录领域补丁及C0入口。
- vendor直接测试因非workspace+devdeps拒绝，纯helper拆独立同源table_projection.rs，由connector test直接include纳入既有C0，未加入外部vendor integration suite。初次E0597纠正为namespace String move到FastStr、RPC仅clone handle，test fixture使用owned值；失败日志保留。9projection定向PASS/0.00s，Iceberg全部1203/0/0ignored/7.93s；Cargo.lock只新增faststr/hive_metastore两条dev依赖边，无version/source/checksum变更。收据p09-hms-table-classification-focused-20261009.json。
- 尚未实际Native Rust/stockHMS复跑，不宣称mixed native、SHOW VIEWS/FORCE zero-mutation或原大CL通过。small correctness preflight与原32×512table+512view大CL分别验证；lake/Paimon/其余P09/P00b/P10/final以及两人工语义门仍OPEN。无push/PR/归档。


### 2026-10-09 P09：actual socket poll_write 闸门组件接入（Native仍OPEN）

- 新 mysql-adapter/mysql_write_gate.rs 仅 cfg(test)，真实OwnedWriteHalf scalar/writev原调用、Ready(n)才计费/hash，栈32IoSlice裁到cut，zero-budget后继非空poll Pending才发布blocked。固定scalar/FramingCursor/sha256/oneWaker，无body或lease aliases；实际owner token组合仍是组件fixture，不冒充Native身份。固定deadline/firstcause sticky，arm/begin/cancel/resume各一次，Stop不刷新clock/clear failure。
- 12项真实TCP组件PASS/1.25s：scalar/writev cuts1..6、S+1实际cursor/hash、错误token/重复binding/begin/earlyresume、33slice拒绝、actualStop/原deadline、无writer control timeout、parentpanic/timeout与childJoinError。失败退出由父唯一JoinSet原handles abort+await实际join，再独立EOF；writer destructor fact不替代Task/root exit。原v1/v2 ignored草稿保留，v2修正detached句柄与control timeout sticky，测试无失败。
- MySQL adapter全部79PASS/0FAIL/1.33s；原default产品路径/config/caps/SQL/deadline/Closing语义未改。收据p09-exact-fe-poll-write-component-20261009.json绑定sourcebase/raw hashes。真实FE接线、原大小行SQL/cuts、实际W2取消时coverage、small missing-tail/coalesced等仍OPEN，不能用合成component代替Native矩阵；其余门不变，无push/PR/归档。


### 2026-10-09 P09：poll_write 闸门七项额外反例（19项组件PASS）

- 六项固定脚本NO-I/O反例覆盖scalar/writev原Pending/waker、Ok0/原rawOSerror、framer原WriteZero、n>offered首错、多个Ready累积准确hash、zero-budget empty slices不假造blocked；与真实TCP/Native证据分列。第七项真实TCP使用实际stale baseline receipt使Receipt失败，resume拒绝、explicitStop唤醒、exact2B+EOF与原JoinSet实际全join。没有新增首错即时wakeup或泛化panic-prone inner合同。
- 初次E0061/E0308因Pin receiver的方法名write解析到AsyncWriteExt；仅fixture改名scripted_poll并get_mut明确dispatch。完整19组件PASS/0FAIL/0ignored/1.25s，原错误日志及ignoreddraft保留；收据p09-exact-fe-poll-write-component-additional-20261009.json。gate实现/产品路径/caps/clock未变，Native接线及其余门仍OPEN。


### 2026-10-09 P09：真实protocol owner身份只读投影

- GovernedProtocolOwner.statement_token仅借原live statement并取其既有token，StreamingStatementResult只delegate；已settled/taken返回None，无新mint/reconstruct/registration别名/registry或取消容量权。为后续exact caller绑定提供原身份，不能把sessionepoch猜作connectiongeneration。
- 原streaming Closing alias真实fixture校验token跨取消cut不变、下一statement仍Busy、普通/Closing位置仍按实际alias退出；focused1PASS、query_control全部26PASS/0FAIL/0.02s。收据p09-protocol-owner-token-projection-20261009.json；feature IO/control/Native接线仍OPEN，无deadline/资格/失败语义改变。


### 2026-10-09 P09：stock HMS小规模Native分类预检接入（真实运行待验）

- 新opt-in exclusive CLI/scenario和原capability helper的独立companion；原helper/四Scala/input/bounds/旧freeze字节不变。仅完整exact4role身份+runner正常完整evidence+逐PID实际不存在才允许freshJava核对及原cleanup；timeout/partial/unknown退出保留Native/HMS/catalog/objectstore，不从host group退出推断角色退出。新freeze模板draft/null pins，拒绝直接运行。
- host14PASS/0.026s，最终runner focused2PASS；runner回归127PASS/0FAIL/2ignored/6.14s在最终primary+save secondary错误合并前运行，最终合并后focused另通过。收据p09-hms-small-native-classification-focused-20261009.json绑定全部日志hash。正常代码0/1完整证据仍分别PASS/FAIL，不把成功cleanup改写实验失败。
- 真实stock HMS/1FE+3BE即将独立cleanHEAD构建运行，尚无Native结论。freshJava finalunchanged不证明zero intermediate mutation RPC；该observer仍OPEN。原大CL、其余P09/P00b/P10/final及两人工语义门不变，无push/PR/归档。


### 2026-10-09 P09：HMS Native首轮失败保留与有限阶段诊断

- clean450b4625真实1FE+3BE runner正常exit1，分类事实保存前断言失败；原stdout仅hash不能定位阶段，不能猜测原因/放宽oracle。四role PID及60host PID独立不存在，五stockJava create/oracle/afterNative-oracle/drop/restored均0/实际exit，freshJava核对未变，6实际containerID均消失，cleanup完整/无unknown retention。实验仍FAILED，收据p09-hms-small-native-v1-failed-20261009.json保留source/freeze/native/artifacts，不因cleanup正确改作PASS。
- 新诊断仅保存有限local阶段、各成功阶段立即facts、SQL数值code+hash（不留provider message），connect错误也hash；原query/input/bounds/clock未变。payload-redaction反例+既有CLI/refusal共3PASS/0FAIL。收据p09-hms-small-native-phase-diagnostic-focused-20261009.json；下一cleanHEAD运行仅定位同原输入失败，不是busyretry。大CL/zero-mutation observer/其余门仍OPEN，无push/PR/归档。


### 2026-10-09 P09：HMS v2定位unsupported SHOW TABLES，修正预检调用面

- cleand1c2e7ca1原生v2 FAILED，finite receipt准确phase=show-tables/server1064；current parser没有SHOW TABLES（catalog只SHOW CREATE TABLE，未知SHOW被show_backends接收），这是新预检调用面错误，不能归因provider分类。四role/host PID与6实际containerID确认消失，freshJava未变/cleanup完整，收据p09-hms-small-native-v2-failed-20261009.json保留前轮失败。
- 仅names查询改为既有 `{catalog}.information_schema.tables` 单列，type查询也准确同catalog qualified；原1table+1view、排序/准确集合/类型/refusal/input/caps/clock不变，不增加产品SHOW语义或缩小负载。focused3PASS/0FAIL，收据p09-hms-small-native-catalog-query-focused-20261009.json；下一cleanHEAD真实Native验证仍待验。其余门OPEN，无push/PR/归档。


### 2026-10-09 P09：stock HMS小规模Native正确分类与精确拒绝PASS

- clean055393db9fff02142df65e7affa779394ea828ae，同HEAD dev server+runner build64s、新reviewed freeze；实际1FE+3BE topology barrier/精确4role身份。FE catalog-qualified names准确{cap_table}、information_schema准确(cap_table,BASE TABLE)，真实Java cap_view未误归table；SHOW VIEWS和DROP DATABASE FORCE实际均1105/HY000、同准确Unsupported/list_views源原因hash。Native断言阶段0.315s（不包括集群启动与stock准备）。
- 独立before/after stockJava新JVM oracle准确table/view/allObjects/namespace、schema/UUID/location/raw metadata hashes完全一致；五create/oracle/afterNative-oracle/drop/restored全0，原default namespace基线恢复。四role PID、60host commands原group/PID、6实际HMS/writer containerID、private HMS/catalog/objectstore ownerrecords均独立核对退出/消失；cleanup完整无error/retention。收据p09-hms-small-native-v3-pass-20261009.json绑定source/binary/build/freezes/actualfacts/raw hashes，v1/v2失败仍保留。
- 仅small provider correctness preflight闭合；仍无intermediate mutation RPC observer，final unchanged不能冒充zero transient mutation。原32×512table+512view大CL、SDK硬byte界、performance/其余P09/P00b/P10/final及两人工语义门仍OPEN，无push/PR/归档。


### 2026-10-09 P09：original socket late-binding组件与statement后继守卫PASS

- 新child仅cfg(test)，原greeting/raw IO→真实metadata poll attach→actual Rows cut/真实flush future drop→精确receipt/local一次resume→同原IO健康followup；一个Hub/scope/move-onlycontroller，无body alias或新registry。原13实际TCP测试覆盖healthy/connectiongeneration/earlyquery/parenttimeout/panic/childJoinError，以及stale/older/wrongCID/wrongepoch/zeroepoch/zerogeneration后继拒绝；healthy gen29→31允许，不把epoch等同conn generation。另3NO-I/O函数分列scripted invariants/原OSerror透明性/sticky无scope超时。
- 独立静态审查发现F1 Scope首因未达Hub、F2followup忽略statement；已修Hub→Core锁序首因投影/仅真实scope.failure同步、未知innerIO不fabricate；同Session且gen严格>原target。初始27PASS；补充首轮27PASS/6FAIL为主线误转wire oracle分支及bad-inner vectored fixture仍在Armed passthrough，纠正fixture为实际metadata receipt后Rows，不改原passthrough。后focused33PASS；最终fulladapter102PASS/0FAIL/0ignored/1.51s（含35gate）全部原日志/hash保留，收据p09-exact-fe-poll-write-late-binding-component-20261009.json。
- 组件token/greeting不冒充真实FE认证/原生KILL；feature/Unix/caller/Native仍OPEN。原listener只log实际JoinError，真实feature接线须从原JoinSet保留异常证据，不能用Ok或writerDrop/census代替。产品路径/caps/clock/SQL未变；其余门OPEN，无push/PR/归档。


### 2026-10-09：用户授权按 IRU-7 收敛 HMS 范围

- 本次用户明确确认：HMS 的永久只读形态已在其他线程决定；M07 不再修改或要求将由 IRU-7 删除的非只读内容正确，若共享改动触及它，仅要求编译正常。已核对 IRU-7 revision 4（`2026-10-09-iru-7-hms-atomic-commit-design`）及对应 review；永久只读 D6 为已确认裁决，但文档仍为 skeleton，当前源码写 delegate 尚在，不能写成 IRU-7 已实现。
- 保留 M07 的 HMS 读取合同：真实 table/view 分类（a9a7ebe00）、names/types、准确读 Unsupported/not-found；原 32 namespace × 512 table + 512 view 的真实只读 CL、clients 1/8/16；generation-owned 8 positions、原 absolute deadline、SDK future 实际退出后还位、whole-refusal、取消/恢复及真实 FE/BE/fixture 退出。大 HMS 只读 CL 仍 OPEN，小规模预检不替代它。
- NovaRocks HMS mutation、提交/H2、HMS 目标发布/维护/统计，以及 HMS FORCE/drop 的正确性与零 transient mutation observer 退出本次 M07 待办，后续收据标 `excluded by user-authorized IRU-7 read-only retirement`，不标 PASS。REST/Hadoop 写路径、共享 FE/SPI 合同和 Paimon 既定只读边界不受此例外影响。
- 历史 spec/plan 执行事实、失败/PASS 收据和 immutable freezes 保留原字节与原结论；新只读输入另行冻结，不能把已有 FORCE 预检改写成新的只读验收。stock Java/Spark 准备、独立 oracle 与私有 fixture 清理仍必要，属于外部测试数据准备，不是 NovaRocks HMS 写正确性验收。审计草稿 `logs/mem-1-m07/p09-hms-readonly-scope-exception-audit-v1.md` SHA256 `d98c09298f66b35cc285150a9f3fffecaefebed5a74363d9c3d0ed4c82dd7eaa`；本补充不改变其余门或宣称完整 M07 完成。


### 2026-10-09 P09：Unix 同 owner 接缝、原 watcher handle 与 feature 组件里程碑

- 显式 default-off `mem-1-m07-exact-mysql-write` 从 Server→FE→MySQL adapter 转发，不新增生产 TOML。FE 启动一次解析 socket/nonce，取原 bound FrontendProcessId；私有 Unix v1 整帧4096B/16commands/onepeer/单absolute20s/90B path，controller 留在同服务 owner，取消的是 run 借用；Stop 成功仅结束控制协议，继续原 MySQL/report/shutdown 监督。路径只清准确 parent/socket inode，未知/替换留下 cleanup failure。
- 实际 original Ordinary OwnedWriteHalf wrapper、原全connectiontoken、readonly statementtoken、原query bytes SHA；metadata finish后 beginRows，实际 Data write future 因 ExplicitKill Drop 后同步receipt/local一次Resume→原Closing，不改30s/5s/Root window freeze/ACK。target multi与shortcut先进入真实dispatch/refusal，未Arm/其它连接/Control维持既有路径。完整window scalar observer/Native矩阵仍OPEN。
- 原 listener同JoinSet实际session JoinError保留；原watcher创建前固定544位置预留，立刻转移同一个真实JoinHandle到该listener的child owner，session仅持generation abort guard。active空槽仍登记waker并被install唤醒；completed-unreaped继续占原registration位置，实际join后释放。accept failure/finalize都原sessiondrain→abort剩余watcher→实际await，不spawn wrapper任务/新registry。scope首因、session/watcher原JoinError与control原IO source进入FE固定四阶段typed error aggregate；registry只作原bounded drain后的即时核查，不重新无界wait。
- 16真实Unix组件PASS/0.99s，3watcher原handle组件PASS/0.01s，5原listener fixture退出测试PASS/0.05s（含actual callback panic/session panic/actual20ms bounded abort）；1selectedshortcut、2FE启动/原IO来源保留PASS。adapter feature milestone127PASS/0FAIL/0ignored/2.51s；latest default/feature server check各PASS/9.03s、9.40s，format/diff PASS。早期E0603及format失败日志保留；listener v2虽PASS但abort fixture释放session，v3改成真正pending并断言actual session aborted1/watchers joined1，不据弱v2闭门。收据 `p09-exact-fe-poll-write-unix-owner-feature-components-20261009.json`。
- 原intermediary io::Error仍仅log后丢失，须下一slice从同ledger保存实际原cause；结构性missing-tail/满池的合法EOF与未知IO需独立事实，尚无相应Native PASS。组件不能证明真实FE认证/KILL/原W2或Native角色退出；本增量启动Native/Docker为0。独立clientcodec/新inputfreeze/全部exact矩阵、P00b/transport/performance/Linux/P10/final及两个人工语义门仍OPEN。HMS按本次用户IRU-7只读例外收敛，无push/PR/归档。

### P09 original intermediary IO source checkpoint (2026-10-09)

默认关闭的 exact-write fixture 把原 registered intermediary 的实际 IO 原因、完整 connection token 和原 Ordinary/Control 类别移交同一个 session ledger；摘要只含有限数字，第一实际 source 保留，counter overflow 和 supervisor wake 均可失败。owner finish 在原第四阶段纳入该原因，不增加第五个 cleanup stage。真实注册 TCP 握手 / ungoverned-result 拒绝测试观察原 socket EOF、watcher actual join、registry drain 后仍能取出原因。feature adapter **130 PASS / 0 FAIL / 0 ignored，2.46s**；default server check PASS。初次测试插入位置错误导致编译失败的原日志保留，修正后通过。

这只闭合原 IO cause 丢失缺口；missing-tail / Closing pool refusal 的 prescribed EOF 仍需结构化分类，当前没有按 ErrorKind/string 放行。native exact cut、W2 原 freeze、真实 KILL、恢复和四个 role 实际退出仍 OPEN。证据：`docs/testing/mem-1-m07/evidence/p09-original-protocol-io-source-components-20261009.json`，source base `5ee78468f52f05e3847e97ac20ea0ce41d36e68a`。

### P09 independent runner frame-v1 component checkpoint (2026-10-09)

独立 Unix frame-v1 codec/client 已接入 system runner actors（而非草稿 SQL runner 路径），无 adapter/gate/private token 依赖；原 source/tests 字节不变，manifest/lock 未改。严格 4096 全 wire、完整 FE UUID、canonical TLVs/bool/options/enums/尾随拒绝、真实 partial EOF/长度/hash、单原 UnixStream owner、取消借用后禁止复用及跨命令原 absolute deadline 定向 **17 PASS / 0 FAIL / 0 ignored，0.22s**。

这仍是 component evidence：原 scene 20s / FE child fixture locator、actual FE 独立来源、真实服务端三指令互通、精确 native cut/W2/Closing/ACK/恢复/四 role 退出保持 OPEN。实际 server 16 指令包括 Stop，第16次非Stop失败；client pair-stream cap测试不能当 server 健康证明。收据 `docs/testing/mem-1-m07/evidence/p09-runner-unix-frame-v1-components-20261009.json`。HMS继续按已记录只读scope；其它容量、期限、待裁决语义无改动。

### P09 prescribed relay EOF component checkpoint (2026-10-09)

仅 opt-in fixture：原 close_relay 实际 typed Capacity 准入拒绝 / 已验证连续 row 缺尾分支，在精确原 hook、statement 和 cancel receipt 验真后给这**一个**返回错误附 opaque 非clone原因；actual原 IO source move 保留。不按 ErrorKind、字符串或全连接 bit 放行；未知/已规定 EOF 的首 cause 分别固定槽留存，Control、错 generation、第二 EOF不豁免；原第四 ownerfinish 同原 gate receipt 检查。

真实注册 TCP 握手 / 原 intermediary / socket EOF / watcher actual join / registry drain 的三项定向 PASS；缺尾使用一个真实V1 partial1MiB body，满池使用实际64个原V1 Closing grant，不改配置。feature adapter full **133 PASS / 0 FAIL / 0 ignored，2.56s**；后加 mint/geometry 两项 focused PASS；default和feature server编译PASS。初次missing import与test select返回值编译失败日志保留。

严格边界：这未证明 native W2双item、原生fullpool/Root/wire/ACK/健康恢复/四role退出；同kind字符串/Control/第二EOF是ledger组件负例。非Capacity/backing拒绝与完整facade混合cause尚无独立用例。actual FE独立marker和prelaunch20s原clock仍待接线；generic stop不代表FE ownerfinish成功。收据 `docs/testing/mem-1-m07/evidence/p09-prescribed-relay-eof-components-20261009.json`；默认产品失败返回、cap、30s/5s/20s无变更，无push/PR/归档。

### P09 original freeze scalar component checkpoint (2026-10-09)

新 QA/API 只读 getter 仅由默认关闭的 exact-write feature 启用；从原 RetainedRootReply / ResidentRootSegment / fallback delivery 复制完整身份、原native/window序号、frontier、visible长度。原close唯一freeze之后使用同原receipt、buffered计数、既有body views和实际resident_tail选出的长度，同步写已有Hub的单固定槽。无第二freeze、body/reply/guard clone、新task、ACK或read能力。root改用既有Hub.checked维持deadline/firstcause和Stop/wake拒绝路径。

原window模型 **1 PASS**：getter无Arc增持、两原slot序号和Root相同、实际最后owner Drop后capacity为0且标量仍可读。feature adapter full **134 PASS / 0 FAIL / 0 ignored，2.54s**；随后observer重复/错receipt/phase/已有失败 focused1 PASS。原registered intermediary focused3 PASS，并准确断言缺尾只有fallback1MiB、无resident window；pool拒绝在freeze之前，因此没有虚构的空W2观察。default和feature server编译PASS。

Unix v1字节/16tests和旧freezes未变；wire-v2/newfreeze/strictdecoder仅新ignored草稿，尚未启用。真实native双item W2/fullpool/ACK/独立wire/健康恢复/FE成功退出和四role实际退出继续OPEN，visible长度不当完整allocation backing。收据 `docs/testing/mem-1-m07/evidence/p09-original-freeze-scalar-components-20261009.json`。默认产品close资格、cap与30s/5s/20s无改动，HMS按只读例外，无push/PR/归档。

### P09 private Unix wire-v2 component checkpoint (2026-10-09)

同原Unix control owner显式保留V1/V2选择；原bind/v1布局/16组件与runner v1 source/tests原字节保留，opt-in facade仅明确bind_v2。v2追加同原freeze已保存的fixed scalars，最大whole reply744B；4096 envelope、16commands、一个peer、原absolute20s及默认产品路径/caps均不改。独立runner DTO/codec不依赖adapter/control/root owner，新输入另冻结为 `inputs/private-unix-frame-v2-freeze-v1.json`，无版本fallback。修复原freeze新文件截断ASF头；encoder入口可见性、module path与test private imports由定向编译验证。

Unix组件 **20 PASS / 0 FAIL / 0 ignored，0.99s**（原v1 16＋v2 4）；runner组件 **43 PASS / 0 FAIL / 0 ignored，0.61s**（v1 17＋v2 26）；feature adapter milestone **139 PASS / 0 FAIL / 0 ignored，2.58s**；feature/default server check PASS，13.04s/9.73s。首次误用dev-opt造成无关profile重编译，root停止task-owned进程，原中断log保留；随后module path/import编译FAIL日志保留，修正后PASS。新Unix测试固定49B读缓冲、同原3s绝对clock，先originalowner close/exactinode清理再断言；Drop只作物理兜底，不作cleanup收据。

744B独立literal只是最大option布局模型，不当实际W2历史。实际v2 Unix Stop/version拒绝和runner pair-stream证明组件协议，不证明实际FE身份、prelaunch时钟、Root/backing/ACK或four-role exit。真实1FE+3BE exact矩阵仍OPEN，本slice Native/Docker为0；formalrelease性能排除fixture feature。收据 `docs/testing/mem-1-m07/evidence/p09-private-unix-frame-v2-components-20261009.json`。HMS仍按IRU-7用户只读范围，其余人工语义门不改，无push/PR/归档。

### P09 HMS readonly CL external preparation host checkpoint (2026-10-09)

新增caller-owned库式bulk helper，不改旧small helper/freezes：沿原RuntimeOwner/HiveOwner持有全生命周期，原32namespace×512真实table＋512trueview分128pair×4shard，正常external生命周期预计514个fresh stock JVM。caller必须供一次immutable prepare/verification/cleanup绝对clock；原每child cap只向其clamp。未知mutation prefix/实际child或Native依赖未退出时保留owner，不重Create、不把预先/后置oracle当transient observer。Native未settle时拒fixture销毁；validator由主线真实四role/source/config/exit证据另行实现。

host unittest **27 PASS / 0 FAIL，0.197s**，保留全部参数组合与两项真实短Pythonchild capture/实际退出检查；不引入pytest依赖。反例覆盖final cleanup/fsync late失败、不刷新clock、迟到成功收据改FAILED/覆盖失败则撤销精确private artifact、原异常对象和次错。源码模板/合成metadata验证器并非Java/HMS执行，库不启动Nova/HMS/provider。

本slice stockJVM/Native/Docker均0。可执行clean-source/base/bulk冻结、外部固定预算、真实stock准备成本、大只读CL native driver、8positions/SDK future实际退出/取消恢复、真实四role退出仍OPEN。ignored新freeze保持draft/null/false，不能执行；旧规模/caps和产品期限不放宽。HMS Nova写/FORCE正确性继续IRU-7用户excluded，stock外部create/drop属于fixture必要工作；REST/Hadoop/Paimon边界无改。收据 `docs/testing/mem-1-m07/evidence/p09-hms-readonly-cl-bulk-host-components-20261009.json`。无push/PR/归档。


### P09 original FE marker / prelaunch clock / host role-exit component checkpoint (2026-10-09)

原 FE fixture bind 后由 NativeTrust 投影有限 UUID marker；原 ManagedProcess 在 spawn 前持实际 File dev/ino，新 scanner 只读原 regular logfile 的有限快照（2MiB / 512B scratch / 384B line），拒绝 replacement/history/重复或不完整 marker，不以 Unix DTO/PID 回填 Native identity。host20s 在原 cluster launch 前只 capture 一次；显式 fixture 只准 FaultScenario 1FE+3BE，binary 解析和 invalid-topology 都保留失败并清理已有 owner。

FE 成功门复查原 deadline 前后并要求实际 ExitSuccess，任何失败仍 stop 同 FE+原三 BE；固定七槽保留 actual error。startup marker IO 首因与 close secondary 保留；exact terminal/diagnostics 有限呈现，内部原 cause 不丢。定向 **harness9 PASS / runner49 PASS / FE marker2 PASS / adapter140 PASS / process-support56 PASS**；其中四个真实 host shell child 分别覆盖 exit0/exit1/expired-exited/expired-live，先原 owner stop 再 actualPID ESRCH。feature/default server check、fmt、diff check 通过。最初 testsupport 缺 imports 的编译 FAIL 保留后已修。

收据：`docs/testing/mem-1-m07/evidence/p09-fe-marker-prelaunch-owner-components-20261009.json`。原 v1/v2 codec/input 与旧 native freeze 字节未改。四 host child不是 Native FE/BE；output join 跨原 deadline 仅源码 postcheck、未定向 faultinject；appendable log identity 不证明原地内容不可变或 rawstdout 独立来源；同步 launch/forcedcleanup 不声称物理20s上界。本轮 Native/Docker0；完整 actual token/Root 独立源、真实Unix互通、W2/精确cut/ACK/recovery/四role实际退出及最终同HEAD验收仍 OPEN。


### P09 original successful-bind raw marker component checkpoint (2026-10-09)

原 `hub.bind_statement` 成功 Some 后、writer 前输出原 connection/session/statement/hash 入参的一次有限 marker；三个 generation/epoch 域分别读取，session CID 单独保留，不从 Gate snapshot/v2 DTO 回填 expected。默认/无 hub/None 路径不输出不锁 stdout。固定384B ASCII栈、一行最大289B、无SQL/nonce/Root/body alias/新query/registry/task。write/flush 原 IO cause 随原 async outcome/terminal.complete/registered protocol ledger 保留，错误封目标原scope，不能按kind/string豁免。

定向6 PASS，adapter里程碑146 PASS/0FAIL/0ignored/2.56s；feature/default server check、fmt、diffcheck PASS。收据 `docs/testing/mem-1-m07/evidence/p09-original-bind-marker-components-20261009.json` 保存实际源/草稿/日志pins。sink及ledger move是组件证据，未实际stdout/intermediary/join；本slice Native/Docker0。有限同步stdout不声称blockedhost sink硬期限；独立external parser/actualsameFE marker、完整Root/握手/wire/取消矩阵与原FE成功及四role实际退出仍OPEN。


### P09 independent original source and one-row geometry checkpoint (2026-10-09)

原 FE/BE durable log visitor 接到同一个 original managed child，保留 spawn-time file identity、原 PID/birth token 与前后 live/deadline 检查；同一次 snapshot 有限扫描完整 FE identity + successful raw bind marker，expected FE/CID/SQL hash 分别来自独立原源，不从 Gate/v2 reply 回填。2MiB / 512B scratch / 384B line 不变，新增全 offset reserved-stem recognition，合法 pair 后的 embedded/overline marker 也整份拒绝。connection generation、session epoch、statement generation 三个原域分别保留。

Root observer沿原三BE日志 baseline prefix hash与原四launch实例，严格实际 live descriptor UUID库存；最多每BE8个fresh markers，按真实fresh task/context集合找唯一 prepared ClientRows Root，不猜2task/最高stage/BE0，不增加RootFetch/proxy/target SQL。descriptor UUID独立保留；ActualBinding从raw bind +独立Root source投影并核对FE/BE，不以被验响应补expected。prepared事实不冒充Installed、running/End/ACK或physical last-alias。

独立one-row oracle补writer remaining在current Data before/after内与buffer≤remaining-after的几何约束，拒绝S−1处Native2/body8+buffer9伪完整尾部及借next body缓冲；保留合法Some(empty)、原zero-length tail parts。原tiny1..6、x S−1/S/S+1、wide q17 S+1 missing-tail十个cut/outcome tuple锁定，不改原SQL、规模、caps或期限。V2 Unix client实际io::Error移入私有nonclone source槽，有限Debug/Display不展开；实际OS connect错误与synthetic inner-source Arc identity在owner close/anyhow move后保留。wire/literals不改。

定向73 PASS/1.14s、Root8 PASS/0.01s、harness11 PASS/2.07s；补whole-source5反例、source projection1、actual IOcause2后，完整runner组件里程碑 **217 PASS / 0 FAIL / 2 existing ignored / 7.09s**。迭代用dev/test unoptimized、jobs1/incremental0/threads1；首次ClientFailure手工fixture缺cause的E0063 compile FAIL原日志保留，已修。fmt/diffcheck PASS。收据 `docs/testing/mem-1-m07/evidence/p09-independent-original-sources-components-20261009.json` pin实际源/原草稿/两次独立review/failed+passed日志；旧input/oracle draft保留原字节。

本slice Native/Docker0，不把paired Unix/host shell/scalar DTO测试当真实FE/BE证据。bounded original MySQL reader、clean-source new inputfreeze、scene driver和actual Unix/握手/Root/wire精确取消矩阵仍待接入运行；fullClosing64/latealias/ACK/recovery/四role实际退出及其余P09/P00b/P10/final门仍OPEN。同步topology/file syscalls只作deadline前后拒晚成功，不声称物理whole-launch/scan硬20s；census不声称FE/BE原子cut。HMS非只读按IRU-7用户授权excluded，两个待裁决语义/caps/clock不变。无push/PR/归档。


### P09 original exact reader / driver / admission integration checkpoint (2026-10-09)

有界原MySQL reader及原十场景driver已接入：保留原x S−1/S/S+1、wide q17 S+1 missing-tail、tiny1..6 SQL/cut/hash；4096 scratch、同原socket/握手CID/absolute clock，实际partial EOF与零response follow-up不按ErrorKind放行。原driver使用private FE-only Unix pair、独立raw bind/Root source和固定16control位置；original prepared config在四role spawn之前冻结实际secret-free artifact，operation前与四role cleanup后复核同File/identity/hash/clock。原20s在launch前一次捕获，实际all-role cleanup即使过期仍执行，并在最终证据写入后拒迟到成功。

严格execution admission已接opt-in CLI，默认registry不变：原large/tiny不可执行草稿字节不改；另要求真实clean source/full commit、实际server/runner binary/hash/build/config、原input stream hash，每case前后同pins再核验。server仅feature提供启动前fullcommit诊断；runnerfullcommit与NativeBuild分别检验。capture原Child、双EOF/exit0/stderr0与一次prep30s、command5s（setup/spawn在内）成立才准入；失败1s actualreap仍未知则保留同Child失败，不把Drop当退出。所有opt-in失败source保留但终端仅有限verdict。

完整runner组件里程碑262 PASS/0 FAIL/2既有ignored/15.52s；后补dispatch唯一选择/canary与pre-spawn clock后定向141 PASS/0 FAIL/9.58s。prelaunch harness真实host反例1 PASS/0.02s，确认callback拒绝在任何role spawn之前；Python最终verifier17纯mock PASS/0.010s。version10组件及feature/default server check PASS。迭代dev/test unoptimized、jobs1/incremental0/threads1；先前wide测试producer缺lenenc/socketlocator过长/tempfile缺依赖失败原日志保留，修正后通过。收据 `docs/testing/mem-1-m07/evidence/p09-exact-native-reader-driver-admission-components-20261009.json`。

本slice Native/Docker0。final verifier组件不是实际运行验收：必须外部独立冻结原launch PID/birth tokens与pre-role artifact，原runner实际wait0、schema5与operation/raw/source/build一致后四PID实际ESRCH；external orchestration与原生十场景仍OPEN。fullClosing64/latealias/ACK/backing/其余压力、provider大CL、P00b/CM/CP/Linux/P10/final及两人工语义裁决不闭合。HMS非只读按IRU-7用户scope excluded；无push/PR/归档。


### P09 exact Native first run and segment-boundary census correction (2026-10-09)

clean `ef0350bc061ffa64222191b9f7e1b425128acaae` actual server+runner同HEAD feature build PASS74s；实际server fullcommit诊断exit0/stderr0，binary分别435081352B/121842488B，另新execution binding frozen/runnable true，原两ignored不可执行输入不改。external orchestration12纯mock PASS/0.024s；实际首次x S−1原1FE+3BE PASS，same originalhandshake/cut/Root/freeze、完整row+ERR1317/70100与sameTCP nativehealth符合独立hash，原runner actualwait0与四原launch PID/birth的ESRCH通过。outer49.682s含prep/hash，绝不当原scene20s耗时。原生收据 `evidence/p09-exact-native-resident-cut-1048575-ef0350bc0-20261009.json`。

接着原x S case FAILED beforeKILL：原cut1048576已让首S-byte native Data消费/退休，actual BE census只保留第二Data8B/segment1、producerExited1/terminal1/End1；driver误要求原整行2Data/S+8。失败operation/runner1与四原roleESRCH原样保留，收据 `evidence/p09-exact-native-resident-cut-1048576-ef0350bc0-20261009.json`，余case未启动，不把清理成功当case PASS。原one-row oracle早已覆盖S/S+1的seq2/currentbody8/Some(empty)；原immutable JSON未要求已消费Data永恒驻留。

仅修正测试driver观察：原complete x cut<S严格2Data/S+8，cut>=S严格1Data/8；tiny仍1Data8，wide仍2Data/2S/running/noEnd。反例拒绝错误whole-row/one-S/producer状态；driver定向19 PASS/0FAIL/0.31s。生产窗口W2、SQL/cut/bytes/hash/30s/5s/20s/commands不改。下一cleanHEAD实际重跑完整十场景，当前原十case整体仍OPEN；剩余P09/P00b/P10/final及两个重大语义决定不闭合，HMS非只读继续IRU-7 excluded，无push/PR/归档。


### P09 original exact ten-case Native matrix PASS (2026-10-09)

clean `1fb1319dfca5b9141b79cbbe4008b7c617e86e54` 的 actual server/runner 同 HEAD feature build PASS（dev unoptimized，8m26s）。另新 immutable execution binding 关联实际 fullcommit、两个 build identity、binary/base/Cargo.lock/source hash；原 large/tiny 非 runnable 草稿和原输入字节保持。串行十个 fresh 1FE+3BE 全部 PASS：原 x S−1/S/S+1、原17列 wide S+1 missing-tail、tiny1..6。actual outer launcher exit0，十个原 runner actual wait0；每项 schema5 passed/0fail、原operation/source/config/wire恢复或准确partialEOF与同socket后续零response一致，40个原launch PID/birth的实际ESRCH通过。

outer十case累计488.338s包含source/build哈希与独立prep，不替代/续期原scene20s、prep30s、command5s、productionClosing5s/write30s。原 ef0350 的 S 边界失败仍保留 FAIL，修正后是新clean十项证据，不覆盖历史。收据 `evidence/p09-exact-native-ten-cases-1fb1319df-20261009.json`；ignored artifact-index钉住272个原产物，binary/input/build/source/wrapper哈希另在收据中保存。

本项只闭合原精确十场景矩阵；不证明process-group/descendants、fullClosing64、seal后late ACK-held Native send alias、fixed-core/完整allocator backing最后alias、其余P07/P08/大provider CL、P00b coefficients/CM/release CP、Linux或final同SHA C0/C10。两个用户语义决定未变；HMS非只读正确性按用户IRU-7 excluded，无push/PR/归档。

### P08 selected Root support full-manifest component PASS (2026-10-09)

在原 `TaskManifestBinding::bind_facts` 校验完整 root task/anchor 后，立即检查准确冻结 root BE descriptor 的现有 V1 support；位置先于 access.instantiate、Connector 初始化和 TaskCreate。支持的其它 BE 不能替无支持 root 放行；不按全体 candidate 过滤 topology，不增加 profile/wire 字段或 fallback。

完整生产绑定回归用真实 Values(Int64) whole-plan validation、root freeze、Native encoder 与 bind_facts，覆盖 ClientRows/CountOnly 缺支持拒绝、支持 root 与 legacy 非 root candidate 共存、foreign attempt 完整 root identity 拒绝。artifact **32 PASS / 0 FAIL / 0 ignored，0.08s**，attempt owner **12 PASS / 0 FAIL / 0 ignored，0.11s**；实际两个命令 exit0，fmt/diff-check PASS。收据 `evidence/p08-selected-root-support-components-20261009.json` 固定 integrated source/diff、两项独立审查、原草稿与实际日志 pins。

测试使用 private finite request-bound/schedule 事实，未伪造 QA 私有 move-only AttemptSchedule 或 Connector 启动计数；实际 literal access 集为空。真实 QA ticket→公开 bind/activation 观测与 Native 缺能力拒绝仍 OPEN。前一十场景 Native 绑定 clean 1fb1319df、早于此 guard，不挪作新代码证据。P08 完整启动包络/SDK联合校验/运行期增容/Host drain、其它 P09/P00b/P10/final 门仍 OPEN；两个人工语义、cap/deadline 与 HMS IRU-7 例外不改，无 push/PR/归档。

### P09 original held-response actor component PASS (2026-10-09)

新增 runner 私有 actor 持同原 H2 response/stream/sender/driver；仅读实际 gRPC prefix，固定4KiB capture、16KiB frame/16 frame界，不释放credit、clone DATA或继续drain。外层borrowed owner保存spawn后的原handle；reset/drop/abort后actual await同handle才产join收据。start被取消、原clock过期、driver提前退出/panic均失败；确定性test-only原join前Notify barrier取消settle后，outer仍持原handle，重入实际join且sticky失败。prepare沿既有strictprobe连接advertised host+原runtime实际BEgrpc，不把proxyport记录成actual endpoint。

原协议10项＋新actor11项定向 **21 PASS / 0 FAIL / 0 ignored，0.33s**；actual command terminal exit0。non-test runner check PASS（1m46s）、fmt/diff-check PASS。独立审查指出的v1重入误成功、端口错绑、expiry测试弃server join结果均已在v2修正，旧ignoredv1 bytes保留。收据 `evidence/p09-held-response-actor-components-20261009.json` pin源码/原草稿/独立review/实际日志/旧freeze与manifest/lock。

真实duplex H2及本地Bytes::from_owner是host组件，不冒充BE alias/authenticated Native。held reply不完整decode；withheld是drop前lastsample，reset_requested仅方法调用，不能推BE收到RST。Drop仅abort不产join证明；新Native scene必须全部分支持同outer+runtime并actual settle、保存原primary和cleanup原因。独立原FE身份/launchclock、新可执行freeze、同context正BE send holder/seal后两次lateACK1维持accepted0、释放后context收敛/原MySQL恢复/四role退出仍OPEN；fullClosing64/backinglastalias/其余P08/P00b/P09/P10/final不闭合。无产品cap/deadline/依赖/旧input改动或publish。

### P08 fixed SDK listing startup agreement component PASS (2026-10-09)

原 Server config load 和 FE/BE composition 入口统一检查实际 owner 的冻结 V1 参数：各 catalog8位置、REST connect5s/read30s、OpenDAL List16MiB及原SPI六维listing bound。只读getter投影实际被使用的常量，无新增TOML/env tuning或第二admission owner。独立V1 literal拒默认漂移；整毫秒/u128→u64/page乘积checked，原ConnectorError保实际类型与InvalidRequest。请求更紧bound不改，不猜read30s≤caller10s、List16MiB≤owned page或SDK位置×FS字节的跨域关系。

参数/溢出/实际error downcast **5 PASS**、app_config **65 PASS**、composition **8 PASS**，均0FAIL/0ignored/actual terminal0；Server非test lib+binarycheck、fmt/diff-checkPASS。后加一个cfg(test)错误类型回归，先前65/8的生产源未改。收据 `evidence/p08-sdk-listing-startup-components-20261009.json` pin实际源码、原draft、独立review与logs/profile；仅加server dev serde_json依赖，Cargo.lock只加既有workspace package边，无生产新依赖。

加载门早于role启动，composition首句不冒充早于调用者已有runtime/scan资源。JSON/参数检查不证明List运行行为、SDK实际future退出、第三方内部硬字节界、Native大CL/CM。P08完整checked进程包络需P00b系数，运行期增容/全Hostdrain/里程碑C0及最终同SHA仍OPEN；其余P09/P00b/P10、人类语义和HMS IRU-7例外不变，无push/PR/归档。


### 2026-10-09：P06s HMS 原 SDK 对象观测组件 PASS

原 catalog generation 的 ListingAdmission allocation 增 default-off 私有 observer；HMS Names/Tables/Views delegate 把原 client 返回的实际 SDK future inline 交给 helper，首次poll/Ready/实际对象析构与 wrapper退出、原permit归还、settled分列，不新增SDKclient/后台task/globalregistry。默认feature-off与原collector/Unsupported/stop/absolute deadline保持；原SDK业务结果不因诊断invalid被替换。IRU-7 Nova HMS非只读正确性继续excluded，共享路径只保证编译。

初次feature测试编译成功，前三项PASS后，原8pending对象＋第9等待的测试实际stack overflow/SIGABRT，terminal101；原日志保留FAIL。State与Snapshot改为同1024条fixed heap slice、独立snapshot clone，idle reset fill原allocation；未缩cap/输入或增thread stack。补phaseMAX sticky invalid；实际generic SDK poll panic反例保留原Arc payload、原future destructor在permit仍持位时退出、permit8恢复、证据invalid与后继原业务OK。deadline组件先poll确定actual SDK Pending，再await同一次100ms absolute deadline，不改生产clock。

actual admission13 PASS、内部3 PASS，feature catalog80 PASS/default catalog67 PASS，均0FAIL/0ignored/terminal0；前两filter包含在80中，不累加。feature非test connectorlib与默认Serverlib+binarycheck、fmt/diffcheckPASS。收据 `docs/testing/mem-1-m07/evidence/p06s-hms-sdk-object-observer-components-20261009.json` pin实际七source/lock、原draft/v1/v2独立review和初始失败/最终通过logs；dev unoptimized/jobs1/incremental0/threads1。旧draft/失败和review bytes不覆盖。

这些是NO-I/O generic future/catalog组件，不是stock Java、真实Thrift或Native HMS退出证明。observer UUID只识别allocation，非原ConnectorControlBinding instance/epoch/FE/QueryExecutionId；每snapshot独立持1024heap records，真实导出owner必须限制并存份数和persist-before-idle-reset。实际FE/generation/request-stop关联、有界出口/phase精确调用预算、原32×512table＋512trueview/clients1,8,16 Native大CL/SDK8取消恢复/四role退出仍OPEN；SDK对象Drop不冒充RPC/连接/返回body最后alias。本slice Native/stock服务0；heldlateACK/fullClosing64/backing/P08/P00b/P09/P10/final及两人工语义门不闭合，无push/PR/归档。


### 2026-10-09：P09 独立 FE 身份与启动前时钟接线组件 PASS

前一 HMS observer 检查点为 `09cda0b5450c6339d1837520a3b2c03d5ec195f8`。新增 default-off `mem-1-m07-root-observation` 从原 NativeTrust 输出真实 Frontend UUID，原 FE managed durable log 保持 birth/file identity 前后核对；whole-source reserved-stem scanner 拒绝嵌入、重复、截断和超长候选，不借旧 exact marker。场景一次 absolute clock 在首个 role spawn 前消费，与 exact clock 互斥；原 exact Hub finish 和四 error 槽不改，neutral 用普通原 role shutdown。

实际 marker IO 保原 io::Error，私有有限 Debug/Display wrapper 经 source formatter-panic canary 验证；实际 run_one 的 launch_config/clock 拒绝保原 primary＋teardown Arc 身份，teardown 一次。scanner6 PASS、新 runner 反例3 PASS、runner全组件279 PASS/0FAIL/2既有ignored、neutral marker2 PASS、双feature FE server24 PASS/0FAIL/0ignored；重叠计数不累加。non-test Server default/neutral/both 三配置与runner check、fmt/diff-check PASS。初次错误 --lib 调用在编译前拒绝和fmt单行换行失败均保留原日志。收据 `docs/testing/mem-1-m07/evidence/p09-neutral-fe-source-components-20261009.json` pin实际11 source、原exact/lock、草稿、v1/v2独立review与实际logs；dev unoptimized/jobs1/incremental0/threads1。

本切片 Native/stock服务0，不把prelaunch拒绝当成功启动、source/marker组件当实际Native身份或20s物理syscall抢占。旧同步startup/source等待保持前后clock拒晚成功；中间Passed artifact须配原runner终态0与外部独立final verifier。实际build identity/clean source input admission、具体held-response scene/原handle全分支settle、同context正BE holder/seal后两次ACK-only consumed1 accepted0、释放后原MySQL恢复/四role退出仍OPEN。fullClosing64/完整backing-lastalias/P08/P00b/P09/P10/final同SHA及两个人工语义门不闭合；IRU-7 HMS非只读正确性excluded，caps/deadlines不改，无push/PR/归档。
