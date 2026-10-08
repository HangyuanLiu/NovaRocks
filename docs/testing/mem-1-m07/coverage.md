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
| information_schema schemata/tables：FE `catalog_application/system_catalog_facts.rs:53,76`；QA `system_catalog.rs:141,194` | bounded snapshot → virtual source（客户查询最后为 ClientRows） | local schema 复制前借用检查；一个 external catalog 的 namespaces+全部 tables 共用预算，table 连 schema 名计费，后续 listing 带 remaining bound；超界拒绝整快照。snapshot/行 Vec/Arrow 依 source/result 最后引用退出 | 预算是单 catalog 快照，不是进程总量；system_catalog 仍先复制 snapshot 成行 Vec 再进 helper，catalog 名重复/行对象/Arrow 共存需包络。SDK 限制见后表；C6 |
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
