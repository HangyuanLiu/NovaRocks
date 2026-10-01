# P00 源头、接管与退出清单

审查基线：`eb35251de575e071ad3657d0ce0fc1fc95d1a91a`。表中是当前事实与待安装保护；没有把 source review 当成容量或产品验收。路径相对仓库根。

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

P06 根据实际 provider 实现调整 SDK / vendor 增长入口属于已批准的源头有界化，不引入另一结果协议；准确文件范围在实施前写入阶段记录。源头尚未受界时保持现有 FE 保护。

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
