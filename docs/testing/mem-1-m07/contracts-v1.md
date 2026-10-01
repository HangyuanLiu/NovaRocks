# MEM-1 M07 共享接口 v1

本文件绑定 approved spec/plan v5 的 P01 接口。接口与定向检查不是产品支持声明：旧生产入口在 P08 硬切换前仍不 advertise bounded-root V1；P04/P05/P07/P08 分别接 producer、准入/relay、MySQL 与 Host 防护。

## 依赖与准确用途

`result-contract` 不依赖 Arrow/transport/application，拥有 NativeRenderType、明确 presentation、ordered occurrence schema、RootOutputContract 与 flat body 预验证。`type-contract` 只核对冻结 storage type/nullability。`execution-contract` 拥有有序内核、root read/End/lifetime 与 membership 能力；`result-render` 依赖 Arrow/纯类型，只编码 ClientRows，不拥有 MySQL socket/framing 或 retained wallet。

PhysicalPlan `RootResult(contract)` → plan-codec（按 occurrence 冻结准确 Native slot）→ flat result.proto → Native 纯 decode → LocalProgram `RootResult(Arc<contract>)` 保留同一用途/profile/类型/nullability。LocalProgram 在安装前拒绝错 slot/type/nullability；root 不默认为 MysqlText。当前旧 Result marker 仅存在于 S0 迁移，P08 删除。

ClientRows 使用有界 MysqlTextV1；InternalFacts 只接受 ScalarValueV1、CowSelectionArrowV1、StatisticsArtifactV1、PreparedWriteCommitV1；CountOnly 无 Data，只发布 checked final-root output_rows。顶层 TimestampUtcMicros 与 nested TimestampContainerText 分开，后者使用准确 native unit/timezone；nested VariantJson 显式冻结原 FE offset（绝对值小于86400秒，wire缺失不默认UTC），纯 encoder不读取 BE时钟/Local。客户端 schema 不冻结运行时 dictionary 等 carrier；Execution 负责宣告 carrier 接受集合并在能力覆盖下 hydrate。

## 有序流与生产责任

| 事件 | 条件/状态 | 不表示 |
| --- | --- | --- |
| publish Data(n) | seq 从 1 开始；非空 body ≤1MiB；item+wire byte 窗口 | 对端消费、物理释放 |
| publish Data(n)+End(n+1) | 同一不可变发布 cut；同返时同时 offered | 临时追加历史 End flag |
| cumulative ACK | consumed 从0开始，不能跨 offered prefix/gap；wanted 独立 | ACK丢回执时回滚 |
| publish standalone End | 一个独立保留 terminal 位置；checked row count | encoder 实际退出 |
| encoder actual exit | RootResultLifetime.encoder_exited | context 接管 |
| context handoff | End已发布、encoder已退出 | payload alias free |
| root FINISHED/Task retirement | context保护留存已接管 | root channel 关闭 |
| local End + root FINISHED | 任意顺序；RootReadConvergence.seal_normal | 必须最终 ACK-only |
| originating fail/cancel | seal_terminated，保留原失败证据 | root复合关闭回执 |
| Release/Abort/lease | seal读、停止新发布、drop队列、唤醒；等待真实holder | 接受关闭即 free |

`RootResultRead` 带 exact TaskIdentity/profile/kind、wanted Option<NonZeroU64>、consumed、1..1000 whole-ms wait；None 是可选 ACK-only。每个 root 的 host/application owner只允许一个读 RPC 在途。`RetainedRootReply` 携带完整预准入位置，只暴露借用 payload slices；不把可独立 clone 的 Bytes 给协议消费方。

body 是 `[u32LE rowTotal][MysqlText payload]…`。新行前缀同段至少1字节 payload；新行尾部剩1..4实际字节拒绝；续行短段合法。整个 body 必须在任何 socket写入前 dry-validate；序列/validation/delivery/consumed 前沿分离。

## Producer 与 renderer 签名

Execution `RootResultWriter::open_root(RootResultWriteSpec {task, contract})` 返回 RootResultSession；新旧开流入口不相互 fallback。

- `try_acquire_input()` 在 final upstream pull 前取得唯一 input position 和完整原始/hydrate overlap；一个 RootInputAuthority 是不可混淆 issuer，同 Task 的另一个 issuer 也拒绝旧 permit。generation checked递增。permit不暴露内部 credit，不可 clone；position和现有 retained pool credit跟到实际 input/cursor退出。
- `submit_input(chunk, permit)` 必须先核对本 session issuer；driver不做同步 hydrate/count/render。
- `finish_input()` 只封输入；producer按 quantum完成编码/End/context移交后才报告 `ContextHeld`，该状态才允许成功 root terminal。
- `producer_state()` 返回 Accepting/Finishing/ContextHeld/Failed；`abort()` 封发布，退出仍由真实持有者负责。

`BoundedMysqlTextEncoder::step(&mut self, output: &mut [u8]) -> RenderTurn` 编码 flat body，包括 non-split u32；返回 emitted_bytes、examined_bytes、visited_cells、completed_rows 与 Yielded/NeedsOutput/InputComplete。64KiB byte/1024 cell quantum同时覆盖计数、escape扫描和emit；不把无输出的大值计数藏成同步长工作。`cancel()` 封推进，`scratch_capacity_bytes()` 提供实际容量 oracle。P02 concrete encoder 的 `try_new(schema, RecordBatch)` 在准备时核对类型/presentation；constructor不能假称已覆盖外部 retained capacity。与旧 MySQL呈现的字节差异准确分列：top Variant为serialized bytes；nested VariantJson保留原非ASCII byte→char映射（不是本次Unicode修复），使用冻结FE offset；nested TIME允许原负值，Decimal256仅nested支持；Binary与JSON按准确 container语义转义。所有 malformed输入显式失败，无legacy null fallback。小行≤64KiB单遍 staging；大行只 exact count+immutable cursor，不重算表达式、不整行物化。

## Scope/window、取消 cut 与独立 closing

WorkloadControl 在 ready前仅一次 `configure_result_capacity`，返回同一 authority 的 ResultCapacityHandle；校验 C+H覆盖完整位置、取消burst/rate/exit界及 checked products。未显式安装为 unsupported。位置按 Client/Local/Internal/Closing 分列；完整包络分别8MiB/96MiB/1GiB/8MiB，固定位置不是 allocator grant 或 MEM reclaim 声明。

`try_acquire(scope, class)` 非阻塞取得完整普通位置；P05 在原 warehouse queue 同一事务内组合 permit+window+domain能力，等待时不生成 payload。foreign authority/scope拒绝。每个 grant在原 scope登记holder并可给物理alias附加 guard；最后alias退出才还位置并唤醒原队列。

`GovernedQueryStatementOwner.accept_cancel_delivery_cut()` 仅在已有取消且成功未sealed时接受；`accept_failed_delivery_cut()` 由原失败owner在停止新发布/fetch后调用。两者 first-wins且只take QueryConcurrencyPermit，保留 business、statement generation、execution/effect责任。已接受 originating failure禁止再seal success；finish走ProtocolFailed，不编造cancel。

`try_acquire_closing(scope, ResultClosingCut)` 独立、只尝试一次；普通 try_acquire不能借Closing。`ClosingDelivery::try_new`要求 exact scope/cut、完整 simultaneous backing（含old+new）、无未移交raw alias；失败把所有owner返回以断连。W持独占 writer、frozen metadata/packet cursor、已验证且完整驻留的本行tail；没有fetch/render能力。`ClosingDeliveryAlias`同时持 generation/protocol owner和closing位置；`settle_after_writer_exit().await`销毁主writer后仍等所有physical aliases退出。取消这个等待不能提前释放 generation、业务责任或位置。P07把该接口接到vendor的真实writer/receipt并证明完整8MiB覆盖、绝对期限与缺尾断连。

## Native 方法/lane 与不可变能力

唯一表是 `proto-codec/native_rpc.rs`。FE intent/分批编码、task-codec control grammar 与现有 BE ingress 分类共用该表；P04/P05 独立 listener/channel owner继续消费它。Task/Worker不识别URI。四条 FE物理lane为 ResultData、Submission、Observation、LifecycleControl；BE独立data/control endpoint随announce与exact heartbeat同一Descriptor核对。

BoundedRootSupport含明确control endpoint、closed V1 profile以及完整79字段支持几何；缺失表示unsupported，不从data endpoint或别的BE推断。端点相同、未知profile、缺geometry、任何capacity/alias/queue/window/FD/dial/closing不匹配拒绝。当前Host不 advertise V1，实际listener/握手/Channel防护仍待接线。

pure Close控制投影保留原 UpdateTask envelope/id、exact TaskIdentity、domain版本/edge/destination及原receipt；一个mixed UpdateTask整体走Submission。混合批次按连续method+相同max_wait拆分，保持逐项原身份/顺序/回执；每run以同一 FE submit origin冻结deadline，通过grpc remaining timeout让BE ingress取min，不用长wait续短wait。原envelope不重写。重复 singular 字段合并前的raw preflight跨整个operation累计domain/schema/node/capacity，不能逐出现重置。

## 尚待产品接线的事实

SQL pre-optimizer的准确 presentation freeze、BE input/hydrate/encoder/context移交、FE原队列整窗准入、四lane实际隔离、heartbeat控制端点、原 MySQL入口、local/domain/count所有源头、Host完整防护与旧LRA/IPC退役属于后续P04–P08。P01接口验证不把这些项目计为完成。
