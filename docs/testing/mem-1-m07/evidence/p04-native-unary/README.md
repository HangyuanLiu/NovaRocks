# P04：Tonic unary 编码与具体 DATA Body

parent `c18f1faef8cb55fc97f05d6f3bf3381489f07c71`；当前 source/hash、精确命令、退出码、压缩日志以及 DATA guard negative mutant 的源 hash 见 [index.json](index.json)。这是 macOS arm64 / Rust 1.92.0 的模块行为切片，没有安装或增加 RPC，既有 FetchTaskResult manifest/signature 尚未切换、V1 未 advertise。

真实 Tonic 0.12.3 `Grpc::unary` 消费现有纯 Root request/reply DTO。Reader 在同一次原 1MiB fixed metadata reservation 内追加实际 post-admission allocation：`Layout<EncodeBody<RootEncoder, Once<OwnedWireReply>>>` 的 Tonic Box、bytes split/freeze Shared header、一个 guarded DATA wrapper、准确 Backend UUID 的 16B Vec；先于 ACK 和 DTO projection，没有另发钱包。内联 DTO/Source/Encoder 已计入该 Box Layout。

RootService 在返回 Ready 前把 strong owner 交给本次调用私有 handoff。Tonic 调用 codec.encoder() 后才创建初始 buffer；encoder 已持有同一个 owner，完整发送 pregrant 按 payload 的 `2*(1MiB+4KiB)` 或 ACK-only 的 `2*4KiB` 留存。单条 unary 使用一个固定初始 buffer，不开启 compression，也不生成 encode_to_vec 中间 copy；编码前先验证 encoded_len+5，再检查 EncodeBuf 的实际连续剩余容量，避免把 remaining_mut 的 usize::MAX 当作实际 capacity。

`grpc.unary().await` 返回后同一个 poll 把 inner BoxBody 和独立 owner 移到 concrete Body；不通过 head/extensions 的寿命证明。默认字段退出顺序先销毁 Tonic Box/encoder buffer，再释放独立 owner。唯一 DATA 通过 additive Bytes exit guard 包裹；任意 H2/消费者的 Bytes slice 都留住原 grant，最后 wrapper allocation 退出后才释放 owner。多 DATA 显式拒绝；正常 DATA、trailers 与 EOF 经真实 Tonic 路径验证。

新增 10 个真实 Worker context/unary 反例，连同旧 reader 15 项共25通过；producer session12、相关 backend_task_execution150通过，合计187个本切片 consumer 测试。覆盖：初始 buffer 在首次 poll 前存在且保留完整信用；Body Drop / seal 后最后 DATA slice 留存；满 data pool 的 ACK-only；post-root transport metadata 不足时 ACK 未生效；long-poll cancellation；Releasing 的准确 closed watermark且不创建 Root holder；两个 unpolled Body 真实占据两个位置；CountOnly End 经准确 codec；oversized request 拒绝；1MiB最大段只有一次 encoder buffer allocation、无realloc、准确Data+End和trailers。Native all-target Clippy（既有 warnings）、fmt/diff通过。

allocator probe 在测试当前线程按精确容量跟踪实际 System allocation/deallocation。payload response 为一次初始 buffer；ACK 的 4KiB 采样还会选到独立 pre-decode request buffer，准确共两次，最新 pointer 才是 encoder backing。DATA slice 测试删除 guard 的 negative mutant 后，实际 backing 仍活着而 context 被错误判 idle，oracle 准确失败（exit101）；源码 byte-identical恢复后25+12项通过。

首次测试的四个失败均保留：最大段不能直接向 output() 返回的64KiB量子写1MiB；ACK采样不能假设只选中encoder；metadata fixture必须计入实际schema.backing_bytes；正常 release已经到TerminalRetained/horizon时Root查找正确返回UnknownRoot，closed marker要在原holder维持的Releasing阶段验证。修正准确fixture/oracle后通过；未弱化产品界或凭空延长已退休context。

本 helper 还没有生产 listener 的 pre-decode lane/stream/header/future/mutex holder；normal trailers HeaderMap、closed/error control buffer也属于该原 lane/header envelope。具体 Body 仍须在真实 Hyper/framework handoff 使用，新增外层 Box 必须由真实 lane/stream owner覆盖。H2小 DATA复制到独立connection writer buffer的事实还需connection owner覆盖，不能把最后Root Bytes alias退出称为全部H2副本退出。上述均保留P04/P08 gate，本模块不证明完整C4、Native lane容量、分布式或性能验收；Statistics source与P05–P10继续。Linux正式性能测试按用户安排由其后续手动执行。
