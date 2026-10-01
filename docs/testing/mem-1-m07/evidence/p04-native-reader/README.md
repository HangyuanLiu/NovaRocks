# P04：同一 admission 的准确 ACK 与 Native send owner

parent `f65f83969eed245f8fb91a2700749057496e1f78`；当前 source SHA256、完整验证命令、退出码和原始/压缩日志 hash 见 [index.json](index.json)。环境为 macOS arm64 / pinned Rust 1.92 / Cargo dev；这是 transport-independent 模块的行为切片。

Reader 从原 Worker context fence 取得唯一 read holder，不依赖 task horizon 或 legacy result reader。固定 wrapper metadata 事前从原 1MiB envelope 子分配；同一个 admission 先应用准确 consumed、解锁并销毁至多 W=2 个退队 owner，然后申请完整两份发送 copy capacity，再独立回答 wanted。ACK-only 的完整两份 4KiB envelope 和 wrapper 使用一次固定 metadata grant，不依赖 Data pool 位置。

满池 ACK 可以先释放真实 backing；仍有 Data alias 时只能退逻辑队列，不能归还其预算或假称 send admission 已成功。seal 先到不应用晚 ACK；ACK 退队后的 callback 通过真实 Registry quiesce/release seal 时，返回 inline AwaitTerminalControl / actual consumed，不 reroute、不造新 holder。Unknown/Mismatch/Preparing/Busy 和容量拒绝分别显式返回。

Native send owner 只允许 strong clone，不导出 Weak/raw Arc/Deref。最后 strong 通过 `Arc::into_inner` 先释放私有 Arc heap，再销毁返回的 Resources，归还 full copy credit / 原 delivery / metadata；pinned std 的准确分配和退出源码 hash 已记录。该 API 要求后续真实 backing wrapper 先销毁 backing 再销毁 owner；encoded_len 或 handler return 不构成退出证明。Worker 的有限 credit 发行壳（同一个实际 closure size、私有 owner Box、Darwin mutex）已纳入原 core，ChannelState mutex 在发布前串行初始化，避免首次锁的临时双份候选。

主 agent 通过实际 library export / consumer 的 reader 15、Worker lib 313、真实 Native Host 61、producer session 12，共 401 个相关测试；Worker/Native 两包 all-target Clippy（既有 warnings）、fmt/diff 全通过。Worker 第一次 root target 23/23 同时包含四个新增反例，不重复计入 401。

Agent 首次编译失败（两个 fixture 缺 Debug derive）和 metadata 拒绝反例的错误 idle 断言均保留。后者的真实语义是：拒绝在 ACK 前，原 Data 仍留存，不应 idle；修正 oracle 后通过。历史日志不冒称绑定最终源 hash，最终主验证绑定 index 中的当前源。

未安装真实 FetchTaskResult 服务、未新增 RPC 或更改 frozen manifest；尚未解决 Tonic 初始 EncodeBody allocation / response extensions 提前 Drop、HTTP Body / H2 DATA alias 的准确 owner 接线。这里未分配独立 wire backing，不能冒称 C4 / Native transport 容量已通过。P04 仍执行，V1 未 advertise；Statistics 的最后 materializer permit 和增长前 workspace 尚待接入，入口见 [源头审计](../../statistics-source-growth-audit.md)。没有候选 1FE+3BE 功能或性能结论；Linux 正式测试按用户指示后续手动执行。
