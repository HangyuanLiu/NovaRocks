# P04 Buffer Semaphore/Handle 原授元数据

本收据对应 parent `324bcd88a686b28791ab7c175de4197d2652d241` 上的 dirty wave，不是完整 M07 验收。新增公有查询覆盖真实 `Arc<Semaphore>`、真实 `Arc<std::sync::Mutex<Option<ServiceError>>>` 及各自私有 PAL。Semaphore 按 Tokio 有效 std/parking_lot 分支查询；Handle 始终使用 std Mutex，不能因为 Tokio 启用 parking_lot 就少算它。original pair 在尚未发布的真实对象上预热，不改变 permit、closed、waiter 或 error 状态；普通 None 路径不预热、不接收原能力。

同一 generation Bytes 随 Buffer、response cells 和最后一个 unpinned Worker 字段保留。Worker 的 Handle 和 Semaphore Weak 先退出，原能力随后退出；Worker 完成和字段真实析构是不同事件。既有 Tonic actual Worker TaskCell 原授继续独立覆盖物理 task backing，重新查询真实返回类型，没有新钱包、cache/Core/JoinHandle 回链。Native 从同一 StockCore 为 230 个 logical generation 各预授实际 common metadata。

新增五项实际 System allocator 测试通过：查询零分配；真实 Arc+首次预热 requested bytes 等于公有查询；重复预热及无 waiter 的 acquire/release 无新增私有分配且状态不变；最后 Weak 才释放 Arc backing；owned/None pair 差值核验两块实际 PAL；Buffer/cells/公共原能力全部退出后，已经完成的真实 Worker 仍挡住原预算，最后 Worker Drop 后才归还并核实最初 pair backing 已 dealloc。构造期间观察到的队列分配只是物理退出 oracle，不是队列资金或峰值证明。parking_lot consumer 同五项通过，验证 Handle 的独立 std PAL 没有遗漏。

三个 compiled runtime source negatives 均失败 101，随后逐字恢复：遗漏 Worker 原能力、遗漏 Handle 预热、把 Darwin Handle PAL 写成零。独立同源码 Tokio scratch 的 Loom cfg 和 active unstable tracing 各一项通过，明确 query/prewarm 返回 Unsupported 且 ordinary state 保持；不是 Loom 模型。四种独立锁定 consumer 通过，最终依赖身份零漂移。第一次 Cargo 发现 pinned pin-project-lite 不支持字段 cfg，改用按 feature 选择的私有字段类型：启用时 Option<Bytes>、关闭时零大小 `()`；首次定向格式化误选 Native edition2021，也已改回实际2024。失败记录均保留。

恢复后的定向 817 项通过。十个最终源码 pins 在全部验证后未变；Cargo-only CI `20261004-100841` PASS，638 秒，component12085/7 ignored、server owner173、binary smoke4，共12262项通过。没有新增生产 unsafe，未重跑不变 oneshot 的 Miri；该切片 Miri 证据留在上一 response-port 收据，不外推为本轮或整个 Native 的 Miri。

最终 binary `626aa2b49fa28281785ace109833455b5e3f5b6fe3381bce168c122695eaefbe` 上，独立本地 1FE+3BE 八个 System 场景和八个 SQL case 全 PASS。System 覆盖 distributed baseline、三种 NativeTrust 配置和四个 ingress/control 场景；只保存 safe allowlist 和原 artifact path/hash，不复制运行 config、JWT 或 private keys。SQL 为 analytic 两项、aggregate 四项、iceberg-dml 两项。DNS 兼容通过不决定 bounded DNS 的 ownership。

实际 std common metadata 为232B×230，共53360B；process stock3590507218B 通过冻结4GiB检查。Worker实际查询仍640B，不把新增字段推算成固定 task 增量；physical connection6672053B 不证明完整2MiB envelope 已闭合。队列、readiness、ServiceError payload、外部 Wakers、shared parking/runtime、外部 service future/body、DNS/TLS/socket/auth 仍开放。队列审查排除了所提 sender 暂停点的无限消费调度，但尚无 blocks 数值峰值证明，不能填8，也不宣称无界。

P04 executing、P05–P10 open、V1 None，Scalar Host gate 闭合；DNS/nested scalar 裁决待用户。未声称 full SQL/default System、Linux/release 性能或最终 M07 完成；Linux 测试由用户后续执行。无 push、PR 或 archive。
