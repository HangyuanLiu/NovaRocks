# M07 P04：有界 peer channel cache 与原连接 lane 门

approved spec/plan v5 保持；P04 executing、P05–P10 open，V1 不 advertise。parent `bc521ad8db7435a5fdfd8888ecfd7e93c35a37a7`。本收据覆盖 BE-origin Exchange、RuntimeFilter、Membership 的缓存/实际 factory 连接容量，不外推为完整 Native/FE 交付。

生产 BE 安装一个原授缓存，230 个固定 entry、每 entry 8 个 single-flight waiter；同一准确 process、canonical endpoint（保留 IP/DNS reference-host 区别）、manifest method 共用一次初始拨号。第 9 waiter 立即拒绝，取消 leader 通知全部已注册 caller；caller 取消实际注销位置。entry/waiter generation checked 不回绕，旧 token 不覆盖新结果。冷 miss 优先空位，否则轮转移出无 waiter 的 Ready cache alias；Connecting、仍占 waiter 和耗尽 generation 不回收。Channel/Waker clone、drop、wake 的重入操作在锁外；一个 wake panic 仍通知其他 waiter，外层 unwind 取消不 double panic。

StockCore 直接嵌入固定 230 行 key kernel，无 Channel、cache、factory 或 callback 反向引用。每准确 BE process/method 合计 Exchange Live2/Connecting1/Closing1、Filter Live1/Connecting1/Closing1；改变 endpoint 不能放大同 process lane。Membership 是明确的 None-peer/准确 FE endpoint 域，Live1/Connecting1/Closing1。Exchange/Filter 共用最多 32 个实际仍持有的 BE process，Membership 最多 2 个 FE endpoint；旧 process 的真实 closing alias 仍计数。安装/退休在同一有限 mutex 内裁决，退休不能复活；closing 满则保留原 Live/Connecting charge，直到实际退出，不提前消账。

实际生产 cache miss 使用 keyed endpoint，Tonic 每次 factory invocation（包括同一 Channel 内部 reconnect）重新 claim 同一个 frozen inline key，先于十池构造和 connector.call。stock 失败回滚尚未发布的 key claim；共同 carrier 的最后 SlotExit 才归还 physical key position。缓存移出、RPC future 返回、deadline、observer/IO 退休均不能替代十池/header/lifecycle 最后 alias 退出。

同一个现有 BE issuer 启动原 grant 覆盖新增 StockCore layout、cache Core Arc/固定 Vec、两处 pinned Darwin std PAL backing，以及增长后的 SlotExit/lifecycle observer layout。两个 mutex 都在共享发布前预热，避免首次并发初始化额外 Box；Linux supported futex state inline。新增 libc 是 workspace 已锁定的既有依赖，未下载输入或改变版本。当前 checked 原容量为每连接 **6,452,381 B**、进程 stock **3,471,600,250 B**；Data518/Control20、acquisition Data32/Control8 保持。6.45 MB 是已列池/metadata 合计，不能代替全部独立连接对象 2 MiB 的分项证明。

最终定向证据：**648 Native lib +151 protocol PASS**，32 项新增行为测试。实际 TCP/H2 覆盖 9 caller 共用一个 accept、peer/endpoint/method 隔离、8 joiner/额外拒绝、leader 取消与同 key 恢复；同一个真实 Channel 内部 reconnect 门满时 connector/accept 计数不变，放开后在同 Channel 恢复；生产 cache miss 也在 TCP 前拒绝。真实 IO/task 自然 join、逃逸 DATA/Channel alias 和原 budget grant 返回是退出 oracle。直接 config/lifecycle 只用来准备明确的 kernel 占位，单列为 kernel/实际 carrier 测试，不冒充真实 socket 证据。

七项 actual-source negatives 编译后 runtime FAILED，并 finally 逐字恢复：额外 waiter、首 wake panic 漏通知、内部 reconnect 漏 key、cache miss 用 unkeyed factory、漏 Live verdict、漏退休迁移、stock 拒绝漏 key rollback。修改 Clippy let-chain 后，两项涉及重写 source anchor 的 negatives 再跑并仍 runtime FAILED。Native lib/实际协议消费面、Clippy、fmt/diff 与五项依赖边界 guards 已验证；Native 原有 Clippy warnings 保留，新增三处 collapsible-if 已修正。首次编译的测试 API 遗漏、缓存 borrow、测试 generation 失败、错误 filter 选择的两次 zero-test 输出和修正均保留，不计为通过证据。

复现：`python3 docs/testing/mem-1-m07/evidence/p04-peer-channel-capacity/verify.py`；串行源码负例：`run_regressions.py`。日志 lossless gzip，source/log SHA256 清单另存。独立只读审查无本切片阻断项；实际 kernel exit 会取得有限 mutex，未声称无等待或整个 scheduler/allocator 图已闭合。

仍开放：认证后的 incoming peer/lane 门、独立生产 Control listener/endpoint/FD headroom、FE lane/window/closing、完整 outer task/future/socket/TLS/auth/error/body/issuer 图及 P05–P10。unkeyed readiness/self-probe 仍使用全局原 stock/acquisition 门，不能外推 per-peer 保障。没有当前候选完整 workspace/SQL/system/native1FE+3BE/性能结论；较早 SHA 的 Cargo-only CI 不移作本收据证明。Linux 由用户手动后补，无 push/PR/archive，persistent goal active，按正常模式继续。
