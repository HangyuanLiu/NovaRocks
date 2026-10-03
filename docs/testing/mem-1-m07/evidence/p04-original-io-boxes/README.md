# M07 P04：原授 Native/Tonic IO Box

本地行为切片，parent `91eedeb752b59c7c8c8e0f11dd2f2e654be61b50`。approved spec/plan v5 不变，P04 executing；P05–P10 仍开放，V1 不 advertise。此收据不构成 M07 完成或最终同 SHA 全量验收。

原连接 stock 在 TCP/TLS 构造前，新增实际 concrete Native Box 的最大 Layout（明文/Automatic/PEM × client/server）与实际 Tonic 外层 `TokioIo<BoxedNativeIo>` Box Layout。复用原 SlotExit 能力及原 BE retained budget，没有新钱包。当前 checked 每连接为 **6,453,685 B**，进程 stock 为 **3,472,301,802 B**；Data518/Control20、acquisition32/8 保持原数量。

Native server 接受的 concrete Box 由无分配、不可提取 IO/owner 的 `OwnedNativeIo` 持有。Tonic 每 attempt 在校验和 dial 前取得同一 IO 能力，成功返回后由 inline `OwnedConnectionIo` 保持到实际 Native/Tonic 两层 Box 释放。错误、deadline、未首次 poll 的取消、实际 IO 析构 panic 使用同一顺序：先退出 IO 与其 Box backing，再退出原能力。EOF、Channel Ready 或逻辑完成不是归还依据。最后逃逸原能力 alias 仍占原位置。

定向验证：**672 Native lib +9 NativeTrust lib +57 protocol =738 PASS**；其中新 IO targets 为6+9。后续追加的正常 last-owner 测试单独及最终整 target 复跑，旧 protocol 日志中5项与新6项按同一测试去重计数。相关 Clippy/fmt/diff 通过。实际 System allocator 记录 concrete Box 地址与 Layout，证明 dealloc 在原信用释放之前；真实 TCP 与 Automatic TLS 两方向完成 handshake/传输，PEM 使用相同实际 concrete 类型且另有生产拓扑验证。默认 None 不新增包装 heap，异步读写保留 Partial/Pending/error/vectored 语义。

本地独立进程 **1FE+3BE** 的 plaintext-IP、automatic-DNS、PEM-IP 三场景通过，使用新构建二进制。收据保存非敏感 scenario 字段与原 artifact/hash locator，未复制凭据或私钥。首次 runner 错把 fixture 的 `NOVA_ENV_CONFIG_FILE`（Compose env）作为 Server TOML；改为 publication 的 `NOVAROCKS_FE_CONFIG` 后通过。该入口错误没有启动集群。首次新 Tonic success 用例在对端 H2 握手完成前关 Channel，得到 BrokenPipe；改为等待真实 peer handshake 并驱动原 connection task后通过，未削弱释放断言。

三个 actual-source negatives 均编译后精确一个测试 runtime FAILED：Native owner-first、Tonic owner-first、Tonic 成功 IO 漏 original owner。逐字恢复后两个完整 targets 再通过；不接受 compile error、zero tests、超时或 SIGABRT 为证据。测试析构只记录顺序，避免负例清理 double panic 隐藏首个失败。

范围只闭合两层实际 IO Box 和原 carrier；**TLS 内部 buffers、Tokio socket 注册、task/future、Waker、auth/body/issuer 和 incoming authenticated peer/lane 全图仍开放**。Tokio socket Drop 后 reactor 仍可能持注册 alias，不能从本收据推导其物理 free；完整独立2MiB子图亦未验收。无 Linux 性能结论（由用户手动后补），无 full SQL/default System/最终全量结论，无 push/PR/archive。
