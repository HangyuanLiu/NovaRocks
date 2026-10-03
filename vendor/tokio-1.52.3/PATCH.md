# Tokio 1.52.3 socket registration patch

本目录从本机 cached registry 的 Tokio **1.52.3** 完整复制。复制前逐字节核对 cached `.crate` 中的 555 个原始文件；版本没有升级、没有下载。crate SHA-256 为 `8fc7f01b389ac15039e4dc9531aa973a135d7a4135281b12d7c1bc79fd57fffe`，与生产 `Cargo.lock` 对应。`UPSTREAM.json` 保存 registry source、crate checksum 和全部原始文件 SHA-256。保留原 LICENSE；不复制 registry `.cargo-ok`、compiled `target` 或缓存目录。

本切片修改八个原始文件：

- `src/runtime/io/{scheduled_io,registration_set,registration,driver,mod}.rs`
- `src/io/poll_evented.rs`
- `src/net/tcp/{listener,stream}.rs`

## Opt-in API

新公开入口需要既有 `net` 与 `io-util` features。`Bytes` 由既有 `io-util` dependency 提供，没有新增 dependency、feature、钱包或 runtime 全局 capacity stock。

- `TcpStream::registration_allocation_capacity_bound() -> io::Result<usize>`
- `TcpStream::registration_platform_mutex_allocation_capacity_bound() -> io::Result<usize>`
- `TcpStream::connect_with_registration_owner(SocketAddr, Bytes)`
- `TcpListener::from_std_with_registration_owner(std::net::TcpListener, Bytes)`
- `TcpListener::accept_with_registration_owner<T>(impl FnOnce() -> io::Result<(Bytes, T)>)`

`connect_with_registration_owner` 只处理一个准确 `SocketAddr`，不解析 DNS，不尝试更多地址，避免一个注册 capacity 被暗中复用于多个并存失败注册。多地址解析/重试由调用方后续独立覆盖。

`accept_with_registration_owner` 先取得实际 OS/mio accepted socket，在任何 `ScheduledIo` allocation 前同步调用原能力 acquisition callback。拒绝或 callback unwind 会关闭未注册 socket；拒绝不分配 `ScheduledIo`。成功返回 `(TcpStream, SocketAddr, T)`，context 属于该次准确 accept。Pending 时不调用 callback，不预占一个握手位置。

调用方必须在实际分配前从原有预算取得完整公开 bound，并传入支撑该分配的原 `Bytes` capability；普通或 empty `Bytes` 本身不证明 funding。Tokio 不取得 provider、query 或预算权威。

## Actual backing and exit

bound 按 `Layout<[std::sync::atomic::AtomicUsize; 2]>` 与实际 cache-aligned `ScheduledIo` 的 `Layout::extend(...).pad_to_align()` 计算 Arc request，并通过公开 `registration_platform_mutex_allocation_capacity_bound` helper 额外覆盖 waiters Mutex 的实际 feature/platform backing。该 helper 准确沿 `src/loom/std/mod.rs` 选择：`cfg(all(feature = "parking_lot", not(miri)))` 使用 inline `parking_lot::Mutex`，该 node 私有 mutex heap 为 0；Miri 或未启用该 feature 时使用 std wrapper。pinned Rust 1.92 的 std macOS 64-bit pthread implementation 只有一个 `Box<pal::Mutex>`，其唯一字段为 `pthread_mutex_t`（signature + 56 bytes，64B）；支持 atomic-32 的 Linux std futex implementation 没有该额外 heap backing。其他 std 平台和 `cfg(loom)` modeled mutex 返回 `Unsupported`，不套用普通 std receipt。workspace feature 合并可能启用 `parking_lot`，不能只按 target OS 推断 node layout。更换 std/dependency pin/ABI 必须重新核验。

`parking_lot` 的 shared parking table、线程 parker 初始化/首次争用和 shared runtime 其他 allocation 不因此纳入 node receipt；private mutex heap 为 0 不表示整个 lock 或 runtime 路径零分配。这些 shared backing 仍属外层 OPEN composition。

funded registration 在最终、尚未发布的 Arc 上预热 waiters Mutex，防止并发 first lock 产生多个临时 Darwin PAL Boxes。内部 `ScheduledIoHandle` 只持 strong Arc，无 Weak 或 extraction API。最后 handle 用 `Arc::into_inner` 先释放 Arc allocation，再退出 moved `ScheduledIo` 的 wake、waiters/PAL，最后释放原 capability；局部 RAII owner 同样覆盖 wake/destructor unwind。

socket `PollEvented::drop` 只完成 deregistration，并不代表 reactor alias 已退出。funded pending retirement 使用第二组 embedded intrusive links，保持准确 driver registration 强引用直到下一个 pre-poll 安全点；没有 pending/shutdown Vec allocation。retirement 和 shutdown detach 在 driver lock 内执行，节点最终析构、wake 和原 owner 退出在锁外执行。单个 funded retirement 会唤醒闲置 reactor。注册失败时，额外局部原能力保证实际 mio source 先关闭。

默认 caller 的 `None` owner 保持 ordinary Arc destructor、原 pending Vec 和 shutdown Vec 路径；没有启用新 funding gate 或改变 TCP/应用协议行为。整个 shared runtime 的旧 pending Vec、poller、events、Waker clones、task/future、socket/TLS 和外层 scaffolds 不因此取得原授证明；公开 bound 仅覆盖该 registration Arc/inline metadata 与 waiters/PAL。OS 调度、socket kernel memory、RSS/allocator caches和完整 Native/2MiB 支持仍属于外层 composition。

本文件记录工程 patch 范围，不记录产品或测试验收。验证命令、source receipts 和实际 physical-exit evidence 由对应 M07 evidence 收据另列。
