# M07 P04：原授入站 socket 注册与 typed connector 接缝

本地行为切片，parent `f55dce9d8f2d6e0f4fdc87e026d502475ec04978`。approved spec/plan v5 不变；P04 executing、P05–P10 open，V1 不 advertise。本收据不构成完整 M07 交付或最终同 SHA 验收。

生产 Native Data/Control listener 各从原 stock 取得一个独立注册位置。OS accept 成功后、Tokio 分配 `ScheduledIo` 前同步取得原连接 config/IO 能力；拒绝直接关闭未注册 socket，不创建注册、排队或借用另一域。握手绝对期限从实际 accept 起算，空闲 accept 不预占 acquisition。每连接 stock 纳入实际注册 Arc/PAL，startup 另纳两个 listener 注册及原 carrier；checked bound 按实际 feature 组合分列：

| macOS aarch64 / Rust 1.92 | 每连接 | 进程 stock | 实际注册 backing |
| --- | ---: | ---: | --- |
| 单 Native package、std Mutex | 6,454,133 B | 3,472,543,834 B | Arc384 B + PAL64 B |
| workspace、parking_lot Mutex | 6,453,949 B | 3,472,444,458 B | Arc256 B、私有 PAL0 B |

stock 同时按实际 Native/Tonic IO Layout 计算；不只手减一个 PAL常数。

Tokio 版本未升级：从本机 cached `1.52.3` crate 完整核对555原文件，只修改8个原文件，保留 LICENSE；`UPSTREAM.json` 保存原文件哈希和 crate checksum，`PATCH.md` 描述 patch。原能力随私有 strong-only `ScheduledIoHandle`，经 reactor 安全退休后，最后 Arc 使用 `into_inner` 先 dealloc backing，再退出 moved node 的 waiters/PAL，最后退出原 carrier/信用。funded pending/shutdown 使用 embedded intrusive links，不新增 Vec；最终 Drop/wake/callback 在 driver 锁外。未发布最终 Arc 预热 Darwin PAL，避免并发第一次 lock 多造临时 Box。默认 None 保留原 TCP、Arc destructor 和 pending/shutdown Vec 行为；节点新增 inline metadata，不宣称原 Layout 完全不变。

`TcpStream` 注册公开 bound 按实际 cache-aligned Arc Layout 计算，platform-mutex helper 精确沿 Tokio 的 feature/cfg 分支：parking_lot && !miri 的 node 私有 mutex heap0；其余 std 路径 pinned Rust1.92 macOS64 waiter PAL64B、Linux atomic32 futex inline，其他 std 平台和 `cfg(loom)` opt-in 明确拒绝。shared parking table/thread parker/首次争用与其他 runtime backing 仍 OPEN，私有 PAL0 不表示整条锁路径零分配。**Linux 未执行验证**，由用户后续手动测试；该源码分支与 macOS 实测分开。

新增 Tonic typed eager/lazy connector：factory 校验后、connector.call 前传入该 attempt 的 URI 与原 IO owner，重连每次取新原能力。旧 URI API 保留。typed connect_timeout 只限制 connect，返回原 IO 类型，避免旧 hyper-timeout 的额外 `TimeoutStream` Box；成功仍由已有 `OwnedConnectionIo` 保持两层 Box。**Native 出站仍使用旧 URI/DNS 路径，尚未切换**；失败多地址注册尾部和旧 TimeoutStream 第三 Box 仍 open，不能用 typed API 准备好来宣称生产出站原授完成。

定向 **673 Native lib +9 NativeTrust lib +75 protocol =757 PASS**；新 socket4、typed connector9包含于其中。实际 System pointer/Layout probes 覆盖 listener、connect/accept、split 最后别名、取消/runtime shutdown、拒绝零注册分配及恢复；socket Drop 且未推进 reactor 时保持原 grant，实际 Arc/PAL 与 carrier dealloc 后才归还。框架/runtime/task/Waker/DNS分配不纳入这个局部探针。2 actual-source negatives 编译后精确一个测试 runtime FAILED，分别恢复 ordinary Arc retirement、遗漏 unpublished PAL prewarm；逐字恢复后完整socket4复跑通过。负例不涉及 pending 指针安全，不接受 compile error/timeout/zero tests/SIGABRT 为通过。

修正后用全量CI生成的生产二进制再次执行本地实际独立进程 **1FE+3BE**，七场景通过：plaintext-IP、automatic-DNS、PEM-IP、outer-preflight-rejection、blocking-saturation-control、partial-body-deadline、registry-contention-control。二进制 source revision 是 parent、dirty候选，实际产品 source pins单列；不假称最终检查点 SHA。最初两次使用错误 scenario 名称被 registry 入口拒绝，未启动集群；按实际注册表修正后七场景通过。首次 Tokio 编译为内部类型重导出 E0603，窄修 `pub(crate)` 后 all-target check 与测试通过。初次外部包 feature 参数拒绝发生在编译前。所有失败日志保留。

本次涉及 shared runtime/全局 dependency patch，按 workflow contract§8.1在定向之后扩大到 Cargo-only全量 CI；修正后全量 Cargo-only CI `logs/ci-full/20261003-171218` **PASS，11,942 passed /7 ignored，555s**，准确结果与 source pins 见 `verification.json`。它不包含全 SQL/default System/性能验收。冷编前仅清理本 worktree 可再生 dev 输出109GiB，未删除 fixture、Docker或业务数据。

两个 upstream 私有 `cfg(tls)` tests不在当前workspace feature中执行，不能冒称通过；实际 Native TLS profiles 使用 NativeTrust另有上述产品拓扑证据。可追加 actual-source public cfgTLS 窄 probe，结果独立列示。

初次 Cargo-only 全量 CI `logs/ci-full/20261003-164010` 发现真实 feature 遗漏：volo-thrift 启用 Tokio parking_lot，原 bound320 B 多算了64 B，四项实测注册测试实际256 B。修正为实际 feature helper，精确数量/字节/物理退出断言不放宽；两组合各 socket4通过。保留失败 candidate source pins，当前 `source-sha256.json`另列，不能把单包通过归为全 workspace通过。

审查另发现 typed timeout 的 Kind-only ioError丢失 legacy Elapsed payload/Display，已保留 ioError::new(TimedOut,elapsed)。最终 get_ref payload oracle 的实际生产 negative 精确一项 runtimeFAILED并逐字恢复；最早 source() oracle接口选错（std source转发内部错误自己的source），其失败和修复后仍失败都保留为**无效负例**，不计source negatives。测试的 edition2021格式化与Native2024不符所致fmt失败保留，pinned2024格式化后CI再次检查。

actual public Tonic cfgTLS probe两项通过：legacy/typed HTTPS无TLS配置同样显式拒绝；typed HTTP经真实H2peer握手传递同一Bytes facts。staticBytes marker只验证API事实传递，不证明原grant或物理free；不是successful TLS握手，亦不是upstream两个私有cfgTLS tests。独立lock校验所有scratch package name/version/source/checksum来自production lock。可复放源在 `public-tonic-tls/`。

仍开放：Native 出站 DNS/注册与 TimeoutStream Box、完整 TaskCell/future/Hyper/Tonic task、TLS内部buffer、auth/error/body/issuer、incoming authenticated peer/lane、完整独立2MiB子图，以及BE新root通道/FE原子整窗与closing。原stock不引入新钱包，不把 logical完成/deadline当物理free。无 push/PR/archive，继续原执行目标。

最终567项源码/lock/toolchain/public-probe pins、137份lossless gzip logs及安全System投影已保存。完整Tokio555原文件逐哈希核对仅上述8原文件变更；`source-sha256.json`保存当前product source，失败candidate map另列。全量CI与七System均在parent SHA dirty候选运行，不能冒称最终本地检查点SHA，更不能外推为M07最终同SHA SQL/defaultSystem/性能验收。
