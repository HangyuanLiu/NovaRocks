# P04 有限 Native producer 与唤醒 owner

本切片实现 `NativeRootResultSession` 的 ClientRows / CountOnly 和真实固定 CPU pool，尚未安装到 Native host。InternalFacts 在私有 codec 接线之前明确拒绝；不宣称 P04/C4 产品闭合、1FE+3BE 或性能验收通过，不 advertise V1。

Pool 先向同一 `ResultRetainedBudget` 取得 process-only grant，再构造固定 slots、队列、线程及请求 stack。每个 root 最多一个排队项，Yielded 公平轮转，Blocked/Idle 不占线程；预算回调只唤醒已激活 job，不保活准备中的 dormant session。显式输入/finish/abort 激活，generation 拒绝旧 token。关闭先取消，再 join；固定 owner/credit 保留到真实 Pool Drop。Session 不持 Pool Arc，callback 不持 Session Weak；Core 的 Weak allocation tail 由准确 Layout 的 root metadata pregrant 覆盖。

Session 接管唯一 issuer-bound 原始 input 前执行有界、零分配 backing 检查，保留原始 Chunk/accounting；CPU turn 才计数或创建 renderer。CountOnly 不 hydrate/render/cell-copy。ClientRows 使用 caller-owned 1MiB segment，按 renderer 的 64KiB/1024-cell quantum 推进，完整小行立即发布，不等下一 input；W=2 满窗暂停。原始 Chunk/cursor 在 input grant 前销毁，Box cursor/columns clone/scratch 由独立 2MiB 预留覆盖；End 不等待消费 ACK。真实 turn 返回、input/builder 已空后发布 exited，再销毁 producer guard 并通知 ContextHeld，避免观察者看到 ContextHeld 却未看到退出。控制 metadata 仍跟随其实际 owner。

Root readiness 使用固定有界 Observable，保留泛用 `new()` 的原行为。Input authority 预建 129 个 scoped slots，支持 64 个 driver 各自 sink/finish callback 加一个观察位置；channel 预建两个位置，分别供唯一生产 bridge 与 host 观察者使用。构造前 Layout 含 Vec spare、全部固定 Arc slot 和 Darwin lazy mutex；串行 pin，通知使用栈 snapshot，解锁后回调。scoped slot 只有全部 token/snapshot 实际退出才复用。`try_subscribe_retained` 的同一 metadata lease 跟随 callback、slot、snapshot，防止回调退出先于 control tail。未新增钱包或 allocation authority。

实证修复：回调 panic poison Session mutex 时，恢复为失败并由下一真实 turn 清理，不能反复 Yielded 卡住 shutdown；退出通知 panic 不能跳过 exited。初次退出顺序测试暴露 ContextHeld 与 exited 之间的竞态，失败原日志保留，调整真实退出事实的发布顺序后全部通过。初次编译的 tuple variant / ChunkSchema Arc 参数错误也已修正，未放宽接受条件。

验证：Pool 11、真实 Session 11、Observable 12、TLS 隔离 allocator 6、Worker channel 19、pipeline 122、original carrier 2、input authority 1，共 184 项通过。涵盖独立 literal dictionary/null/repeated-occurrence 字节、Decimal256 CountOnly、issuer 拒绝、无 ACK End/关闭、满窗取消、response alias、通知 snapshot 晚于 Session、panic 及真正 grant 归还。分配探针证明有界构造落在 pregrant 内，通知/订阅复用/重入无新增分配。Native all-target check、三包 all-target Clippy（既有 warning）、fmt/diff 通过。日志、命令、源码及原始/压缩 hash 见 `index.json`。

边界：Pool 证明固定请求的 Rust backing/stack；libc/TLS/guard pages、平台 stack minimum/cache 与整进程 RSS 不由该 Layout 推导。完整 Native/DOP callback metadata、共享 budget registry/transport、exchange/special source、domain codec、reader/H2/lane/listener及生产 host 接线继续按 P04–P08 收敛。此处不用模块测试替代产品门。Docker Desktop 的 all-consumer live 校验再次通过，镜像/JAR 齐备；Linux 正式测试由用户后续手动执行。
