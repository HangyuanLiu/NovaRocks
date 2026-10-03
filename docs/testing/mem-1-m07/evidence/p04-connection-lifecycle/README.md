# M07 P04：原连接生命周期与失败 acquisition 名额真实退出

approved spec/plan v5 保持；P04 executing、P05–P10 open，V1 不 advertise。本收据是一个本地可验证切片；parent `a68ec6a25c01e82d6995320b920622c05b83005f`。

h2 新 strong-only lifecycle Core/observer 和可选 retained-acquisition Bytes Box 先按真实 Layout 在同一 BE StockCore 原 grant 内预授。强引用不暴露 Arc/Weak/raw ownership；最后 Arc.into_inner 释放 Core allocation，observer/retention backing 在共同 carrier 前退出。一次绑定连接；初始 SETTINGS 真正 apply 与 ACK/local flush 后才记 INITIAL_COMPLETE，最终 acquisition 由 Tonic/Native 在同一绝对 D 的 late-Ready/callback/post-D verdict 后发布。真实 H2 Connection 持 bound lease，terminal/GOAWAY/失败/Drop 永久 retire；不是外层 conn.await 的替代证明。Hyper server 在 initial→Serving 后显式让出一次，Native 完成最终 verdict 后才派发应用；任意外部 caller 仍必须遵守这一手交接。

Native 固定 physical record 使用 checked generation，不回绕；ACQUIRING→INITIAL_COMPLETE→LIVE→RETIRING 与最后 SlotExit→VACANT 分开，旧 generation 回调先拒绝。真实十池/header alias 与 lifecycle 共用同一 original carrier，最后一个实际 alias 退出才能归还 stock。当前每连接原容量 **6,452,333 B**，进程 stock **3,471,366,250 B**，Data518/Control20、共享 acquisition Data32/Control8 保持；没有从这些全局数推导 per-peer/lane 保证。

最终 callback 拒绝或跨 D 时，Hyper 的独立 ConnTask 已可能持 IO，返回 Err/drop SendRequest 不能同步证明 IO 退出。Tonic 在拨号前让 lifecycle 共同持原 acquisition carrier；失败/取消等到实际 codec/IO 后 bound lease Drop 清额外 alias，ordered wrapper 继续持原 alias 到真实 future 退出，取两者较晚归还。成功 post-D verdict 后 explicit release，允许应用 IO 继续服务。安装使用一次 AtomicPtr 发布协议，INSTALLING→UNBOUND CAS，退休赢后 setter 自清；只有 swap-null 赢家接管 Box，无 pointer/reference 逃逸、无 Darwin lazy Mutex 隐式分配。move Bytes 先让 Box 物理释放；RAII 包含 observer Drop panic 清理。

固定源码验证 **151 protocol +57 Native lib PASS**，独立实际源码 primitive **17 System +17 Miri PASS**、primitive 和 h2/Hyper/Tonic 严格 Clippy PASS，Native scoped Clippy/fmt/diff PASS。12 个新实际协议测试验证缺 D 首 IO 前拒绝、跨 client/server once-bind、真实 ACK flush、server ready HEADERS 派发顺序、GOAWAY/IO/alias、Tonic final refusal/cross-D/cancel/自然 ConnTask join、实际失败 acquisition Exit 必须看到 IO exit、成功释放而 IO 尚 live、None legacy。三项新 Native generation/phase/最后 alias oracle 包含耗尽回滚。79 product pins、291 六个 vendor Rust source pins；logs 为 lossless gzip，hash 清单另存。

五个 **actual production source** negatives 均编译后 runtime FAILED、finally 逐字恢复：漏 retention、忽略 generation、initial 直接 Live、server 漏 yield、Tonic 漏 final verdict。四个 primitive negatives 是精确正常 module 的 **scratch copy** compiled runtime FAILED，和 root 实际源码 mutation 分列。

首次实际接线编译失败与修正、首轮 callback 两项 IO 同步退出错误断言、单 crate Clippy 前的 warning、首轮 edition2021 定向 rustfmt 与 workspace2024 格式差异均保留日志。callback case 已改为自然 join 实际独立任务，不用 abort、mock finished 或睡眠假造退出。错误断言并不替代随后发现的 production acquisition 提前归还缺口；新实际 carrier oracle击穿该缺口。

复现：`python3 docs/testing/mem-1-m07/evidence/p04-connection-lifecycle/verify.py`；primitive `primitive-reproduce.py --clippy --miri`；真实 production negatives `run_regressions.py`。negative 会临时改实际源码，须串行执行；driver finally 精确恢复。primitive 使用私有 crate 和生产 Cargo.lock 一致的离线 dependency，无安装/下载。

仍开放：准确 peer/lane physical quotas、bounded singleflight/cache、production Control 分流、完整外层 future/task/socket/TLS/message/auth/error/issuer 图、P05–P10。独立 h2 client preface async 取消的 IO→owner 顺序不由本收据外推；真实 Native/Tonic 有 ordered outer owner 与 bound retention 两重实际退出证据。当前没有完整 workspace/SQL/system/native1FE+3BE/性能候选结论；上一 checkpoint Cargo-only CI 收据仍只属于上一 SHA。Linux 按用户安排手动后补，无 push/PR/archive，goal active。
