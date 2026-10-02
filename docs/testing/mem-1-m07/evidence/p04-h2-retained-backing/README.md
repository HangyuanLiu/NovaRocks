# P04：H2 retained DATA 实际 backing 与原信用退出

parent `23643d2619c5152db50097453c1669840e0404ad`。本切片新增 opt-in `h2::ReceiveBufferPool`，并经 Hyper client/server builders 转发。P04 仍 executing，尚未安装 Native listener/FE Channel，V1 未 advertise；原 codec read、HPACK/header/continuation、writer copy、stream/task/socket 元数据与完整 2MiB connection 预算没有闭合。本证据不构成 Native、SQL/system 或性能验收。

固定 slot array 与每个完整 Vec capacity 在 caller 取得原 pregrant 后一次构造。池只能绑定一个 connection 一次，两个 builder 在 handshake I/O 前检查复用、有效本地 receive MAX_FRAME_SIZE 和显式 event bound；client 必须显式关闭 push。不存在通过 peer send frame size 或 flow credit 推导 retained backing 数量的路径。默认 None 保持原路径，Cargo 版本/依赖/lock 本轮未改变。

FramedRead 在读取下一 frame 前检查真正空闲位置；DATA 解码后把整个 payload（包括空 DATA）复制到已授固定 block，丢弃原 codec buffer alias 后才返回。最终 Bytes owner 把 Vec 归还到 RETIRING；配对 bytes 1.11.0 exit guard 在包装 Box 实际 dealloc 后才公布 FREE/通知原 parser。若仅在 owner Drop 中先返槽，会让已有 wrapper 尚未释放时新 wrapper 不断进入，不能证明 wrapper 数量界。池只导出 strong handle，最后 Arc::into_inner 先退出 Arc heap，再退出 Core 的 slots/buffers/waker，最后退出 caller 的原 ownership carrier；FramedRead Drop 解除保存的 task waker，wake panic 在清理实际 pool owner 后恢复。

`allocation_capacity_bound` 覆盖 Rust 请求的 Core/Arc、固定 slot Vec、每个完整 block、全部同时存活/RETIRING 的 Bytes wrapper Layout。caller carrier metadata 与 Waker target/framework task 另授，allocator cache/whole RSS 不包含在该界。真实 ResultRetainedBudget 测试先一次预授此 bound 和 carrier metadata，再构造池；不是后测 payload 长度或另开钱包。

实际 H2 6 项测试覆盖两端 DATA、flow release 后别名仍阻塞、最后 clone/slice Drop 才 refill/wake、transport/builder/pool handle 退出后最后 alias 仍持原信用、空 DATA、几何/overflow、frame 超界与复用/缺 count/push 冲突握手前零写入。原 H2 count 7 项继续通过。Hyper 9 项中新增两项通过真实 Incoming 验证：其返回 DATA 时已返 flow credit，但 escaping Bytes 保留固定 backing；body/connection/builder/executor tasks/pool 全部退出后信用仍 Blocked，最后 Bytes Drop 后才可申请完整原容量。测试 raw frame 提前写入，manual executor/poll 控制推进；预置 SETTINGS ACK 只证明解析和受控推进，不证明真实网络握手时序。

隔离 ownership probe 使用当前实际 receive_pool.rs 与实际 patched bytes API，不依赖复制替身实现。System allocator 钩子在实际 dealloc 之后记账；6 项验证完整请求容量界、原信用退出前所有已观察 pool allocation 均退出、wrapper dealloc 先于返槽/wake、空 clone 保留而空 slice 脱离 owner、wake panic 清理，以及 Barrier 同步的两线程最后 alias 退出。普通运行与 Miri 各 6 项通过；Barrier/Miri 不构成全 interleaving 的 Loom 证明。隔离 probe 中未使用的 codec 方法有 dead_code warnings，生产 h2/Hyper lib strict Clippy 无 warnings。

四类反例实际失败 101并恢复：owner Drop 提前 wake 的 physical-wrapper oracle、漏算 block capacity 的 allocator oracle、FramedRead 省略 pool readiness 导致无槽解析、Hyper server 省略 pool 转发导致 available 仍为 1。原 5 项 probe 的初次 early-wake 反例与最终 6 项反例都保留。实际源码逐字节恢复后协议 22 项与 Native lib 574 项再次通过，Worker 313 项通过，合计 909 非重复 production workspace 测试；独立 probe 正常/Miri 单列。只读审查核对 Hyper default/clone/转发和运输退出后原 budget oracle，未发现具体缺陷；审查不替代测试。

中间失败全部保留：首次新增测试编译时用了错误 ResultRetainedBudget 引用/Blocked 名称；随后 5 pass/1 fail 的空 DATA 测试错误地把 `Bytes::slice(..)` 当作强别名。空切片会脱离原 owner，改为实际 clone 后通过；非空仍用 slice。有效 handshake 的 peer fixture 保留至 poll。未改变 production bytes 空切片语义。

Native all-target Clippy（既有 warnings）、workspace all-target check、root/vendor fmt、diff check 均通过。公开 H2/Hyper 接缝及共享 codec unsafe owner 的 wave 收敛触发 workspace check；尚未接完产品，因此未提前运行最终 SQL/system/native CI，也未尝试 standalone upstream h2/Hyper dev suites。原始 registry 文件 hash 再核对：h2 66/Hyper 68 个均与 UPSTREAM.json 一致；本地修改总计 h2 10 个原文件、Hyper 4 个原文件，另新增 receive_pool.rs，包含前两个 checkpoint 的 count 修改。来源、源码与 26 份完整 raw/gzip 日志 hash、5 个完整 mutant gzip 在 index.json。

满池仍会暂停整个连接输入，包括后续 HEADERS/RST/WINDOW_UPDATE；后续接线必须使用独立控制连接与真实 local cancel/deadline→body Drop。原始 codec backing 与 writer 副本不随此 pool 自动受界。Docker desktop-linux fixture BOM 本轮 live 通过，无缺 image/JAR、未 pull 或切换全局 context；Linux 正式测试按用户安排后续手动进行。无 push/PR/archive。

可从仓库根复现协议与 ownership probe：

```bash
cargo test -p novarocks-native-adapter --offline --locked \
  --test native_h2_bounded_receive --test native_h2_retained_backing \
  --test native_hyper_bounded_receive
python3 docs/testing/mem-1-m07/evidence/p04-h2-retained-backing/reproduce.py --miri
```

reproduce.py 创建独立临时 workspace，从当前 vendor 源拼接已保存的探针模块，使用锁定 atomic-waker 1.1.2。`--miri` 需要已安装 nightly/Miri；脚本不安装工具链。snapshot 只记录本次实际受测源码，重跑读取当前源码。
