# P04：固定 H2 原始帧输入收据

此切片相对 parent `1b4e56bae4df2231bd9f42e135aa144cbb480319`，是 P04 的局部实现。h2/Hyper 两端增加默认 None 的 `receive_frame_buffer`，使用同一原 owner 的固定 raw Vec/Core，握手前 once-bind；不升级依赖、不修改生产默认值。P04 继续 executing，P05–P10 未完成，V1 未 advertise。没有 Native 产品、全 SQL/system 或性能验收结论；Linux 正式测试按用户要求后续手动执行。

## 实现与证明范围

调用者先取得完整 `allocation_capacity_bound`，另授 ownership carrier metadata，再构造固定 `max_payload + 9` 原始存储。公开 handle 只有 strong clone；唯一不可 clone 的 mutable lease 经 CAS 产生，独占 UnsafeCell。最后 raw Arc 和 Vec 实际 dealloc 后原 carrier 才退出。clone/bind 不新增 raw backing，失败 geometry 不消耗一次绑定机会；同一 handle 退出 connection 后仍不可重新绑定。

Codec 的 Some 分支直接构造 fixed reader。审查发现过的“先 Codec::new 再安装”会临时生成默认 8KiB raw read backing，最终实现已消除。None 分支保持原 Tokio length-delimited codec。

Fixed reader 只读完整 9-byte header，检查 U24 声明是否超出本地上限后才读 payload；不预读下一帧。Pending/部分 header/body 保留实际进度；clean EOF、truncated EOF、I/O error 均有准确终态；零 payload 不调用 empty ReadBuf。完整 frame 才产生独立准确 BytesMut 副本。超限直接 FRAME_SIZE_ERROR，真实 reader 只消耗 header9 bytes。

raw 信用仅覆盖其 Core/Arc 和 fixed Vec。frame copy、io::Error 诊断、caller carrier metadata、retained DATA、HTTP/HPACK/CONTINUATION/GOAWAY、socket/I/O/task、allocator caches/RSS 另计。16KiB frame copy 实测请求16393 bytes；可以晚于 raw owner 退出。没有把16KiB逻辑上限当作整个连接 footprint，也没有把本 probe 的 ExitCredit 当作 Worker 钱包集成证明。

## 行为与退出验证

真实 production-lock h2/Hyper：新7项覆盖固定地址和禁止预读、DATA pool 满且逃逸 alias 仍在时不读取下一 header、raw/DATA 原信用独立退出、零/1-byte frame、client response、超限 body 缺失时立即拒绝、失败 geometry/reuse 在 client preface 写前退出、克隆 Hyper 双向转发。SpyIo 记录真实 AsyncRead 请求/指针/位置，非 parser 内部计数。raw peer 预置 SETTINGS ACK，是受控字段/帧 fixture，不能证明真实 Native 握手时序。

恢复后协议组合43项（新raw7 + header8 + count7 + pool6 + send6 + Hyper9）通过。Native lib574 + Worker313 通过，本切片930项非重复 workspace tests。组合重复运行不再累加计数。

隔离 probe 复制未经改写的当前 receive_frame.rs 和 patched bytes，使用生产锁的 tokio1.52.3/io-util、bytes1.11.0、pin-project-lite0.2.16，offline + locked。普通14项、Miri13项通过；Miri仅跳过16MiB最大存储例。actual System allocator 在实际 dealloc 返回后才记退出，检测 raw two allocations 与 original Bytes carrier physical-exit hook。16KiB raw请求16481≤保守bound16497；64KiB65633≤65649。超限 U24例仅有小 io::Error backing（stable24B/Miri40B），没有 payload/frame copy，真实读取9字节/1poll。并发 once-bind和最后drop覆盖实际lease与退出；Miri不是全部线程交错证明。

probe 还覆盖无 clone/bind 分配、partial/Pending恢复、0..8-byte header EOF、部分body EOF、零帧、sticky I/O错误、pending取消、输出副本晚退出、invalid geometry 和完整最大U24 geometry。复现：

```bash
python3 docs/testing/mem-1-m07/evidence/p04-h2-raw-input/reproduce.py --miri
```

脚本只使用已经安装的 nightly/Miri/rust-src，不安装、不下载，先拒绝缺失组件。隔离 strict Clippy和定向fmt通过。

## 可失败反例与检查

六个临时 product mutations 都产生实际测试 FAILED / cargo101，并保存完整diff与日志后逐字节恢复：普通 Arc drop使credit早于Arc dealloc；bound漏Vec低估实际请求；CAS不设置bound允许第二lease；改回default reader导致读取越过首个DATA边界；Hyper server/client分别漏转发导致首次read8192而非9。反例没有以编译失败或源码形状匹配冒充行为验证。

恢复后14普通/13Miri及43协议重跑通过。h2/Hyper lib strict Clippy零warning；Native all-target Clippy、workspace all-target check通过（既有warnings）；root/vendor fmt及diff检查通过。全局vendor公共API/共享Codec变化触发此次wave的workspace编译检查；未接线产品不提前冒充最终生产验收。

初始两个失败完整保留：协议测试 harness 用了不存在的 Poll::is_ready_and，SpyIo 缺 Debug；probe harness E0502用step_index修复。它们是测试编译失败，不是runtime反例。原型检查与修正后直接构造检查都记录。

h2原66文件、Hyper原68文件及crate checksum与本机registry完整核对；原文件累计修改清单、source SHA256、命令exit、原日志与gzip hash在 [index.json](index.json)。日志没有截取关键段替代完整输出。未运行独立上游dev-suite，不称该suite通过。

## 后续接线

frame copy、GOAWAY公开错误多别名、HeaderMap容量/重复metadata与carrier lifetime、HPACK table spare、stream/task/socket/握手仍需同一Native原信用和完整2MiB包络证明。Native listener/FE Tonic Endpoint/Channel未安装本API，连接reconnect必须每条物理连接获得fresh buffer。独立control/result lanes、入站/预decode/FD/handshake和P05整窗接管继续。当前Docker desktop-linux完整BOM通过，无缺镜像/JAR；默认orbstack无需切换或pull。
