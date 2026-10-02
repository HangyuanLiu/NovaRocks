# P04：H2 原 DATA owner 保留至实际 flush

parent `0c7ccb1a190a8e882f44e99df9d2ca336313de44`。h2/Hyper 两端新增默认 false 的 `retain_data_payloads(bool)`，只编码 DATA header，跳过 tiny payload 和 large prefix 的 codec copy；原 Data<B> 在 Next 中保留至所有部分写和 upstream poll_flush 成功后才供 reclaim/requeue。空 DATA 的 header 也必须写完。Native 的 SendBuf<Bytes>/Bytes::advance 保留原 owner；一般 Buf 可以在 advance 时释放内部 chunk，不能泛称所有泛型 Buf backing 均受同样保护。原 min_buffer_capacity/默认 copy thresholds 保持，9B header/HPACK/TLS/task/backing 另授；未安装 Native listener/FE Channel，P04仍 executing、V1 未 advertise。

默认 writer 的独立 payload copy 通常复用预分配的16KiB BytesMut，不等于每帧都 malloc。strict 模式避免这一责任转移，并防止 flush Pending 后下一次 poll_ready/reclaim 提前 Drop 原 frame。实际剩余编码字节时 Ready(0) 现在返回 WriteZero，修复旧循环无进展重试；这是通用 writer fail-fast 修正，正常默认 batching 不变。错误/reset/逻辑结束并不代表 frame 物理退出。

6 项实际 production-lock 协议测试包含：0/1/255/256/1023/1024/16384 × vectored/non-vectored 的14组，每次写最多3B，先只允许1B原 payload，再允许完整写但重复卡 flush，最后完整 flush；实际 writer 收到的 payload pointer 全部来自原 backing，原 ResultRetainedBudget pregrant 始终 held，成功 flush/reclaim 才能重新申请完整容量。Payload fixture 包含完整64KiB inline array，其实际 owner/exit-guard Box Layout在构造前由同一信用预授；可见 len 不代替完整 backing。0B 也保留此强 owner，wire END_STREAM/header 正确。独立 32769B case跨16384/16384/1三帧，通过逐次 flush allowance验证同一 grant跟随 requeue、前两 flags0/最后1与全部原指针写量，到第三 flush才释放。当前peer默认16KiB；此模式本身不限制 peer SETTINGS调大的outbound maximum，冻结Native profile仍待接线。

partial write/flush Pending下本地send_reset、connection Drop、write/flush error与WriteZero都有实际信用退出oracle。审查指出最初reset fixture用了send_data(...,true)，请求也EOS，send_reset会沿closed/empty早返回；该旧fixture的positive不算InFlightData::Drop覆盖。最终Reset/ResetFlush改为保持发送半边打开；partial case在drop stream/response后仍held直到connection实际Drop，完整写/flush Pending case解除flush后验证真正RST_STREAM(stream1,CANCEL)的13B wire与原owner退出。恢复旧EOS并同时放宽DATA flags的negative仍在RST wire断言真实101，证明准确取消路径。首次文本修改命中两个fixture而AssertionError，未修改源码；重试仅限定server_send。中间未修正fixture的重跑日志保留但不冒充最终reset证据。

Hyper client/server经过真实 cloned Config、SendBuf<Bytes>与AsyncWrite，分别验证原指针、部分写、重复Pending flush、最后退出。manual executor锁外移走tasks后drop，清掉引用环。默认mode单独保持旧small copy/最终相同wire。raw peer帧/ACK预置只证明受控解析/推进，不认证真实网络handshake时序，不构成Native cancel/deadline/lane验证。

7 类negative均101并逐字节恢复：恢复payload copy→原指针写量0，提前flush reclaim→原credit提前可用，empty忽略header→DATA帧丢失，移除WriteZero→fixture捕获第二次zero write而有限失败，两端Hyper各漏转发→指针/owner失败，以及上述closed reset fixture→缺RST。zero fixture在第二次调用断言，防止错误实现无限spin；并未测试所有任意IO panic清理。初次测试编译错用了server::handshake的第二泛型参数，日志完整保留。只读审查确认真实源码两处原缺口闭合，指出并促成reset/requeue证据补齐，不替代测试。

最终恢复源码的28个H2/Hyper integration、Native lib574/Worker lib313合计915非重复测试通过；h2/Hyper strict lib Clippy无warnings，Native alltargets Clippy有既有warnings，workspace alltargets check、root/vendor fmt/diff通过。公开builder/共享codec以及通用WriteZero修正触发wave workspace检查；最终reset变动仅为fixture，定向重跑与alltargetClippy再次通过，无重复扩大lib测试。未尝试standalone upstream dev suite，未运行未接线产品的最终SQL/system/native CI，无性能验收。strict mode可能增加scalar writes/每帧flush，后续正式Linux测试由用户手动执行。source/log/mutant/原registry hash见index.json，Cargo锁/版本未变。无push/PR/archive。

剩余完整connection read/HPACK/header/stream/task/socket对象、queued generic Body count、握手/认证/连接位置与Native冻结配置继续。Docker desktop-linux输入BOM已在本turn前一checkpoint live核验完整，没有缺image/JAR，未pull/改变全局context。

```bash
cargo test -p novarocks-native-adapter --offline --locked \
  --test native_h2_retained_send --test native_h2_retained_backing \
  --test native_h2_bounded_receive --test native_hyper_bounded_receive
```
