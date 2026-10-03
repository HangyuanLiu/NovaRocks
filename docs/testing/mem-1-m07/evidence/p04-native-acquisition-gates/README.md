# P04：Native 共享入站/出站 acquisition 门

Parent `b3b60eb5ee2285ed029f912b1ba1f681ec6f2f15`。approved spec/plan v5 不变，P04 executing、P05–P10 open、V1 不 advertise。Linux 由用户手动测试；无 push/PR/archive。

同一个 BE 原 StockCore 内置 Data32 / Control8 原子计数，入站 accept 和每次 Tonic factory（含内部 reconnect）共用。两个域不得借用，满额在 connector、TLS、应用 owner clone 和 task 创建前拒绝，无握手等待队列。固定池 stock 的 Data518 / Control20 是另一种能力；握手完成归还 acquisition，不归还 live connection 与原字段 aliases 的 stock。

同一绝对 D 从 accept/factory 前起，覆盖 TCP/TLS、preface、non-ACK peer SETTINGS 应用和实际 ACK/local SETTINGS flush。服务端直接 poll 真实 Hyper connection 的 completion 状态，完成即归还 acquisition，不等两秒定时器。正常应用流不受握手期限限制。

Tonic 公开 `ConnectionAcquisition<F>` 复用已有 pin-project，把 future 和原 Bytes owner 内联放入既有 task/future。构造在返回排队 future 或 spawn 前完成；首次 poll 前取消、Pending 取消、失败及 Ready 都先销毁真实 F，再退原 owner。超时的真实 connector/TLS/H2 future 在 wrapper 内退出；late Ready 的 live connection/IO 输出先 drop 再返回错误。成功输出的连接和原池独立继续存活。没有增加 Box/Arc backing；既有外层 Box/task、输出 aliases 与 TLS 图仍由各原 owner 负责。

计数 inline metadata 与最多40个 acquisition carrier 的准确 bound 加入同一启动预授，stock **3,471,280,708 bytes**，十项池仍 **6,452,189 bytes/position**。stock不足时先前 acquisition claim 回滚。原 issuer 的 callback 图仍开放；不将此小计当整个 Native 内存图。

新增13项测试通过：Native4、Tonic9。Native使用实际 accepted socket、真实 capacity Endpoint 和共享原 factory；其余限额由真实原 funded unpolled configs 占用，明确不是32个真实TCP客户端压力测试。验证 Data/Control隔离、incoming+outgoing同门、拒绝首次IO/service0、实际IO析构先于计数归还，以及完成后 HeaderValue alias 继续持 stock。Tonic验证 dial失败、取消、握手期限、成功完成、无正期限/非法配置、过期首次poll、从未poll的Drop、Ready Err保留IO至F析构。真实退出 oracle为 IO→future→owner及原 Worker credit重授。

六个 actual-source negatives 均编译后 runtime FAILED，finally逐字恢复：多放一位、Control借用Data计数、Ready提前归还owner、取消提前归还owner、Tonic遗漏owner、服务端延迟完成归还至D。最终14协议target **139 PASS**、六Native lib组 **54 PASS**；check、四vendor strict libClippy、Native相关targets/libClippy、root/vendor fmt、diff均0。新增large inline enum用有理由expect避免另外Box；annotation后定向NativeClippy复跑0，既有依赖warnings保留。三个最小feature组合通过，63个dependency identities匹配原锁。

首次编译的测试错误类型推断失败、首次Native case因仍持有可重连Endpoint而无法重授credit的失败与修复日志完整保存。后者单跑同样失败，是真实引用生命周期，修正测试主动退出Endpoint后通过，没有缩短owner生命期。52产品pins、完整五vendor270源码/manifest pins以及压缩日志的原始/压缩SHA与长度均可校验。没有新增unsafe primitive，未重复上一切片的Miri。

仍未闭合：每lane live/connecting/closing、cache single-flight与准确 peer identity、生产Control listener/方法分流、完整stream queue/Inner/socket/TLS/task/diagnostic/auth/future/issuer图及FE/BE部署联动。生产BE仍只有Data listener；组件Control测试不证明生产控制保障。没有全workspace/SQL/system/1FE+3BE或性能验收。Docker fixture BOM确认images/JAR完整，不pull/build缺失输入或改变全局context。按正常模式继续，批准范围不缩减。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-native-acquisition-gates/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-native-acquisition-gates/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-native-acquisition-gates/reproduce_features.py
```
