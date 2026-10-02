# P04：启动预授 Native 连接原池及真实 BE 接线

Parent `a402473c57868804fc3dfc5918e1df75af7dc7a0`。approved spec/plan v5 不变，P04 executing、P05–P10 open、V1 未 advertise。Linux 由用户手动后补；本轮仅本地实现与验证，无 push、PR 或 archive。

BE startup 在 root producer 增长、listener bind 和 Native dial 前，从 `backend_application` 原有 `ResultRetainedBudget` 预授固定 stock；同一 Arc 继续进入 root producer、result writer 与 Task host。没有单独钱包，没有 root 增长后的剩余容量竞争。startup 容量不足为 Configuration error，两个监听端口均未绑定。安装后的 BackendDataRuntime 建立独立空 channel cache，所有 BE consumer 与 readiness 使用该 runtime，避免复用未授额的旧 generation。

Data 518、Control 20，checked connection pool receipt **6,407,013 bytes**，whole fixed stock **3,446,973,748 bytes**。Data 包含 incoming FE32、双方 peer448、handshake floor32、额外 outgoing FE report6；Control 为保守 incoming/outgoing12+handshake floor8。计数仅为固定 stock，**尚未证明实际握手并发、每 peer/lane 门或生产 Control 路由**：本轮 production listener/client/readiness 全部使用 Data。Control 在实际 endpoint helper 的测试中有独立能力，但生产分流待后续。

每次 attempt/reconnect 从同一预授 stock 取得位置，生成九类 fresh 原能力：raw frame、encoded headers、decoded field arena、typed HPACK table、send header block、send frame、DATA、GOAWAY、HTTP HeaderMap。窗口256KiB/1MiB，frame/header16KiB、send64KiB、adaptive=false、Tonic pending8；表初始typed4096 backing，advertised/send table0；DATA64×16KiB、GOAWAY1×16KiB，field1MiB/4096位置/max16352，map1024/keys16/extras16，额外请求 fail closed。新 h2 production dependency 由既有 dev dependency 移入，实际锁版本不变。

stock 计算包含九池 public allocation bounds、每位置 guarded Bytes wrapper、Core Arc 与固定 AtomicBool Vec、既有 issuer callback 的 Weak capture。原 factory 为 strong-only handle：最后 `Arc::into_inner` 使 Core Arc allocation 先退场，随后固定 slots Vec，再退原 credit；budget Arc 保持 issuer 存活。位置由所有实际池/字段 alias 共享的同一个 Bytes exit guard 归还，guard 在 wrapper 实际 free 后执行；连接完成、timeout、逻辑取消不能提前归槽。**既有 ResultWriteCredit 的 boxed callback 在调用 release 后才退出，因此 callback/issuer/observer graph 的完整物理退场仍开放，不能据 stock credit 数字扩写为全部 issuer graph 的 physical reclamation。** 本轮未新增 GlobalAlloc probe；原 vendor 完整 source 与既有 pool probes 在同组验证中运行。

Listener 在 `incoming.accept`/首 TLS I/O 前获得 fresh config 并纯校验/attach 九池，实际 h2 once bind 仍发生在 TLS 后 `serve_connection`；没有声称 pre-TLS bind。Tonic endpoint 的 actual factory 在 connector.call/TCP/TLS 前构造 fresh config，重连再次取得。readiness 改为同一 factory-backed HTTP/2 channel acquisition，drop 后 background task 持池至实际退出；它没有执行 JWT application RPC。Listener2s 当前仅包 TLS accept（从 accepted_at 算起），client connect2s不等于完整 H2握手。h2旧 handshake 不等待初始peer SETTINGS，**完整 preface/SETTINGS/首次flush 绝对期限仍待下一切片**。

最终10相关协议目标107项、Native六组定向lib46项，共 **153 PASS**。新增13项：factory8、startup/cache2、真实IO3。实际TCP listener 验证 request 原 map/field family，HeaderValue逃逸，在 listener 与 peer 连接任务实际退出后仍持一个 Data位置；最后 alias Drop才归槽。两条真实 duplex Channel 经 actual capacity_endpoint 获得独立 map/field families，实际 peer/clientIO退出后原 HeaderValue aliases 使Control18→19→20，异步buffer/connector handle真实退出后原预算归还。None listener 与原 consumer 行为同时回归；没有把测试中的Control helper当生产Control路由。

6个实际源码负例均编译后 runtime FAILED，finally精确恢复：遗漏 listener 安装、独立 startup 钱包、复用旧cache、ordinary outbound config、提前slot release、Control借Data。初始 ordinary-config 负例因缺少 Result error 类型注解发生 compile failure，未计入负例证据；修正 driver 后六项全部得到真实运行时失败。最终源码已重新 pin 并通过153项。Native check、HTTP/Tonic strictlibClippy、Native production lib/六protocol目标Clippy、root/vendorfmt、diff均0；既有其他 Native warnings保留，新 production factory/wiring 与新测试的 Rust 编译无warning。Clippy未覆盖全部cfg(test)源码，不声称完整crate零warning。

Hyper server/http2、Tonic channel-only最小feature均0，63个dependency identities与生产锁一致。26个actual product/test/issuer/geometry/lock pins、264个完整 actual vendor source/manifests pins、65份lossless日志/diff及feature manifest/lock在本目录。没有替代算法副本。`verification.json` 区分 scoped checks、open gates 和产品验收。

**冻结算术**仍为 connection independent2MiB + streams×(bookkeeping4KiB+idledecoder8KiB+headers32KiB) + Tonicpending8×4KiB；不能把本轮6.41MB九池总和当2MiB连接独立证明，也不能把额外stream/Tonic/thread/TLS/socket/closure/cache scaffolds遗漏。每 stream实际backing分项 census/持续local stream128、全body/message/errors/auth/decode图、Control/perlane/handshake gates、FE issuer/整窗中继、closing/退出deadline及P05–P10仍开放。没有完整 Native1FE+3BE、workspace/SQL/system 或性能验收。本轮按contract第8节覆盖本crate接线与既有传输行为，既有h2依赖移位未引入版本/锁/全局词汇表变更，未触发里程碑全量。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-native-transport-capacity/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-native-transport-capacity/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-native-transport-capacity/reproduce_features.py
```

Docker此前 desktop-linux全BOM已经通过，image/JAR齐全。本次offline切片没有重复pull/build输入、改变globalcontext或要求Linux环境。fast选择不缩减批准范围，persistent goal保持active。
