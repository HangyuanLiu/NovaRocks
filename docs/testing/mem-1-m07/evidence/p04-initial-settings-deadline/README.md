# P04：Native 完整初始 SETTINGS 的绝对握手期限

Parent `bcb71253eadaf4995f199d0f0d098277272813e5`。spec/plan v5 accepted/approved 不变；P04 executing、P05–P10 open、V1 未 advertise。Linux 测试由用户手动后补；本轮只本地实现、验证和检查点。

真实 BE listener 从 TCP accept 后、原池构造与首 TLS I/O 前记录绝对 D；真实 Tonic factory 从创建每次 fresh 能力前记录 D。同一个产品 **2 秒**期限覆盖同步准备、TCP/TLS、preface、初始非 ACK peer SETTINGS 的成功 apply，以及本地 initial SETTINGS 和 peer ACK 的真实 `poll_flush` 完成。期限为零、checked-add 溢出和准备已超时均提前拒绝。异步 acquisition 的首次 poll 和最终 Ready 各检查 D；同步准备的 CPU/分配成本仍须后续完整 census，不将事后拒绝当作 CPU quantum 上界。

h2 public builder/connection 增加 opt-in 绝对期限和 phase poll；普通 None 路径保持原合同。phase 不发布应用 stream，以原 Settings/Codec 处理控制帧；remote initial marker 只在成功 apply 后成立，local WaitingAck 不是 flush 证据。错误为 terminal latch，复 poll 不可转成成功。每个 surfaced control 和 raw skipped unknown/CONTINUATION turn 均检查同一 std::Instant；连续 ready 输入每 32 帧主动 yield。每次 write/flush 与成功 commit 前检查 D。库本身没有新增 timer/Arc/Box；Pending 的到期唤醒由 Tonic 的绝对 timeout 和 Native 的一个绝对 Sleep 提供。

Hyper client 等 phase 成功才交出 SendRequest/启动连接 task；Hyper server 的 InitialSettings 状态等 phase 成功才进入 Serving/调用 service。成功清除 codec/connection acquisition deadline；Native 观察 Serving 后停用 acquisition timer，正常应用流继续遵循其原生命周期。每次 reconnect 获得新的 D 与原池，timeout 只 drop 实际 holder，不能提前退原槽或原 credit。

最终11相关 protocol targets **116 PASS**，Native六组定向 lib **50 PASS**，合计 **166 PASS**；新增13项（kernel7/Native4/Tonic2）。六个 actual-source negatives 全部编译后 runtime FAILED，finally 精确恢复，最终 pin 后全部回归。Native check、HTTP/H2/Hyper/Tonic strict lib Clippy、Native相关八target/production lib Clippy（既有warning）、root/13修改vendor文件fmt、diff全部0；三个最小feature0且63个依赖identity匹配production lock。41 actual source pins、269 whole actual vendor source/manifests pins（包括Cargo.toml.orig）、47份lossless完整日志/diff已保存。最早相关编译遗漏新增Config字段、测试fixture用了Tower而非Hyper service_fn，两次compile失败均保留，修复后回归；vendor fmt最初仅两个新snippet折行，机械格式修正后0。真实 Native TCP/duplex 测试保留产品 2 秒：zero/23-byte-preface/no-SETTINGS/partial-SETTINGS 的真实 EOF/连接退出、service0、failure callback1、原位置与预算归还；后两个在连接后1100ms才发 preface，检测错误重新计时。真实 outbound 无 peer SETTINGS 时不得交出 Channel。成功 bootstrap 后2200ms应用流仍成功。四种 h2/Hyper 过期首次 poll 必须 IO0/service0。新增kernel200ms component-only真实frame测试覆盖ACK/PING、malformed终态、raw32 selfwake、已apply值7但ACKflush Pending、Pending到期与Ready-late；它只验证kernel逻辑，不冒称分配图。Tonic 50ms component-only fixture 加80ms同步 factory 时 connector0，zero timeout 同样拒绝；该 fixture 不修改 Native 产品几何。

本切片闭合完整初始 SETTINGS 的逻辑 acquisition deadline；**没有证明** TLS/kernel/socket/task/error/auth/body/message/stream/cache/issuer callback graph 的完整物理 backing 上界或实际退出 deadline。新增 inline Config/Connection/Hyper phase/Sleep metadata 须进入完整 scaffold census；不能把旧九池 receipt 当新增完整图证明。持续 local stream128、production Control/perlane/handshake concurrency gates、connection independent2MiB 分项、FE issuer/整窗/closing 与后续 P04–P10 继续开放。没有完整 Native1FE+3BE、workspace/SQL/system 或性能结论；按当前切片定向验证，未触发里程碑全量。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-initial-settings-deadline/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-initial-settings-deadline/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-initial-settings-deadline/reproduce_features.py
```

Docker desktop-linux 完整 fixture BOM 本轮 live PASS，image/JAR 齐全，没有 pull/build 输入或改变 global context。无 push、PR 或 archive；persistent goal active，fast 设置不改变批准的实现终态。
