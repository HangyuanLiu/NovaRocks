# P04：原始 resident Stream Store 与有限 readiness 位置

Parent `70902f23acf06a97d681342d9e1c0fc4c3e04b0a`。approved spec/plan v5 不变；P04 executing、P05–P10 open、V1 不 advertise。Linux 由用户后续手动执行，无 push/PR/archive。

Native 的第十项原能力为 `StreamStoreBuffer`：构造前 checked 两个 typed 数组与 Core/Arc 的准确 allocation bound；两次 try_reserve_exact 和一次 Arc 原分配，空槽不构造 Stream。opaque strong-only aliases 只能一次绑定，CAS 唯一胜者提取原数组，失败在两端首次 I/O 前拒绝。固定 Store 直接消费这些数组，线性 ID 查找有硬界，不再使用会增长的 Slab/IndexMap；None 保留原存储。最后 Core Arc 先退出，再退出仍未绑定的数组及原 owner；绑定后的 Stream/Waker 与数组先退出，最后才退出原 owner。

真实 resident limit 为128，包含 queued、reserved、reset、closed 与 retained StreamRef；peer SETTINGS 的1000/省略/0不会覆盖此界。request、incoming HEADERS/PUSH、local PUSH 和未知 reset 均在新 resident 构造前检查原位置。incoming shortage 使用已有单位置 REFUSED_STREAM 路径，在推进下一帧前发送，不能绕过固定 Store 派发；未知 reset 满额为明确 connection error。wire stream关闭、task退出和原数组退场是不同事件，闭合 wire 的实际 RecvStream alias仍保留其 resident及原 stock。

每个 SendRequest clone 独占一个有限 readiness lease，重复 poll替换旧 Waker并锁外drop；ready/error/cancel明确注销。唤醒只标记原固定位置，InnerGuard先释放原 Mutex，再逐原位置短持锁取通知、解锁wake，不新增 Vec/Box/Arc backing。唤醒不提前释放 lease；取消或下一 ready 才归还。None/no-pending/Ready不触碰用户 Waker。既有 Stream task callbacks与任意用户自行构造的强引用环不属于本切片的自动退场证明。

独立只读审查发现 malformed local PUSH在child占槽后用`?`早退，遗漏rollback。公开测试先准确失败101，修复把POST/Content-Length转换检查放在child构造前，保留promised ID推进；合法GET仍能使用第二位置，真正send失败继续rollback。审查同时发现None多了一次Waker clone；不可变opt-in标记和公开RawWaker计数恢复旧行为。两项修复经最终只读确认，无本切片剩余阻点。

第十项加入同一启动原 issuer：Data518/Control20不变，checked pool **6,452,189 bytes/position**，fixed stock **3,471,278,436 bytes**。没有第二钱包。stock只是原池容量，不是Data32/Control8实际握手门；production仍全Data，per-lane/handshake/Control listener与cache single-flight尚未安装。原位置仍由全部十项能力及字段alias的最后物理退出归还。

最终13个相关协议目标 **130 PASS**，六组Native lib **50 PASS**；新增公开14项（resident10/owner4）。实际普通 primitive五项与Miri同五项全部通过，仅证明真实arrays/Core/once-CAS，不外推完整H2/Hyper/Native。公开System oracle观察128/128构造准确三项分配、原指针、真实dealloc先于原credit；实际Hyper两端/Tonic透传once-bound失败时IO0/service0。实际H2检查peer1000/None/0、closed alias复用/连接退出后保留、cloned pending、本地reserved PUSH、remote第三条RST7、finite4 waiters、16次cancel与不同Waker repoll、EOF wake、解锁后actual StreamRef destructor重入以及None零clone。组件2槽/4waiters测试没有改变Native128几何。

七个actual-source negatives均编译后runtime FAILED，finally精确恢复：丢弃原Store、取消不注销、锁内wake、None无条件clone、Hyper client/server遗漏透传、Tonic遗漏透传。最终pin后180项及质量检查再次通过。最早Config字段/私有模块/不可Clone binding编译失败、remote case最初错误期待整连接关闭导致watchdog、waiter错误类型/try_lock遗漏、negative driver Tonic anchor不一致均保存完整日志；修正没有削弱原界。malformed PUSH先失败再修复的记录亦保存。

Native check、H2/Hyper/HTTP/Tonic strict libClippy、Native相关十target及production libClippy、root/vendor fmt和diff check全部0。既有Native/依赖warnings保留，新两个target无warning，不声称全部cfg(test)零warning。三个独立最小Hyper client/http2、server/http2、Tonic channel-only均0，63个依赖identity逐项匹配production lock。50个产品/测试/原issuer/geometry/lock pins、270个完整五vendor Rust/manifests pins、61份lossless日志/diff及probe/feature manifests与lock保存于本目录。private probe仅对完整actual normal sources的scratch副本追加直接bind/观察wrapper，不复制算法；上游H2 dev-dependency suite在production workspace不可运行，未更新锁或下载输入。

完整冻结分项仍为 independent2MiB + streams×(4KiB bookkeeping+8KiB idledecoder+32KiB headers) + Tonicpending8×4KiB。固定Store数组不证明每Stream动态queue/payload、Inner/sendbuffer/Mutex、socket/TLS/task/error/auth/future/cache等全部原授图，也不把6.45MB总池冒称独立2MiB证明。原ResultWriteCredit callback Box在release之后才退出，issuer/callback图完整physical reclamation仍开放。没有全workspace/SQL/system/Native1FE+3BE/性能验收；最终门留待P04–P10收敛。当前desktop-linux fixture BOM已确认image/JAR齐备，未pull/build输入或改变全局Docker context。fast不缩减批准范围，goal继续active。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-resident-stream-store/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-resident-stream-store/run_regressions.py
python3 docs/testing/mem-1-m07/evidence/p04-resident-stream-store/reproduce.py --ordinary --miri
python3 docs/testing/mem-1-m07/evidence/p04-resident-stream-store/reproduce_features.py
```
