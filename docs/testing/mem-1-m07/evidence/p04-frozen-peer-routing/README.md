# P04：冻结 peer 身份与 BE outbound cache 隔离

Parent `e563352924ee5fd405981c3970aa20ee0d17826c`。approved spec/plan v5 不变；P04 executing、P05–P10 open，V1 不 advertise。Linux 由用户手动测试，无 push/PR/archive。

FE 从 sealed ParticipantTopology 将准确 BackendProcessId 写入 Filter RemotePeer。wire 的 mandatory bytes 必须恰为16字节且是非 nil UUIDv7；Native decode 拒绝缺失、错误长度、nil 与非 v7，无猜测、默认或当前 membership 重查。Worker 保存 typed process，实际 Remote routing decision 与 Filter client 消费同一事实。Exchange 首次拨号前必须取得已冻结 destination TaskIdentity 的 process。

BE role-local NativeChannelKey 同时包含 process、endpoint 和唯一 manifest 的 method。当前三个 BE-origin 方法各自对应独立 traffic lane；Exchange/Filter 必须带 BE process，Membership 必须不带 BE process，FE-origin/retired 方法不能进入此 cache。BE client 的两个显式构造入口保留这些域；旧请求晚 insert 只写旧 process 的 key，不覆盖 replacement。

真实 TCP/Hyper H2/原 Tonic Channel 测试通过实际响应的 connection id 观察：同 endpoint 的 replacement process 与另一 method 各建独立连接，同 process 的另一 endpoint 也隔离，重复原 key 使用原连接。测试使用普通 no-capacity Backend runtime，明确不证明远端 process 认证、完整 funding、physical generation、single-flight 或 per-lane admission。服务与 executor 子任务显式停止并 join，fixture 仅移除自己的 cache keys。

三个 actual-source negatives 均编译后 runtime FAILED，finally 逐字恢复：cache 漏 process、cache 漏 lane/method、decoder 为缺失 process 发明值。FE compiler、Native install、Worker route 的真实生产调用测试分别覆盖冻结身份、非法输入及 decision；Exchange 缺失身份用实际 nonblocking listener 证明首次拨号前拒绝。独立只读审核未发现本切片阻断项。

公共 wire/route 变化按 contract §8.1(3)触发完整 Cargo 检查。首次独立 workspace run 在136个成功 target groups（7,743 passed、5 ignored）后暴露真实 BE 默认预算遗漏：原 stock 已需3.47GB，而 Server joint cap 仍256MiB。默认值改为批准的 frozen4GiB，显式小预算仍拒绝，issuer 原错误保留；原失败 binary case 修正后单跑通过。这是提前补入 P08 默认接线，不代表默认 V1 产品支持。

Cargo-only CI 的前置失败也保留：path bytes/h2 的 registry advisory ignores 不再匹配，严格 unused=deny 拒绝；移除活动旧 ignores，在两个 vendor PATCH 中保留 known advisory/source audit OPEN，不称漏洞修复。Native wire/PhysicalPlan/LocalProgram/NCP-8 guards 漏列早前新增的 dependency-free result-contract 与准确 vendor 身份；补显式 owner 集合及 version/source/Cargo id/manifest path，拒绝任意同名替换、runtime/build/feature/target 扩张。LocalProgram18项、NCP-8正例及16负例通过；完整结果在 verification.json 与压缩原始日志中记录。

下一继续 finite peer/lane owner、每次真实 reconnect 的 checked generation、single-flight 与 Live/Connecting/Closing；Closing 必须等十池最后 alias 真实退出。仍无生产独立 Control listener、完整 transport allocation 图、FE 整窗/closing 或 Native1FE+3BE/SQL/system/性能验收。

最终 `NOVA_CI_CARGO_PROFILE=dev tools/ci/local-full-ci.sh --cargo-only` 在 `logs/ci-full/20261003-115649` 全部通过（890秒）：所有脚本/依赖政策守卫、fmt、workspace全目标check、无jemalloc Server check、Clippy、workspace build、SQL错误清单及三组测试。组件11,647 passed/7 ignored（187成功target groups）、Server owner165 passed、binary smoke3 passed，总**11,815 passed、7 ignored**。已有warnings保留，未称零warning。25个改变的源码/政策/来源记录pins、270个完整vendor source/manifests pins、140份lossless日志/diff已核验；测试使用相同dirty实现与pins，CI parent SHA不是新检查点SHA。此 Cargo-only 结论不含SQL suites、system scenarios或完整生产拓扑验收。

```bash
python3 docs/testing/mem-1-m07/evidence/p04-frozen-peer-routing/verify.py
python3 docs/testing/mem-1-m07/evidence/p04-frozen-peer-routing/run_regressions.py
NOVA_CI_CARGO_PROFILE=dev tools/ci/local-full-ci.sh --cargo-only
```
