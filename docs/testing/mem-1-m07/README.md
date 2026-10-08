# MEM-1 M07 执行证据

> 原始日志及批量测试产物已于 2026-10-06 移至 Git 外的本地归档；保存边界、历史 SHA 映射与获取限制见 [证据保存说明](evidence/README.md)。历史文件路径仅作归档定位，不表示仓库内存在原始产物。

本目录记录已批准的 spec / plan 第 5 版的本地实施与验收。P00/P01/P02/P03 本地检查点已保存，P04 正在执行；这里的目标参数或源码审查不代表产品已经实现或通过验收。

## 第 7 版续接（2026-10-08）

accepted spec与本次获批plan revision7在 `codex/mem-1-m07-v7-resume` 本地继续，代码从fork
`c313bc1bb`（与用户给定`92fe1f718`同code tree）续接。P07 SET整窗/compiler实际退出已保存，
FE Membership共享有限准入已补齐（[收据](evidence/p07-membership-ingress.md)）。P06s独立
worktree检查点已集成（[收据](evidence/p06s-external-listing.md)）：D15 trusted endpoint例外、
REST单页公开接口、generation8 gate/ctx期限、FS List16MiB非retry及维护streaming边界。

P06s组件1492PASS/1既有ignored，Frontend错误传播3PASS，vendor lib58PASS；vendor
all-targets clippy保留既有integration缺test-utils/async调用失败，未扩大SDK补丁掩盖。
当前集成workspace、剩余P07生产用途/实际Local aliases/Internal执行owner、P08唯一切换、
P00b测量、P09原生1FE+3BE/性能与P10仍OPEN。c_*仍null，旧FE/BE保护保留，无push/PR/归档。
以下v5/v6记录按历史解释，不冒称当前完整验收。

## 第 6 版（2026-10-06）

spec 与 plan 已改为第 6 版并获批：计量边界改为 NovaRocks 自有对象；撤回十个第三方 vendor patch，Native 传输只用上游公开配置与库外准入（ADR-0168）。执行在同一分支继续，先合入 main（`f008e2682`），再剥离 v5 的传输计费。R1 已删除的模块：`native_transport_capacity`、`native_response`、`native_task_executor`、`native_channel_worker_capacity` 及专测补丁的集成测试；保留并迁移的 D12 行为：Data/Control 独立 listener 与拒绝不失败的 accept 循环、端点方法分类、按 peer process/endpoint/lane 的单飞有界 channel 缓存、调用方进程签名与连接封印。R2 待补：每 lane 的 FE 连接数与 stream 位置持有到 body 退出、DNS 在自有阻塞闭包中解析、FD 上限、lane 指标与 NIG-1 交接口径。下文第 5 版记录保留为历史。

2026-10-08 P00b 部分输出：[结构算术](transport-envelope-v1.md) / [JSON](transport-envelope-v1.json)
按当前 R2 geometry 复算；profile 与 check_profile 不再使用 v5 每连接2MiB、Native3GiB/整体16GiB
断言。这些旧数值归入 historical_v5_transport_targets，不能被当作当前容量 grant。
R2 计数/公开退出接缝已有模块验证；系数、lane grid、repeatability、soak 与当前 main 旧路径基线
尚未完成，结构算术不是测量通过。后续 P07/P08/P09/P10 仍在执行，未发布。

## 批准与基线

- 2026-10-01 用户批准已落盘 plan 第 5 版，并授权 sub-agent。
- 实施分支：`codex/mem-1-m07-be-encoded-results`。
- 设计审查基线：`b1d13989c50231ee7a5031163255b2607c5176c0`。
- 执行 / 旧版测量基线：`eb35251de575e071ad3657d0ce0fc1fc95d1a91a`，启动时工作树干净，已核实与 `origin/main` 一致。
- 两基线之间的相关变化：M02a 已替换 memory core 并删除 memory-arrow；SQL / decimal / complex-type golden 有独立语义修复。M07 必须消费当前代码，不恢复旧 Charge / Reservation API，也不改写既有 golden 来掩盖结果差异。
- 持久 goal 包含 P00–P10 的本地实现、1FE+3BE 功能与冻结性能门、最终同 SHA workspace / SQL / system 和交接。当前没有 push / PR 授权。

## 证据状态

| 内容 | 状态 | 说明 |
|---|---|---|
| producer / transport / MySQL census | 已调查 | 三个只读 sub-agent 独立调查，已归入 coverage；准确产品接线仍待实现 |
| profile / 完整 checked 包络 | 冻结目标 v1，算术 PASS | profile-v1.json / owner-envelopes-v1.md；actual allocation/holder 防护是后续实施与验收门，尚未产品实现 |
| 旧版 release / dev-opt 构建 | PASS | 原始构建与 binary hash 见旧基线 index |
| macOS 原生 1FE+3BE 旧路径 | 部分 PASS | 短查询/慢读/type smoke 和四个功能场景已留存；扩展 wire 测量准备中 |
| Linux 正式测试 | 用户后续手动执行 | 用户明确取消本次 agent 的 Linux 测试要求；不阻塞本地实施，保留复现材料 |
| 共享 REST / MinIO fixture | READY | `DOCKER_CONTEXT=desktop-linux` 的 BOM 离线校验/up 通过；原 OrbStack 空 daemon 判断已纠正，无需下载输入 |
| P01 / P02 / P03 | 已完成对应切片 | 共享合同/纯 renderer/vendor framing 已留收据；完整产品切换仍待 P04–P08 |
| P04 context-root core | 定向通过，P04 继续 | [原核心收据](evidence/p04-context-root/README.md)：301 Worker、54 Native host、3 Observable、28 Renderer；后续 Host producer 接管和 reader 收据列于下文，真实 fetch/lane 仍待接线 |

## 验证口径

每份运行收据绑定完整 source SHA、binary hash、实际 role 配置语义、机器、三 BE 身份、fixture publication / 数据身份、负载与冻结门。原始错误、拒绝和超时保留，不只统计成功子集。功能用 dev-opt，正式性能旧版与候选使用同 toolchain release；macOS scratch 与 Linux 正式结果分别记录。

最终 CI 使用 `tools/ci/local-full-ci.sh --tier full --all-discovered --cluster-mode cross-process --cluster-size 3`。其默认 system discovery 是 `--list-default`；`--list` 用于定位显式性能 / 外部 fixture 场景。M07 的定向性能另按准确场景执行，不扩大默认功能 CI。

执行期间保留旧 FE 保护与现有 BE retained 防护；只有 P08 的完整生产接线验证成立后删除 FE LRA。M03a/M03b 的真实 Query@BE 分配归属和自身上限、M04b 的 MEM-backed 窗口授权仍是后续独立交接。

2026-10-01 Docker 入口纠正：本机默认 context 为 OrbStack（0 images），预备好的输入在 Docker Desktop。后续 fixture/SQL/system 命令显式设 `DOCKER_CONTEXT=desktop-linux`，不改变全局 Docker context。已核验锁定的 Spark/Paimon/REST/MinIO/mc 和派生镜像/BOM。先前对 OrbStack 执行 provision 的拉取超时保留为错误 daemon 的准备失败，不能归为当前 fixture 缺失。

P00 本地收据：release旧server未改变；原始wire smoke 22/22成功，16/64/256各3轮共11088样本全部保留。16客户端528/528成功；64客户端1016/2112成功；256客户端2196/8448成功，其余服务端1105（尚未细分内部原因，不能断言全来自某一gate）。deterministic结果无重复不一致。旧REST混合场景（MV/ANALYZE/OPTIMIZE+foreground）通过；此smoke不当作正式吞吐/容量验收。测量器四个定向协议测试和格式检查通过。P00的Linux正式集中root、RTT/control、完整CPU/network对照收据按用户指示由用户后补；保留版本化门，不阻塞本次本地实施，不称已性能通过。

P01环境复核：`--consumer iceberg-rest`再次通过；`--consumer all`发现已有`paimon-writer`派生镜像定义收据不匹配当前源码（prepared `320bd65a…`，current `23dc030b…`），不是镜像缺失。完整CI的Paimon输入尚未通过当前版本核验；独立准备/准确版本验证留在P09/P10入口，不能沿用REST通过冒称全BOM通过。

Paimon定义差异已只读定位：prepared版本对应`3af07efbb`，当前变化来自已合入`375c9e0ea`的host env parser（unset/变量名/shlex quoting）；Dockerfile/versions/JAR lock/SQL/golden未变。全部5个canonical image、6个JAR已验证存在且hash正确，当前派生image仍带旧definition。完整CI无条件all verify+Paimon准备，故需在独立测试准备阶段用已验证本地输入真实重建并经owner发布，不能仅改BOM/label；现有provision无条件重新下载JAR，不直接使用其联网流程。当前REST运行实例固定image ID不受current alias重建影响。

P04 context-root 核心收据已保存：新增 immutable 通道/物理 backing owner、context fence 接管与读取、正常 seal/abort/lease 收敛；allocation failure callback、schema spare 合并容量和默认并行 allocator probe 已修复且反例通过。Native host 仍为 root=None、V1 未 advertise；P04/P05–P10 继续。此前 Paimon definition 不匹配已在 P02 离线修正，当前 all-consumer 校验通过，详见 [fixture-ready](evidence/fixture-ready/README.md)。

P04最后pull/Execution生命周期检查点：唯一事前input grant、original-carrier直移、built DOP绑定、explicit RootRegistration与actual-exit成功门已定向通过；[收据](evidence/p04-last-pull/README.md)。P04仍执行，原始Arrow/schema metadata实际backing证明与Native producer/fetch/lane产品接线继续。

P04 metadata 来源已接真实 Native output / ChunkSchema / ExecPlan lowering / LocalProgram projection，且覆盖嵌套 carrier 派生和独立 work 上限；[定向收据](evidence/p04-source-origin/README.md)。完整 actual Arrow/Chunk backing proof 与 Native producer 仍继续，未宣称产品验收。

P04来源接线进一步覆盖Native scan/Project和生产Project/hydration，包含重复occurrence/CSE/empty输出及known/unknown同结果反例；[收据](evidence/p04-scan-project-hydration/README.md)。完整Arrow/Chunk source proof、exchange与Native finite producer仍继续。

[P04 input backing and process-pool reservation](evidence/p04-input-backing-pool/README.md)

P04 有限 Native producer/session 与固定唤醒槽位已完成模块切片，184 项定向测试及 check/Clippy/fmt 通过；[收据](evidence/p04-native-producer/README.md)。实际 host、显式 domain/read ingress/lane/source 接线继续，V1 未 advertise；不称 C4/native/performance 验收完成。Docker Desktop all-consumer live 校验通过，无缺失镜像。

P04 真实 Host 的 ClientRows/CountOnly 接管与 context 留存已保存本地行为切片，553 项相关测试及四包 all-target Clippy/fmt/diff 通过；[收据](evidence/p04-native-host/README.md)。包含准确取消原因 fan-out 竞态、多 DOP、准备回滚、连续 observer panic 的真实释放反例；InternalFacts/source/read ingress/Native lane 仍继续，P04 未完成、V1 未 advertise，无性能验收结论。完整 desktop-linux fixture 校验通过，当前无缺失镜像或 JAR。

P04 seal/ACK 竞态端口已准确携带实际接受水位，codec 仅允许 closed marker 保留未生效 ACK；[收据](evidence/p04-seal-watermark/README.md)，Worker309/codec5/真实Host61共375定向通过。实际Native服务和传输copy生命周期继续，P04未完成。

P04 StatisticsArtifactV1 独立流式 codec 已完成模块切片，main 通过真实 library consumer 的12项定向测试/目标Clippy/Native all-target check/fmt/diff；[收据](evidence/p04-statistics-codec/README.md)。STA1准确声明先验、逐turn工作及实际零分配已证明；Session/SQL/FE/源头32MiB增长防护仍继续，未开启该domain产品路径。

P04 Native reader 的同一 admission ACK/完整发送 pregrant 与 strong-only owner 已保存模块切片，401 项相关测试和两包 all-target Clippy/fmt/diff 通过；[收据](evidence/p04-native-reader/README.md)。包括满池 ACK、真实 alias、回调 seal、task horizon 后 replay；真实 HTTP/H2 仍未接线，P04 继续、V1 未 advertise。统计源头的 Unpivot→Project→Root permit 缝隙已准确定位，见 [增长审计](statistics-source-growth-audit.md)。

P04 Bytes wrapper 的真实 allocator exit 已接原 segment/offered/read owner，negative mutant、normal/两类 panic、Barrier alias 和 Miri 通过；[收据](evidence/p04-bytes-physical-exit/README.md)。vendor 全 suite 1,250、consumer 490 项及 workspace all-target check 通过；既有 Clippy/依赖解析失败保留。真实 Tonic/HTTP/H2 与 lane 接线仍继续，P04 未完成。

P04 真实 Tonic unary 的 post-admission metadata/初始 buffer/concrete Body/last DATA alias 已完成模块切片，187项相关测试、Frameguard负向反例、目标all-target Clippy/fmt/diff通过；[收据](evidence/p04-native-unary/README.md)。未安装新FetchTaskResult签名，pre-decode lane/stream/headers/outerframework及H2独立副本仍待接线；P04继续、V1未advertise。

P04 固定 H2 writer 与本地出站 frame cap 已完成可验证切片；[收据](evidence/p04-h2-fixed-writer/README.md)。68 项实际 H2/Hyper/Tonic 协议、Native574/Worker313共955非重复测试及独立物理分配/退出与Miri通过，五类负向验证真实失败并恢复。只闭合 writer Vec/Core 与相关转发；HeaderMap/HPACK/queue/framecopy/stream/task/socket/TLS和完整2MiB连接包络、Native安装继续，P04仍executing/V1未advertise。desktop-linux fixture完整BOM校验通过，无缺Docker image/JAR；Linux继续由用户手动测试。

P04 本地出站HPACK表上限已完成切片；[收据](evidence/p04-h2-send-header-table/README.md)。fresh0在peer大设置下保留正确size-update/static/literal语义且Table两容器0分配，positive仅逻辑界、晚清零保留spare。75实际协议/Native574/Worker313共962非重复workspace tests、actualsource35/1ignored及7Miri通过；五类runtime负例实际失败/byteexact恢复。wholeheaderblock/HTTPmetadata与完整2MiBconnection/Native安装继续，P04executing/V1未advertise。

线协议探针校验完整 ColumnDefinition41 结构，并比较执行前 v4 冻结的列名和 MySQL type。v3 的合法大行 root backing 拒绝已保留为 FAIL；历史行字节观察不证明列元数据正确。原生复跑须使用强化探针，全部输入与上限不变。

`result-delivery/root-read-profile-kind-refusal` 使用真实认证 Native RootResult endpoint：结构合法的 foreign-process V1 请求必须进入准确身份拒绝；其余六个仅改变 profile/kind/sequence 的请求必须在结构 decode 被拒绝。此场景明确限定 authenticated plaintext/IP；每次请求有绝对 RPC probe deadline 与 4KiB gRPC 请求 frame 与累计响应 DATA 界（不作为 header/第三方分配界），不外推 DNS/TLS teardown 界，完成后用真正 Native SUM 查询验证精确行字节及公开 owner 收敛。冻结输入 `inputs/root-read-refusal-freeze-v1.json`；不证明 installed-root replay/ACK/生产退休/ClosingRow，也不证明非法响应拒绝。


### 2026-10-09：P09 paused-client 取消场景，native 待执行

- 首次 native 前冻结 `root-cancel-closing-freeze-v1.json`：原 S+8 单行暂停客户端读取，要求独立 Closing=1/Client=0、完整 row+ERR1317、同 socket 精确 SUM oracle 与实际 Native task 增长；原17×1MiB合法行冻结到W=2未完成producer，要求正 wire prefix/零完整行、物理EOF/reset、同 socket 后续零schema/packet/bytes且三BE task counter不增长。握手真实connection ID用于KILL，无猜测。
- KILL start/return/resume时间保存；返回后复查原2s观察截止，Closing观察2s从KILL返回计算，不作为生产5s deadline的起点证明。失败仍恢复并join客户端保存wire；不证明具体framing cursor/partial-tail/full-pool/allocator exit。生产配置、容量和deadline未改。
- runner all-target100 PASS/0 FAIL/2既有ignored，覆盖真实handshake身份拒绝与follow-up错误分类；最初缺少test import的E0425保留。C0 from_ref测试清理已纳入。两个主要语义裁决/P09其它门/P00b/P10/final仍OPEN；无push/PR/归档。

取消探针收紧：runner101 PASS/0 FAIL/2 ignored。真实短header后EOF反例记录3 wire bytes但0 packets，后续拒绝必须wire_bytes=0；resume后timing写失败延后到join/save wire后传播。独立只读复核已修正两项具体证据漏洞。native仍待执行。


### 2026-10-09：P09 paused-client 取消与留存复跑 native PASS

- 干净 `43463c6f678341e979629cc51ca7387fd813aa1f`、实际同build identity，3个独立native1FE+3BE场景全部PASS，总7.575s；原root-retention场景复跑验证actor改动，无server配置/容量/deadline修改。12个精确FE/BE PID已退出。收据 `p09-row-cancel-native-pass-20261009.json` 保存准确binary/input hashes与原始观测。
- resident S+8：连续两次W=2/producer guard exited/task退休/End未ACK；KILL仅0.789ms，101ms后观察Client=0/Closing=1；恢复得到完整1048580B行、5packets、独立digest/schema一致、ERR1317。同socket SUM精确5050/native task counter总增2。
- missing-tail17MiB：W=2/payload2S/producer仍running/End未发布，KILL后收到655360B行前缀、零完整行并真实EOF；后续同socket查询zero wire/schema/row/packet、EOF且三BE task counter完全不变。KILL仅0.755ms。该行为不证明FE parser未收到命令，不外推具体framing cursor/partial-tail/fullpool/allocator最后退出。
- 定向runner101 PASS/0 FAIL/2既有ignored；上一个产品切片C0为9695源码12280/0/7，当前test-only增量不冒充final同HEAD C0。ACK-only、small/large精确partial矩阵、满池、real CL、CM/CP/P00b/P10/final及两项待裁决语义继续OPEN，无push/PR/归档。


### 2026-10-09：P09 installed-root ACK/replay 冻结，native 待执行

- 新 `installed-root-protocol-freeze-v1.json` 依附准确原S+8 retention输入：解析三BE fresh TaskCreateApplied完整typed identities，仅允许同一execution、无重复、occupied BE最多8候选；零ACK路由必须定位恰一个真实installed root，其余仅exact UnknownRoot/status5/零DATA。不猜stage/task或假定单task，不加产品marker/registry。
- ZeroAck场景两次ACK0/no-retirement后恢复正常wire独立oracle。FinalAck独立协议干扰场景真实fetch Data1(S)/Data2(8B+End3/rows1)、replay1摘要/元数据等价、实际End3后ACK3两次、Retired1，逐步fresh census要求context持有/producer exited、最后Data0/payload0/EndACK1；随后KILL/resume/save事实wire，不宣称正常结果或physical-last-alias。
- Whole probe链5s，实际BE上游认证plaintext/IP；请求frame4KiB/累计响应DATA≤S+4096，逐块释放h2 flow-control。严格唯一application/grpc、有DATA成功仅final trailer0、重复status拒绝、单未压缩message，使用生产task-codec解码与实际proven watermark，不设无限上界。Busy/Preparing/其它状态直接FAIL不重试。失败仍resume/join/savewire。
- runner all-target111 PASS/0 FAIL/2既有ignored；10个helper负例覆盖实际身份/候选与response结构。独立只读复核重算native prefix与Data1/Data2摘要，并修正content-type/status位置宽松点。生产配置/容量/deadline未改；P09其它门/两个裁决/P00b/P10/final仍OPEN，无push/PR/归档。

P09 installed-root pre-native接入修正：此前request builder的wait=0违反生产RootResultRead正值契约，v1未执行native、原输入保留。v2明确冻结100ms request wait并附v1 SHA/correction；原S+8/全部操作/容量/5s链期限/20s actor不变，生产配置不变。新增实际冻结请求经过生产decode_read与zero拒绝反例，runner112 PASS/0 FAIL/2既有ignored。native待执行。


### 2026-10-09：P09 native空拒绝响应缺口与修正

- 首次clean63bb/v2 installed-root native失败（4.757s）：准确BE1/stage1/task1 typed零ACK定位成功，stage2/task2非root拒绝无法满足strict trailers-only；后者实为相同execution的第二个真实task，未猜root。失败仍resume/join，原1048580B row/独立schema/hash正常，4精确PID已退出；FinalAck场景未执行。收据 `p09-installed-root-native-fail-20261009.json`，不转换为PASS。
- 根因是 `native_ingress.rs` 的OwnedResponseBody只poll_frame，未转发inner.is_end_stream/size_hint；内层Tonic空status response被默认false掩盖，Hyper产生empty EOS DATA，违反gRPC拒绝应在status HEADERS上结束的结构。真实Hyper+h2 duplex回归旧码0PASS/1FAIL，修正后Native lib720PASS/0FAIL；收据 `p09-empty-grpc-refusal-focused-20261009.json`。
- 最小修法仅转发两个inner facts，ownership仍由实际Drop/last DATA alias退出，不主动释放permit、不改变status/容量/deadline/owner分类；strict probe保留。原生复跑待执行，跨共享Native响应包装器修正触发一次C0里程碑。两个待裁决语义与其它P09/P00b/P10/final仍OPEN，无push/PR/归档。
