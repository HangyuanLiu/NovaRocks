# MEM-1 M07 执行证据

> 原始日志及批量测试产物已于 2026-10-06 移至 Git 外的本地归档；保存边界、历史 SHA 映射与获取限制见 [证据保存说明](evidence/README.md)。历史文件路径仅作归档定位，不表示仓库内存在原始产物。

本目录记录已批准的 spec / plan 第 5 版的本地实施与验收。P00/P01/P02/P03 本地检查点已保存，P04 正在执行；这里的目标参数或源码审查不代表产品已经实现或通过验收。

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
